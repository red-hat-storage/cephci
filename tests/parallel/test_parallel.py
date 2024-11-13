import importlib
import logging
import os
from multiprocessing import Manager
from time import sleep

from ceph.parallel import parallel
from utility.log import Log
from utility.utils import magna_url

log = Log(__name__)
LOG_FORMAT = "%(asctime)s (%(name)s) - %(module)s:%(lineno)d - %(funcName)s - [%(levelname)s] - %(message)s"


def run(**kwargs):
    # Use a Manager dictionary for shared state across processes
    with Manager() as manager:
        results = manager.dict()
        parallel_tests = kwargs["parallel"]
        max_time = kwargs.get("config", {}).get("max_time", None)
        wait_till_complete = kwargs.get("config", {}).get("wait_till_complete", True)
        cancel_pending = kwargs.get("config", {}).get("cancel_pending", False)

        with parallel(
            thread_pool=False,
            timeout=max_time,
            shutdown_wait=wait_till_complete,
            shutdown_cancel_pending=cancel_pending,
        ) as p:
            for test in parallel_tests:
                p.spawn(execute, test, kwargs, results)
                sleep(1)

        # Convert results to a regular dictionary to inspect
        results = dict(results)
        log.info(f"Final test results: {results}")

        test_rc = 0
        for key, value in results.items():
            log.info(f"{key} test result is {'PASS' if value == 0 else 'FAILED'}")
            if value != 0:
                test_rc = value

        return test_rc


def execute(test, args, results):
    """
    Executes the test under parallel in module named 'Parallel run' parallely.

    Args:
        test: The test module which needs to be executed
        args: Arguments passed to the test
        results: Shared dictionary to store test results
    """
    test = test.get("test")
    test_name = test.get("name", "unknown_test")
    module_name = os.path.splitext(test.get("module"))[0]
    run_dir = args["run_config"]["log_dir"]
    url_base = (
        magna_url + run_dir.split("/")[-1]
        if "/ceph/cephci-jenkins" in run_dir
        else run_dir
    )
    log_url = f"{url_base}/{test_name}.log"
    log_file = os.path.join(run_dir, f"{test_name}.log")
    log.info(f"Log File location for test {test_name} is {log_url}")
    # Configure logger for this test
    test_logger = Log(module_name)
    for handler in logging.getLogger().handlers[:]:
        logging.getLogger().removeHandler(handler)
    logging.basicConfig(
        handlers=[logging.FileHandler(log_file)], level=logging.INFO, format=LOG_FORMAT
    )
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(test_logger.log_format))
    if not any(
        isinstance(h, logging.StreamHandler) for h in test_logger.logger.handlers
    ):
        test_logger.logger.addHandler(console_handler)

    test_logger.info(f"Starting test: {test_name}")
    try:
        # Import and execute the test module
        file_name = test.get("module")
        mod_file_name = os.path.splitext(file_name)[0]
        test_mod = importlib.import_module(mod_file_name)
        run_config = {
            "log_dir": run_dir,
            "run_id": args["run_config"]["run_id"],
        }
        rc = test_mod.run(
            ceph_cluster=args.get("ceph_cluster"),
            ceph_nodes=args.get("ceph_nodes"),
            config=args.get("config"),
            parallel=args.get("parallel"),
            test_data=args.get("test_data"),
            ceph_cluster_dict=args.get("ceph_cluster_dict"),
            clients=args.get("clients"),
            run_config=run_config,
        )

        results[test_name] = rc
        test_logger.info(
            f"Test {test_name} completed with result: {'PASS' if rc == 0 else 'FAILED'}"
        )
    except KeyError as e:
        log.error(f"Missing required argument: {e}")
        raise
    except Exception as e:
        log.error(f"An error occurred while running the test module: {e}")
        raise
    except Exception as e:
        test_logger.error(f"Test {test_name} failed with error: {e}")
        results[test_name] = 1
    finally:
        # Reset root logger configuration to avoid conflicts
        for handler in logging.getLogger().handlers[:]:
            logging.getLogger().removeHandler(handler)
