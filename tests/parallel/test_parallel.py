import copy
import datetime
import glob
import importlib
import logging
import os
import string
from multiprocessing import Manager
from random import random
from time import sleep

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from utility.log import LOG_FORMAT, Log
from utility.utils import magna_url

parallel_log = Log(__name__)
polarion_default_url = (
    "https://polarion.engineering.redhat.com/polarion/#/project/CEPH/workitem?id="
)


class MultiModuleFilter(logging.Filter):
    """Custom filter to log only messages from specific modules."""

    def __init__(self, module_names):
        super().__init__()
        self.module_names = module_names

    def filter(self, record):
        """Filter logs to include only those from the specified modules."""
        return any(module_name in record.module for module_name in self.module_names)


# Add a filter for `test_parallel` to parallel_log
module_filter = MultiModuleFilter(["test_parallel", "run"])

# Ensure the filter is applied to all handlers of parallel_log
for handler in parallel_log.logger.handlers:
    handler.addFilter(module_filter)


def run(**kwargs):
    """Main function to run parallel tests."""
    # Use a Manager dictionary for shared state across processes
    with Manager() as manager:
        results = manager.dict()
        parallel_tests = kwargs["parallel"]
        parallel_tcs = manager.list()
        max_time = kwargs.get("config", {}).get("max_time", None)
        wait_till_complete = kwargs.get("config", {}).get("wait_till_complete", True)
        cancel_pending = kwargs.get("config", {}).get("cancel_pending", False)
        parallel_log.info(kwargs)

        with parallel(
            thread_pool=False,
            timeout=max_time,
            shutdown_wait=wait_till_complete,
            shutdown_cancel_pending=cancel_pending,
        ) as p:
            for test in parallel_tests:
                p.spawn(execute, test, kwargs, results, parallel_tcs)
                sleep(1)  # Avoid overloading processes

        # Convert results to a regular dictionary to inspect
        results = dict(results)
        parallel_log.info(f"Final test results: {results}")
        parallel_log.info(f"Parallel test cases: {parallel_tcs}")
        test_rc = 0

        for key, value in results.items():
            parallel_log.info(
                f"{key} test result is {'PASS' if value == 0 else 'FAILED'}"
            )
            if value != 0:
                test_rc = value

        return list(parallel_tcs), test_rc


def execute(test, args, results, parallel_tcs):
    """
    Executes the test in parallel.

    Args:
        test: The test module to execute.
        args: Arguments passed to the test.
        results: Shared dictionary to store test results.
        parallel_tcs: Shared list to store test case details.
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
    if not args.get("tc"):
        raise CommandFailed(f"Test case is not passed as part of args : {args}")
    tc = copy.deepcopy(args["tc"])
    if test.get("polarion-id"):
        tc["polarion-id-link"] = f"{polarion_default_url}/{test.get('polarion-id')}"
    test_name = test_name.replace(" ", "_")
    _log_matches = glob.glob(os.path.join(run_dir, f"{test_name}.*"))
    if _log_matches:
        _prefix = "".join(random.choices(string.ascii_letters + string.digits, k=4))
        file_name = f"{test_name}-{_prefix}"
    else:
        file_name = f"{test_name}"
    log_url = f"{url_base}/{file_name}.log"
    log_file = os.path.join(run_dir, f"{file_name}.log")
    parallel_log.info(f"Log File location for test {test_name}: {log_url}")

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
    err_logfile = os.path.join(run_dir, f"{file_name}.err")
    error_handler = logging.FileHandler(err_logfile)
    error_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    error_handler.setLevel(logging.ERROR)  # This ensures only errors are logged
    test_logger.logger.addHandler(error_handler)

    test_logger.info(f"Starting test: {test_name}")
    try:
        # Import and execute the test module
        mod_file_name = os.path.splitext(test.get("module"))[0]
        test_mod = importlib.import_module(mod_file_name)
        run_config = {
            "log_dir": run_dir,
            "run_id": args["run_config"]["run_id"],
        }
        start = datetime.datetime.now()

        # Merging configurations safely
        test_config = args.get("config", {}).copy()
        test_config.update(test.get("config", {}))
        tc["name"] = test.get("name")
        tc["desc"] = test.get("desc")
        tc["log-link"] = log_url
        rc = test_mod.run(
            ceph_cluster=args.get("ceph_cluster"),
            ceph_nodes=args.get("ceph_nodes"),
            config=test_config,
            parallel=args.get("parallel"),
            test_data=args.get("test_data"),
            ceph_cluster_dict=args.get("ceph_cluster_dict"),
            clients=args.get("clients"),
            run_config=run_config,
        )
        elapsed = datetime.datetime.now() - start
        tc["duration"] = str(elapsed)
        results[test_name] = rc

        test_logger.info(
            f"Test {test_name} completed with result: {'PASS' if rc == 0 else 'FAILED'}"
        )
        tc["status"] = "Pass" if rc == 0 else "Failed"
        if rc == -1:
            tc["status"] = "Skipped"

        parallel_tcs.append(tc)
    except KeyError as e:
        parallel_log.error(f"Missing required argument: {e}")
        raise
    except Exception as e:
        test_logger.error(f"Test {test_name} failed with error: {e}")
        results[test_name] = 1
    finally:
        # Reset root logger configuration to avoid conflicts
        for handler in logging.getLogger().handlers[:]:
            logging.getLogger().removeHandler(handler)
