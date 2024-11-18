"""
The purpose of this test module is to run the test in a suite parallely

Syntax:
  - tests:
      -test:
        name: Parallel run
        module: test_parallel.py
        parallel:
           - test:
              ...
           - test:
               ...
        desc: Running tests parallely

Requirement parameters
     ceph_nodes:    The list of node participating in the RHCS environment.
     config:        The test configuration
     parallel:      Consist of test which needs to be executed parallely

Entry Point:
    def run(**kwargs):
"""

import importlib
import os
from time import sleep

from ceph.parallel import parallel
from utility.log import Log

log = Log(__name__)


def run(**kwargs):
    results = {}
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

    test_rc = 0
    for key, value in results.items():
        log.info(f"{key} test result is {'PASS' if value == 0 else 'FAILED'}")
        if value != 0:
            test_rc = value

    return test_rc


def execute(test, args, results: dict):
    """
    Executes the test under parallel in module named 'Parallel run'  parallely.

    It involves the following steps
        - Importing of test module based on test
        - Running the test module

    Args:
        test: The test module which needs to be executed
        cluster: Ceph cluster participating in the test.
        config:  The key/value pairs passed by the tester.
        results: results in dictionary

    Returns:
        int: non-zero on failure, zero on pass
    """

    test = test.get("test")
    if "clusters" in test:
        log.info("Its a multisite setup")
        cluster = test["clusters"].get(
            "ceph-rgw1", test["clusters"][list(test["clusters"].keys())[0]]
        )
        config = cluster["config"]
    else:
        config = test.get("config", dict())
    config.update(args["config"])
    file_name = test.get("module")
    mod_file_name = os.path.splitext(file_name)[0]
    test_mod = importlib.import_module(mod_file_name)
    testcase_name = test.get("name", "Test Case Unknown")

    rc = test_mod.run(
        ceph_cluster=args["ceph_cluster"],
        ceph_nodes=args["ceph_nodes"],
        config=config,
        test_data=args["test_data"],
        ceph_cluster_dict=args["ceph_cluster_dict"],
        clients=args["clients"],
    )

    file_string = f"{testcase_name}"
    results.update({file_string: rc})
