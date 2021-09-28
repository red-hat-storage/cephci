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

from ceph.parallel import parallel


def run(**kwargs):
    results = dict()

    parallel_tests = kwargs["parallel"]

    with parallel() as p:
        for test in parallel_tests:
            p.spawn(execute, test, kwargs, results)
    return 0


def execute(test, args, results):
    """
    Executes the test under parallel in module named 'Parallel run'  parallely.

    It involves the following steps
        - Importing of test module based on test
        - Running the test module

    Args:
        test: The test module which needs to be executed
        cluster: Ceph cluster participating in the test.
        config:  The key/value pairs passed by the tester.

    Returns:
        int: non-zero on failure, zero on pass
    """

    test = test.get("test")
    config = test.get("config")
    config.update(args["config"])
    file_name = test.get("module")

    mod_file_name = file_name.split(".")
    test_mod = importlib.import_module(mod_file_name[0])

    rc = test_mod.run(
        ceph_cluster=args["ceph_cluster"],
        ceph_nodes=args["ceph_nodes"],
        config=config,
        test_data=args["test_data"],
        ceph_cluster_dict=args["ceph_cluster_dict"],
        clients=args["clients"],
    )
    results.update(file_name=rc)
    return 0
