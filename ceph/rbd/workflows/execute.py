import importlib


def execute(mod_file_name, args, test_name, results={}, raise_exception=False):
    """
    Executes the test specified and updates return
    value to results dict.

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
    try:
        test_mod = importlib.import_module(mod_file_name)

        rc = test_mod.run(
            ceph_cluster=args["ceph_cluster"],
            ceph_nodes=args["ceph_nodes"],
            config=args["config"],
            test_data=args["test_data"],
            ceph_cluster_dict=args["ceph_cluster_dict"],
            clients=args["clients"],
        )
        if rc and raise_exception:
            raise Exception(f"{mod_file_name} execution failed")
        results.update({test_name: rc})
    except Exception as e:
        if raise_exception:
            raise Exception(e)
        results.update({test_name: 1})
