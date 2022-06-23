import builtins
import datetime
import importlib
import logging
import os
import sys
from keyword import iskeyword

from gevent import monkey

from ceph.parallel import parallel
from run import collect_recipe, create_nodes, store_cluster_state
from utility.core_utils.execute_command import ExecuteCommandMixin
from utility.core_utils.loader import LoaderMixin
from utility.core_utils.parallel_executor import ParallelExecutor

monkey.patch_all()


from ceph.utils import cleanup_ceph_nodes, cleanup_ibmc_ceph_nodes
from utility.polarion import post_to_polarion
from utility.utils import configure_logger, create_unique_test_name

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(name)s:%(lineno)d - %(message)s"
)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.DEBUG)
logger.addHandler(stream_handler)

execute_command_mixin = ExecuteCommandMixin()
METHOD_MAP = dict(
    {
        "must_pass": execute_command_mixin.must_pass,
        "must_fail": execute_command_mixin.must_fail,
    }
)


class RunDetails:
    ceph_cluster_dict = None
    rp_logger = None
    test_names = None
    run_dir = None
    post_to_report_portal = None
    rhbuild = None
    ceph_clusters_file = None
    post_results = None
    tcs = None
    cloud_type = None
    osp_cred = None
    instances_name = None
    conf = (None,)
    inventory = (None,)
    run_id = (None,)
    service = (None,)
    enable_eus = None
    store = None
    fetch_test_details = None

    def __init__(
        self,
        ceph_cluster_dict,
        rp_logger,
        test_names,
        run_dir,
        post_to_report_portal,
        rhbuild,
        ceph_clusters_file,
        post_results,
        tcs,
        cloud_type,
        osp_cred,
        instances_name,
        conf,
        inventory,
        run_id,
        service,
        enable_eus,
        store,
        fetch_test_details,
    ):
        self.rp_logger = rp_logger
        self.test_names = test_names
        self.run_dir = run_dir
        self.tcs = tcs
        self.ceph_cluster_dict = ceph_cluster_dict
        self.instances_name = instances_name
        self.osp_cred = osp_cred
        self.cloud_type = cloud_type
        self.post_results = post_results
        self.rhbuild = rhbuild
        self.ceph_clusters_file = ceph_clusters_file
        self.post_to_report_portal = post_to_report_portal
        self.conf = conf
        self.inventory = inventory
        self.run_id = run_id
        self.service = service
        self.enable_eus = enable_eus
        self.store = store
        self.fetch_test_details = fetch_test_details


class RunTestSuite:
    """
    This module provides CLI interface to run the test suite.
    """

    def __init__(self, config, run_details) -> None:
        sys.path.append(os.path.abspath("utility/core_utils"))
        self.config = config
        self.rp_logger = run_details.rp_logger
        self.test_names = run_details.test_names
        self.run_dir = run_details.run_dir
        self.tcs = run_details.tcs
        self.ceph_cluster_dict = run_details.ceph_cluster_dict
        self.instances_name = run_details.instances_name
        self.osp_cred = run_details.osp_cred
        self.cloud_type = run_details.cloud_type
        self.post_results = run_details.post_results
        self.rhbuild = run_details.rhbuild
        self.ceph_clusters_file = run_details.ceph_clusters_file
        self.post_to_report_portal = run_details.post_to_report_portal
        self.conf = run_details.conf
        self.inventory = run_details.inventory
        self.run_id = run_details.run_id
        self.service = run_details.service
        self.enable_eus = run_details.enable_eus
        self.store = run_details.store
        self.fetch_test_details = run_details.fetch_test_details

        # self.cli_obj = CLI()
        self.suite_config = LoaderMixin().load_file(config, kind="suites")
        self.parallel_executor = ParallelExecutor()

    def redefine_service(self, method):
        """
        This method is used to rename the inbuilt method by concatenating underscore.
        Args:
          method(str): Takes inbuilt method name as input.

        Returns:
          method(str): Modified method name.

        Example: changes the inbuilt keyword 'import' to 'import_'.
        """
        if "-" in str(method):
            method = str(method).replace("-", "_")
        if self.check_for_builtins(method):
            method = method + "_"
        return method

    def check_for_builtins(self, service):
        """
        This method is used to check whether the service is a inbuilt function.
        If it is an inbuilt function it is modified.
        Args:
          service(str): name of the method

        Returns:
          None
        """
        if iskeyword(service) or service in dir(builtins):
            return True
        else:
            for key in builtins.__dict__.keys():
                if service in dir(builtins.__dict__[key]):
                    return True
            return False

    def get_must_key(self, test_config):
        """
        This method is used to check whether test has to pass or fail or raise an exception mandatorily.
        Args:
          test_config(Dict): dictionary consists of test configuration in key-value pair.
        Returns:
          must_present_key(key): a key [must_pass | must_fail ]
        """
        for must_present_key in METHOD_MAP.keys():
            if test_config.get(must_present_key):
                return must_present_key

    def run_tests(self, tests):
        """
        This method is used to run list of tests present in a yaml file.
        Args:
          None

        Returns:
          None
        """
        return self.parallel_executor.run_until_complete((self.run_tests_async, tests))

    async def run_tests_async(self, tests):
        """
        This method is used to run list of tests present in a yaml file.
        Args:
          None

        Returns:
          None
        """
        logger.info("Running test suite")
        logger.info(f"Running test {self.config}")
        jenkins_rc = 0
        for test in tests:
            if test.get("parallel"):
                with parallel() as p:
                    for test_pll in test.get("parallel"):
                        p.spawn(self.run_tests_async, test_pll)
            test = test.get("test")
            tc = self.fetch_test_details(test)
            report_portal_description = test.get("desc") or ""
            unique_test_name = create_unique_test_name(
                test.get("name"), self.test_names
            )
            self.test_names.append(unique_test_name)
            tc["log-link"] = configure_logger(unique_test_name, self.run_dir)
            test_data = test.get("test_data")
            module = test.get("module")
            module = importlib.import_module(module)
            tc = test
            tc["log-link"] = configure_logger(unique_test_name, self.run_dir)
            runs_on = test.get("runs_on", [None])
            logger.info(f"Running test {test_data}")
            start = datetime.datetime.now()
            if runs_on:
                with parallel() as p:
                    for cluster_name in runs_on:
                        p.spawn(module, test_data, cluster_name)
            for out in p:
                cluster_name = out.get("cluster_name")
                rc = out.get("rc")
                try:
                    if self.post_to_report_portal:
                        self.rp_logger.start_test_item(
                            name=unique_test_name,
                            description=report_portal_description,
                            item_type="STEP",
                        )
                        self.rp_logger.log(
                            message=f"Logfile location - {tc['log-link']}"
                        )
                        self.rp_logger.log(message=f"Polarion ID: {tc['polarion-id']}")

                    # Initialize the cluster with the expected rhcs_version hence the
                    # precedence would be from test suite.
                    # rhbuild would start with the version for example 5.0 or 4.2-rhel-7
                    _rhcs_version = test.get("ceph_rhcs_version", self.rhbuild[:3])
                    self.ceph_cluster_dict[cluster_name].rhcs_version = _rhcs_version
                except BaseException as be:  # noqa
                    logger.exception(be)
                    rc = 1
                finally:
                    collect_recipe(self.ceph_cluster_dict[cluster_name])
                    if self.store:
                        store_cluster_state(
                            self.ceph_cluster_dict, self.ceph_clusters_file
                        )

                if rc != 0:
                    break

            elapsed = datetime.datetime.now() - start
            tc["duration"] = elapsed

            # Write to report portal
            if self.post_to_report_portal:
                self.rp_logger.finish_test_item(
                    status="PASSED" if rc == 0 else "FAILED"
                )

            if rc == 0:
                tc["status"] = "Pass"
                msg = "Test {} passed".format(test_data)
                logger.info(msg)
                print(msg)

                if self.post_results:
                    post_to_polarion(tc=tc)
            else:
                tc["status"] = "Failed"
                msg = "Test {} failed".format(test_data)
                logger.info(msg)
                print(msg)
                jenkins_rc = 1

                if self.post_results:
                    post_to_polarion(tc=tc)

                if test.get("abort-on-fail", False):
                    logger.info("Aborting on test failure")
                    self.tcs.append(tc)
                    break

            if test.get("destroy-cluster") is True:
                if self.cloud_type == "openstack":
                    cleanup_ceph_nodes(self.osp_cred, self.instances_name)
                elif self.cloud_type == "ibmc":
                    cleanup_ibmc_ceph_nodes(self.osp_cred, self.instances_name)

            if test.get("recreate-cluster") is True:
                ceph_cluster_dict, clients = create_nodes(
                    self.conf,
                    self.inventory,
                    self.osp_cred,
                    self.run_id,
                    self.cloud_type,
                    self.service,
                    self.instances_name,
                    enable_eus=self.enable_eus,
                )
            self.tcs.append(tc)
        return jenkins_rc
