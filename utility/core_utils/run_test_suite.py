import datetime
import os
import sys

import yaml
from gevent import monkey
from RhcsQeSdk.tests.ceph import test_ceph
from RhcsQeSdk.tests.misc import test_cluster_bootstrap
from RhcsQeSdk.tests.rbd import test_rbd, test_rbd_verify_status
from RhcsQeSdk.tests.rbd_mirror import test_rbd_mirror, test_rbdmirror_bootstrap

from utility.log import Log

monkey.patch_all()


from ceph.utils import cleanup_ceph_nodes, cleanup_ibmc_ceph_nodes
from utility.core_utils.parallel_executor import ParallelExecutor
from utility.polarion import post_to_polarion
from utility.utils import (
    collect_recipe,
    configure_logger,
    create_unique_test_name,
    store_cluster_state,
)

logger = Log()

SERVICE_MAP = dict(
    {
        "test_cluster_bootstrap.py": test_cluster_bootstrap,
        "test_ceph.py": test_ceph,
        "test_rbd.py": test_rbd,
        "test_rbd_mirror.py": test_rbd_mirror,
        "test_rbdmirror_bootstrap.py": test_rbdmirror_bootstrap,
        "test_rbd_verify_status.py": test_rbd_verify_status,
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
        self.parallel_executor = ParallelExecutor()
        self.jenkins_rc = 0

    def run_tests(self, tests):
        """
        This method is used to run list of tests present in a yaml file.
        Args:
          None
        Returns:
          None
        """
        self.parallel_executor.run_until_complete(self.run_tests_async, tests)
        return self.jenkins_rc

    def run_tests_async(self, tests):
        """
        This method is used to run list of tests present in a yaml file.
        Args:
          None
        Returns:
          None
        """
        logger.info("Running test suite")
        self.jenkins_rc = 0
        for test in tests:
            if test.get("parallel"):
                passed = True
                tasks = []
                for test_pll in test.get("parallel"):
                    tasks.append((self.run_test, test_pll))
                results = self.parallel_executor.run(tasks)
                for result in results:
                    if not result:
                        passed = False
                        break
                if not passed:
                    break
            if not self.run_test(test):
                break

    def run_test(self, test):
        rc = 0
        test = test.get("test")
        tc = self.fetch_test_details(test)
        report_portal_description = test.get("desc") or ""
        unique_test_name = create_unique_test_name(test.get("name"), self.test_names)
        self.test_names.append(unique_test_name)
        tc["log-link"] = configure_logger(unique_test_name, self.run_dir)
        test_data_file = test.get("test_data")
        module = test.get("module")
        obj = SERVICE_MAP[module]
        runs_on = test.get("runs_on", None)
        start = datetime.datetime.now()
        results = []
        if runs_on:
            tasks = []
            for cluster_name in runs_on:
                with open("test_configs/" + test_data_file) as test_data:
                    test_data = yaml.safe_load(test_data)
                logger.info(f"Running test {test_data_file} with {test_data}")
                kw = {
                    "test_data": test_data,
                    "cluster_name": cluster_name,
                    "osp_cred": self.osp_cred,
                    "ceph_cluster_dict": self.ceph_cluster_dict,
                }
                tasks.append((obj.run, kw))
            try:
                results = self.parallel_executor.run(tasks)
            except Exception:
                self.jenkins_rc = 1
                results = [{"cluster_names": runs_on, "rc": 1}]
        else:
            with open("test_configs/" + test_data_file) as test_data:
                test_data = yaml.safe_load(test_data)
            logger.info(f"Running test {test_data_file} with {test_data}")
            kw = {
                "test_data": test_data,
                "cluster_name": None,
                "osp_cred": self.osp_cred,
                "ceph_cluster_dict": self.ceph_cluster_dict,
            }
            results = obj.run(kw)
            results = [results]
        for out in results:
            cluster_names = out.get("cluster_names")
            rc = out.get("rc")
            try:
                if self.post_to_report_portal:
                    self.rp_logger.start_test_item(
                        name=unique_test_name,
                        description=report_portal_description,
                        item_type="STEP",
                    )
                    self.rp_logger.log(message=f"Logfile location - {tc['log-link']}")
                    self.rp_logger.log(message=f"Polarion ID: {tc['tcms-id']}")

                # Initialize the cluster with the expected rhcs_version hence the
                # precedence would be from test suite.
                # rhbuild would start with the version for example 5.0 or 4.2-rhel-7
                _rhcs_version = test.get("ceph_rhcs_version", self.rhbuild[:3])
                for cluster_name in cluster_names:
                    self.ceph_cluster_dict[cluster_name].rhcs_version = _rhcs_version
            except BaseException as be:  # noqa
                logger.exception(be)
                rc = 1
            finally:
                for cluster_name in cluster_names:
                    collect_recipe(self.ceph_cluster_dict[cluster_name])
                if self.store:
                    for cluster_name in cluster_names:
                        store_cluster_state(
                            self.ceph_cluster_dict, self.ceph_clusters_file
                        )

            if rc != 0:
                break

        elapsed = datetime.datetime.now() - start
        tc["duration"] = elapsed

        # Write to report portal
        if self.post_to_report_portal:
            self.rp_logger.finish_test_item(status="PASSED" if rc == 0 else "FAILED")

        if rc == 0:
            tc["status"] = "Pass"
            msg = "Test {} passed".format(unique_test_name)
            logger.info(msg)

            if self.post_results:
                post_to_polarion(tc=tc)
        else:
            tc["status"] = "Failed"
            msg = "Test {} failed".format(unique_test_name)
            logger.info(msg)
            self.jenkins_rc = 1

            if self.post_results:
                post_to_polarion(tc=tc)

            if test.get("abort-on-fail", False):
                logger.info("Aborting on test failure")
                self.tcs.append(tc)
                return False

        if test.get("destroy-cluster") is True:
            if self.cloud_type == "openstack":
                cleanup_ceph_nodes(self.osp_cred, self.instances_name)
            elif self.cloud_type == "ibmc":
                cleanup_ibmc_ceph_nodes(self.osp_cred, self.instances_name)

        self.tcs.append(tc)
        if tc["status"] == "Failed":
            return False
        return True
