"""
Module to test bluestore_min_alloc_size functionality on RHCS 7.0 and above clusters

"""

import re
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados import utils
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.rados_test_util import get_device_path, wait_for_device_rados
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log
from utility.utils import method_should_succeed, should_not_be_empty

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Module to test bluestore_min_alloc_size functionality on RHCS 7.0 and above clusters
    Bugzilla automated : 2264726
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    custom_min_alloc_size = config.get("custom_min_alloc_size", 8192)
    default_min_alloc_size = config.get("default_min_alloc_size", 4096)

    regex = r"\s*(\d.\d)-rhel-\d"
    build = (re.search(regex, config.get("build", config.get("rhbuild")))).groups()[0]
    if not float(build) >= 7.0:
        log.info(
            "Test running on version less than 7.0, skipping verifying Reads Balancer functionality"
        )
        return 0

    try:
        if config.get("pre_deployment_config_changes"):
            log.info(
                "Updating the configs on the cluster before OSD deployment on the cluster"
            )
            min_alloc_size_hdd = int(
                mon_obj.get_config(section="osd", param="bluestore_min_alloc_size_hdd")
            )

            min_alloc_size_ssd = int(
                mon_obj.get_config(section="osd", param="bluestore_min_alloc_size_ssd")
            )

            if not min_alloc_size_hdd == min_alloc_size_ssd == 4096:
                log.error(
                    f"min_alloc_size does not match the expected default value of 4096"
                    f"min_alloc_size_ssd on cluster: {min_alloc_size_ssd}"
                    f"min_alloc_size_hdd on cluster: {min_alloc_size_hdd}"
                )
                raise Exception("non-default values for min_alloc_size on cluster")

            log.info(
                "Verified the default value of min_alloc_size. Modifying the value before OSD deployments"
            )

            mon_obj.set_config(
                section="osd",
                name="bluestore_min_alloc_size_hdd",
                value=custom_min_alloc_size,
            )
            mon_obj.set_config(
                section="osd",
                name="bluestore_min_alloc_size_ssd",
                value=custom_min_alloc_size,
            )
            time.sleep(10)

            min_alloc_size_hdd = int(
                mon_obj.get_config(section="osd", param="bluestore_min_alloc_size_hdd")
            )

            min_alloc_size_ssd = int(
                mon_obj.get_config(section="osd", param="bluestore_min_alloc_size_ssd")
            )
            if not min_alloc_size_hdd == min_alloc_size_ssd == custom_min_alloc_size:
                log.error(
                    f"min_alloc_size does not match the expected custom value of {custom_min_alloc_size}"
                    f"min_alloc_size_ssd on cluster: {min_alloc_size_ssd}"
                    f"min_alloc_size_hdd on cluster: {min_alloc_size_hdd}"
                )
                raise Exception("Value not updated for min_alloc_size on cluster")

            log.info("Successfully modified the value of min_alloc_size")
            return 0

        if config.get("post_deployment_config_verification"):
            min_alloc_size_hdd = int(
                mon_obj.get_config(section="osd", param="bluestore_min_alloc_size_hdd")
            )

            min_alloc_size_ssd = int(
                mon_obj.get_config(section="osd", param="bluestore_min_alloc_size_ssd")
            )

            # Getting random OSDs and checking their alloc size
            pg_set = rados_obj.get_pg_acting_set()
            for osd_id in pg_set:
                osd_meta = rados_obj.get_daemon_metadata(
                    daemon_type="osd", daemon_id=osd_id
                )

                if (
                    not min_alloc_size_hdd
                    == min_alloc_size_ssd
                    == custom_min_alloc_size
                    == int(osd_meta["bluestore_min_alloc_size"])
                ):
                    log.error(
                        f"min_alloc_size does not match the expected updated value of {custom_min_alloc_size}\n"
                        f"min_alloc_size_ssd on cluster: {min_alloc_size_ssd}\n"
                        f"min_alloc_size_hdd on cluster: {min_alloc_size_hdd}\n"
                        f"min_alloc_size on osd {osd_id} metadata: {osd_meta['bluestore_min_alloc_size']}\n"
                    )
                    raise Exception(
                        "default values for min_alloc_size on cluster post changing"
                    )

                log.info(
                    f"OSDs successfully deployed with the new alloc size, and verified the size on OSD: {osd_id}"
                )

            mon_obj.set_config(
                section="osd",
                name="bluestore_min_alloc_size_hdd",
                value=default_min_alloc_size,
            )
            mon_obj.set_config(
                section="osd",
                name="bluestore_min_alloc_size_ssd",
                value=default_min_alloc_size,
            )
            time.sleep(10)

            def remove_osd_check_metadata(target_osd, alloc_size):
                log.debug(
                    f"Ceph osd tree before OSD removal : \n\n {rados_obj.run_ceph_command(cmd='ceph osd tree')} \n\n"
                )
                test_host = rados_obj.fetch_host_node(
                    daemon_type="osd", daemon_id=target_osd
                )
                should_not_be_empty(test_host, "Failed to fetch host details")
                dev_path = get_device_path(test_host, target_osd)
                log.debug(
                    f"osd device path  : {dev_path}, osd_id : {target_osd}, hostname : {test_host.hostname}"
                )
                utils.set_osd_devices_unmanaged(
                    ceph_cluster, target_osd, unmanaged=True
                )
                method_should_succeed(utils.set_osd_out, ceph_cluster, target_osd)
                method_should_succeed(wait_for_clean_pg_sets, rados_obj, timeout=12000)
                log.debug("Cluster clean post draining of OSD for removal")
                utils.osd_remove(ceph_cluster, target_osd)
                method_should_succeed(wait_for_clean_pg_sets, rados_obj, timeout=12000)
                method_should_succeed(
                    utils.zap_device, ceph_cluster, test_host.hostname, dev_path
                )
                method_should_succeed(
                    wait_for_device_rados, test_host, target_osd, action="remove"
                )
                # Waiting for recovery to post OSD host removal
                method_should_succeed(wait_for_clean_pg_sets, rados_obj, timeout=12000)

                # Adding the removed OSD back and checking the cluster status
                log.debug("Adding the removed OSD back and checking the cluster status")
                utils.add_osd(ceph_cluster, test_host.hostname, dev_path, target_osd)
                method_should_succeed(
                    wait_for_device_rados, test_host, target_osd, action="add"
                )
                time.sleep(30)
                log.debug(
                    "Completed addition of OSD post removal. Checking bluestore_min_alloc_size value post OSD addition"
                )
                osd_meta = rados_obj.get_daemon_metadata(
                    daemon_type="osd", daemon_id=target_osd
                )
                if int(osd_meta["bluestore_min_alloc_size"]) != alloc_size:
                    log.error(
                        f"bluestore_min_alloc_size not set to {alloc_size} "
                        f"after updating the config and OSD : {target_osd} redeployment"
                        f"OSD Metadata : {osd_meta}"
                    )
                    return False

                log.info(
                    "bluestore_min_alloc_size correctly displayed "
                    "in OSD metadata post modification and redeployment"
                )
                return True

            rm_osd = pg_set[0]
            log.debug(f"Selected OSD for removal : {rm_osd}")
            try:
                if not remove_osd_check_metadata(
                    target_osd=rm_osd, alloc_size=default_min_alloc_size
                ):
                    log.error(
                        f"OSD : {rm_osd} could not be redeployed with alloc size {default_min_alloc_size}"
                    )
                    return 1
                log.info(
                    f"OSD : {rm_osd} successfully redeployed with alloc size {default_min_alloc_size}"
                )
            except Exception as err:
                log.error(f"Hit Exception during OSD redeployment : {err}")
                return 1
            log.info(
                f"Redeploying OSD : {rm_osd} with alloc size {custom_min_alloc_size}, So that the cluster alloc_size is"
                f"Homogeneous among all OSDs"
            )
            mon_obj.set_config(
                section="osd",
                name="bluestore_min_alloc_size_hdd",
                value=custom_min_alloc_size,
            )
            mon_obj.set_config(
                section="osd",
                name="bluestore_min_alloc_size_ssd",
                value=custom_min_alloc_size,
            )
            try:
                if not remove_osd_check_metadata(
                    target_osd=rm_osd, alloc_size=custom_min_alloc_size
                ):
                    log.error(
                        f"OSD : {rm_osd} could not be redeployed with alloc size {custom_min_alloc_size}"
                    )
                    return 1
                log.info(
                    f"OSD : {rm_osd} successfully redeployed with alloc size {custom_min_alloc_size}"
                )
            except Exception as err:
                log.error(f"Hit Exception during OSD redeployment : {err}")
                return 1
            log.info("All tests completed. Pass")
            return 0
    except Exception as err:
        log.error(f"Hit exception during execution. Error: {err} ")

    finally:
        rados_obj.rados_pool_cleanup()
        time.sleep(60)
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
