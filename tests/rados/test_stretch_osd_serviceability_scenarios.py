"""
This test module is used to test OSD & OSD host replacement scenarios with recovery in the stretch environment
includes:

1. OSD daemon removed
2. OSD daemon added.
3. OSD host removed.
4. OSD host added.
5. Heath warning generated on the Cluster upon uneven weights of data sites.

"""

import time
from collections import namedtuple

from ceph.ceph_admin import CephAdmin
from ceph.rados import utils
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from ceph.rados.serviceability_workflows import ServiceabilityMethods
from tests.rados.rados_test_util import get_device_path, wait_for_device_rados
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from tests.rados.test_stretch_site_down import (
    get_stretch_site_hosts,
    stretch_enabled_checks,
)
from utility.log import Log
from utility.utils import method_should_succeed, should_not_be_empty

log = Log(__name__)

Hosts = namedtuple("Hosts", ["dc_1_hosts", "dc_2_hosts", "tiebreaker_hosts"])


def run(ceph_cluster, **kw):
    """
    performs replacement scenarios in stretch mode
    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster
    """

    log.info(run.__doc__)
    config = kw.get("config")
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    service_obj = ServiceabilityMethods(cluster=ceph_cluster, **config)
    client_node = ceph_cluster.get_nodes(role="client")[0]
    rhbuild = config.get("rhbuild")
    pool_name = config.get("pool_name", "test_stretch_io")
    stretch_bucket = config.get("stretch_bucket", "datacenter")
    tiebreaker_mon_site_name = config.get("tiebreaker_mon_site_name", "arbiter")
    add_network_delay = config.get("add_network_delay", False)

    def check_stretch_health_warning():
        """
        Method to check if Warn "UNEVEN_WEIGHTS_STRETCH_MODE" is generated on cluster.
        Bug Fix : https://bugzilla.redhat.com/show_bug.cgi?id=2125107

        Args:
        Returns (bool) :
            True -> Warning present on the cluster
            False -> Warning not present on the cluster

        """
        log.info(
            "Checking if health warning for different site weights is Present on cluster"
        )
        status_report = rados_obj.run_ceph_command(cmd="ceph report", client_exec=True)
        ceph_health_status = list(status_report["health"]["checks"].keys())
        log.debug(f"Ceph report: \n\n {status_report} \n\n")
        expected_warn = "UNEVEN_WEIGHTS_STRETCH_MODE"
        if expected_warn in ceph_health_status:
            log.info("warning  present on the cluster")
            log.info(f"Warnings generated on the cluster : {ceph_health_status}")
            return True
        log.info("Warning not present on the cluster")
        return False

    try:
        if not stretch_enabled_checks(rados_obj=rados_obj):
            log.error(
                "The cluster has not cleared the pre-checks to run stretch tests. Exiting..."
            )
            raise Exception("Test pre-execution checks failed")

        log.info(
            "Starting tests performing replacement of OSDs and Hosts in Stretch mode Pre-checks Passed"
        )
        osd_tree_cmd = "ceph osd tree"
        buckets = rados_obj.run_ceph_command(osd_tree_cmd)
        dc_buckets = [d for d in buckets["nodes"] if d.get("type") == stretch_bucket]
        dc_1 = dc_buckets.pop()
        dc_1_name = dc_1["name"]
        dc_2 = dc_buckets.pop()
        dc_2_name = dc_2["name"]
        all_hosts = get_stretch_site_hosts(
            rados_obj=rados_obj, tiebreaker_mon_site_name=tiebreaker_mon_site_name
        )
        dc_1_hosts = all_hosts.dc_1_hosts
        dc_2_hosts = all_hosts.dc_2_hosts
        tiebreaker_hosts = all_hosts.tiebreaker_hosts

        log.debug(f"Hosts present in {stretch_bucket} : {dc_1_name} : {dc_1_hosts}")
        log.debug(f"Hosts present in {stretch_bucket} : {dc_2_name} : {dc_2_hosts}")
        log.debug(
            f"Hosts present in {stretch_bucket} : {tiebreaker_mon_site_name} : {tiebreaker_hosts}"
        )
        if add_network_delay:
            for host in dc_1_hosts:
                rados_obj.add_network_delay_on_host(
                    hostname=host, delay="10ms", set_delay=True
                )
            for host in dc_2_hosts:
                rados_obj.add_network_delay_on_host(
                    hostname=host, delay="10ms", set_delay=True
                )
            for host in tiebreaker_hosts:
                rados_obj.add_network_delay_on_host(
                    hostname=host, delay="100ms", set_delay=True
                )

            log.info("Successfully set network delays on the Stretch cluster nodes")

        # Creating test pool to check the effect of Site down on the Pool IO
        if not rados_obj.create_pool(pool_name=pool_name):
            log.error(f"Failed to create pool : {pool_name}")
            raise Exception("Test execution failed")

        # Sleeping for 10 seconds for pool to be populated in the cluster
        time.sleep(10)

        # Collecting the init no of objects on the pool, before site down
        pool_stat = rados_obj.get_cephdf_stats(pool_name=pool_name)
        init_objects = pool_stat["stats"]["objects"]
        inactive_pgs = 0

        # Increasing the recovery threads on the cluster
        rados_obj.change_recovery_threads(config={}, action="set")

        # Proceeding to remove one OSD from any of the data sites
        target_osd = rados_obj.get_pg_acting_set(pool_name=pool_name)[0]
        log.debug(
            f"Proceeding to remove one OSD from any of the data sites. OSD selected at random : {target_osd}"
        )
        log.debug(
            f"Ceph osd tree before OSD removal : \n\n {rados_obj.run_ceph_command(cmd='ceph osd tree')} \n\n"
        )
        test_host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=target_osd)
        should_not_be_empty(test_host, "Failed to fetch host details")
        dev_path = get_device_path(test_host, target_osd)
        log.debug(
            f"osd device path  : {dev_path}, osd_id : {target_osd}, hostname : {test_host.hostname}"
        )
        utils.set_osd_devices_unmanaged(ceph_cluster, target_osd, unmanaged=True)
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

        # Checking if the expected health warning about the different site weights are seen.
        if float(rhbuild.split("-")[0]) >= 7.1:
            if not check_stretch_health_warning():
                log.error("Warnings not generated on the cluster")
                raise Exception("Warning not present on cluster")

        # Checking cluster health after OSD removal
        method_should_succeed(rados_obj.run_pool_sanity_check)
        if not rados_obj.check_inactive_pgs_on_pool(pool_name=pool_name):
            log.error(f"Inactive PGs found on pool : {pool_name}")
            inactive_pgs += 1

        log.debug(
            f"Ceph osd tree after OSD removal : \n\n {rados_obj.run_ceph_command(cmd='ceph osd tree')} \n\n"
        )
        log.info(
            f"Removal of OSD : {target_osd} is successful. Proceeding to add back the OSD daemon."
        )

        # Adding the removed OSD back and checking the cluster status
        log.debug("Adding the removed OSD back and checking the cluster status")
        utils.add_osd(ceph_cluster, test_host.hostname, dev_path, target_osd)
        method_should_succeed(
            wait_for_device_rados, test_host, target_osd, action="add"
        )
        time.sleep(30)
        log.debug(
            "Completed addition of OSD post removal. Checking for inactive PGs post OSD addition"
        )
        if not rados_obj.check_inactive_pgs_on_pool(pool_name=pool_name):
            log.error(f"Inactive PGs found on pool : {pool_name}")
            inactive_pgs += 1

        if inactive_pgs > 10:
            log.error("Found inactive PGs on the cluster during OSD removal/Addition")
            raise Exception("Inactive PGs during stop error")

        # Checking if the expected health warning about the different site weights is removed post addition of OSD.
        if float(rhbuild.split("-")[0]) >= 7.1:
            if check_stretch_health_warning():
                log.error("Warnings not removed on the cluster post osd addition")
                raise Exception("Warning present on cluster")

        # Checking cluster health after OSD Addition
        method_should_succeed(rados_obj.run_pool_sanity_check)
        log.info(
            f"Addition of OSD : {target_osd} back into the cluster was successful, and the health is good!"
        )
        utils.set_osd_devices_unmanaged(ceph_cluster, target_osd, unmanaged=False)
        log.info("Completed the removal and addition of OSD daemons")

        # perform rados put to check if write ops is possible
        pool_obj.do_rados_put(client=client_node, pool=pool_name, nobj=200, timeout=50)

        log.debug("sleeping for 20 seconds for the objects to be displayed in ceph df")
        time.sleep(20)

        # Getting the number of objects post write, to check if writes were successful
        pool_stat = rados_obj.get_cephdf_stats(pool_name=pool_name)
        log.debug(pool_stat)
        post_osd_rm_objs = pool_stat["stats"]["objects"]

        # Objects should be more than the initial no of objects
        if post_osd_rm_objs <= init_objects:
            log.error(
                "Write ops should be possible, number of objects in the pool has not changed"
            )
            raise Exception(
                f"Pool {pool_name} has {pool_stat['stats']['objects']} objs"
            )
        log.info(
            f"Successfully wrote {pool_stat['stats']['objects']} on pool {pool_name} in degraded mode\n"
            f"Proceeding to bring up the nodes and recover the cluster from degraded mode"
        )
        log.info("Completed OSD daemon replacement scenarios")

        log.info("Proceeding to do OSD host replacement in Stretch mode")
        test_host_site = dc_1_name if test_host.hostname in dc_1_hosts else dc_2_name
        log.info(
            f"OSD host to be removed : {test_host.hostname} from site {test_host_site}"
        )

        log.debug(
            f"Ceph osd tree before host removal : \n\n {rados_obj.run_ceph_command(cmd='ceph osd tree')} \n\n"
        )
        try:
            service_obj.remove_custom_host(host_node_name=test_host.hostname)
        except Exception as err:
            log.error(f"Could not remove host : {test_host.hostname}. Error : {err}")
            raise Exception("Host not removed error")
        time.sleep(10)

        # Waiting for recovery to post OSD host removal
        if not rados_obj.check_inactive_pgs_on_pool(pool_name=pool_name):
            log.error(f"Inactive PGs found on pool : {pool_name}")
            inactive_pgs += 1
        method_should_succeed(wait_for_clean_pg_sets, rados_obj, timeout=12000)
        method_should_succeed(rados_obj.run_pool_sanity_check)
        if inactive_pgs > 10:
            log.error("Found inactive PGs on the cluster during OSD removal/Addition")
            raise Exception("Inactive PGs during stop error")

        # Checking if the expected health warning about the different site weights is displayed post removal of OSD host
        if float(rhbuild.split("-")[0]) >= 7.1:
            if not check_stretch_health_warning():
                log.error(
                    "Warnings is not displayed on the cluster post osd host removal"
                )
                raise Exception("Warning not present on cluster")

        log.debug(
            f"Ceph osd tree after host removal : \n\n {rados_obj.run_ceph_command(cmd='ceph osd tree')} \n\n"
        )
        # Adding a new host to the cluster
        try:
            service_obj.add_new_hosts(
                add_nodes=[test_host.hostname],
                crush_bucket_name=test_host.hostname,
                crush_bucket_type=stretch_bucket,
                crush_bucket_val=test_host_site,
            )
        except Exception as err:
            log.error(
                f"Could not add host : {test_host.hostname} into the cluster and deploy OSDs. Error : {err}"
            )
            raise Exception("Host not added error")
        time.sleep(60)

        # Waiting for recovery to post OSD host Addition
        if not rados_obj.check_inactive_pgs_on_pool(pool_name=pool_name):
            log.error(f"Inactive PGs found on pool : {pool_name}")
            inactive_pgs += 1
        method_should_succeed(wait_for_clean_pg_sets, rados_obj, timeout=12000)
        method_should_succeed(rados_obj.run_pool_sanity_check)
        if inactive_pgs > 10:
            log.error("Found inactive PGs on the cluster during OSD removal/Addition")
            raise Exception("Inactive PGs during stop error")

        # Checking if the health warning about the different site weights is removed post addition of OSD host
        if float(rhbuild.split("-")[0]) >= 7.1:
            if check_stretch_health_warning():
                log.error(
                    "Warnings is not removed on the cluster post osd host addition"
                )
                raise Exception("Warning present on cluster")

        # perform rados put to check if write ops is possible
        pool_obj.do_rados_put(client=client_node, pool=pool_name, nobj=200, timeout=50)

        log.debug("sleeping for 20 seconds for the objects to be displayed in ceph df")
        time.sleep(20)

        log.info("Completed OSD Host replacement scenarios")
        log.info("Successfully removed and added hosts on stretch mode cluster")

    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1

    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        if add_network_delay:
            for host in dc_1_hosts:
                rados_obj.add_network_delay_on_host(hostname=host, set_delay=False)
            for host in dc_2_hosts:
                rados_obj.add_network_delay_on_host(hostname=host, set_delay=False)
            for host in tiebreaker_hosts:
                rados_obj.add_network_delay_on_host(hostname=host, set_delay=False)

            log.info(
                "Successfully removed the network delays added on the Stretch cluster nodes"
            )
        # restoring the recovery threads on the cluster
        rados_obj.change_recovery_threads(config={}, action="rm")

        if config.get("delete_pool"):
            rados_obj.delete_pool(pool=pool_name)
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info("All the tests completed on the cluster, Pass!!!")
    return 0
