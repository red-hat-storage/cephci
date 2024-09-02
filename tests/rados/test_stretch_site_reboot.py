"""
This test module is used to test site reboot scenarios with recovery in the stretch environment.
includes:
CEPH-83574977 - Reboot all the nodes of Site.
1. Perform Data Site reboots
2. Perform Arbiter Site hosts reboots
"""

import re
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from tests.rados.test_stretch_site_down import (
    get_stretch_site_hosts,
    post_site_down_checks,
    stretch_enabled_checks,
)
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    performs site reboot scenarios in stretch mode
    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster
    """

    log.info(run.__doc__)
    config = kw.get("config")
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    client_node = ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "test_stretch_io")
    affected_site = config.get("affected_site", "DC1")
    tiebreaker_mon_site_name = config.get("tiebreaker_mon_site_name", "arbiter")

    try:
        if not stretch_enabled_checks(rados_obj=rados_obj):
            log.error(
                "The cluster has not cleared the pre-checks to run stretch tests. Exiting..."
            )
            raise Exception("Test pre-execution checks failed")

        log.info(
            f"Starting with tests to reboot {affected_site} hosts. Pre-checks Passed"
        )
        osd_tree_cmd = "ceph osd tree"
        buckets = rados_obj.run_ceph_command(osd_tree_cmd)
        dc_buckets = [d for d in buckets["nodes"] if d.get("type") == "datacenter"]
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

        log.debug(
            f"Hosts present in Datacenter : {dc_1_name} : {[entry for entry in dc_1_hosts]}"
        )
        log.debug(
            f"Hosts present in Datacenter : {dc_2_name} : {[entry for entry in dc_2_hosts]}"
        )
        log.debug(
            f"Hosts present in Datacenter : {tiebreaker_mon_site_name} : {[entry for entry in tiebreaker_hosts]}"
        )

        # Checking if the site passed to reboot is present in the Cluster CRUSH
        if affected_site not in [tiebreaker_mon_site_name, dc_1_name, dc_2_name]:
            log.error(
                f"Passed site : {affected_site} not part of crush locations on cluster.\n"
                f"locations present on cluster : {[tiebreaker_mon_site_name, dc_1_name, dc_2_name]}"
            )
            raise Exception("Test execution failed")

        # Creating test pool to check the site reboots on the Pool IO
        if not rados_obj.create_pool(pool_name=pool_name):
            log.error(f"Failed to create pool : {pool_name}")
            raise Exception("Test execution failed")

        # Sleeping for 10 seconds for pool to be populated in the cluster
        time.sleep(10)

        # Collecting the init no of objects on the pool, before reboots
        pool_stat = rados_obj.get_cephdf_stats(pool_name=pool_name)
        init_objects = pool_stat["stats"]["objects"]

        # Checking which DC to be rebooted
        if affected_site in [dc_1_name, dc_2_name]:
            log.debug(f"Proceeding to reboot hosts of the data site {dc_1_name}")
            # Proceeding to reboot hosts
            for host in dc_1_hosts:
                log.debug(f"Proceeding to reboot host : {host}")
                host_obj = get_host_obj_from_hostname(
                    hostname=host, rados_obj=rados_obj
                )
                host_obj.exec_command(cmd="reboot", sudo=True, check_ec=False)
                time.sleep(2)

            log.info(f"Completed reboot of all the hosts in data site {dc_1_name}")

            # sleeping for 5 seconds for the hosts to reboot and proceeding to next checks
            time.sleep(5)

            # Checking the health status of the cluster and the active alerts
            # These should be generated on the cluster
            status_report = rados_obj.run_ceph_command(
                cmd="ceph report", client_exec=True
            )
            ceph_health_status = list(status_report["health"]["checks"].keys())
            expected_health_warns = (
                "OSD_HOST_DOWN",
                "OSD_DOWN",
                "MON_DOWN",
            )
            if not all(elem in ceph_health_status for elem in expected_health_warns):
                log.error(
                    f"We do not have the expected health warnings generated on the cluster.\n"
                    f" Warns on cluster : {ceph_health_status}\n"
                    f"Expected Warnings : {expected_health_warns}\n"
                )

            log.info(
                f"The expected health warnings are generated on the cluster. Warnings on cluster: {ceph_health_status}"
            )

            log.info("Proceeding to try writes into cluster post reboots")

        else:
            log.info("Rebooting host of arbiter mon site")
            for host in tiebreaker_hosts:
                log.debug(f"Proceeding to reboot host : {host}")
                host_obj = get_host_obj_from_hostname(
                    hostname=host, rados_obj=rados_obj
                )
                host_obj.exec_command(cmd="reboot", sudo=True, check_ec=False)
                time.sleep(2)

            # Installer node will be in maintenance mode this point. all operations need to be done at client nodes
            status_report = rados_obj.run_ceph_command(
                cmd="ceph report", client_exec=True
            )
            ceph_health_status = list(status_report["health"]["checks"].keys())
            expected_health_warns = ("MON_DOWN",)
            if not all(elem in ceph_health_status for elem in expected_health_warns):
                log.error(
                    f"We do not have the expected health warnings generated on the cluster.\n"
                    f" Warns on cluster : {ceph_health_status}\n"
                    f"Expected Warnings : {expected_health_warns}\n"
                )

            log.info(
                f"The expected health warnings are generated on the cluster. Warnings : {ceph_health_status}"
            )
            log.info(
                f"Completed addition of hosts into maintenance mode in site {affected_site}."
                f" Host names :{tiebreaker_hosts}. Proceeding to write"
            )

        # perform rados put to check if write ops is possible
        pool_obj.do_rados_put(client=client_node, pool=pool_name, nobj=200, timeout=50)

        log.debug("sleeping for 20 seconds for the objects to be displayed in ceph df")
        time.sleep(20)

        # Getting the number of objects post write, to check if writes were successful
        pool_stat = rados_obj.get_cephdf_stats(pool_name=pool_name)
        log.debug(pool_stat)

        # Objects should be more than the initial no of objects
        if pool_stat["stats"]["objects"] <= init_objects:
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

        log.info("Proceeding to do checks post Stretch mode site reboot scenarios")

    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1

    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        if not post_site_down_checks(rados_obj=rados_obj):
            log.error(f"Checks failed post Site {affected_site} Down and Up scenarios")
            raise Exception("Post execution checks failed on the Stretch cluster")

        if not rados_obj.run_pool_sanity_check():
            log.error(f"Checks failed post Site {affected_site} Down and Up scenarios")
            raise Exception("Post execution checks failed on the Stretch cluster")

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


def get_host_obj_from_hostname(hostname, rados_obj):
    """
    returns the host object for the hostname passed

    Args:
        hostname: name of the host whose host obj is required
        rados_obj: rados object to perform operations

    Returns:
        Host object for the hostname passed

    """
    host_nodes = rados_obj.ceph_cluster.get_nodes()
    for node in host_nodes:
        if (
            re.search(hostname.lower(), node.hostname.lower())
            or re.search(hostname.lower(), node.vmname.lower())
            or re.search(hostname.lower(), node.shortname.lower())
        ):
            return node
