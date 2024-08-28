"""
This test module is used to test site maintenance mode scenarios with recovery in the stretch environment.
includes:
CEPH-83574976 - Data Site and Arbiter site Nodes enter maintenance mode.
1. Data Sites enter maintenance mode
2. Arbiter Site hosts enter maintenance mode
"""

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
    performs site maintenance mode scenarios in stretch mode
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
            f"Starting tests of Moving {affected_site} hosts to maintenance modes. Pre-checks Passed"
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

        # Checking if the site passed to add into maintenance mode is present in the Cluster CRUSH
        if affected_site not in [tiebreaker_mon_site_name, dc_1_name, dc_2_name]:
            log.error(
                f"Passed site : {affected_site} not part of crush locations on cluster.\n"
                f"locations present on cluster : {[tiebreaker_mon_site_name, dc_1_name, dc_2_name]}"
            )
            raise Exception("Test execution failed")

        # Creating test pool to check the effect of maintenance mode on the Pool IO
        if not rados_obj.create_pool(pool_name=pool_name):
            log.error(f"Failed to create pool : {pool_name}")
            raise Exception("Test execution failed")

        # Sleeping for 10 seconds for pool to be populated in the cluster
        time.sleep(10)

        # Collecting the init no of objects on the pool, before maintenance mode
        pool_stat = rados_obj.get_cephdf_stats(pool_name=pool_name)
        init_objects = pool_stat["stats"]["objects"]
        log.debug(f"pool stats before site down : {pool_stat}")

        # Checking which DC to be added to maintenance mode is It data site or Arbiter site
        if affected_site in [dc_1_name, dc_2_name]:
            log.debug(
                f"Proceeding to add hosts of the data site {dc_2_name} into maintenance mode"
            )
            # Proceeding to add hosts into maintenance mode, adding site 1
            # Getting the name of the active mgr instance host for failover
            # Checking for every host as a site might have more than 1 mgr instance,
            # and the mgr can failover in same site
            for host in dc_2_hosts:
                log.debug(f"Proceeding to add host : {host} into maintenance mode")
                mgr_dump = "ceph mgr dump"
                active_mgr = rados_obj.run_ceph_command(cmd=mgr_dump, client_exec=True)
                mgr_host = active_mgr["active_name"]
                if host in mgr_host:
                    log.info(
                        f"Active Mgr : {mgr_host} is running on host. Preparing to fail over"
                    )
                    cmd = "ceph mgr fail"
                    rados_obj.run_ceph_command(cmd=cmd, client_exec=True)
                    # sleeping for 10 seconds post mgr fail
                    time.sleep(10)

                # Moving host into maintenance mode
                time.sleep(10)
                if not rados_obj.host_maintenance_enter(
                    hostname=host, retry=25, yes_i_really_mean_it=True
                ):
                    log.error(f"Failed to add host : {host} into maintenance mode")
                    raise Exception("Test execution Failed")
                log.debug(
                    f"Host {host} added to maintenance mode, sleeping for 40 seconds before proceeding to next node"
                )
                time.sleep(40)
            log.info(
                f"Completed addition of all the hosts in data site {dc_1_name} into maintenance mode"
            )

            # sleeping for 240 seconds for the DC to be identified as in maintenance mode and proceeding to next checks
            time.sleep(240)

            # Checking the health status of the cluster and the active alerts for maintenance mode
            # These should be generated on the cluster
            status_report = rados_obj.run_ceph_command(
                cmd="ceph report", client_exec=True
            )
            ceph_health_status = list(status_report["health"]["checks"].keys())
            expected_health_warns = (
                "OSD_HOST_DOWN",
                "OSD_DOWN",
                "OSD_FLAGS",
                "OSD_DATACENTER_DOWN",
                "MON_DOWN",
                "DEGRADED_STRETCH_MODE",
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

            # Checking is the cluster is marked degraed and operating in degraded mode post data site down
            stretch_details = rados_obj.get_stretch_mode_dump()
            if not stretch_details["degraded_stretch_mode"]:
                log.error(
                    f"Stretch Cluster is not marked as degraded even though we have DC down : {stretch_details}"
                )
                raise Exception(
                    "Stretch mode degraded test Failed on the provided cluster"
                )

            log.info(
                f"Cluster is marked degraded post DC Failure {stretch_details},"
                f"Proceeding to try writes into cluster"
            )

        else:
            log.info("Moving host of arbiter mon site into maintenance mode")
            for host in tiebreaker_hosts:
                mgr_dump = "ceph mgr dump"
                active_mgr = rados_obj.run_ceph_command(cmd=mgr_dump, client_exec=True)
                mgr_host = active_mgr["active_name"]
                if host in mgr_host:
                    log.info(
                        f"Active Mgr : {mgr_host} is running on host. Preparing to fail over"
                    )
                    cmd = "ceph mgr fail"
                    rados_obj.run_ceph_command(cmd=cmd, client_exec=True)
                    # sleeping for 10 seconds post mgr fail
                    time.sleep(10)

                log.debug(f"Proceeding to add host : {host} into maintenance mode")
                if not rados_obj.host_maintenance_enter(
                    hostname=host, retry=10, yes_i_really_mean_it=True
                ):
                    log.error(f"Failed to add host : {host} into maintenance mode")
                    raise Exception("Test execution Failed")

            # Sleeping for 4 minutes. Observed that sometimes warning gets generated post 2 minutes.
            time.sleep(240)

            # Installer node will be in maintenance mode this point. all operations need to be done at client nodes
            status_report = rados_obj.run_ceph_command(
                cmd="ceph report", client_exec=True
            )
            ceph_health_status = list(status_report["health"]["checks"].keys())
            expected_health_warns = ("MON_DOWN", "HOST_IN_MAINTENANCE")
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

        # sleeping for 20 seconds for the services to be logically marked down in stretch mode
        time.sleep(20)
        # perform rados put to check if write ops is possible
        pool_obj.do_rados_put(client=client_node, pool=pool_name, nobj=200, timeout=50)

        log.debug("sleeping for 20 seconds for the objects to be displayed in ceph df")
        time.sleep(40)

        # Getting the number of objects post write, to check if writes were successful
        pool_stat = rados_obj.get_cephdf_stats(pool_name=pool_name)
        log.debug(f"pool stats post IOs in site down : {pool_stat}")

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

        # Starting to remove the hosts from maintenance mode.
        if affected_site in [dc_1_name, dc_2_name]:
            log.debug(
                f"Proceeding to remove hosts data site {dc_1_name} from maintenance mode"
            )
            for host in dc_2_hosts:
                if not rados_obj.host_maintenance_exit(hostname=host, retry=15):
                    log.error(f"Failed to remove host : {host} from maintenance mode")
                    raise Exception("Test execution Failed")
            log.info(
                f"Completed removing all the hosts in site {dc_2_name} from maintenance mode. Host names : {dc_1_hosts}"
            )

        else:
            log.info("removing arbiter mon site host from maintenance mode")
            for host in tiebreaker_hosts:
                if not rados_obj.host_maintenance_exit(hostname=host, retry=15):
                    log.error(f"Failed to remove host : {host} from maintenance mode")
                    raise Exception("Test execution Failed")
            log.info(
                f"Completed removal of all the hosts in site {affected_site} from maintainence mode."
                f" Host names : {tiebreaker_hosts}"
            )
            time.sleep(120)
            # Checking the active alerts for maintenance mode. They should not be present.
            status_report = rados_obj.run_ceph_command(
                cmd="ceph report", client_exec=True
            )
            ceph_health_status = list(status_report["health"]["checks"].keys())
            expected_health_warns = (
                "HOST_IN_MAINTENANCE",
                "OSD_DATACENTER_DOWN",
            )
            if any(elem in ceph_health_status for elem in expected_health_warns):
                log.error(
                    f"Health warnings on the cluster not cleared for Maintainence mode on the hosts."
                    f" Warns on cluster : {ceph_health_status}\n"
                )
                raise Exception("Health warnings not cleared error")

        log.info(
            "Proceeding to do checks post Stretch mode post maintenance mode scenarios"
        )
    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        log.debug(
            "Checking if any hosts are in Maintenance mode and if found, removing them"
        )
        hosts = ceph_cluster.get_nodes()
        for host in hosts:
            if not host.role == "client":
                if rados_obj.check_host_status(
                    hostname=host.hostname, status="Maintenance"
                ):
                    rados_obj.host_maintenance_exit(hostname=host.hostname, retry=15)
        time.sleep(60)

        if not post_site_down_checks(rados_obj=rados_obj):
            log.error(
                f"Checks failed post Site {affected_site} post maintenance mode scenarios"
            )
            raise Exception("Post execution checks failed on the Stretch cluster")

        if not rados_obj.run_pool_sanity_check():
            log.error(
                f"Checks failed post Site {affected_site} post maintenance mode scenarios"
            )
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
