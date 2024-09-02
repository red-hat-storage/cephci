"""
This test module is used to test net-split scenarios with recovery in the stretch environment
includes:
CEPH-83574979 - Netsplit b/w Zone-A and Zone-B is resolved. Zone-C Connected to both sites. Fail...
1. Netsplit b/w data sites
2. Netsplit b/w data site and arbiter site

"""

import time
from collections import namedtuple

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

Hosts = namedtuple("Hosts", ["dc_1_hosts", "dc_2_hosts", "tiebreaker_hosts"])


def run(ceph_cluster, **kw):
    """
    performs Netsplit scenarios in stretch mode
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
    netsplit_site = config.get("netsplit_site", "DC1")
    tiebreaker_mon_site_name = config.get("tiebreaker_mon_site_name", "arbiter")
    cluster_nodes = ceph_cluster.get_nodes()
    installer = ceph_cluster.get_nodes(role="installer")[0]
    init_time, _ = installer.exec_command(cmd="sudo date '+%Y-%m-%d %H:%M:%S'")
    log.debug(f"Initial time when test was started : {init_time}")

    try:
        # Starting to flush IP table rules on all hosts
        for host in cluster_nodes:
            log.debug(f"Proceeding to flush iptable rules on host : {host.hostname}")
            host.exec_command(sudo=True, cmd="iptables -F", long_running=True)
        time.sleep(20)

        if not stretch_enabled_checks(rados_obj=rados_obj):
            log.error(
                "The cluster has not cleared the pre-checks to run stretch tests. Exiting..."
            )
            raise Exception("Test pre-execution checks failed")

        log.info(
            f"Starting Netsplit scenario with site : {netsplit_site}. Pre-checks Passed"
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

        log.debug(f"Hosts present in Datacenter : {dc_1_name} : {dc_1_hosts}")
        log.debug(f"Hosts present in Datacenter : {dc_2_name} : {dc_2_hosts}")
        log.debug(
            f"Hosts present in Datacenter : {tiebreaker_mon_site_name} : {tiebreaker_hosts}"
        )

        # Checking if the site passed to induce netsplit is present in the Cluster CRUSH
        if netsplit_site not in [tiebreaker_mon_site_name, dc_1_name, dc_2_name]:
            log.error(
                f"Passed site : {netsplit_site} not part of crush locations on cluster.\n"
                f"locations present on cluster : {[tiebreaker_mon_site_name, dc_1_name, dc_2_name]}"
            )
            raise Exception("Test execution failed")

        # Creating test pool to check the effect of Netsplit scenarios on the Pool IO
        if not rados_obj.create_pool(pool_name=pool_name):
            log.error(f"Failed to create pool : {pool_name}")
            raise Exception("Test execution failed")

        # Sleeping for 10 seconds for pool to be populated in the cluster
        time.sleep(10)

        # Collecting the init no of objects on the pool, before site down
        pool_stat = rados_obj.get_cephdf_stats(pool_name=pool_name)
        init_objects = pool_stat["stats"]["objects"]
        log.debug(
            f"initial number of objects on the pool : {pool_name} is {init_objects}"
        )

        # Checking where netsplit scenario needs to be induced. It would be either b/w data sites or Arbiter site

        # Starting test to induce netsplit b/w Arbiter site and 1 data site
        if netsplit_site in [dc_1_name, dc_2_name]:
            log.debug(
                f"Proceeding to induce netsplit scenario b/w the two data sites. Adding IPs of {netsplit_site}"
                f"into other site for blocking Incoming and Outgoing packets"
            )

            for host1 in dc_1_hosts:
                target_host_obj = rados_obj.get_host_object(hostname=host1)
                if not target_host_obj:
                    log.error(f"target host : {host1} not found . Exiting...")
                    raise Exception("Test execution Failed")
                log.debug(
                    f"Proceeding to add IPtables rules to block incoming - outgoing traffic to host {host1} "
                )
                for host2 in dc_2_hosts:
                    source_host_obj = rados_obj.get_host_object(hostname=host2)
                    log.debug(
                        f"Proceeding to add IPtables rules to block incoming - outgoing traffic to host {host1} "
                        f"Applying rules on host : {host2}"
                    )
                    if not source_host_obj:
                        log.error(f"Source host : {host2} not found . Exiting...")
                    if not rados_obj.block_in_out_packets_on_host(
                        source_host=source_host_obj, target_host=target_host_obj
                    ):
                        log.error(
                            f"Failed to add IPtable rules to block {host1} on {host2}"
                        )
                        raise Exception("Test execution Failed")

            log.info("Completed adding IPtable rules into all hosts of DC1 to DC2")

            # sleeping for 180 seconds for the DC to be identified as down and proceeding to next checks
            time.sleep(180)

            # Checking the health status of the cluster and the active alerts for site down
            # These should be generated on the cluster
            status_report = rados_obj.run_ceph_command(
                cmd="ceph report", client_exec=True
            )
            ceph_health_status = list(status_report["health"]["checks"].keys())
            expected_health_warns = (
                "OSD_HOST_DOWN",
                "OSD_DOWN",
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
                f"The expected health warnings are generated on the cluster. Warnings : {ceph_health_status}"
            )

            log.debug(
                "Checking is the cluster is marked degraded and "
                "operating in degraded mode post Netsplit b/w data sites"
            )
            stretch_details = rados_obj.get_stretch_mode_dump()
            if not stretch_details["degraded_stretch_mode"]:
                log.error(
                    f"Stretch Cluster is not marked as degraded even though we have "
                    f"netsplit b/w data sites : {stretch_details}"
                )
                raise Exception(
                    "Stretch mode degraded test Failed on the provided cluster"
                )

            log.info(
                f"Cluster is marked degraded post netsplit b/w data sites {stretch_details},"
                f"Proceeding to try writes into cluster"
            )

        # Starting test to induce netsplit b/w Arbiter site and 1 data site
        else:
            log.info("Proceeding to induce newtsplit b/w data site and Arbiter site")
            for host1 in tiebreaker_hosts:
                target_host_obj = rados_obj.get_host_object(hostname=host1)
                if not target_host_obj:
                    log.error(f"target host : {host1} not found . Exiting...")
                    raise Exception("Test execution Failed")

                log.debug(
                    f"Proceeding to add IPtables rules to block incoming - outgoing traffic to host {host1} "
                )
                data_site_hosts = dc_2_hosts + dc_1_hosts
                for host2 in data_site_hosts:
                    source_host_obj = rados_obj.get_host_object(hostname=host2)
                    log.debug(
                        f"Proceeding to add IPtables rules to block incoming - outgoing traffic to host {host1} "
                        f"Applying rules on host : {host2}"
                    )
                    if not source_host_obj:
                        log.error(f"Source host : {host2} not found . Exiting...")
                        raise Exception("Test execution Failed")
                    if not rados_obj.block_in_out_packets_on_host(
                        source_host=source_host_obj, target_host=target_host_obj
                    ):
                        log.error(
                            f"Failed to add IPtable rules to block {host1} on {host2}"
                        )
                        raise Exception("Test execution Failed")

            log.info(
                "Completed adding IPtable rules into all hosts of Arbiter site to DC2 site"
            )

            # sleeping for 120 seconds for the DC to be identified as down and proceeding to next checks
            time.sleep(120)

            # Installer node will be not be able to communicate at this point.
            # all operations need to be done at client nodes
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
                f"Completed adding netsplits in hosts in site {netsplit_site}. Host names :{tiebreaker_hosts}."
                f" Proceeding to write"
            )

        log.debug("sleeping for 2 minutes before starting writes.")
        time.sleep(120)
        # Starting checks to see availability of cluster during netsplit scenario
        # perform rados put to check if write ops is possible
        pool_obj.do_rados_put(client=client_node, pool=pool_name, nobj=200, timeout=100)
        # rados_obj.bench_write(pool_name=pool_name, rados_write_duration=100)

        log.debug("sleeping for 4 minutes for the objects to be displayed in ceph df")
        time.sleep(240)

        # Getting the number of objects post write, to check if writes were successful
        pool_stat_final = rados_obj.get_cephdf_stats(pool_name=pool_name)
        log.debug(pool_stat_final)
        final_objects = pool_stat_final["stats"]["objects"]
        log.debug(
            f"Final number of objects on the pool : {pool_name} is {final_objects}"
        )

        # Objects should be more than the initial no of objects
        if int(final_objects) <= int(init_objects):
            log.error(
                "Write ops should be possible, number of objects in the pool has not changed"
            )
            log.debug(
                "Test case expected to fail until bug fix : https://bugzilla.redhat.com/show_bug.cgi?id=2265116"
            )
            raise Exception(
                f"Pool {pool_name} has {pool_stat['stats']['objects']} objs"
            )
        log.info(
            f"Successfully wrote {pool_stat['stats']['objects']} on pool {pool_name} in degraded mode\n"
            f"Proceeding to remove the IPtable rules and recover the cluster from degraded mode"
        )

        time.sleep(5)

        # Starting to flush IP table rules on all hosts
        for host in cluster_nodes:
            log.debug(f"Proceeding to flush iptable rules on host : {host.hostname}")
            host.exec_command(sudo=True, cmd="iptables -F", long_running=True)
        log.debug("Sleeping for 30 seconds...")
        time.sleep(30)

        log.info("Proceeding to do checks post Stretch mode netsplit scenarios")

        if not post_site_down_checks(rados_obj=rados_obj):
            log.error(f"Checks failed post Site {netsplit_site} Down and Up scenarios")
            raise Exception("Post execution checks failed on the Stretch cluster")

        if not rados_obj.run_pool_sanity_check():
            log.error(f"Checks failed post Site {netsplit_site} Down and Up scenarios")
            raise Exception("Post execution checks failed on the Stretch cluster")

    except Exception as err:
        log.error(f"Hit an exception: {err}. Test failed")
        log.debug(
            "Test case expected to fail until bug fix : https://bugzilla.redhat.com/show_bug.cgi?id=2265116"
        )
        return 1
    finally:
        log.debug("---------------- In Finally Block -------------")
        # Starting to flush IP table rules on all hosts
        for host in cluster_nodes:
            log.debug(f"Proceeding to flush iptable rules on host : {host.hostname}")
            host.exec_command(sudo=True, cmd="iptables -F", long_running=True)

        if config.get("delete_pool"):
            rados_obj.delete_pool(pool=pool_name)

        init_time, _ = installer.exec_command(cmd="sudo date '+%Y-%m-%d %H:%M:%S'")
        log.debug(f"time when test was Ended : {init_time}")

        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info("All the tests completed on the cluster, Pass!!!")
    return 0
