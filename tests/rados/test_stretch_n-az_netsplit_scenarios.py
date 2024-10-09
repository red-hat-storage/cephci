"""
This test module is used to test net-split scenarios with recovery in the stretch pool environment - 3 AZ
includes:
1. Netsplit b/w data sites in a 3 AZ cluster with post test checks.

"""

import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from utility.log import Log

log = Log(__name__)


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
    stretch_bucket = config.get("stretch_bucket", "datacenter")
    netsplit_site_1 = config.get("netsplit_site_1", "DC1")
    netsplit_site_2 = config.get("netsplit_site_2", "DC2")
    set_debug = config.get("set_debug", False)
    rule_name = config.get("rule_name", "3az_rule_netsplit")
    cluster_nodes = ceph_cluster.get_nodes()
    installer = ceph_cluster.get_nodes(role="installer")[0]
    init_time, _ = installer.exec_command(cmd="sudo date '+%Y-%m-%d %H:%M:%S'")
    log.debug(f"Initial time when test was started : {init_time}")

    try:

        osd_tree_cmd = "ceph osd tree"
        buckets = rados_obj.run_ceph_command(osd_tree_cmd)
        dc_buckets = [d for d in buckets["nodes"] if d.get("type") == stretch_bucket]
        dc_names = [name["name"] for name in dc_buckets]

        if netsplit_site_1 not in dc_names and netsplit_site_2 not in dc_names:
            log.error(
                f"Passed DC names does not exist on the cluster."
                f"DC's on cluster : {dc_names}"
                f"Passed names : {netsplit_site_1} & {netsplit_site_2}"
            )
            raise Exception("DC names not found to test netsplit")

        # Starting to flush IP table rules on all hosts
        for host in cluster_nodes:
            log.debug(f"Proceeding to flush iptable rules on host : {host.hostname}")
            host.exec_command(sudo=True, cmd="iptables -F", long_running=True)
        time.sleep(60)

        if not rados_obj.run_pool_sanity_check():
            log.error(
                "Cluster PGs not in active + clean state before starting the tests"
            )
            raise Exception("Post execution checks failed on the Stretch cluster")

        # log cluster health
        rados_obj.log_cluster_health()

        all_hosts = rados_obj.get_multi_az_stretch_site_hosts(
            num_data_sites=len(dc_names), stretch_bucket=stretch_bucket
        )
        for site in dc_names:
            log.debug(
                f"Hosts present in Datacenter : {site} : {getattr(all_hosts, site)}"
            )

        log.info(
            f"Starting Netsplit scenario in the cluster B/W site {netsplit_site_1} & "
            f"{netsplit_site_2}."
            f" Pre-checks Passed and IP tables flushed on the cluster"
        )

        if set_debug:
            log.debug("Setting up debug configs on the cluster for mon & osd")
            rados_obj.run_ceph_command(
                cmd="ceph config set mon debug_mon 30", client_exec=True
            )
            rados_obj.run_ceph_command(
                cmd="ceph config set osd debug_osd 20", client_exec=True
            )

        # Creating test pool to check the effect of Netsplit scenarios on the Pool IO

        if not rados_obj.create_n_az_stretch_pool(
            pool_name=pool_name,
            rule_name=rule_name,
            rule_id=101,
            peer_bucket_barrier=stretch_bucket,
            num_sites=3,
            num_copies_per_site=2,
            total_buckets=3,
            req_peering_buckets=2,
        ):
            log.error(f"Unable to Create/Enable stretch mode on the pool : {pool_name}")
            raise Exception("Unable to enable stretch pool")

        # Sleeping for 10 seconds for pool to be populated in the cluster
        time.sleep(10)

        # Collecting the init no of objects on the pool, before site down
        pool_stat = rados_obj.get_cephdf_stats(pool_name=pool_name)
        init_objects = pool_stat["stats"]["objects"]
        log.debug(
            f"initial number of objects on the pool : {pool_name} is {init_objects}"
        )

        # Starting test to induce netsplit b/w
        log.debug(
            f"Proceeding to induce netsplit scenario b/w the two data sites. Adding IPs of {netsplit_site_1} hosts"
            f"into other site, i.e {netsplit_site_2} for blocking Incoming and Outgoing "
            f"packets between the two sites"
        )

        for host1 in getattr(all_hosts, netsplit_site_1):
            target_host_obj = rados_obj.get_host_object(hostname=host1)
            if not target_host_obj:
                log.error(f"target host : {host1} not found . Exiting...")
                raise Exception("Test execution Failed")
            log.debug(
                f"Proceeding to add IPtables rules to block incoming - outgoing traffic to host {host1} "
            )
            for host2 in getattr(all_hosts, netsplit_site_2):
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

        log.info(
            f"Completed adding IPtable rules into all hosts of {netsplit_site_1} to {netsplit_site_2}"
        )

        # sleeping for 120 seconds for the DC to be identified as down and proceeding to next checks
        time.sleep(120)

        # log cluster health
        rados_obj.log_cluster_health()

        # Checking the health status of the cluster and the active alerts for site down
        # These should be generated on the cluster
        status_report = rados_obj.run_ceph_command(cmd="ceph report", client_exec=True)
        ceph_health_status = list(status_report["health"]["checks"].keys())
        expected_health_warns = (
            "OSD_HOST_DOWN",
            "OSD_DOWN",
            "OSD_DATACENTER_DOWN",
            "MON_DOWN",
        )
        if not all(elem in ceph_health_status for elem in expected_health_warns):
            log.error(
                f"We do not have the expected health warnings generated on the cluster.\n"
                f" Warns on cluster : {ceph_health_status}\n"
                f"Expected Warnings : {expected_health_warns}\n"
            )
            # raise execption()

        log.info(
            f"The expected health warnings are generated on the cluster. Warnings : {ceph_health_status}"
        )

        log.debug(
            "Checking is the cluster is marked degraded and "
            "operating in degraded mode post Netsplit b/w data sites"
        )

        log.debug("sleeping for 4 minutes before starting writes.")
        time.sleep(600)

        # log cluster health
        rados_obj.log_cluster_health()

        # Starting checks to see availability of cluster during netsplit scenario
        # perform rados put to check if write ops is possible
        pool_obj.do_rados_put(client=client_node, pool=pool_name, nobj=200, timeout=100)
        # rados_obj.bench_write(pool_name=pool_name, rados_write_duration=100)

        log.debug("sleeping for 4 minutes for the objects to be displayed in ceph df")
        time.sleep(600)

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
            raise Exception(
                f"Pool {pool_name} has {pool_stat['stats']['objects']} objs"
            )

        log.info(
            f"Successfully wrote {int(final_objects) - int(init_objects)} on pool {pool_name} in degraded mode\n"
            f"Proceeding to remove the IPtable rules and recover the cluster from degraded mode"
        )

        time.sleep(5)

        # Starting to flush IP table rules on all hosts
        for host in cluster_nodes:
            log.debug(f"Proceeding to flush iptable rules on host : {host.hostname}")
            host.exec_command(sudo=True, cmd="iptables -F", long_running=True)
            log.debug(
                "Observed that just IP tables flush did not work to bring back the nodes to cluster."
                f"rebooting the nodes post testing. Rebooting node : {host.hostname}"
            )
            host.exec_command(sudo=True, cmd="reboot")
        log.debug("Sleeping for 30 seconds...")
        time.sleep(30)

        log.info("Proceeding to do checks post Stretch mode netsplit scenarios")

        if not rados_obj.run_pool_sanity_check():
            log.error("Checks failed post Site Netsplit scenarios")
            raise Exception("Post execution checks failed on the Stretch cluster")

    except Exception as err:
        log.error(f"Hit an exception: {err}. Test failed")
        log.debug(
            "Test case expected to fail until bug fix : https://bugzilla.redhat.com/show_bug.cgi?id=2318975"
        )
        return 1
    finally:
        log.debug("---------------- In Finally Block -------------")
        # Starting to flush IP table rules on all hosts
        for host in cluster_nodes:
            log.debug(f"Proceeding to flush iptable rules on host : {host.hostname}")
            host.exec_command(sudo=True, cmd="iptables -F", long_running=True)
            log.debug(
                "Observed that just IP tables flush did not work to bring back the nodes to cluster."
                f"rebooting the nodes post testing. Rebooting node : {host.hostname}"
            )
            host.exec_command(sudo=True, cmd="reboot")

        rados_obj.rados_pool_cleanup()
        cmd = f"ceph osd crush rule rm {rule_name}"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)

        init_time, _ = installer.exec_command(cmd="sudo date '+%Y-%m-%d %H:%M:%S'")
        log.debug(f"time when test was Ended : {init_time}")
        if set_debug:
            log.debug("Removing debug configs on the cluster for mon & osd")
            rados_obj.run_ceph_command(
                cmd="ceph config rm mon debug_mon", client_exec=True
            )
            rados_obj.run_ceph_command(
                cmd="ceph config rm osd debug_osd", client_exec=True
            )

        time.sleep(60)
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info("All the tests completed on the cluster, Pass!!!")
    return 0
