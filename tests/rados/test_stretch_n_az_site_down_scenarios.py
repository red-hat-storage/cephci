"""
This test module is used to test site down scenarios with recovery in the stretch pool environment - 3 AZ
includes:
Scenario 1: Shutdown & Start 1 OSD on 1 host in 1 DC
Scenario 2: Shutdown & Start 1 OSD on all host in 1 DC
Scenario 3: Shutdown & Start the OSD on 1 host in all DC
Scenario 4 : Shutdown & Start 1 Host on 1 DC
Scenario 5 : Shutdown & Start 1 Host on all DC
Scenario 6 : Shutdown & Start all Host on 1 DC
Scenario 7 : Shutdown & Start all Host on DC-1 + 1 host on DC-2
Scenario 8 : Addition of host into maintenance mode - 1 Host on 1 DC
Scenario 9 : Addition of host into maintenance mode - 1 Host on all DC
Scenario 10 : Addition of host into maintenance mode - all Host on 1 DC
"""

import random
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from ceph.rados.utils import get_cluster_timestamp
from ceph.utils import find_vm_node_by_hostname
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    performs site down scenarios in stretch mode
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
    set_debug = config.get("set_debug", False)
    rule_name = config.get("rule_name", "3az_rule")
    installer = ceph_cluster.get_nodes(role="installer")[0]
    stime, _ = installer.exec_command(cmd="sudo date '+%Y-%m-%d %H:%M:%S'")
    log.debug("Initial time when test was started : " + stime)
    scenarios_to_run = config.get(
        "scenarios_to_run",
        [
            "scenario-1",
            "scenario-2",
            "scenario-3",
            "scenario-4",
            "scenario-5",
            "scenario-6",
            "scenario-7",
        ],
    )
    start_time = get_cluster_timestamp(rados_obj.node)
    log.debug(f"Test workflow started. Start time: {start_time}")
    try:

        def write_obj_check_health_warn(
            scenario: str, health_warns: tuple, num_obj: int = 200
        ) -> bool:
            """
            Method to write objects into pool and check health warnings on the cluster

            Args:
                scenario: unique scenario name
                health_warns: expected health warnings on cluster
                num_obj: number of objects to be written into the pool

            Return:
                True -> Pass, False -> Fail
            """
            ini_objects = rados_obj.get_cephdf_stats(pool_name=pool_name)["stats"][
                "objects"
            ]
            # Performing writes on pool
            pool_obj.do_rados_put(
                client=client_node,
                pool=pool_name,
                nobj=num_obj,
                timeout=100,
                obj_name=scenario,
            )
            time.sleep(60)

            # Collecting the no of objects on the pool, after Scenario 1
            objects_post = rados_obj.get_cephdf_stats(pool_name=pool_name)["stats"][
                "objects"
            ]
            debug_msg = f"Number of objects on the pool : {pool_name} is {objects_post}"
            log.info(debug_msg)

            if int(ini_objects) + num_obj != int(objects_post):
                log.error(
                    "Could not write %s objects into the pool %s post scenario %s",
                    num_obj,
                    pool_name,
                    scenario,
                )
                return False
            log.debug(
                "IOs successful post scenario : %s, Checking Health warnings on cluster",
                scenario,
            )

            for warning in health_warns:
                for _ in range(3):
                    if rados_obj.check_health_warning(warning=warning):
                        log.info(
                            "Expected health warning : %s found on the cluster", warning
                        )
                        break
                    log.info(
                        "Expected health warning : %s not found on the cluster\n"
                        "Retrying after 20 seconds",
                        warning,
                    )
                    time.sleep(20)
                else:
                    log.info(
                        "Expected health warning : %s not found on the cluster after retries",
                        warning,
                    )
                    return False
                log.debug("Warning : %s present on the cluster", warning)
            log.info("Completed IOs and verified the health warnings")
            return True

        osd_tree_cmd = "ceph osd tree"
        buckets = rados_obj.run_ceph_command(osd_tree_cmd)
        dc_buckets = [d for d in buckets["nodes"] if d.get("type") == stretch_bucket]
        dc_names = [name["name"] for name in dc_buckets]

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

        # Creating test pool to check the effect of OSD/ host down scenarios
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

        log.info("Starting with OSD/ Host down scenarios on the cluster")

        if set_debug:
            log.debug("Setting up debug configs on the cluster for mon & osd")
            rados_obj.run_ceph_command(
                cmd="ceph config set mon debug_mon 30", client_exec=True
            )
            rados_obj.run_ceph_command(
                cmd="ceph config set osd debug_osd 20", client_exec=True
            )

        # Collecting the init no of objects on the pool, before site down
        init_objects = rados_obj.get_cephdf_stats(pool_name=pool_name)["stats"][
            "objects"
        ]
        debug_msg = f"initial number of objects on the pool : {pool_name} is {init_objects} before starting tests"
        log.info(debug_msg)

        if "scenario-1" in scenarios_to_run:
            # Scenario 1: Shutdown & Start 1 OSD on 1 host in 1 DC
            target_dc = random.choice(dc_names)
            target_hosts = getattr(all_hosts, target_dc)
            # Ensure target_host has OSD daemons in it
            osd_hosts = set(rados_obj.get_osd_hosts())
            target_host = random.choice(target_hosts)
            while target_host not in osd_hosts:
                target_host = random.choice(target_hosts)
            target_osd = random.choice(
                rados_obj.collect_osd_daemon_ids(osd_node=target_host)
            )
            debug_msg = (
                f"\n Scenario 1 : Shutdown & Start 1 OSD on 1 host in 1 DC \n"
                f"Selected items for the scenario 1: \n"
                f"DC : {target_dc} \n"
                f"Host : {target_host}\n"
                f"OSD : {target_osd} \n "
            )
            log.info(debug_msg)

            # stopping the OSD
            if not rados_obj.change_osd_state(action="stop", target=target_osd):
                log.error("Unable to stop the OSD : %s", target_osd)
                raise Exception("Execution error - Scenario 1")
            log.debug("Stopped OSD : %s", target_osd)

            expected_health_warns = ("OSD_DOWN",)
            if not write_obj_check_health_warn(
                scenario="scenario-1", health_warns=expected_health_warns
            ):
                log.error(
                    "Could not write objects or could not verify health warnings post scenario-1"
                )
                raise Exception("Execution error")

            # Starting the OSD
            if not rados_obj.change_osd_state(action="restart", target=target_osd):
                log.error("Unable to start the OSD : %s", target_osd)
                raise Exception("Execution error: OSD not Restarted post scenario-1")
            log.debug(
                "Started OSD : %s and waiting for clean PGs post scenario-1", target_osd
            )

            if not wait_for_clean_pg_sets(rados_obj):
                log.error(
                    "The cluster did not reach active + Clean state after Scenario 1"
                )
                raise Exception("Execution error: No Clean PGS post scenario-1")
            log.info("\n\n\n\nCompleted scenario 1\n\n\n\n")
            rados_obj.log_cluster_health()

        if "scenario-2" in scenarios_to_run:
            # Scenario 2: Shutdown & Start 1 OSD on all host in 1 DC
            target_dc = random.choice(dc_names)
            target_hosts = getattr(all_hosts, target_dc)
            # Ensure target_host has OSD daemons in it
            osd_hosts = set(rados_obj.get_osd_hosts())
            target_osd_hosts = list(set(osd_hosts) & set(target_hosts))
            stopped_osds = []
            for target_host in target_osd_hosts:
                target_osd = random.choice(
                    rados_obj.collect_osd_daemon_ids(osd_node=target_host)
                )
                debug_msg = (
                    f"\n Scenario 2 : Shutdown & Start 1 OSD on all host in 1 DC \n"
                    f"Selected items for the scenario 2: \n"
                    f"DC : {target_dc} \n"
                    f"Host : {target_host}\n"
                    f"OSDs : {target_osd} \n "
                )
                log.info(debug_msg)
                stopped_osds.append(target_osd)

                # stopping the OSD
                if not rados_obj.change_osd_state(action="stop", target=target_osd):
                    log.error("Unable to stop the OSD : %s", target_osd)
                    raise Exception("Execution error post scenario-2")
                log.debug("Stopped OSD : %s", target_osd)

            log.debug("Stopped 1 OSD on all hosts of 1 DC & Performing writes")
            time.sleep(10)

            expected_health_warns = ("OSD_DOWN",)
            if not write_obj_check_health_warn(
                scenario="scenario-2", health_warns=expected_health_warns
            ):
                log.error(
                    "Could not write objects or could not verify health warnings post scenario-2"
                )
                raise Exception("Execution error - post scenario-2")

            # Starting the OSD
            for target_osd in stopped_osds:
                if not rados_obj.change_osd_state(action="restart", target=target_osd):
                    log.error(
                        "Unable to start the OSD : %s post scenario-2", target_osd
                    )
                    raise Exception(
                        "Execution error: OSD not Restarted post scenario-2"
                    )
                log.debug(
                    "Started OSD : %s and waiting for clean PGs post scenario-2",
                    target_osd,
                )
            time.sleep(60)

            if not wait_for_clean_pg_sets(rados_obj):
                log.error(
                    "The cluster did not reach active + Clean state after Scenario 2"
                )
                raise Exception("Execution error: No Clean PGS post scenario-2")
            log.info("\n\n\n\nCompleted scenario 2\n\n\n\n")
            rados_obj.log_cluster_health()

        if "scenario-3" in scenarios_to_run:
            # Scenario 3: Shutdown & Start the OSD on 1 host in all DC
            stopped_osds = []
            for target_dc in dc_names:
                target_hosts = getattr(all_hosts, target_dc)
                # Ensure target_host has OSD daemons in it
                osd_hosts = set(rados_obj.get_osd_hosts())
                target_host = random.choice(target_hosts)
                while target_host not in osd_hosts:
                    target_host = random.choice(target_hosts)
                target_osd = random.choice(
                    rados_obj.collect_osd_daemon_ids(osd_node=target_host)
                )

                debug_msg = (
                    f"\n Scenario 3 : Shutdown & Start 1 OSD on 1 host in all DC \n "
                    f"Selected items for the scenario 3: \n"
                    f"DC : {target_dc} \n"
                    f"Host : {target_host}\n"
                    f"OSDs : {target_osd} \n "
                )
                log.info(debug_msg)
                stopped_osds.append(target_osd)

                # stopping the OSD
                if not rados_obj.change_osd_state(action="stop", target=target_osd):
                    log.error("Unable to stop the OSD : %s", target_osd)
                    raise Exception("Execution error - post scenario-3")
                log.debug("Stopped OSD : %s", target_osd)
            log.debug("Stopped 1 OSD on 1 hosts of all DC & Performing writes")
            time.sleep(10)

            expected_health_warns = ("OSD_DOWN",)
            if not write_obj_check_health_warn(
                scenario="scenario-3", health_warns=expected_health_warns
            ):
                log.error(
                    "Could not write objects or could not verify health warnings post scenario-3"
                )
                raise Exception("Execution error - post scenario-3")

            # Starting the OSD
            for target_osd in stopped_osds:
                if not rados_obj.change_osd_state(action="restart", target=target_osd):
                    log.error("Unable to start the OSD : %s", target_osd)
                    raise Exception(
                        "Execution error: OSD not Restarted post scenario-3"
                    )
                log.debug("Started OSD : %s and waiting for clean PGs", target_osd)
            time.sleep(60)

            if not wait_for_clean_pg_sets(rados_obj):
                log.error(
                    "The cluster did not reach active + Clean state after Scenario 3"
                )
                raise Exception("Execution error: No Clean PGS - post scenario-3")
            log.info("\n\n\n\nCompleted scenario 3\n\n\n\n")
            rados_obj.log_cluster_health()

        if "scenario-4" in scenarios_to_run:
            # Scenario 4 : Shutdown & Start 1 Host on 1 DC
            target_dc = random.choice(dc_names)
            target_hosts = getattr(all_hosts, target_dc)
            # Ensure target_host has OSD daemons in it
            osd_hosts = set(rados_obj.get_osd_hosts())
            target_host = random.choice(target_hosts)
            while target_host not in osd_hosts:
                target_host = random.choice(target_hosts)
            debug_msg = (
                f"\n Scenario 4 : Shutdown & Start 1 OSD on 1 host in 1 DC \n "
                f"Selected items for the scenario 4: \n"
                f"DC : {target_dc} \n"
                f"OSD Host : {target_host}\n"
            )
            log.info(debug_msg)

            # stopping the Host
            log.debug("Proceeding to shutdown host %s", target_host)
            target_node = find_vm_node_by_hostname(ceph_cluster, target_host)
            target_node.shutdown(wait=True)
            log.debug("Stopped the host")
            time.sleep(10)

            expected_health_warns = (
                "OSD_HOST_DOWN",
                "OSD_DOWN",
            )
            if not write_obj_check_health_warn(
                scenario="scenario-4", health_warns=expected_health_warns
            ):
                log.error(
                    "Could not write objects or could not verify health warnings post scenario-4"
                )
                raise Exception("Execution error - post scenario-4")

            log.debug("Wrote IOs and Verified health warnings post post scenario-4")

            # Starting the Host
            target_node = find_vm_node_by_hostname(ceph_cluster, target_host)
            target_node.power_on()
            log.debug("restarted the host")
            time.sleep(10)

            if not wait_for_clean_pg_sets(rados_obj):
                log.error(
                    "The cluster did not reach active + Clean state after Scenario 4"
                )
                raise Exception("Execution error: No Clean PGS - post scenario-4")
            log.info("\n\n\n\nCompleted scenario 4\n\n\n\n")
            rados_obj.log_cluster_health()

        if "scenario-5" in scenarios_to_run:
            # Scenario 5 : Shutdown & Start 1 Host on all DC
            shutdown_hosts = []
            osd_hosts = set(rados_obj.get_osd_hosts())
            for target_dc in dc_names:
                target_hosts = getattr(all_hosts, target_dc)
                # Ensure target_host has OSD daemons in it
                target_host = random.choice(target_hosts)
                while target_host not in osd_hosts:
                    target_host = random.choice(target_hosts)
                debug_msg = (
                    f"\n Scenario 5 : Shutdown & Start 1 Host on all DC \n "
                    f"Selected items for the scenario 5: \n"
                    f"DC : {target_dc} \n"
                    f"OSD Host : {target_host}\n"
                )
                log.info(debug_msg)
                shutdown_hosts.append(target_host)

                # stopping the Host
                log.debug(f"Proceeding to shutdown host {target_host}")
                target_node = find_vm_node_by_hostname(ceph_cluster, target_host)
                target_node.shutdown(wait=True)
                log.debug("Stopped the host")
                time.sleep(10)

            expected_health_warns = (
                "OSD_HOST_DOWN",
                "OSD_DOWN",
            )
            if not write_obj_check_health_warn(
                scenario="scenario-5", health_warns=expected_health_warns
            ):
                log.error(
                    "Could not write objects or could not verify health warnings post scenario-5"
                )
                raise Exception("Execution error - post scenario-5")

            # Starting the Host
            for target_host in shutdown_hosts:
                log.debug("Proceeding to Restart host %s", target_host)
                target_node = find_vm_node_by_hostname(ceph_cluster, target_host)
                target_node.power_on()
                log.debug("restarted the host")
            time.sleep(60)

            if not wait_for_clean_pg_sets(rados_obj):
                log.error(
                    "The cluster did not reach active + Clean state after Scenario 5"
                )
                raise Exception("Execution error: No Clean PGS - post scenario-5")
            log.info("\n\n\n\nCompleted scenario 5\n\n\n\n")
            rados_obj.log_cluster_health()

        if "scenario-6" in scenarios_to_run:
            # Scenario 6 : Shutdown & Start all Host on 1 DC
            target_dc = random.choice(dc_names)
            target_hosts = getattr(all_hosts, target_dc)
            for target_host in target_hosts:
                debug_msg = (
                    f"\n Scenario 6 : Shutdown & Start all Host on 1 DC \n "
                    f"Selected items for the scenario 6: \n"
                    f"DC : {target_dc} \n"
                    f"OSD Host : {target_host}\n"
                )
                log.info(debug_msg)

                # stopping the Host
                log.debug(f"Proceeding to shutdown host {target_host}")
                target_node = find_vm_node_by_hostname(ceph_cluster, target_host)
                target_node.shutdown(wait=True)
                log.debug("Stopped the host")
                time.sleep(10)

            log.debug("Stopped all hosts of 1 DC. Proceeding to write IOs")

            expected_health_warns = (
                "OSD_HOST_DOWN",
                "OSD_DOWN",
                "OSD_DATACENTER_DOWN",
                "MON_DOWN",
            )
            if not write_obj_check_health_warn(
                scenario="scenario-6", health_warns=expected_health_warns
            ):
                log.error(
                    "Could not write objects or could not verify health warnings post scenario-6"
                )
                raise Exception("Execution error post scenario-6")

            # Starting the Host
            for target_host in target_hosts:
                log.debug("Proceeding to Restart host %s", target_host)
                target_node = find_vm_node_by_hostname(ceph_cluster, target_host)
                target_node.power_on()
                log.debug("restarted the host")
            time.sleep(60)

            if not wait_for_clean_pg_sets(rados_obj):
                log.error(
                    "The cluster did not reach active + Clean state after Scenario 6"
                )
                raise Exception("Execution error: No Clean PGS - post scenario-6")
            log.info("\n\n\n\nCompleted scenario 6\n\n\n\n")
            rados_obj.log_cluster_health()

        if "scenario-7" in scenarios_to_run:
            # Scenario 7 : Shutdown & Start all Host on DC-1 + 1 host on DC-2
            target_dc_1 = dc_names[1]
            target_dc_2 = dc_names[2]
            target_hosts_dc1 = getattr(all_hosts, target_dc_1)
            target_hosts_dc2 = getattr(all_hosts, target_dc_2)
            # Ensure target_host has OSD daemons in it
            osd_hosts = set(rados_obj.get_osd_hosts())
            target_host_dc2 = random.choice(target_hosts_dc2)
            while target_host_dc2 not in osd_hosts:
                target_host_dc2 = random.choice(target_hosts_dc2)
            target_hosts = target_hosts_dc1 + [target_host_dc2]
            debug_msg = (
                f"\n Scenario 7 : Shutdown & Start all Host on DC-1 + 1 host on DC-2 \n "
                f"Selected items for the scenario 7: \n"
                f"DC : {target_dc_1} & {target_dc_2} \n"
                f"OSD Hosts : {target_hosts}\n"
                f"\n ---- In this scenario there might be some downtime, but cluster should recover on it's own "
                f"--------- \n "
            )
            log.info(debug_msg)
            for target_host in target_hosts:
                # stopping the Host
                log.debug("Proceeding to shutdown host %s", target_host)
                target_node = find_vm_node_by_hostname(ceph_cluster, target_host)
                target_node.shutdown(wait=True)
                log.debug("Stopped the host")
                time.sleep(10)

            log.debug(
                "\n post scenario-7 - Stopped all hosts of 1 DC + 1 host of DC 2.\n"
                " Sleeping for 10+2 minutes for down OSds to be marked out and some recovery to be completed"
            )
            time.sleep(12 * 60)

            expected_health_warns = (
                "OSD_HOST_DOWN",
                "OSD_DOWN",
                "OSD_DATACENTER_DOWN",
                "MON_DOWN",
            )
            if not write_obj_check_health_warn(
                scenario="scenario-7", health_warns=expected_health_warns
            ):
                log.error(
                    "Could not write objects or could not verify health warnings post scenario-7"
                )
                raise Exception("Execution error - post scenario-7")

            # Starting the Host
            for target_host in target_hosts:
                log.debug("Proceeding to Restart host %s", target_host)
                target_node = find_vm_node_by_hostname(ceph_cluster, target_host)
                target_node.power_on()
                log.debug("restarted the host")
            time.sleep(60)

            if not wait_for_clean_pg_sets(rados_obj):
                log.error(
                    "The cluster did not reach active + Clean state after Scenario 7"
                )
                raise Exception("Execution error: No Clean PGS - post scenario-7")
            log.info("\n\n\n\nCompleted scenario 7\n\n\n\n")
            rados_obj.log_cluster_health()

        if "scenario-8" in scenarios_to_run:
            # Scenario 8 : Addition of host into maintenance mode - 1 Host on 1 DC
            target_dc = random.choice(dc_names)
            target_hosts = getattr(all_hosts, target_dc)
            # Ensure target_host has OSD daemons in it
            osd_hosts = set(rados_obj.get_osd_hosts())
            target_host = random.choice(target_hosts)
            while target_host not in osd_hosts:
                target_host = random.choice(target_hosts)
            debug_msg = (
                f"\n Scenario 8 :  Addition of host into maintenance mode - 1 Host on 1 DC \n "
                f"Selected items for the scenario 8: \n"
                f"DC : {target_dc} \n"
                f"OSD Host : {target_host}\n"
            )
            log.info(debug_msg)

            # stopping the Host
            log.debug("Proceeding to add host : %s into maintenance mode", target_host)
            if not rados_obj.host_maintenance_enter(
                hostname=target_host, retry=10, yes_i_really_mean_it=True
            ):
                log.error(
                    "Failed to add host : %s into maintenance mode post scenario-8",
                    target_host,
                )
                raise Exception("Test execution Failed: Scenario 8")

            # Sleeping for 4 minutes. Observed that sometimes warning gets generated post 2 minutes.
            time.sleep(240)

            expected_health_warns = (
                "OSD_HOST_DOWN",
                "OSD_DOWN",
            )
            if not write_obj_check_health_warn(
                scenario="scenario-8", health_warns=expected_health_warns
            ):
                log.error(
                    "Could not write objects or could not verify health warnings post scenario-8"
                )
                raise Exception("Execution error post scenario-8")

            # Starting the Host
            log.debug("Proceeding to remove host %s from maintenance mode", target_host)
            if not rados_obj.host_maintenance_exit(hostname=target_host, retry=15):
                log.error(
                    "Failed to remove host : %s from maintenance mode post scenario-8",
                    target_host,
                )
                raise Exception("Test execution Failed")
            log.info(
                "Completed removing the host %s from maintenance mode.", target_host
            )
            time.sleep(10)

            if not wait_for_clean_pg_sets(rados_obj):
                log.error(
                    "The cluster did not reach active + Clean state after Scenario 8"
                )
                raise Exception("Execution error: No Clean PGS - post scenario-8")
            log.info("\n\n\n\nCompleted scenario 8\n\n\n\n")
            rados_obj.log_cluster_health()

        if "scenario-9" in scenarios_to_run:
            # Scenario 9 : Addition of host into maintenance mode - 1 Host on all DC
            down_hosts = []
            for target_dc in dc_names:
                target_hosts = getattr(all_hosts, target_dc)
                # Ensure target_host has OSD daemons in it
                osd_hosts = set(rados_obj.get_osd_hosts())
                target_host = random.choice(target_hosts)
                while target_host not in osd_hosts:
                    target_host = random.choice(target_hosts)
                debug_msg = (
                    f"\n  Scenario 9 : Addition of host into maintenance mode - 1 Host on all DC \n "
                    f"Selected items for the scenario 9: \n"
                    f"DC : {target_dc} \n"
                    f"OSD Host : {target_host}\n"
                )
                log.info(debug_msg)
                down_hosts.append(target_host)

                log.debug(
                    "Proceeding to add host : %s into maintenance mode", target_host
                )
                if not rados_obj.host_maintenance_enter(
                    hostname=target_host, retry=10, yes_i_really_mean_it=True
                ):
                    log.error(
                        "Failed to add host : %s into maintenance mode post scenario-9",
                        target_host,
                    )
                    raise Exception("Test execution Failed: Scenario 9")

            # Sleeping for 4 minutes. Observed that sometimes warning gets generated post 2 minutes.
            time.sleep(240)

            expected_health_warns = (
                "OSD_HOST_DOWN",
                "OSD_DOWN",
            )
            if not write_obj_check_health_warn(
                scenario="scenario-9", health_warns=expected_health_warns
            ):
                log.error(
                    "Could not write objects or could not verify health warnings post scenario-9"
                )
                raise Exception("Execution error")

            for target_host in down_hosts:
                # Starting the Host
                log.debug(
                    "Proceeding to remove host %s from maintenance mode", target_host
                )
                if not rados_obj.host_maintenance_exit(hostname=target_host, retry=15):
                    log.error(
                        "Failed to remove host : %s from maintenance mode post scenario-9",
                        target_host,
                    )
                    raise Exception("Test execution Failed")
                log.info(
                    "Completed removing the host %s from maintenance mode.", target_host
                )
                time.sleep(10)
            if not wait_for_clean_pg_sets(rados_obj):
                log.error(
                    "The cluster did not reach active + Clean state after Scenario 9"
                )
                raise Exception("Execution error: No Clean PGS post scenario-9")
            log.info("\n\n\n\nCompleted scenario 9\n\n\n\n")
            rados_obj.log_cluster_health()

        if "scenario-10" in scenarios_to_run:
            # Scenario 10 : Addition of host into maintenance mode - all Host on 1 DC
            target_dc = random.choice(dc_names)
            target_hosts = getattr(all_hosts, target_dc)
            for target_host in target_hosts:
                debug_msg = (
                    f"\n Scenario 10 : Addition of host into maintenance mode - all Host on 1 DC \n "
                    f"Selected items for the scenario 10: \n"
                    f"DC : {target_dc} \n"
                    f"OSD Host : {target_host}\n"
                )
                log.info(debug_msg)

                log.debug(
                    "Proceeding to add host : %s into maintenance mode", target_host
                )
                if not rados_obj.host_maintenance_enter(
                    hostname=target_host, retry=10, yes_i_really_mean_it=True
                ):
                    log.error(
                        "Failed to add host : %s into maintenance mode post scenario-10",
                        target_host,
                    )
                    raise Exception("Test execution Failed: Scenario 10")

            log.debug(
                "Added all hosts of DC into maintenance mode. Proceeding to write IOs"
            )

            expected_health_warns = (
                "OSD_HOST_DOWN",
                "OSD_DOWN",
                "OSD_DATACENTER_DOWN",
                "MON_DOWN",
            )
            if not write_obj_check_health_warn(
                scenario="scenario-10", health_warns=expected_health_warns
            ):
                log.error(
                    "Could not write objects or could not verify health warnings post scenario-10"
                )
                raise Exception("Execution error post scenario-10")

            for target_host in target_hosts:
                # Starting the Host
                log.debug(
                    "Proceeding to remove host %s from maintenance mode", target_host
                )
                if not rados_obj.host_maintenance_exit(hostname=target_host, retry=15):
                    log.error(
                        "Failed to remove host : %s from maintenance mode post scenario-10",
                        target_host,
                    )
                    raise Exception("Test execution Failed - post scenario-10")
                log.info(
                    "Completed removing the host %s from maintenance mode.", target_host
                )
                time.sleep(10)

            if not wait_for_clean_pg_sets(rados_obj):
                log.error(
                    "The cluster did not reach active + Clean state after Scenario 10"
                )
                raise Exception("Execution error: No Clean PGS - post scenario-10")
            log.info("\n\n\n\nCompleted scenario 10\n\n\n\n")
            rados_obj.log_cluster_health()

        if not rados_obj.run_pool_sanity_check():
            log.error("Checks failed post Site down scenarios")
            raise Exception("Post execution checks failed on the Stretch cluster")

        log.info("Completed all site down scenarios")

    except Exception as err:
        log.error("Hit an exception: %s. Test failed", err)
        return 1
    finally:
        log.debug("\n\n\n\n---------------- In Finally Block -------------\n\n\n\n")

        rados_obj.rados_pool_cleanup()

        init_time, _ = installer.exec_command(cmd="sudo date '+%Y-%m-%d %H:%M:%S'")
        log.debug("time when test was Ended : %s", init_time)

        if set_debug:
            log.debug("Removing debug configs on the cluster for mon & osd")
            rados_obj.run_ceph_command(
                cmd="ceph config rm mon debug_mon", client_exec=True
            )
            rados_obj.run_ceph_command(
                cmd="ceph config rm osd debug_osd", client_exec=True
            )
        cluster_nodes = ceph_cluster.get_nodes(ignore="client")
        site_down_scenarios = [
            "scenario-1",
            "scenario-2",
            "scenario-3",
            "scenario-4",
            "scenario-5",
            "scenario-6",
            "scenario-7",
        ]
        maintenance_mode_tests = [
            "scenario-8",
            "scenario-9",
            "scenario-10",
        ]
        if any(key in site_down_scenarios for key in scenarios_to_run):
            for host in cluster_nodes:
                if rados_obj.check_host_status(
                    hostname=host.hostname, status="offline"
                ):
                    log.info(
                        "Host: %s, is offline. trying to restart the host",
                        host.hostname,
                    )
                    target_node = find_vm_node_by_hostname(ceph_cluster, host.hostname)
                    target_node.power_on()
                    log.info("started the host %s", host.hostname)
                    time.sleep(60)
            log.debug(
                "All down hosts were started up or no hosts were down."
                "Checking if ther are any Down OSDs"
            )
            down_osds = rados_obj.get_osd_list(status="down")
            for target_osd in down_osds:
                if not rados_obj.change_osd_state(action="start", target=target_osd):
                    log.error("Unable to start the OSD : target_osd", target_osd)
                    return 1
                log.debug("Started OSD : %s and waiting for clean PGs", target_osd)

            if down_osds:
                time.sleep(300)
            # log cluster health
            rados_obj.log_cluster_health()
            # check for crashes after test execution
            test_end_time = get_cluster_timestamp(rados_obj.node)
            log.debug(
                f"Test workflow completed. Start time: {start_time}, End time: {test_end_time}"
            )
            if rados_obj.check_crash_status(
                start_time=start_time, end_time=test_end_time
            ):
                log.error("Test failed due to crash at the end of test")
                return 1

        if any(key in maintenance_mode_tests for key in scenarios_to_run):
            for host in cluster_nodes:
                if rados_obj.check_host_status(
                    hostname=host.hostname, status="Maintenance"
                ):
                    log.info(
                        "Host: %s, is offline. trying to restart the host",
                        host.hostname,
                    )
                    if not rados_obj.host_maintenance_exit(
                        hostname=host.hostname, retry=15
                    ):
                        log.error(
                            "Failed to remove host : %s from maintenance mode",
                            host.hostname,
                        )
                        return 1
                    log.info(
                        "Completed removing the host %s from maintenance mode.",
                        host.hostname,
                    )
            log.debug(
                "All down hosts were started up or no hosts were in maintenance mode"
            )

        time.sleep(100)
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        test_end_time = get_cluster_timestamp(rados_obj.node)
        log.debug(
            f"Test workflow completed. Start time: {start_time}, End time: {test_end_time}"
        )
        if rados_obj.check_crash_status(start_time=start_time, end_time=test_end_time):
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info("All the tests completed on the cluster, Pass!!!")
    return 0
