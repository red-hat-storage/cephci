"""
test Module to :
1. Create a 2+2 ec pool
2. fill the pool
3. Test the effects of bulk flag and no IO stoppage
4. rolling reboot of OSDs of a host
5. OSD start and stop for failure domain level
6. Serviceability tests
7. Combined workflow: PG splits with bulk flag and EC pool creation while marking OSDs in/out
"""

import datetime
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados import utils
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from ceph.rados.serviceability_workflows import ServiceabilityMethods
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.rados_test_util import (
    get_device_path,
    wait_for_daemon_status,
    wait_for_device_rados,
)
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log
from utility.utils import method_should_succeed, should_not_be_empty

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test to Verify the ec 2+2 pool
    Returns:
        1 -> Fail, 0 -> Pass
    Scenarios:
        scenario-1: Test the effects of bulk flag and no IO stoppage
        scenario-2: Perform rolling reboot of all the OSDs on a particular host.
        scenario-3: OSD operations
        scenario-4: Stopping 1 OSD from each host
        scenario-5: Stopping all OSDs of 1 host
        scenario-6: Add new host and OSDs into the cluster
        scenario-7: Remove 1 OSD from 1 host
        scenario-8: Remove 1 host from the cluster
        scenario-9: Combined workflow - Create PG splits with bulk flag and run EC pool
                    creation while marking OSDs in/out
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    pool_obj = PoolFunctions(node=cephadm)
    service_obj = ServiceabilityMethods(cluster=ceph_cluster, **config)
    test_fail = False
    set_debug = config.get("set_debug", False)
    new_node_label = config.get("new_node_label", "osd-bak")
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
            "scenario-8",
            "scenario-9",
        ],
    )

    if not mon_obj.verify_set_config(
        section="mon", name="mon_osd_down_out_subtree_limit", value="host"
    ):
        log.error(
            "mon_osd_down_out_subtree_limit not set on the cluster. setting the same now"
        )
        mon_obj.set_config(
            section="mon", name="mon_osd_down_out_subtree_limit", value="host"
        )

    # commenting the addition of osd_async_recovery_min_cost param on cluster.
    # Bug Fixed : https://bugzilla.redhat.com/show_bug.cgi?id=2228778
    # if not mon_obj.verify_set_config(
    #     section="osd", name="osd_async_recovery_min_cost", value="1099511627776"
    # ):
    #     log.error(
    #         "osd_async_recovery_min_cost not set on the cluster. setting the same now"
    #     )
    #     mon_obj.set_config(
    #         section="osd", name="osd_async_recovery_min_cost", value="1099511627776"
    #     )

    log.debug(
        "Completed setting of the global configs need to run 2+2 tests. Proceeding further"
    )

    # Creating the EC pool
    ec_config = config.get("ec_pool")
    pool_name = ec_config["pool_name"]
    # Due to below tracker, suppressing few heath warnings when fast ec is enabled on pools.
    # https://tracker.ceph.com/issues/71645 . ( bugzilla to be raised soon )
    # bugzilla : https://bugzilla.redhat.com/show_bug.cgi?id=2400427
    fast_ec_enabled = ec_config.get("enable_fast_ec_features")
    warning_ignore_list = ["OSD_SCRUB_ERRORS", "PG_DAMAGED"] if fast_ec_enabled else []

    try:
        if set_debug:
            log.debug(
                "Setting up debug configs on the cluster for mon, osd & Mgr daemons"
            )
            mon_obj.set_config(section="osd", name="debug_osd", value="20/20")
            mon_obj.set_config(section="mon", name="debug_mon", value="30/30")
            mon_obj.set_config(section="mgr", name="debug_mgr", value="20/20")

        if not rados_obj.create_erasure_pool(
            name=ec_config["profile_name"], **ec_config
        ):
            log.error("Failed to create the EC Pool")
            return 1

        time.sleep(5)
        # Performing writes on pool
        if not rados_obj.bench_write(
            pool_name=pool_name, max_objs=500, verify_stats=True
        ):
            log.error("Could not perform IO operations")
            raise Exception("IO could not be completed")
        time.sleep(10)
        log.debug(
            "Completed creating & Writing objects into the EC pool: %s for testing",
            pool_name,
        )

        cmd = "ceph osd pool autoscale-status"
        pool_status = rados_obj.run_ceph_command(cmd=cmd)

        for entry in pool_status:
            if entry["pool_name"] == pool_name:
                if entry["pg_autoscale_mode"] == "off":
                    log.error(
                        "Pg autoscaler turned off for the new pool : %s "
                        "New pools should have autoscaler turned on by default",
                        entry["pool_name"],
                    )
                    return 1

        # Increasing the recovery threads on the cluster
        rados_obj.change_recovery_threads(config={}, action="set")

        if "scenario-1" in scenarios_to_run:
            log.info(
                "\nscenario-1: Test the effects of bulk flag and no IO stoppage - START\n"
            )
            init_pg_count = rados_obj.get_pool_property(pool=pool_name, props="pg_num")[
                "pg_num"
            ]
            log.debug("init PG count on the pool upon creation: %s", init_pg_count)

            bulk = pool_obj.get_bulk_details(pool_name=pool_name)
            if bulk:
                log.error("Expected bulk flag should be False upon pool creation")
                raise Exception("Expected bulk flag should be False.")

            log.debug(
                "Bulk flag not enabled on pool %s, Proceeding to enable bulk", pool_name
            )

            new_bulk = pool_obj.set_bulk_flag(pool_name=pool_name)
            if not new_bulk:
                log.error("Expected bulk flag should be True.")
                raise Exception("Expected bulk flag should be True.")
            # Sleeping for 60 seconds for bulk flag application and PG count to be increased.
            time.sleep(60)
            log.debug("Enabled bulk flag on the pool : %s", pool_name)
            pg_count_bulk_true = rados_obj.get_pool_details(pool=pool_name)[
                "pg_num_target"
            ]
            log.debug(
                "PG count on pool %s post addition of bulk flag : %s"
                "Starting to wait for PG count on the pool to go from %s to"
                " %s while checking for PG inactivity",
                pool_name,
                pg_count_bulk_true,
                init_pg_count,
                pg_count_bulk_true,
            )

            inactive_pg = 0
            endtime = datetime.datetime.now() + datetime.timedelta(seconds=14000)
            while datetime.datetime.now() < endtime:
                pool_pg_num = rados_obj.get_pool_property(
                    pool=pool_name, props="pg_num"
                )["pg_num"]
                if pool_pg_num == pg_count_bulk_true:
                    log.info(
                        "PG count on pool %s is achieved post adding the bulk flag",
                        pool_name,
                    )
                    break
                log.info(
                    "PG count on pool %s has not reached desired levels."
                    "Expected : %s, Current : %s",
                    pool_name,
                    pg_count_bulk_true,
                    pool_pg_num,
                )
                if not rados_obj.check_inactive_pgs_on_pool(pool_name=pool_name):
                    log.error("Inactive PGs found on pool : %s", pool_name)
                    inactive_pg += 1

                log.info("Sleeping for 60 secs and checking the PG states and PG count")
                time.sleep(60)
            else:
                raise Exception(
                    "pg_num on pool %s did not reach the desired levels of PG count "
                    "with bulk flag enabled"
                    "Expected : %s" % (pool_name, pg_count_bulk_true)
                )
            log.info(
                "PGs increased to desired levels after application of bulk flag on the pool with no inactive PGs"
            )

            if inactive_pg > 5:
                log.error(
                    "Found inactive PGs on the cluster multiple times during bulk flag addition on pool %s"
                    "Count %s",
                    pool_name,
                    inactive_pg,
                )
                raise Exception("Inactive PGs during bulk on error")
            log.info(
                "\nscenario-1: Test the effects of bulk flag and no IO stoppage - COMPLETE\n"
            )

        if "scenario-2" in scenarios_to_run:
            log.info(
                "\nscenario-2: Perform rolling reboot of all the OSDs on a particular host. - START\n"
            )
            log.info("Starting with OSD reboot scenarios for a Host")

            # Restarting OSDs belonging to a particular host
            osd_node = ceph_cluster.get_nodes(role="osd")[0]
            osd_list = rados_obj.collect_osd_daemon_ids(osd_node=osd_node)

            for osd_id in osd_list:
                log.debug("Rebooting OSD : %s and checking health status", osd_id)
                if not rados_obj.change_osd_state(action="restart", target=osd_id):
                    log.error("Unable to restart the OSD : %s", osd_id)
                    raise Exception("Execution error")

                time.sleep(5)
                # Waiting for recovery to post OSD reboot
                method_should_succeed(
                    wait_for_clean_pg_sets,
                    rados_obj,
                    timeout=12000,
                    test_pool=pool_name,
                )
                log.debug(
                    "PG's are active + clean post OSD reboot, proceeding to restart next OSD"
                )

            log.info(
                "All the planned  OSD reboots have completed for host %s",
                osd_node.hostname,
            )
            # Upgrade workflow and the checks moved to dedicated test, which will be called from suite file
            log.info(
                "\nscenario-2: Perform rolling reboot of all the OSDs on a particular host. - COMPLETE\n"
            )

        osd_nodes = ceph_cluster.get_nodes(role="osd")
        if "scenario-3" in scenarios_to_run:
            log.info("\nscenario-3: OSD operations - START\n")
            # Beginning with OSD stop operations
            log.debug("Stopping 1 OSD from each host. No inactive PGs")
            for node in osd_nodes:
                osd_list = rados_obj.collect_osd_daemon_ids(osd_node=node)
                log.debug(
                    "Reboot OSDs: Chosen host %s , Chosen OSD : %s",
                    node.hostname,
                    osd_list[0],
                )
                if not rados_obj.change_osd_state(action="stop", target=osd_list[0]):
                    log.error("Unable to stop the OSD : %s", osd_list[0])
                    raise Exception("Execution error")
                time.sleep(5)

                log.debug(
                    "Completed reboot of OSD %s. Checking for any inactive PGs due to reboot",
                    osd_list[0],
                )
                inactive_pgs = 0
                if not rados_obj.check_inactive_pgs_on_pool(pool_name=pool_name):
                    log.error("Inactive PGs found on pool : %s", pool_name)
                    inactive_pgs += 1
                time.sleep(5)
                # Waiting for recovery to post OSD reboot
                method_should_succeed(
                    wait_for_clean_pg_sets,
                    rados_obj,
                    timeout=12000,
                    test_pool=pool_name,
                )
                log.debug("PG's are active + clean post OSD reboot : %s", osd_list[0])
                if not rados_obj.change_osd_state(action="start", target=osd_list[0]):
                    log.error("Unable to stop the OSD : %s", osd_list[0])
                    raise Exception("Execution error")
                time.sleep(5)
                log.debug(
                    "OSD : %s Started.  proceeding to restart next OSD", osd_list[0]
                )

            if inactive_pgs > 5:
                log.error("Found inactive PGs on the cluster during OSD reboots")
                raise Exception("Inactive PGs during reboot error")
            log.debug("Completed scenario of rebooting 1 OSD from each host")
            log.info("\nscenario-3: OSD operations - COMPLETE\n")

        if "scenario-4" in scenarios_to_run:
            log.info("\nscenario-4: Stopping 1 OSD from each host - START\n")
            log.debug("Stopping all OSDs of 1 host and check for inactive PGs")
            # Stopping all OSDs of 1 host and check for inactive PGs
            stop_host = osd_nodes[0]
            osd_list = rados_obj.collect_osd_daemon_ids(osd_node=stop_host)
            inactive_pgs = 0
            for osd in osd_list:
                if not rados_obj.change_osd_state(action="stop", target=osd):
                    log.error("Unable to stop the OSD : %s", osd_list[0])
                    raise Exception("Unable to stop OSDs error")
                time.sleep(5)

            log.debug("Stopped all OSDs on host : %s", stop_host.hostname)
            if not rados_obj.check_inactive_pgs_on_pool(pool_name=pool_name):
                log.error("Inactive PGs found on pool : %s", pool_name)
                inactive_pgs += 1

            if inactive_pgs > 5:
                log.error("Found inactive PGs on the cluster during OSD stop")
                raise Exception("Inactive PGs during stop error")
            log.debug(
                "No inactive PGs found upon stopping OSDs on host : %s",
                stop_host.hostname,
            )

            for osd in osd_list:
                if not rados_obj.change_osd_state(action="start", target=osd):
                    log.error("Unable to start the OSD : %s", osd_list[0])
                    raise Exception("Unable to start OSDs error")
                time.sleep(5)

            log.debug("Completed restart of all the OSDs on the Host")
            log.info("\nscenario-4: Stopping 1 OSD from each host - COMPLETE\n")

        if "scenario-5" in scenarios_to_run:
            log.info("\nscenario-5: Stopping all OSDs of 1 host - START\n")
            log.debug("Starting to reboot all the OSD hosts and waiting till recovery")
            osd_nodes = ceph_cluster.get_nodes(role="osd")
            for node in osd_nodes:
                log.debug("Proceeding to reboot the host : %s", node.hostname)
                node.exec_command(cmd="reboot", sudo=True, check_ec=False)
                time.sleep(2)
                log.info(
                    "\nceph status : %s\n",
                    rados_obj.run_ceph_command(cmd="ceph -s", client_exec=True),
                )
                # Waiting for recovery to post OSD host addition
                method_should_succeed(wait_for_clean_pg_sets, rados_obj, timeout=12000)
                # Checking cluster health after OSD removal
                # fast EC has issues, where there are scrub errors reported.
                method_should_succeed(
                    rados_obj.run_pool_sanity_check,
                    ignore_list=warning_ignore_list,
                )
                log.info("reboot of OSD host : %s is successful.", node.hostname)
            log.debug("Done with reboot of all the OSD hosts")
            log.info("\nscenario-5: Stopping all OSDs of 1 host - COMPLETE\n")

        if "scenario-6" in scenarios_to_run:
            log.info("\nscenario-6: Add new host + OSDs to the cluster - START\n")
            log.debug("Starting test to add host into the cluster")
            try:
                node_id = ceph_cluster.get_nodes(role=new_node_label)[0]
            except Exception as err:
                log.error(
                    "Could not find the host for the Addition process with label 'osd-bak'. Err: %s",
                    err,
                )
                raise Exception("New Host not found for addition error")
            try:
                service_obj.add_new_hosts(
                    add_nodes=[node_id.hostname],
                    deploy_osd=True,
                    osd_label=new_node_label,
                )
            except Exception as err:
                log.error("Could not add the host in cluster. Err: %s", err)
                raise Exception("New Host addition error")

            log.debug("Waiting for clean PGs post New host & osd addition")
            # Waiting for recovery to post Host & OSD addition
            method_should_succeed(
                wait_for_clean_pg_sets,
                rados_obj,
                timeout=12000,
            )
            log.debug("PG's are active + clean post New Host & OSD Addition")
            log.info("\nscenario-6: Add new host + OSDs to the cluster - COMPLETE\n")

        if "scenario-7" in scenarios_to_run:
            log.info("\nscenario-7: Remove & Add 1 OSD from 1 host - START\n")
            log.debug("Starting test to remove OSD from the newly host.")
            # Remove one OSD
            inactive_pgs = 0
            try:
                node_id = ceph_cluster.get_nodes(role=new_node_label)[0]
            except Exception as err:
                log.error(
                    "Could not find the host for the removal process with label 'osd-bak'. Err: %s",
                    err,
                )
                raise Exception("Host not found error")
            target_osd = rados_obj.collect_osd_daemon_ids(osd_node=node_id)[0]
            log.debug("Target OSD for removal : %s", target_osd)
            host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=target_osd)
            log.debug(
                "Target Host from where OSD removal is scheduled: %s", host.hostname
            )
            should_not_be_empty(host, "Failed to fetch host details")
            dev_path = get_device_path(host, target_osd)
            target_osd_spec_name = service_obj.get_osd_spec(osd_id=target_osd)
            log_lines = (
                "\nosd device path  : %s,\n osd_id : %s,\n hostname : %s,\n"
                "Target OSD Spec : %s"
                % (dev_path, target_osd, host.hostname, target_osd_spec_name)
            )
            log.debug(log_lines)
            rados_obj.set_service_managed_type(service_type="osd", unmanaged=True)
            method_should_succeed(utils.set_osd_out, ceph_cluster, target_osd)
            method_should_succeed(wait_for_clean_pg_sets, rados_obj, timeout=12000)
            log.debug("Cluster clean post draining of OSD for removal")
            utils.osd_remove(ceph_cluster, target_osd)
            method_should_succeed(wait_for_clean_pg_sets, rados_obj, timeout=12000)
            method_should_succeed(
                utils.zap_device, ceph_cluster, host.hostname, dev_path
            )
            method_should_succeed(
                wait_for_device_rados, host, target_osd, action="remove"
            )

            # Waiting for recovery to post OSD host addition
            method_should_succeed(wait_for_clean_pg_sets, rados_obj, timeout=12000)
            # Checking cluster health after OSD removal
            method_should_succeed(
                rados_obj.run_pool_sanity_check,
                ignore_list=warning_ignore_list,
            )
            log.info(
                "Removal of OSD : %s is successful. Proceeding to add back the OSD daemon.",
                target_osd,
            )
            if not rados_obj.check_inactive_pgs_on_pool(pool_name=pool_name):
                log.error("Inactive PGs found on pool : %s", pool_name)
                inactive_pgs += 1

            # Adding the removed OSD back and checking the cluster status
            log.debug("Adding the removed OSD back and checking the cluster status")
            utils.add_osd(ceph_cluster, host.hostname, dev_path, target_osd)
            method_should_succeed(wait_for_device_rados, host, target_osd, action="add")
            method_should_succeed(
                wait_for_daemon_status,
                rados_obj=rados_obj,
                daemon_type="osd",
                daemon_id=target_osd,
                status="running",
                timeout=300,
            )
            assert service_obj.add_osds_to_managed_service(
                osds=[target_osd], spec=target_osd_spec_name
            )
            time.sleep(30)
            log.debug(
                "Completed addition of OSD post removal. Checking for inactive PGs post OSD addition"
            )
            if not rados_obj.check_inactive_pgs_on_pool(pool_name=pool_name):
                log.error("Inactive PGs found on pool : %s", pool_name)
                inactive_pgs += 1

            if inactive_pgs > 10:
                log.error(
                    "Found inactive PGs on the cluster during OSD removal/Addition"
                )
                raise Exception("Inactive PGs during stop error")

            # Checking cluster health after OSD removal
            method_should_succeed(
                rados_obj.run_pool_sanity_check,
                ignore_list=warning_ignore_list,
            )
            log.info(
                "Addition of OSD : %s back into the cluster was successful, and the health is good!",
                target_osd,
            )

            rados_obj.set_service_managed_type(service_type="osd", unmanaged=False)
            log.info("Completed the removal and addition of OSD daemons")
            log.info("\nscenario-7: Remove & Add 1 OSD from 1 host - COMPLETE\n")

        if "scenario-8" in scenarios_to_run:
            log.info("\nscenario-8: Remove 1 host from the cluster- START\n")
            # Remove one host
            log.info("Starting the removal of OSD Host added")
            node_id = ceph_cluster.get_nodes(role=new_node_label)[0]
            log.info("Starting the removal of OSD Host added : %s", node_id.hostname)
            try:
                service_obj.remove_custom_host(host_node_name=node_id.hostname)
            except Exception as err:
                log.error(
                    "Could not remove host : %s. Error : %s", node_id.hostname, err
                )
                raise Exception("Host not removed error")

            # Waiting for recovery to post OSD host removal
            res, inactive_count = wait_for_clean_pg_sets_check_inactive(
                rados_obj=rados_obj
            )
            if not res:
                log.error("PGs did not reach active + clean state post Host removal")
                test_fail = True
            if inactive_count > 5:
                log.error("Observed inactive PGs with OSD removal")
                test_fail = True

            method_should_succeed(
                rados_obj.run_pool_sanity_check,
                ignore_list=warning_ignore_list,
            )
            log.info("PG's are active + clean post OSD Host Addition and Removal")
            log.info("\nscenario-8: Remove 1 host from the cluster- COMPLETE\n")

        if "scenario-9" in scenarios_to_run:
            log.info(
                "\nscenario-9: Create EC pools with all supported profiles and"
                " plugins When PGs are scaling up and OSDs are moving In/OUT- START\n"
            )

            # Define EC profile configurations for different plugins and techniques
            ec_profiles_config = {
                "jerasure": {
                    "reed_sol_van": {
                        "plugin": "jerasure",
                        "technique": "reed_sol_van",
                        "k": 2,
                        "m": 2,
                        "bulk": True,
                    },
                    "reed_sol_r6_op": {
                        "plugin": "jerasure",
                        "technique": "reed_sol_r6_op",
                        "k": 2,
                        "m": 2,
                        "bulk": True,
                    },
                    "cauchy_orig": {
                        "plugin": "jerasure",
                        "technique": "cauchy_orig",
                        "k": 2,
                        "m": 2,
                        "bulk": True,
                    },
                    "cauchy_good": {
                        "plugin": "jerasure",
                        "technique": "cauchy_good",
                        "k": 2,
                        "m": 2,
                        "bulk": True,
                    },
                },
                "isa": {
                    "reed_sol_van": {
                        "plugin": "isa",
                        "technique": "reed_sol_van",
                        "k": 2,
                        "m": 2,
                        "bulk": True,
                    },
                    "cauchy": {
                        "plugin": "isa",
                        "technique": "cauchy",
                        "k": 2,
                        "m": 2,
                        "bulk": True,
                    },
                },
                "clay": {
                    "reed_sol_van": {
                        "plugin": "clay",
                        "technique": "reed_sol_van",
                        "k": 2,
                        "m": 2,
                        "d": 3,
                        "bulk": True,
                    },
                    "cauchy_orig": {
                        "plugin": "clay",
                        "technique": "cauchy_orig",
                        "k": 2,
                        "m": 2,
                        "d": 3,
                        "bulk": True,
                    },
                },
            }

            created_pools = []
            created_profiles = []

            try:
                # Get list of OSDs for in/out operations
                all_osds = rados_obj.get_osd_list(status="UP")

                log.info("Available OSDs for in/out operations: %s", all_osds)
                target_osds = all_osds[0]

                # Step 2: Create EC pools with various profiles while manipulating OSDs
                log.info(
                    "Step 2: Creating EC pools with various profiles while marking OSDs in/out"
                )

                for plugin_name, techniques in ec_profiles_config.items():
                    log.info(
                        "Testing %s plugin with OSD manipulation", plugin_name.upper()
                    )

                    for technique_name, config in techniques.items():
                        profile_name = "test-s10-%s-%s-profile" % (
                            plugin_name,
                            technique_name,
                        )
                        pool_name_ec = "test-s10-%s-%s-pool" % (
                            plugin_name,
                            technique_name,
                        )

                        log.debug(
                            "Creating EC profile: %s with config: %s",
                            profile_name,
                            config,
                        )

                        try:
                            rados_obj.update_osd_state_on_cluster(
                                state="out", osd_id=target_osds
                            )

                            # Create EC profile and pool
                            if not rados_obj.create_erasure_pool(
                                name=profile_name, pool_name=pool_name_ec, **config
                            ):
                                log.error(
                                    "Failed to create EC profile/pool: %s/%s",
                                    profile_name,
                                    pool_name_ec,
                                )
                                test_fail = True
                                continue

                            created_profiles.append(profile_name)
                            created_pools.append(pool_name_ec)
                            log.info(
                                "Successfully created EC profile: %s and pool: %s",
                                profile_name,
                                pool_name_ec,
                            )

                            log.debug("Writing test data to pool: %s", pool_name_ec)
                            if not rados_obj.bench_write(
                                pool_name=pool_name_ec,
                                max_objs=1000,
                                verify_stats=True,
                            ):
                                log.warning(
                                    "Failed to write data to pool: %s", pool_name_ec
                                )
                                test_fail = True
                            else:
                                log.info(
                                    "Successfully wrote test data to pool: %s",
                                    pool_name_ec,
                                )

                            rados_obj.update_osd_state_on_cluster(
                                state="in", osd_id=target_osds
                            )

                        except Exception as profile_err:
                            log.warning(
                                "Failed to create/test profile %s with technique %s: %s",
                                profile_name,
                                technique_name,
                                profile_err,
                            )
                            rados_obj.update_osd_state_on_cluster(
                                state="in", osd_id=target_osds
                            )
                            test_fail = True

                log.info(
                    "Successfully created and tested %s EC pools with OSD manipulation",
                    len(created_pools),
                )
                # Wait for PGs to stabilize
                method_should_succeed(
                    wait_for_clean_pg_sets,
                    rados_obj,
                    timeout=1200,
                )

                # Step 3: Cleanup created pools and profiles
                log.info("Step 3: Starting cleanup of created EC pools and profiles")
                method_should_succeed(
                    rados_obj.run_pool_sanity_check,
                    ignore_list=warning_ignore_list,
                )

                for pool_name_cleanup in created_pools:
                    try:
                        if not rados_obj.delete_pool(pool=pool_name_cleanup):
                            log.warning("Failed to delete pool: %s", pool_name_cleanup)
                        else:
                            log.debug(
                                "Successfully deleted pool: %s", pool_name_cleanup
                            )
                    except Exception as cleanup_err:
                        log.warning(
                            "Error during pool cleanup %s: %s",
                            pool_name_cleanup,
                            cleanup_err,
                        )

            except Exception as scenario_err:
                log.error("Error in scenario-9 execution: %s", scenario_err)

                # Cleanup pools and profiles
                for pool_name_cleanup in created_pools:
                    rados_obj.delete_pool(pool=pool_name_cleanup)

            # Wait for cluster to stabilize
            method_should_succeed(
                wait_for_clean_pg_sets,
                rados_obj,
                timeout=1200,
            )
            log.info(
                "\nscenario-9: Create EC pools with all supported profiles and"
                " plugins When PGs are scaling up and OSDs are moving In/OUT - COMPLETE\n"
            )

    except Exception as err:
        log.error("Hit exception during execution of test. Exception : %s", err)
        return 1

    finally:
        log.info("---------------IN FINALLY-----------------------")
        rados_obj.rados_pool_cleanup()

        # removing the recovery threads on the cluster
        rados_obj.change_recovery_threads(config={}, action="rm")
        # remove empty service specs after host removal
        rados_obj.remove_empty_service_spec()

        if set_debug:
            log.debug("Removing debug configs on the cluster for mon, osd & Mgr")
            mon_obj.remove_config(section="osd", name="debug_osd")
            mon_obj.remove_config(section="mon", name="debug_mon")
            mon_obj.remove_config(section="mgr", name="debug_mgr")

        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1

    if not test_fail:
        log.info("EC 2+2 pool is working as expected.")
        return 0
    else:
        log.error("EC 2+2 pool tests failed")
        return 1


def wait_for_clean_pg_sets_check_inactive(
    rados_obj: RadosOrchestrator, timeout=12000, _sleep=60
) -> (bool, int):
    """
    Waiting for up to 2.5 hours for the PG's to enter active + Clean state while checking for any inactive PGs
    during the workflow, reporting the count of times inactive PGs were seen on the cluster
    Automation for bug : [1] & [2]
    Args:
        rados_obj: RadosOrchestrator object to run commands
        timeout: timeout in seconds or "unlimited"
        _sleep: sleep timeout in seconds (default: 120)

    Returns:  a tuple, consisting of method status and the inactive PG count
        True -> pass, False -> fail

    """
    end_time = 0
    inactive_count = 0
    end_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
    while end_time > datetime.datetime.now():
        flag = True
        status_report = rados_obj.run_ceph_command(cmd="ceph report", client_exec=True)

        # Proceeding to check if all PG's are in active + clean
        for entry in status_report["num_pg_by_state"]:
            rec = (
                "remapped",
                "backfilling",
                "peering",
                "recovering",
                "recovery_wait",
                "backfilling_wait",
            )
            if any(key in rec for key in entry["state"].split("+")):
                flag = False
            if "unknown" in [key for key in entry["state"].split("+")]:
                inactive_count += 1
                log.debug("Observed inactive PGs : %s", entry["state"])

        if flag:
            log.info("The recovery and back-filling of the OSD is completed")
            return True, inactive_count
        log.info(
            "Waiting for active + clean. Active alerts: %s,"
            "PG States : %s"
            " checking status again in %s seconds",
            status_report["health"]["checks"].keys(),
            status_report["num_pg_by_state"],
            _sleep,
        )
        time.sleep(_sleep)

    log.error("The cluster did not reach active + Clean state")
    return False, inactive_count
