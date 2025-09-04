"""
test Module to :
1. Create a 2+2 ec pool
2. fill the pool
3. Test the effects of bulk flag and no IO stoppage
4. rolling reboot of OSDs of a host
5. OSD start and stop for failure domain level
6. Serviceability tests
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
    fast_ec_enabled = ec_config.get("enable_fast_ec_features")

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
            f"Completed creating & Writing objects into the EC pool: {pool_name} for testing"
        )

        cmd = "ceph osd pool autoscale-status"
        pool_status = rados_obj.run_ceph_command(cmd=cmd)

        for entry in pool_status:
            if entry["pool_name"] == pool_name:
                if entry["pg_autoscale_mode"] == "off":
                    log.error(
                        f"Pg autoscaler turned off for the new pool : {entry['pool_name']} "
                        f"New pools should have autoscaler turned on by default"
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
            log.debug(f"init PG count on the pool upon creation: {init_pg_count}")

            bulk = pool_obj.get_bulk_details(pool_name=pool_name)
            if bulk:
                log.error("Expected bulk flag should be False upon pool creation")
                raise Exception("Expected bulk flag should be False.")

            log.debug(
                f"Bulk flag not enabled on pool {pool_name}, Proceeding to enable bulk"
            )

            new_bulk = pool_obj.set_bulk_flag(pool_name=pool_name)
            if not new_bulk:
                log.error("Expected bulk flag should be True.")
                raise Exception("Expected bulk flag should be True.")
            # Sleeping for 60 seconds for bulk flag application and PG count to be increased.
            time.sleep(60)
            log.debug(f"Enabled bulk flag on the pool : {pool_name}")
            pg_count_bulk_true = rados_obj.get_pool_details(pool=pool_name)[
                "pg_num_target"
            ]
            log.debug(
                f"PG count on pool {pool_name} post addition of bulk flag : {pg_count_bulk_true}"
                f"Starting to wait for PG count on the pool to go from {init_pg_count} to"
                f" {pg_count_bulk_true} while checking for PG inactivity"
            )

            inactive_pg = 0
            endtime = datetime.datetime.now() + datetime.timedelta(seconds=14000)
            while datetime.datetime.now() < endtime:
                pool_pg_num = rados_obj.get_pool_property(
                    pool=pool_name, props="pg_num"
                )["pg_num"]
                if pool_pg_num == pg_count_bulk_true:
                    log.info(
                        f"PG count on pool {pool_name} is achieved post adding the bulk flag"
                    )
                    break
                log.info(
                    f"PG count on pool {pool_name} has not reached desired levels."
                    f"Expected : {pg_count_bulk_true}, Current : {pool_pg_num}"
                )
                if not rados_obj.check_inactive_pgs_on_pool(pool_name=pool_name):
                    log.error(f"Inactive PGs found on pool : {pool_name}")
                    inactive_pg += 1

                log.info("Sleeping for 60 secs and checking the PG states and PG count")
                time.sleep(60)
            else:
                raise Exception(
                    f"pg_num on pool {pool_name} did not reach the desired levels of PG count "
                    f"with bulk flag enabled"
                    f"Expected : {pg_count_bulk_true}"
                )
            log.info(
                "PGs increased to desired levels after application of bulk flag on the pool with no inactive PGs"
            )

            if inactive_pg > 5:
                log.error(
                    f"Found inactive PGs on the cluster multiple times during bulk flag addition on pool {pool_name}"
                    f"Count {inactive_pg}"
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
                log.debug(f"Rebooting OSD : {osd_id} and checking health status")
                if not rados_obj.change_osd_state(action="restart", target=osd_id):
                    log.error(f"Unable to restart the OSD : {osd_id}")
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
                f"All the planned  OSD reboots have completed for host {osd_node.hostname}"
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
                    f"Reboot OSDs: Chosen host {node.hostname} , Chosen OSD : {osd_list[0]}"
                )
                if not rados_obj.change_osd_state(action="stop", target=osd_list[0]):
                    log.error(f"Unable to stop the OSD : {osd_list[0]}")
                    raise Exception("Execution error")
                time.sleep(5)

                log.debug(
                    f"Completed reboot of OSD {osd_list[0]}. Checking for any inactive PGs due to reboot"
                )
                inactive_pgs = 0
                if not rados_obj.check_inactive_pgs_on_pool(pool_name=pool_name):
                    log.error(f"Inactive PGs found on pool : {pool_name}")
                    inactive_pgs += 1
                time.sleep(5)
                # Waiting for recovery to post OSD reboot
                method_should_succeed(
                    wait_for_clean_pg_sets,
                    rados_obj,
                    timeout=12000,
                    test_pool=pool_name,
                )
                log.debug(f"PG's are active + clean post OSD reboot : {osd_list[0]}")
                if not rados_obj.change_osd_state(action="start", target=osd_list[0]):
                    log.error(f"Unable to stop the OSD : {osd_list[0]}")
                    raise Exception("Execution error")
                time.sleep(5)
                log.debug(
                    f"OSD : {osd_list[0]} Started.  proceeding to restart next OSD"
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
                    log.error(f"Unable to stop the OSD : {osd_list[0]}")
                    raise Exception("Unable to stop OSDs error")
                time.sleep(5)

            log.debug(f"Stopped all OSDs on host : {stop_host.hostname}")
            if not rados_obj.check_inactive_pgs_on_pool(pool_name=pool_name):
                log.error(f"Inactive PGs found on pool : {pool_name}")
                inactive_pgs += 1

            if inactive_pgs > 5:
                log.error("Found inactive PGs on the cluster during OSD stop")
                raise Exception("Inactive PGs during stop error")
            log.debug(
                f"No inactive PGs found upon stopping OSDs on host : {stop_host.hostname}"
            )

            for osd in osd_list:
                if not rados_obj.change_osd_state(action="start", target=osd):
                    log.error(f"Unable to start the OSD : {osd_list[0]}")
                    raise Exception("Unable to start OSDs error")
                time.sleep(5)

            log.debug("Completed restart of all the OSDs on the Host")
            log.info("\nscenario-4: Stopping 1 OSD from each host - COMPLETE\n")

        if "scenario-5" in scenarios_to_run:
            log.info("\nscenario-5: Stopping all OSDs of 1 host - START\n")
            log.debug("Starting to reboot all the OSD hosts and waiting till recovery")
            osd_nodes = ceph_cluster.get_nodes(role="osd")
            for node in osd_nodes:
                log.debug(f"Proceeding to reboot the host : {node.hostname}")
                node.exec_command(cmd="reboot", sudo=True, check_ec=False)
                time.sleep(2)
                log.info(
                    f"\nceph status : {rados_obj.run_ceph_command(cmd='ceph -s', client_exec=True)}\n"
                )
                # Waiting for recovery to post OSD host addition
                method_should_succeed(wait_for_clean_pg_sets, rados_obj, timeout=12000)
                # Checking cluster health after OSD removal
                # fast EC has issues, where there are scrub errors reported.
                method_should_succeed(
                    rados_obj.run_pool_sanity_check,
                    ignore_list=(
                        ["OSD_SCRUB_ERRORS", "PG_DAMAGED"] if fast_ec_enabled else []
                    ),
                )
                log.info(f"reboot of OSD host : {node.hostname} is successful.")
            log.debug("Done with reboot of all the OSD hosts")
            log.info("\nscenario-5: Stopping all OSDs of 1 host - COMPLETE\n")

        if "scenario-6" in scenarios_to_run:
            log.info("\nscenario-6: Add new host + OSDs to the cluster - START\n")
            log.debug("Starting test to add host into the cluster")
            try:
                node_id = ceph_cluster.get_nodes(role=new_node_label)[0]
            except Exception as err:
                log.error(
                    f"Could not find the host for the Addition process with label 'osd-bak'. Err: {err}"
                )
                raise Exception("New Host not found for addition error")
            try:
                service_obj.add_new_hosts(
                    add_nodes=[node_id.hostname],
                    deploy_osd=True,
                    osd_label=new_node_label,
                )
            except Exception as err:
                log.error(f"Could not add the host in cluster. Err: {err}")
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
                    f"Could not find the host for the removal process with label 'osd-bak'. Err: {err}"
                )
                raise Exception("Host not found error")
            target_osd = rados_obj.collect_osd_daemon_ids(osd_node=node_id)[0]
            log.debug(f"Target OSD for removal : {target_osd}")
            host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=target_osd)
            log.debug(
                f"Target Host from where OSD removal is scheduled: {host.hostname}"
            )
            should_not_be_empty(host, "Failed to fetch host details")
            dev_path = get_device_path(host, target_osd)
            target_osd_spec_name = service_obj.get_osd_spec(osd_id=target_osd)
            log_lines = (
                f"\nosd device path  : {dev_path},\n osd_id : {target_osd},\n hostname : {host.hostname},\n"
                f"Target OSD Spec : {target_osd_spec_name}"
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
                ignore_list=(
                    ["OSD_SCRUB_ERRORS", "PG_DAMAGED"] if fast_ec_enabled else []
                ),
            )
            log.info(
                f"Removal of OSD : {target_osd} is successful. Proceeding to add back the OSD daemon."
            )
            if not rados_obj.check_inactive_pgs_on_pool(pool_name=pool_name):
                log.error(f"Inactive PGs found on pool : {pool_name}")
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
                timeout=60,
            )
            assert service_obj.add_osds_to_managed_service(
                osds=[target_osd], spec=target_osd_spec_name
            )
            time.sleep(30)
            log.debug(
                "Completed addition of OSD post removal. Checking for inactive PGs post OSD addition"
            )
            if not rados_obj.check_inactive_pgs_on_pool(pool_name=pool_name):
                log.error(f"Inactive PGs found on pool : {pool_name}")
                inactive_pgs += 1

            if inactive_pgs > 10:
                log.error(
                    "Found inactive PGs on the cluster during OSD removal/Addition"
                )
                raise Exception("Inactive PGs during stop error")

            # Checking cluster health after OSD removal
            method_should_succeed(
                rados_obj.run_pool_sanity_check,
                ignore_list=(
                    ["OSD_SCRUB_ERRORS", "PG_DAMAGED"] if fast_ec_enabled else []
                ),
            )
            log.info(
                f"Addition of OSD : {target_osd} back into the cluster was successful, and the health is good!"
            )

            rados_obj.set_service_managed_type(service_type="osd", unmanaged=False)
            log.info("Completed the removal and addition of OSD daemons")
            log.info("\nscenario-7: Remove & Add 1 OSD from 1 host - COMPLETE\n")

        if "scenario-8" in scenarios_to_run:
            log.info("\nscenario-8: Remove 1 host from the cluster- START\n")
            # Remove one host
            log.info("Starting the removal of OSD Host added")
            node_id = ceph_cluster.get_nodes(role=new_node_label)[0]
            log.info(f"Starting the removal of OSD Host added : {node_id.hostname}")
            try:
                service_obj.remove_custom_host(host_node_name=node_id.hostname)
            except Exception as err:
                log.error(f"Could not remove host : {node_id.hostname}. Error : {err}")
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
                ignore_list=(
                    ["OSD_SCRUB_ERRORS", "PG_DAMAGED"] if fast_ec_enabled else []
                ),
            )
            log.info("PG's are active + clean post OSD Host Addition and Removal")
            log.info("\nscenario-8: Remove 1 host from the cluster- COMPLETE\n")

    except Exception as err:
        log.error(f"Hit exception during execution of test. Exception : {err}")
        return 1

    finally:
        log.info("---------------IN FINALLY-----------------------")
        if not rados_obj.delete_pool(pool=pool_name):
            log.error(f"the pool {pool_name} could not be deleted")

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
                log.debug(f"Observed inactive PGs : {entry['state']}")

        if flag:
            log.info("The recovery and back-filling of the OSD is completed")
            return True, inactive_count
        log.info(
            f"Waiting for active + clean. Active alerts: {status_report['health']['checks'].keys()},"
            f"PG States : {status_report['num_pg_by_state']}"
            f" checking status again in {_sleep} seconds"
        )
        time.sleep(_sleep)

    log.error("The cluster did not reach active + Clean state")
    return False, inactive_count
