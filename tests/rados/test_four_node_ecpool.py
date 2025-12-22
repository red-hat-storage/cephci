"""
test Module to :

    Scenarios:
        scenario-1: Test the effects of bulk flag and no IO stoppage
        scenario-2: Perform rolling reboot of all the OSDs on a particular host.
        scenario-3: OSD operations
        scenario-4: Stopping all OSDs of 1 host
        scenario-5: Reboot all OSD hosts
        scenario-6: Add new host and OSDs into the cluster
        scenario-7: Remove 1 OSD from 1 host
        scenario-8: Remove 1 host from the cluster
        scenario-9: Combined workflow - Create PG splits with bulk flag and run EC pool
                    creation while marking OSDs in/out
        scenario-10: Acting set based OSD failure tests with IO verification
        scenario-11: Object copy and read pattern verification
        scenario-12: Reboot same host during recovery/backfill
        scenario-13: Reboot different host during recovery/backfill

    TRY BLOCK test execution workflow:
        ├── Crash archive at start
        ├── SETUP PHASE
        │   ├── EC pool creation
        │   ├── Rados bench writes (5000 objects)
        │   ├── Parallel tasks (overwrites, appends, truncates, xattr, sizes, verification)
        │   └── RBD image + 18+ snapshots + clone
        ├── PRE-SCENARIO CHECKS (via run_pre_post_scenario_checks)
        │   └── 8 checks: health log, scrub, deep-scrub, PG clean,
        │       pool sanity, object count, data integrity, crash check
        ├── SCENARIOS (1-13) execution
        ├── POST-SCENARIO CHECKS (via run_pre_post_scenario_checks)
        │   └── Same 8 checks as pre-scenario

        EXCEPT BLOCK:
        └── return 1 on exception

        FINALLY BLOCK (Cleanup):
        ├── RBD unmount
        ├── RBD unmap
        ├── RBD clone removal
        ├── RBD snapshot unprotect + purge
        ├── Metadata pool deletion
        ├── EC pool cleanup
        ├── Recovery thread removal
        └── Debug config removal

        RETURN:
        └── 0 if test_fail=False, else 1

"""

import datetime
import json
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Tuple

from ceph.ceph_admin import CephAdmin
from ceph.rados import utils
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from ceph.rados.serviceability_workflows import ServiceabilityMethods
from ceph.rados.utils import get_cluster_timestamp
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


    1. Create a 2+2 ec pool
    2. Fill the pool with comprehensive data (parallel execution where possible):
       - rados bench writes (5000 objects) - sequential, runs first
       - [PARALLEL] Direct RADOS writes with partial overwrites (50 objects)
       - [PARALLEL] RADOS append operations (50 objects)
       - [PARALLEL] RADOS truncate operations (50 objects)
       - [PARALLEL] Objects with extended attributes (xattr) - 50 objects
       - [PARALLEL] Comprehensive test objects with various sizes, operations, and MD5 checksums
         * Creates objects from 0 bytes to 4MB
         * Performs overwrite, append, truncate on objects >= stripe_unit
         * Stores MD5 checksums as xattr for data integrity verification
       - RBD image creation with writes (dd + FIO), snapshots, and clone - sequential
         * Creates 18+ RBD snapshots (after each write pattern for comprehensive testing)
         * Creates RBD clone from snapshot and flattens it (tests COW on EC pool)
         * Keeps all snapshots and flattened clone for recovery testing
    3. Pre-scenario checks (8 checks via run_pre_post_scenario_checks):
       - Cluster health logging
       - Scrub and deep-scrub on pool
       - Wait for PGs to be active+clean
       - Pool sanity check
       - Object count verification
       - Data integrity verification
       - Crash status check
    4. Test scenarios:
       - scenario-1: Test the effects of bulk flag and no IO stoppage
       - scenario-2: Perform rolling reboot of all the OSDs on a particular host
       - scenario-3: OSD operations (stop/start 1 OSD from each host)
       - scenario-4: Stopping all OSDs of 1 host
       - scenario-5: Reboot all OSD hosts
       - scenario-6: Add new host and OSDs into the cluster
       - scenario-7: Remove 1 OSD from 1 host and add back
       - scenario-8: Remove 1 host from the cluster
       - scenario-9: Create EC pools with all supported profiles while OSDs moving in/out
       - scenario-10: Acting set based OSD failure tests with IO verification
         * Stop M-1 OSDs from acting set (IOs should succeed)
         * Stop M OSDs (cluster degraded)
         * Start all OSDs and wait for recovery
       - scenario-11: Object copy and read pattern verification
       - scenario-12: Reboot same host during recovery/backfill
         * Reboot host, wait for recovery to start, reboot same host again
         * Verify recovery completes after double reboot
       - scenario-13: Reboot different host during recovery/backfill
         * Reboot Host A, wait for recovery to start, reboot Host B
         * Verify recovery completes after cascading host reboots
    5. Post-scenario checks (same 8 checks via run_pre_post_scenario_checks):
       - Cluster health logging
       - Scrub and deep-scrub on pool
       - Wait for PGs to be active+clean
       - Pool sanity check
       - Object count verification
       - Data integrity verification
       - Crash status check

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
            "scenario-10",
            "scenario-11",
            "scenario-12",
            "scenario-13",
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

    log.debug(
        "Completed setting of the global configs need to run 2+2 tests. Proceeding further"
    )

    # Creating the EC pool
    ec_config = config.get("ec_pool")
    pool_name = ec_config["pool_name"]
    # Due to below tracker, suppressing few heath warnings when fast ec is enabled on pools.
    # Bugzillas fixed. No need to suppress warnings anymore.
    # bugzilla : https://bugzilla.redhat.com/show_bug.cgi?id=2400427
    warning_ignore_list = []

    # Setup phase configuration
    stripe_unit = ec_config.get("stripe_unit", 16384)  # Default 16KB
    ec_k = ec_config.get("k", 2)
    stripe_width = ec_k * stripe_unit
    metadata_pool_name = config.get("metadata_pool_name", f"{pool_name}_metadata")
    rbd_image_name = config.get("rbd_image_name", f"{pool_name}_rbd_image")
    image_size = config.get("image_size", "1G")
    run_extended_setup = config.get("run_extended_setup", True)

    # Track RBD resources for cleanup
    device_path = None
    mount_path = None
    metadata_pool_created = False
    start_time = get_cluster_timestamp(rados_obj.node)
    log.debug(f"Test workflow started. Start time: {start_time}")
    try:
        # Archive any pre-existing crashes at the very beginning
        # This ensures we start with a clean slate for crash detection
        log.info("Archiving any pre-existing crashes before test starts...")
        rados_obj.run_ceph_command(cmd="ceph crash archive-all", client_exec=True)
        time.sleep(2)
        log.info("Pre-existing crashes archived, Starting workflows")

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

        # =================================================================
        # SETUP PHASE: Fill pool with comprehensive data patterns
        # =================================================================
        log.info("=" * 70)
        log.info("SETUP PHASE: Filling EC pool with comprehensive data patterns")
        log.info("=" * 70)

        log.info("Install FIO on client nodes")
        client_nodes = ceph_cluster.get_nodes(role="client")
        cmd = "yum install fio -y"
        for node in client_nodes:
            node.exec_command(cmd=cmd, sudo=True)

        # Step 1: Performing rados bench writes on pool
        log.info("Setup Step 1: Running rados bench writes (5000 objects)...")
        if not rados_obj.bench_write(
            pool_name=pool_name, max_objs=5000, verify_stats=False
        ):
            log.error("Could not perform IO operations")
            raise Exception("IO could not be completed")
        log.info("Rados bench writes completed successfully")
        time.sleep(5)

        if run_extended_setup:
            # =============================================================
            # PARALLEL EXECUTION PHASE: Run independent tasks concurrently
            # =============================================================
            log.info("-" * 70)
            log.info("Running parallel setup tasks for EC pool data population...")
            log.info("-" * 70)

            # Define parallel tasks - these write to different objects and can run concurrently
            parallel_tasks = {
                "overwrites": lambda: run_rados_overwrites(
                    rados_obj=rados_obj,
                    pool_name=pool_name,
                    stripe_unit=stripe_unit,
                    stripe_width=stripe_width,
                    num_objects=50,
                ),
                "appends": lambda: run_rados_appends(
                    rados_obj=rados_obj,
                    pool_name=pool_name,
                    stripe_unit=stripe_unit,
                    num_objects=50,
                ),
                "truncates": lambda: run_rados_truncates(
                    rados_obj=rados_obj,
                    pool_name=pool_name,
                    stripe_unit=stripe_unit,
                    num_objects=50,
                ),
                "xattr_objects": lambda: create_objects_with_xattr(
                    rados_obj=rados_obj,
                    pool_name=pool_name,
                    num_objects=50,
                ),
                "various_sizes": lambda: create_comprehensive_test_objects(
                    rados_obj=rados_obj,
                    pool_name=pool_name,
                    stripe_unit=stripe_unit,
                ),
            }

            # Execute parallel tasks
            parallel_results = {}
            failed_tasks = []
            with ThreadPoolExecutor(max_workers=10) as executor:
                future_to_task = {
                    executor.submit(task_func): task_name
                    for task_name, task_func in parallel_tasks.items()
                }

                for future in as_completed(future_to_task):
                    task_name = future_to_task[future]
                    try:
                        result = future.result()
                        parallel_results[task_name] = result
                        if result:
                            log.info(
                                "Parallel task '%s' completed successfully", task_name
                            )
                        else:
                            log.error("Parallel task '%s' returned failure", task_name)
                            failed_tasks.append(task_name)
                    except Exception as task_err:
                        log.error(
                            "Parallel task '%s' failed with exception: %s",
                            task_name,
                            task_err,
                        )
                        parallel_results[task_name] = False
                        failed_tasks.append(task_name)

            log.info("-" * 70)
            log.info("Parallel setup tasks completed. Results: %s", parallel_results)
            log.info("-" * 70)

            # Fail test if any parallel task failed
            if failed_tasks:
                log.error("Failed parallel tasks: %s", failed_tasks)
                raise Exception(f"Parallel setup tasks failed: {failed_tasks}")

            # =============================================================
            # SEQUENTIAL PHASE: RBD image creation
            # =============================================================

            # Create metadata pool for RBD
            log.info("Creating metadata pool for RBD...")
            metadata_pool_created = rados_obj.create_pool(
                pool_name=metadata_pool_name, app_name="rbd"
            )
            if not metadata_pool_created:
                log.error("Failed to create metadata pool")
                raise Exception("Failed to create metadata pool")
            else:
                log.info("Metadata pool created: %s", metadata_pool_name)

                # Create RBD image and write data
                log.info("Creating RBD image and writing data & snapshots...")
                rbd_result = create_rbd_and_write_data(
                    rados_obj=rados_obj,
                    ec_pool_name=pool_name,
                    metadata_pool_name=metadata_pool_name,
                    image_name=rbd_image_name,
                    image_size=image_size,
                    stripe_unit=stripe_unit,
                )
                if rbd_result:
                    device_path, mount_path = rbd_result
                    log.info(
                        "RBD image created, data written, and snapshots created successfully"
                    )
                else:
                    log.error("RBD creation encountered issues, failing test")
                    raise Exception("RBD creation encountered issues")

        log.info("=" * 70)
        log.info("SETUP PHASE COMPLETE: EC pool filled with comprehensive data")
        log.info("=" * 70)
        time.sleep(10)

        # =================================================================
        # PRE-SCENARIO CHECKS: Verify cluster health before scenarios
        # =================================================================
        pre_check_passed, _ = run_pre_post_scenario_checks(
            rados_obj=rados_obj,
            pool_name=pool_name,
            phase="pre",
            run_extended_setup=run_extended_setup,
            warning_ignore_list=warning_ignore_list,
        )
        if not pre_check_passed:
            raise Exception("Pre-scenario checks failed - cannot proceed with test")
        time.sleep(5)

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
                        "PG autoscaler turned off for the new pool: %s. "
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

            # Set pg_num_max to limit how high bulk flag can increase PG count
            # Calculate the next power of 2 greater than current PG count
            # This prevents bulk from scaling PGs to a very high value (saves time)
            pg_num_max = 1 << init_pg_count.bit_length()
            log.info(
                "Setting pg_num_max to %s (next power of 2 after %s) to limit bulk scaling",
                pg_num_max,
                init_pg_count,
            )
            rados_obj.set_pool_property(
                pool=pool_name, props="pg_num_max", value=pg_num_max
            )
            log.debug("pg_num_max set to %s on pool %s", pg_num_max, pool_name)
            time.sleep(2)

            new_bulk = pool_obj.set_bulk_flag(pool_name=pool_name)
            if not new_bulk:
                log.error("Expected bulk flag should be True.")
                raise Exception("Expected bulk flag should be True.")
            # Sleeping for 60 seconds for bulk flag application and PG count to be increased.
            time.sleep(60)
            log.debug("Enabled bulk flag on pool %s", pool_name)
            pg_count_bulk_true = rados_obj.get_pool_details(pool=pool_name)[
                "pg_num_target"
            ]
            log.debug(
                "PG count on pool %s post addition of bulk flag: %s. "
                "Starting to wait for PG count on the pool to go from %s to %s "
                "while checking for PG inactivity",
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
                    "PG count on pool %s has not reached desired levels. "
                    "Expected: %s, Current: %s",
                    pool_name,
                    pg_count_bulk_true,
                    pool_pg_num,
                )
                if not rados_obj.check_inactive_pgs_on_pool(pool_name=pool_name):
                    log.error("Inactive PGs found on pool %s", pool_name)
                    inactive_pg += 1

                log.info("Sleeping for 60 secs and checking the PG states and PG count")
                time.sleep(60)
            else:
                raise Exception(
                    "pg_num on pool %s did not reach the desired levels of PG count "
                    "with bulk flag enabled. Expected: %s"
                    % (pool_name, pg_count_bulk_true)
                )
            log.info(
                "PGs increased to desired levels after application of bulk flag on the pool with no inactive PGs"
            )

            if inactive_pg > 5:
                log.error(
                    "Found inactive PGs on the cluster multiple times during bulk flag "
                    "addition on pool %s. Count: %s",
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

            # running background IOs for 10 minutes and the objects of this run will be deleted at the end
            # This is so that there are constant IOs happening in background during operations
            rados_obj.bench_write(
                pool_name=pool_name,
                background=True,
                rados_write_duration=600,
                verify_stats=False,
                nocleanup=False,
            )

            # Restarting OSDs belonging to a particular host
            osd_nodes_s2 = ceph_cluster.get_nodes(role="osd")
            if not osd_nodes_s2:
                log.error("No OSD nodes found for scenario-2")
                raise Exception("No OSD nodes available")
            osd_node = osd_nodes_s2[0]
            osd_list = rados_obj.collect_osd_daemon_ids(osd_node=osd_node)
            if not osd_list:
                log.error("No OSDs found on host %s for scenario-2", osd_node.hostname)
                raise Exception("No OSDs found on target host")

            for osd_id in osd_list:
                log.debug("Rebooting OSD %s and checking health status", osd_id)
                if not rados_obj.change_osd_state(action="restart", target=osd_id):
                    log.error("Unable to restart OSD %s", osd_id)
                    raise Exception("Execution error")

                log.info("Starting background IO after OSD reboot...")
                rados_obj.bench_write(
                    pool_name=pool_name,
                    background=True,
                    rados_write_duration=100,
                    max_objs=5000,
                    verify_stats=False,
                )

                # Waiting for recovery to post OSD reboot
                method_should_succeed(
                    wait_for_clean_pg_sets,
                    rados_obj,
                    timeout=3000,
                    test_pool=pool_name,
                )
                log.debug(
                    "PGs are active+clean post OSD reboot, proceeding to restart next OSD"
                )

            log.info(
                "All planned OSD reboots completed for host %s",
                osd_node.hostname,
            )
            # Upgrade workflow and the checks moved to dedicated test, which will be called from suite file
            log.info(
                "\nscenario-2: Perform rolling reboot of all the OSDs on a particular host. - COMPLETE\n"
            )

        osd_nodes = ceph_cluster.get_nodes(role="osd")
        if "scenario-3" in scenarios_to_run:
            log.info("\nscenario-3: OSD operations - START\n")

            if not osd_nodes:
                log.warning("Skipping scenario-3: No OSD nodes found")
                log.info("\nscenario-3: OSD operations - SKIPPED\n")
            else:
                # setting config mon_osd_down_out_interval so that OSDs are marked out at 2 minutes
                mon_obj.set_config(
                    section="global",
                    name="mon_osd_down_out_interval",
                    value="120",
                )
                log.info("Config mon_osd_down_out_interval set to 120 seconds")

                # running background IOs for 10 minutes and the objects of this run will be deleted at the end
                # This is so that there are constant IOs happening in background during operations
                rados_obj.bench_write(
                    pool_name=pool_name,
                    background=True,
                    rados_write_duration=600,
                    verify_stats=False,
                    nocleanup=False,
                )

                # Beginning with OSD stop operations
                log.debug("Stopping 1 OSD from each host. No inactive PGs")
                inactive_pgs = 0  # Initialize before loop
                for node in osd_nodes:
                    osd_list = rados_obj.collect_osd_daemon_ids(osd_node=node)
                    if not osd_list:
                        log.warning("No OSDs found on host %s, skipping", node.hostname)
                        continue
                    log.debug(
                        "Stop/Start OSDs: Chosen host %s, Chosen OSD %s",
                        node.hostname,
                        osd_list[0],
                    )
                    if not rados_obj.change_osd_state(
                        action="stop", target=osd_list[0]
                    ):
                        log.error("Unable to stop OSD %s", osd_list[0])
                        raise Exception("Execution error")
                    time.sleep(5)
                    log.info("Starting background IO after OSD stop...")
                    rados_obj.bench_write(
                        pool_name=pool_name,
                        background=True,
                        rados_write_duration=200,
                        max_objs=5000,
                        verify_stats=False,
                    )
                    log.debug(
                        "Completed stop of OSD %s. Checking for any inactive PGs",
                        osd_list[0],
                    )
                    if not rados_obj.check_inactive_pgs_on_pool(pool_name=pool_name):
                        log.error("Inactive PGs found on pool %s", pool_name)
                        inactive_pgs += 1
                    time.sleep(5)
                    # Waiting for recovery to post OSD stop
                    method_should_succeed(
                        wait_for_clean_pg_sets,
                        rados_obj,
                        timeout=3000,
                        test_pool=pool_name,
                    )
                    log.debug("PGs are active+clean post OSD stop: %s", osd_list[0])
                    if not rados_obj.change_osd_state(
                        action="start", target=osd_list[0]
                    ):
                        log.error("Unable to start OSD %s", osd_list[0])
                        raise Exception("Execution error")
                    time.sleep(5)
                    log.debug("OSD %s started, proceeding to next OSD", osd_list[0])
                    log.info(
                        "Starting background IO after OSD start and"
                        " waiting for PGs to be active + clean before next OSD power cycle..."
                    )
                    rados_obj.bench_write(
                        pool_name=pool_name,
                        background=True,
                        rados_write_duration=200,
                        max_objs=5000,
                        verify_stats=False,
                    )
                    # Waiting for recovery to post OSD start
                    method_should_succeed(
                        wait_for_clean_pg_sets,
                        rados_obj,
                        timeout=3000,
                        test_pool=pool_name,
                    )
                if inactive_pgs > 5:
                    log.error("Found inactive PGs on the cluster during OSD reboots")
                    raise Exception("Inactive PGs during reboot error")
                log.debug("Completed scenario of stop/start 1 OSD from each host")

                mon_obj.remove_config(
                    section="global",
                    name="mon_osd_down_out_interval",
                )
                log.debug("Config mon_osd_down_out_interval removed")
                log.info("\nscenario-3: OSD operations - COMPLETE\n")

        if "scenario-4" in scenarios_to_run:
            log.info("\nscenario-4: Stopping all OSDs of 1 host - START\n")

            if not osd_nodes:
                log.warning("Skipping scenario-4: No OSD nodes found")
                log.info("\nscenario-4: Stopping all OSDs of 1 host - SKIPPED\n")
            else:
                log.debug("Stopping all OSDs of 1 host and check for inactive PGs")

                # running background IOs for 10 minutes and the objects of this run will be deleted at the end
                # This is so that there are constant IOs happening in background during operations
                rados_obj.bench_write(
                    pool_name=pool_name,
                    background=True,
                    rados_write_duration=600,
                    verify_stats=False,
                    nocleanup=False,
                )

                stop_host = osd_nodes[0]
                osd_list = rados_obj.collect_osd_daemon_ids(osd_node=stop_host)
                if not osd_list:
                    log.error(
                        "No OSDs found on host %s for scenario-4", stop_host.hostname
                    )
                    raise Exception("No OSDs found on target host")

                for osd in osd_list:
                    if not rados_obj.change_osd_state(action="stop", target=osd):
                        log.error("Unable to stop OSD %s", osd)
                        raise Exception("Unable to stop OSDs error")
                    time.sleep(5)

                log.debug("Stopped all OSDs on host %s", stop_host.hostname)
                if not rados_obj.check_inactive_pgs_on_pool(pool_name=pool_name):
                    log.error("Inactive PGs found on pool: %s", pool_name)
                    raise Exception("Inactive PGs during OSD stop error")
                log.debug(
                    "No inactive PGs found upon stopping OSDs on host %s",
                    stop_host.hostname,
                )
                log.info(
                    "Starting background IO after all OSDs from 1 host are stopped..."
                )
                rados_obj.bench_write(
                    pool_name=pool_name,
                    rados_write_duration=300,
                    max_objs=5000,
                    verify_stats=True,
                )

                for osd in osd_list:
                    if not rados_obj.change_osd_state(action="start", target=osd):
                        log.error("Unable to start OSD %s", osd)
                        raise Exception("Unable to start OSDs error")
                    time.sleep(5)

                log.debug("Completed restart of all the OSDs on the Host")

                # Wait for PGs to be active+clean after starting all OSDs
                method_should_succeed(
                    wait_for_clean_pg_sets,
                    rados_obj,
                    timeout=3000,
                    test_pool=pool_name,
                )
                log.debug("PGs are active+clean after restarting all OSDs on host")
                log.info("\nscenario-4: Stopping all OSDs of 1 host - COMPLETE\n")

        if "scenario-5" in scenarios_to_run:
            log.info("\nscenario-5: Reboot all OSD hosts - START\n")

            log.debug(
                "Starting rolling reboot of all the OSD hosts and waiting till recovery"
            )
            # Re-fetch osd_nodes to ensure we have current list
            osd_nodes_s5 = ceph_cluster.get_nodes(role="osd")
            if not osd_nodes_s5:
                log.warning("Skipping scenario-5: No OSD nodes found")
                log.info("\nscenario-5: rolling Reboot all OSD hosts - SKIPPED\n")
            else:
                # running background IOs for 10 minutes and the objects of this run will be deleted at the end
                # This is so that there are constant IOs happening in background during operations
                rados_obj.bench_write(
                    pool_name=pool_name,
                    background=True,
                    rados_write_duration=600,
                    verify_stats=False,
                    nocleanup=False,
                )
                for node in osd_nodes_s5:
                    log.debug("Proceeding to reboot host %s", node.hostname)
                    node.exec_command(cmd="reboot", sudo=True, check_ec=False)
                    time.sleep(2)
                    log.info(
                        "\nceph status : %s\n",
                        rados_obj.run_ceph_command(cmd="ceph -s", client_exec=True),
                    )
                    log.info("Starting background IO after OSD host reboot...")
                    rados_obj.bench_write(
                        pool_name=pool_name,
                        background=True,
                        rados_write_duration=1200,
                        max_objs=5000,
                        verify_stats=False,
                    )
                    # Waiting for recovery post OSD host reboot
                    method_should_succeed(
                        wait_for_clean_pg_sets, rados_obj, timeout=3000
                    )
                    # Checking cluster health after host reboot
                    method_should_succeed(
                        rados_obj.run_pool_sanity_check,
                        ignore_list=warning_ignore_list,
                    )
                    log.info(
                        "Reboot of OSD host %s completed successfully", node.hostname
                    )
                log.debug("Done with reboot of all the OSD hosts")
                log.info("\nscenario-5: Reboot all OSD hosts - COMPLETE\n")

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
                # running background IOs for 5 minutes and the objects of this run will be deleted at the end
                # This is so that there are constant IOs happening in background during operations
                rados_obj.bench_write(
                    pool_name=pool_name,
                    background=True,
                    rados_write_duration=300,
                    verify_stats=False,
                    nocleanup=False,
                )

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
                timeout=1800,
            )
            log.debug("PGs are active+clean post new host and OSD addition")
            log.info("\nscenario-6: Add new host + OSDs to the cluster - COMPLETE\n")

        if "scenario-7" in scenarios_to_run:
            log.info("\nscenario-7: Remove & Add 1 OSD from 1 host - START\n")
            log.debug("Starting test to remove OSD from the new host.")
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
            osd_list = rados_obj.collect_osd_daemon_ids(osd_node=node_id)
            if not osd_list:
                log.error("No OSDs found on host %s", node_id.hostname)
                raise Exception("No OSDs found on target host")
            target_osd = osd_list[0]
            log.debug("Target OSD for removal: %s", target_osd)
            host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=target_osd)
            log.debug(
                "Target Host from where OSD removal is scheduled: %s", host.hostname
            )

            # running background IOs for 5 minutes and the objects of this run will be deleted at the end
            # This is so that there are constant IOs happening in background during operations
            rados_obj.bench_write(
                pool_name=pool_name,
                background=True,
                rados_write_duration=300,
                verify_stats=False,
                nocleanup=False,
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
            method_should_succeed(wait_for_clean_pg_sets, rados_obj, timeout=1800)
            log.debug("Cluster clean post draining of OSD for removal")
            utils.osd_remove(ceph_cluster, target_osd)
            method_should_succeed(wait_for_clean_pg_sets, rados_obj, timeout=1800)
            method_should_succeed(
                utils.zap_device, ceph_cluster, host.hostname, dev_path
            )
            method_should_succeed(
                wait_for_device_rados, host, target_osd, action="remove"
            )

            # Waiting for recovery post OSD removal
            method_should_succeed(wait_for_clean_pg_sets, rados_obj, timeout=1800)
            # Checking cluster health after OSD removal
            method_should_succeed(
                rados_obj.run_pool_sanity_check,
                ignore_list=warning_ignore_list,
            )
            rados_obj.bench_write(
                pool_name=pool_name,
                background=True,
                rados_write_duration=1200,
                max_objs=5000,
                verify_stats=False,
            )
            log.info(
                "Removal of OSD %s successful. Proceeding to add back the OSD daemon.",
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
            if not service_obj.add_osds_to_managed_service(
                osds=[target_osd], spec=target_osd_spec_name
            ):
                log.error("Failed to add OSD %s to managed service", target_osd)
                raise Exception(f"Failed to add OSD {target_osd} to managed service")
            time.sleep(30)
            log.debug(
                "Completed addition of OSD post removal. Checking for inactive PGs post OSD addition"
            )

            # Waiting for recovery post OSD addition
            method_should_succeed(wait_for_clean_pg_sets, rados_obj, timeout=1800)

            if not rados_obj.check_inactive_pgs_on_pool(pool_name=pool_name):
                log.error("Inactive PGs found on pool : %s", pool_name)
                inactive_pgs += 1

            if inactive_pgs > 10:
                log.error(
                    "Found inactive PGs on the cluster during OSD removal/addition"
                )
                raise Exception("Inactive PGs during OSD removal/addition error")

            # Checking cluster health after OSD removal
            method_should_succeed(
                rados_obj.run_pool_sanity_check,
                ignore_list=warning_ignore_list,
            )
            log.info(
                "Addition of OSD %s back into the cluster was successful",
                target_osd,
            )

            rados_obj.set_service_managed_type(service_type="osd", unmanaged=False)
            log.info("Completed the removal and addition of OSD daemons")
            log.info("\nscenario-7: Remove & Add 1 OSD from 1 host - COMPLETE\n")

        if "scenario-8" in scenarios_to_run:
            log.info("\nscenario-8: Remove 1 host from the cluster- START\n")
            # Remove one host
            log.info("Starting the removal of OSD Host added")
            try:
                node_id = ceph_cluster.get_nodes(role=new_node_label)[0]
            except Exception as err:
                log.error(
                    "Could not find the host for removal with label '%s'. "
                    "Ensure scenario-6 was run first. Err: %s",
                    new_node_label,
                    err,
                )
                raise Exception("Host not found for removal - run scenario-6 first")

            # running background IOs for 10 minutes and the objects of this run will be deleted at the end
            # This is so that there are constant IOs happening in background during operations
            rados_obj.bench_write(
                pool_name=pool_name,
                background=True,
                rados_write_duration=600,
                verify_stats=False,
                nocleanup=False,
            )
            log.info("Starting the removal of OSD Host: %s", node_id.hostname)
            try:
                service_obj.remove_custom_host(host_node_name=node_id.hostname)
            except Exception as err:
                log.error("Could not remove host %s: %s", node_id.hostname, err)
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
            log.info("PGs are active + clean post OSD Host Removal")
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
                        "pg_num_max": 128,
                    },
                    "reed_sol_r6_op": {
                        "plugin": "jerasure",
                        "technique": "reed_sol_r6_op",
                        "k": 2,
                        "m": 2,
                        "bulk": True,
                        "pg_num_max": 128,
                    },
                    "cauchy_orig": {
                        "plugin": "jerasure",
                        "technique": "cauchy_orig",
                        "k": 2,
                        "m": 2,
                        "bulk": True,
                        "pg_num_max": 128,
                    },
                    "cauchy_good": {
                        "plugin": "jerasure",
                        "technique": "cauchy_good",
                        "k": 2,
                        "m": 2,
                        "bulk": True,
                        "pg_num_max": 128,
                    },
                },
                "isa": {
                    "reed_sol_van": {
                        "plugin": "isa",
                        "technique": "reed_sol_van",
                        "k": 2,
                        "m": 2,
                        "bulk": True,
                        "pg_num_max": 128,
                    },
                    "cauchy": {
                        "plugin": "isa",
                        "technique": "cauchy",
                        "k": 2,
                        "m": 2,
                        "bulk": True,
                        "pg_num_max": 128,
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
                        "pg_num_max": 128,
                    },
                    "cauchy_orig": {
                        "plugin": "clay",
                        "technique": "cauchy_orig",
                        "k": 2,
                        "m": 2,
                        "d": 3,
                        "bulk": True,
                        "pg_num_max": 128,
                    },
                },
            }

            created_pools = []

            try:
                # Get list of OSDs for in/out operations
                all_osds = rados_obj.get_osd_list(status="UP")
                if not all_osds:
                    log.error("No UP OSDs found for scenario-9")
                    raise Exception("No OSDs available")

                log.info("Available OSDs for in/out operations: %s", all_osds)
                target_osd = all_osds[0]  # Single OSD for in/out operations

                # Create EC pools with various profiles while manipulating OSDs
                log.info(
                    "Creating EC pools with various profiles while marking OSDs in/out"
                )

                for plugin_name, techniques in ec_profiles_config.items():
                    log.info(
                        "Testing %s plugin with OSD manipulation", plugin_name.upper()
                    )

                    for technique_name, ec_params in techniques.items():
                        profile_name = f"test-s9-{plugin_name}-{technique_name}-profile"
                        pool_name_ec = f"test-s9-{plugin_name}-{technique_name}-pool"

                        log.debug(
                            "Creating EC profile: %s with params: %s",
                            profile_name,
                            ec_params,
                        )

                        try:
                            rados_obj.update_osd_state_on_cluster(
                                state="out", osd_id=target_osd
                            )

                            # Create EC profile and pool
                            if not rados_obj.create_erasure_pool(
                                name=profile_name, pool_name=pool_name_ec, **ec_params
                            ):
                                log.error(
                                    "Failed to create EC profile/pool: %s/%s",
                                    profile_name,
                                    pool_name_ec,
                                )
                                test_fail = True
                                continue

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
                                log.error(
                                    "Failed to write data to pool: %s", pool_name_ec
                                )
                                test_fail = True
                            else:
                                log.info(
                                    "Successfully wrote test data to pool: %s",
                                    pool_name_ec,
                                )
                            # Waiting for recovery post OSD OUT + pool creation
                            method_should_succeed(
                                wait_for_clean_pg_sets, rados_obj, timeout=3000
                            )

                            rados_obj.update_osd_state_on_cluster(
                                state="in", osd_id=target_osd
                            )

                            # Waiting for recovery post OSD IN + pool creation
                            method_should_succeed(
                                wait_for_clean_pg_sets, rados_obj, timeout=3000
                            )

                        except Exception as profile_err:
                            log.error(
                                "Failed to create/test profile %s with technique %s: %s",
                                profile_name,
                                technique_name,
                                profile_err,
                            )
                            rados_obj.update_osd_state_on_cluster(
                                state="in", osd_id=target_osd
                            )
                            test_fail = True

                log.info(
                    "Successfully created and tested %s EC pools with OSD manipulation",
                    len(created_pools),
                )
                # Cleanup created pools and profiles
                log.info("Starting cleanup of created EC pools and profiles")
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
                test_fail = True

                # Cleanup pools and profiles
                for pool_name_cleanup in created_pools:
                    rados_obj.delete_pool(pool=pool_name_cleanup)

            log.info(
                "\nscenario-9: Create EC pools with all supported profiles and"
                " plugins When PGs are scaling up and OSDs are moving In/OUT - COMPLETE\n"
            )

        if "scenario-10" in scenarios_to_run:
            log.info(
                "\nscenario-10: Acting set based OSD failure tests with IO verification - START\n"
            )
            ec_m = ec_config.get("m", 2)

            # This scenario requires m >= 2 for meaningful M-1 OSD failure testing
            if ec_m < 2:
                log.warning(
                    "Skipping scenario-10: requires m >= 2 for acting set failure tests (current m=%s)",
                    ec_m,
                )
                log.info(
                    "\nscenario-10: Acting set based OSD failure tests - SKIPPED (m < 2)\n"
                )
            else:
                # Get the acting set for a PG in the pool
                log.info("Fetching acting set for pool %s", pool_name)
                acting_set = rados_obj.get_pg_acting_set(pool_name=pool_name)
                log.info("Acting set for pool %s: %s", pool_name, acting_set)

                # running background IOs for 10 minutes and the objects of this run will be deleted at the end
                # This is so that there are constant IOs happening in background during operations
                rados_obj.bench_write(
                    pool_name=pool_name,
                    background=True,
                    rados_write_duration=600,
                    verify_stats=False,
                    nocleanup=False,
                )

                # setting noout flags so that the acting set OSDs are not marked out during test workflow
                rados_obj.client.exec_command(cmd="ceph osd set noout", sudo=True)

                # Test 1: Stop M-1 OSDs from acting set - IOs should still succeed
                log.info(
                    "Test 1: Stopping %s OSDs (M-1) from acting set - IOs should succeed",
                    ec_m - 1,
                )
                stopped_osds = []
                log.info(
                    "Starting background writes for scenario-10 before stopping OSDs..."
                )
                rados_obj.bench_write(
                    pool_name=pool_name,
                    background=True,
                    rados_write_duration=600,
                    max_objs=5000,
                    verify_stats=False,
                )
                for i in range(ec_m - 1):
                    osd_id = acting_set[i]
                    log.debug("Stopping OSD.%s from acting set", osd_id)
                    if not rados_obj.change_osd_state(action="stop", target=osd_id):
                        log.error("Failed to stop OSD.%s", osd_id)
                        raise Exception(f"Failed to stop OSD.{osd_id}")
                    stopped_osds.append(osd_id)
                    time.sleep(5)

                log.info("Stopped %s OSDs: %s", len(stopped_osds), stopped_osds)

                # Start background reads to verify reads work with M-1 OSDs down
                log.info("Starting background reads with M-1 OSDs down...")
                rados_obj.bench_read(
                    pool_name=pool_name,
                    background=True,
                    rados_read_duration=120,
                )
                rados_obj.bench_write(
                    pool_name=pool_name,
                    background=True,
                    rados_write_duration=600,
                    max_objs=5000,
                    verify_stats=False,
                )
                log.info("Background writes and reads running with M-1 OSDs down")

                # Test 2: Stop one more OSD (total M OSDs down) - cluster should be degraded
                log.info(
                    "Test 2: Stopping 1 more OSD (total %s OSDs / M OSDs down) - cluster degraded",
                    ec_m,
                )
                additional_osd = acting_set[ec_m - 1]
                log.debug("Stopping additional OSD.%s from acting set", additional_osd)
                if not rados_obj.change_osd_state(action="stop", target=additional_osd):
                    log.error("Failed to stop OSD.%s", additional_osd)
                    test_fail = True
                else:
                    stopped_osds.append(additional_osd)
                    log.info(
                        "Stopped additional OSD.%s - total %s OSDs down",
                        additional_osd,
                        len(stopped_osds),
                    )

                time.sleep(5)

                # Log cluster state with M OSDs down
                log.info("Cluster state with %s OSDs down:", len(stopped_osds))
                rados_obj.log_cluster_health()

                # un-setting noout flag so that the acting set OSDs are marked out and PG recovers
                rados_obj.client.exec_command(cmd="ceph osd unset noout", sudo=True)
                time.sleep(10)

                # Wait for PGs to become active+clean
                method_should_succeed(
                    wait_for_clean_pg_sets,
                    rados_obj,
                    timeout=1800,
                    test_pool=pool_name,
                )
                log.info("PGs are active+clean after stopping M OSDs")
                rados_obj.bench_write(
                    pool_name=pool_name,
                    background=True,
                    rados_write_duration=600,
                    max_objs=5000,
                    verify_stats=True,
                )

                # Test 3: Start all stopped OSDs and wait for recovery
                log.info(
                    "Test 3: Starting all stopped OSDs and waiting for recovery..."
                )
                for osd_id in stopped_osds:
                    log.debug("Starting OSD.%s", osd_id)
                    if not rados_obj.change_osd_state(action="start", target=osd_id):
                        log.error("Failed to start OSD.%s", osd_id)
                        test_fail = True
                    time.sleep(5)

                log.info("All stopped OSDs started. Waiting for recovery...")

                # Wait for PGs to become active+clean
                method_should_succeed(
                    wait_for_clean_pg_sets,
                    rados_obj,
                    timeout=1800,
                    test_pool=pool_name,
                )
                log.info("PGs are active+clean after recovery")
                log.info(
                    "\nscenario-10: Acting set based OSD failure tests with IO verification - COMPLETE\n"
                )

        if "scenario-11" in scenarios_to_run:
            log.info(
                "\nscenario-11: Object copy and read pattern verification - START\n"
            )

            # This scenario requires objects created by create_comprehensive_test_objects()
            # which only runs when run_extended_setup is True
            if not run_extended_setup:
                log.warning(
                    "Skipping scenario-11: requires run_extended_setup=True for test objects"
                )
                log.info(
                    "\nscenario-11: Object copy and read pattern verification - SKIPPED\n"
                )
            else:
                # Test object copy operations using objects from create_comprehensive_test_objects()
                log.info("Testing object copy operations on various-sized objects...")

                # Objects created by create_comprehensive_test_objects() with different sizes
                # Copy all 7 objects to test various size patterns
                objects_to_copy = [
                    "ec_size_small_obj_4k",
                    "ec_size_exact_chunk_obj",
                    "ec_size_chunk_plus_half",
                    "ec_size_two_chunks_obj",
                    "ec_size_three_chunks_obj",
                    "ec_size_four_chunks_obj",
                    "ec_size_large_obj_1m",
                ]
                log.info(
                    "Copying %s various-sized objects for copy test",
                    len(objects_to_copy),
                )

                copied_objects = []
                copy_success = 0
                copy_fail = 0

                for src_obj in objects_to_copy:
                    try:
                        dst_obj = f"copy_{src_obj}_{int(time.time())}"

                        # Get source object stats before copy
                        src_stat = rados_obj.get_object_stat(
                            pool_name=pool_name, obj_name=src_obj
                        )
                        if not src_stat:
                            log.error("Failed to get source object stat: %s", src_obj)
                            copy_fail += 1
                            continue

                        # Copy object within same pool
                        cmd = f"rados -p {pool_name} cp {src_obj} {dst_obj}"
                        rados_obj.client.exec_command(cmd=cmd, sudo=True)
                        log.info("Object copy successful: %s -> %s", src_obj, dst_obj)
                        copied_objects.append(dst_obj)

                        # Get copied object stats and verify size matches source
                        dst_stat = rados_obj.get_object_stat(
                            pool_name=pool_name, obj_name=dst_obj
                        )
                        if not dst_stat:
                            log.error("Failed to get copied object stat: %s", dst_obj)
                            copy_fail += 1
                            continue

                        # Verify source and copied object sizes match
                        if src_stat["size"] != dst_stat["size"]:
                            log.error(
                                "Size mismatch after copy: %s (src: %s bytes) -> "
                                "%s (dst: %s bytes)",
                                src_obj,
                                src_stat["size"],
                                dst_obj,
                                dst_stat["size"],
                            )
                            copy_fail += 1
                            continue

                        log.debug(
                            "Object copy verified: %s -> %s (size: %s bytes)",
                            src_obj,
                            dst_obj,
                            dst_stat["size"],
                        )

                        # Test read on copied object
                        read_file = f"/tmp/read_{dst_obj}.bin"
                        cmd = f"rados -p {pool_name} get {dst_obj} {read_file}"
                        rados_obj.client.exec_command(cmd=cmd, sudo=True)
                        log.debug("Read operation completed for %s", dst_obj)

                        # Cleanup temp read file
                        rados_obj.client.exec_command(
                            cmd=f"rm -f {read_file}", sudo=True, check_ec=False
                        )

                        copy_success += 1

                    except Exception as copy_err:
                        log.error(
                            "Failed to copy/read object %s: %s", src_obj, copy_err
                        )
                        copy_fail += 1

                log.info(
                    "Object copy results: %s succeeded, %s failed out of %s",
                    copy_success,
                    copy_fail,
                    len(objects_to_copy),
                )

                if copy_fail > 0:
                    log.error("Some object copy operations failed")
                    test_fail = True
                log.info(
                    "\nscenario-11: Object copy and read pattern verification - COMPLETE\n"
                )

        if "scenario-12" in scenarios_to_run:
            log.info(
                "\nscenario-12: Reboot same host during recovery/backfill - START\n"
            )
            # Test: Host reboot -> recovery starts -> reboot SAME host -> verify recovery

            osd_nodes = ceph_cluster.get_nodes(role="osd")
            if not osd_nodes:
                log.warning("Skipping scenario-12: No OSD hosts found")
                log.info(
                    "\nscenario-12: Reboot same host during recovery/backfill - SKIPPED\n"
                )
            else:
                target_host = osd_nodes[0]
                log.info("Target host for scenario-12: %s", target_host.hostname)
                # running background IOs for 10 minutes and the objects of this run will be deleted at the end
                # This is so that there are constant IOs happening in background during operations
                rados_obj.bench_write(
                    pool_name=pool_name,
                    background=True,
                    rados_write_duration=600,
                    verify_stats=False,
                    nocleanup=False,
                )
                try:
                    # First reboot
                    log.info("Step 1-3: First reboot cycle...")
                    reboot_host_and_wait(target_host, rados_obj)

                    log.info("Starting background IO before second reboot...")
                    rados_obj.bench_write(
                        pool_name=pool_name,
                        background=True,
                        rados_write_duration=600,
                        max_objs=5000,
                        verify_stats=False,
                    )

                    # Second reboot of SAME host during recovery
                    log.info("Step 4-5: Second reboot of same host during recovery...")
                    reboot_host_and_wait(target_host, rados_obj)

                    log.info("Starting background IO after second reboot...")
                    rados_obj.bench_write(
                        pool_name=pool_name,
                        background=True,
                        rados_write_duration=600,
                        max_objs=5000,
                        verify_stats=False,
                    )

                    # Wait for recovery to complete
                    log.info("Step 6: Waiting for PGs to be active+clean...")
                    method_should_succeed(
                        wait_for_clean_pg_sets,
                        rados_obj,
                        timeout=1800,
                        test_pool=pool_name,
                    )
                    log.info("PGs are active+clean after double reboot of same host")
                    log.info("Scenario-12 completed successfully")

                except Exception as scenario_err:
                    log.error("Scenario-12 failed: %s", scenario_err)
                    test_fail = True
                    try:
                        wait_for_clean_pg_sets(rados_obj, timeout=1800)
                    except Exception:
                        log.error(
                            "Could not stabilize cluster after scenario-12 failure"
                        )

                log.info(
                    "\nscenario-12: Reboot same host during recovery/backfill - COMPLETE\n"
                )

        if "scenario-13" in scenarios_to_run:
            log.info(
                "\nscenario-13: Reboot different host during recovery/backfill - START\n"
            )
            # Test: Host A reboot -> recovery starts -> reboot Host B -> verify recovery
            osd_nodes = ceph_cluster.get_nodes(role="osd")
            if len(osd_nodes) < 2:
                log.warning("Skipping scenario-13: requires at least 2 OSD hosts")
                log.info(
                    "\nscenario-13: Reboot different host during recovery/backfill - SKIPPED\n"
                )
            else:
                # running background IOs for 10 minutes and the objects of this run will be deleted at the end
                # This is so that there are constant IOs happening in background during operations
                rados_obj.bench_write(
                    pool_name=pool_name,
                    background=True,
                    rados_write_duration=600,
                    verify_stats=False,
                    nocleanup=False,
                )
                host_a, host_b = osd_nodes[0], osd_nodes[1]
                log.info(
                    "Target hosts: Host A=%s, Host B=%s",
                    host_a.hostname,
                    host_b.hostname,
                )

                try:
                    rados_obj.bench_write(
                        pool_name=pool_name,
                        background=True,
                        rados_write_duration=600,
                        max_objs=5000,
                        verify_stats=False,
                    )

                    # Reboot Host A
                    log.info("Step 1-3: Reboot Host A...")
                    reboot_host_and_wait(host_a, rados_obj)
                    log.info("Starting background IO after Host A reboot...")
                    rados_obj.bench_write(
                        pool_name=pool_name,
                        background=True,
                        rados_write_duration=600,
                        max_objs=5000,
                        verify_stats=False,
                    )
                    # Reboot Host B during recovery
                    log.info("Step 4-5: Reboot Host B during recovery...")
                    reboot_host_and_wait(host_b, rados_obj)
                    log.info("Starting background IO after Host B reboot...")
                    rados_obj.bench_write(
                        pool_name=pool_name,
                        background=True,
                        rados_write_duration=600,
                        max_objs=5000,
                        verify_stats=False,
                    )
                    # Wait for recovery to complete
                    log.info("Step 6: Waiting for PGs to be active+clean...")
                    method_should_succeed(
                        wait_for_clean_pg_sets,
                        rados_obj,
                        timeout=3000,
                        test_pool=pool_name,
                    )
                    log.info("PGs are active+clean after cascading host reboots")
                    log.info("Scenario-13 completed successfully")

                except Exception as scenario_err:
                    log.error("Scenario-13 failed: %s", scenario_err)
                    test_fail = True
                log.info(
                    "\nscenario-13: Reboot different host during recovery/backfill - COMPLETE\n"
                )

        # =================================================================
        # POST-SCENARIO CHECKS: Verify cluster health after all scenarios
        # (Runs at end of try block - only if scenarios complete without exception)
        # =================================================================
        post_check_passed, _ = run_pre_post_scenario_checks(
            rados_obj=rados_obj,
            pool_name=pool_name,
            phase="post",
            run_extended_setup=run_extended_setup,
            warning_ignore_list=warning_ignore_list,
        )
        if not post_check_passed:
            test_fail = True

    except Exception as err:
        log.error("Hit exception during test execution: %s", err)
        return 1

    finally:
        # =================================================================
        # CLEANUP : Delete pools and resources
        # =================================================================
        log.info("---------------IN FINALLY BLOCK: CLEANUP-----------------------")
        log.info("Starting cleanup...")
        # Remove any leftover config
        try:
            mon_obj.remove_config(
                section="global",
                name="mon_osd_down_out_interval",
            )
        except Exception:
            pass  # Config may not exist, ignore

        # Cleanup RBD resources if created
        try:
            if mount_path:
                clean_mount_path = mount_path.rstrip("/")
                rados_obj.client.exec_command(
                    cmd=f"umount {clean_mount_path}", sudo=True, check_ec=False
                )
                rados_obj.client.exec_command(
                    cmd=f"rm -rf {clean_mount_path}", sudo=True, check_ec=False
                )
                log.debug("Unmounted and cleaned up %s", clean_mount_path)
        except Exception as e:
            log.warning("Failed to unmount: %s", e)

        try:
            if device_path:
                rados_obj.client.exec_command(
                    cmd=f"rbd unmap {device_path}", sudo=True, check_ec=False
                )
                log.debug("Unmapped RBD device %s", device_path)
        except Exception as e:
            log.warning("Failed to unmap RBD device: %s", e)

        # Cleanup RBD clones
        try:
            if metadata_pool_created and rbd_image_name:
                clone_name = f"{rbd_image_name}_clone"
                # Remove clone image if it exists
                cmd = f"rbd rm {metadata_pool_name}/{clone_name}"
                rados_obj.client.exec_command(cmd=cmd, sudo=True, check_ec=False)
                log.debug("Removed RBD clone: %s", clone_name)
        except Exception as e:
            log.debug("Clone cleanup (may not exist): %s", e)

        # Cleanup RBD snapshots
        try:
            if metadata_pool_created and rbd_image_name:
                cmd = f"rbd snap ls {metadata_pool_name}/{rbd_image_name} --format json"
                snap_out, _ = rados_obj.client.exec_command(
                    cmd=cmd, sudo=True, check_ec=False
                )
                if snap_out and snap_out.strip() and snap_out.strip() != "[]":
                    try:
                        snaps = json.loads(snap_out.strip())
                        for snap in snaps:
                            snap_name = snap.get("name", "")
                            if snap_name and snap.get("protected", "") == "true":
                                cmd = f"rbd snap unprotect {metadata_pool_name}/{rbd_image_name}@{snap_name}"
                                rados_obj.client.exec_command(
                                    cmd=cmd, sudo=True, check_ec=False
                                )
                    except json.JSONDecodeError:
                        pass
                    # Purge RBD snapshots
                    cmd = f"rbd snap purge {metadata_pool_name}/{rbd_image_name}"
                    rados_obj.client.exec_command(cmd=cmd, sudo=True, check_ec=False)
                    log.debug("Purged RBD snapshots for %s", rbd_image_name)
        except Exception as e:
            log.warning("Failed to cleanup RBD snapshots: %s", e)

        # Delete metadata pool
        try:
            if metadata_pool_created:
                rados_obj.delete_pool(pool=metadata_pool_name)
                log.debug("Deleted metadata pool: %s", metadata_pool_name)
        except Exception as e:
            log.warning("Failed to delete metadata pool: %s", e)

        rados_obj.rados_pool_cleanup()

        # removing the recovery threads
        rados_obj.change_recovery_threads(config={}, action="rm")

        if set_debug:
            log.debug("Removing debug configs on the cluster for mon, osd & Mgr")
            mon_obj.remove_config(section="osd", name="debug_osd")
            mon_obj.remove_config(section="mon", name="debug_mon")
            mon_obj.remove_config(section="mgr", name="debug_mgr")

        log.info("Cleanup complete")
        test_end_time = get_cluster_timestamp(rados_obj.node)
        log.debug(
            f"Test workflow completed. Start time: {start_time}, End time: {test_end_time}"
        )
        if rados_obj.check_crash_status(start_time=start_time, end_time=test_end_time):
            log.error("Test failed due to crash at the end of test")
            return 1

    if not test_fail:
        log.info("EC 2+2 pool is working as expected.")
        return 0
    else:
        log.error("EC 2+2 pool tests failed")
        return 1


def run_pre_post_scenario_checks(
    rados_obj,
    pool_name: str,
    phase: str,
    run_extended_setup: bool = True,
    warning_ignore_list: list = None,
) -> Tuple[bool, dict]:
    """
    Run comprehensive cluster health checks for pre/post scenario validation.

    This unified method performs all health checks needed before and after
    test scenarios, ensuring consistent validation across the test workflow.

    Checks performed:
        1. Cluster health logging
        2. Scrub initiation on pool
        3. Deep-scrub initiation on pool
        4. Wait for PGs to be active+clean
        5. Pool sanity check
        6. Object count verification
        7. Data integrity verification (uses ec_size_* objects with checksums)
        8. Crash status check

    Args:
        rados_obj: RadosOrchestrator object
        pool_name: Pool name to check
        phase: "pre" for pre-scenario checks, "post" for post-scenario checks
        run_extended_setup: Whether extended setup was run (affects data integrity check)
        warning_ignore_list: List of warnings to ignore in pool sanity check

    Returns:
        Tuple of (success: bool, results: dict)
        - success: True if all checks passed, False otherwise
        - results: Dictionary containing check results including object_count
    """
    if warning_ignore_list is None:
        warning_ignore_list = []

    phase_label = "Pre-check" if phase == "pre" else "Post-check"
    phase_title = "PRE-SCENARIO CHECKS" if phase == "pre" else "POST-SCENARIO CHECKS"
    phase_desc = "before scenarios" if phase == "pre" else "after scenarios"

    results = {
        "cluster_health": False,
        "scrub": False,
        "deep_scrub": False,
        "pg_clean": False,
        "pool_sanity": False,
        "object_count": -1,
        "data_integrity": False,
        "no_crashes": False,
    }
    all_passed = True

    log.info("=" * 70)
    log.info(f"{phase_title}: Verifying cluster health {phase_desc}")
    log.info("=" * 70)

    # Check 1: Cluster health logging
    log.info(f"{phase_label} 1: Logging cluster health state...")
    try:
        rados_obj.log_cluster_health()
        results["cluster_health"] = True
        log.info("Cluster health logged successfully")
    except Exception as e:
        log.error(f"Failed to log cluster health: {e}")
        all_passed = False

    # Check 2-3: Scrub and deep-scrub
    log.info(
        f"{phase_label} 2-3: Initiating scrub and deep-scrub on pool {pool_name}..."
    )
    try:
        rados_obj.run_scrub(pool=pool_name)
        results["scrub"] = True
        rados_obj.run_deep_scrub(pool=pool_name)
        results["deep_scrub"] = True
        log.info(f"Scrub & deep-scrub initiated on pool {pool_name}")
    except Exception as e:
        log.error(f"Scrub/ deep-scrub failed: {e}")
        all_passed = False

    # Allow scrub operations to be initiated
    time.sleep(60)

    # Check 4: Wait for PGs to be active+clean
    log.info(f"{phase_label} 4: Waiting for PGs to be active+clean...")
    try:
        clean_pgs = wait_for_clean_pg_sets(rados_obj, timeout=1800, test_pool=pool_name)
        if clean_pgs:
            results["pg_clean"] = True
            log.info("PGs are active+clean")
        else:
            log.error("PGs not active + clean")
            all_passed = False
    except Exception as e:
        log.error(f"PGs did not reach active+clean state: {e}")
        all_passed = False

    # Check 5: Pool sanity check
    log.info(f"{phase_label} 5: Running pool sanity check...")
    try:
        sanity_result = rados_obj.run_pool_sanity_check(ignore_list=warning_ignore_list)
        if sanity_result:
            results["pool_sanity"] = True
            log.info("Pool sanity check passed")
        else:
            log.error("Pool sanity check returned failure")
            all_passed = False
    except Exception as e:
        log.error(f"Pool sanity check failed: {e}")
        all_passed = False

    # Check 6: Object count verification
    log.info(f"{phase_label} 6: Verifying object count...")
    try:
        object_count = verify_object_listing(rados_obj=rados_obj, pool_name=pool_name)
        results["object_count"] = object_count
        if object_count >= 0:
            log.info(f"Object count: {object_count} objects in pool {pool_name}")
        else:
            log.error("Object count returned invalid value")
            all_passed = False
    except Exception as e:
        log.error(f"Object count verification failed: {e}")
        all_passed = False

    # Check 7: Data integrity verification
    log.info(f"{phase_label} 7: Verifying data integrity...")
    if run_extended_setup:
        try:
            integrity_result = verify_data_integrity(
                rados_obj=rados_obj,
                pool_name=pool_name,
            )
            results["data_integrity"] = integrity_result
            if integrity_result:
                log.info("Data integrity verification passed")
            else:
                log.error("Data integrity verification failed")
                all_passed = False
        except Exception as e:
            log.error(f"Data integrity verification failed: {e}")
            all_passed = False
    else:
        log.info("Data integrity check skipped (extended setup not run)")
        results["data_integrity"] = True  # Mark as passed when skipped

    # Check 8: Crash status check (must run last)
    log.info(f"{phase_label} 8: Checking for crashes...")
    try:
        crash_detected = rados_obj.check_crash_status()
        results["no_crashes"] = not crash_detected
        if crash_detected:
            log.error("Crashes detected in cluster!")
            all_passed = False
        else:
            log.info("No crashes detected - cluster is healthy")
    except Exception as e:
        log.error(f"Crash status check failed: {e}")
        all_passed = False

    # Log summary
    log.info("=" * 70)
    log.info(f"{phase_title} SUMMARY:")
    for check_name, check_result in results.items():
        if check_name == "object_count":
            status = "PASS" if check_result >= 0 else "FAIL"
            log.info(f"  {check_name}: {status} ({check_result} objects)")
        else:
            status = "PASS" if check_result else "FAIL"
            log.info(f"  {check_name}: {status}")
    overall_status = "PASSED" if all_passed else "FAILED"
    log.info(f"Overall: {overall_status}")
    log.info("=" * 70)

    return all_passed, results


def wait_for_clean_pg_sets_check_inactive(
    rados_obj: RadosOrchestrator, timeout=1800, _sleep=60
) -> Tuple[bool, int]:
    """
    Wait for PGs to enter active + clean state while checking for inactive PGs.

    Monitors PG states during the workflow and reports the count of times
    inactive PGs were observed on the cluster.

    Args:
        rados_obj: RadosOrchestrator object to run commands
        timeout: timeout in seconds (default: 1800 = 30 minutes)
        _sleep: sleep interval in seconds (default: 60)

    Returns:  a tuple, consisting of method status and the inactive PG count
        True -> pass, False -> fail

    """
    inactive_count = 0
    end_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
    while end_time > datetime.datetime.now():
        flag = True
        status_report = rados_obj.run_ceph_command(cmd="ceph report", client_exec=True)

        # Proceeding to check if all PGs are in active + clean
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
                log.debug("Observed inactive PGs: %s", entry["state"])

        if flag:
            log.info("The recovery and back-filling of the OSD is completed")
            return True, inactive_count
        log.info(
            "Waiting for active + clean. Active alerts: %s, PG States: %s, "
            "checking status again in %s seconds",
            status_report["health"]["checks"].keys(),
            status_report["num_pg_by_state"],
            _sleep,
        )
        time.sleep(_sleep)

    log.error("The cluster did not reach active + Clean state")
    return False, inactive_count


def wait_for_host_up(host, rados_obj, timeout: int = 1200) -> bool:
    """
    Wait for a host to come back up after reboot.

    Checks both SSH connectivity and Ceph orchestrator host status.
    Host is considered online when it's NOT in "offline" status.

    Args:
        host: The host node object
        rados_obj: RadosOrchestrator object for checking host status via Ceph
        timeout: Maximum time to wait in seconds (default: 1200)

    Returns:
        True if host is up and online in Ceph, raises Exception if timeout
    """
    endtime = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
    ssh_up = False

    while datetime.datetime.now() < endtime:
        # First check SSH connectivity
        if not ssh_up:
            try:
                host.exec_command(cmd="uptime", sudo=True)
                log.info("Host %s is reachable via SSH", host.hostname)
                ssh_up = True
            except Exception:
                log.debug(
                    "Host %s not yet reachable via SSH, waiting...", host.hostname
                )
                time.sleep(10)
                continue

        # Once SSH is up, verify host is online in Ceph orchestrator
        # Host is online if it's NOT in "offline" status
        if ssh_up:
            if not rados_obj.check_host_status(
                hostname=host.hostname, status="offline"
            ):
                log.info("Host %s is online in Ceph orchestrator", host.hostname)
                time.sleep(10)
                return True
            else:
                log.debug(
                    "Host %s reachable via SSH but still offline in Ceph, waiting...",
                    host.hostname,
                )
                time.sleep(10)

    log.error("Host %s did not come back up within %s seconds", host.hostname, timeout)
    raise Exception(f"Host {host.hostname} did not come back up")


def is_recovery_ongoing(rados_obj) -> bool:
    """
    Check if recovery/backfill is ongoing in the cluster.

    Args:
        rados_obj: RadosOrchestrator object

    Returns:
        True if recovery/backfill is ongoing, False otherwise
    """
    status_report = rados_obj.run_ceph_command(cmd="ceph report", client_exec=True)
    recovery_states = (
        "recovering",
        "recovery_wait",
        "backfilling",
        "backfill_wait",
        "peering",
    )

    for entry in status_report.get("num_pg_by_state", []):
        if any(state in entry["state"] for state in recovery_states):
            log.info("Recovery/backfill activity detected: %s", entry["state"])
            return True

    return False


def reboot_host_and_wait(host, rados_obj, wait_time: int = 10) -> None:
    """
    Reboot a host and wait for it to come back up, then check recovery state.

    Uses check_host_status() to verify host goes offline and comes back online
    from Ceph orchestrator's perspective.

    Args:
        host: Host node to reboot
        rados_obj: RadosOrchestrator object
        wait_time: Time to wait after reboot command before checking (default: 10)
    """
    log.info("Rebooting host %s...", host.hostname)
    host.exec_command(cmd="reboot", sudo=True, check_ec=False)
    time.sleep(wait_time)

    log.info("Waiting for host %s to come back up...", host.hostname)
    wait_for_host_up(host, rados_obj)

    rados_obj.log_cluster_health()

    if not is_recovery_ongoing(rados_obj):
        log.info("No recovery activity detected, cluster may have recovered quickly")


def run_rados_overwrites(
    rados_obj,
    pool_name: str,
    stripe_unit: int,
    stripe_width: int,
    num_objects: int = 10,
) -> bool:
    """
    Perform direct RADOS writes with partial overwrites.

    Creates objects and performs partial overwrites at various offsets
    to test EC partial write optimization.

    Args:
        rados_obj: RadosOrchestrator object
        pool_name: EC pool name
        stripe_unit: EC stripe_unit in bytes
        stripe_width: Stripe width (k * stripe_unit) in bytes
        num_objects: Number of objects to create

    Returns:
        True on success, False on failure
    """
    try:
        log.info("Performing RADOS overwrites on %s objects", num_objects)

        # Base object size (multiple stripes)
        base_object_size = stripe_width * 4

        # Overwrite patterns
        overwrite_patterns = [
            {"size": stripe_unit, "offset": 0, "desc": "single chunk at start"},
            {
                "size": stripe_unit // 2,
                "offset": stripe_unit,
                "desc": "sub-chunk at chunk boundary",
            },
            {"size": 4096, "offset": stripe_width, "desc": "4KB at stripe boundary"},
            {
                "size": stripe_unit,
                "offset": stripe_width * 2,
                "desc": "chunk at 2nd stripe",
            },
        ]

        for i in range(num_objects):
            obj_name = f"ec_overwrite_obj_{i}"

            # Create base object
            base_file = f"/tmp/{obj_name}_base.bin"
            cmd = f"dd if=/dev/urandom of={base_file} bs={base_object_size} count=1 2>/dev/null"
            rados_obj.client.exec_command(cmd=cmd, sudo=True)

            cmd = f"rados -p {pool_name} put {obj_name} {base_file}"
            rados_obj.client.exec_command(cmd=cmd, sudo=True)

            # Perform partial overwrites
            for idx, pattern in enumerate(overwrite_patterns):
                partial_file = f"/tmp/{obj_name}_partial{idx}.bin"
                cmd = f"dd if=/dev/urandom of={partial_file} bs={pattern['size']} count=1 2>/dev/null"
                rados_obj.client.exec_command(cmd=cmd, sudo=True)

                cmd = f"rados -p {pool_name} put {obj_name} {partial_file} --offset {pattern['offset']}"
                rados_obj.client.exec_command(cmd=cmd, sudo=True)

            if (i + 1) % 5 == 0:
                log.debug("  Created and overwrote %s/%s objects", i + 1, num_objects)

        # Cleanup temp files
        rados_obj.client.exec_command(
            cmd="rm -f /tmp/ec_overwrite_obj_*",
            sudo=True,
            check_ec=False,
        )

        log.info("Completed RADOS overwrites on %s objects", num_objects)
        return True

    except Exception as e:
        log.error("RADOS overwrites failed: %s", e)
        return False


def run_rados_appends(
    rados_obj,
    pool_name: str,
    stripe_unit: int,
    num_objects: int = 10,
) -> bool:
    """
    Perform RADOS append operations.

    Creates objects and performs append operations to test
    EC append handling.

    Args:
        rados_obj: RadosOrchestrator object
        pool_name: EC pool name
        stripe_unit: EC stripe_unit in bytes
        num_objects: Number of objects to create

    Returns:
        True on success, False on failure
    """
    try:
        log.info("Performing RADOS appends on %s objects", num_objects)

        initial_size = stripe_unit // 2
        append_sizes = [stripe_unit // 2, stripe_unit, 4096]

        for i in range(num_objects):
            obj_name = f"ec_append_obj_{i}"

            # Create initial object
            initial_file = f"/tmp/{obj_name}_initial.bin"
            cmd = f"dd if=/dev/urandom of={initial_file} bs={initial_size} count=1 2>/dev/null"
            rados_obj.client.exec_command(cmd=cmd, sudo=True)

            cmd = f"rados -p {pool_name} put {obj_name} {initial_file}"
            rados_obj.client.exec_command(cmd=cmd, sudo=True)

            # Perform appends
            for append_size in append_sizes:
                append_file = f"/tmp/{obj_name}_append.bin"
                cmd = f"dd if=/dev/urandom of={append_file} bs={append_size} count=1 2>/dev/null"
                rados_obj.client.exec_command(cmd=cmd, sudo=True)

                cmd = f"rados -p {pool_name} append {obj_name} {append_file}"
                rados_obj.client.exec_command(cmd=cmd, sudo=True)

            if (i + 1) % 5 == 0:
                log.debug("  Created and appended %s/%s objects", i + 1, num_objects)

        # Cleanup temp files
        rados_obj.client.exec_command(
            cmd="rm -f /tmp/ec_append_obj_*",
            sudo=True,
            check_ec=False,
        )

        log.info("Completed RADOS appends on %s objects", num_objects)
        return True

    except Exception as e:
        log.error("RADOS appends failed: %s", e)
        return False


def run_rados_truncates(
    rados_obj,
    pool_name: str,
    stripe_unit: int,
    num_objects: int = 50,
) -> bool:
    """
    Perform RADOS truncate operations.

    Creates objects and performs truncate operations to test
    EC truncate handling and partial write optimization.

    Args:
        rados_obj: RadosOrchestrator object
        pool_name: EC pool name
        stripe_unit: EC stripe_unit in bytes
        num_objects: Number of objects to create

    Returns:
        True on success, False on failure
    """
    try:
        log.info("Performing RADOS truncates on %s objects", num_objects)

        # Base object size - create larger objects then truncate
        base_size = stripe_unit * 4

        # Truncate patterns to test various boundary conditions
        truncate_sizes = [
            stripe_unit * 3,  # Truncate to 3 stripe_units
            stripe_unit * 2,  # Truncate to 2 stripe_units
            stripe_unit + stripe_unit // 2,  # Truncate to 1.5 stripe_units
            stripe_unit,  # Truncate to exact stripe_unit
            stripe_unit // 2,  # Truncate to half stripe_unit
            4096,  # Truncate to 4KB
            0,  # Truncate to zero (empty object)
        ]

        for i in range(num_objects):
            obj_name = f"ec_truncate_obj_{i}"

            # Create base object with initial data
            base_file = f"/tmp/{obj_name}_base.bin"
            cmd = (
                f"dd if=/dev/urandom of={base_file} bs={base_size} count=1 2>/dev/null"
            )
            rados_obj.client.exec_command(cmd=cmd, sudo=True)

            cmd = f"rados -p {pool_name} put {obj_name} {base_file}"
            rados_obj.client.exec_command(cmd=cmd, sudo=True)

            # Perform truncate - cycles through different size patterns
            truncate_size = truncate_sizes[i % len(truncate_sizes)]
            cmd = f"rados -p {pool_name} truncate {obj_name} {truncate_size}"
            rados_obj.client.exec_command(cmd=cmd, sudo=True)

            if (i + 1) % 10 == 0:
                log.debug("  Created and truncated %s/%s objects", i + 1, num_objects)

        # Cleanup temp files
        rados_obj.client.exec_command(
            cmd="rm -f /tmp/ec_truncate_obj_*",
            sudo=True,
            check_ec=False,
        )

        log.info("Completed RADOS truncates on %s objects", num_objects)
        return True

    except Exception as e:
        log.error("RADOS truncates failed: %s", e)
        return False


def create_rbd_and_write_data(
    rados_obj,
    ec_pool_name: str,
    metadata_pool_name: str,
    image_name: str,
    image_size: str,
    stripe_unit: int,
) -> Tuple:
    """
    Create RBD image on EC data pool, write data using dd and FIO,
    and create RBD snapshots after each write phase.

    Also tests RBD clone and flatten operations on EC pools.

    Args:
        rados_obj: RadosOrchestrator object
        ec_pool_name: EC data pool name
        metadata_pool_name: Metadata pool name
        image_name: RBD image name
        image_size: Image size (e.g., "1G")
        stripe_unit: EC stripe_unit in bytes

    Returns:
        Tuple of (device_path, mount_path) on success, None on failure
    """
    try:
        # Create RBD image with EC data pool
        cmd = f"rbd create --size {image_size} --data-pool {ec_pool_name} {metadata_pool_name}/{image_name}"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        log.info("Created RBD image: %s/%s", metadata_pool_name, image_name)

        # Create initial snapshot (empty image)
        snap_name_initial = f"snap_initial_{int(time.time())}"
        cmd = f"rbd snap create {metadata_pool_name}/{image_name}@{snap_name_initial}"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        log.debug("Created RBD snapshot: %s (empty image)", snap_name_initial)

        # Mount the image
        mount_result = rados_obj.mount_image_on_client(
            pool_name=metadata_pool_name,
            img_name=image_name,
            client_obj=rados_obj.client,
            mount_path="/tmp/ec_test_mount/",
        )

        if not mount_result:
            log.error("Failed to mount RBD image")
            return None

        mount_path, device_path = mount_result
        log.info("Mounted RBD image at %s (device: %s)", mount_path, device_path)

        # Write data to mounted filesystem and create snapshots after each operation
        # Target: 15-20 RBD snapshots to test snapshot operations on EC pools
        stripe_unit_kb = stripe_unit // 1024
        test_file = f"{mount_path.rstrip('/')}/test_data.dat"
        snap_count = 1  # Already have snap_initial

        # Helper function to create snapshot
        def create_snapshot(snap_suffix: str) -> str:
            nonlocal snap_count
            snap_name = f"snap_{snap_count:02d}_{snap_suffix}_{int(time.time())}"
            rados_obj.client.exec_command(cmd="sync", sudo=True)
            cmd = f"rbd snap create {metadata_pool_name}/{image_name}@{snap_name}"
            rados_obj.client.exec_command(cmd=cmd, sudo=True)
            snap_count += 1
            log.debug("  Created snapshot %s: %s", snap_count, snap_name)
            return snap_name

        # Phase 1: dd writes with snapshots after each pattern (6 patterns = 6 snaps)
        log.info("Writing data using dd with various patterns (creating snapshots)...")
        dd_patterns = [
            {"bs": f"{stripe_unit_kb}K", "count": 50, "desc": "stripe-aligned-50"},
            {"bs": "4K", "count": 100, "desc": "4kb-100"},
            {"bs": "8K", "count": 75, "desc": "8kb-75"},
            {"bs": "16K", "count": 50, "desc": "16kb-50"},
            {"bs": "64K", "count": 25, "desc": "64kb-25"},
            {"bs": "1M", "count": 10, "desc": "1mb-10"},
        ]

        for i, pattern in enumerate(dd_patterns):
            # Use different files for variety
            dd_file = f"{mount_path.rstrip('/')}/dd_test_{i}.dat"
            cmd = (
                f"dd if=/dev/urandom of={dd_file} bs={pattern['bs']} "
                f"count={pattern['count']} conv=fsync 2>/dev/null"
            )
            rados_obj.client.exec_command(cmd=cmd, sudo=True)
            log.debug("  Wrote %s", pattern["desc"])
            create_snapshot(f"dd_{pattern['desc']}")

        log.info("dd writes completed - %s snapshots created so far", snap_count)

        # Phase 2: FIO writes with snapshots after each pattern (8 patterns = 8 snaps)
        log.info(
            "Writing data using FIO with various IO patterns (creating snapshots)..."
        )
        fio_dir = f"{mount_path.rstrip('/')}/fio_test"
        rados_obj.client.exec_command(cmd=f"mkdir -p {fio_dir}", sudo=True)

        fio_patterns = [
            {
                "name": "seq_write_stripe",
                "rw": "write",
                "bs": f"{stripe_unit_kb}k",
                "size": "20M",
                "desc": "seq-write-stripe-aligned",
            },
            {
                "name": "seq_write_4k",
                "rw": "write",
                "bs": "4k",
                "size": "15M",
                "desc": "seq-write-4k",
            },
            {
                "name": "rand_write_4k",
                "rw": "randwrite",
                "bs": "4k",
                "size": "10M",
                "desc": "rand-write-4k",
            },
            {
                "name": "rand_write_8k",
                "rw": "randwrite",
                "bs": "8k",
                "size": "10M",
                "desc": "rand-write-8k",
            },
            {
                "name": "rand_write_16k",
                "rw": "randwrite",
                "bs": "16k",
                "size": "10M",
                "desc": "rand-write-16k",
            },
            {
                "name": "mixed_rw_70_30",
                "rw": "randrw",
                "bs": "8k",
                "size": "15M",
                "rwmixread": "70",
                "desc": "mixed-rw-70-30",
            },
            {
                "name": "mixed_rw_50_50",
                "rw": "randrw",
                "bs": "4k",
                "size": "10M",
                "rwmixread": "50",
                "desc": "mixed-rw-50-50",
            },
            {
                "name": "seq_write_1m",
                "rw": "write",
                "bs": "1m",
                "size": "20M",
                "desc": "seq-write-1m",
            },
        ]

        for pattern in fio_patterns:
            fio_file = f"{fio_dir}/{pattern['name']}.dat"
            fio_cmd = (
                f"fio --name={pattern['name']} --filename={fio_file} "
                f"--rw={pattern['rw']} --bs={pattern['bs']} --size={pattern['size']} "
                f"--ioengine=libaio --direct=1 --numjobs=1 --runtime=10 "
                f"--time_based=0 --group_reporting --output-format=normal"
            )
            if "rwmixread" in pattern:
                fio_cmd += f" --rwmixread={pattern['rwmixread']}"

            fio_output, _ = rados_obj.client.exec_command(cmd=fio_cmd, sudo=True)
            log.debug("  FIO completed: %s", pattern["desc"])

            # Create snapshot after each FIO pattern
            create_snapshot(f"fio_{pattern['desc']}")

            # Log FIO stats for first and last patterns only (reduce log verbosity)
            if fio_output and pattern["name"] in ["seq_write_stripe", "seq_write_1m"]:
                log.info("  FIO Stats for %s:", pattern["name"])
                for line in fio_output.strip().split("\n"):
                    line = line.strip()
                    if any(
                        keyword in line.lower()
                        for keyword in ["iops", "bw=", "read:", "write:"]
                    ):
                        log.info("    %s", line)

        log.info("FIO writes completed - %s snapshots created so far", snap_count)

        # Phase 3: Additional file operations with snapshots (3-4 more snaps)
        log.info("Creating additional files with snapshots...")

        # Create multiple small files
        small_files_dir = f"{mount_path.rstrip('/')}/small_files"
        rados_obj.client.exec_command(cmd=f"mkdir -p {small_files_dir}", sudo=True)
        for i in range(10):
            cmd = f"dd if=/dev/urandom of={small_files_dir}/file_{i}.dat bs=4K count=10 2>/dev/null"
            rados_obj.client.exec_command(cmd=cmd, sudo=True)
        create_snapshot("small_files")

        # Create a large file
        large_file = f"{mount_path.rstrip('/')}/large_file.dat"
        cmd = (
            f"dd if=/dev/urandom of={large_file} bs=1M count=50 conv=fsync 2>/dev/null"
        )
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        create_snapshot("large_file_50m")

        # Overwrite parts of files (tests partial overwrites)
        cmd = f"dd if=/dev/urandom of={test_file} bs=4K count=50 seek=10 conv=notrunc,fsync 2>/dev/null"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        snap_name_dd = create_snapshot("partial_overwrite")

        # Final snapshot after all writes
        create_snapshot("final")

        log.info("All writes completed - Total %s RBD snapshots created", snap_count)

        # List all snapshots
        cmd = f"rbd snap ls {metadata_pool_name}/{image_name}"
        snap_list, _ = rados_obj.client.exec_command(cmd=cmd, sudo=True)
        log.info("RBD snapshots created:\n%s", snap_list.strip())

        # Test RBD clone and flatten operations on EC pool
        log.info("Creating RBD clone and flattening it...")
        clone_name = f"{image_name}_clone"

        # Protect the dd snapshot for cloning (will remain protected)
        cmd = f"rbd snap protect {metadata_pool_name}/{image_name}@{snap_name_dd}"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        log.debug("Protected snapshot: %s", snap_name_dd)

        # Create clone from snapshot
        cmd = f"rbd clone {metadata_pool_name}/{image_name}@{snap_name_dd} {metadata_pool_name}/{clone_name}"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        log.info("Created RBD clone: %s from snapshot %s", clone_name, snap_name_dd)

        # Flatten the clone (makes it independent, tests EC write path during COW)
        log.debug("Flattening clone %s...", clone_name)
        cmd = f"rbd flatten {metadata_pool_name}/{clone_name}"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)
        log.info("Flattened RBD clone: %s (now independent)", clone_name)

        # Keep both snapshots and clone for recovery testing
        # Clone is now independent after flatten, snapshot remains protected
        # Both will be cleaned up in finally block

        # List all RBD images
        cmd = f"rbd ls {metadata_pool_name}"
        img_list, _ = rados_obj.client.exec_command(cmd=cmd, sudo=True)
        log.info("RBD images in pool %s:\n%s", metadata_pool_name, img_list.strip())

        log.info(
            "Data written to RBD image successfully "
            "(dd + FIO + %s snapshots + 1 flattened clone)",
            snap_count,
        )
        return device_path, mount_path

    except Exception as e:
        log.error("Failed to create RBD and write data: %s", e)
        return None


def create_objects_with_xattr(
    rados_obj,
    pool_name: str,
    num_objects: int = 10,
) -> bool:
    """
    Create objects with extended attributes (xattr) on the pool.

    Extended attributes are key-value pairs stored with the object metadata.
    This tests EC pool handling of objects with xattr data.

    Args:
        rados_obj: RadosOrchestrator object
        pool_name: Pool name
        num_objects: Number of objects to create with xattr

    Returns:
        True on success, False on failure
    """
    try:
        log.info("Creating %s objects with extended attributes (xattr)", num_objects)

        for i in range(num_objects):
            obj_name = f"ec_xattr_obj_{i}"

            # Create object with data
            data_file = f"/tmp/{obj_name}_data.bin"
            cmd = f"dd if=/dev/urandom of={data_file} bs=4096 count=10 2>/dev/null"
            rados_obj.client.exec_command(cmd=cmd, sudo=True)

            cmd = f"rados -p {pool_name} put {obj_name} {data_file}"
            rados_obj.client.exec_command(cmd=cmd, sudo=True)

            # Set multiple extended attributes on the object
            xattr_pairs = [
                ("user.testkey1", f"value1_obj{i}"),
                ("user.testkey2", f"value2_obj{i}"),
                ("user.metadata", f"metadata_for_object_{i}"),
                ("user.timestamp", f"{int(time.time())}"),
                ("user.description", f"Test object {i} with extended attributes"),
            ]

            for xattr_name, xattr_value in xattr_pairs:
                # Set xattr using rados setxattr (value passed directly)
                cmd = f"rados -p {pool_name} setxattr {obj_name} {xattr_name} '{xattr_value}'"
                rados_obj.client.exec_command(cmd=cmd, sudo=True)

            if (i + 1) % 10 == 0:
                log.debug("  Created %s/%s objects with xattr", i + 1, num_objects)

        # Cleanup temp files
        rados_obj.client.exec_command(
            cmd="rm -f /tmp/ec_xattr_obj_*",
            sudo=True,
            check_ec=False,
        )

        log.info("Completed creating %s objects with xattr", num_objects)
        return True

    except Exception as e:
        log.error("Failed to create objects with xattr: %s", e)
        return False


def create_comprehensive_test_objects(
    rados_obj,
    pool_name: str,
    stripe_unit: int,
    obj_prefix: str = None,
) -> bool:
    """
    Create objects with various sizes for comprehensive coverage.

    Creates objects of various sizes and performs additional operations
    (overwrite, append, truncate) on selected objects to create objects
    that have experienced multiple operations. After all operations,
    calculates MD5 checksum and stores it as xattr for data integrity
    verification.

    Creates:
    - Zero-size objects
    - Small objects (< stripe_unit)
    - Exact stripe_unit sized objects
    - Objects at stripe boundaries
    - Large objects (multiple stripes)

    Additional operations on objects >= stripe_unit:
    1. Partial overwrite at offset 0
    2. Append operation
    3. Truncate operation
    4. Calculate MD5 checksum of final object
    5. Store checksum and metadata as xattr (user.checksum)

    Objects with checksums (for verify_data_integrity):
    - ec_size_exact_chunk_obj (or ec_size_exact_chunk_obj_<prefix> if obj_prefix provided)
    - ec_size_chunk_plus_1
    - ec_size_chunk_plus_half
    - ec_size_two_chunks_obj
    - ec_size_three_chunks_obj
    - ec_size_four_chunks_obj
    - ec_size_large_obj_1m
    - ec_size_large_obj_4m

    Args:
        rados_obj: RadosOrchestrator object
        pool_name: Pool name
        stripe_unit: EC stripe_unit in bytes
        obj_prefix: Optional prefix to add as suffix to object names for uniqueness

    Returns:
        True on success, False on failure
    """
    try:
        log.info("Creating objects with various sizes for coverage")

        # Define size patterns to test
        size_patterns = [
            # Small sizes
            {"name": "zero_size_obj", "size": 0, "desc": "zero-size object"},
            {"name": "tiny_obj_1b", "size": 1, "desc": "1 byte object"},
            {"name": "tiny_obj_512b", "size": 512, "desc": "512 byte object"},
            {"name": "small_obj_1k", "size": 1024, "desc": "1KB object"},
            {"name": "small_obj_4k", "size": 4096, "desc": "4KB object"},
            # Stripe unit boundary sizes
            {
                "name": "sub_chunk_obj",
                "size": stripe_unit // 2,
                "desc": "half stripe_unit",
            },
            {
                "name": "chunk_minus_1",
                "size": stripe_unit - 1,
                "desc": "stripe_unit - 1 byte",
            },
            {
                "name": "exact_chunk_obj",
                "size": stripe_unit,
                "desc": "exact stripe_unit",
            },
            {
                "name": "chunk_plus_1",
                "size": stripe_unit + 1,
                "desc": "stripe_unit + 1 byte",
            },
            {
                "name": "chunk_plus_half",
                "size": stripe_unit + stripe_unit // 2,
                "desc": "1.5 stripe_units",
            },
            # Multiple stripe_units
            {
                "name": "two_chunks_obj",
                "size": stripe_unit * 2,
                "desc": "2 stripe_units",
            },
            {
                "name": "three_chunks_obj",
                "size": stripe_unit * 3,
                "desc": "3 stripe_units",
            },
            {
                "name": "four_chunks_obj",
                "size": stripe_unit * 4,
                "desc": "4 stripe_units",
            },
            # Large objects
            {"name": "large_obj_1m", "size": 1024 * 1024, "desc": "1MB object"},
            {"name": "large_obj_4m", "size": 4 * 1024 * 1024, "desc": "4MB object"},
        ]

        created_count = 0
        ops_count = 0
        temp_files = []  # Collect all temp files for cleanup at end

        for pattern in size_patterns:
            # Create object name with optional prefix suffix for uniqueness
            if obj_prefix:
                obj_name = f"ec_size_{pattern['name']}_{obj_prefix}"
            else:
                obj_name = f"ec_size_{pattern['name']}"
            obj_size = pattern["size"]

            if obj_size == 0:
                # Create empty object using rados create
                cmd = f"rados -p {pool_name} create {obj_name}"
                rados_obj.client.exec_command(cmd=cmd, sudo=True)
            else:
                # Create object with data
                data_file = f"/tmp/{obj_name}_data.bin"
                temp_files.append(data_file)
                cmd = f"dd if=/dev/urandom of={data_file} bs={obj_size} count=1 2>/dev/null"
                rados_obj.client.exec_command(cmd=cmd, sudo=True)

                cmd = f"rados -p {pool_name} put {obj_name} {data_file}"
                rados_obj.client.exec_command(cmd=cmd, sudo=True)

            created_count += 1
            log.debug("  Created %s (%s)", obj_name, pattern["desc"])

            # Perform additional operations on objects >= stripe_unit size
            # These objects will have experienced multiple operations
            if obj_size >= stripe_unit:
                ops_performed = []

                # 1. Partial overwrite at offset 0 (4KB overwrite)
                overwrite_size = min(4096, obj_size // 2)
                overwrite_file = f"/tmp/{obj_name}_overwrite.bin"
                temp_files.append(overwrite_file)
                cmd = f"dd if=/dev/urandom of={overwrite_file} bs={overwrite_size} count=1 2>/dev/null"
                rados_obj.client.exec_command(cmd=cmd, sudo=True)
                cmd = f"rados -p {pool_name} put {obj_name} {overwrite_file} --offset 0"
                rados_obj.client.exec_command(cmd=cmd, sudo=True)
                ops_performed.append("overwrite")

                # 2. Append operation (append 4KB)
                append_size = 4096
                append_file = f"/tmp/{obj_name}_append.bin"
                temp_files.append(append_file)
                cmd = f"dd if=/dev/urandom of={append_file} bs={append_size} count=1 2>/dev/null"
                rados_obj.client.exec_command(cmd=cmd, sudo=True)
                cmd = f"rados -p {pool_name} append {obj_name} {append_file}"
                rados_obj.client.exec_command(cmd=cmd, sudo=True)
                ops_performed.append("append")

                # 3. Truncate operation (reduce size by half stripe_unit)
                new_size = obj_size + append_size - (stripe_unit // 2)
                cmd = f"rados -p {pool_name} truncate {obj_name} {new_size}"
                rados_obj.client.exec_command(cmd=cmd, sudo=True)
                ops_performed.append("truncate")

                # 4. Calculate MD5 checksum of final modified object and store as xattr
                # Read the final object back to calculate checksum
                checksum_file = f"/tmp/{obj_name}_checksum.bin"
                temp_files.append(checksum_file)
                cmd = f"rados -p {pool_name} get {obj_name} {checksum_file}"
                rados_obj.client.exec_command(cmd=cmd, sudo=True)

                # Calculate MD5 checksum
                cmd = f"md5sum {checksum_file} | awk '{{print $1}}'"
                checksum, _ = rados_obj.client.exec_command(cmd=cmd, sudo=True)
                checksum = checksum.strip()

                # 5. Store checksum and metadata as xattr
                xattr_pairs = [
                    ("user.checksum", checksum),
                    ("user.original_size", f"{obj_size}"),
                    ("user.final_size", f"{new_size}"),
                    ("user.ops_performed", "overwrite_append_truncate"),
                    ("user.timestamp", f"{int(time.time())}"),
                ]
                for xattr_name, xattr_value in xattr_pairs:
                    cmd = f"rados -p {pool_name} setxattr {obj_name} {xattr_name} '{xattr_value}'"
                    rados_obj.client.exec_command(cmd=cmd, sudo=True)
                ops_performed.append("checksum")

                ops_count += 1
                log.debug(
                    "    Additional ops on %s: %s (checksum: %s)",
                    obj_name,
                    ", ".join(ops_performed),
                    checksum[:8] + "...",
                )

        # Cleanup all temp files at once
        if temp_files:
            rados_obj.client.exec_command(
                cmd=f"rm -f {' '.join(temp_files)}", sudo=True, check_ec=False
            )

        log.info(
            "Completed creating %s objects with various sizes "
            "(%s objects with additional ops: overwrite, append, xattr, truncate)",
            created_count,
            ops_count,
        )
        return True

    except Exception as e:
        log.error("Failed to create objects with various sizes: %s", e)
        return False


def verify_object_listing(
    rados_obj,
    pool_name: str,
) -> int:
    """
    Verify object listing works and return object count.

    Args:
        rados_obj: RadosOrchestrator object
        pool_name: Pool name

    Returns:
        Object count on success, -1 on failure
    """
    try:
        # List objects in pool using get_object_list method
        objects = rados_obj.get_object_list(pool_name=pool_name)
        obj_count = len(objects)

        log.debug("Object listing: found %s objects in pool %s", obj_count, pool_name)
        return obj_count

    except Exception as e:
        log.error("Failed to verify object listing: %s", e)
        return -1


def verify_data_integrity(
    rados_obj,
    pool_name: str,
) -> bool:
    """
    Verify data integrity of objects created by create_comprehensive_test_objects().

    Verifies ec_size_* objects that have MD5 checksums stored as xattr.
    These are objects >= stripe_unit that underwent overwrite, append,
    and truncate operations.

    Reads back each object, calculates MD5 checksum, and compares with
    the stored user.checksum xattr value.

    Args:
        rados_obj: RadosOrchestrator object
        pool_name: Pool name

    Returns:
        True if all objects pass integrity check, False otherwise
    """
    try:
        # Objects created by create_comprehensive_test_objects() with checksums
        # These are objects >= stripe_unit that have user.checksum xattr
        ec_size_objects_with_checksum = [
            "ec_size_exact_chunk_obj",
            "ec_size_chunk_plus_1",
            "ec_size_chunk_plus_half",
            "ec_size_two_chunks_obj",
            "ec_size_three_chunks_obj",
            "ec_size_four_chunks_obj",
            "ec_size_large_obj_1m",
            "ec_size_large_obj_4m",
        ]

        log.info(
            "Verifying data integrity: checking %s objects with checksums",
            len(ec_size_objects_with_checksum),
        )

        passed = 0
        failed = 0

        for obj_name in ec_size_objects_with_checksum:
            result = _verify_single_object_checksum(rados_obj, pool_name, obj_name)
            if result:
                passed += 1
            else:
                failed += 1

        log.info(
            "Data integrity verification: %s passed, %s failed out of %s",
            passed,
            failed,
            len(ec_size_objects_with_checksum),
        )

        return failed == 0

    except Exception as e:
        log.error("Data integrity verification failed: %s", e)
        return False


def _verify_single_object_checksum(
    rados_obj,
    pool_name: str,
    obj_name: str,
) -> bool:
    """
    Verify checksum of a single object.

    Reads the object, calculates MD5 checksum, and compares with stored xattr.

    Args:
        rados_obj: RadosOrchestrator object
        pool_name: Pool name
        obj_name: Object name to verify

    Returns:
        True if checksum matches, False otherwise
    """
    read_file = f"/tmp/{obj_name}_verify.bin"
    try:
        # Read the object
        cmd = f"rados -p {pool_name} get {obj_name} {read_file}"
        rados_obj.client.exec_command(cmd=cmd, sudo=True)

        # Calculate checksum of read data
        cmd = f"md5sum {read_file} | awk '{{print $1}}'"
        current_checksum, _ = rados_obj.client.exec_command(cmd=cmd, sudo=True)
        current_checksum = current_checksum.strip()

        # Get stored checksum from xattr
        cmd = f"rados -p {pool_name} getxattr {obj_name} user.checksum"
        stored_checksum, _ = rados_obj.client.exec_command(cmd=cmd, sudo=True)
        stored_checksum = stored_checksum.strip()

        if current_checksum == stored_checksum:
            log.debug("  %s: checksum OK", obj_name)
            return True
        else:
            log.error(
                "Checksum mismatch for %s: expected %s, got %s",
                obj_name,
                stored_checksum,
                current_checksum,
            )
            return False

    except Exception as obj_err:
        log.error("Failed to verify object %s: %s", obj_name, obj_err)
        return False

    finally:
        # Cleanup temp file
        rados_obj.client.exec_command(
            cmd=f"rm -f {read_file}", sudo=True, check_ec=False
        )


def run_read_verification(
    rados_obj,
    pool_name: str,
) -> bool:
    """
    Perform read operations on objects created by create_comprehensive_test_objects().

    Reads all objects with various sizes to verify EC read patterns work correctly.
    Also performs partial reads at random offsets on larger objects.

    Args:
        rados_obj: RadosOrchestrator object
        pool_name: Pool name

    Returns:
        True if all reads succeed, False otherwise
    """
    try:
        # Objects created by create_comprehensive_test_objects() for read verification
        # These objects have various sizes that test different EC read patterns
        various_size_objects = [
            "ec_size_zero_size_obj",
            "ec_size_tiny_obj_1b",
            "ec_size_tiny_obj_512b",
            "ec_size_small_obj_1k",
            "ec_size_small_obj_4k",
            "ec_size_sub_chunk_obj",
            "ec_size_chunk_minus_1",
            "ec_size_exact_chunk_obj",
            "ec_size_chunk_plus_1",
            "ec_size_chunk_plus_half",
            "ec_size_two_chunks_obj",
            "ec_size_three_chunks_obj",
            "ec_size_four_chunks_obj",
            "ec_size_large_obj_1m",
            "ec_size_large_obj_4m",
        ]

        log.info(
            "Performing read verification on %s objects", len(various_size_objects)
        )

        success_count = 0
        fail_count = 0

        for i, obj_name in enumerate(various_size_objects):
            read_file = f"/tmp/read_verify_{i}_{int(time.time())}.bin"

            try:
                # Full object read
                cmd = f"rados -p {pool_name} get {obj_name} {read_file}"
                rados_obj.client.exec_command(cmd=cmd, sudo=True)

                # Verify file was created and get size
                cmd = f"wc -c < {read_file}"
                size_out, _ = rados_obj.client.exec_command(cmd=cmd, sudo=True)
                size = int(size_out.strip())

                # If we got here, the read succeeded (even 0-byte objects are valid)
                success_count += 1

                # Also try a partial read with random offset for larger objects
                if size > 1024:
                    partial_file = f"/tmp/partial_read_{i}_{int(time.time())}.bin"
                    offset = random.randint(0, min(size - 1, 4096))
                    cmd = f"rados -p {pool_name} get {obj_name} {partial_file} --offset {offset}"
                    rados_obj.client.exec_command(cmd=cmd, sudo=True)
                    rados_obj.client.exec_command(
                        cmd=f"rm -f {partial_file}", sudo=True, check_ec=False
                    )

            except Exception as read_err:
                log.debug("Read failed for %s: %s", obj_name, read_err)
                fail_count += 1

            finally:
                rados_obj.client.exec_command(
                    cmd=f"rm -f {read_file}", sudo=True, check_ec=False
                )

        log.info(
            "Read verification: %s succeeded, %s failed out of %s objects",
            success_count,
            fail_count,
            len(various_size_objects),
        )

        return fail_count == 0

    except Exception as e:
        log.error("Read verification failed: %s", e)
        return False
