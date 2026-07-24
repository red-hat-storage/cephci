"""
Tier-3 test module to perform various operations on large OMAPS
and execute OSD compaction during IOPS with and without OSD failure
"""

import random
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from ceph.rados.utils import get_cluster_timestamp
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.test_mon_osd_mgr_resiliency import (
    run_cephfs_snapshot_stress,
    run_rbd_snapshot_stress,
)
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)

DEFAULT_SAMPLE_PERCENTAGE = 25
OSD_RESTART_TIMEOUT = 180  # seconds - max time to wait for OSD to come up
OSD_RESTART_CHECK_INTERVAL = 10  # seconds - interval between status checks
COMPACTION_WAIT = 10
OMAP_OPERATION_WAIT = 5


def run(ceph_cluster, **kw):
    """
    # CEPH-11681
    This test is to perform various operations on OMAPS and execute
    OSD compaction with and without OSD failure
    1. Create cluster with default configuration
    2. Create a pool and write large number of OMAPS to few objects
    3. Perform various operations on the OMAP entries, list, get,
       add new, delete, clear, etc
    4. [Optional] Run Ceph client OMAP stress tests (RBD and CephFS snapshots)
       if 'ceph_client_omap_tests' config is provided
    5. Check rocksdb_cf_compact_on_deletion configuration for OSDs
    6. Manual compaction test:
        - Get initial BlueFS statistics for single chosen OSD before compaction
        - Execute 'ceph tell osd.X compact' for that OSD
        - Get final BlueFS statistics for that OSD after compaction
        - Verify db_used_bytes reduction for that single OSD
    7. Restart compaction test (bulk OSDs):
        - Enable osd_compact_on_start config on all OSDs
        - Get BlueFS stats for 25% of OSDs before restart
        - Restart the sampled OSDs twice (to trigger compaction on each restart)
        - Get BlueFS stats for same OSDs after both restarts
        - Verify db_used_bytes reduction for bulk OSDs
    8. Stop the OSD using systemctl
    9. Execute OSD compaction on the down OSD, expected to fail
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    omap_config = config["omap_config"]
    rhbuild = config.get("rhbuild")
    start_time = get_cluster_timestamp(rados_obj.node)
    log.debug(f"Test workflow started. Start time: {start_time}")
    # Creating pools and starting the test
    try:
        log.debug(
            "Creating replicated pool on the cluster with name %s",
            omap_config["pool_name"],
        )
        method_should_succeed(rados_obj.create_pool, **omap_config)
        pool_name = omap_config.pop("pool_name")
        normal_objs = omap_config["normal_objs"]
        if normal_objs > 0:
            # create n number of objects without any omap entry
            rados_obj.bench_write(
                pool_name=pool_name,
                **{
                    "rados_write_duration": 600,
                    "byte_size": "4096KB",
                    "max_objs": normal_objs,
                    "verify_stats": False,
                },
            )

        # calculate objects to be written with omaps and begin omap creation process
        omap_obj_num = omap_config["obj_end"] - omap_config["obj_start"]
        log.debug(
            "Created the pool. beginning to create omap entries on the pool. Count : %s",
            omap_obj_num,
        )
        if not pool_obj.fill_omap_entries(pool_name=pool_name, **omap_config):
            log.error("Omap entries not generated on pool %s", pool_name)
            raise Exception(f"Omap entries not generated on pool {pool_name}")

        assert pool_obj.check_large_omap_warning(
            pool=pool_name,
            obj_num=omap_obj_num,
            check=omap_config["large_warn"],
            obj_check=False,
        )

        # getting list of omap objects present in the pool
        out, _ = cephadm.shell([f"rados ls -p {pool_name} | grep 'omap_obj'"])
        omap_obj_list = out.split()
        log.info(
            "List of Objects having large omaps in %s, listing first 100: %s",
            pool_name,
            omap_obj_list[:100],
        )

        # performing various operations on large omap objects
        # 1. List omap key entries for an object
        obj_rand = random.choice(omap_obj_list)
        log.info("#1 Listing OMAP key entries for obj: %s", obj_rand)
        omap_keys, _ = pool_obj.perform_omap_operation(
            pool=pool_name, obj=obj_rand, ops="listomapkeys"
        )
        log.info(
            "Printing first 20 OMAP keys present for Object %s: \n%s",
            obj_rand,
            omap_keys.split()[:20],
        )

        # 2. List omap keys & values entries for an object
        obj_rand = random.choice(omap_obj_list)
        log.info("#2 Listing OMAP key & value entries for obj: %s", obj_rand)
        omap_key_val, _ = pool_obj.perform_omap_operation(
            pool=pool_name, obj=obj_rand, ops="listomapvals"
        )
        omap_kv_trim = "\n".join(omap_key_val.split("\n")[-21:-1])
        log.info(
            "Printing last few OMAP keys and values present for Object %s: %s",
            obj_rand,
            omap_kv_trim,
        )

        # 3. create a custom omap key & value entry for an object
        obj_rand = random.choice(omap_obj_list)
        log.info("#3 Creating a custom OMAP key & value entry for obj: %s", obj_rand)
        custom_val = random.randint(pow(10, 5), pow(10, 6) - 1)
        custom_key = f"key_{custom_val}"
        pool_obj.perform_omap_operation(
            pool=pool_name,
            obj=obj_rand,
            ops="setomapval",
            key=custom_key,
            val=custom_val,
        )
        time.sleep(OMAP_OPERATION_WAIT)

        # verifying addition of custom OMAP key
        # 4. Fetch value of a particular omap key
        log.info("#4 Fetching value of a particular OMAP key obj: %s", obj_rand)
        omap_val, _ = pool_obj.perform_omap_operation(
            pool=pool_name, obj=obj_rand, ops="getomapval", key=custom_key
        )
        assert str(custom_val) in omap_val
        log.info("Value of OMAP key %s is: %s", custom_key, omap_val)

        # 5. Remove the newly added OMAP entry
        log.info("#5 Removing a custom OMAP entry for obj: %s", obj_rand)
        pool_obj.perform_omap_operation(
            pool=pool_name, obj=obj_rand, ops="rmomapkey", key=custom_key
        )
        time.sleep(OMAP_OPERATION_WAIT)

        # verifying successful removal of custom OMAP key
        # below validation does not help as failure is not getting
        # caught correctly
        # try:
        #     _, err = pool_obj.perform_omap_operation(
        #         pool=pool_name, obj=obj_rand, ops="listomapkeys", key=custom_key
        #     )
        # except Exception as ex:
        #     log.info(f"Expected to fail | STDERR:\n {ex}")
        # if "No such key" not in ex:
        #     raise AssertionError(f"{custom_key} should have been deleted, expected error msg not found")
        omap_keys, _ = pool_obj.perform_omap_operation(
            pool=pool_name, obj=obj_rand, ops="listomapkeys"
        )
        assert custom_key not in omap_keys
        log.info("%s key successfully removed from OMAP entries", custom_key)

        # 6. Clear all OMAP entries from an object
        obj_rand = random.choice(omap_obj_list)
        omap_obj_list.remove(obj_rand)
        log.info("#6 Clearing all OMAP entries for obj: %s", obj_rand)
        pool_obj.perform_omap_operation(pool=pool_name, obj=obj_rand, ops="clearomap")
        time.sleep(OMAP_OPERATION_WAIT)

        # verifying removal of all omap entries for the obj
        omap_keys, _ = pool_obj.perform_omap_operation(
            pool=pool_name, obj=obj_rand, ops="listomapkeys"
        )
        assert len(omap_keys) == 0
        log.info("All OMAP entries succesfully removed for obj: %s", obj_rand)

        log.info("OMAP operations have been successfully executed")

        # Run Client OMAP stress tests if configured
        if config.get("ceph_client_omap_tests"):
            log.info("Starting Ceph client OMAP stress tests (RBD and CephFS)")
            client_config = config.get("ceph_client_omap_tests", {})

            # Collect BlueFS stats before RBD workload
            bluefs_before_rbd = get_bluefs_stats_for_multiple_osds(
                rados_obj, stage="before RBD workload"
            )

            # Run RBD snapshot stress test
            if client_config.get("run_rbd_stress", True):
                log.info("Starting RBD snapshot stress test")
                rbd_config = client_config.get("rbd_config", {})
                run_rbd_snapshot_stress(
                    rados_obj=rados_obj,
                    pool_name=rbd_config.get("pool_name", "rbd_stress"),
                    num_pools=rbd_config.get("num_pools", 5),
                    images_per_pool=rbd_config.get("images_per_pool", 5),
                    snaps_per_image=rbd_config.get("snaps_per_image", 10),
                    parallel_jobs=rbd_config.get("parallel_jobs", 5),
                    duration=rbd_config.get("duration", 30),
                )
                log.info("RBD snapshot stress test completed successfully")

            # Collect BlueFS stats after RBD, before CephFS
            bluefs_after_rbd = get_bluefs_stats_for_multiple_osds(
                rados_obj, stage="after RBD, before CephFS"
            )

            # Run CephFS snapshot stress test
            if client_config.get("run_cephfs_stress", True):
                log.info("Starting CephFS snapshot stress test")
                cephfs_config = client_config.get("cephfs_config", {})
                run_cephfs_snapshot_stress(
                    rados_obj=rados_obj,
                    fs_name=cephfs_config.get("fs_name", "cephfs"),
                    num_subvols=cephfs_config.get("num_subvols", 5),
                    files_per_subvol=cephfs_config.get("files_per_subvol", 10),
                    snaps_per_subvol=cephfs_config.get("snaps_per_subvol", 10),
                    parallel_jobs=cephfs_config.get("parallel_jobs", 5),
                    volume_group=cephfs_config.get("volume_group", "cfsstress"),
                    duration=cephfs_config.get("duration", 30),
                )
                log.info("CephFS snapshot stress test completed successfully")

            # Collect BlueFS stats after CephFS workload
            bluefs_after_cephfs = get_bluefs_stats_for_multiple_osds(
                rados_obj, stage="after CephFS workload"
            )

            if not bluefs_after_cephfs:
                log.error("Failed to collect BlueFS stats after CephFS workload")
                raise AssertionError(
                    "Could not collect BlueFS statistics after CephFS workload"
                )

            # Log comparison summary
            if bluefs_before_rbd and bluefs_after_rbd and bluefs_after_cephfs:
                rbd_impact = (
                    bluefs_after_rbd["averages"]["db_used_bytes"]
                    - bluefs_before_rbd["averages"]["db_used_bytes"]
                )
                cephfs_impact = (
                    bluefs_after_cephfs["averages"]["db_used_bytes"]
                    - bluefs_after_rbd["averages"]["db_used_bytes"]
                )
                total_change = (
                    bluefs_after_cephfs["averages"]["db_used_bytes"]
                    - bluefs_before_rbd["averages"]["db_used_bytes"]
                )
                log.info(
                    "BlueFS Usage Trend During Client Workloads:\n"
                    "  Before RBD:      %s bytes (avg)\n"
                    "  After RBD:       %s bytes (avg)\n"
                    "  After CephFS:    %s bytes (avg)\n"
                    "  RBD impact:      %+s bytes\n"
                    "  CephFS impact:   %+s bytes\n"
                    "  Total change:    %+s bytes",
                    f"{bluefs_before_rbd['averages']['db_used_bytes']:,}",
                    f"{bluefs_after_rbd['averages']['db_used_bytes']:,}",
                    f"{bluefs_after_cephfs['averages']['db_used_bytes']:,}",
                    f"{rbd_impact:,}",
                    f"{cephfs_impact:,}",
                    f"{total_change:,}",
                )

            log.info("All client OMAP stress tests completed successfully")

        time.sleep(20)
        # Fetch an Object with OMAPs and its primary OSD to perform OSD compaction
        _, osd_id = get_random_osd_for_object(
            rados_obj, pool_name, omap_obj_list, purpose="compaction"
        )

        # fetch the initial use % for the chosen OSD
        log.debug("Fetching the initial use % for the chosen OSD before compaction")
        osd_df_init = rados_obj.get_osd_df_stats(filter=f"osd.{osd_id}")["nodes"][0]
        osd_util_init = osd_df_init["utilization"]

        # Get initial BlueFS stats for the single chosen OSD before compaction
        log.info("Collecting BlueFS stats for OSD %s before compaction", osd_id)
        bluefs_init = rados_obj.get_osd_perf_dump(osd_id, filter="bluefs")
        bluefs_init = bluefs_init.get("bluefs", {})
        bluefs_init_used = bluefs_init.get("db_used_bytes", 0)
        bluefs_init_total = bluefs_init.get("db_total_bytes", 0)
        log.info(
            "OSD %s BlueFS stats before compaction:\n"
            "  db_used_bytes:  %s\n"
            "  db_total_bytes: %s",
            osd_id,
            f"{bluefs_init_used:,}",
            f"{bluefs_init_total:,}",
        )

        # Compact the single chosen OSD
        log.info("Starting OSD compaction for OSD %s", osd_id)
        out, _ = cephadm.shell([f"ceph tell osd.{osd_id} compact"])
        log.debug("Compaction output for OSD %s: %s", osd_id, out)

        # "ceph tell osd.1 compact" does not print elapsed_time
        # in 8.1. BZ https://bugzilla.redhat.com/show_bug.cgi?id=2351352
        if float(rhbuild.split("-")[0]) < 8.1:
            assert "elapsed_time" in out

        log.info("Completed compaction of OSD %s", osd_id)
        time.sleep(COMPACTION_WAIT)

        # fetch the final use % for the originally chosen OSD after compaction
        log.debug(
            "Fetching the final use % for the originally chosen OSD after compaction"
        )
        osd_df_final = rados_obj.get_osd_df_stats(filter=f"osd.{osd_id}")["nodes"][0]
        osd_util_final = osd_df_final["utilization"]

        # Get final BlueFS stats for the single chosen OSD after compaction
        log.info("Collecting BlueFS stats for OSD %s after compaction", osd_id)
        bluefs_final = rados_obj.get_osd_perf_dump(osd_id, filter="bluefs")
        bluefs_final = (
            bluefs_final.get("bluefs", {}) if isinstance(bluefs_final, dict) else {}
        )
        bluefs_final_used = bluefs_final.get("db_used_bytes", 0)
        bluefs_final_total = bluefs_final.get("db_total_bytes", 0)
        log.info(
            "OSD %s BlueFS stats after compaction:\n"
            "  db_used_bytes:  %s\n"
            "  db_total_bytes: %s",
            osd_id,
            f"{bluefs_final_used:,}",
            f"{bluefs_final_total:,}",
        )

        log.info(
            "Initial OSD %s utilization before compaction: %s", osd_id, osd_util_init
        )
        log.info(
            "Final OSD %s utilization after compaction: %s", osd_id, osd_util_final
        )
        assert osd_util_final < osd_util_init

        # Verify BlueFS db_used_bytes reduction for the single OSD
        reduction = bluefs_init_used - bluefs_final_used
        reduction_pct = (
            (reduction / bluefs_init_used * 100) if bluefs_init_used > 0 else 0
        )
        log.info(
            "OSD %s BlueFS db_used_bytes reduction after compaction:\n"
            "  Reduction: %+s bytes (%+.2f%%)",
            osd_id,
            f"{reduction:,}",
            reduction_pct,
        )
        assert bluefs_final_used < bluefs_init_used, (
            f"OSD {osd_id} BlueFS db_used_bytes increased after compaction: "
            f"{bluefs_final_used:,} > {bluefs_init_used:,}"
        )

        # Test OSD compaction on restart with osd_compact_on_start enabled
        log.info(
            "Testing OSD compaction on restart with osd_compact_on_start configuration"
        )

        # Enable osd_compact_on_start configuration for all OSDs
        log.info("Enabling osd_compact_on_start for all OSDs")
        if not mon_obj.set_config(
            section="osd", name="osd_compact_on_start", value="true"
        ):
            log.error("Failed to set osd_compact_on_start for OSDs")
            raise AssertionError("Could not enable osd_compact_on_start configuration")
        else:
            log.info("Successfully enabled osd_compact_on_start for all OSDs")

        # Get BlueFS stats from multiple OSDs before restart
        log.info("Collecting BlueFS stats from 25% of OSDs before restart")
        bluefs_before_restart = get_bluefs_stats_for_multiple_osds(
            rados_obj, stage="before restart"
        )

        if not bluefs_before_restart:
            log.error("Failed to collect BlueFS stats before restart")
            raise AssertionError(
                "Could not collect BlueFS statistics before OSD restart"
            )
        else:
            # Restart ALL sampled OSDs twice (not just once) for consistent measurement
            # following KB : https://access.redhat.com/solutions/7132824
            sampled_osds_for_restart = bluefs_before_restart["sampled_osds"]
            log.info(
                "Restarting %s sampled OSDs twice to trigger compaction on start: %s",
                len(sampled_osds_for_restart),
                sampled_osds_for_restart,
            )

            # First restart of all sampled OSDs
            log.info("=== First restart cycle ===")
            for osd_restart_id in sampled_osds_for_restart:
                log.info(
                    "First restart of OSD %s to trigger compaction on start",
                    osd_restart_id,
                )
                rados_obj.change_osd_state(action="restart", target=osd_restart_id)

            log.info(
                "All %s OSDs are back up after first restart",
                len(sampled_osds_for_restart),
            )

            # Second restart of all sampled OSDs
            log.info("=== Second restart cycle ===")
            for osd_restart_id in sampled_osds_for_restart:
                log.info(
                    "Second restart of OSD %s to trigger compaction on start",
                    osd_restart_id,
                )
                rados_obj.change_osd_state(action="restart", target=osd_restart_id)

            log.info(
                "All %s OSDs are back up after both restarts",
                len(sampled_osds_for_restart),
            )

            # Get BlueFS stats from the same restarted OSDs
            log.info("Collecting BlueFS stats from restarted OSDs")
            bluefs_after_restart = get_bluefs_stats_for_multiple_osds(
                rados_obj, stage="after restart"
            )

            if not bluefs_after_restart:
                log.error("Failed to collect BlueFS stats after restart")
                raise AssertionError(
                    "Could not collect BlueFS statistics after OSD restart"
                )

            # Verify BlueFS db_used_bytes reduction after both restarts (average across all restarted OSDs)
            verify_bluefs_reduction(
                bluefs_before_restart,
                bluefs_after_restart,
                "OSD restart (2x) with osd_compact_on_start",
            )

        log.info(
            "OSD restart test (2 restart cycles) with osd_compact_on_start completed"
        )

        # stop the OSD to check compaction failure
        log.info("Stopping OSD %s to perform compaction again", osd_id)
        rados_obj.change_osd_state(action="stop", target=osd_id)
        try:
            _, err = cephadm.shell([f"ceph tell osd.{osd_id} compact"])
        except Exception as e:
            log.info("expected to fail with below error:")
            log.info(e)
            assert "problem getting command descriptions" in str(e)

        log.info(
            "OSD compaction with and without failure has been successfully verified."
        )
    except Exception as e:
        log.error("Failed with exception: %s", e.__doc__)
        log.exception(e)
        # log cluster health
        rados_obj.log_cluster_health()
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        rados_obj.delete_pool(pool=pool_name)
        if "osd_id" in locals() or "osd_id" in globals():
            rados_obj.change_osd_state(action="start", target=osd_id)

        # Reset osd_compact_on_start config if it was set
        if "bluefs_before_restart" in locals():
            log.info("Resetting osd_compact_on_start config for all OSDs")
            if not mon_obj.remove_config(section="osd", name="osd_compact_on_start"):
                log.warning("Failed to reset osd_compact_on_start config for OSDs")

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

    log.info("Completed testing effects of large number of omap entries on pools ")
    return 0


def verify_bluefs_reduction(
    bluefs_before, bluefs_after, stage_name, allow_increase=False, threshold_pct=0
):
    """
    Verify BlueFS db_used_bytes reduction and log results

    Args:
        bluefs_before: BlueFS stats dict before operation
        bluefs_after: BlueFS stats dict after operation
        stage_name: Name of the stage (for logging)
        allow_increase: Whether to allow small increases
        threshold_pct: Maximum allowed increase percentage (only used if allow_increase=True)

    Returns:
        bool: True if reduction verified successfully

    Raises:
        AssertionError: If unexpected increase occurs
    """
    if not bluefs_before or not bluefs_after:
        log.warning("Missing BlueFS stats for %s verification", stage_name)
        return False

    avg_used_before = bluefs_before["averages"]["db_used_bytes"]
    avg_used_after = bluefs_after["averages"]["db_used_bytes"]
    avg_reduction = avg_used_before - avg_used_after
    reduction_pct = (
        (avg_reduction / avg_used_before * 100) if avg_used_before > 0 else 0
    )

    log.info(
        "Average BlueFS db_used_bytes change (%s):\n"
        "  Before: %s bytes (avg)\n"
        "  After:  %s bytes (avg)\n"
        "  Change: %+s bytes (%+.2f%%)\n"
        "  OSDs sampled before: %s\n"
        "  OSDs sampled after:  %s",
        stage_name,
        f"{avg_used_before:,}",
        f"{avg_used_after:,}",
        f"{avg_reduction:,}",
        reduction_pct,
        bluefs_before["valid_count"],
        bluefs_after["valid_count"],
    )

    if avg_used_after < avg_used_before:
        log.info(
            "Average BlueFS db_used_bytes has reduced or remained stable after %s",
            stage_name,
        )
        return True
    else:
        increase_pct = (avg_used_after - avg_used_before) / avg_used_before * 100
        log.error(
            "Average BlueFS db_used_bytes increased after %s: %s > %s (%.2f%% increase)",
            stage_name,
            f"{avg_used_after:,}",
            f"{avg_used_before:,}",
            increase_pct,
        )

        if allow_increase and increase_pct <= threshold_pct:
            log.warning(
                "Small increase (%.2f%%) accepted (threshold: %s%%)",
                increase_pct,
                threshold_pct,
            )
            return True
        else:
            raise AssertionError(
                f"BlueFS db_used_bytes increased by {increase_pct:.2f}% after {stage_name}. "
                f"Expected reduction or minimal increase."
            )


def get_random_osd_for_object(rados_obj, pool_name, obj_list, purpose=""):
    """
    Get a random object and its primary OSD

    Args:
        rados_obj: RadosOrchestrator object
        pool_name: Pool name
        obj_list: List of objects to choose from
        purpose: Purpose description for logging

    Returns:
        tuple: (object_name, osd_id)
    """
    obj = random.choice(obj_list)
    osd_id = rados_obj.get_osd_map(pool=pool_name, obj=obj)["acting_primary"]
    purpose_desc = f" for {purpose}" if purpose else ""
    log.info("Primary OSD of object %s%s: %s", obj, purpose_desc, osd_id)
    return obj, osd_id


def get_bluefs_stats_for_multiple_osds(
    rados_obj, stage="", sample_percentage=DEFAULT_SAMPLE_PERCENTAGE
):
    """
    Fetch BlueFS statistics for multiple OSDs and calculate averages

    Args:
        rados_obj: RadosOrchestrator object
        stage: Description of the stage (e.g., "before compaction", "after restart")
        sample_percentage: Percentage of UP OSDs to sample (default: 25%)

    Returns:
        dict: Contains individual OSD stats and aggregate statistics
            {
                "osd_stats": {osd_id: bluefs_stats_dict},
                "sampled_osds": [list of OSD IDs],
                "averages": {
                    "db_total_bytes": avg,
                    "db_used_bytes": avg,
                    "gift_bytes": avg,
                    "reclaim_bytes": avg
                },
                "totals": {similar structure}
            }
    """
    stage_desc = f" ({stage})" if stage else ""
    log.info("Fetching BlueFS statistics for multiple OSDs%s", stage_desc)

    # Get all UP OSDs
    try:
        up_osds = rados_obj.get_osd_list(status="up")
        if not up_osds:
            log.error("No UP OSDs found in the cluster")
            return None

        log.info("Total UP OSDs in cluster: %s", len(up_osds))

        # Calculate sample size (25% of UP OSDs, minimum 1)
        sample_size = max(1, int(len(up_osds) * sample_percentage / 100))

        # Randomly select OSDs
        sampled_osds = random.sample(up_osds, sample_size)
        log.info(
            "Sampling %s OSDs (%s%%) for BlueFS stats: %s",
            sample_size,
            sample_percentage,
            sampled_osds,
        )

        # Collect stats from sampled OSDs
        osd_stats = {}
        valid_stats_count = 0

        for osd_id in sampled_osds:
            perf_dump = rados_obj.get_osd_perf_dump(osd_id, filter="bluefs")

            # Extract bluefs stats from the returned structure
            # get_osd_perf_dump returns {"bluefs": {...}} even with filter
            bluefs_stats = (
                perf_dump.get("bluefs", {}) if isinstance(perf_dump, dict) else {}
            )

            if bluefs_stats and bluefs_stats.get("db_total_bytes", 0) > 0:
                osd_stats[osd_id] = bluefs_stats
                valid_stats_count += 1
                log.debug(
                    "OSD %s: db_used=%s, db_total=%s",
                    osd_id,
                    f"{bluefs_stats.get('db_used_bytes', 0):,}",
                    f"{bluefs_stats.get('db_total_bytes', 0):,}",
                )
            else:
                log.warning("OSD %s returned empty or zero BlueFS stats", osd_id)

        # Calculate totals and averages
        totals = {
            "db_total_bytes": sum(
                stats.get("db_total_bytes", 0) for stats in osd_stats.values()
            ),
            "db_used_bytes": sum(
                stats.get("db_used_bytes", 0) for stats in osd_stats.values()
            ),
            "gift_bytes": sum(
                stats.get("gift_bytes", 0) for stats in osd_stats.values()
            ),
            "reclaim_bytes": sum(
                stats.get("reclaim_bytes", 0) for stats in osd_stats.values()
            ),
        }

        averages = {key: value // valid_stats_count for key, value in totals.items()}

        # Log summary
        avg_utilization = (
            (averages["db_used_bytes"] / averages["db_total_bytes"] * 100)
            if averages["db_total_bytes"] > 0
            else 0
        )
        log.info(
            "BlueFS Stats Summary%s (from %s OSDs):\n"
            "  Average db_total_bytes: %s\n"
            "  Average db_used_bytes:  %s\n"
            "  Total db_total_bytes:   %s\n"
            "  Total db_used_bytes:    %s\n"
            "  Average utilization:    %.2f%%",
            stage_desc,
            valid_stats_count,
            f"{averages['db_total_bytes']:,}",
            f"{averages['db_used_bytes']:,}",
            f"{totals['db_total_bytes']:,}",
            f"{totals['db_used_bytes']:,}",
            avg_utilization,
        )

        return {
            "osd_stats": osd_stats,
            "sampled_osds": sampled_osds,
            "valid_count": valid_stats_count,
            "averages": averages,
            "totals": totals,
        }

    except Exception as e:
        log.error("Failed to collect BlueFS stats for multiple OSDs: %s", e)
        log.exception(e)
        return None
