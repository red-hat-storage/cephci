"""
Test Module: Bug #2423971 - EC Divergent Log Reproduction
==========================================================

This test reproduces the critical data corruption/OSD panic bug in Fast EC
(Tracker: https://tracker.ceph.com/issues/74221, BZ: 2423971).

BUG SUMMARY:
============
When a partial write triggers a clone (due to snapshot) and is interrupted by
a peering event, the clone log is sent to ALL shards but the write data only
goes to SOME shards. This creates a divergent log condition where a non-primary
OSD incorrectly concludes that recovery is needed and DELETES the object.

REPRODUCTION SEQUENCE:
=========================================
1. Create EC pool with k=2, m=2, stripe_unit=4096
2. Disable cluster interference (noout, noscrub, noautoscale, balancer off)
3. Enable bluestore_debug_inject_read_err (REQUIRED)
4. Create initial object (16k write)
5. Get acting set and identify shard OSDs
6. Inject EC write error on primary OSD to drop writes to shard 2:
   `ceph daemon osd.$primary injectecwriteerr pool "*" 2 1 0 1`
7. Create snapshot: `rados mksnap`
8. Start partial write in background: `rados put --offset 0` (4k write)
9. Immediately mark shard 1 OSD as down (causes peering + divergent log)
10. Wait for peering to complete
11. Provoke failure: read, write, or append to the object

FAILURE MODES:
==============
- READ: Returns -EIO or silently recovers with error logged
- WRITE/OVERWRITE: OSD panic (assertion failure)
- APPEND: Either panic or silent data corruption (zeros in data range)
- SNAPSHOT: OSD panic

SETUP PHASE
├── Archive existing crashes
├── Save cluster settings
├── Configure: noout, noscrub, nodeep-scrub, balancer off
├── Enable bluestore_debug_inject_read_err
├── Create EC pool (k=2, m=2, stripe_unit=4096, pg_num=1)
└── Create test files (16k, 4k, 8k)

BUG REPRODUCTION PHASE (per iteration)
├── Step 1: Create base object (16k)
├── Step 2: Get acting set (primary, secondary OSDs)
├── Step 3: Inject EC write error on primary
├── Step 4: Create snapshot
├── Step 5: Partial write + OSD down (CRITICAL)
├── Step 6: Wait for peering (30s)
├── Step 7: Provoke failures (read, write, append)
├── Check crashes (with start_time)
├── Restart all OSDs
└── Break if bug_reproduced && fail_on_crash

VALIDATION PHASE
├── Re-enable scrubbing
├── Wait for clean PGs
├── Run deep scrub
└── Final crash check

FINALLY BLOCK
├── Restore OSD flags (noout, noscrub, nodeep-scrub, noup)
├── Restart any down OSDs
├── Remove config overrides
├── Re-enable balancer
├── Delete test pool
├── Delete EC profile
└── Cleanup test files
"""

import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.utils import get_cluster_timestamp
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw) -> int:
    """
    Main test execution for Bug #2423971 EC Divergent Log Reproduction.

    This test follows the exact reproduction steps from the dev script to
    trigger the Fast EC divergent log bug that causes data corruption or
    OSD panic.

    Test Args (in config):
        iterations (int): Max iterations when fail_on_crash=True (default: 10)
        duration (int): Test duration in seconds - always respected (default: 600)
        pool_name (str): Name for the test pool (default: "bug-2423971-pool")
        fail_on_crash (bool): If True, stop on crash or after 'iterations' limit.
                              If False, continue until 'duration' timeout. (default: False)

    Returns:
        0: Test passed - Bug was NOT reproduced, no crashes detected
        1: Test failed - Bug reproduced (crash/corruption detected) or unexpected error
    """
    log.info(run.__doc__)
    config = kw["config"]

    # Initialize cluster objects
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    client_node = ceph_cluster.get_nodes(role="client")[0]

    # Test parameters
    iterations = config.get("iterations", 10)
    duration = config.get("duration", 600)
    pool_name = config.get("pool_name", "bug-2423971-pool")
    fail_on_crash = config.get("fail_on_crash", False)
    profile_name = f"{pool_name}-profile"

    log.info(
        f"\n{'=' * 60}\n"
        f"BUG #2423971 EC DIVERGENT LOG REPRODUCTION TEST\n"
        f"{'=' * 60}\n"
        f"Duration: {duration}s\n"
        f"Max iterations: {iterations} (only if fail_on_crash=True)\n"
        f"Pool: {pool_name}\n"
        f"Fail on crash: {fail_on_crash}\n"
        f"{'=' * 60}"
    )

    log.info("Archiving existing crashes...")
    rados_obj.run_ceph_command(cmd="ceph crash archive-all", client_exec=True)
    time.sleep(2)

    start_time = get_cluster_timestamp(rados_obj.node)
    log.debug(f"Test started at: {start_time}")

    test_failed = False
    bug_reproduced = False
    original_settings = {}

    try:
        log.info(f"\n{'=' * 60}\nSETUP PHASE\n{'=' * 60}")

        log.info("Saving original cluster settings...")
        # Save original settings for restoration
        original_settings = {
            "noout": False,
            "noscrub": False,
            "nodeep-scrub": False,
            "noup": False,
        }

        log.info("Configuring cluster to prevent interference...")

        # Set osd_max_markdown_count to prevent OSD markdown during test
        mon_obj.set_config(
            section="global", name="osd_max_markdown_count", value="99999999"
        )

        # Disable OSD out marking
        rados_obj.run_ceph_command(cmd="ceph osd set noout", client_exec=True)
        original_settings["noout"] = True

        # Disable autoscaling (set default mode to off for new pools)
        mon_obj.set_config(
            section="global", name="osd_pool_default_pg_autoscale_mode", value="off"
        )

        # Disable balancer and scrubbing
        rados_obj.run_ceph_command(cmd="ceph balancer off", client_exec=True)
        rados_obj.run_ceph_command(cmd="ceph osd set noscrub", client_exec=True)
        rados_obj.run_ceph_command(cmd="ceph osd set nodeep-scrub", client_exec=True)
        original_settings["noscrub"] = True
        original_settings["nodeep-scrub"] = True

        # CRITICAL: Enable bluestore debug inject read error
        log.info("Enabling bluestore_debug_inject_read_err (REQUIRED for bug repro)...")
        mon_obj.set_config(
            section="global", name="bluestore_debug_inject_read_err", value="true"
        )

        log.info("Cluster configured for bug reproduction")

        log.info(f"\nCreating EC pool: {pool_name}")
        log.info("  - k=2, m=2 - stripe_unit=4096 - allow_ec_optimizations=true")
        if not rados_obj.create_erasure_pool(
            pool_name=pool_name,
            profile_name=profile_name,
            k=2,
            m=2,
            plugin="jerasure",
            stripe_unit=4096,  # ESSENTIAL: 4k stripe unit
            pg_num=1,
            pg_num_max=1,
            min_size=2,
            erasure_code_use_overwrites=True,
            enable_fast_ec_features=True,
        ):
            raise Exception(f"Failed to create EC pool {pool_name}")

        log.info(f"EC pool {pool_name} created successfully")

        # Wait for PGs to be clean
        if not wait_for_clean_pg_sets(rados_obj, timeout=300, recovery_thread=False):
            log.warning("PGs not clean after pool creation, continuing anyway...")

        # Create test files ONCE (reused across all iterations)
        log.info("Creating test files (16k, 4k, 8k)...")
        client_node.exec_command(
            cmd=(
                "dd if=/dev/urandom of=/tmp/file_16k bs=16k count=1 2>/dev/null; "
                "dd if=/dev/urandom of=/tmp/file_4k bs=4k count=1 2>/dev/null; "
                "dd if=/dev/urandom of=/tmp/file_8k bs=8k count=1 2>/dev/null"
            ),
            sudo=True,
        )

        log.info(f"\n{'=' * 60}\nBUG REPRODUCTION PHASE\n{'=' * 60}")

        phase_start_time = time.time()
        phase_end_time = phase_start_time + duration
        iteration = 0

        # Loop until duration is reached (primary condition)
        # iterations limit only applies when fail_on_crash=True
        while time.time() < phase_end_time:
            iteration += 1

            # Check iteration limit only if fail_on_crash is True
            if fail_on_crash and iteration > iterations:
                log.info(f"Iteration limit ({iterations}) reached")
                break

            remaining_time = int(phase_end_time - time.time())
            log.info(
                f"\n--- Iteration {iteration} (time remaining: {remaining_time}s) ---"
            )

            obj_name = f"test_obj_{iteration}_{int(time.time())}"

            try:
                # Step 1: Create initial object (16k write)
                log.debug(f"Creating base object: {obj_name} (16k)")
                client_node.exec_command(
                    cmd=f"rados -p {pool_name} put {obj_name} /tmp/file_16k",
                    sudo=True,
                )

                # Step 2: Get acting set and identify shard OSDs
                log.debug("Getting acting set for object...")
                osd_map = rados_obj.get_osd_map(pool=pool_name, obj=obj_name)
                if not osd_map or "acting" not in osd_map:
                    log.warning(f"Could not get OSD map for {obj_name}, skipping")
                    continue

                acting = osd_map["acting"]
                if len(acting) < 2:
                    log.warning(f"Acting set too small: {acting}, skipping")
                    continue

                primary_osd = acting[0]  # shard 0 OSD
                secondary_osd = acting[1]  # shard 1 OSD

                log.info(
                    f"Acting set: {acting} (primary=OSD.{primary_osd}, "
                    f"secondary=OSD.{secondary_osd})"
                )

                # Step 3: Inject EC write error on primary OSD
                # "injectecwriteerr pool obj shard type skip inject"
                # shard=2, type=1 (drop sub write), skip=0, inject=1
                log.info(f"Injecting EC write error on OSD.{primary_osd}...")
                inject_cmd = (
                    f"cephadm shell -- ceph daemon osd.{primary_osd} injectecwriteerr "
                    f"{pool_name} '*' 2 1 0 1"
                )
                osd_host = rados_obj.fetch_host_node(
                    daemon_type="osd", daemon_id=str(primary_osd)
                )
                inject_out, _ = osd_host.exec_command(
                    cmd=inject_cmd, sudo=True, check_ec=False
                )
                if "ok" in inject_out.lower():
                    log.info(
                        f"  Inject SUCCESS on OSD.{primary_osd}: {inject_out.strip()}"
                    )
                else:
                    log.error(
                        f"  Inject FAILED on OSD.{primary_osd}: {inject_out.strip()}"
                    )
                    log.error(
                        "  Bug reproduction requires successful inject - skipping"
                    )
                    continue

                # Step 4: Create snapshot
                snap_name = f"snap_{obj_name}"
                log.info(f"Creating snapshot: {snap_name}")
                client_node.exec_command(
                    cmd=f"rados -p {pool_name} mksnap {snap_name}",
                    sudo=True,
                )

                # Step 5: Start partial write in background AND immediately mark OSD down
                log.info("Starting partial write + OSD down sequence (CRITICAL)...")

                # Start partial write in background, set noup, then mark OSD down
                # This exact sequence is required to create the divergent log
                divergent_cmd = (
                    f"rados -p {pool_name} put {obj_name} /tmp/file_4k --offset 0 & "
                    f"ceph osd set noup; "
                    f"ceph osd down {secondary_osd}; "
                    f"wait; "
                    f"ceph osd unset noup"
                )
                client_node.exec_command(cmd=divergent_cmd, sudo=True, check_ec=False)

                # Step 6: Wait for peering to complete
                log.info("Waiting for peering to complete (30s)...")
                time.sleep(30)

                # Step 7: Provoke failure modes - read, write, append
                log.info("Provoking failure modes (read, write@8k, append@16k)...")
                provoke_cmds = (
                    f"rados -p {pool_name} get {obj_name} /tmp/readback 2>&1; "
                    f"rados -p {pool_name} put {obj_name} /tmp/file_8k --offset 8192 2>&1; "
                    f"rados -p {pool_name} put {obj_name} /tmp/file_8k --offset 16384 2>&1"
                )
                provoke_out, _ = client_node.exec_command(
                    cmd=provoke_cmds, sudo=True, check_ec=False
                )

                # Check for errors in operation output
                if provoke_out and (
                    "error" in provoke_out.lower() or "eio" in provoke_out.lower()
                ):
                    log.warning(f"Operation error detected: {provoke_out}")
                    bug_reproduced = True

                # Check for crashes since test started
                if rados_obj.check_crash_status(start_time=start_time):
                    log.error("CRASH DETECTED! Bug reproduced.")
                    bug_reproduced = True

                # Restart all OSDs to ensure they are up before next iteration
                log.info("Restarting all OSD daemons...")
                if not rados_obj.restart_daemon_services(daemon="osd", timeout=120):
                    log.warning("Some OSDs did not restart in time, continuing...")

                # Check if we should stop after bug reproduction
                if bug_reproduced and fail_on_crash:
                    log.info(
                        "BUG REPRODUCED! Stopping iterations (fail_on_crash=True)."
                    )
                    break
                elif bug_reproduced:
                    log.info("Bug reproduced, but continuing (fail_on_crash=False)...")

                time.sleep(20)
                log.info(f"Iteration {iteration} completed")

            except Exception as e:
                log.warning(f"Iteration {iteration} failed with exception: {e}")
                # Check if exception is due to OSD crash (bug reproduced)
                if rados_obj.check_crash_status(start_time=start_time):
                    log.error("CRASH DETECTED! Bug reproduced.")
                    bug_reproduced = True

                # Restart all OSDs to recover
                log.info("Restarting all OSD daemons after exception...")
                rados_obj.restart_daemon_services(daemon="osd", timeout=120)

                # Check if we should stop after bug reproduction
                if bug_reproduced and fail_on_crash:
                    log.info("Stopping iterations (fail_on_crash=True).")
                    break
                elif bug_reproduced:
                    log.info("Continuing despite crash (fail_on_crash=False)...")

        log.info(f"\n{'=' * 60}\nVALIDATION PHASE\n{'=' * 60}")

        # Re-enable scrubbing to detect issues
        log.info("Re-enabling scrubbing for detection...")
        rados_obj.run_ceph_command(cmd="ceph osd unset noscrub", client_exec=True)
        rados_obj.run_ceph_command(cmd="ceph osd unset nodeep-scrub", client_exec=True)

        # Wait for PGs to stabilize
        log.info("Waiting for PGs to stabilize...")
        wait_for_clean_pg_sets(rados_obj, timeout=300, recovery_thread=False)

        # Run deep scrub to detect missing shards or inconsistencies
        log.info("Running deep scrub to detect issues...")
        rados_obj.run_deep_scrub(pool=pool_name)
        time.sleep(60)  # Wait for scrub to complete

        # Final crash check using proper timestamp-based method
        log.info("Checking for crashes...")
        test_end_time = get_cluster_timestamp(rados_obj.node)
        log.debug(
            f"Test workflow completed. Start time: {start_time}, End time: {test_end_time}"
        )
        if rados_obj.check_crash_status(start_time=start_time, end_time=test_end_time):
            log.error("CRASH DETECTED! Bug reproduced.")
            bug_reproduced = True

        log.info("Validation phase completed")

    except Exception as e:
        log.error(f"Test execution failed with exception: {e}")
        log.exception(e)
        test_failed = True

    finally:
        log.info(f"\n{'=' * 60}\nFINALLY BLOCK\n{'=' * 60}")

        # Restore cluster settings
        log.info("Restoring cluster settings...")

        try:
            # Unset OSD flags
            if original_settings.get("noout"):
                rados_obj.run_ceph_command(cmd="ceph osd unset noout", client_exec=True)
            if original_settings.get("noscrub"):
                rados_obj.run_ceph_command(
                    cmd="ceph osd unset noscrub", client_exec=True
                )
            if original_settings.get("nodeep-scrub"):
                rados_obj.run_ceph_command(
                    cmd="ceph osd unset nodeep-scrub", client_exec=True
                )

            # Unset noup if it was left set
            rados_obj.run_ceph_command(cmd="ceph osd unset noup", client_exec=True)

            # Ensure all OSDs are up - restart all OSD services
            log.info("Restarting all OSD daemons to ensure cluster is healthy...")
            rados_obj.restart_daemon_services(daemon="osd", timeout=120)

            # Remove config overrides
            mon_obj.remove_config(
                section="global", name="bluestore_debug_inject_read_err"
            )
            mon_obj.remove_config(section="global", name="osd_max_markdown_count")
            mon_obj.remove_config(
                section="global", name="osd_pool_default_pg_autoscale_mode"
            )

            # Re-enable balancer
            rados_obj.run_ceph_command(cmd="ceph balancer on", client_exec=True)

            log.info("Cluster settings restored")
        except Exception as e:
            log.warning(f"Failed to restore some cluster settings: {e}")

        # Delete test pool and EC profile
        if config.get("cleanup_pool", True):
            log.info(f"Deleting test pool: {pool_name}")
            try:
                rados_obj.delete_pool(pool=pool_name)
                log.info("Test pool deleted")
            except Exception as e:
                log.warning(f"Failed to delete pool: {e}")

            log.info(f"Deleting EC profile: {profile_name}")
            try:
                rados_obj.delete_ec_profile(profile=profile_name)
                log.info("EC profile deleted")
            except Exception as e:
                log.warning(f"Failed to delete EC profile: {e}")

            # Final crash check using proper timestamp-based method
        log.info("Checking for crashes...")
        test_end_time = get_cluster_timestamp(rados_obj.node)
        log.debug(
            f"Test workflow completed. Start time: {start_time}, End time: {test_end_time}"
        )
        if rados_obj.check_crash_status(start_time=start_time, end_time=test_end_time):
            log.error("CRASH DETECTED! Bug reproduced.")
            bug_reproduced = True

        # Cleanup test files
        try:
            client_node.exec_command(
                cmd="rm -f /tmp/file_16k /tmp/file_4k /tmp/file_8k /tmp/readback",
                sudo=True,
                check_ec=False,
            )
        except Exception:
            pass

        log.info("Cleanup phase completed")

    log.info(f"\n{'=' * 60}\nTEST RESULT\n{'=' * 60}")

    if test_failed:
        log.error("TEST FAILED - Unexpected error during test execution")
        return 1

    if bug_reproduced:
        log.error("TEST FAILED - Bug #2423971 reproduced (crash/corruption detected)")
        log.error("  - Check crash logs for OSD panic details")
        return 1

    log.info("TEST PASSED - Bug was NOT reproduced in this run")
    return 0
