"""
Test Module: OSD Thrashing Test with Concurrent I/O

This module performs aggressive OSD thrashing with concurrent I/O workload to verify
cluster stability under stress conditions. It tests both EC and replicated pools,
and checks for any crashes or issues during OSD state transitions.

Test Workflow:
    ├── Crash archive at start
    ├── SETUP PHASE
    │   ├── Create pools from suite file configuration
    │   │   - Supports both EC and replicated pools
    │   │   - Multiple pools with different configurations
    │   ├── Enable EC optimizations for EC pools
    │   └── Configure aggressive recovery settings
    ├── THRASHING PHASE
    │   ├── Concurrent I/O workload (rados bench in bursts)
    │   ├── Comprehensive I/O (various sizes with overwrites, appends, truncates)
    │   ├── FIO workloads on CephFS and RBD
    │   ├── OSD thrashing (mark out → wait → mark in)
    │   ├── CRUSH weight modifications (configurable, default: enabled)
    │   └── PG count thrashing (bulk flag enable/disable, configurable, default: enabled)
    ├── VALIDATION PHASE
    │   ├── Wait for cluster stabilization
    │   ├── Check cluster health
    │   ├── Check PG states (active+clean)
    │   ├── Check for ANY crashes (ceph crash ls-new)
    │   └── Report findings and fail if crashes detected
    └── CLEANUP PHASE
        └── Delete test pools and profiles

"""

import concurrent.futures as cf
import random
import time
from typing import Dict, List

import yaml

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from ceph.rados.utils import get_cluster_timestamp
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from tests.rados.test_four_node_ecpool import create_comprehensive_test_objects
from utility.log import Log

log = Log(__name__)

# Path to pool configurations file
POOL_CONFIGS_FILE = "conf/tentacle/rados/test-confs/pool-configurations.yaml"

# Cache for pool configurations (loaded once per module)
_POOL_CONFIGS_CACHE = None


def run(ceph_cluster, **kw):
    """
    Main test execution for OSD thrashing with concurrent I/O workload.

    This test creates pools (EC and/or replicated) and performs aggressive OSD
    thrashing with concurrent I/O to verify cluster stability. Any crashes
    detected during the test are reported and cause test failure.

    Test Args (in config):
        pool_configs (list): List of pool configurations to create (REQUIRED)
            - pool_type (str): "erasure" or "replicated"
            - sample_pool_id (str): Pool ID from conf/tentacle/rados/test-confs/pool-configurations.yaml
            - Any additional parameters will override file config

        iterations (int): Number of OSD thrash iterations (default: 20)
        duration (int): Test duration in seconds (default: 600)
        aggressive (bool): Enable aggressive mode (default: True)
        enable_crush_thrashing (bool): Enable CRUSH weight thrashing (default: True)
        enable_pg_thrashing (bool): Enable PG count thrashing via bulk flag and scrubbing (default: True)
        compression (bool): Enable pool compression with mode=force (default: False)
        cleanup_pools (bool): Delete pools after test (default: True)

    Returns:
        0: Test passed - No crashes detected, cluster remained stable
        1: Test failed - Crashes detected or unexpected error occurred
    """
    log.info(run.__doc__)
    config = kw["config"]

    # Initialize cluster objects
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    client_node = ceph_cluster.get_nodes(role="client")[0]

    # Test parameters
    iterations = config.get("iterations", 20)
    duration = config.get("duration", 600)
    aggressive = config.get("aggressive", True)
    enable_crush_thrashing = config.get("enable_crush_thrashing", True)
    enable_pg_thrashing = config.get("enable_pg_thrashing", True)
    compression = config.get("compression", False)
    compression_v2 = config.get("compression_v2", False)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)

    # Log test parameters in a single block for efficiency
    test_params = (
        f"\n{'=' * 60}\n"
        f"OSD THRASHING TEST WITH CONCURRENT I/O\n"
        f"{'=' * 60}\n"
        f"Test parameters:\n"
        f"  Iterations: {iterations}\n"
        f"  Duration: {duration}s\n"
        f"  Aggressive mode: {aggressive}\n"
        f"  CRUSH thrashing: {enable_crush_thrashing}\n"
        f"  PG thrashing via bulk flag and scrubbing: {enable_pg_thrashing}\n"
        f"  Compression (mode=force): {compression}\n"
        f"  RADOS pools from config: {len(config.get('pool_configs', []))}\n"
        f"  Additional pools: CephFS (2 pools) + RBD (2 pools)"
    )
    log.info(test_params)

    # Archive existing crashes at start
    log.info("Archiving existing crashes...")
    rados_obj.run_ceph_command(cmd="ceph crash archive-all", client_exec=True)
    time.sleep(5)
    start_time = get_cluster_timestamp(rados_obj.node)
    log.debug(f"Test workflow started. Start time: {start_time}")

    # Variables to track created resources
    created_pools = []
    test_failed = False
    fs_name = None
    cephfs_mount_path = None
    rbd_mount_path = None
    rbd_device_path = None

    try:
        # POOL CREATION PHASE - RADOS, CephFS, RBD (Parallel)
        log.info(f"\n{'=' * 60}\nSETUP PHASE\n{'=' * 60}")

        # Get OSD list
        osd_list = rados_obj.get_osd_list(status="up")
        log.info(f"\nPool Creation Phase - RADOS, CephFS & RBD (Parallel)\n{'-' * 60}")

        # Launch pool creation tasks in parallel
        with cf.ThreadPoolExecutor(max_workers=3) as pool_executor:
            log.info("Creating pools in parallel...")

            # Submit all pool creation tasks
            rados_future = pool_executor.submit(create_rados_pools, rados_obj, config)
            cephfs_future = pool_executor.submit(
                rados_obj.create_cephfs_filesystem_mount,
                client_node,
                pool_type="erasure",
            )
            rbd_future = pool_executor.submit(
                rados_obj.create_ec_rbd_pools,
                pool_type="erasure",
            )

            # Collect results (order: RADOS, CephFS, RBD)
            try:
                rados_pools = rados_future.result()
                created_pools.extend(rados_pools)
                log.info(f"Created {len(rados_pools)} RADOS pool(s)")

                fs_name, cephfs_mount_path, cephfs_pools = cephfs_future.result()
                created_pools.extend(cephfs_pools)
                log.info(f"Created CephFS: {fs_name} at {cephfs_mount_path}")

                rbd_mount_path, rbd_device_path, rbd_pools = rbd_future.result()
                created_pools.extend(rbd_pools)
                log.info(f"Created RBD pools at {rbd_mount_path}")
            except Exception as e:
                log.error(f"Failed to create pools: {e}")
                raise

        log.info(f"\nTotal pools created: {len(created_pools)}")
        log.info("Pool creation phase completed (parallel execution)")

        # Define compression algorithms for display
        compression_algorithms = ["snappy", "zlib"]

        # Enable compression on all pools if configured
        if compression or compression_v2:
            if compression_v2:
                log.info("\n\nEnabling compression v2 on pool (mode=force)...\n")
                mon_obj.set_config(
                    section="global",
                    name="bluestore_write_v2",
                    value="true",
                )
                assert rados_obj.restart_daemon_services(daemon="osd")
            else:
                log.info("\n\nEnabling compression v1 on pool (mode=force)...\n")
            log.info("\n\nEnabling compression on all pools (mode=force)...\n")
            for idx, pool in enumerate(created_pools):
                pool_name = pool["pool_name"]
                # Alternate between snappy and zlib
                algorithm = compression_algorithms[idx % 2]
                log.info(f"  Enabling {algorithm} compression on {pool_name}")
                rados_obj.pool_inline_compression(
                    pool_name=pool_name,
                    compression_mode="force",
                    compression_algorithm=algorithm,
                )
            log.info("\n\nCompression enabled on all pools\n")

        # Print Pool Configuration Details
        log.info("=" * 70)
        log.info("POOL CONFIGURATION SUMMARY")
        log.info("=" * 70)
        log.info("Total Pools Created: %s", len(created_pools))
        log.info("")

        for idx, pool in enumerate(created_pools):
            pool_name = pool["pool_name"]
            pool_type = pool["pool_type"]
            client_type = pool.get("client", "rados")

            log.info("Pool %s/%s:", idx + 1, len(created_pools))
            log.info("  Name            : %s", pool_name)
            log.info("  Type            : %s", pool_type)
            log.info("  Client          : %s", client_type)
            log.info(f"pool details: {rados_obj.get_pool_details(pool_name)}")
            log.info("")

        log.info("=" * 70)
        # Wait for PGs to be active+clean after pool creation
        log.info("Checking PG states post pool creation...")
        if not wait_for_clean_pg_sets(rados_obj, timeout=600):
            log.error("Not all PGs are active+clean")
            raise Exception("Not all PGs are active+clean post creation")

        # Configure recovery settings for aggressive recovery
        log.info("Configuring aggressive recovery settings...")
        recovery_config = {"osd_max_backfills": 15, "osd_recovery_max_active": 20}
        rados_obj.change_recovery_threads(config=recovery_config, action="set")

        log.info("Setup phase completed successfully")

        # THRASHING PHASE
        log.info(f"\n{'=' * 60}\nTHRASHING PHASE\n{'=' * 60}")

        # Stop flag for coordinated shutdown of background threads
        stop_flag = {"stop": False}

        # Launch background workload threads
        log.info("Launching background I/O workload threads...")

        # Calculate worker count: 5 base threads + optional CRUSH + optional PG
        max_workers = (
            5 + (1 if enable_crush_thrashing else 0) + (1 if enable_pg_thrashing else 0)
        )

        with cf.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []

            # I/O workload burst thread (rados bench) - RADOS pools
            futures.append(
                executor.submit(
                    io_workload_burst, rados_obj, created_pools, duration, stop_flag
                )
            )

            # Comprehensive I/O workload thread (various sizes + operations) - RADOS pools
            futures.append(
                executor.submit(
                    comprehensive_io_workload,
                    rados_obj,
                    created_pools,
                    duration,
                    stop_flag,
                )
            )

            # FIO workload on CephFS
            futures.append(
                executor.submit(
                    _run_fio_workload,
                    client_node,
                    cephfs_mount_path,
                    "cephfs",
                    duration,
                    stop_flag,
                )
            )

            # FIO workload on RBD
            futures.append(
                executor.submit(
                    _run_fio_workload,
                    client_node,
                    rbd_mount_path,
                    "rbd",
                    duration,
                    stop_flag,
                )
            )

            # OSD thrashing thread (main thrashing operation)
            futures.append(
                executor.submit(
                    thrash_osds, rados_obj, osd_list, iterations, aggressive, stop_flag
                )
            )

            # CRUSH weight thrashing (default: enabled)
            if enable_crush_thrashing:
                log.info("CRUSH weight thrashing enabled")
                futures.append(
                    executor.submit(
                        thrash_crush_weights,
                        rados_obj,
                        osd_list,
                        iterations // 2,
                        stop_flag,
                    )
                )

            # PG count thrashing (default: enabled)
            if enable_pg_thrashing:
                log.info("PG count thrashing enabled")
                futures.append(
                    executor.submit(
                        thrash_pg_count,
                        rados_obj,
                        pool_obj,
                        created_pools,
                        iterations // 3,
                        stop_flag,
                    )
                )

            # Monitor thrashing operations with efficient waiting
            log.info(f"Thrashing operations in progress (max duration: {duration}s)...")

            # Use as_completed with timeout for efficient monitoring
            try:
                for future in cf.as_completed(futures, timeout=duration):
                    log.debug("A thrashing thread completed")

                log.info("All thrashing operations completed before timeout")
            except cf.TimeoutError:
                log.info(
                    f"Duration limit ({duration}s) reached, some operations may still be running"
                )

            # Signal threads to stop
            stop_flag["stop"] = True
            log.info("Signaling threads to stop...")

            # Wait for threads to finish (with timeout for I/O completion)
            done, not_done = cf.wait(futures, timeout=300)
            log.info(
                f"\nThread completion: {len(done)}/{len(futures)} completed within timeout"
            )
            if not_done:
                log.warning(f"{len(not_done)} thread(s) did not complete in time")

            # Collect results and check for exceptions
            log.info("Checking thread results...")
            for future in done:
                try:
                    result = future.result()
                    log.debug(f"Thread completed with result: {result}")
                except Exception as e:
                    log.error(f"Thread failed with exception: {e}")
                    test_failed = True

        log.info("Thrashing phase completed")

        # VALIDATION PHASE
        log.info(f"\n{'=' * 60}\nVALIDATION PHASE\n{'=' * 60}")

        # Wait for PGs to be active+clean (this includes stabilization time)
        log.info("Waiting for PGs to reach active+clean state...")
        if not wait_for_clean_pg_sets(rados_obj, timeout=600):
            log.error("Not all PGs are active+clean")
            raise Exception("Not all PGs are active+clean")

        # Check cluster health
        log.info("Checking cluster health and PG states...")
        rados_obj.log_cluster_health()

        log.info("\nValidation phase completed")

    except Exception as e:
        log.error(f"Test execution failed with exception: {e}")
        log.exception(e)
        test_failed = True

    finally:
        # CLEANUP PHASE
        log.info(f"\n{'=' * 60}\nCLEANUP PHASE\n{'=' * 60}")

        # Reset recovery settings to defaults
        log.info("Resetting recovery settings to defaults...")
        rados_obj.change_recovery_threads(config={}, action="rm")

        # Clean up resources (if configured)
        if config.get("cleanup_pools", True) and created_pools:
            log.debug("Cleaning up test resources...")

            # Parallel cleanup of CephFS and RBD
            with cf.ThreadPoolExecutor(max_workers=2) as cleanup_executor:
                # Submit cleanup tasks
                cephfs_cleanup = (
                    cleanup_executor.submit(
                        _cleanup_cephfs, client_node, cephfs_mount_path, fs_name
                    )
                    if (cephfs_mount_path or fs_name)
                    else None
                )

                rbd_cleanup = (
                    cleanup_executor.submit(
                        _cleanup_rbd, client_node, rbd_mount_path, rbd_device_path
                    )
                    if rbd_mount_path
                    else None
                )

                # Wait for cleanup tasks to complete
                if cephfs_cleanup:
                    try:
                        cephfs_cleanup.result()
                    except Exception as e:
                        log.warning(f"CephFS cleanup failed: {e}")

                if rbd_cleanup:
                    try:
                        rbd_cleanup.result()
                    except Exception as e:
                        log.warning(f"RBD cleanup failed: {e}")

            # Clean up rados pools sequentially (depends on CephFS/RBD cleanup)
            log.info(f"Deleting {len(created_pools)} pool(s)...")
            failed_pools = []

            for pool in created_pools:
                pool_name = pool["pool_name"]
                try:
                    rados_obj.delete_pool(pool=pool_name)
                except Exception as e:
                    log.error(f"Failed to delete pool {pool_name}: {e}")
                    failed_pools.append(pool_name)

            if failed_pools:
                log.warning(
                    f"Failed to delete {len(failed_pools)} pool(s): {failed_pools}"
                )
            else:
                log.info("All pools deleted successfully")
        else:
            log.info("Skipping cleanup (cleanup_pools=False or no pools created)")

        test_end_time = get_cluster_timestamp(rados_obj.node)
        log.debug(f"Test completed. Start: {start_time}, End: {test_end_time}")

        # Final crash check - fail if any crashes detected
        try:
            if rados_obj.check_crash_status(
                start_time=start_time, end_time=test_end_time
            ):
                log.error("Test failed due to crash detected")
                test_failed = True
        except Exception as e:
            log.error(f"Crash check failed: {e}")
            test_failed = True

        log.info("Cleanup phase completed")

    # TEST RESULT
    log.info(f"\n{'=' * 60}\nTEST RESULT\n{'=' * 60}")

    if test_failed:
        log.error("TEST FAILED - Exceptions occurred during test execution")
        return 1

    log.info("TEST PASSED - No crashes detected, cluster remained stable")
    return 0


def create_rados_pools(rados_obj: RadosOrchestrator, config: Dict) -> List[Dict]:
    """
    Create RADOS pools from file-based configuration.

    Args:
        rados_obj: RadosOrchestrator object
        config: Test configuration with pool_configs

    Returns:
        List of created pool configurations
    """
    global _POOL_CONFIGS_CACHE

    created_pools = []
    pool_configs = config.get("pool_configs", [])

    # Load pool configs from file (with caching)
    if _POOL_CONFIGS_CACHE is None:
        log.debug(f"Loading pool configs from: {POOL_CONFIGS_FILE}")
        with open(POOL_CONFIGS_FILE, "r") as f:
            _POOL_CONFIGS_CACHE = yaml.safe_load(f)
        log.debug("Pool configs cached for future use")

    all_configs = _POOL_CONFIGS_CACHE

    # Skip keys when applying overrides (use set for O(1) lookup)
    skip_keys = {"pool_type", "sample_pool_id"}

    # Create pools from file references
    for ref in pool_configs:
        pool_type = ref["pool_type"]
        sample_id = ref["sample_pool_id"]
        pool_config = all_configs[pool_type][sample_id].copy()

        # Apply overrides from ref (optimized with set lookup)
        pool_config.update({k: v for k, v in ref.items() if k not in skip_keys})

        pool_name = pool_config["pool_name"]
        log.info(f"Creating {pool_type} pool: {pool_name}")

        # Remove pool_type from params
        pool_params = {k: v for k, v in pool_config.items() if k != "pool_type"}

        if pool_type == "erasure":
            # Set EC pool parameters
            pool_params.update(
                {
                    "name": pool_name,
                    "profile_name": f"{pool_name}-profile",
                    "enable_fast_ec_features": True,
                    "stripe_unit": 16384,
                }
            )

            # Auto-set CRUSH failure domain to 'osd' if k+m > 4
            k = pool_params.get("k", 2)
            m = pool_params.get("m", 2)
            if (k + m) > 4:
                pool_params["crush-failure-domain"] = "osd"

            if not rados_obj.create_erasure_pool(**pool_params):
                raise Exception(f"Failed to create EC pool {pool_name}")

            created_pools.append(
                {
                    "pool_name": pool_name,
                    "pool_type": "erasure",
                    "profile_name": pool_params["profile_name"],
                }
            )
        else:  # replicated
            pool_params["pool_name"] = pool_name

            if not rados_obj.create_pool(**pool_params):
                raise Exception(f"Failed to create replicated pool {pool_name}")

            created_pools.append(
                {
                    "pool_name": pool_name,
                    "pool_type": "replicated",
                }
            )
        log.debug(f"Created {pool_name}")

    return created_pools


def io_workload_burst(
    rados_obj: RadosOrchestrator, pools: List[Dict], duration: int, stop_flag: Dict
) -> int:
    """
    Run rados bench in bursts with cleanup on RADOS pools.

    Args:
        rados_obj: RadosOrchestrator object
        pools: List of pool configurations
        duration: Total duration in seconds
        stop_flag: Dict with 'stop' key to signal early termination

    Returns:
        Number of bursts completed
    """
    # Filter RADOS pools once (exclude CephFS and RBD pools)
    rados_pools = tuple(p for p in pools if p.get("client") not in ("cephfs", "rbd"))

    if not rados_pools:
        log.warning("No RADOS pools found, skipping rados bench workload")
        return 0

    log.debug(
        f"Starting rados bench burst workload on {len(rados_pools)} pool(s) (duration: {duration}s)"
    )
    burst_count = 0
    end_time = time.time() + duration

    while time.time() < end_time:
        if stop_flag.get("stop"):
            break

        burst_count += 1

        # Run rados bench on each RADOS pool
        for pool in rados_pools:
            rados_obj.bench_write(
                pool_name=pool["pool_name"],
                rados_write_duration=30,
                byte_size="64K",
                nocleanup=False,
                verify_stats=False,
                check_ec=False,
            )

        if stop_flag.get("stop"):
            break
        time.sleep(5)

    log.info(f"Rados bench burst completed: {burst_count} bursts")
    return burst_count


def comprehensive_io_workload(
    rados_obj: RadosOrchestrator, pools: List[Dict], duration: int, stop_flag: Dict
) -> int:
    """
    Run comprehensive I/O with various object sizes and operations on RADOS pools.

    Creates objects with unique names per iteration to avoid conflicts.

    Args:
        rados_obj: RadosOrchestrator object
        pools: List of pool configurations
        duration: Total duration in seconds
        stop_flag: Dict with 'stop' key to signal early termination

    Returns:
        Number of iterations completed
    """
    # Filter RADOS pools once (exclude CephFS and RBD pools)
    rados_pools = tuple(p for p in pools if p.get("client") not in ("cephfs", "rbd"))

    if not rados_pools:
        log.info("No RADOS pools found, skipping comprehensive I/O workload")
        return 0

    log.info(
        f"Starting comprehensive I/O workload on {len(rados_pools)} pool(s) (duration: {duration}s)"
    )
    iteration = 0
    end_time = time.time() + duration
    stripe_unit = 16384  # Default 16KB stripe unit

    while time.time() < end_time:
        if stop_flag.get("stop"):
            break

        for pool in rados_pools:
            if stop_flag.get("stop"):
                break

            create_comprehensive_test_objects(
                rados_obj=rados_obj,
                pool_name=pool["pool_name"],
                stripe_unit=stripe_unit,
                obj_prefix=f"_{int(time.time() * 1000)}",
            )
            iteration += 1

        if stop_flag.get("stop"):
            break
        time.sleep(random.uniform(5, 10))

    log.info(f"Comprehensive I/O completed: {iteration} iterations")
    return iteration


def _run_fio_workload(
    client_node, mount_path: str, workload_name: str, duration: int, stop_flag: Dict
) -> int:
    """
    Run FIO workload with file creation and deletion on a pre-mounted filesystem.

    Args:
        client_node: Client node to execute FIO
        mount_path: Mount path
        workload_name: Name for the workload (for logging)
        duration: Total duration in seconds
        stop_flag: Dict with 'stop' key to signal early termination

    Returns:
        Number of FIO runs completed
    """
    log.info(
        f"Starting FIO workload ({workload_name}) with file create/delete on: {mount_path}"
    )
    runs = 0
    end_time = time.time() + duration

    # FIO command with file creation and deletion
    # - create_on_open=1: Create files on open
    # - unlink=1: Delete files after completion
    # - nrfiles=10: Use 10 files per job
    # - file_service_type=random: Random file selection for more churn
    fio_cmd = (
        f"fio --name={workload_name}-thrash --directory={mount_path} "
        "--rw=randrw --bs=4k --size=100M --numjobs=4 --ioengine=libaio --direct=1 "
        "--runtime=30 --time_based --group_reporting "
        "--create_on_open=1 --unlink=1 --nrfiles=10 --file_service_type=random"
    )

    try:
        # Run FIO in loop (each run creates and deletes files)
        while time.time() < end_time:
            if stop_flag.get("stop"):
                break

            runs += 1
            client_node.exec_command(cmd=fio_cmd, sudo=True, check_ec=False)

            if stop_flag.get("stop"):
                break
            time.sleep(random.uniform(5, 10))

        log.info(f"FIO {workload_name} workload completed: {runs} runs")
        return runs

    except Exception as e:
        log.warning(f"FIO {workload_name} workload error: {e}")
        return runs


def _restore_osds_to_in(rados_obj: RadosOrchestrator, osd_ids: List[int]) -> None:
    """Restore OSDs to 'in' state, logging failures but continuing."""
    if not osd_ids:
        return

    log.info(f"Restoring {len(osd_ids)} OSD(s) to 'in' state: {osd_ids}")
    for osd_id in osd_ids:
        try:
            rados_obj.update_osd_state_on_cluster(osd_id=osd_id, state="in")
            time.sleep(5)
        except Exception as e:
            log.error(f"Failed to restore OSD.{osd_id}: {e}")


def thrash_osds(
    rados_obj: RadosOrchestrator,
    osd_list: List[int],
    iterations: int,
    aggressive: bool,
    stop_flag: Dict,
) -> int:
    """
    Thrash OSDs by marking out/in to trigger PG remapping.
    Ensures all OSDs are marked back 'in' when stop signal is received.

    Args:
        rados_obj: RadosOrchestrator object
        osd_list: List of OSD IDs
        iterations: Number of thrashing iterations
        aggressive: Enable aggressive mode (long waits between iterations)
        stop_flag: Dict with 'stop' key to signal early termination

    Returns:
        Number of iterations completed
    """
    log.info(
        f"Starting OSD thrashing (iterations: {iterations}, aggressive: {aggressive})"
    )

    # Pre-calculate sleep ranges and max OSDs to thrash
    sleep_duration = (8, 15) if aggressive else (3, 6)
    max_to_thrash = max(1, min(3, len(osd_list) // 3))

    completed = 0
    osds_currently_out = []

    try:
        for iteration in range(iterations):
            if stop_flag.get("stop"):
                break

            # Select random OSDs to thrash
            osds_to_thrash = random.sample(osd_list, random.randint(1, max_to_thrash))
            log.info(
                f"Iteration {iteration + 1}/{iterations}: Thrashing {osds_to_thrash}"
            )

            for osd_id in osds_to_thrash:
                rados_obj.update_osd_state_on_cluster(osd_id=osd_id, state="out")
                osds_currently_out.append(osd_id)

                # Sleep after marking out
                time.sleep(random.uniform(*sleep_duration))

                # Mark in
                rados_obj.update_osd_state_on_cluster(osd_id=osd_id, state="in")
                osds_currently_out.remove(osd_id)

                # Short sleep after marking in
                time.sleep(random.uniform(*sleep_duration))

            completed += 1

            # Sleep between iterations
            time.sleep(random.uniform(*sleep_duration))

    finally:
        # restore any OSDs still marked out
        if osds_currently_out:
            log.info(f"Restoring {len(osds_currently_out)} OSD(s) to 'in' state...")
            _restore_osds_to_in(rados_obj, osds_currently_out)

    log.info(f"OSD thrashing completed: {completed} iterations")
    return completed


def _restore_crush_weight(
    rados_obj: RadosOrchestrator, osd_id: int, weight: float
) -> None:
    """Restore CRUSH weight, logging failure but continuing."""
    try:
        rados_obj.run_ceph_command(
            cmd=f"ceph osd crush reweight osd.{osd_id} {weight}",
            client_exec=True,
        )
        log.debug(f"Restored OSD.{osd_id} weight to {weight}")
    except Exception as e:
        log.error(f"Failed to restore OSD.{osd_id} weight: {e}")


def thrash_crush_weights(
    rados_obj: RadosOrchestrator, osd_list: List[int], iterations: int, stop_flag: Dict
) -> int:
    """
    Thrash CRUSH weights to force data movement.
    Fetches original weights and restores them when stop signal is received.

    Args:
        rados_obj: RadosOrchestrator object
        osd_list: List of OSD IDs
        iterations: Number of iterations
        stop_flag: Dict with 'stop' key to signal early termination

    Returns:
        Number of iterations completed
    """
    log.info(f"Starting CRUSH weight thrashing (iterations: {iterations})")

    # Fetch original CRUSH weights for all OSDs
    osd_tree = rados_obj.run_ceph_command(cmd="ceph osd tree")
    osd_list_set = set(osd_list)
    original_weights = {
        node["id"]: node.get("crush_weight", 1.0)
        for node in osd_tree.get("nodes", [])
        if node.get("type") == "osd" and node["id"] in osd_list_set
    }
    log.info(f"Captured original weights for {len(original_weights)} OSD(s)")

    completed = 0
    current_modified_osd = None
    current_original_weight = None

    try:
        for iteration in range(iterations):
            if stop_flag.get("stop"):
                break

            # Select random OSD and get its original weight
            osd_id = random.choice(osd_list)
            original_weight = original_weights.get(osd_id, 1.0)
            log.info(
                f"Iteration {iteration + 1}/{iterations}: OSD.{osd_id} (weight: {original_weight})"
            )

            # Reduce weight to 50%
            rados_obj.run_ceph_command(
                cmd=f"ceph osd crush reweight osd.{osd_id} {original_weight * 0.5}",
                client_exec=True,
            )
            current_modified_osd = osd_id
            current_original_weight = original_weight

            # Sleep with reduced weight
            time.sleep(random.uniform(30, 60))

            # Restore weight to original
            _restore_crush_weight(rados_obj, osd_id, original_weight)
            current_modified_osd = None
            current_original_weight = None
            completed += 1

            # Sleep between iterations
            time.sleep(random.uniform(30, 60))

    finally:
        # restore any OSD with modified weight
        if current_modified_osd is not None:
            log.info(f"Restoring CRUSH weight for OSD.{current_modified_osd}...")
            _restore_crush_weight(
                rados_obj, current_modified_osd, current_original_weight
            )

    log.info(f"CRUSH weight thrashing completed: {completed} iterations")
    return completed


def thrash_pg_count(
    rados_obj: RadosOrchestrator,
    pool_obj: PoolFunctions,
    pools: List[Dict],
    iterations: int,
    stop_flag: Dict,
) -> int:
    """
    Thrash PG count by enabling/disabling bulk flag to trigger PG splits/merges.
    Removes bulk flag when stop signal is received.

    Also triggers scrubbing & deep scrubbing on all pools

    Args:
        rados_obj: RadosOrchestrator object
        pool_obj: PoolFunctions object for bulk flag operations
        pools: List of pool configurations
        iterations: Number of iterations
        stop_flag: Dict with 'stop' key to signal early termination

    Returns:
        Number of iterations completed
    """
    log.info(f"Starting PG count thrashing and scrubbing (iterations: {iterations})")

    # Filter RADOS pools only (exclude CephFS and RBD pools)
    rados_pools = tuple(p for p in pools if p.get("client") not in ("cephfs", "rbd"))

    if not rados_pools:
        log.info("No RADOS pools found, skipping PG count thrashing and scrubbing")
        return 0

    log.info(f"Will thrash PG count and scrub PGs on {len(rados_pools)} RADOS pool(s)")

    log.info("Setting pg_num_max for all pools...")
    for pool in rados_pools:
        pool_name = pool["pool_name"]
        init_pg_count = rados_obj.get_pool_property(pool=pool_name, props="pg_num")[
            "pg_num"
        ]
        pg_num_max = 1 << init_pg_count.bit_length()
        rados_obj.set_pool_property(
            pool=pool_name, props="pg_num_max", value=pg_num_max
        )

    completed = 0
    bulk_enabled_pools = []

    try:
        for iteration in range(iterations):
            if stop_flag.get("stop"):
                break

            log.info(
                f"Iteration {iteration + 1}/{iterations}: PG thrashing and deep scrubbing on all pools"
            )

            # Phase 1: Enable bulk flag and deep scrub on ALL pools
            for pool in rados_pools:
                pool_name = pool["pool_name"]
                pool_obj.set_bulk_flag(pool_name=pool_name)
                bulk_enabled_pools.append(pool_name)
                rados_obj.run_deep_scrub(pool=pool_name)

            if bulk_enabled_pools:
                pool_count = len(bulk_enabled_pools)
                log.info(
                    f"  Enabled bulk on {pool_count} pool(s), sleeping for PG splits and deep scrubbing..."
                )

                # Phase 2: Sleep while ALL pools split PGs simultaneously and deep scrubbing
                time.sleep(random.uniform(90, 120))

                # Phase 3: Remove bulk flag from ALL pools and scrub PGs
                log.info(
                    f"  Removing bulk from {pool_count} pool(s) and scrubbing PGs..."
                )
                for pool_name in bulk_enabled_pools:
                    pool_obj.rm_bulk_flag(pool_name=pool_name)
                    rados_obj.run_scrub(pool=pool_name)
                bulk_enabled_pools.clear()

                # Phase 4: Sleep for PG merges and scrubbing
                log.info(
                    f"  Sleeping while {pool_count} pool(s) merge PGs and scrubbing PGs..."
                )
                time.sleep(random.uniform(90, 120))

                completed += 1
            else:
                log.warning(
                    f"Iteration {iteration + 1}: No pools had bulk enabled, skipping PG thrashing and scrubbing"
                )
    except Exception as e:
        log.error(f"Exception in thrash_pg_count: {e}")
        return completed
    finally:
        # cleanup of any remaining bulk flags
        if bulk_enabled_pools:
            log.info("Cleaning up bulk flags PGs on remaining pools...")
            for pool_name in bulk_enabled_pools:
                pool_obj.rm_bulk_flag(pool_name=pool_name)

    log.info(f"PG count thrashing and scrubbing completed: {completed} iterations")
    return completed


def _cleanup_cephfs(client_node, mount_path: str, fs_name: str) -> None:
    """
    Clean up CephFS mount and filesystem.

    Args:
        client_node: Client node to execute commands
        mount_path: CephFS mount path
        fs_name: Filesystem name
    """
    if mount_path:
        log.info(f"Unmounting CephFS from: {mount_path}")
        try:
            mount_path = mount_path.rstrip("/")
            client_node.exec_command(
                cmd=f"umount {mount_path}", sudo=True, check_ec=False
            )
            client_node.exec_command(
                cmd=f"rm -rf {mount_path}", sudo=True, check_ec=False
            )
            log.debug(f"CephFS unmounted from {mount_path}")
        except Exception as e:
            log.warning(f"Failed to unmount CephFS: {e}")

    if fs_name:
        log.info(f"Removing CephFS filesystem: {fs_name}")
        try:
            client_node.exec_command(
                cmd=f"ceph fs fail {fs_name}", sudo=True, check_ec=False
            )
            time.sleep(2)
            client_node.exec_command(
                cmd=f"ceph fs rm {fs_name} --yes-i-really-mean-it",
                sudo=True,
                check_ec=False,
            )
            log.debug(f"CephFS filesystem {fs_name} removed")
        except Exception as e:
            log.warning(f"Failed to remove CephFS: {e}")


def _cleanup_rbd(client_node, mount_path: str, device_path: str) -> None:
    """
    Clean up RBD mount and image.

    Args:
        client_node: Client node to execute commands
        mount_path: RBD mount path
        device_path: RBD device path
    """
    if mount_path:
        log.info(f"Unmounting RBD image from: {mount_path}")
        try:
            mount_path = mount_path.rstrip("/")
            client_node.exec_command(
                cmd=f"umount {mount_path}", sudo=True, check_ec=False
            )
            if device_path:
                client_node.exec_command(
                    cmd=f"rbd unmap {device_path}",
                    sudo=True,
                    check_ec=False,
                )
            client_node.exec_command(
                cmd=f"rm -rf {mount_path}", sudo=True, check_ec=False
            )
            log.debug(f"RBD unmounted from {mount_path}")
        except Exception as e:
            log.warning(f"Failed to unmount RBD: {e}")
