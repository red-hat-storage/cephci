"""
Test Module: OSD Thrashing Test with Concurrent I/O
====================================================

This module performs aggressive OSD thrashing with concurrent I/O workload to verify
cluster stability under stress conditions. It tests both EC and replicated pools,
and checks for any crashes or issues during OSD state transitions.

TEST WORKFLOW:
==============

    ├── Crash archive at start
    │
    ├── SETUP PHASE
    │   ├── Create pools (RADOS EC/replicated, CephFS, RBD) in parallel
    │   ├── Enable EC optimizations for EC pools
    │   ├── Enable compression (if configured)
    │   ├── Configure aggressive recovery settings
    │   ├── Enable debug logging: debug_osd=20/20 (if configured)
    │   └── Inject EC write errors on random OSDs (if configured)
    │       ├── bluestore_debug_inject_read_err=true
    │       └── ceph daemon osd.$id injectecwriteerr <pool> '*' 2 1 0 1
    │
    ├── THRASHING PHASE (dynamically configured parallel threads)
    │   ├── io_workload_burst: rados bench 30s write bursts (64K blocks) [always]
    │   ├── comprehensive_io_workload: Various object sizes + overwrites + appends [always]
    │   ├── monitor_cluster_health: Periodic ceph health detail + ceph -s logging [always]
    │   ├── _run_fio_workload (CephFS): FIO with snapshots on work directory [if enabled]
    │   ├── _run_fio_workload (RBD): FIO on mounted RBD image [if enabled]
    │   ├── thrash_cephfs_snapshots: 5 snaps/iter, parallel writes, ~20% deletion [if enabled]
    │   ├── thrash_rbd_snapshots: 4 snaps/iter, protect, clone, flatten, ~20% deletion [if enabled]
    │   ├── thrash_ec_pool_snapshots: RADOS-level snapshots + partial writes [if enabled]
    │   ├── thrash_osds: Mark out → orch daemon stop → wait → mark in → start [if enabled]
    │   ├── thrash_crush_weights: Reduce to 50% → restore [if enabled]
    │   ├── thrash_pg_count: Bulk flag toggle + scrub/deep-scrub [if enabled]
    │   ├── thrash_rgw_s3: S3 PUT/GET/DELETE/multipart on RGW [if enabled]
    │   ├── thrash_mon: Leader failover, rolling restart, election strategy [if enabled]
    │   ├── thrash_mgr: Failover, rolling restart, random fail [if enabled]
    │   └── thrash_mds: Failover, rolling restart, random fail [if enabled]
    │
    ├── VALIDATION PHASE
    │   ├── Wait for cluster stabilization (active+clean PGs)
    │   ├── Check cluster health
    │   ├── Check for crashes (ceph crash ls-new)
    │   └── Report findings and fail if crashes detected
    │
    └── CLEANUP PHASE
        ├── Reset recovery settings
        ├── Remove debug logging configs (if enabled)
        ├── Remove error injection configs (if enabled)
        ├── Force log rotation on all OSD hosts
        ├── Unmount and cleanup CephFS and RBD
        └── Delete test pools and EC profiles

CONFIGURATION OPTIONS:
======================
- iterations: Number of OSD thrash iterations (default: 60)
- duration: Total test duration in seconds (default: 1200)
- aggressive: Enable aggressive mode with longer waits (default: True)
- enable_crush_thrashing: Enable CRUSH weight modifications (default: True)
- enable_pg_thrashing: Enable PG count thrashing via bulk flag (default: True)
- compression: Enable pool compression with mode=force (default: False)
- cleanup_pools: Delete pools after test (default: True)
- enable_debug_logs: Enable debug logging for OSD peering and BlueStore (default: False)
- inject_errors: Inject EC write errors on random OSDs (default: False)
- num_osds_to_inject: Number of OSDs to inject errors on (default: 3)
- enable_fio_cephfs: Enable FIO workload on CephFS mount (default: True)
- enable_fio_rbd: Enable FIO workload on RBD mount (default: True)
- enable_cephfs_snapshots: Enable CephFS snapshot thrashing (default: True)
- enable_rbd_snapshots: Enable RBD snapshot thrashing (default: True)
- enable_ec_snapshots: Enable EC pool snapshot thrashing (default: True)
- enable_osd_thrashing: Enable OSD thrashing operations (default: True)
- enable_rgw_thrashing: Enable RGW S3 thrashing (default: False)
- enable_mon_thrashing: Enable MON thrashing - leader failover/restart (default: False)
- enable_mgr_thrashing: Enable MGR thrashing - failover/restart/random fail (default: False)
- enable_mds_thrashing: Enable MDS thrashing - failover/restart/random fail (default: False)
- enable_nfs_thrashing: Enable NFS cluster/export setup for thrashing (default: False)
- enable_election_strategy_thrash: Enable election strategy changes during MON thrash (default: False)

"""

import concurrent.futures as cf
import random
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import yaml

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.mgr_workflows import MgrWorkflows
from ceph.rados.monitor_workflows import MonitorWorkflows
from ceph.rados.pool_workflows import PoolFunctions
from ceph.rados.utils import get_cluster_timestamp
from tests.rados.monitor_configurations import MonConfigMethods, MonElectionStrategies
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
            - sample_pool_id (str): Pool ID from pool-configurations.yaml
            - Any additional parameters will override file config

        iterations (int): Number of OSD thrash iterations (default: 60)
        duration (int): Test duration in seconds (default: 1200)
        aggressive (bool): Enable aggressive mode with longer waits (default: True)
        enable_crush_thrashing (bool): Enable CRUSH weight thrashing (default: True)
        enable_pg_thrashing (bool): Enable PG count thrashing via bulk flag (default: True)
        compression (bool): Enable pool compression with mode=force (default: False)
        cleanup_pools (bool): Delete pools after test (default: True)
        enable_debug_logs (bool): Enable debug logging for OSD/BlueStore (default: False)
        inject_errors (bool): Inject EC write errors on random OSDs (default: False)
        num_osds_to_inject (int): Number of OSDs to inject errors on (default: 3)
        num_writes_to_inject (int): Number of writes to inject errors per OSD (default: 1000)
        enable_rgw_thrashing (bool): Enable RGW S3 thrashing (default: False, requires RGW)
        enable_fio_cephfs (bool): Enable FIO workload on CephFS mount (default: True)
        enable_fio_rbd (bool): Enable FIO workload on RBD mount (default: True)
        enable_cephfs_snapshots (bool): Enable CephFS snapshot thrashing (default: True)
        enable_rbd_snapshots (bool): Enable RBD snapshot thrashing (default: True)
        enable_ec_snapshots (bool): Enable EC pool snapshot thrashing (default: True)
        enable_osd_thrashing (bool): Enable OSD thrashing - main thrash operation (default: True)
        enable_mon_thrashing (bool): Enable MON thrashing - leader failover/restart (default: False)
        enable_mgr_thrashing (bool): Enable MGR thrashing - failover/restart/random fail (default: False)
        enable_mds_thrashing (bool): Enable MDS thrashing - failover/restart/random fail (default: False)
        enable_nfs_thrashing (bool): Enable NFS cluster/export setup for thrashing (default: False)
        enable_election_strategy_thrash (bool): Enable election strategy changes (default: False)
        enable_fast_ec_config_params (bool): Enable fast EC config params (default: True)

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
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    client_node = ceph_cluster.get_nodes(role="client")[0]
    mon_workflow_obj = MonitorWorkflows(node=cephadm)
    mon_election_obj = MonElectionStrategies(rados_obj=rados_obj)
    mgr_workflow_obj = MgrWorkflows(node=cephadm)

    # Test parameters
    iterations = config.get("iterations", 60)
    duration = config.get("duration", 1200)
    aggressive = config.get("aggressive", True)
    enable_crush_thrashing = config.get("enable_crush_thrashing", True)
    enable_pg_thrashing = config.get("enable_pg_thrashing", True)
    compression = config.get("compression", False)
    enable_debug_logs = config.get("enable_debug_logs", True)
    inject_errors = config.get("inject_errors", False)
    num_osds_to_inject = config.get("num_osds_to_inject", 3)
    num_writes_to_inject = config.get("num_writes_to_inject", 1000)
    enable_rgw_thrashing = config.get("enable_rgw_thrashing", True)
    enable_fio_cephfs = config.get("enable_fio_cephfs", True)
    enable_fio_rbd = config.get("enable_fio_rbd", True)
    enable_cephfs_snapshots = config.get("enable_cephfs_snapshots", True)
    enable_rbd_snapshots = config.get("enable_rbd_snapshots", True)
    enable_ec_snapshots = config.get("enable_ec_snapshots", True)
    enable_osd_thrashing = config.get("enable_osd_thrashing", True)
    enable_mon_thrashing = config.get("enable_mon_thrashing", True)
    enable_mgr_thrashing = config.get("enable_mgr_thrashing", False)
    enable_mds_thrashing = config.get("enable_mds_thrashing", False)
    enable_nfs_thrashing = config.get("enable_nfs_thrashing", False)
    enable_election_strategy_thrash = config.get(
        "enable_election_strategy_thrash", True
    )
    enable_fast_ec_config_params = config.get("enable_fast_ec_config_params", True)
    disabled_ec_optimizations = False
    major_version = int(rados_obj.rhbuild.split(".")[0])

    if major_version < 9:
        enable_fast_ec_config_params = False
        log.info(
            "Fast EC config params are not supported in this version. "
            "Running tests without fast EC config params."
        )
    elif not enable_fast_ec_config_params:
        log.info(
            "Fast EC is enabled by default in 9.x and above. "
            "Explicitly disabling it by setting osd_pool_default_flag_ec_optimizations to false."
        )
        mon_obj.set_config(
            section="global",
            name="osd_pool_default_flag_ec_optimizations",
            value="false",
        )
        disabled_ec_optimizations = True

    compression_algorithms = ["snappy", "zlib"]
    rgw_config = None

    up_osds = rados_obj.get_osd_list(status="up")
    inject_osds = random.sample(up_osds, min(num_osds_to_inject, len(up_osds)))
    log.info(
        f"Injecting EC write errors on {len(inject_osds)} OSDs: "
        f"{', '.join(str(osd) for osd in inject_osds)} if enabled in config"
    )

    # Determine which pool types will be created based on enabled workflows
    enable_cephfs_pools = enable_fio_cephfs or enable_cephfs_snapshots
    enable_rbd_pools = enable_fio_rbd or enable_rbd_snapshots

    additional_pools = []
    if enable_cephfs_pools:
        additional_pools.append("CephFS (2 pools)")
    if enable_rbd_pools:
        additional_pools.append("RBD (2 pools)")
    additional_pools_str = " + ".join(additional_pools) if additional_pools else "None"

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
        f"  Debug logs: {enable_debug_logs}\n"
        f"  RADOS pools from config: {len(config.get('pool_configs', []))}\n"
        f"  Additional pools: {additional_pools_str}\n"
        f"  Inject errors: {inject_errors}\n"
        f"  RGW S3 thrashing: {enable_rgw_thrashing}\n"
        f"  OSDs to inject errors: {len(inject_osds) if inject_errors else 0}\n"
        f"  Writes to inject per OSD: {num_writes_to_inject if inject_errors else 0}\n"
        f"  FIO on CephFS: {enable_fio_cephfs}\n"
        f"  FIO on RBD: {enable_fio_rbd}\n"
        f"  CephFS snapshot thrashing: {enable_cephfs_snapshots}\n"
        f"  RBD snapshot thrashing: {enable_rbd_snapshots}\n"
        f"  EC pool snapshot thrashing: {enable_ec_snapshots}\n"
        f"  OSD thrashing: {enable_osd_thrashing}\n"
        f"  MON thrashing: {enable_mon_thrashing}\n"
        f"  MGR thrashing: {enable_mgr_thrashing}\n"
        f"  MDS thrashing: {enable_mds_thrashing}\n"
        f"  NFS thrashing: {enable_nfs_thrashing}\n"
        f"  Election strategy thrash: {enable_election_strategy_thrash}\n"
        f"  Fast EC config params: {enable_fast_ec_config_params}\n"
        f"{'=' * 60}\n"
    )
    log.info(test_params)

    log.info("Archiving existing crashes...")
    rados_obj.run_ceph_command(cmd="ceph crash archive-all", client_exec=True)
    time.sleep(5)
    start_time = get_cluster_timestamp(rados_obj.node)
    log.debug(f"Test workflow started. Start time: {start_time}")

    created_pools = []
    test_failed = False
    fs_name = None
    fs_names = []  # List of all filesystems created (for MDS thrashing)
    cephfs_mount_path = None
    rbd_mount_path = None
    rbd_device_path = None
    nfs_config = None

    try:
        # POOL CREATION PHASE - RADOS, CephFS, RBD (Parallel)
        log.info(f"\n{'=' * 60}\nSETUP PHASE\n{'=' * 60}")

        osd_list = rados_obj.get_osd_list(status="up")
        log.info(
            f"\nPool Creation Phase - RADOS, CephFS, RBD, RGW & NFS (Parallel)\n{'-' * 60}"
        )

        pool_workers = (
            1
            + (1 if enable_cephfs_pools else 0)
            + (1 if enable_rbd_pools else 0)
            + (1 if enable_rgw_thrashing else 0)
            + (1 if enable_nfs_thrashing else 0)
        )
        with cf.ThreadPoolExecutor(max_workers=pool_workers) as pool_executor:
            log.info("Creating pools in parallel...")

            rados_future = pool_executor.submit(
                create_rados_pools,
                rados_obj,
                config=config,
                enable_fast_ec_config_params=enable_fast_ec_config_params,
            )

            cephfs_future = None
            if enable_cephfs_pools:
                log.info("CephFS workflows enabled - creating CephFS pools")
                cephfs_future = pool_executor.submit(
                    rados_obj.create_cephfs_filesystem_mount,
                    client_node,
                    pool_type="erasure",
                    enable_fast_ec_config_params=enable_fast_ec_config_params,
                )

            rbd_future = None
            if enable_rbd_pools:
                log.info("RBD workflows enabled - creating RBD pools")
                rbd_future = pool_executor.submit(
                    rados_obj.create_ec_rbd_pools,
                    enable_fast_ec_config_params=enable_fast_ec_config_params,
                )

            rgw_future = None
            if enable_rgw_thrashing:
                log.info("RGW workflows enabled - setting up RGW S3 client")
                rgw_future = pool_executor.submit(
                    setup_rgw_s3_client,
                    client_node,
                    rados_obj,
                    data_pool_type="erasure",
                    ec_config={"k": 2, "m": 2},
                    enable_fast_ec_config_params=enable_fast_ec_config_params,
                )

            nfs_future = None
            if enable_nfs_thrashing:
                log.info("NFS workflows enabled - creating NFS clusters and exports")
                nfs_future = pool_executor.submit(
                    rados_obj.create_nfs_clusters_and_exports,
                    client_node,
                    num_clusters=config.get("nfs_num_clusters", 3),
                    exports_per_cluster=config.get("nfs_exports_per_cluster", 4),
                    placement=config.get("nfs_placement", 1),
                    pool_type="erasure",
                    enable_fast_ec_config_params=enable_fast_ec_config_params,
                )

            # Collect results
            try:
                rados_pools = rados_future.result()
                created_pools.extend(rados_pools)
                log.info(f"Created {len(rados_pools)} RADOS pool(s)")

                if cephfs_future:
                    fs_name, cephfs_mount_path, cephfs_pools = cephfs_future.result()
                    created_pools.extend(cephfs_pools)
                    fs_names.append(fs_name)
                    log.info(f"Created CephFS: {fs_name} at {cephfs_mount_path}")
                else:
                    log.info(
                        "CephFS pool creation skipped (no CephFS workflows enabled)"
                    )

                if rbd_future:
                    rbd_mount_path, rbd_device_path, rbd_pools = rbd_future.result()
                    created_pools.extend(rbd_pools)
                    log.info(f"Created RBD pools at {rbd_mount_path}")
                else:
                    log.info("RBD pool creation skipped (no RBD workflows enabled)")

                if rgw_future:
                    rgw_config = rgw_future.result()
                    if rgw_config:
                        log.info("RGW S3 client setup completed successfully")
                    else:
                        log.warning("RGW S3 setup failed, disabling RGW thrashing")
                        enable_rgw_thrashing = False
                else:
                    log.info("RGW setup skipped (RGW thrashing not enabled)")

                if nfs_future:
                    nfs_config = nfs_future.result()
                    if nfs_config:
                        created_pools.extend(nfs_config.get("pools", []))
                        fs_names.append(nfs_config.get("fs_name"))
                        log.info(
                            f"Created NFS setup: {len(nfs_config.get('clusters', []))} clusters, "
                            f"{len(nfs_config.get('exports', []))} exports"
                        )
                    else:
                        log.warning("NFS setup failed, disabling NFS thrashing")
                        enable_nfs_thrashing = False
                else:
                    log.info("NFS setup skipped (NFS thrashing not enabled)")
            except Exception as e:
                log.error(f"Failed to create pools: {e}")
                raise

        log.info(f"\nTotal pools created: {len(created_pools)}")
        log.info("Pool creation phase completed (parallel execution)")

        if compression:
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

        # Additional setup details
        log.info("=" * 70)
        log.info("ADDITIONAL SETUP DETAILS")
        log.info("=" * 70)

        if cephfs_mount_path:
            log.info("CephFS Setup:")
            log.info("  Filesystem Name : %s", fs_name)
            log.info("  Mount Path      : %s", cephfs_mount_path)
            log.info("")

        if rbd_mount_path:
            log.info("RBD Setup:")
            log.info("  Mount Path      : %s", rbd_mount_path)
            log.info("  Device Path     : %s", rbd_device_path)
            log.info("")

        if rgw_config:
            log.info("RGW Setup:")
            log.info("  Endpoint        : %s", rgw_config.get("endpoint", "N/A"))
            log.info("  Bucket          : %s", rgw_config.get("bucket_name", "N/A"))
            log.info("  Data Pool       : %s", rgw_config.get("data_pool", "N/A"))
            log.info("")

        if nfs_config:
            log.info("NFS Setup:")
            log.info("  Filesystem      : %s", nfs_config.get("fs_name", "N/A"))
            log.info("  Clusters        : %d", len(nfs_config.get("clusters", [])))
            for cluster in nfs_config.get("clusters", []):
                log.info(
                    "    - %s (port: %d, host: %s)",
                    cluster["cluster_id"],
                    cluster["port"],
                    cluster.get("host", "N/A"),
                )
            exports = nfs_config.get("exports", [])
            log.info("  Exports         : %d", len(exports))
            # Show first 5 exports, summarize rest
            for export in exports[:5]:
                log.info(
                    "    - %s on %s",
                    export["pseudo_path"],
                    export["cluster_id"],
                )
            if len(exports) > 5:
                log.info("    ... and %d more exports", len(exports) - 5)
            log.info("")

        # MDS thrashing filesystem targets
        if fs_names:
            log.info("Filesystems for MDS thrashing: %s", fs_names)
            log.info("")

        log.info("=" * 70)
        log.info("Checking PG states post pool creation...")
        if not wait_for_clean_pg_sets(rados_obj, timeout=600, recovery_thread=False):
            log.error("Not all PGs are active+clean")
            raise Exception("Not all PGs are active+clean post creation")

        log.info("Configuring aggressive recovery settings...")
        recovery_config = {"osd_max_backfills": 15, "osd_recovery_max_active": 20}
        rados_obj.change_recovery_threads(config=recovery_config, action="set")

        if enable_debug_logs:
            log.debug("Enabling debug logging for divergent log analysis...")
            mon_obj.set_config(section="osd", name="debug_osd", value="20/20")
            # mon_obj.set_config(section="osd", name="debug_bluestore", value="20/20")
            # mon_obj.set_config(section="mgr", name="debug_mgr", value="20/20")
            # mon_obj.set_config(section="mon", name="debug_mon", value="20/20")
            # mon_obj.set_config(section="mds", name="debug_mds", value="20/20")
            log.info("Debug logging enabled for daemons")

        if inject_errors:
            mon_obj.set_config(
                section="global", name="bluestore_debug_inject_read_err", value="true"
            )
            # Get EC pools for injection
            ec_pool_names = [
                p["pool_name"] for p in created_pools if p.get("pool_type") == "erasure"
            ]
            if ec_pool_names:
                for ec_pool in ec_pool_names:
                    for osd_id in inject_osds:
                        # injectecwriteerr <pool> <obj> <shard> <type> <skip> <inject>
                        # shard=2, type=1 (drop sub write), skip=0
                        inject_cmd = (
                            f"cephadm shell -- ceph daemon osd.{osd_id} injectecwriteerr "
                            f"{ec_pool} '*' 2 1 0 {num_writes_to_inject}"
                        )
                        try:
                            osd_host = rados_obj.fetch_host_node(
                                daemon_type="osd", daemon_id=str(osd_id)
                            )
                            out, _ = osd_host.exec_command(
                                cmd=inject_cmd, sudo=True, check_ec=False
                            )
                            if "ok" in out.lower():
                                log.info(
                                    f"Injected on OSD.{osd_id} for {ec_pool}: {out.strip()}"
                                )
                            else:
                                log.warning(
                                    f"Injection failed on OSD.{osd_id} for {ec_pool}: {out.strip()}"
                                )
                        except Exception as e:
                            log.warning(
                                f"Failed to inject on OSD.{osd_id} for {ec_pool}: {e}"
                            )
                log.info(
                    f"Injected EC write errors on {len(inject_osds)} OSDs "
                    f"for {len(ec_pool_names)} pools: "
                    f"{', '.join(str(osd) for osd in inject_osds)}"
                )

        mon_obj.set_config(
            section="global", name="osd_max_markdown_count", value="99999999"
        )

        log.info("Setup phase completed successfully")

        log.info(f"\n{'=' * 60}\nTHRASHING PHASE\n{'=' * 60}")

        # Stop flag for coordinated shutdown of background threads
        stop_flag = {"stop": False}
        log.info("Launching background I/O workload threads...")

        reboot_osd = False if inject_errors else True

        # Calculate worker count dynamically based on enabled workloads
        # Base threads: rados bench (always), comprehensive I/O (always),
        #               cluster health monitoring (always)
        max_workers = 3  # rados bench + comprehensive I/O + health monitoring
        max_workers += 1 if enable_fio_cephfs else 0
        max_workers += 1 if enable_fio_rbd else 0
        max_workers += 1 if enable_cephfs_snapshots else 0
        max_workers += 1 if enable_rbd_snapshots else 0
        max_workers += 1 if enable_ec_snapshots else 0
        max_workers += 1 if enable_osd_thrashing else 0
        max_workers += 1 if enable_crush_thrashing else 0
        max_workers += 1 if enable_pg_thrashing else 0
        max_workers += 1 if enable_rgw_thrashing else 0
        max_workers += 1 if enable_mon_thrashing else 0
        max_workers += 1 if enable_mgr_thrashing else 0
        max_workers += 1 if enable_mds_thrashing else 0
        log.debug(f"ThreadPoolExecutor max_workers: {max_workers}")

        with cf.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []

            # I/O workload burst thread (rados bench) - RADOS pools
            futures.append(
                executor.submit(
                    io_workload_burst,
                    rados_obj=rados_obj,
                    pools=created_pools,
                    duration=duration,
                    stop_flag=stop_flag,
                )
            )

            # Comprehensive I/O workload thread (various sizes + operations) - RADOS pools
            futures.append(
                executor.submit(
                    comprehensive_io_workload,
                    rados_obj=rados_obj,
                    pools=created_pools,
                    duration=duration,
                    stop_flag=stop_flag,
                )
            )

            # Cluster health monitoring thread
            futures.append(
                executor.submit(
                    monitor_cluster_health,
                    client_node=client_node,
                    start_time=start_time,
                    duration=duration,
                    stop_flag=stop_flag,
                    interval=30,
                )
            )

            # FIO workload on CephFS (default: enabled)
            if enable_fio_cephfs and cephfs_mount_path:
                log.info("FIO workload on CephFS enabled")
                futures.append(
                    executor.submit(
                        _run_fio_workload,
                        client_node=client_node,
                        mount_path=cephfs_mount_path,
                        workload_name="cephfs",
                        duration=duration,
                        stop_flag=stop_flag,
                        enable_cephfs_snapshots=enable_cephfs_snapshots,
                    )
                )
            elif enable_fio_cephfs:
                log.warning("FIO CephFS enabled but mount path not available")

            # FIO workload on RBD (default: enabled)
            if enable_fio_rbd and rbd_mount_path:
                log.info("FIO workload on RBD enabled")
                futures.append(
                    executor.submit(
                        _run_fio_workload,
                        client_node=client_node,
                        mount_path=rbd_mount_path,
                        workload_name="rbd",
                        duration=duration,
                        stop_flag=stop_flag,
                        enable_cephfs_snapshots=enable_cephfs_snapshots,
                    )
                )
            elif enable_fio_rbd:
                log.warning("FIO RBD enabled but mount path not available")

            # CephFS snapshot thrashing (create, modify, rollback) (default: enabled)
            if enable_cephfs_snapshots and cephfs_mount_path:
                log.info("CephFS snapshot thrashing enabled")
                futures.append(
                    executor.submit(
                        thrash_cephfs_snapshots,
                        client_node=client_node,
                        mount_path=cephfs_mount_path,
                        duration=duration,
                        stop_flag=stop_flag,
                    )
                )
            elif enable_cephfs_snapshots:
                log.warning("CephFS snapshots enabled but mount path not available")

            # RBD snapshot thrashing (create, clone, write to clones) (default: enabled)
            # Requires RBD pool to be created (rbd_mount_path exists)
            if enable_rbd_snapshots and rbd_mount_path:
                log.info("RBD snapshot thrashing enabled")
                futures.append(
                    executor.submit(
                        thrash_rbd_snapshots,
                        client_node=client_node,
                        pool_name="rbd-thrash-metadata",
                        image_name="thrash-test-image",
                        duration=duration,
                        stop_flag=stop_flag,
                    )
                )
            elif enable_rbd_snapshots:
                log.warning("RBD snapshots enabled but RBD mount not available")

            # EC pool snapshot thrashing (default: enabled)
            # Uses RADOS-level snapshots + partial writes to trigger BlueStore clones
            # Clone log → ALL shards, write data → SOME shards
            if enable_ec_snapshots:
                log.info("EC pool snapshot thrashing enabled")
                futures.append(
                    executor.submit(
                        thrash_ec_pool_snapshots,
                        rados_obj=rados_obj,
                        pools=created_pools,
                        duration=duration,
                        stop_flag=stop_flag,
                        reboot_osd=reboot_osd,
                    )
                )

            # OSD thrashing thread (main thrashing operation) (default: enabled)
            if enable_osd_thrashing:
                log.info("OSD thrashing enabled")
                futures.append(
                    executor.submit(
                        thrash_osds,
                        rados_obj=rados_obj,
                        osd_list=osd_list,
                        iterations=iterations,
                        aggressive=aggressive,
                        stop_flag=stop_flag,
                        reboot_osd=reboot_osd,
                    )
                )

            # CRUSH weight thrashing (default: enabled)
            if enable_crush_thrashing:
                log.info("CRUSH weight thrashing enabled")
                futures.append(
                    executor.submit(
                        thrash_crush_weights,
                        rados_obj=rados_obj,
                        osd_list=osd_list,
                        iterations=iterations // 2,
                        stop_flag=stop_flag,
                    )
                )

            # PG count thrashing (default: enabled)
            if enable_pg_thrashing:
                log.info("PG count thrashing enabled")
                futures.append(
                    executor.submit(
                        thrash_pg_count,
                        rados_obj=rados_obj,
                        pool_obj=pool_obj,
                        pools=created_pools,
                        iterations=iterations // 3,
                        stop_flag=stop_flag,
                    )
                )

            # RGW S3 thrashing (optional: requires RGW deployment)
            if enable_rgw_thrashing and rgw_config:
                log.info("RGW S3 thrashing enabled")
                futures.append(
                    executor.submit(
                        thrash_rgw_s3,
                        client_node=client_node,
                        rgw_config=rgw_config,
                        duration=duration,
                        stop_flag=stop_flag,
                    )
                )

            # MON thrashing (optional: leader failover, rolling restart, election strategy)
            if enable_mon_thrashing:
                log.info("MON thrashing enabled")
                futures.append(
                    executor.submit(
                        thrash_mon,
                        rados_obj=rados_obj,
                        mon_workflow_obj=mon_workflow_obj,
                        mon_election_obj=mon_election_obj,
                        duration=duration,
                        stop_flag=stop_flag,
                        enable_election_strategy_thrash=enable_election_strategy_thrash,
                    )
                )

            # MGR thrashing (optional: failover, rolling restart, random fail)
            if enable_mgr_thrashing:
                log.info("MGR thrashing enabled")
                futures.append(
                    executor.submit(
                        thrash_mgr,
                        rados_obj=rados_obj,
                        mgr_workflow_obj=mgr_workflow_obj,
                        duration=duration,
                        stop_flag=stop_flag,
                    )
                )

            # MDS thrashing (optional: failover, rolling restart, random fail)
            if enable_mds_thrashing and fs_names:
                log.info(f"MDS thrashing enabled for filesystems: {fs_names}")
                futures.append(
                    executor.submit(
                        thrash_mds,
                        rados_obj=rados_obj,
                        fs_names=fs_names,
                        duration=duration,
                        stop_flag=stop_flag,
                    )
                )
            elif enable_mds_thrashing:
                log.warning("MDS thrashing enabled but no CephFS filesystems available")

            log.info(f"Thrashing operations in progress (max duration: {duration}s)...")
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
                    log.warning(f"Thread had exception (non-fatal): {e}")

            # Log iteration counts for each thrash workflow
            log.info(f"\n{'=' * 60}")
            log.info("THRASH WORKFLOW ITERATION SUMMARY")
            log.info(f"{'=' * 60}")
            workflow_results = {
                "io_workload_burst": None,
                "comprehensive_io_workload": None,
                "monitor_cluster_health": None,
                "fio_cephfs": None,
                "fio_rbd": None,
                "cephfs_snapshots": None,
                "rbd_snapshots": None,
                "ec_pool_snapshots": None,
                "osd_thrashing": None,
                "crush_weight_thrashing": None,
                "pg_count_thrashing": None,
                "rgw_s3_thrashing": None,
                "mon_thrashing": None,
                "mgr_thrashing": None,
            }
            # Map futures to workflow names based on their index
            workflow_order = []
            # Always enabled workflows
            workflow_order.extend(
                [
                    "io_workload_burst",
                    "comprehensive_io_workload",
                    "monitor_cluster_health",
                ]
            )
            if enable_fio_cephfs and cephfs_mount_path:
                workflow_order.append("fio_cephfs")
            if enable_fio_rbd and rbd_mount_path:
                workflow_order.append("fio_rbd")
            if enable_cephfs_snapshots and cephfs_mount_path:
                workflow_order.append("cephfs_snapshots")
            if enable_rbd_snapshots and rbd_mount_path:
                workflow_order.append("rbd_snapshots")
            if enable_ec_snapshots:
                workflow_order.append("ec_pool_snapshots")
            if enable_osd_thrashing:
                workflow_order.append("osd_thrashing")
            if enable_crush_thrashing:
                workflow_order.append("crush_weight_thrashing")
            if enable_pg_thrashing:
                workflow_order.append("pg_count_thrashing")
            if enable_rgw_thrashing and rgw_config:
                workflow_order.append("rgw_s3_thrashing")
            if enable_mon_thrashing:
                workflow_order.append("mon_thrashing")
            if enable_mgr_thrashing:
                workflow_order.append("mgr_thrashing")
            if enable_mds_thrashing and fs_names:
                workflow_order.append("mds_thrashing")

            for idx, future in enumerate(futures):
                workflow_name = (
                    workflow_order[idx]
                    if idx < len(workflow_order)
                    else f"unknown_{idx}"
                )
                try:
                    if future in done:
                        result = future.result()
                        workflow_results[workflow_name] = result
                        log.info(
                            f"  {workflow_name}: {result} iterations/operations completed"
                        )
                    else:
                        log.info(f"  {workflow_name}: Did not complete in time")
                except Exception as e:
                    log.info(f"  {workflow_name}: Failed with exception - {e}")
            log.info(f"{'=' * 60}")

        log.info(
            "Thrashing phase completed\n Sleeping for 60 seconds to allow for recovery..."
        )
        time.sleep(60)
        log.info(f"\n{'=' * 60}\nVALIDATION PHASE\n{'=' * 60}")
        rados_obj.log_cluster_health()

        # Check for inconsistent PGs across all pools and repair if found
        log.info("Checking for inconsistent PGs across all pools...")
        all_pools = rados_obj.list_pools()
        inconsistent_pgs_found = []
        for check_pool in all_pools:
            try:
                inconsistent_pgs = rados_obj.get_inconsistent_pg_list(check_pool)
                if inconsistent_pgs:
                    log.warning(
                        f"Found {len(inconsistent_pgs)} inconsistent PG(s) in pool "
                        f"{check_pool}: {inconsistent_pgs}"
                    )
                    inconsistent_pgs_found.extend(inconsistent_pgs)
            except Exception as e:
                log.debug(f"Error checking pool {check_pool}: {e}")

        if inconsistent_pgs_found:
            log.debug(
                f"Total inconsistent PGs found: {len(inconsistent_pgs_found)}. "
                f"Inconsistent PGs: {inconsistent_pgs_found}. "
                f"Initiating PG repair..."
            )
            for pg_id in inconsistent_pgs_found:
                try:
                    client_node.exec_command(
                        cmd=f"ceph pg {pg_id} query -f json-pretty > /tmp/{pg_id}_query.txt",
                        sudo=True,
                    )
                    client_node.exec_command(
                        cmd=f"rados list-inconsistent-obj {pg_id} -f json-pretty "
                        f"> /tmp/{pg_id}_inconsistent_obj.txt",
                        sudo=True,
                    )
                    time.sleep(5)
                    log.info(f"Running pg repair on PG: {pg_id}")
                    rados_obj.run_ceph_command(cmd=f"ceph pg repair {pg_id}")
                except Exception as e:
                    log.error(f"Failed to repair PG {pg_id}: {e}")
            time.sleep(30)
        else:
            log.info("No inconsistent PGs found across all pools")

        log.info("Waiting for PGs to reach active+clean state...")
        if not wait_for_clean_pg_sets(rados_obj, timeout=1200, recovery_thread=False):
            log.error("Not all PGs are active+clean")
            raise Exception("Not all PGs are active+clean")

        rados_obj.log_cluster_health()

        log.info("\nValidation phase completed")

    except Exception as e:
        log.error(f"Test execution failed with exception: {e}")
        log.exception(e)
        test_failed = True

    finally:
        # CLEANUP PHASE
        log.info(f"\n{'=' * 60}\n----------IN FINALLY BLOCK----------\n{'=' * 60}")

        # Reset recovery settings to defaults
        log.info("Resetting recovery settings to defaults...")
        rados_obj.change_recovery_threads(config={}, action="rm")

        if enable_debug_logs:
            # Remove debug logging configs
            log.info("Removing debug logging configs...")
            mon_obj.remove_config(section="osd", name="debug_osd")
            # mon_obj.remove_config(section="osd", name="debug_bluestore")
            # mon_obj.remove_config(section="mgr", name="debug_mgr")
            # mon_obj.remove_config(section="mon", name="debug_mon")
            # mon_obj.remove_config(section="mds", name="debug_mds")
            log.info("Debug logging configs removed")

        if inject_errors:
            log.info("Removing EC write error injection config...")
            mon_obj.remove_config(
                section="global", name="bluestore_debug_inject_read_err"
            )

        if disabled_ec_optimizations:
            log.info("Reverting osd_pool_default_flag_ec_optimizations config...")
            mon_obj.remove_config(
                section="global", name="osd_pool_default_flag_ec_optimizations"
            )

        # Restore any OSDs that are out or down
        log.info("Checking for OSDs that need restoration...")
        try:
            osd_tree = rados_obj.run_ceph_command(cmd="ceph osd tree")
            osds_to_restore = [
                node.get("id")
                for node in osd_tree.get("nodes", [])
                if node.get("type") == "osd"
                and (node.get("reweight", 1.0) == 0 or node.get("status") == "down")
            ]
            mon_obj.remove_config(
                section="global",
                name="osd_max_markdown_count",
                verify_rm=False,
            )
            if osds_to_restore:
                _restore_osds_to_in(rados_obj, osds_to_restore)
            else:
                log.info("All OSDs are healthy, no restoration needed")
        except Exception as e:
            log.warning(f"Failed to check/restore OSDs: {e}")

        # Restore election strategy to classic (default) if it was modified
        if enable_election_strategy_thrash:
            log.info("Restoring MON election strategy to classic (default)...")
            try:
                current_strategy = mon_election_obj.get_election_strategy()
                if current_strategy != 1:  # 1 = classic
                    mon_election_obj.set_election_strategy(mode="classic")
                    log.info("Election strategy restored to classic")
                else:
                    log.info("Election strategy already set to classic")
            except Exception as e:
                log.warning(f"Failed to restore election strategy: {e}")

        if config.get("cleanup_pools", True):
            log.debug("Cleaning up test resources...")

            # Phase 1: Unmount filesystems, cleanup RGW and NFS (parallel)
            cleanup_tasks = []
            with cf.ThreadPoolExecutor(max_workers=4) as cleanup_executor:
                if enable_cephfs_pools:
                    cleanup_tasks.append(
                        (
                            "CephFS",
                            cleanup_executor.submit(
                                _cleanup_cephfs,
                                client_node,
                                cephfs_mount_path,
                                fs_name if fs_name else "cephfs-thrash",
                            ),
                        )
                    )

                if enable_rbd_pools:
                    cleanup_tasks.append(
                        (
                            "RBD",
                            cleanup_executor.submit(
                                _cleanup_rbd,
                                client_node,
                                rbd_mount_path,
                                rbd_device_path,
                            ),
                        )
                    )

                if rgw_config:
                    cleanup_tasks.append(
                        (
                            "RGW",
                            cleanup_executor.submit(
                                cleanup_rgw_s3, client_node, rgw_config, rados_obj
                            ),
                        )
                    )

                if nfs_config:
                    cleanup_tasks.append(
                        (
                            "NFS",
                            cleanup_executor.submit(
                                rados_obj.cleanup_nfs_clusters, nfs_config
                            ),
                        )
                    )

                for name, task in cleanup_tasks:
                    try:
                        task.result()
                    except Exception as e:
                        log.warning(f"{name} cleanup failed: {e}")

            # Phase 2: Delete tracked pools
            if created_pools:
                log.info(f"Deleting {len(created_pools)} tracked pool(s)...")
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
                    log.info("All tracked pools deleted successfully")
            else:
                log.info("No pools to delete")

            log.info("Pool cleanup completed")
        else:
            log.info("Skipping cleanup (cleanup_pools=False)")

        test_end_time = get_cluster_timestamp(rados_obj.node)
        log.debug(f"Test completed. Start: {start_time}, End: {test_end_time}")

        try:
            if rados_obj.check_crash_status(
                start_time=start_time, end_time=test_end_time
            ):
                log.error("Test failed due to crash detected")
                test_failed = True
        except Exception as e:
            log.error(f"Crash check failed: {e}")
            test_failed = True

        if enable_debug_logs:
            log.info("Forcing log rotation on OSD nodes...")
            try:
                fsid_result = rados_obj.run_ceph_command(cmd="ceph fsid")
                fsid = (
                    fsid_result.get("fsid")
                    if isinstance(fsid_result, dict)
                    else fsid_result
                )
                if fsid:
                    logrotate_cmd = f"logrotate -f /etc/logrotate.d/ceph-{fsid}"
                    osd_hosts = rados_obj.get_osd_hosts()
                    for host in osd_hosts:
                        try:
                            host.exec_command(
                                cmd=logrotate_cmd,
                                sudo=True,
                                check_ec=False,
                                long_running=True,
                            )
                            time.sleep(10)
                            log.debug(f"Log rotation completed on {host.hostname}")
                        except Exception:
                            pass
                    log.info(f"Log rotation triggered on {len(osd_hosts)} OSD host(s)")
            except Exception as e:
                log.warning(f"Failed to rotate logs: {e}")

        log.info("Cleanup phase completed")

    log.info(f"\n{'=' * 60}\nTEST RESULT\n{'=' * 60}")

    if test_failed:
        log.error("TEST FAILED - Exceptions occurred during test execution")
        return 1

    log.info("TEST PASSED - No crashes detected, cluster remained stable")
    return 0


def create_rados_pools(
    rados_obj: RadosOrchestrator, config: Dict, enable_fast_ec_config_params: bool
) -> List[Dict]:
    """
    Create RADOS pools from file-based configuration.

    Args:
        rados_obj: RadosOrchestrator object
        config: Test configuration with pool_configs
        enable_fast_ec_config_params: Whether to enable fast EC config params
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
                    "enable_fast_ec_features": enable_fast_ec_config_params,
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

    Uses create_comprehensive_test_objects() which creates objects with:
    - Sub-chunk writes (4KB, 8KB, 12KB objects)
    - Single chunk writes (16KB, 32KB objects)
    - Multi-chunk writes (48KB, 64KB objects)
    - Full stripe writes (96KB, 128KB objects)
    - Multi-stripe writes (192KB, 256KB objects)
    - Overwrites and appends on existing objects
    - Truncate operations

    These operations create partial writes that trigger BlueStore clones
    when snapshots exist on the pool.

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


def monitor_cluster_health(
    client_node, start_time: str, duration: int, stop_flag: dict, interval: int = 30
) -> int:
    """
    Periodically log `ceph health detail` and `ceph -s` with elapsed time.

    Args:
        client_node: Client node to execute ceph commands
        start_time: Test start timestamp (from get_cluster_timestamp)
        duration: Total duration in seconds
        stop_flag: Dict with 'stop' key to signal early termination
        interval: Seconds between health checks (default: 30)

    Returns:
        Number of health checks completed
    """
    log.info(f"Starting cluster health monitoring (interval: {interval}s)")

    # Parse start_time for elapsed calculation
    try:
        fmt = "%Y-%m-%dT%H:%M:%S" if "T" in start_time else "%Y-%m-%d %H:%M:%S"
        test_start_dt = datetime.strptime(start_time.split(".")[0], fmt)
    except ValueError:
        test_start_dt = datetime.now()

    check_count, end_time, commands = (
        0,
        time.time() + duration,
        ("ceph health detail", "ceph -s"),
    )

    while time.time() < end_time and not stop_flag.get("stop"):
        now = datetime.now()
        elapsed = str(now - test_start_dt).split(".")[0]

        log.info(
            f"\n{'=' * 60}\n"
            f"HEALTH STATUS: #{check_count + 1} | {now:%Y-%m-%d %H:%M:%S} | "
            f"Elapsed: {elapsed}\n"
        )

        for cmd in commands:
            try:
                out, _ = client_node.exec_command(cmd=cmd, sudo=True, timeout=60)
                log.info(f"[{cmd}]\n{out.strip()}")
            except Exception as e:
                log.warning(f"Failed '{cmd}': {e}")

        log.info(f"\n{'=' * 60}\n")
        check_count += 1

        # Wait for next interval, checking stop_flag every 5s
        for _ in range(interval // 5):
            if stop_flag.get("stop") or time.time() >= end_time:
                break
            time.sleep(5)

    log.info(f"Cluster health monitoring completed: {check_count} checks")
    return check_count


def _run_fio_workload(
    client_node,
    mount_path: str,
    workload_name: str,
    duration: int,
    stop_flag: Dict,
    enable_cephfs_snapshots: bool,
) -> int:
    """
    Run comprehensive FIO workload with writes, overwrites, partial writes, reads.

    Performs multiple phases per iteration:
    1. Initial writes (create files)
    2. Create snapshot (CephFS only - enables clone-on-write, gated by flag)
    3. Overwrites AFTER snapshot (triggers BlueStore clones)
    4. Partial writes AFTER snapshot (triggers BlueStore clones)
    5. Mixed read/write (reads expose corruption)
    6. Delete ~20% of files
    7. Delete ~20% of snapshots (CephFS only)

    Note: For RBD, clone-on-write is handled by thrash_rbd_snapshots which
    creates RBD-level snapshots on the image. FIO writes to the mounted
    RBD image will trigger clones when those snapshots exist.

    Args:
        client_node: Client node to execute FIO
        mount_path: Mount path
        workload_name: Name for the workload (for logging) - "cephfs" or "rbd"
        duration: Total duration in seconds
        stop_flag: Dict with 'stop' key to signal early termination
        enable_cephfs_snapshots: Whether to create/delete CephFS snapshots (only applies when workload_name="cephfs")

    Returns:
        Number of FIO runs completed
    """
    log.info(f"Starting FIO workload ({workload_name}) on: {mount_path}")
    runs, end_time = 0, time.time() + duration
    work_dir = f"{mount_path}/fio_{workload_name}"
    is_cephfs = workload_name == "cephfs"
    snap_dir, all_snaps = (f"{work_dir}/.snap", []) if is_cephfs else (None, [])

    client_node.exec_command(cmd=f"mkdir -p {work_dir}", sudo=True, check_ec=False)

    # FIO commands with shared base options
    base = (
        f"--directory={work_dir} --size=50M --numjobs=4 --ioengine=libaio "
        "--direct=1 --time_based --group_reporting --nrfiles=20"
    )
    fio_cmds = [
        f"fio --name={workload_name}-write {base} --rw=write --bs=64k --runtime=15 --create_on_open=1",
        f"fio --name={workload_name}-overwrite {base} --rw=write --bs=64k --runtime=10 --overwrite=1",
        f"fio --name={workload_name}-partial {base} --rw=randwrite --bs=4k --runtime=15 --overwrite=1",
        f"fio --name={workload_name}-mixed {base} --rw=randrw --rwmixread=70 --bs=4k --runtime=15 --overwrite=1",
    ]

    try:
        while time.time() < end_time and not stop_flag.get("stop"):
            runs += 1
            log.debug(f"FIO {workload_name} iteration {runs}")

            # Phase 1: Initial writes (create files)
            client_node.exec_command(cmd=fio_cmds[0], sudo=True, check_ec=False)
            if stop_flag.get("stop"):
                break

            # Phase 2: Create CephFS snapshot (enables clone-on-write for subsequent writes)
            if is_cephfs and enable_cephfs_snapshots:
                snap_name = f"fio_snap_{runs}_{int(time.time())}"
                client_node.exec_command(
                    cmd=f"mkdir -p {snap_dir}/{snap_name}", sudo=True, check_ec=False
                )
                all_snaps.append(snap_name)
                log.debug(f"Created CephFS snapshot: {snap_name}")

            # Phases 3-5: Overwrites, partial writes, mixed I/O (trigger clones after snapshot)
            for cmd in fio_cmds[1:]:
                if stop_flag.get("stop"):
                    break
                client_node.exec_command(cmd=cmd, sudo=True, check_ec=False)

            if stop_flag.get("stop"):
                break

            # Phase 6: Delete ~20% of files
            delete_cmd = (
                f"find {work_dir} -maxdepth 1 -type f 2>/dev/null | "
                "awk 'BEGIN{srand()} {if(rand()<0.2) print}' | xargs -r rm -f"
            )
            client_node.exec_command(
                cmd=delete_cmd,
                sudo=True,
                check_ec=False,
            )

            # Phase 7: Delete ~20% of snapshots (CephFS only)
            if (
                is_cephfs
                and enable_cephfs_snapshots
                and all_snaps
                and random.random() < 0.2
            ):
                snap = random.choice(all_snaps)
                client_node.exec_command(
                    cmd=f"rmdir {snap_dir}/{snap} 2>/dev/null || true",
                    sudo=True,
                    check_ec=False,
                )
                all_snaps.remove(snap)

            time.sleep(random.uniform(2, 5))

        snap_info = (
            f", {len(all_snaps)} snapshots"
            if is_cephfs and enable_cephfs_snapshots
            else ""
        )
        log.info(f"FIO {workload_name} completed: {runs} iterations{snap_info}")
        return runs
    except Exception as e:
        log.warning(f"FIO {workload_name} error: {e}")
        return runs


def _restore_osds_to_in(rados_obj: RadosOrchestrator, osd_ids: List[int]) -> None:
    """Restore OSDs to 'in' state, logging failures but continuing."""
    if not osd_ids:
        return

    log.info(f"Restoring {len(osd_ids)} OSD(s) to 'in' state: {osd_ids}")
    for osd_id in osd_ids:
        try:
            rados_obj.update_osd_state_on_cluster(osd_id=osd_id, state="in")
            rados_obj.client.exec_command(
                cmd=f"ceph orch daemon start osd.{osd_id}", sudo=True
            )
            time.sleep(5)
        except Exception as e:
            log.error(f"Failed to restore OSD.{osd_id}: {e}")


def thrash_osds(
    rados_obj: RadosOrchestrator,
    osd_list: List[int],
    iterations: int,
    aggressive: bool,
    stop_flag: Dict,
    reboot_osd: bool = False,
) -> int:
    """
    Thrash OSDs by marking out/in and stopping/starting to trigger peering events.

    Per iteration:
    1. Select 1-3 random OSDs
    2. Mark each OSD out (ceph osd out)
    3. Stop each OSD daemon (ceph orch daemon stop)
    4. Wait (3-6s normal, 8-15s aggressive)
    5. Mark each OSD in (ceph osd in)
    6. Start each OSD daemon (ceph orch daemon start)

    The OSD state transitions during active I/O create peering events. When
    combined with EC pool snapshots (thrash_ec_pool_snapshots), this creates
    conditions for divergent PG logs that can trigger the bug.

    Args:
        rados_obj: RadosOrchestrator object
        osd_list: List of OSD IDs
        iterations: Number of thrashing iterations
        aggressive: Enable aggressive mode (longer waits for recovery stress)
        stop_flag: Dict with 'stop' key to signal early termination

    Returns:
        Number of iterations completed
    """
    log.info(
        f"Starting OSD thrashing (iterations: {iterations}, aggressive: {aggressive})"
    )

    # Pre-calculate parameters
    sleep_duration = (2, 4) if aggressive else (3, 6)
    max_to_thrash = min(random.choice([1, 2]), len(osd_list))

    completed = 0
    osds_currently_out = []

    try:
        for iteration in range(iterations):
            if stop_flag.get("stop"):
                break

            # Select random OSDs to thrash (ensure we don't sample more than available)
            num_to_thrash = random.randint(1, max_to_thrash)
            osds_to_thrash = random.sample(osd_list, num_to_thrash)
            log.info(
                f"Iteration {iteration + 1}/{iterations}: Thrashing {osds_to_thrash}"
            )

            # Mark OSDs out and stop them
            for osd_id in osds_to_thrash:
                # Add to tracking list BEFORE making changes
                if osd_id not in osds_currently_out:
                    osds_currently_out.append(osd_id)

                rados_obj.update_osd_state_on_cluster(osd_id=osd_id, state="out")
                if reboot_osd:
                    rados_obj.client.exec_command(
                        cmd=f"ceph orch daemon stop osd.{osd_id}", sudo=True
                    )

                if stop_flag.get("stop"):
                    break

            # Wait while OSDs are down (allows I/O to continue on degraded PGs)
            time.sleep(random.uniform(*sleep_duration))

            # Bring OSDs back in - ALWAYS restore all OSDs that were marked out this iteration
            for osd_id in osds_to_thrash:
                try:
                    rados_obj.update_osd_state_on_cluster(osd_id=osd_id, state="in")
                    if reboot_osd:
                        rados_obj.client.exec_command(
                            cmd=f"ceph orch daemon start osd.{osd_id}", sudo=True
                        )
                    if osd_id in osds_currently_out:
                        osds_currently_out.remove(osd_id)
                except Exception as e:
                    log.warning(f"Failed to restore OSD.{osd_id} in iteration: {e}")

            # Check stop flag AFTER restoration, not during
            if stop_flag.get("stop"):
                break

            # Wait for recovery to start
            time.sleep(random.uniform(1, 3))

            completed += 1
            time.sleep(random.uniform(2, 5))

    finally:
        # Restore any OSDs still marked out
        if osds_currently_out:
            log.info(f"Restoring {len(osds_currently_out)} OSD(s) to 'in' state...")
            _restore_osds_to_in(rados_obj, osds_currently_out)
            osds_currently_out.clear()

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
    Thrash CRUSH weights to force data movement and trigger backfill/recovery.

    Per iteration:
    1. Select random OSD
    2. Reduce CRUSH weight to 50% (triggers data migration)
    3. Wait 30-60 seconds
    4. Restore original weight (triggers reverse migration)

    This creates additional stress by forcing data movement during other
    thrashing operations, increasing chances of catching race conditions.

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
            try:
                rados_obj.run_ceph_command(
                    cmd=f"ceph osd crush reweight osd.{osd_id} {original_weight * 0.5}",
                    client_exec=True,
                )
                current_modified_osd = osd_id
                current_original_weight = original_weight
            except Exception as e:
                log.warning(f"Failed to reduce weight for OSD.{osd_id}: {e}")
                continue

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
    Thrash PG count via bulk flag and trigger scrubbing to detect inconsistencies.

    Per iteration:
    1. Enable bulk flag on all pools (triggers PG split)
    2. Run cluster-wide deep-scrub
    3. Wait 90-120 seconds for splits and deep-scrub
    4. Remove bulk flag from all pools (triggers PG merge)
    5. Run cluster-wide scrub
    6. Wait 90-120 seconds for merges and scrub

    Deep-scrub and scrub operations are critical for detecting:
    - Missing EC shard objects (from bug #74221)
    - Data inconsistencies from incorrect clones
    - Object mismatches between shards

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

            # Phase 1: Enable bulk flag on ALL pools
            for pool in rados_pools:
                pool_name = pool["pool_name"]
                pool_obj.set_bulk_flag(pool_name=pool_name)
                bulk_enabled_pools.append(pool_name)

            # Trigger cluster-wide deep scrub once (not per-pool)
            if bulk_enabled_pools:
                rados_obj.run_deep_scrub()
                pool_count = len(bulk_enabled_pools)
                log.info(
                    f"  Enabled bulk on {pool_count} pool(s), sleeping for PG splits and deep scrubbing..."
                )

                # Phase 2: Sleep while ALL pools split PGs simultaneously and deep scrubbing
                time.sleep(random.uniform(90, 120))

                # Phase 3: Remove bulk flag from ALL pools
                log.info(
                    f"  Removing bulk from {pool_count} pool(s) and triggering scrub..."
                )
                for pool_name in bulk_enabled_pools:
                    pool_obj.rm_bulk_flag(pool_name=pool_name)
                bulk_enabled_pools.clear()

                # Trigger cluster-wide scrub once (not per-pool)
                rados_obj.run_scrub()

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


def thrash_cephfs_snapshots(
    client_node, mount_path: str, duration: int, stop_flag: Dict
) -> int:
    """
    Thrash CephFS snapshots with parallel operations to amplify clone triggers.

    Creates MULTIPLE snapshots per iteration, then performs parallel writes
    (each write triggers clone for EACH existing snapshot). Deletes ~20% of
    snapshots to add variety.

    Args:
        client_node: Client node to execute commands
        mount_path: CephFS mount path
        duration: Total duration in seconds
        stop_flag: Dict with 'stop' key to signal early termination

    Returns:
        Number of snapshot operations completed
    """
    log.info(f"Starting CephFS snapshot thrashing on: {mount_path}")
    ops_completed, end_time, iteration = 0, time.time() + duration, 0
    all_snaps = []  # Track (test_dir, snap_name) tuples

    try:
        while time.time() < end_time and not stop_flag.get("stop"):
            iteration += 1
            ts = int(time.time())
            test_dir = f"{mount_path}/snap_test_{iteration}_{ts}"
            snap_dir = f"{test_dir}/.snap"

            # Create test directory and files in parallel
            client_node.exec_command(
                cmd=f"mkdir -p {test_dir}", sudo=True, check_ec=False
            )
            with cf.ThreadPoolExecutor(max_workers=5) as executor:
                futures = [
                    executor.submit(
                        client_node.exec_command,
                        cmd=f"dd if=/dev/urandom of={test_dir}/file{i}.dat bs=1M count=3 conv=fsync 2>/dev/null",
                        sudo=True,
                        check_ec=False,
                    )
                    for i in range(5)
                ]
                for f in futures:
                    try:
                        f.result(timeout=60)
                        ops_completed += 1
                    except Exception:
                        pass

            if stop_flag.get("stop"):
                break

            # Create 5 snapshots with writes between them (triggers clones)
            snap_names = []
            for s in range(5):
                snap_name = f"snap{s}_{iteration}_{ts}"
                try:
                    client_node.exec_command(
                        cmd=f"mkdir -p {snap_dir}/{snap_name}",
                        sudo=True,
                        check_ec=False,
                    )
                    snap_names.append(snap_name)
                    all_snaps.append((test_dir, snap_name))
                    ops_completed += 1
                except Exception:
                    pass
                # Write between snapshots (triggers clone for previous snaps)
                if s < 4:
                    try:
                        write_cmd = (
                            f"dd if=/dev/urandom of={test_dir}/file{s}.dat "
                            "bs=64K count=1 conv=notrunc,fsync 2>/dev/null"
                        )
                        client_node.exec_command(
                            cmd=write_cmd,
                            sudo=True,
                            check_ec=False,
                        )
                        ops_completed += 1
                    except Exception:
                        pass

            if stop_flag.get("stop"):
                break

            # Parallel operations: writes, truncate, append
            with cf.ThreadPoolExecutor(max_workers=8) as executor:
                cmds = [
                    *[
                        f"dd if=/dev/urandom of={test_dir}/file{i}.dat bs=32K count=1 conv=notrunc,fsync 2>/dev/null"
                        for i in range(5)
                    ],
                    f"truncate -s 512K {test_dir}/file0.dat",
                    f"dd if=/dev/urandom bs=8K count=1 >> {test_dir}/file1.dat 2>/dev/null",
                    f"dd if=/dev/urandom of={test_dir}/file2.dat bs=16K count=1 conv=notrunc,fsync 2>/dev/null",
                ]
                futures = [
                    executor.submit(
                        client_node.exec_command, cmd=c, sudo=True, check_ec=False
                    )
                    for c in cmds
                ]
                for f in futures:
                    try:
                        f.result(timeout=30)
                        ops_completed += 1
                    except Exception:
                        pass

            # Rollback from random snapshot
            if snap_names:
                snap = random.choice(snap_names)
                try:
                    client_node.exec_command(
                        cmd=f"cp {snap_dir}/{snap}/file0.dat {test_dir}/file0_restored.dat",
                        sudo=True,
                        check_ec=False,
                    )
                    ops_completed += 1
                except Exception:
                    pass

            # Parallel reads + list snapshots
            with cf.ThreadPoolExecutor(max_workers=6) as executor:
                read_cmds = [
                    f"cat {test_dir}/file{i}.dat > /dev/null" for i in range(5)
                ]
                read_cmds.append(f"ls -la {snap_dir}/")
                futures = [
                    executor.submit(
                        client_node.exec_command, cmd=c, sudo=True, check_ec=False
                    )
                    for c in read_cmds
                ]
                for f in futures:
                    try:
                        f.result(timeout=30)
                        ops_completed += 1
                    except Exception:
                        pass

            # Delete ~20% of snapshots
            if all_snaps and random.random() < 0.2:
                snap_to_delete = random.choice(all_snaps)
                try:
                    client_node.exec_command(
                        cmd=f"rmdir {snap_to_delete[0]}/.snap/{snap_to_delete[1]}",
                        sudo=True,
                        check_ec=False,
                    )
                    all_snaps.remove(snap_to_delete)
                    ops_completed += 1
                    log.debug(f"Deleted snapshot: {snap_to_delete[1]}")
                except Exception:
                    pass

            log.debug(
                f"Iteration {iteration}: {len(snap_names)} new snaps, {len(all_snaps)} total"
            )
            time.sleep(random.uniform(1, 3))

    except Exception as e:
        log.warning(f"CephFS snapshot thrashing error: {e}")

    log.info(
        f"CephFS snapshot thrashing completed: {ops_completed} ops, {len(all_snaps)} snapshots"
    )
    return ops_completed


def thrash_rbd_snapshots(
    client_node,
    pool_name: str,
    image_name: str,
    duration: int,
    stop_flag: Dict,
) -> int:
    """
    Thrash RBD snapshots with parallel operations to amplify clone triggers.

    Creates MULTIPLE snapshots per iteration, performs parallel writes (each
    write triggers clone for ALL existing snapshots), creates clones from
    snapshots, and deletes ~20% of snapshots.

    Args:
        client_node: Client node to execute commands
        pool_name: RBD pool name (metadata pool)
        image_name: Existing RBD image name
        duration: Total duration in seconds
        stop_flag: Dict with 'stop' key to signal early termination

    Returns:
        Number of snapshot operations completed
    """
    log.info(f"Starting RBD snapshot thrashing on {pool_name}/{image_name}")
    ops_completed, end_time, iteration = 0, time.time() + duration, 0
    all_clones, all_snaps = [], []  # clones list, (snap_name, is_protected) tuples

    try:
        while time.time() < end_time and not stop_flag.get("stop"):
            iteration += 1
            ts = int(time.time())

            # Create 4 snapshots with writes between them (triggers clones)
            new_snaps = []
            for s in range(4):
                if stop_flag.get("stop"):
                    break
                snap_name = f"snap{s}_{iteration}_{ts}"
                try:
                    client_node.exec_command(
                        cmd=f"rbd snap create {pool_name}/{image_name}@{snap_name}",
                        sudo=True,
                        check_ec=False,
                    )
                    new_snaps.append(snap_name)
                    all_snaps.append((snap_name, False))
                    ops_completed += 1
                except Exception:
                    pass
                # Write between snapshots (triggers clone for previous snaps)
                if s < 3:
                    try:
                        client_node.exec_command(
                            cmd=f"rbd bench {pool_name}/{image_name} --io-type write --io-size 4K --io-total 256K",
                            sudo=True,
                            check_ec=False,
                        )
                        ops_completed += 1
                    except Exception:
                        pass

            if stop_flag.get("stop"):
                break

            # Parallel writes after all snapshots (each write triggers clone for all snaps)
            with cf.ThreadPoolExecutor(max_workers=4) as executor:
                cmd = f"rbd bench {pool_name}/{image_name} --io-type write --io-size 4K --io-total 512K"
                futures = [
                    executor.submit(
                        client_node.exec_command, cmd=cmd, sudo=True, check_ec=False
                    )
                    for _ in range(4)
                ]
                for f in futures:
                    try:
                        f.result(timeout=60)
                        ops_completed += 1
                    except Exception:
                        pass

            # Protect random snapshots and create clones
            unprotected = [(s, p) for s, p in all_snaps if not p]
            if len(unprotected) >= 2:
                for snap_name, _ in random.sample(
                    unprotected, min(2, len(unprotected))
                ):
                    try:
                        client_node.exec_command(
                            cmd=f"rbd snap protect {pool_name}/{image_name}@{snap_name}",
                            sudo=True,
                            check_ec=False,
                        )
                        all_snaps = [
                            (s, True) if s == snap_name else (s, p)
                            for s, p in all_snaps
                        ]
                        ops_completed += 1
                        # Create clone from protected snapshot
                        clone_name = f"clone_{snap_name}"
                        client_node.exec_command(
                            cmd=f"rbd clone {pool_name}/{image_name}@{snap_name} {pool_name}/{clone_name}",
                            sudo=True,
                            check_ec=False,
                        )
                        all_clones.append(clone_name)
                        ops_completed += 1
                    except Exception:
                        pass

            if stop_flag.get("stop"):
                break

            # Parallel writes to clones (triggers copy-on-write)
            if all_clones:
                with cf.ThreadPoolExecutor(max_workers=3) as executor:
                    clones_to_write = random.sample(all_clones, min(3, len(all_clones)))
                    futures = [
                        executor.submit(
                            client_node.exec_command,
                            cmd=f"rbd bench {pool_name}/{c} --io-type write --io-size 4K --io-total 1M",
                            sudo=True,
                            check_ec=False,
                        )
                        for c in clones_to_write
                    ]
                    for f in futures:
                        try:
                            f.result(timeout=60)
                            ops_completed += 1
                        except Exception:
                            pass

            # Read + list snapshots
            try:
                client_node.exec_command(
                    cmd=f"rbd bench {pool_name}/{image_name} --io-type read --io-size 4K --io-total 256K",
                    sudo=True,
                    check_ec=False,
                )
                ops_completed += 1
            except Exception:
                log.debug(f"Read failed on {pool_name}/{image_name}")

            try:
                client_node.exec_command(
                    cmd=f"rbd snap ls {pool_name}/{image_name}",
                    sudo=True,
                    check_ec=False,
                )
                ops_completed += 1
            except Exception:
                pass

            # Delete ~20% of unprotected snapshots
            unprotected = [(s, p) for s, p in all_snaps if not p]
            if unprotected and random.random() < 0.2:
                snap_to_delete = random.choice(unprotected)[0]
                try:
                    client_node.exec_command(
                        cmd=f"rbd snap rm {pool_name}/{image_name}@{snap_to_delete}",
                        sudo=True,
                        check_ec=False,
                    )
                    all_snaps = [(s, p) for s, p in all_snaps if s != snap_to_delete]
                    ops_completed += 1
                    log.debug(f"Deleted RBD snapshot: {snap_to_delete}")
                except Exception:
                    pass

            log.debug(
                f"Iteration {iteration}: {len(new_snaps)} new snaps, {len(all_snaps)} total, {len(all_clones)} clones"
            )
            time.sleep(random.uniform(1, 3))

    except Exception as e:
        log.warning(f"RBD snapshot thrashing error: {e}")

    log.info(
        f"RBD snapshot thrashing completed: {ops_completed} ops, {len(all_snaps)} snapshots, {len(all_clones)} clones"
    )
    return ops_completed


def thrash_ec_pool_snapshots(
    rados_obj: RadosOrchestrator,
    pools: List[Dict],
    duration: int,
    stop_flag: Dict,
    reboot_osd: bool = False,
) -> int:
    """
    Thrash EC pools with MULTIPLE RADOS-level snapshots and parallel partial writes
    to maximize BlueStore clone operations (Tracker #74221 bug trigger).

    1. Create MULTIPLE pool-level snapshots
    2. Partial write + OSD down SIMULTANEOUSLY (background &)
       - Get acting set for object
       - Run: partial_write & ; ceph osd down <acting_osd>
       - OSD goes down during write, comes back automatically
    3. Clone log → ALL shards, write → SOME shards → divergent log → CRASH

    Args:
        rados_obj: RadosOrchestrator object
        pools: List of pool configurations (filters EC pools only)
        duration: Total duration in seconds
        stop_flag: Dict with 'stop' key to signal early termination
        reboot_osd: Whether to reboot the OSD

    Returns:
        Number of operations completed
    """
    ec_pools = [p for p in pools if p.get("pool_type") == "erasure"]
    if not ec_pools:
        log.info("No EC pools found, skipping EC pool snapshot thrashing")
        return 0

    log.info(f"Starting EC pool snapshot thrashing on {len(ec_pools)} EC pool(s)")
    ops_completed, end_time, iteration = 0, time.time() + duration, 0
    all_snaps = {}  # pool_name -> list of snap_names

    def _partial_write_with_osd_down(
        pool_name: str, obj_name: str, offset: int, size: int = 4096
    ):
        """Partial write + OSD down simultaneously (matches dev script)."""
        try:
            write_cmd = (
                f"dd if=/dev/urandom bs={size} count=1 2>/dev/null | "
                f"rados -p {pool_name} put {obj_name} - --offset {offset}"
            )
            osd_map = rados_obj.get_osd_map(pool=pool_name, obj=obj_name)
            acting = osd_map.get("acting", []) if osd_map else []

            if not acting or not reboot_osd:
                rados_obj.client.exec_command(cmd=write_cmd, sudo=True)
                return True

            target_osd = acting[1] if len(acting) > 1 else acting[0]
            cmd = f"{{ {write_cmd}; }} & ceph osd down {target_osd};ceph orch daemon restart osd.{target_osd}"
            rados_obj.client.exec_command(cmd=cmd, sudo=True, check_ec=False)
            log.debug(
                f"Partial write + OSD.{target_osd} restart: {pool_name}/{obj_name}"
            )
            return True
        except Exception:
            return False

    try:
        while time.time() < end_time and not stop_flag.get("stop"):
            iteration += 1
            ts = int(time.time())

            for pool in ec_pools:
                if stop_flag.get("stop"):
                    break

                pool_name = pool["pool_name"]
                if pool_name not in all_snaps:
                    all_snaps[pool_name] = []

                obj_names = [f"ec_snap_obj_{iteration}_{ts}_{i}" for i in range(3)]

                # Step 1: Create base objects in parallel (full stripe writes)
                with cf.ThreadPoolExecutor(max_workers=3) as executor:
                    futures = [
                        executor.submit(
                            rados_obj.client.exec_command,
                            cmd=f"dd if=/dev/urandom bs=65536 count=2 2>/dev/null | rados -p {pool_name} put {oname} -",
                            sudo=True,
                        )
                        for oname in obj_names
                    ]
                    created_objs = []
                    for oname, f in zip(obj_names, futures):
                        try:
                            f.result(timeout=60)
                            created_objs.append(oname)
                            ops_completed += 1
                        except Exception:
                            pass

                if not created_objs:
                    continue

                # Step 2: Create 4 snapshots with partial writes between them
                new_snaps = []
                for s in range(4):
                    snap_name = f"snap{s}_{iteration}_{ts}"
                    try:
                        rados_obj.client.exec_command(
                            cmd=f"rados -p {pool_name} mksnap {snap_name}", sudo=True
                        )
                        new_snaps.append(snap_name)
                        all_snaps[pool_name].append(snap_name)
                        ops_completed += 1
                    except Exception:
                        pass
                    # Partial write between snapshots (with OSD down)
                    if s < 3 and _partial_write_with_osd_down(
                        pool_name, created_objs[0], s * 4096
                    ):
                        ops_completed += 1

                if stop_flag.get("stop"):
                    break

                # Step 3: Parallel partial writes + OSD down on all objects
                with cf.ThreadPoolExecutor(max_workers=18) as executor:
                    futures = [
                        executor.submit(
                            _partial_write_with_osd_down, pool_name, oname, offset
                        )
                        for oname in created_objs
                        for offset in [0, 4096, 8192, 16384, 32768, 49152]
                    ]
                    for f in futures:
                        try:
                            if f.result(timeout=30):
                                ops_completed += 1
                        except Exception:
                            pass

                # Step 4: Append and truncate on all objects
                with cf.ThreadPoolExecutor(max_workers=6) as executor:
                    cmds = []
                    for oname in created_objs:
                        cmds.append(
                            f"dd if=/dev/urandom bs=8192 count=1 2>/dev/null | rados -p {pool_name} append {oname} -"
                        )
                        cmds.append(f"rados -p {pool_name} truncate {oname} 32768")
                    futures = [
                        executor.submit(rados_obj.client.exec_command, cmd=c, sudo=True)
                        for c in cmds
                    ]
                    for f in futures:
                        try:
                            f.result(timeout=30)
                            ops_completed += 1
                        except Exception:
                            pass

                # Step 5: Rollback all objects to random snapshot
                if new_snaps:
                    random_snap = random.choice(new_snaps)
                    with cf.ThreadPoolExecutor(
                        max_workers=len(created_objs)
                    ) as executor:
                        futures = [
                            executor.submit(
                                rados_obj.client.exec_command,
                                cmd=f"rados -p {pool_name} rollback {oname} {random_snap}",
                                sudo=True,
                            )
                            for oname in created_objs
                        ]
                        for f in futures:
                            try:
                                f.result(timeout=30)
                                ops_completed += 1
                            except Exception:
                                pass

                # Step 6: More partial writes + OSD down after rollback
                with cf.ThreadPoolExecutor(max_workers=9) as executor:
                    futures = [
                        executor.submit(
                            _partial_write_with_osd_down, pool_name, oname, offset
                        )
                        for oname in created_objs
                        for offset in [0, 8192, 16384]
                    ]
                    for f in futures:
                        try:
                            if f.result(timeout=30):
                                ops_completed += 1
                        except Exception:
                            pass

                # Step 7: Read + listsnaps + lssnap
                with cf.ThreadPoolExecutor(
                    max_workers=len(created_objs) * 2 + 1
                ) as executor:
                    # Reads use check_ec=False
                    read_futures = [
                        executor.submit(
                            rados_obj.client.exec_command,
                            cmd=f"rados -p {pool_name} get {o} /dev/null",
                            sudo=True,
                            check_ec=False,
                        )
                        for o in created_objs
                    ]
                    listsnaps_futures = [
                        executor.submit(
                            rados_obj.client.exec_command,
                            cmd=f"rados -p {pool_name} listsnaps {o}",
                            sudo=True,
                        )
                        for o in created_objs
                    ]
                    lssnap_future = executor.submit(
                        rados_obj.client.exec_command,
                        cmd=f"rados -p {pool_name} lssnap",
                        sudo=True,
                    )
                    for f in read_futures + listsnaps_futures + [lssnap_future]:
                        try:
                            f.result(timeout=30)
                            ops_completed += 1
                        except Exception:
                            pass

                # Step 8: Delete ~20% of snapshots
                pool_snaps = all_snaps.get(pool_name, [])
                if pool_snaps and random.random() < 0.2:
                    snap_to_delete = random.choice(pool_snaps)
                    try:
                        rados_obj.client.exec_command(
                            cmd=f"rados -p {pool_name} rmsnap {snap_to_delete}",
                            sudo=True,
                        )
                        all_snaps[pool_name].remove(snap_to_delete)
                        ops_completed += 1
                        log.debug(
                            f"Deleted RADOS snapshot: {pool_name}@{snap_to_delete}"
                        )
                    except Exception:
                        pass

            total_snaps = sum(len(s) for s in all_snaps.values())
            log.debug(
                f"EC pool snapshot iteration {iteration}: {total_snaps} total snaps"
            )
            time.sleep(random.uniform(0.1, 0.5))

    except Exception as e:
        log.warning(f"EC pool snapshot thrashing error: {e}")

    total_snaps = sum(len(s) for s in all_snaps.values())
    log.info(
        f"EC pool snapshot thrashing completed: {ops_completed} ops, {total_snaps} snapshots"
    )
    return ops_completed


def setup_rgw_s3_client(
    client_node,
    rados_obj: RadosOrchestrator,
    data_pool_type: str = "replicated",
    ec_config: Optional[Dict[str, Any]] = None,
    enable_fast_ec_config_params: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    Setup RGW S3 client by discovering endpoints, installing dependencies,
    creating user, and configuring tools.

    This is a self-contained setup method that:
    1. Discovers RGW daemon endpoints via 'ceph orch ps --daemon_type rgw'
    2. Installs awscli and s3cmd on the client node
    3. Creates or reuses an RGW user with S3 credentials
    4. Configures awscli and s3cmd with credentials
    5. Creates both a normal bucket and a versioned bucket
    6. Optionally configures an EC pool as the RGW data pool

    Args:
        client_node: Client node to configure s3cmd/awscli
        rados_obj: RadosOrchestrator object for running ceph commands
        data_pool_type: Type of data pool - "replicated" (default) or "erasure"
        ec_config: Optional EC pool configuration when data_pool_type is "erasure"
                   Example: {"k": 2, "m": 2, "plugin": "jerasure"}
                   If not provided, defaults to k=2, m=2
        enable_fast_ec_config_params: Whether to enable fast EC config params
    Returns:
        Dict with access_key, secret_key, endpoints, buckets, uid, ec_pool_name or None if setup fails
    """
    import json as json_module  # Only needed for zone config serialization

    ec_pool_name = None
    original_zone_config = None

    try:
        # Step 1: Discover RGW endpoints via ceph orch ps
        log.info("Discovering RGW daemon endpoints...")
        rgw_endpoints = []
        try:
            rgw_daemons = rados_obj.run_ceph_command(
                cmd="ceph orch ps --daemon_type rgw"
            )
            if rgw_daemons:
                for daemon in rgw_daemons:
                    if daemon.get("status_desc") == "running":
                        hostname = daemon.get("hostname")
                        ports = daemon.get("ports") or [80]
                        port = ports[0] if ports else 80
                        if hostname:
                            rgw_endpoints.append(
                                {
                                    "hostname": hostname,
                                    "port": port,
                                    "daemon_id": daemon.get("daemon_id"),
                                    "endpoint": f"http://{hostname}:{port}",
                                }
                            )
        except Exception as e:
            log.warning(f"Failed to query RGW daemons: {e}")

        if not rgw_endpoints:
            log.warning("No running RGW daemons found")
            return None

        log.info(
            f"Found {len(rgw_endpoints)} RGW endpoint(s): "
            f"{', '.join(ep['endpoint'] for ep in rgw_endpoints)}"
        )

        # Use first endpoint for initial setup
        primary_ep = rgw_endpoints[0]
        rgw_host = primary_ep["hostname"]
        rgw_port = primary_ep["port"]
        rgw_endpoint = primary_ep["endpoint"]

        # Step 1.5: Configure EC pool as RGW data pool if requested
        if data_pool_type == "erasure":
            log.info("Configuring EC pool as RGW data pool...")

            # Get EC parameters from config or use defaults
            if ec_config is None:
                ec_config = {}
            ec_k = ec_config.get("k", 2)
            ec_m = ec_config.get("m", 2)
            ec_plugin = ec_config.get("plugin", "isa")
            enable_fast_ec = ec_config.get(
                "enable_fast_ec_features", enable_fast_ec_config_params
            )

            ts = int(time.time())
            ec_pool_name = f"rgw-ec-data-pool-{ts}"

            # Create EC pool using existing method with fast EC features enabled
            log.info(
                f"Creating EC pool: {ec_pool_name} (k={ec_k}, m={ec_m}, "
                f"plugin={ec_plugin}, fast_ec={enable_fast_ec})"
            )
            ec_pool_params = {
                "pool_name": ec_pool_name,
                "k": ec_k,
                "m": ec_m,
                "plugin": ec_plugin,
                "app_name": "rgw",
                "enable_fast_ec_features": enable_fast_ec,
            }

            if not rados_obj.create_erasure_pool(**ec_pool_params):
                log.error(f"Failed to create EC pool: {ec_pool_name}")
                raise Exception(f"Failed to create EC pool {ec_pool_name}")

            log.info(
                f"EC pool {ec_pool_name} created successfully with fast EC features"
            )

            # Get current zone configuration
            log.info("Fetching current RGW zone configuration...")
            zone_out, _ = client_node.exec_command(
                cmd="radosgw-admin zone get", sudo=True
            )
            zone_config = json_module.loads(zone_out)
            original_zone_config = json_module.loads(zone_out)  # Independent copy
            log.debug("Original zone config saved for cleanup")

            # Update default-placement to use EC pool for STANDARD storage class
            placement_pools = zone_config.get("placement_pools", [])
            default_placement = next(
                (p for p in placement_pools if p.get("key") == "default-placement"),
                None,
            )
            if default_placement:
                default_placement.setdefault("val", {}).setdefault(
                    "storage_classes", {}
                ).setdefault("STANDARD", {})["data_pool"] = ec_pool_name
            else:
                placement_pools.append(
                    {
                        "key": "default-placement",
                        "val": {
                            "index_pool": "default.rgw.buckets.index",
                            "storage_classes": {
                                "STANDARD": {"data_pool": ec_pool_name}
                            },
                            "data_extra_pool": "default.rgw.buckets.non-ec",
                        },
                    }
                )
            zone_config["placement_pools"] = placement_pools
            log.info(f"Updated STANDARD storage class data_pool to: {ec_pool_name}")

            # Write modified zone config to temp file and set it
            zone_config_str = json_module.dumps(zone_config, indent=2)
            zone_file = f"/tmp/rgw_zone_ec_{ts}.json"
            client_node.exec_command(
                cmd=f"echo '{zone_config_str}' > {zone_file}", sudo=True
            )

            log.info("Setting modified RGW zone configuration...")
            client_node.exec_command(
                cmd=f"radosgw-admin zone set --infile {zone_file}", sudo=True
            )

            # Cleanup temp file
            client_node.exec_command(
                cmd=f"rm -f {zone_file}", sudo=True, check_ec=False
            )

            # Commit the period update to apply zone changes
            log.info("Committing period update to apply zone changes...")
            client_node.exec_command(
                cmd="radosgw-admin period update --commit",
                sudo=True,
                check_ec=False,  # May fail on non-multisite, which is OK
            )

            # Restart RGW daemons to apply zone configuration
            log.info("Restarting RGW daemons to apply zone configuration...")
            if not rados_obj.restart_daemon_services(daemon="rgw", timeout=600):
                log.warning("RGW daemon restart may not have completed successfully")
                raise Exception(
                    "RGW daemon restart may not have completed successfully"
                )

            # Verify zone configuration was applied
            log.info("Verifying zone configuration...")
            zone_verify_out, _ = client_node.exec_command(
                cmd="radosgw-admin zone get", sudo=True
            )
            updated_zone = json_module.loads(zone_verify_out)
            placement_pools = updated_zone.get("placement_pools", [])
            default_placement = next(
                (p for p in placement_pools if p.get("key") == "default-placement"),
                None,
            )
            if default_placement:
                data_pool = (
                    default_placement.get("val", {})
                    .get("storage_classes", {})
                    .get("STANDARD", {})
                    .get("data_pool", "")
                )
                if data_pool == ec_pool_name:
                    log.info(f"Zone configuration verified: data_pool = {ec_pool_name}")
                else:
                    log.warning(
                        f"Zone data_pool mismatch: expected {ec_pool_name}, got {data_pool}"
                    )
                    raise Exception(
                        f"Zone data_pool mismatch: expected {ec_pool_name}, got {data_pool}"
                    )

            log.info(f"EC pool {ec_pool_name} configured as RGW data pool")

        # Step 2: Install S3 client dependencies (awscli, s3cmd)
        log.info("Installing S3 client dependencies (awscli, s3cmd)...")
        client_node.exec_command(
            cmd="pip3 install awscli s3cmd --quiet", sudo=True, check_ec=False
        )

        # Step 3: Check if user already exists, create if not
        uid = f"thrash-user-{int(time.time())}"

        def create_new_user(user_id: str) -> dict:
            """Create a new RGW user and return user info."""
            log.info(f"Creating new RGW user: {user_id}")
            out, _ = client_node.exec_command(
                cmd=f"radosgw-admin user create --uid={user_id} "
                f"--display-name='OSD Thrash Test User' --email={user_id}@test.local",
                sudo=True,
            )
            return json_module.loads(out)

        try:
            users_out, _ = client_node.exec_command(
                cmd="radosgw-admin user list", sudo=True
            )
            existing_users = json_module.loads(users_out) if users_out.strip() else []

            # Look for any existing thrash-user to reuse
            for existing_uid in existing_users:
                if existing_uid.startswith("thrash-user-"):
                    info_out, _ = client_node.exec_command(
                        cmd=f"radosgw-admin user info --uid={existing_uid}",
                        sudo=True,
                    )
                    user_info = json_module.loads(info_out)
                    if user_info.get("keys"):
                        uid = existing_uid
                        log.info(f"Reusing existing RGW user: {uid}")
                        break
            else:
                user_info = create_new_user(uid)
        except Exception as e:
            log.warning(f"Error checking/creating user: {e}, creating new user")
            user_info = create_new_user(uid)

        access_key = user_info["keys"][0]["access_key"]
        secret_key = user_info["keys"][0]["secret_key"]

        # Step 4: Configure awscli
        log.info("Configuring awscli credentials...")
        aws_config = f"""[default]
aws_access_key_id = {access_key}
aws_secret_access_key = {secret_key}
"""
        client_node.exec_command(cmd="mkdir -p ~/.aws", sudo=True, check_ec=False)
        client_node.exec_command(
            cmd=f"echo '{aws_config}' > ~/.aws/credentials", sudo=True, check_ec=False
        )

        # Step 5: Configure s3cmd
        log.info("Configuring s3cmd...")
        s3cmd_config = f"""[default]
access_key = {access_key}
secret_key = {secret_key}
host_base = {rgw_host}:{rgw_port}
host_bucket = {rgw_host}:{rgw_port}/%(bucket)s
use_https = False
signature_v2 = True
"""
        client_node.exec_command(
            cmd=f"echo '{s3cmd_config}' > ~/.s3cfg", sudo=True, check_ec=False
        )

        # Step 6: Create buckets (normal + versioned)
        ts = int(time.time())
        bucket_name = f"thrash-bucket-{ts}"
        versioned_bucket = f"thrash-bucket-ver-{ts}"

        for bkt, enable_versioning in [(bucket_name, False), (versioned_bucket, True)]:
            bkt_type = "versioned" if enable_versioning else "normal"
            log.info(f"Creating {bkt_type} bucket: {bkt}")
            client_node.exec_command(
                cmd=f"aws s3 mb s3://{bkt} --endpoint {rgw_endpoint}",
                sudo=True,
                check_ec=False,
            )
            if enable_versioning:
                client_node.exec_command(
                    cmd=(
                        f"aws --endpoint {rgw_endpoint} s3api put-bucket-versioning "
                        f"--versioning-configuration Status=Enabled --bucket {bkt}"
                    ),
                    sudo=True,
                    check_ec=False,
                )

        # Verify buckets exist
        out, _ = client_node.exec_command(
            cmd=f"aws s3 ls --endpoint {rgw_endpoint}", sudo=True, check_ec=False
        )
        if bucket_name not in out or versioned_bucket not in out:
            log.warning("Bucket creation may have failed")
            raise Exception(
                "Bucket creation may have failed for bucket: {bucket_name} or {versioned_bucket}"
            )

        # Verify bucket placement if EC pool was configured
        pool_info = ""
        if ec_pool_name:
            pool_info = f", data_pool: {ec_pool_name} (EC)"
            # Check bucket stats to verify data pool
            try:
                stats_out, _ = client_node.exec_command(
                    cmd=f"radosgw-admin bucket stats --bucket={bucket_name}",
                    sudo=True,
                )
                bucket_stats = json_module.loads(stats_out)
                bucket_placement = bucket_stats.get("placement_rule", "unknown")
                log.info(f"Bucket {bucket_name} placement_rule: {bucket_placement}")
                log.debug(f"Bucket stats: {bucket_stats}")
            except Exception as bucket_err:
                log.warning(f"Could not verify bucket placement: {bucket_err}")
        log.info(
            f"RGW S3 client setup complete: {len(rgw_endpoints)} endpoints, "
            f"buckets: {bucket_name} (normal), {versioned_bucket} (versioned){pool_info}"
        )
        return {
            "access_key": access_key,
            "secret_key": secret_key,
            "endpoints": rgw_endpoints,
            "primary_endpoint": rgw_endpoint,
            "bucket_name": bucket_name,
            "versioned_bucket": versioned_bucket,
            "uid": uid,
            "data_pool_type": data_pool_type,
            "ec_pool_name": ec_pool_name,
            "original_zone_config": original_zone_config,
        }
    except Exception as e:
        log.warning(f"Failed to setup RGW S3 client: {e}")
        # Cleanup EC pool if created during failed setup
        if ec_pool_name:
            try:
                log.info(f"Cleaning up EC pool {ec_pool_name} after setup failure")
                rados_obj.delete_pool(pool=ec_pool_name)
            except Exception:
                pass
        return None


def thrash_rgw_s3(
    client_node,
    rgw_config: Dict[str, Any],
    duration: int,
    stop_flag: Dict,
) -> int:
    """
    Thrash RGW with S3 operations during OSD thrashing using round-robin endpoints.

    Operations per bucket (normal and versioned):
    1. PUT objects (various sizes: 4KB, 64KB, 1MB, 5MB)
    2. Overwrite existing objects
    3. Multipart upload (10MB in 5MB parts)
    4. GET/verify random objects
    5. LIST bucket contents
    6. HEAD object metadata checks
    7. List object versions (versioned bucket only)
    8. DELETE ~50% of objects

    Notes:
    - Versioned buckets use fixed object names to create multiple versions
    - Normal buckets use unique object names per iteration
    - Uses round-robin across all available RGW endpoints for load distribution

    Args:
        client_node: Client node with awscli/s3cmd installed
        rgw_config: Dict with access_key, secret_key, endpoints, bucket_name, versioned_bucket
        duration: Total duration in seconds
        stop_flag: Dict with 'stop' key to signal early termination

    Returns:
        Number of S3 operations completed
    """
    endpoints = rgw_config.get("endpoints", [])
    access_key = rgw_config["access_key"]
    secret_key = rgw_config["secret_key"]

    if not endpoints:
        log.warning("No RGW endpoints available, skipping RGW S3 thrashing")
        return 0

    # Setup bucket configurations
    buckets = [
        {
            "name": rgw_config["bucket_name"],
            "versioned": False,
            "objects": [],
            "sizes": [4096, 65536, 1048576, 5242880],  # 4KB, 64KB, 1MB, 5MB
        }
    ]
    if rgw_config.get("versioned_bucket"):
        buckets.append(
            {
                "name": rgw_config["versioned_bucket"],
                "versioned": True,
                "objects": [],
                "sizes": [4096, 65536],  # Smaller sizes for versioned bucket
            }
        )

    bucket_info = ", ".join(
        f"{b['name']} ({'versioned' if b['versioned'] else 'normal'})" for b in buckets
    )
    log.info(
        f"Starting RGW S3 thrashing on {len(buckets)} bucket(s): "
        f"{bucket_info}, {len(endpoints)} endpoint(s)"
    )

    ops_completed = 0
    end_time = time.time() + duration
    iteration = 0
    endpoint_idx = 0  # Round-robin counter

    def get_next_endpoint() -> Dict[str, Any]:
        """Get next endpoint in round-robin fashion."""
        nonlocal endpoint_idx
        ep = endpoints[endpoint_idx % len(endpoints)]
        endpoint_idx += 1
        return ep

    def run_s3cmd(cmd_suffix: str, timeout: int = 60) -> bool:
        """Run s3cmd with round-robin endpoint selection."""
        ep = get_next_endpoint()
        host_base = f"{ep['hostname']}:{ep['port']}"
        full_cmd = (
            f"s3cmd --access_key={access_key} --secret_key={secret_key} "
            f"--host={host_base} --host-bucket='{host_base}/%(bucket)s' "
            f"--no-ssl --signature-v2 {cmd_suffix}"
        )
        try:
            client_node.exec_command(
                cmd=full_cmd, sudo=True, check_ec=False, timeout=timeout
            )
            return True
        except Exception as e:
            log.debug(f"s3cmd failed on {ep['endpoint']}: {e}")
            return False

    def run_awscli(cmd_suffix: str, timeout: int = 60) -> bool:
        """Run awscli with round-robin endpoint selection."""
        ep = get_next_endpoint()
        full_cmd = f"aws --endpoint {ep['endpoint']} {cmd_suffix}"
        try:
            client_node.exec_command(
                cmd=full_cmd, sudo=True, check_ec=False, timeout=timeout
            )
            return True
        except Exception as e:
            log.debug(f"awscli failed on {ep['endpoint']}: {e}")
            return False

    try:
        while time.time() < end_time and not stop_flag.get("stop"):
            iteration += 1
            ts = int(time.time() * 1000)

            # Iterate over all buckets (normal and versioned)
            for bucket_cfg in buckets:
                if stop_flag.get("stop"):
                    break

                bucket_name = bucket_cfg["name"]
                is_versioned = bucket_cfg["versioned"]
                bucket_objects = bucket_cfg["objects"]
                bucket_sizes = bucket_cfg["sizes"]

                # Step 1: PUT objects (uses fixed names for versioned to create versions)
                for idx, size in enumerate(bucket_sizes):
                    if stop_flag.get("stop"):
                        break
                    obj_name = (
                        f"obj_{idx}" if is_versioned else f"obj_{iteration}_{idx}_{ts}"
                    )
                    tmp_file = f"tmp_{iteration}_{idx}_{ts}"
                    try:
                        client_node.exec_command(
                            cmd=f"dd if=/dev/urandom of=/tmp/{tmp_file} bs={size} count=1 2>/dev/null",
                            sudo=True,
                            check_ec=False,
                        )
                        if run_s3cmd(
                            f"put /tmp/{tmp_file} s3://{bucket_name}/{obj_name}",
                            timeout=120,
                        ):
                            if obj_name not in bucket_objects:
                                bucket_objects.append(obj_name)
                            ops_completed += 1
                    except Exception:
                        pass
                    finally:
                        client_node.exec_command(
                            cmd=f"rm -f /tmp/{tmp_file}", sudo=True, check_ec=False
                        )

                if stop_flag.get("stop"):
                    break

                # Step 2: Overwrite random existing objects
                if bucket_objects and len(bucket_objects) > 2:
                    overwrite_targets = random.sample(
                        bucket_objects,
                        min(3 if is_versioned else 3, len(bucket_objects)),
                    )
                    for obj_name in overwrite_targets:
                        if stop_flag.get("stop"):
                            break
                        try:
                            size = random.choice(bucket_sizes[:2])
                            client_node.exec_command(
                                cmd=f"dd if=/dev/urandom of=/tmp/overwrite bs={size} count=1 2>/dev/null",
                                sudo=True,
                                check_ec=False,
                            )
                            if run_s3cmd(
                                f"put /tmp/overwrite s3://{bucket_name}/{obj_name}",
                                timeout=60,
                            ):
                                ops_completed += 1
                        except Exception:
                            pass
                        finally:
                            client_node.exec_command(
                                cmd="rm -f /tmp/overwrite", sudo=True, check_ec=False
                            )

                # Step 3: Multipart upload (all buckets)
                # For versioned buckets, use fixed name to create new version
                if not stop_flag.get("stop"):
                    mp_obj = (
                        "multipart_obj"
                        if is_versioned
                        else f"multipart_{iteration}_{ts}"
                    )
                    try:
                        client_node.exec_command(
                            cmd=f"dd if=/dev/urandom of=/tmp/{mp_obj} bs=1M count=10 2>/dev/null",
                            sudo=True,
                            check_ec=False,
                        )
                        if run_s3cmd(
                            f"put --multipart-chunk-size-mb=5 /tmp/{mp_obj} s3://{bucket_name}/{mp_obj}",
                            timeout=180,
                        ):
                            if mp_obj not in bucket_objects:
                                bucket_objects.append(mp_obj)
                            ops_completed += 1
                    except Exception:
                        pass
                    finally:
                        client_node.exec_command(
                            cmd=f"rm -f /tmp/{mp_obj}", sudo=True, check_ec=False
                        )

                # Step 4: GET random objects
                if bucket_objects:
                    get_targets = random.sample(
                        bucket_objects, min(5, len(bucket_objects))
                    )
                    for obj_name in get_targets:
                        if stop_flag.get("stop"):
                            break
                        if run_s3cmd(
                            f"get s3://{bucket_name}/{obj_name} /tmp/s3get_{obj_name} --force",
                            timeout=60,
                        ):
                            ops_completed += 1
                        client_node.exec_command(
                            cmd=f"rm -f /tmp/s3get_{obj_name}",
                            sudo=True,
                            check_ec=False,
                        )

                # Step 5: LIST bucket
                if run_s3cmd(f"ls s3://{bucket_name}/", timeout=30):
                    ops_completed += 1

                # Step 6: HEAD objects
                if bucket_objects:
                    for obj_name in random.sample(
                        bucket_objects, min(3, len(bucket_objects))
                    ):
                        if stop_flag.get("stop"):
                            break
                        if run_s3cmd(f"info s3://{bucket_name}/{obj_name}", timeout=30):
                            ops_completed += 1

                # Step 7: List object versions (versioned bucket only)
                if is_versioned:
                    if run_awscli(
                        f"s3api list-object-versions --bucket {bucket_name} --max-keys 50",
                        timeout=30,
                    ):
                        ops_completed += 1

                # Step 8: DELETE ~50% of objects
                if bucket_objects and random.random() < 0.5:
                    delete_count = max(1, len(bucket_objects) // 2)
                    delete_targets = random.sample(
                        bucket_objects, min(delete_count, len(bucket_objects))
                    )
                    for obj_name in delete_targets:
                        if run_s3cmd(f"del s3://{bucket_name}/{obj_name}", timeout=30):
                            # For versioned buckets, don't remove from list (delete marker created)
                            if not is_versioned:
                                bucket_objects.remove(obj_name)
                            ops_completed += 1

            # Log with endpoint rotation info
            current_ep_idx = endpoint_idx % len(endpoints)
            bucket_stats = ", ".join(
                f"{b['name']}={len(b['objects'])}" for b in buckets
            )
            log.debug(
                f"RGW S3 iteration {iteration}: {ops_completed} ops, "
                f"buckets: {bucket_stats}, "
                f"next endpoint: {endpoints[current_ep_idx]['endpoint']}"
            )
            time.sleep(random.uniform(1, 3))

    except Exception as e:
        log.warning(f"RGW S3 thrashing error: {e}")

    bucket_stats = ", ".join(f"{b['name']}={len(b['objects'])}" for b in buckets)
    log.info(
        f"RGW S3 thrashing completed: {ops_completed} ops, "
        f"buckets: {bucket_stats}, "
        f"used {endpoint_idx} requests across {len(endpoints)} endpoint(s)"
    )
    return ops_completed


def thrash_mon(
    rados_obj: RadosOrchestrator,
    mon_workflow_obj,
    mon_election_obj,
    duration: int,
    stop_flag: Dict,
    enable_election_strategy_thrash: bool = False,
) -> int:
    """
    Holistic MON thrashing - randomly picks between operations each iteration.

    Operations:
    1. Leader failover - Stop leader to trigger re-election
    2. Election strategy change - Switch between classic/connectivity (if enabled)
    3. Rolling restart - Restart MONs daemons via orchestrator

    Args:
        rados_obj: RadosOrchestrator instance
        mon_workflow_obj: MonitorWorkflows instance
        mon_election_obj: MonElectionStrategies instance
        duration: Total duration in seconds
        stop_flag: Dict with 'stop' key to signal early termination
        enable_election_strategy_thrash: Enable election strategy changes (default: False)

    Returns:
        Number of thrash operations completed
    """
    ops_completed = 0
    end_time = time.time() + duration
    iteration = 0

    # Build list of available operations
    operations = ["leader_failover", "rolling_restart"]
    if enable_election_strategy_thrash:
        operations.append("election_strategy")

    log.info(
        f"Starting MON thrashing with operations: {operations} "
        f"(duration: {duration}s)"
    )

    while time.time() < end_time and not stop_flag.get("stop"):
        iteration += 1
        operation = random.choice(operations)
        log.info(f"MON thrash iteration {iteration}: Selected operation = {operation}")

        try:
            if operation == "leader_failover":
                result = _thrash_mon_leader(rados_obj, mon_workflow_obj)
            elif operation == "election_strategy":
                result = _thrash_mon_election_strategy(mon_election_obj)
            elif operation == "rolling_restart":
                result = _thrash_mon_rolling_restart(rados_obj, mon_workflow_obj)
            else:
                result = False

            if result:
                ops_completed += 1
                log.info(f"MON thrash operation '{operation}' completed successfully")
            else:
                log.warning(f"MON thrash operation '{operation}' did not complete")

        except Exception as e:
            log.warning(f"MON thrash iteration {iteration} failed: {e}")

        # Wait between operations to allow cluster stabilization
        time.sleep(20)

    log.info(
        f"MON thrashing completed: {ops_completed} operations in {iteration} iterations"
    )
    return ops_completed


def _thrash_mon_leader(rados_obj: RadosOrchestrator, mon_workflow_obj) -> bool:
    """
    Stop the current MON leader to trigger re-election, then restart it.
    Maintains quorum by only stopping one MON at a time.
    """
    try:
        # Get current leader and quorum
        leader = mon_workflow_obj.get_mon_quorum_leader()
        quorum_hosts = mon_workflow_obj.get_mon_quorum_hosts()
        log.info(f"Current MON leader: {leader}, quorum size: {len(quorum_hosts)}")

        # Stop the leader daemon to trigger election
        log.info(f"Stopping MON leader: {leader}")
        if not rados_obj.change_daemon_systemctl_state(
            action="stop", daemon_type="mon", daemon_id=leader
        ):
            log.warning(f"Failed to stop MON leader: {leader}")
            return False

        time.sleep(15)

        try:
            new_leader = mon_workflow_obj.get_mon_quorum_leader()
            log.info(
                f"New leader elected: {new_leader}"
                if new_leader != leader
                else "Leader unchanged"
            )
        except Exception:
            pass

        # Start the stopped MON
        log.info(f"Starting MON: {leader}")
        rados_obj.change_daemon_systemctl_state(
            action="start", daemon_type="mon", daemon_id=leader
        )
        time.sleep(20)

        if leader in mon_workflow_obj.get_mon_quorum_hosts():
            log.info(f"MON {leader} rejoined quorum successfully")
            return True

        log.warning(f"MON {leader} did not rejoin quorum within 20 seconds")
        return False

    except Exception as e:
        log.error(f"Leader failover failed: {e}")
        return False


def _thrash_mon_election_strategy(mon_election_obj) -> bool:
    """
    Switch MON election strategy randomly between classic, disallow, and connectivity.
    """
    try:
        # Get current strategy
        current_strategy = mon_election_obj.get_election_strategy()
        strategy_map = {1: "classic", 2: "disallow", 3: "connectivity"}
        current_name = strategy_map.get(current_strategy, "unknown")
        log.info(f"Current election strategy: {current_name} ({current_strategy})")

        available_strategies = ["classic", "disallow", "connectivity"]
        # Remove current strategy to ensure we actually change it
        if current_name in available_strategies:
            available_strategies.remove(current_name)
        new_strategy = random.choice(available_strategies)

        log.info(f"Changing election strategy to: {new_strategy}")
        if mon_election_obj.set_election_strategy(mode=new_strategy):
            log.info(f"Election strategy changed to {new_strategy}")
            return True
        else:
            log.warning(f"Failed to change election strategy to {new_strategy}")
            return False

    except Exception as e:
        log.error(f"Election strategy change failed: {e}")
        return False


def _thrash_mon_rolling_restart(rados_obj: RadosOrchestrator, mon_workflow_obj) -> bool:
    """
    Rolling restart of MON daemons using orchestrator.

    Uses restart_daemon_services() which triggers 'ceph orch restart mon'.
    The orchestrator handles rolling restart internally to maintain quorum.
    """
    try:
        # Get quorum hosts before restart for verification
        quorum_hosts_before = mon_workflow_obj.get_mon_quorum_hosts()
        log.info(
            f"Initiating rolling restart of MON daemons via orchestrator. "
            f"Current quorum: {quorum_hosts_before}"
        )

        # Use restart_daemon_services() - orchestrator handles rolling restart
        if not rados_obj.restart_daemon_services(daemon="mon", timeout=300):
            log.warning(
                "MON rolling restart via orchestrator did not complete successfully"
            )
            return False

        # Verify all MONs are back in quorum
        quorum_hosts_after = mon_workflow_obj.get_mon_quorum_hosts()
        if set(quorum_hosts_before) == set(quorum_hosts_after):
            log.info(
                f"MON rolling restart completed. All MONs back in quorum: {quorum_hosts_after}"
            )
            return True
        else:
            log.warning(
                f"Some MONs may not have rejoined quorum. "
                f"Before: {quorum_hosts_before}, After: {quorum_hosts_after}"
            )
            return False

    except Exception as e:
        log.error(f"Rolling restart failed: {e}")
        return False


def thrash_mgr(
    rados_obj: RadosOrchestrator,
    mgr_workflow_obj: MgrWorkflows,
    duration: int,
    stop_flag: Dict,
) -> int:
    """
    Holistic MGR thrashing - randomly picks between operations each iteration.

    Operations:
    1. Failover - Fail active MGR to trigger failover to standby
    2. Rolling restart - Restart MGR daemons via orchestrator
    3. Random fail - Fail a random MGR (active or standby)

    Args:
        rados_obj: RadosOrchestrator instance
        mgr_workflow_obj: MgrWorkflows instance
        duration: Total duration in seconds
        stop_flag: Dict with 'stop' key to signal early termination

    Returns:
        Number of thrash operations completed
    """
    ops_completed = 0
    end_time = time.time() + duration
    iteration = 0

    operations = ["failover", "rolling_restart", "random_fail"]

    log.info(
        f"Starting MGR thrashing with operations: {operations} "
        f"(duration: {duration}s)"
    )

    while time.time() < end_time and not stop_flag.get("stop"):
        iteration += 1
        operation = random.choice(operations)
        log.info(f"MGR thrash iteration {iteration}: Selected operation = {operation}")

        try:
            if operation == "failover":
                result = _thrash_mgr_failover(mgr_workflow_obj)
            elif operation == "rolling_restart":
                result = _thrash_mgr_rolling_restart(rados_obj)
            elif operation == "random_fail":
                result = _thrash_mgr_random_fail(mgr_workflow_obj)
            else:
                result = False

            if result:
                ops_completed += 1
                log.info(f"MGR thrash operation '{operation}' completed successfully")
            else:
                log.warning(f"MGR thrash operation '{operation}' did not complete")

        except Exception as e:
            log.warning(f"MGR thrash iteration {iteration} failed: {e}")

        # Wait between operations to allow cluster stabilization
        time.sleep(20)

    log.info(
        f"MGR thrashing completed: {ops_completed} operations in {iteration} iterations"
    )
    return ops_completed


def _thrash_mgr_failover(mgr_workflow_obj: MgrWorkflows) -> bool:
    """
    Fail the active MGR to trigger failover to a standby MGR.
    """
    try:
        # Get current active MGR
        active_mgr = mgr_workflow_obj.get_active_mgr()
        log.info(f"Current active MGR: {active_mgr}")

        # Get standby list to verify failover target exists
        mgr_list = mgr_workflow_obj.get_mgr_daemon_list()
        standby_mgrs = [m for m in mgr_list if m != active_mgr]

        if not standby_mgrs:
            log.warning("No standby MGR available for failover")
            return False

        log.info(f"Standby MGRs available: {standby_mgrs}")
        log.info(f"Failing active MGR: {active_mgr}")

        mgr_workflow_obj.set_mgr_fail(host=active_mgr)
        time.sleep(10)

        # Verify failover occurred
        new_active_mgr = mgr_workflow_obj.get_active_mgr()
        if new_active_mgr != active_mgr:
            log.info(f"MGR failover successful. New active MGR: {new_active_mgr}")
            return True
        else:
            log.warning(f"MGR failover did not occur. Active MGR still: {active_mgr}")
            return False

    except Exception as e:
        log.error(f"MGR failover failed: {e}")
        return False


def _thrash_mgr_rolling_restart(rados_obj: RadosOrchestrator) -> bool:
    """
    Rolling restart of MGR daemons using orchestrator.
    """
    try:
        log.info("Initiating MGR rolling restart via orchestrator")

        if not rados_obj.restart_daemon_services(daemon="mgr"):
            log.warning("MGR orch restart command returned failure")
            return False

        log.info("MGR rolling restart initiated successfully")
        time.sleep(30)  # Allow time for restart to complete
        return True

    except Exception as e:
        log.error(f"MGR rolling restart failed: {e}")
        return False


def _thrash_mgr_random_fail(mgr_workflow_obj: MgrWorkflows) -> bool:
    """
    Randomly fail any MGR daemon (active or standby) to test recovery.
    """
    try:
        mgr_list = mgr_workflow_obj.get_mgr_daemon_list()

        if len(mgr_list) < 2:
            log.warning("Need at least 2 MGRs for random fail thrashing")
            return False

        # Randomly select a MGR to fail
        target_mgr = random.choice(mgr_list)
        active_mgr = mgr_workflow_obj.get_active_mgr()

        mgr_type = "active" if target_mgr == active_mgr else "standby"
        log.info(f"Randomly selected {mgr_type} MGR to fail: {target_mgr}")

        mgr_workflow_obj.set_mgr_fail(host=target_mgr)
        time.sleep(15)

        # Verify cluster still has an active MGR
        new_active = mgr_workflow_obj.get_active_mgr()
        if new_active:
            log.info(f"MGR fail successful. Current active MGR: {new_active}")
            return True
        else:
            log.warning("No active MGR after fail operation")
            return False

    except Exception as e:
        log.error(f"MGR random fail failed: {e}")
        return False


def thrash_mds(
    rados_obj: RadosOrchestrator,
    fs_names: List[str],
    duration: int,
    stop_flag: Dict,
) -> int:
    """
    Holistic MDS thrashing - randomly picks a filesystem and operation each iteration.

    Operations:
    1. Failover - Fail active MDS to trigger failover to standby
    2. Rolling restart - Restart MDS daemons via orchestrator
    3. Random fail - Fail a random MDS (active or standby)

    Args:
        rados_obj: RadosOrchestrator instance
        fs_names: List of CephFS filesystem names to thrash
        duration: Total duration in seconds
        stop_flag: Dict with 'stop' key to signal early termination

    Returns:
        Number of thrash operations completed
    """
    ops_completed = 0
    end_time = time.time() + duration
    iteration = 0

    operations = ["failover", "rolling_restart", "random_fail"]

    log.info(
        f"Starting MDS thrashing for filesystems: {fs_names} "
        f"with operations: {operations} (duration: {duration}s)"
    )

    while time.time() < end_time and not stop_flag.get("stop"):
        iteration += 1

        # Randomly select a filesystem and an operation
        fs_name = random.choice(fs_names)
        operation = random.choice(operations)

        log.info(
            f"MDS thrash iteration {iteration}: fs={fs_name}, operation={operation}"
        )

        try:
            if operation == "failover":
                result = _thrash_mds_failover(rados_obj, fs_name)
            elif operation == "rolling_restart":
                result = _thrash_mds_rolling_restart(rados_obj, fs_name)
            elif operation == "random_fail":
                result = _thrash_mds_random_fail(rados_obj, fs_name)
            else:
                result = False

            if result:
                ops_completed += 1
                log.info(
                    f"MDS thrash operation '{operation}' on fs={fs_name} completed successfully"
                )
            else:
                log.warning(
                    f"MDS thrash operation '{operation}' on fs={fs_name} did not complete"
                )

        except Exception as e:
            log.warning(f"MDS thrash iteration {iteration} failed: {e}")

        # Wait between operations to allow cluster stabilization
        time.sleep(10)

    log.info(
        f"MDS thrashing completed: {ops_completed} operations in {iteration} iterations "
        f"across {len(fs_names)} filesystem(s)"
    )
    return ops_completed


def _get_mds_info(rados_obj: RadosOrchestrator, fs_name: str) -> Dict:
    """Get MDS active and standby info for a filesystem."""
    try:
        fs_status = rados_obj.run_ceph_command(cmd=f"ceph fs status {fs_name}")
        active_mds = None
        standby_mds = []

        # Parse MDS info from mdsmap - contains both active and standby entries
        for mds in fs_status.get("mdsmap", []):
            mds_name = mds.get("name")
            mds_state = mds.get("state")
            if mds_state == "active":
                active_mds = mds_name
            elif mds_state == "standby":
                standby_mds.append(mds_name)

        return {"active": active_mds, "standbys": standby_mds}
    except Exception as e:
        log.error(f"Failed to get MDS info for {fs_name}: {e}")
        return {"active": None, "standbys": []}


def _thrash_mds_failover(rados_obj: RadosOrchestrator, fs_name: str) -> bool:
    """Fail the active MDS to trigger failover to a standby MDS."""
    try:
        mds_info = _get_mds_info(rados_obj, fs_name)
        active_mds = mds_info["active"]
        standby_mds = mds_info["standbys"]

        if not active_mds:
            log.warning(f"No active MDS found for {fs_name}")
            return False

        if not standby_mds:
            log.warning(f"No standby MDS available for failover on {fs_name}")
            return False

        log.info(f"Current active MDS for {fs_name}: {active_mds}")
        log.info(f"Standby MDSs available: {standby_mds}")
        log.info(f"Failing active MDS: {active_mds}")

        rados_obj.run_ceph_command(cmd=f"ceph mds fail {active_mds}")
        time.sleep(15)

        # Verify failover occurred
        new_mds_info = _get_mds_info(rados_obj, fs_name)
        new_active = new_mds_info["active"]

        if new_active and new_active != active_mds:
            log.info(f"MDS failover successful. New active MDS: {new_active}")
            return True
        else:
            log.warning(f"MDS failover may not have occurred. Active MDS: {new_active}")
            return False

    except Exception as e:
        log.error(f"MDS failover failed: {e}")
        return False


def _thrash_mds_rolling_restart(rados_obj: RadosOrchestrator, fs_name: str) -> bool:
    """Rolling restart of MDS daemons using orchestrator."""
    try:
        log.info(f"Initiating MDS rolling restart for fs={fs_name} via orchestrator")

        if not rados_obj.restart_daemon_services(daemon="mds"):
            log.warning("MDS orch restart command returned failure")
            return False

        log.info("MDS rolling restart initiated successfully")
        time.sleep(30)  # Allow time for restart to complete
        return True

    except Exception as e:
        log.error(f"MDS rolling restart failed: {e}")
        return False


def _thrash_mds_random_fail(rados_obj: RadosOrchestrator, fs_name: str) -> bool:
    """Randomly fail any MDS daemon (active or standby) to test recovery."""
    try:
        mds_info = _get_mds_info(rados_obj, fs_name)
        active_mds = mds_info["active"]
        standby_mds = mds_info["standbys"]

        all_mds = []
        if active_mds:
            all_mds.append(active_mds)
        all_mds.extend(standby_mds)

        if len(all_mds) < 2:
            log.warning(f"Need at least 2 MDSs for random fail thrashing on {fs_name}")
            return False

        # Randomly select an MDS to fail
        target_mds = random.choice(all_mds)
        mds_type = "active" if target_mds == active_mds else "standby"
        log.info(f"Randomly selected {mds_type} MDS to fail: {target_mds}")

        rados_obj.run_ceph_command(cmd=f"ceph mds fail {target_mds}")
        time.sleep(15)

        # Verify cluster still has an active MDS
        new_mds_info = _get_mds_info(rados_obj, fs_name)
        new_active = new_mds_info["active"]

        if new_active:
            log.info(f"MDS fail successful. Current active MDS: {new_active}")
            return True
        else:
            log.warning(f"No active MDS for {fs_name} after fail operation")
            return False

    except Exception as e:
        log.error(f"MDS random fail failed: {e}")
        return False


def cleanup_rgw_s3(
    client_node, rgw_config: Dict[str, Any], rados_obj: RadosOrchestrator = None
) -> None:
    """
    Cleanup RGW S3 resources after thrashing.

    Handles both normal and versioned buckets. For versioned buckets,
    deletes all object versions before removing the bucket.
    Also handles cleanup of EC pool and restoration of original zone config.

    Args:
        client_node: Client node to execute cleanup
        rgw_config: Dict with bucket_name, versioned_bucket, uid, primary_endpoint,
                    ec_pool_name, ec_profile_name, original_zone_config
        rados_obj: Optional RadosOrchestrator for ceph commands (needed for EC cleanup)
    """
    import json as json_module

    if not rgw_config:
        return

    bucket = rgw_config.get("bucket_name")
    versioned_bucket = rgw_config.get("versioned_bucket")
    uid = rgw_config.get("uid")
    endpoint = rgw_config.get("primary_endpoint", "")
    ec_pool_name = rgw_config.get("ec_pool_name")
    original_zone_config = rgw_config.get("original_zone_config")

    try:
        # Cleanup buckets (both normal and versioned) using force delete
        for bkt in [bucket, versioned_bucket]:
            if not bkt:
                continue
            log.info(f"Cleaning up RGW bucket: {bkt}")
            # Force delete with s3cmd (handles objects and versions)
            client_node.exec_command(
                cmd=f"s3cmd rb --recursive --force s3://{bkt}",
                sudo=True,
                check_ec=False,
                timeout=300,
            )
            # Fallback: awscli force delete if s3cmd fails
            if endpoint:
                client_node.exec_command(
                    cmd=f"aws --endpoint {endpoint} s3 rb s3://{bkt} --force",
                    sudo=True,
                    check_ec=False,
                    timeout=300,
                )

        # Remove user (will also purge any remaining data)
        if uid:
            log.info(f"Removing RGW user: {uid}")
            client_node.exec_command(
                cmd=f"radosgw-admin user rm --uid={uid} --purge-data",
                sudo=True,
                check_ec=False,
            )

        # Restore original zone configuration and cleanup EC pool if used
        if ec_pool_name:
            log.info("Cleaning up EC pool and restoring original zone configuration...")

            # Restore original zone configuration first
            if original_zone_config:
                log.info("Restoring original RGW zone configuration...")
                try:
                    zone_config_str = json_module.dumps(original_zone_config, indent=2)
                    zone_file = "/tmp/rgw_zone_restore.json"
                    client_node.exec_command(
                        cmd=f"echo '{zone_config_str}' > {zone_file}", sudo=True
                    )
                    client_node.exec_command(
                        cmd=f"radosgw-admin zone set --infile {zone_file}", sudo=True
                    )
                    client_node.exec_command(
                        cmd=f"rm -f {zone_file}", sudo=True, check_ec=False
                    )
                    # Commit the period update
                    client_node.exec_command(
                        cmd="radosgw-admin period update --commit",
                        sudo=True,
                        check_ec=False,
                    )
                    log.info("Original zone configuration restored")

                    # Restart RGW daemons to apply restored configuration
                    if rados_obj:
                        rados_obj.restart_daemon_services(daemon="rgw", timeout=300)
                except Exception as zone_err:
                    log.warning(f"Failed to restore zone configuration: {zone_err}")

            # Delete EC pool using rados_obj if available, otherwise use client_node
            log.info(f"Deleting EC pool: {ec_pool_name}")
            if rados_obj:
                try:
                    rados_obj.delete_pool(pool=ec_pool_name)
                except Exception as pool_err:
                    log.warning(f"Failed to delete EC pool via rados_obj: {pool_err}")
                    # Fallback to direct command
                    client_node.exec_command(
                        cmd=f"ceph osd pool delete {ec_pool_name} {ec_pool_name} "
                        f"--yes-i-really-really-mean-it",
                        sudo=True,
                        check_ec=False,
                    )
            else:
                client_node.exec_command(
                    cmd=f"ceph osd pool delete {ec_pool_name} {ec_pool_name} "
                    f"--yes-i-really-really-mean-it",
                    sudo=True,
                    check_ec=False,
                )

            # Remove EC profile (profile name follows ecp_{pool_name} convention)
            profile_name = f"ecp_{ec_pool_name}"
            log.info(f"Removing EC profile: {profile_name}")
            client_node.exec_command(
                cmd=f"ceph osd erasure-code-profile rm {profile_name}",
                sudo=True,
                check_ec=False,
            )

            log.info("EC pool cleanup completed")

    except Exception as e:
        log.warning(f"RGW cleanup error: {e}")


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
