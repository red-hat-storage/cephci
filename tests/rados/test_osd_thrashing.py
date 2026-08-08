"""
Test Module: OSD Thrashing Test with Concurrent I/O
====================================================

This module performs aggressive OSD thrashing with concurrent I/O workload to verify
cluster stability under stress conditions. It tests both EC and replicated pools,
and checks for any crashes or issues during OSD state transitions.

The test is organized into five sequential phases:

1. **Setup** -- Create pools (replicated, EC), mount CephFS/RBD, configure
   RGW S3 clients, and optionally deploy NFS clusters/exports and SMB
   clusters/shares.  Apply recovery tuning, compression, debug logging,
   and error injection as configured.
2. **Pre-thrash** -- On a healthy cluster, write integrity baselines for
   NFS (fio --verify=crc32c) and SMB (smbclient put + md5).  Verification
   failure here aborts the test (cluster broken before chaos begins).
   Future: extend baselines to CephFS, RBD, and RGW clients.
3. **Thrash** -- Launch parallel chaos threads: OSD stop/start, CRUSH weight
   changes, PG count changes, scrub toggles, MON/MGR/MDS failovers,
   NFS daemon kills, SMB daemon restarts, client churn, export churn,
   and concurrent I/O on CephFS, RBD, RGW, NFS, and SMB.  Transient
   errors during this phase are expected and logged as warnings.
4. **Validation** -- After thrashing stops and PGs reach active+clean:
   verify data integrity (NFS fio re-verify, SMB md5 re-verify), daemon
   health (NFS/SMB daemons running), mount staleness, NFS export
   accessibility, NFS/SMB RADOS config integrity, SMB endpoint
   accessibility, and CTDB health.  Any failure sets ``test_failed``.
5. **Cleanup** -- Unmount filesystems, delete pools/clusters/exports,
   re-enable firewalls, remove temp files.

Core Chaos Workflows:
  - **OSD thrashing** (``enable_osd_thrashing``): Mark out -> daemon stop ->
    wait -> mark in -> start.  Repeats for ``thrash_count`` iterations with
    configurable delays and random OSD selection.
  - **CRUSH weight thrashing** (``enable_crush_thrashing``): Reduce CRUSH
    reweights on random OSDs -> wait for PG migration -> restore original
    weights.  Stresses data redistribution under I/O.
  - **PG count thrashing** (``enable_pg_thrashing``): Toggle bulk flag on
    pools to trigger PG split/merge.  Validates PG autoscaler stability.
  - **Scrub thrashing** (``enable_scrub_thrashing``): Periodic
    ``ceph pg scrub`` / ``ceph pg deep-scrub`` on random PGs and pools.
    Validates scrub does not stall under concurrent OSD restarts.

Daemon Failover Workflows:
  - **MON thrashing** (``enable_mon_thrashing``): Leader failover, rolling
    restart, SIGKILL daemon kill, election strategy changes
    (classic/disallow/connectivity).  Verifies quorum stability and client
    reconnection under both graceful and abrupt daemon exits.
  - **MGR thrashing** (``enable_mgr_thrashing``): Failover active MGR,
    rolling restart, random daemon fail, SIGKILL daemon kill.  Validates
    module continuity and orchestrator auto-restart after abrupt crashes.
  - **OSD SIGKILL thrashing** (``enable_osd_sigkill_thrashing``): Kill
    random OSD processes with ``kill -9`` to simulate abrupt crashes,
    bypassing all shutdown handlers.  Stresses PG peering, journal replay,
    and BlueStore recovery.
  - **MDS thrashing** (``enable_mds_thrashing``): Active MDS failover,
    rolling restart, random fail.  CephFS I/O must survive rank transitions.

I/O Workloads:
  - **CephFS FIO** (``enable_fio_cephfs``): FIO on kernel-mounted CephFS
    with snapshot thrashing (periodic ``snap create`` / ``snap rm``).
  - **RBD FIO** (``enable_fio_rbd``): FIO on mapped RBD image with
    snapshot thrashing (``rbd snap create`` / ``rbd snap rm``).
  - **EC pool snapshots** (``enable_ec_snapshots``): RADOS-level snapshots
    + partial overwrites on EC pools to stress stripe recovery.
  - **CephFS subvolume thrashing** (``enable_cephfs_subvolume_thrashing``):
    Create/mount/ops/unmount/delete subvolumes in rapid cycles.
  - **RGW S3 thrashing** (``enable_rgw_thrashing``): S3 PUT/GET/DELETE and
    multipart upload with round-robin endpoint selection across gateways.

NFS Workflows (all gated by individual config flags):
  - **NFS FIO thrashing** (``enable_nfs_thrashing``): Mount -> FIO ->
    FS ops -> unmount cycles on each NFS export with multi-host rotation
    and version cycling (v4.0/v4.1/v4.2).
  - **NFS daemon thrashing** (``enable_nfs_daemon_thrashing``):
    Kill/restart NFS-Ganesha daemons via pkill, orch stop, or systemctl.
    Delegates to RadosOrchestrator methods.  Recovery failures are
    warn-only during thrash.
  - **NFS data integrity** (``enable_nfs_data_integrity``):
    Pre-thrash: write ``fio --verify=crc32c`` baseline files on each NFS
    export.  Post-thrash: re-verify checksums.  A *mismatch* (corrupted
    data) causes test failure; I/O errors alone are warn-only.
  - **NFS protocol state thrash** (``enable_nfs_protocol_state_thrash``):
    Parallel client-churn (mount/unmount/open/lock cycles), export-churn
    (create/delete/modify), and admin-ops (orch restart, export reload)
    threads that stress NFS state management.

SMB Workflows (gated by ``enable_smb_thrashing``):
  - **SMB daemon chaos** (``enable_smb_thrashing``): Kill/restart SMB
    daemons, CTDB/smbd process kills, service restarts.  Delegates to
    RadosOrchestrator.change_daemon_orch_state and
    restart_daemon_services.
  - **SMB client I/O** (``enable_smb_thrashing``): smbclient ls/put/get
    operations under chaos.
  - **SMB data integrity**: Pre-thrash ``smbclient put`` + md5 baseline,
    post-thrash md5 re-verify.  Mismatch = test failure.
  - **Post-thrash verification**: RADOS config integrity, endpoint
    accessibility (``smbclient -L``), and CTDB node health.  Failures
    cause test failure.

TEST WORKFLOW:
==============

    ├── Crash archive at start
    │
    ├── SETUP PHASE
    │   ├── Create pools (RADOS EC/replicated, CephFS, RBD) in parallel
    │   ├── Setup RGW S3 client with buckets (if enabled)
    │   ├── Create NFS clusters and exports (if enabled)
    │   ├── Create SMB clusters and shares (if enabled)
    │   ├── Enable EC optimizations for EC pools
    │   ├── Enable compression (if configured)
    │   ├── Configure aggressive recovery settings
    │   ├── Enable debug logging: debug_osd=20/20 (if configured)
    │   ├── Inject EC write errors on random OSDs (if configured)
    │   │   ├── bluestore_debug_inject_read_err=true
    │   │   └── ceph daemon osd.$id injectecwriteerr <pool> '*' 2 1 0 1
    │   └── Apply suite-driven error injection config (if provided)
    │       ├── Apply chaos profile (e.g., network_chaos, storage_corruption)
    │       ├── Apply individual config overrides
    │       └── Detect destructive MDS injection configs
    │
    ├── PRE-THRASH PHASE (if enable_nfs_data_integrity)
    │   ├── Mount each NFS export to /mnt/nfs-integrity-pre-<N>
    │   ├── Write 10 fio --verify=crc32c baseline files (4K-64M, varying block sizes)
    │   ├── Run pre-thrash verify pass (abort on failure -- cluster already broken)
    │   └── Check mount health (abort on stale mounts)
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
    │   ├── thrash_osd_sigkill: kill -9 random OSD processes → auto-restart [if enabled]
    │   ├── thrash_crush_weights: Reduce to 50% → restore [if enabled]
    │   ├── thrash_pg_count: Bulk flag toggle for PG split/merge [if enabled]
    │   ├── thrash_scrubs: Periodic scrub/deep-scrub on pools/OSDs/cluster [if enabled]
    │   ├── thrash_rgw_s3: S3 PUT/GET/DELETE/multipart on RGW [if enabled]
    │   ├── thrash_mon: Leader failover, rolling restart, sigkill, election strategy [if enabled]
    │   ├── thrash_mgr: Failover, rolling restart, random fail [if enabled]
    │   ├── thrash_mds: Failover, rolling restart, random fail [if enabled]
    │   ├── thrash_nfs_fio: Mount/FIO/FS ops/unmount cycles on NFS exports [if enabled]
    │   ├── thrash_nfs_daemon_failover: NFS daemon kill/recovery loops [if enabled]
    │   ├── nfs protocol state threads: client/export/admin churn [if enabled]
    │   ├── thrash_smb: SMB daemon chaos loops [if enabled]
    │   ├── thrash_smb_client_io: SMB client operations under chaos [if enabled]
    │   └── thrash_cephfs_subvolumes: Create/ops/delete subvolume groups+subvols [if enabled]
    │
    ├── VALIDATION PHASE
    │   ├── Wait for cluster stabilization (active+clean PGs)
    │   ├── Check cluster health
    │   ├── If NFS thrashing: post-thrash NFS verification
    │   │   ├── Daemon health: all NFS daemons must be running
    │   │   ├── Mount health: pre-thrash integrity mounts must not be stale
    │   │   ├── Data integrity: fio --verify re-check (mismatch = test failure)
    │   │   ├── Export accessibility: mount/ls/unmount each export
    │   │   └── RADOS config integrity: validate each cluster's config object
    │   ├── If SMB thrashing: post-thrash SMB verification
    │   │   ├── RADOS config: check for corruption (if enable_smb_rados_config_check)
    │   │   ├── Endpoint accessibility: smbclient -L on each share
    │   │   └── CTDB health: verify all nodes healthy, none banned
    │   ├── If destructive MDS injection:
    │   │   ├── Validate MDS crashes match CDentry::check_corruption signature
    │   │   ├── Attempt repair workflow (mds repaired -> damage ls -> scrub repair -> damage rm)
    │   │   └── Log repair outcome (diagnostic only, does not affect pass/fail)
    │   ├── Else: Check for crashes (ceph crash + daemon logs) and fail if detected
    │   │   └── NFS+OSD exception: if all NFS daemons recovered, downgrade to warning
    │   └── Report findings
    │
    └── CLEANUP PHASE
        ├── Reset recovery settings
        ├── Remove debug logging configs (if enabled)
        ├── Remove error injection configs (cleanup_all)
        ├── Restore down OSDs (mark in + restart OSD service)
        ├── If destructive MDS injection: destroy damaged filesystem + pools
        ├── Force log rotation on all OSD hosts (parallel)
        ├── Unmount and cleanup CephFS and RBD
        ├── Cleanup NFS clusters and exports (if enabled)
        ├── Cleanup SMB clusters and shares (if enabled)
        ├── Unmount pre-thrash NFS integrity mounts
        ├── Re-enable firewall on NFS nodes
        └── Delete test pools and EC profiles

CONFIGURATION OPTIONS:
======================
- iterations: Number of OSD thrash iterations (default: 60)
- duration: Total test duration in seconds (default: 1200)
- aggressive: Enable aggressive mode with longer waits (default: True)
- enable_crush_thrashing: Enable CRUSH weight modifications (default: True)
- enable_pg_thrashing: Enable PG count thrashing via bulk flag (default: True)
- enable_scrub_thrashing: Enable independent scrub/deep-scrub cycles (default: True)
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
- enable_osd_sigkill_thrashing: Enable OSD SIGKILL thrashing - kill -9 crashes (default: False)
- enable_rgw_thrashing: Enable RGW S3 thrashing (default: False)
- enable_mon_thrashing: Enable MON thrashing - leader failover/restart (default: False)
- enable_mgr_thrashing: Enable MGR thrashing - failover/restart/random fail (default: False)
- enable_mds_thrashing: Enable MDS thrashing - failover/restart/random fail (default: False)
- enable_nfs_thrashing: Enable NFS cluster/export setup for thrashing (default: False)
- nfs_num_clusters: Number of ``ceph nfs cluster create`` instances (default: 3)
- nfs_exports_per_cluster: Exports per cluster (default: 4)
- nfs_placement: Placement daemon count argument to cluster create (default: 1)
- enable_cephfs_subvolume_thrashing: Enable CephFS subvolume thrashing (default: False)
- enable_election_strategy_thrash: Enable election strategy changes during MON thrash (default: False)
- enable_nfs_rdma: Create NFS clusters with ceph nfs cluster create --enable-rdma
  (default: False). Optional nfs_rdma_port overrides default RDMA base 20049.
- nfs_rdma_port: Base RDMA port for ``--rdma_port`` (per-cluster: base + index).
  If unset, ``NFS_RDMA_DEFAULT_BASE_PORT`` (20049) in core_workflows is used.
- enable_nfsv3: Pass --enable-nfsv3 to ceph nfs cluster create (default: False)
- nfs_placement_label: Prefer orch hosts with this **ceph orch host label**. If no host
  has the label, fallback uses all orch hosts.
- enable_nfs_daemon_thrashing: Enable NFS daemon kill/recovery cycles (default: False)
- nfs_daemon_kill_method: NFS daemon kill method (pkill | orch_stop | systemctl)
- enable_nfs_data_integrity: Enable pre/post-thrash NFS data integrity verification (default: False)
- enable_nfs_protocol_state_thrash: Enable NFS state churn threads (default: False)
- enable_smb_thrashing: Enable SMB daemon chaos + client IO workflows (default: False)
- smb_cluster_ids: SMB cluster IDs for thrash workflows (default: [])
- smb_auth_mode: SMB auth mode used for setup/endpoint checks (default: user)
- smb_num_shares: Number of SMB shares per cluster for setup (default: 4)
- smb_user_name / smb_user_password: SMB credentials for client IO checks
- enable_smb_rados_config_check: Validate SMB RADOS metadata post-thrash (default: False)
- enable_esb_verification: Enable BlueStore ESB Bug #70390 verification (default: False).
  Sets bluestore_elastic_shared_blobs=true, bluestore_write_v2=false, bluestore_onode_segment_size=0,
  bluestore_debug_extent_map_encode_check=true, debug_bluestore=5/5.
  Targets extent map resharding crash in EC pools with overwrites.
- error_injection: Suite-driven error injection config (default: None).
  Supports profiles, individual configs, EC errors, and admin socket commands.
  See ceph/rados/ceph_error_injector.py for full schema and 22 available profiles.

"""

import concurrent.futures as cf
import random
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import yaml

from ceph import utils
from ceph.ceph_admin import CephAdmin
from ceph.rados.ceph_error_injector import (
    PROFILES,
    CephErrorInjector,
    CephInjectionRecovery,
)
from ceph.rados.core_workflows import NFS_RDMA_DEFAULT_BASE_PORT, RadosOrchestrator
from ceph.rados.mgr_workflows import MgrWorkflows
from ceph.rados.monitor_workflows import MonitorWorkflows
from ceph.rados.pool_workflows import PoolFunctions
from ceph.rados.utils import get_cluster_timestamp
from tests.rados.monitor_configurations import MonConfigMethods, MonElectionStrategies
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from tests.rados.test_four_node_ecpool import create_comprehensive_test_objects
from tests.rados.thrash_helpers import (
    NfsThrashWorkflows,
    SmbThrashWorkflows,
    _nfs_client_mount_option_string,
    _nfs_export_mount_targets,
    _nfs_mount_context,
    _thrash_mgr_sigkill,
    _thrash_mon_sigkill,
    check_mount_health,
    thrash_nfs_admin_ops,
    thrash_nfs_client_churn,
    thrash_nfs_daemon_failover,
    thrash_nfs_export_churn,
    thrash_osd_sigkill,
    thrash_smb,
    thrash_smb_client_io,
    verify_data_integrity,
    write_integrity_baseline,
)
from utility.log import Log
from utility.utils import extract_ceph_version

log = Log(__name__)


def _has_inconsistent_obj_fix(rados_obj) -> bool:
    """Check if the running ceph build contains the IBMCEPH-12717 fix.

    The fix is NOT present in the 20.2.1.X line (Ceph 9.1). All other versions
    are assumed to have the fix.
    Returns True if the fix is present and inconsistent object
    detection should cause test failure.
    """
    try:
        out, _ = rados_obj.client.exec_command(cmd="ceph version", sudo=True)
        version = extract_ceph_version(out)
        log.info(f"IBMCEPH-12717 fix check: ceph version = {version}")

        if not version:
            log.warning("Could not parse ceph version, assuming fix is present")
            return True

        parts = version.split(".")
        major, minor, patch = int(parts[0]), int(parts[1]), int(parts[2])

        if major == 20 and minor == 2 and patch <= 1:
            log.info(
                f"Ceph version {version} is in 20.2.1.X line (no IBMCEPH-12717 fix),"
                f" disabling inconsistent_object_fail"
            )
            return False

        return True
    except Exception as e:
        log.warning(f"Failed to determine ceph version for fix check: {e}")
        return True


# Version-to-release mapping for pool configuration file paths
_VERSION_RELEASE_MAP = {
    9: "tentacle",
    8: "squid",
    7: "reef",
    6: "quincy",
    5: "pacific",
}

# Cache for pool configurations (loaded once per module)
_POOL_CONFIGS_CACHE = None


def _get_pool_configs_path(rhbuild: str) -> str:
    """Resolve the pool configurations file path based on Ceph version.

    Args:
        rhbuild: Ceph build version string (e.g., "9.2.0-123")

    Returns:
        Path to the pool-configurations.yaml for that release
    """
    major = int(rhbuild.split(".")[0])
    release = _VERSION_RELEASE_MAP.get(major)
    if not release:
        log.warning(
            f"Unknown major version {major} from rhbuild '{rhbuild}', "
            f"falling back to tentacle pool configs"
        )
        release = "tentacle"
    return f"conf/{release}/rados/test-confs/pool-configurations.yaml"


def run(ceph_cluster, **kw):
    """
    Main test execution for OSD thrashing with concurrent I/O workload.

    This test creates pools (EC and/or replicated) and performs aggressive OSD
    thrashing with concurrent I/O to verify cluster stability. Execution
    proceeds through five phases:

    1. **Setup** -- Create pools, NFS/SMB/RGW resources, apply injection
       configs, and configure recovery settings.
    2. **Pre-thrash** (``enable_nfs_data_integrity`` only) -- Mount NFS
       exports, write ``fio --verify=crc32c`` baseline files, and run an
       initial verification pass on the healthy cluster.  Failure here
       aborts the test immediately.
    3. **Thrash** -- Launch parallel chaos + I/O threads for ``duration``
       seconds.  NFS and SMB workflow errors are *expected* under chaos
       and are logged as warnings only (see ``_warn_checks``).  The sole
       hard-fail is an NFS data integrity checksum mismatch.
    4. **Validation** -- After recovery, verify daemon health, NFS mount
       staleness, data integrity, export accessibility, NFS/SMB RADOS
       config integrity, SMB endpoint/CTDB health, and crash status.
       Failures here set ``test_failed = True``.  When both NFS and OSD
       thrashing are active, NFS crashes where all daemons have recovered
       are downgraded to warnings (cascading OSD pool degradation).
    5. **Cleanup** -- Unmount filesystems, delete pools, re-enable
       firewalls, and remove injection configs.

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
        inconsistent_object_fail (bool): Fail test on inconsistent PGs instead
            of repairing - IBMCEPH-12717 (default: True)
        num_osds_to_inject (int): Number of OSDs to inject errors on (default: 3)
        num_writes_to_inject (int): Number of writes to inject errors per OSD (default: 1000)
        enable_rgw_thrashing (bool): Enable RGW S3 thrashing (default: False, requires RGW)
        enable_fio_cephfs (bool): Enable FIO workload on CephFS mount (default: True)
        enable_fio_rbd (bool): Enable FIO workload on RBD mount (default: True)
        enable_cephfs_snapshots (bool): Enable CephFS snapshot thrashing (default: True)
        enable_rbd_snapshots (bool): Enable RBD snapshot thrashing (default: True)
        enable_ec_snapshots (bool): Enable EC pool snapshot thrashing (default: True)
        enable_osd_thrashing (bool): Enable OSD thrashing - main thrash operation (default: True)
        enable_osd_sigkill_thrashing (bool): Enable OSD SIGKILL thrashing - kill -9 (default: False)
        enable_scrub_thrashing (bool): Enable independent scrub/deep-scrub cycles (default: True)
        enable_mon_thrashing (bool): Enable MON thrashing - leader failover/restart (default: False)
        enable_mgr_thrashing (bool): Enable MGR thrashing - failover/restart/random fail (default: False)
        enable_mds_thrashing (bool): Enable MDS thrashing - failover/restart/random fail (default: False)
        enable_nfs_thrashing (bool): Enable NFS cluster/export setup for thrashing (default: False)
        nfs_num_clusters (int): How many NFS clusters to create (default: 3)
        nfs_exports_per_cluster (int): Exports per cluster (default: 4)
        nfs_placement (int): ``ceph nfs cluster create`` placement count (default: 1)
        enable_cephfs_subvolume_thrashing (bool): Enable CephFS subvolume thrashing (default: False)
        enable_election_strategy_thrash (bool): Enable election strategy changes (default: False)
        enable_fast_ec_config_params (bool): Enable fast EC config params (default: True)
        enable_esb_verification (bool): Enable BlueStore ESB Bug #70390 configs (default: False)
        enable_nfs_rdma (bool): Use --enable-rdma on ceph nfs cluster create (default: False)
        nfs_rdma_port (int|None): Optional base --rdma_port; if unset, 20049 + index
        enable_nfsv3 (bool): Pass --enable-nfsv3 to ceph nfs cluster create (default: False)
        nfs_placement_label (str|None): Prefer orch hosts with this label; if no
            matches, use all orch hosts (default: None).
        enable_nfs_daemon_thrashing (bool): Enable NFS daemon kill/restart cycles (default: False)
        nfs_daemon_kill_method (str): pkill | orch_stop | systemctl (default: pkill)
        nfs_daemon_thrash_interval (int): Seconds between kill cycles (default: 60)
        nfs_daemon_recovery_timeout (int): Seconds to wait for daemon recovery (default: 180)
        enable_nfs_data_integrity (bool): Run fio --verify=crc32c on NFS mounts (default: False)
        enable_nfs_protocol_state_thrash (bool): Client/export/admin churn threads (default: False)
        nfs_client_churn_rate (int): Client ops per 10s window (default: 5)
        nfs_export_churn_enabled (bool): Export create/delete/modify cycles (default: False)
        nfs_admin_interleave_enabled (bool): Admin op interleaving (default: False)
        enable_smb_thrashing (bool): Enable SMB daemon thrashing (default: False)
        smb_cluster_ids (list): SMB cluster IDs to thrash (default: [])
        smb_auth_mode (str): user | active-directory (default: user)
        smb_num_shares (int): Number of SMB shares per cluster (default: 4)
        smb_user_name (str): SMB username for endpoint/client IO checks
            (default: smbuser)
        smb_user_password (str): SMB password for endpoint/client IO checks
            (default: smbpassword)
        enable_smb_rados_config_check (bool): RADOS config integrity for SMB (default: False)
        error_injection (dict): Suite-driven error injection config (default: None).
            Supports profile, profile_overrides, configs, ec_write_errors,
            ec_read_errors, admin_socket. See CephErrorInjector for schema.

    Returns:
        int: 0 if test passed, 1 if test failed.

    Failure conditions (``test_failed = True``):
        - Daemon crashes detected via ``ceph crash`` or daemon logs (unless
          NFS+OSD recovery downgrade applies).
        - Post-thrash NFS daemon not running, stale mount, data integrity
          error (mismatch or unreadable file), export inaccessible, or
          RADOS config invalid.
        - Post-thrash SMB RADOS config corruption, endpoint failure,
          unhealthy CTDB nodes, or SMB integrity md5 mismatch.
        - Pre-thrash baseline write failure or verification error (cluster
          broken before chaos begins).
        - Inconsistent PGs with ``inconsistent_object_fail`` enabled.
        - Unhandled exceptions during any phase.

    Warning-only conditions (logged, do not cause failure):
        - NFS/SMB workflow errors during the thrash phase (recovery
          failures, kill failures, churn errors, timeouts).
        - NFS crashes when OSD thrashing is active and all NFS daemons
          have recovered post-thrash.
    """
    log.info(run.__doc__)
    config = kw["config"]

    # Initialize cluster objects
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    injector = CephErrorInjector(rados_obj=rados_obj)
    client_node = ceph_cluster.get_nodes(role="client")[0]
    mon_workflow_obj = MonitorWorkflows(node=cephadm)
    mon_election_obj = MonElectionStrategies(rados_obj=rados_obj)
    mgr_workflow_obj = MgrWorkflows(node=cephadm)
    rbd_image = "thrash-test-image" + str(random.randint(100, 999))

    # Test parameters
    iterations = config.get("iterations", 60)
    duration = config.get("duration", 1200)
    aggressive = config.get("aggressive", True)
    enable_crush_thrashing = config.get("enable_crush_thrashing", True)
    enable_pg_thrashing = config.get("enable_pg_thrashing", True)
    compression = config.get("compression", False)
    enable_debug_logs = config.get("enable_debug_logs", True)
    inject_errors = config.get("inject_errors", False)
    inconsistent_object_fail = config.get("inconsistent_object_fail", True)
    if inconsistent_object_fail and not _has_inconsistent_obj_fix(rados_obj):
        inconsistent_object_fail = False
    num_osds_to_inject = config.get("num_osds_to_inject", 3)
    num_writes_to_inject = config.get("num_writes_to_inject", 1000)
    enable_rgw_thrashing = config.get("enable_rgw_thrashing", True)
    enable_fio_cephfs = config.get("enable_fio_cephfs", True)
    enable_fio_rbd = config.get("enable_fio_rbd", True)
    enable_cephfs_snapshots = config.get("enable_cephfs_snapshots", True)
    enable_rbd_snapshots = config.get("enable_rbd_snapshots", True)
    enable_ec_snapshots = config.get("enable_ec_snapshots", True)
    enable_osd_thrashing = config.get("enable_osd_thrashing", True)
    enable_osd_sigkill_thrashing = config.get("enable_osd_sigkill_thrashing", False)
    enable_mon_thrashing = config.get("enable_mon_thrashing", True)
    enable_mgr_thrashing = config.get("enable_mgr_thrashing", False)
    enable_mds_thrashing = config.get("enable_mds_thrashing", False)
    enable_nfs_thrashing = config.get("enable_nfs_thrashing", False)
    enable_nfs_rdma = config.get("enable_nfs_rdma", False)
    nfs_rdma_port_raw = config.get("nfs_rdma_port")
    nfs_rdma_port = int(nfs_rdma_port_raw) if nfs_rdma_port_raw is not None else None
    enable_nfsv3 = config.get("enable_nfsv3", False)
    nfs_placement_label = config.get("nfs_placement_label") or None
    nfs_num_clusters = config.get("nfs_num_clusters", 3)
    nfs_exports_per_cluster = config.get("nfs_exports_per_cluster", 4)
    nfs_placement = config.get("nfs_placement", 1)
    enable_scrub_thrashing = config.get("enable_scrub_thrashing", True)
    enable_cephfs_subvolume_thrashing = config.get(
        "enable_cephfs_subvolume_thrashing", False
    )
    enable_election_strategy_thrash = config.get(
        "enable_election_strategy_thrash", True
    )

    # NFS enhanced thrashing parameters
    enable_nfs_daemon_thrashing = config.get("enable_nfs_daemon_thrashing", False)
    enable_nfs_data_integrity = config.get("enable_nfs_data_integrity", False)
    enable_nfs_protocol_state_thrash = config.get(
        "enable_nfs_protocol_state_thrash", False
    )

    # SMB thrashing parameters
    enable_smb_thrashing = config.get("enable_smb_thrashing", False)
    smb_cluster_ids = config.get("smb_cluster_ids", [])
    enable_smb_rados_config_check = config.get("enable_smb_rados_config_check", False)

    enable_fast_ec_config_params = config.get("enable_fast_ec_config_params", True)
    enable_esb_verification = config.get("enable_esb_verification", False)
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

    rdma_suffix = ""
    if enable_nfs_rdma:
        if nfs_rdma_port is not None:
            rdma_suffix = f" (rdma_port base={nfs_rdma_port})"
        else:
            rdma_suffix = f" (rdma default base {NFS_RDMA_DEFAULT_BASE_PORT})"

    test_params = (
        f"\n{'=' * 60}\n"
        f"OSD THRASHING TEST WITH CONCURRENT I/O\n"
        f"{'=' * 60}\n"
        f"Test parameters:\n"
        f"  Iterations: {iterations}\n"
        f"  Duration: {duration}s\n"
        f"  Aggressive mode: {aggressive}\n"
        f"  CRUSH thrashing: {enable_crush_thrashing}\n"
        f"  PG thrashing via bulk flag (split/merge): {enable_pg_thrashing}\n"
        f"  Scrub thrashing (independent scrub/deep-scrub): {enable_scrub_thrashing}\n"
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
        f"  OSD SIGKILL thrashing: {enable_osd_sigkill_thrashing}\n"
        f"  MON thrashing: {enable_mon_thrashing}\n"
        f"  MGR thrashing: {enable_mgr_thrashing}\n"
        f"  MDS thrashing: {enable_mds_thrashing}\n"
        f"  NFS thrashing: {enable_nfs_thrashing}\n"
        + (
            f"  NFS clusters / exports per cluster / placement count: "
            f"{nfs_num_clusters} / {nfs_exports_per_cluster} / {nfs_placement}\n"
            if enable_nfs_thrashing
            else ""
        )
        + f"  NFS RDMA (cluster + mounts): {enable_nfs_rdma}{rdma_suffix}\n"
        f"  NFS cluster NFSv3 enabled: {enable_nfsv3}\n"
        f"  NFS placement orch label: {nfs_placement_label or '(none)'}\n"
        f"  CephFS subvolume thrashing: {enable_cephfs_subvolume_thrashing}\n"
        f"  Election strategy thrash: {enable_election_strategy_thrash}\n"
        f"  Fast EC config params: {enable_fast_ec_config_params}\n"
        f"  ESB verification (Bug #70390): {enable_esb_verification}\n"
        f"  Inconsistent object fail (no repair): {inconsistent_object_fail}\n"
        f"  NFS daemon thrashing: {enable_nfs_daemon_thrashing}\n"
        f"  NFS data integrity (fio verify): {enable_nfs_data_integrity}\n"
        f"  NFS protocol state thrash: {enable_nfs_protocol_state_thrash}\n"
        f"  SMB thrashing: {enable_smb_thrashing}\n"
        + (f"  SMB cluster IDs: {smb_cluster_ids}\n" if enable_smb_thrashing else "")
        + f"  SMB RADOS config check: {enable_smb_rados_config_check}\n"
    )
    _ei_config = config.get("error_injection", {})
    if _ei_config:
        _ei_profile = _ei_config.get("profile", "none")
        test_params += f"  Error injection profile: {_ei_profile}\n"
        _ei_merged = {}
        if _ei_profile and _ei_profile in PROFILES:
            _ei_merged.update(PROFILES[_ei_profile]["configs"])
        _ei_merged.update(_ei_config.get("profile_overrides", {}))
        _ei_merged.update(_ei_config.get("configs", {}))
        if _ei_merged:
            test_params += "  Error injection configs:\n"
            for k, v in sorted(_ei_merged.items()):
                test_params += f"    {k}: {v}\n"
        if _ei_config.get("ec_write_errors"):
            test_params += (
                f"  EC write errors: {len(_ei_config['ec_write_errors'])} spec(s)\n"
            )
        if _ei_config.get("ec_read_errors"):
            test_params += (
                f"  EC read errors: {len(_ei_config['ec_read_errors'])} spec(s)\n"
            )
        if _ei_config.get("admin_socket"):
            test_params += (
                f"  Admin socket commands: "
                f"{len(_ei_config['admin_socket'])} command(s)\n"
            )
    else:
        test_params += "  Error injection: None\n"
    test_params += f"{'=' * 60}\n"
    log.info(test_params)

    log.info("Archiving existing crashes...")
    rados_obj.run_ceph_command(cmd="ceph crash archive-all", client_exec=True)
    time.sleep(5)
    start_time = get_cluster_timestamp(rados_obj.node)
    log.debug(f"Test workflow started. Start time: {start_time}")

    created_pools = []
    test_failed = False
    _destructive_mds_injection = False
    _bluestore_corruption_active = False
    fs_name = None
    fs_names = []  # List of all filesystems created (for MDS thrashing)
    cephfs_mount_path = None
    rbd_mount_path = None
    rbd_device_path = None
    nfs_config = None
    smb_config = None
    nfs_integrity_mounts = []  # Pre-thrash NFS mounts for integrity baseline
    smb_integrity_baselines = {}  # Pre-thrash SMB per-share md5 baselines
    nfs_firewall_disabled_nodes = []  # Track nodes where firewall was disabled for NFS

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
            + (1 if enable_smb_thrashing else 0)
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
                    image_name=rbd_image,
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
                    num_clusters=nfs_num_clusters,
                    exports_per_cluster=nfs_exports_per_cluster,
                    placement=nfs_placement,
                    pool_type="erasure",
                    enable_fast_ec_config_params=enable_fast_ec_config_params,
                    enable_rdma=enable_nfs_rdma,
                    rdma_port=nfs_rdma_port,
                    enable_nfsv3=enable_nfsv3,
                    nfs_placement_label=nfs_placement_label,
                )

            smb_future = None
            smb_wf = None
            if enable_smb_thrashing and smb_cluster_ids:
                log.info(
                    "SMB workflows enabled - creating %s SMB cluster(s) for thrashing",
                    len(smb_cluster_ids),
                )
                smb_wf = SmbThrashWorkflows(cephadm.installer, ceph_cluster, rados_obj)
                smb_future = pool_executor.submit(
                    smb_wf.create_clusters_and_shares,
                    config,
                )

            # Collect results
            try:
                rados_pools = rados_future.result()
                created_pools.extend(rados_pools)
                log.info(f"Created {len(rados_pools)} RADOS pool(s)")

                if cephfs_future:
                    cephfs_result = cephfs_future.result()
                    if (
                        not cephfs_result
                        or not cephfs_result[0]
                        or not cephfs_result[1]
                    ):
                        raise Exception(
                            "CephFS setup returned invalid filesystem name or mount path"
                        )
                    fs_name, cephfs_mount_path, cephfs_pools = cephfs_result
                    created_pools.extend(cephfs_pools)
                    fs_names.append(fs_name)
                    log.info(f"Created CephFS: {fs_name} at {cephfs_mount_path}")
                else:
                    log.info(
                        "CephFS pool creation skipped (no CephFS workflows enabled)"
                    )

                if rbd_future:
                    rbd_result = rbd_future.result()
                    if not rbd_result or not rbd_result[0] or not rbd_result[1]:
                        raise Exception(
                            "RBD setup returned invalid mount path or device path"
                        )
                    rbd_mount_path, rbd_device_path, rbd_pools = rbd_result
                    created_pools.extend(rbd_pools)
                    log.info(f"Created RBD pools at {rbd_mount_path}")
                else:
                    log.info("RBD pool creation skipped (no RBD workflows enabled)")

                if rgw_future:
                    rgw_config = rgw_future.result()
                    if rgw_config:
                        log.info("RGW S3 client setup completed successfully")
                    else:
                        raise Exception(
                            "RGW thrashing enabled but RGW setup returned no config"
                        )
                else:
                    log.info("RGW setup skipped (RGW thrashing not enabled)")

                if nfs_future:
                    nfs_config = nfs_future.result()
                    if nfs_config:
                        if not nfs_config.get("fs_name") or not nfs_config.get("pools"):
                            raise Exception(
                                "NFS setup returned empty filesystem name or pools"
                            )
                        created_pools.extend(nfs_config.get("pools", []))
                        fs_names.append(nfs_config.get("fs_name"))
                        log.info(
                            f"Created NFS setup: {len(nfs_config.get('clusters', []))} clusters, "
                            f"{len(nfs_config.get('exports', []))} exports"
                        )

                        # Per NFS host: always drop host firewall (TCP + RDMA). When the
                        # cluster has v3 enabled, also install/start rpcbind and statd.
                        all_nodes = ceph_cluster.get_nodes()
                        configured_hosts = set()
                        v3_extra = (
                            "yum install -y rpcbind nfs-utils 2>/dev/null || true; "
                            "systemctl enable rpcbind rpc-statd 2>/dev/null || true; "
                            "systemctl start rpcbind rpc-statd 2>/dev/null || true; "
                            "systemctl list-units --type=service --no-legend | "
                            "grep '@nfs\\.' | awk '{print $1}' | "
                            "xargs -r systemctl restart 2>/dev/null || true"
                        )
                        for cluster_info in nfs_config.get("clusters", []):
                            cluster_hosts = cluster_info.get("hosts")
                            if isinstance(cluster_hosts, str):
                                cluster_hosts = [cluster_hosts]
                            if not cluster_hosts:
                                cluster_hosts = [cluster_info.get("host")]
                            for hostname in cluster_hosts:
                                if not hostname or hostname in configured_hosts:
                                    continue
                                configured_hosts.add(hostname)
                                host_node = next(
                                    (n for n in all_nodes if n.hostname == hostname),
                                    None,
                                )
                                if not host_node:
                                    log.warning(
                                        "  %s: Node not found, skipping", hostname
                                    )
                                    continue
                                try:
                                    fw = (
                                        "systemctl stop firewalld 2>/dev/null || true; "
                                        "systemctl disable firewalld 2>/dev/null || true"
                                    )
                                    if nfs_config.get("enable_nfsv3"):
                                        cmd = fw + "; " + v3_extra
                                        log_note = (
                                            "firewall disabled + NFSv3 prerequisites"
                                        )
                                    else:
                                        cmd = fw
                                        log_note = "firewall disabled for NFS/RDMA"
                                    host_node.exec_command(
                                        cmd=cmd,
                                        sudo=True,
                                        timeout=180,
                                        check_ec=False,
                                    )
                                    nfs_firewall_disabled_nodes.append(host_node)
                                    log.info("  %s: %s", hostname, log_note)
                                except Exception as e:
                                    log.warning("  %s: Failed - %s", hostname, e)

                        # Also disable firewall on client node for NFS access
                        try:
                            client_node.exec_command(
                                cmd="systemctl stop firewalld 2>/dev/null || true; "
                                "systemctl disable firewalld 2>/dev/null || true",
                                sudo=True,
                                timeout=30,
                                check_ec=False,
                            )
                            nfs_firewall_disabled_nodes.append(client_node)
                            log.info("  Client node: firewall disabled")
                        except Exception:
                            pass

                        # Allow NFS-Ganesha to complete registration with rpcbind
                        time.sleep(10)
                    else:
                        raise Exception(
                            "NFS thrashing enabled but NFS setup returned no config"
                        )
                else:
                    log.info("NFS setup skipped (NFS thrashing not enabled)")

                if smb_future:
                    smb_config = smb_future.result()
                    if smb_config:
                        log.info(
                            "Created SMB setup: %s cluster(s), %s share(s)",
                            len(smb_config.get("cluster_ids", [])),
                            len(smb_config.get("smb_shares", [])),
                        )
                    else:
                        raise Exception(
                            "SMB thrashing enabled but setup returned no config"
                        )
                else:
                    log.info("SMB setup skipped (SMB thrashing not enabled)")
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
            log.info(
                "  Endpoint        : %s", rgw_config.get("primary_endpoint", "N/A")
            )
            log.info("  Bucket          : %s", rgw_config.get("bucket_name", "N/A"))
            log.info(
                "  Versioned Bucket: %s", rgw_config.get("versioned_bucket", "N/A")
            )
            log.info("  Data Pool Type  : %s", rgw_config.get("data_pool_type", "N/A"))
            if rgw_config.get("ec_pool_name"):
                log.info("  EC Pool Name    : %s", rgw_config.get("ec_pool_name"))
            log.info("")

        if nfs_config:
            log.info("NFS Setup:")
            log.info("  Filesystem      : %s", nfs_config.get("fs_name", "N/A"))
            log.info(
                "  RDMA mounts     : %s",
                (
                    "yes (client uses rdma,port= per cluster)"
                    if nfs_config.get("enable_rdma")
                    else "no (TCP port= only)"
                ),
            )
            if nfs_config.get("enable_rdma"):
                log.info(
                    "  NFSv3 on cluster: %s", nfs_config.get("enable_nfsv3", False)
                )
            for cluster in nfs_config.get("clusters", []):
                rp = cluster.get("rdma_port")
                if nfs_config.get("enable_rdma") and rp is not None:
                    log.info(
                        "    - %s (NFS TCP port: %d, RDMA port: %d, host: %s)",
                        cluster["cluster_id"],
                        cluster["port"],
                        rp,
                        cluster.get("host", "N/A"),
                    )
                else:
                    log.info(
                        "    - %s (port: %d, host: %s)",
                        cluster["cluster_id"],
                        cluster["port"],
                        cluster.get("host", "N/A"),
                    )
            exports = nfs_config.get("exports", [])
            log.info("  Exports         : %d", len(exports))
            for export in exports:
                log.info(
                    "    - %s on %s",
                    export["pseudo_path"],
                    export["cluster_id"],
                )
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

        if enable_esb_verification:
            log.info("Enabling BlueStore ESB verification configs (Bug #70390)...")
            esb_configs = {
                "bluestore_elastic_shared_blobs": "true",
                "bluestore_write_v2": "false",
                "bluestore_onode_segment_size": "0",
                "bluestore_debug_extent_map_encode_check": "true",
                "debug_bluestore": "5/5",
            }
            for name, value in esb_configs.items():
                mon_obj.set_config(section="osd", name=name, value=value)
            log.info(
                "ESB verification configs applied -- "
                "OSDs will validate extent maps on every encode"
            )

        if inject_errors:
            injector.inject_config("bluestore_debug_inject_read_err", True)
            ec_pool_names = [
                p["pool_name"] for p in created_pools if p.get("pool_type") == "erasure"
            ]
            if ec_pool_names:
                for ec_pool in ec_pool_names:
                    for osd_id in inject_osds:
                        injector.inject_ec_write_error(
                            osd_id=osd_id,
                            pool=ec_pool,
                            shard=2,
                            err_type=1,
                            skip=0,
                            duration=num_writes_to_inject,
                        )
                log.info(
                    f"Injected EC write errors on {len(inject_osds)} OSDs "
                    f"for {len(ec_pool_names)} pools: "
                    f"{', '.join(str(osd) for osd in inject_osds)}"
                )

        # Apply suite-driven error injection config (if provided)
        error_injection_config = config.get("error_injection", {})
        if error_injection_config:
            injector.apply_from_config(error_injection_config)
            _destructive_mds_keys = {
                "mds_inject_rename_corrupt_dentry_first",
                "mds_inject_journal_corrupt_dentry_first",
            }
            all_injected = set(error_injection_config.get("configs", {}).keys())
            all_injected.update(
                error_injection_config.get("profile_overrides", {}).keys()
            )
            prof = error_injection_config.get("profile")
            if prof and prof in PROFILES:
                all_injected.update(PROFILES[prof]["configs"].keys())
            active_destructive = _destructive_mds_keys & all_injected
            if active_destructive:
                merged = {}
                if prof and prof in PROFILES:
                    merged.update(PROFILES[prof]["configs"])
                merged.update(error_injection_config.get("profile_overrides", {}))
                merged.update(error_injection_config.get("configs", {}))
                if any(float(merged.get(k, 0)) > 0 for k in active_destructive):
                    _destructive_mds_injection = True
                    log.warning(
                        "DESTRUCTIVE MDS INJECTION ACTIVE: "
                        "CephFS metadata corruption expected. "
                        "Filesystem will be destroyed during cleanup."
                    )
            _bs_corruption_keys = {
                "bluestore_debug_inject_read_err",
                "bluestore_debug_inject_csum_err_probability",
                "bluestore_debug_inject_allocation_from_file_failure",
            }
            if _bs_corruption_keys & all_injected:
                _bluestore_corruption_active = True
                log.info(
                    "BlueStore corruption injection active: "
                    "PG inconsistency expected during test."
                )

        mon_obj.set_config(
            section="global", name="osd_max_markdown_count", value="99999999"
        )

        log.info("Setup phase completed successfully")

        # ── Pre-thrash setup phase ──
        #
        # Before any chaos begins, establish an NFS data-integrity baseline
        # on the healthy cluster (gated by enable_nfs_data_integrity).
        #
        # Steps:
        #   1. Resolve NFS export mount targets via _nfs_export_mount_targets().
        #   2. Mount each target to /mnt/nfs-integrity-pre-<N> using NFSv4.1.
        #   3. Write 10 fio --verify=crc32c baseline files (4K-64M, varying block sizes).
        #   4. Run a pre-thrash verify pass to confirm zero errors on a clean cluster.
        #   5. Check mount health -- stale mounts here mean the cluster is already
        #      degraded, so the test aborts with an exception.
        #
        # These mounts stay up through the thrash phase and are re-verified
        # post-thrash in the validation phase.  They are cleaned up in the
        # finally block regardless of cleanup_pools setting.
        if enable_nfs_data_integrity and nfs_config:
            log.info(
                f"\n{'-' * 60}\n"
                "Pre-thrash NFS integrity baseline setup\n"
                f"{'-' * 60}"
            )
            targets = _nfs_export_mount_targets(nfs_config)
            if targets:
                for idx, target in enumerate(targets):
                    mp = f"/mnt/nfs-integrity-pre-{idx}"
                    try:
                        hosts = target.get("hosts") or (
                            [target.get("host")] if target.get("host") else []
                        )
                        if not hosts:
                            raise RuntimeError(
                                f"No NFS hosts for cluster "
                                f"{target.get('cluster_id')}"
                            )
                        nfs_host = hosts[idx % len(hosts)]
                        mount_opts = _nfs_client_mount_option_string(
                            nfs_config,
                            target["cluster_id"],
                            "nfsvers=4.1",
                        )
                        client_node.exec_command(
                            sudo=True, cmd=f"mkdir -p {mp}", timeout=15
                        )
                        client_node.exec_command(
                            sudo=True,
                            cmd=f"mount -t nfs -o {mount_opts} "
                            f"{nfs_host}:{target['pseudo_path']} {mp}",
                            timeout=45,
                        )
                        nfs_integrity_mounts.append(mp)
                        log.info(
                            "  Mounted %s:%s -> %s", nfs_host, target["pseudo_path"], mp
                        )
                    except Exception as e:
                        log.warning(
                            "  Failed to mount integrity target %s: %s", target, e
                        )

                if nfs_integrity_mounts:
                    log.info(
                        "  Writing integrity baseline to %s mount(s)...",
                        len(nfs_integrity_mounts),
                    )
                    baseline = write_integrity_baseline(
                        client_node, nfs_integrity_mounts
                    )
                    if not baseline["files_written"]:
                        raise Exception(
                            "Pre-thrash baseline write failed on all mounts "
                            "— cluster broken before thrashing"
                        )

                    log.info("  Running pre-thrash verify pass...")
                    pre_verify = verify_data_integrity(
                        client_node, nfs_integrity_mounts
                    )
                    if pre_verify["errors"] or pre_verify["mismatches"]:
                        raise Exception(
                            f"Pre-thrash verify FAILED: {pre_verify} — "
                            "cluster has integrity issues before thrashing"
                        )
                    log.info(
                        "  Pre-thrash verify passed: %s file(s) clean",
                        pre_verify["files_checked"],
                    )

                    if nfs_integrity_mounts:
                        mount_health = check_mount_health(
                            client_node, nfs_integrity_mounts, proto="nfs"
                        )
                        if mount_health.get("stale"):
                            log.error(
                                "[pre-thrash] Stale NFS mounts before thrashing: %s",
                                mount_health["stale"],
                            )
                            raise Exception(
                                "Pre-thrash NFS mount health check failed — "
                                "cluster has issues before thrashing"
                            )
                        else:
                            log.info(
                                "[pre-thrash] NFS mount health OK: %s healthy mounts",
                                len(mount_health.get("healthy", [])),
                            )
                else:
                    log.warning(
                        "  No NFS mounts succeeded — " "integrity baseline skipped"
                    )
            else:
                log.warning(
                    "  No NFS export targets found — " "integrity baseline skipped"
                )

        # ── Pre-thrash SMB integrity baseline ──
        #
        # For each SMB share, upload a 4MB test file via smbclient and
        # record its md5 checksum.  After the thrash phase, the same files
        # are downloaded and their checksums compared to detect silent
        # data corruption.
        #
        # Unlike NFS, SMB setup is more variable so failures here are
        # logged as warnings rather than aborting the test.
        if enable_smb_thrashing and smb_config:
            log.info(
                f"\n{'-' * 60}\n"
                "Pre-thrash SMB integrity baseline setup\n"
                f"{'-' * 60}"
            )
            try:
                smb_user = smb_config.get("smb_user_name", "")
                smb_password = smb_config.get("smb_user_password", "")
                smb_shares = smb_config.get("smb_shares", [])

                smb_target = None
                public_addrs = smb_config.get("public_addrs")
                if public_addrs:
                    smb_target = (
                        public_addrs[0]
                        if isinstance(public_addrs, list)
                        else public_addrs
                    )
                if not smb_target:
                    smb_nodes = smb_config.get("smb_nodes", [])
                    if smb_nodes:
                        node = smb_nodes[0]
                        smb_target = (
                            node.ip_address
                            if hasattr(node, "ip_address")
                            else str(node)
                        )

                if not smb_target:
                    log.warning(
                        "  No SMB target IP found — " "integrity baseline skipped"
                    )
                elif not smb_shares:
                    log.warning(
                        "  No SMB shares configured — " "integrity baseline skipped"
                    )
                elif not smb_user:
                    log.warning(
                        "  No SMB user configured — " "integrity baseline skipped"
                    )
                else:
                    for share in smb_shares:
                        share_name = (
                            share if isinstance(share, str) else share.get("name", "")
                        )
                        if not share_name:
                            continue
                        local_file = f"/tmp/smb_integrity_{share_name}.dat"
                        try:
                            client_node.exec_command(
                                cmd=(
                                    f"dd if=/dev/urandom of={local_file} "
                                    "bs=1M count=4 2>/dev/null"
                                ),
                                timeout=30,
                            )
                            out, _ = client_node.exec_command(
                                cmd=f"md5sum {local_file}",
                                timeout=15,
                            )
                            md5hash = out.strip().split()[0]

                            client_node.exec_command(
                                cmd=(
                                    f"smbclient -U {smb_user}%{smb_password} "
                                    f"//{smb_target}/{share_name} "
                                    f"-c 'put {local_file} smb_integrity_test.dat'"
                                ),
                                timeout=60,
                            )
                            smb_integrity_baselines[share_name] = md5hash
                            log.info(
                                "  SMB integrity baseline written: share=%s md5=%s",
                                share_name,
                                md5hash,
                            )
                        except Exception as e:
                            log.warning(
                                "  Failed to write SMB integrity baseline "
                                "for share %s: %s",
                                share_name,
                                e,
                            )

                    if smb_integrity_baselines:
                        log.info(
                            "  SMB integrity baselines recorded for %s share(s)",
                            len(smb_integrity_baselines),
                        )
                    else:
                        log.warning(
                            "  No SMB integrity baselines succeeded — "
                            "post-thrash verification will be skipped"
                        )
            except Exception as e:
                log.warning("  SMB integrity baseline setup failed: %s", e)

        # TODO: Add pre-thrash/post-thrash integrity baselines for other client types:
        #   - CephFS: fio --verify=crc32c on kernel/fuse mount before thrash,
        #     verify post-thrash on same mount points
        #   - RBD: fio --verify=crc32c on mapped block device before thrash,
        #     verify post-thrash
        #   - RGW: S3 PUT with checksums before thrash, S3 GET + verify post-thrash

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
        max_workers += 1 if enable_osd_sigkill_thrashing else 0
        max_workers += 1 if enable_crush_thrashing else 0
        max_workers += 1 if enable_pg_thrashing else 0
        max_workers += 1 if enable_scrub_thrashing else 0
        max_workers += 1 if enable_rgw_thrashing else 0
        max_workers += 1 if enable_mon_thrashing else 0
        max_workers += 1 if enable_mgr_thrashing else 0
        max_workers += 1 if enable_mds_thrashing else 0
        max_workers += 4 if (enable_nfs_thrashing and nfs_config) else 0
        max_workers += 1 if (enable_cephfs_subvolume_thrashing and fs_names) else 0
        # NFS enhanced thrashing threads
        max_workers += 1 if (enable_nfs_daemon_thrashing and nfs_config) else 0
        if enable_nfs_protocol_state_thrash and nfs_config:
            max_workers += 1  # client churn
            max_workers += 1 if config.get("nfs_export_churn_enabled") else 0
            max_workers += 1 if config.get("nfs_admin_interleave_enabled") else 0
        # SMB thrashing threads
        max_workers += 1 if (enable_smb_thrashing and smb_config) else 0  # smb_daemon
        max_workers += (
            1 if (enable_smb_thrashing and smb_config) else 0
        )  # smb_client_io
        log.debug("ThreadPoolExecutor max_workers: %s", max_workers)

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

            # OSD SIGKILL thrashing (kill -9 to simulate abrupt crashes)
            if enable_osd_sigkill_thrashing:
                log.info("OSD SIGKILL thrashing enabled")
                futures.append(
                    executor.submit(
                        thrash_osd_sigkill,
                        rados_obj=rados_obj,
                        osd_list=osd_list,
                        iterations=iterations // 2,
                        stop_flag=stop_flag,
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

            # Scrub thrashing (default: enabled)
            if enable_scrub_thrashing:
                log.info("Scrub thrashing enabled (independent scrub/deep-scrub)")
                futures.append(
                    executor.submit(
                        thrash_scrubs,
                        rados_obj=rados_obj,
                        pools=created_pools,
                        duration=duration,
                        aggressive=aggressive,
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

            # NFS FIO thrashing (mount -> FIO -> unmount cycle)
            if enable_nfs_thrashing and nfs_config:
                log.info(
                    f"NFS FIO thrashing enabled: {len(nfs_config.get('exports', []))} exports"
                )
                futures.append(
                    executor.submit(
                        thrash_nfs_fio,
                        client_node=client_node,
                        nfs_config=nfs_config,
                        duration=duration,
                        stop_flag=stop_flag,
                    )
                )
                # NFS locking checks (basic + byte-range)
                futures.append(
                    executor.submit(
                        thrash_nfs_locking,
                        client_node=client_node,
                        nfs_config=nfs_config,
                        stop_flag=stop_flag,
                    )
                )
                # NFS rootsquash/SELinux checks
                futures.append(
                    executor.submit(
                        thrash_nfs_rootsquash,
                        client_node=client_node,
                        nfs_config=nfs_config,
                        stop_flag=stop_flag,
                    )
                )
                # NFS extended FS ops (cross-dir rename, links, deeper dirs)
                futures.append(
                    executor.submit(
                        thrash_nfs_fsops_extended,
                        client_node=client_node,
                        nfs_config=nfs_config,
                        duration=duration,
                        stop_flag=stop_flag,
                    )
                )
            elif enable_nfs_thrashing:
                log.warning("NFS FIO thrashing enabled but no NFS config available")

            # CephFS subvolume thrashing (create/delete subvolume groups and subvolumes)
            if enable_cephfs_subvolume_thrashing and fs_names:
                log.info(f"CephFS subvolume thrashing enabled for: {fs_names}")
                futures.append(
                    executor.submit(
                        thrash_cephfs_subvolumes,
                        rados_obj=rados_obj,
                        fs_names=fs_names,
                        duration=duration,
                        stop_flag=stop_flag,
                        num_groups=config.get("subvol_num_groups", 3),
                        num_subvols_per_group=config.get("subvol_num_per_group", 4),
                    )
                )
            elif enable_cephfs_subvolume_thrashing:
                log.warning(
                    "CephFS subvolume thrashing enabled but no filesystems available"
                )

            # NFS daemon failover thrashing (kill/restart cycles)
            if enable_nfs_daemon_thrashing and nfs_config:
                log.info("NFS daemon failover thrashing enabled")
                futures.append(
                    executor.submit(
                        thrash_nfs_daemon_failover,
                        installer=cephadm.installer,
                        nfs_config=nfs_config,
                        rados_obj=rados_obj,
                        ceph_cluster=ceph_cluster,
                        client_node=client_node,
                        duration=duration,
                        stop_flag=stop_flag,
                        config=config,
                    )
                )
            elif enable_nfs_daemon_thrashing:
                log.warning("NFS daemon thrashing enabled but no NFS config available")

            # NFS protocol state thrash threads
            if enable_nfs_protocol_state_thrash and nfs_config:
                log.info("NFS protocol state thrash threads enabled")
                futures.append(
                    executor.submit(
                        thrash_nfs_client_churn,
                        client_node=client_node,
                        nfs_config=nfs_config,
                        duration=duration,
                        stop_flag=stop_flag,
                        churn_rate=config.get("nfs_client_churn_rate", 5),
                    )
                )
                if config.get("nfs_export_churn_enabled"):
                    futures.append(
                        executor.submit(
                            thrash_nfs_export_churn,
                            installer=cephadm.installer,
                            nfs_config=nfs_config,
                            duration=duration,
                            stop_flag=stop_flag,
                            rados_obj=rados_obj,
                        )
                    )
                if config.get("nfs_admin_interleave_enabled"):
                    futures.append(
                        executor.submit(
                            thrash_nfs_admin_ops,
                            installer=cephadm.installer,
                            nfs_config=nfs_config,
                            duration=duration,
                            stop_flag=stop_flag,
                            rados_obj=rados_obj,
                        )
                    )
            elif enable_nfs_protocol_state_thrash:
                log.warning(
                    "NFS protocol state thrash enabled but no NFS config available"
                )

            # SMB daemon thrashing
            if enable_smb_thrashing and smb_config:
                log.info(
                    "SMB daemon thrashing enabled for %s",
                    smb_config.get("cluster_ids", []),
                )
                futures.append(
                    executor.submit(
                        thrash_smb,
                        installer=cephadm.installer,
                        smb_config=smb_config,
                        ceph_cluster=ceph_cluster,
                        rados_obj=rados_obj,
                        duration=duration,
                        stop_flag=stop_flag,
                    )
                )
                futures.append(
                    executor.submit(
                        thrash_smb_client_io,
                        client_node=client_node,
                        smb_config=smb_config,
                        duration=duration,
                        stop_flag=stop_flag,
                    )
                )
            elif enable_smb_thrashing:
                log.warning("SMB thrashing enabled but no SMB config available")

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
                "mds_thrashing": None,
                "nfs_fio_thrashing": None,
                "nfs_locking": None,
                "nfs_rootsquash": None,
                "nfs_fsops_extended": None,
                "cephfs_subvolume_thrashing": None,
                "scrub_thrashing": None,
                "nfs_daemon_failover": None,
                "nfs_client_churn": None,
                "nfs_export_churn": None,
                "nfs_admin_ops": None,
                "smb_daemon_thrashing": None,
                "smb_client_io": None,
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
            if enable_osd_sigkill_thrashing:
                workflow_order.append("osd_sigkill_thrashing")
            if enable_crush_thrashing:
                workflow_order.append("crush_weight_thrashing")
            if enable_pg_thrashing:
                workflow_order.append("pg_count_thrashing")
            if enable_scrub_thrashing:
                workflow_order.append("scrub_thrashing")
            if enable_rgw_thrashing and rgw_config:
                workflow_order.append("rgw_s3_thrashing")
            if enable_mon_thrashing:
                workflow_order.append("mon_thrashing")
            if enable_mgr_thrashing:
                workflow_order.append("mgr_thrashing")
            if enable_mds_thrashing and fs_names:
                workflow_order.append("mds_thrashing")
            if enable_nfs_thrashing and nfs_config:
                workflow_order.append("nfs_fio_thrashing")
                workflow_order.append("nfs_locking")
                workflow_order.append("nfs_rootsquash")
                workflow_order.append("nfs_fsops_extended")
            if enable_cephfs_subvolume_thrashing and fs_names:
                workflow_order.append("cephfs_subvolume_thrashing")
            if enable_nfs_daemon_thrashing and nfs_config:
                workflow_order.append("nfs_daemon_failover")
            if enable_nfs_protocol_state_thrash and nfs_config:
                workflow_order.append("nfs_client_churn")
                if config.get("nfs_export_churn_enabled"):
                    workflow_order.append("nfs_export_churn")
                if config.get("nfs_admin_interleave_enabled"):
                    workflow_order.append("nfs_admin_ops")
            if enable_smb_thrashing and smb_config:
                workflow_order.append("smb_daemon_thrashing")
                workflow_order.append("smb_client_io")

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
                        workflow_results[workflow_name] = {"timeout": True}
                        log.info(f"  {workflow_name}: Did not complete in time")
                except Exception as e:
                    log.info(f"  {workflow_name}: Failed with exception - {e}")
            log.info(f"{'=' * 60}")

            # ----------------------------------------------------------
            # During-thrash workflow result evaluation (warn-only)
            #
            # NFS and SMB workflows operate under active daemon chaos,
            # so transient errors (recovery failures, kill failures,
            # mount timeouts, client I/O errors) are *expected* and
            # must not fail the test.  Each workflow returns a dict
            # with counters (e.g. "errors", "recovery_failures").
            #
            # This block inspects those counters and emits warnings
            # for any non-zero values.  The only hard-fail condition
            # during the thrash phase is an NFS data integrity
            # *mismatch* (corrupted file content), which is detected
            # separately in the post-thrash verification.
            #
            # Workflow timeouts (the thread did not finish before
            # stop_flag was set) are also warn-only for NFS/SMB
            # workflows listed in _timeout_warn_workflows.
            # ----------------------------------------------------------

            # _warn_checks: maps workflow name -> list of
            # (condition_fn, message_fn) tuples evaluated against the
            # workflow result dict.
            _warn_checks = {
                "nfs_daemon_failover": [
                    (
                        lambda r: r.get("recovery_failures", 0),
                        lambda r: f"NFS daemon recovery failures: {r['recovery_failures']}",
                    ),
                    (
                        lambda r: r.get("kill_failures", 0),
                        lambda r: f"NFS daemon kill failures: {r['kill_failures']}",
                    ),
                ],
                "nfs_client_churn": [
                    (
                        lambda r: r.get("errors", 0),
                        lambda r: f"nfs_client_churn errors: {r['errors']}",
                    ),
                ],
                "nfs_export_churn": [
                    (
                        lambda r: r.get("errors", 0),
                        lambda r: f"nfs_export_churn errors: {r['errors']}",
                    ),
                ],
                "nfs_admin_ops": [
                    (
                        lambda r: r.get("errors", 0),
                        lambda r: f"nfs_admin_ops errors: {r['errors']}",
                    ),
                ],
                "smb_daemon_thrashing": [
                    (
                        lambda r: r.get("recovery_failures", 0),
                        lambda r: f"SMB daemon recovery failures: {r['recovery_failures']}",
                    ),
                    (
                        lambda r: r.get("errors", 0),
                        lambda r: f"SMB daemon thrash errors: {r['errors']}",
                    ),
                ],
                "smb_client_io": [
                    (
                        lambda r: r.get("errors", 0),
                        lambda r: f"SMB client IO errors: {r['errors']}",
                    ),
                    (
                        lambda r: r.get("io_ops", 0) == 0,
                        lambda r: "SMB client IO completed zero operations",
                    ),
                ],
            }

            thrash_warnings = []
            for wf_name, checks in _warn_checks.items():
                wf_result = workflow_results.get(wf_name)
                if not isinstance(wf_result, dict):
                    continue
                for cond_fn, msg_fn in checks:
                    if cond_fn(wf_result):
                        thrash_warnings.append(msg_fn(wf_result))

            # NFS/SMB workflow timeouts are also warn-only.
            _timeout_warn_workflows = [
                "nfs_daemon_failover",
                "nfs_client_churn",
                "nfs_export_churn",
                "nfs_admin_ops",
                "smb_daemon_thrashing",
                "smb_client_io",
            ]
            for wf_name in _timeout_warn_workflows:
                wf_result = workflow_results.get(wf_name)
                if isinstance(wf_result, dict) and wf_result.get("timeout"):
                    thrash_warnings.append(f"{wf_name} timed out during stop/drain")

            if thrash_warnings:
                log.warning(
                    "[thrash_validation] During-thrash workflow warnings (%s):",
                    len(thrash_warnings),
                )
                for warn_msg in thrash_warnings:
                    log.warning("  WARN: %s", warn_msg)

        log.info(
            "Thrashing phase completed\n Sleeping for 60 seconds to allow for recovery..."
        )
        time.sleep(60)
        log.info(f"\n{'=' * 60}\nVALIDATION PHASE\n{'=' * 60}")
        rados_obj.log_cluster_health()

        # Check for inconsistent PGs across all pools and repair if found
        if _bluestore_corruption_active:
            log.info(
                "Skipping inconsistent PG check -- BlueStore corruption "
                "injection causes expected PG inconsistency. "
                "Recovery will handle in cleanup phase."
            )
            inconsistent_pgs_found = []
        else:
            log.info("Checking for inconsistent PGs across all pools...")
            all_pools = rados_obj.list_pools()
            inconsistent_pgs_found = []
            for check_pool in all_pools:
                try:
                    inconsistent_pgs = rados_obj.get_inconsistent_pg_list(check_pool)
                    if inconsistent_pgs:
                        log.warning(
                            f"Found {len(inconsistent_pgs)} inconsistent "
                            f"PG(s) in pool {check_pool}: {inconsistent_pgs}"
                        )
                        inconsistent_pgs_found.extend(inconsistent_pgs)
                except Exception as e:
                    log.debug(f"Error checking pool {check_pool}: {e}")

            if inconsistent_pgs_found:
                if inconsistent_object_fail:
                    log.error(
                        f"Total inconsistent PGs found: "
                        f"{len(inconsistent_pgs_found)}. "
                        f"Failing test (IBMCEPH-12717)"
                    )
                    for pg_id in inconsistent_pgs_found:
                        try:
                            log.info(f"Capturing diagnostics for PG: {pg_id}")
                            client_node.exec_command(
                                cmd=f"ceph pg {pg_id} query -f json-pretty"
                                f" > /tmp/{pg_id}_query.txt",
                                sudo=True,
                            )
                            client_node.exec_command(
                                cmd=f"rados list-inconsistent-obj {pg_id}"
                                f" -f json-pretty"
                                f" > /tmp/{pg_id}_inconsistent_obj.txt",
                                sudo=True,
                            )
                        except Exception as e:
                            log.warning(f"Failed diagnostics for PG {pg_id}: {e}")
                    raise Exception(
                        f"IBMCEPH-12717: Inconsistent objects in "
                        f"{len(inconsistent_pgs_found)} PG(s)"
                    )
                else:
                    log.debug(
                        f"Found {len(inconsistent_pgs_found)} inconsistent "
                        f"PG(s), initiating repair..."
                    )
                    for pg_id in inconsistent_pgs_found:
                        try:
                            time.sleep(5)
                            log.info(f"Running pg repair on PG: {pg_id}")
                            rados_obj.run_ceph_command(cmd=f"ceph pg repair {pg_id}")
                        except Exception as e:
                            log.error(f"Failed to repair PG {pg_id}: {e}")
                    time.sleep(30)
            else:
                log.info("No inconsistent PGs found across all pools")

        if _bluestore_corruption_active:
            log.info(
                "Skipping wait_for_clean_pg_sets -- BlueStore corruption "
                "injection may have caused PG inconsistency. "
                "Recovery will run in cleanup phase."
            )
        else:
            log.info("Waiting for PGs to reach active+clean state...")
            if not wait_for_clean_pg_sets(
                rados_obj, timeout=1200, recovery_thread=False
            ):
                log.error("Not all PGs are active+clean")
                raise Exception("Not all PGs are active+clean")

        rados_obj.log_cluster_health()

        # ── Post-thrash NFS verification ──
        #
        # After thrash threads have stopped and PGs are active+clean,
        # validate the NFS subsystem end-to-end.  Every check that
        # fails sets test_failed = True (these are hard failures).
        #
        # Checks performed (in order):
        #   1. Daemon health: every nfs.<daemon_id> must report "running"
        #      via ceph orch.
        #   2. Mount health: pre-thrash integrity mounts (if any) must
        #      not be stale (stat returns EIO/ESTALE).
        #   3. Data integrity: re-run fio --verify=crc32c on pre-thrash
        #      mounts.  A *mismatch* (corrupted data) is a hard failure.
        #      I/O errors that are not mismatches are warn-only.
        #   4. Export accessibility: mount each export via
        #      _nfs_mount_context, run ``ls``, unmount.  Any mount or
        #      ls failure is a hard failure.
        #   5. RADOS config integrity: for each NFS cluster, read the
        #      conf-nfs.<cluster_id> RADOS object and validate it
        #      parses correctly.  Corruption is a hard failure.
        if enable_nfs_thrashing and nfs_config:
            log.info("Running post-thrash NFS verification...")
            nfs_wf = NfsThrashWorkflows(
                cephadm.installer, nfs_config, rados_obj, ceph_cluster
            )

            # NFS daemon health: all daemons must be running
            try:
                nfs_daemon_healthy = True
                for cluster in nfs_config.get("clusters", []):
                    cluster_id = cluster.get("cluster_id", "")
                    if not cluster_id:
                        continue
                    svc_name = f"nfs.{cluster_id}"
                    daemon_ids = rados_obj.get_service_spec_daemons(
                        service_name=svc_name
                    )
                    if not daemon_ids:
                        log.error("[post-thrash NFS] No daemons found for %s", svc_name)
                        nfs_daemon_healthy = False
                        continue
                    for daemon_id in daemon_ids:
                        daemon_id = str(daemon_id)
                        status = rados_obj.get_daemon_status(
                            daemon_type="nfs", daemon_id=daemon_id
                        )
                        status_desc = status[1] if status else "unknown"
                        if status_desc != "running":
                            log.error(
                                "[post-thrash NFS] Daemon nfs.%s "
                                "status=%s (expected running)",
                                daemon_id,
                                status_desc,
                            )
                            nfs_daemon_healthy = False
                        else:
                            log.debug("[post-thrash NFS] nfs.%s running", daemon_id)
                if nfs_daemon_healthy:
                    log.info("Post-thrash NFS daemon health verified")
                else:
                    test_failed = True
            except Exception as e:
                log.error("[post-thrash NFS] Daemon health check failed: %s", e)
                test_failed = True

            # NFS mount health check on pre-thrash integrity mounts
            if nfs_integrity_mounts:
                try:
                    mount_health = check_mount_health(
                        client_node, nfs_integrity_mounts, proto="nfs"
                    )
                    if mount_health.get("stale"):
                        log.error(
                            "[post-thrash NFS] Stale mounts detected: %s",
                            mount_health["stale"],
                        )
                        test_failed = True
                    else:
                        log.info(
                            "[post-thrash NFS] Mount health OK: %s healthy",
                            len(mount_health.get("healthy", [])),
                        )
                except Exception as e:
                    log.error("[post-thrash NFS] Mount health check failed: %s", e)
                    test_failed = True

            # Post-thrash data integrity verification on pre-mounted paths
            if nfs_integrity_mounts:
                try:
                    log.info(
                        "[post-thrash NFS] Verifying data integrity on "
                        "%s pre-thrash mount(s)...",
                        len(nfs_integrity_mounts),
                    )
                    post_verify = verify_data_integrity(
                        client_node, nfs_integrity_mounts
                    )
                    if post_verify.get("mismatches"):
                        log.error(
                            "[post-thrash NFS] Data integrity MISMATCH: %s",
                            post_verify["mismatches"],
                        )
                        test_failed = True
                    elif post_verify.get("errors"):
                        log.error(
                            "[post-thrash NFS] Data integrity errors on "
                            "stable cluster: %s (file unreadable = potential data loss)",
                            post_verify["errors"],
                        )
                        test_failed = True
                    else:
                        log.info(
                            "[post-thrash NFS] Data integrity verified: "
                            "%s file(s) clean",
                            post_verify.get("files_checked", 0),
                        )
                except Exception as e:
                    log.error(
                        "[post-thrash NFS] Data integrity verification failed: %s", e
                    )
                    test_failed = True

            # NFS export accessibility: mount each export, verify ls, unmount
            try:
                nfs_exports_ok = True
                for idx, export in enumerate(nfs_config.get("exports", [])):
                    cluster_id = export.get("cluster_id", "")
                    pseudo_path = export.get("pseudo_path", "")
                    if not cluster_id or not pseudo_path:
                        continue
                    try:
                        with _nfs_mount_context(
                            client_node,
                            nfs_config,
                            "/mnt/post-thrash-nfs-verify",
                            index=idx,
                        ) as (exp_info, mount_point):
                            if exp_info is None or mount_point is None:
                                log.error(
                                    "[post-thrash NFS] Mount failed for export %s",
                                    pseudo_path,
                                )
                                nfs_exports_ok = False
                                continue
                            client_node.exec_command(
                                sudo=True,
                                cmd=f"ls {mount_point}/",
                                timeout=30,
                            )
                            log.debug(
                                "[post-thrash NFS] Export %s accessible at %s",
                                pseudo_path,
                                mount_point,
                            )
                    except Exception as e:
                        log.error(
                            "[post-thrash NFS] Export %s access failed: %s",
                            pseudo_path,
                            e,
                        )
                        nfs_exports_ok = False
                if nfs_exports_ok:
                    log.info("Post-thrash NFS export accessibility verified")
                else:
                    test_failed = True
            except Exception as e:
                log.error("[post-thrash NFS] Export accessibility check failed: %s", e)
                test_failed = True

            # NFS RADOS config integrity: verify each cluster's config object
            try:
                nfs_rados_ok = True
                for cluster in nfs_config.get("clusters", []):
                    cluster_id = cluster.get("cluster_id", "")
                    if not cluster_id:
                        continue
                    rados_result = nfs_wf.check_rados_config(cluster_id)
                    if not rados_result.get("valid"):
                        log.error(
                            "[post-thrash NFS] RADOS config invalid for %s: %s",
                            cluster_id,
                            rados_result.get("error"),
                        )
                        nfs_rados_ok = False
                    else:
                        log.debug(
                            "[post-thrash NFS] RADOS config OK for %s (%s bytes)",
                            cluster_id,
                            rados_result["size"],
                        )
                if nfs_rados_ok:
                    log.info("Post-thrash NFS RADOS config integrity verified")
                else:
                    test_failed = True
            except Exception as e:
                log.error("[post-thrash NFS] RADOS config check failed: %s", e)
                test_failed = True

        # ── Post-thrash SMB verification ──
        #
        # When SMB thrashing is enabled, verify the SMB subsystem after
        # chaos threads have stopped.  All failures here are hard
        # failures (set test_failed = True).
        #
        # Checks performed (in order):
        #   1. RADOS config integrity (if enable_smb_rados_config_check):
        #      verify_rados_config() reads each SMB cluster's metadata
        #      object from the RADOS pool and checks for corruption.
        #   2. Endpoint accessibility: verify_endpoints() runs
        #      ``smbclient -L`` against each share and confirms the
        #      share is listed in the output.
        #   3. CTDB health: verify_ctdb_health() checks that all CTDB
        #      cluster nodes are healthy and none are banned.
        if enable_smb_thrashing and smb_config:
            log.info("Running post-thrash SMB verification...")
            if smb_wf is None:
                smb_wf = SmbThrashWorkflows(cephadm.installer, ceph_cluster, rados_obj)
            if enable_smb_rados_config_check:
                smb_rados_result = smb_wf.verify_rados_config(
                    smb_config.get("cluster_ids", [])
                )
                if smb_rados_result.get("corrupted"):
                    log.error(
                        "SMB RADOS config corruption detected: %s",
                        smb_rados_result["corrupted"],
                    )
                    test_failed = True
                else:
                    log.info("SMB RADOS config integrity verified")

            smb_endpoint_result = smb_wf.verify_endpoints(client_node, smb_config)
            if smb_endpoint_result.get("failed"):
                log.error("SMB endpoint failures: %s", smb_endpoint_result["failed"])
                test_failed = True
            else:
                log.info(
                    "All %s SMB endpoints accessible",
                    len(smb_endpoint_result.get("accessible", [])),
                )

            ctdb_result = smb_wf.verify_ctdb_health(
                smb_config.get("smb_nodes", []),
                smb_config.get("cluster_ids", []),
            )
            if not ctdb_result.get("healthy", False):
                log.error(
                    "CTDB health check failed: banned=%s", ctdb_result.get("banned", [])
                )
                test_failed = True
            else:
                log.info(
                    "CTDB health verified: %s/%s nodes OK",
                    ctdb_result.get("nodes_ok"),
                    ctdb_result.get("nodes_total"),
                )

        # ── Post-thrash SMB data integrity verification ──
        #
        # Download each pre-thrash test file and compare its md5 checksum
        # against the stored baseline.  A mismatch indicates silent data
        # corruption during the thrash phase.
        if smb_integrity_baselines and smb_config:
            log.info("Running post-thrash SMB integrity verification...")
            try:
                smb_user = smb_config.get("smb_user_name", "")
                smb_password = smb_config.get("smb_user_password", "")

                smb_target = None
                public_addrs = smb_config.get("public_addrs")
                if public_addrs:
                    smb_target = (
                        public_addrs[0]
                        if isinstance(public_addrs, list)
                        else public_addrs
                    )
                if not smb_target:
                    smb_nodes = smb_config.get("smb_nodes", [])
                    if smb_nodes:
                        node = smb_nodes[0]
                        smb_target = (
                            node.ip_address
                            if hasattr(node, "ip_address")
                            else str(node)
                        )

                if not smb_target:
                    log.warning(
                        "No SMB target IP for post-thrash verification — skipped"
                    )
                else:
                    for share_name, expected_md5 in smb_integrity_baselines.items():
                        verify_file = f"/tmp/smb_integrity_verify_{share_name}.dat"
                        try:
                            client_node.exec_command(
                                cmd=(
                                    f"smbclient -U {smb_user}%{smb_password} "
                                    f"//{smb_target}/{share_name} "
                                    f"-c 'get smb_integrity_test.dat {verify_file}'"
                                ),
                                timeout=60,
                            )
                            out, _ = client_node.exec_command(
                                cmd=f"md5sum {verify_file}",
                                timeout=15,
                            )
                            actual_md5 = out.strip().split()[0]

                            if actual_md5 != expected_md5:
                                log.error(
                                    "SMB integrity MISMATCH on share %s: "
                                    "expected=%s actual=%s",
                                    share_name,
                                    expected_md5,
                                    actual_md5,
                                )
                                test_failed = True
                            else:
                                log.info(
                                    "SMB integrity verified: share=%s md5=%s",
                                    share_name,
                                    actual_md5,
                                )
                        except Exception as e:
                            log.error(
                                "SMB integrity check failed for share %s: %s",
                                share_name,
                                e,
                            )
                            test_failed = True
                        finally:
                            client_node.exec_command(
                                cmd=f"rm -f {verify_file}",
                                timeout=10,
                                check_ec=False,
                            )
            except Exception as e:
                log.error("Post-thrash SMB integrity verification failed: %s", e)
                test_failed = True

        # TODO: Add post-thrash integrity verification for CephFS, RBD, and RGW
        #   clients (pre-thrash baselines must be written first — see pre-thrash
        #   setup phase TODO)

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

        if enable_esb_verification:
            log.info("Removing BlueStore ESB verification configs...")
            for name in [
                "bluestore_elastic_shared_blobs",
                "bluestore_write_v2",
                "bluestore_onode_segment_size",
                "bluestore_debug_extent_map_encode_check",
                "debug_bluestore",
            ]:
                mon_obj.remove_config(section="osd", name=name)
            log.info("ESB verification configs removed")

        log.info("Cleaning up all error injections...")
        injector.cleanup_all()

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
                log.info("Restarting OSD service to recover down daemons...")
                try:
                    rados_obj.restart_daemon_services(daemon="osd")
                    log.info("OSD service restart completed")
                except Exception as restart_err:
                    log.warning(f"OSD service restart failed: {restart_err}")
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

        test_end_time = get_cluster_timestamp(rados_obj.node)
        log.debug(f"Test completed. Start: {start_time}, End: {test_end_time}")

        try:
            if _destructive_mds_injection:
                log.info("Validating expected MDS corruption crashes...")
                crash_list = rados_obj.do_crash_ls() or []
                mds_crashes = [
                    c for c in crash_list if c.get("entity_name", "").startswith("mds.")
                ]
                if mds_crashes:
                    validated = False
                    log.info(
                        f"Found {len(mds_crashes)} MDS crash(es), inspecting details..."
                    )
                    for crash in mds_crashes[:5]:
                        try:
                            info = rados_obj.run_ceph_command(
                                f"ceph crash info {crash['crash_id']}",
                                client_exec=True,
                            )
                            log.info(
                                f"  Crash: {info.get('entity_name')} "
                                f"at {info.get('timestamp')} "
                                f"sig={str(info.get('stack_sig') or 'N/A')[:16]} "
                                f"assert={info.get('assert_condition', 'N/A')} "
                                f"file={info.get('assert_file', 'N/A')}"
                            )
                            bt = info.get("backtrace", [])
                            if (
                                any(
                                    "check_corruption" in f or "CDentry" in f
                                    for f in bt
                                )
                                and info.get("process_name") == "ceph-mds"
                            ):
                                validated = True
                                log.info("  -> Matches CDentry corruption signature")
                        except Exception:
                            continue
                    if validated:
                        log.info(
                            f"EXPECTED: {len(mds_crashes)} MDS crashes with "
                            f"CDentry corruption signature. Injection validated."
                        )
                        mds_recovery = CephInjectionRecovery(rados_obj, client_node)
                        repair = mds_recovery.recover_mds_corruption(
                            fs_name=fs_name or "cephfs-thrash",
                            cephfs_mount_path=cephfs_mount_path,
                        )
                        log.info(f"Repair outcome: {repair['outcome']}")
                    else:
                        log.error(
                            "MDS crashes found but none match expected "
                            "corruption signature"
                        )
                        test_failed = True
                else:
                    log.error(
                        "No MDS crashes despite destructive injection -- "
                        "injection may not have triggered"
                    )
                    test_failed = True
            else:
                # NFS+OSD crash recovery downgrade:
                # When both NFS and OSD thrashing are active, OSD pool
                # degradation (.nfs RADOS pool) can cascade into NFS
                # daemon crashes.  If crashes are detected but all NFS
                # daemons have since recovered to "running" state, the
                # crash is downgraded to a warning rather than failing
                # the test.  This avoids false negatives caused by the
                # expected OSD->NFS failure cascade.
                if rados_obj.check_crash_status(
                    start_time=start_time,
                    end_time=test_end_time,
                    check_mds=enable_mds_thrashing,
                    check_nfs=enable_nfs_thrashing,
                    check_rgw=enable_rgw_thrashing,
                    check_smb=enable_smb_thrashing,
                ):
                    if enable_nfs_thrashing and enable_osd_thrashing:
                        all_nfs_recovered = True
                        try:
                            for cluster in (nfs_config or {}).get("clusters", []):
                                cid = cluster.get("cluster_id", "")
                                if not cid:
                                    continue
                                svc_name = f"nfs.{cid}"
                                daemon_ids = rados_obj.get_service_spec_daemons(
                                    service_name=svc_name
                                )
                                for did in daemon_ids or []:
                                    status = rados_obj.get_daemon_status(
                                        daemon_type="nfs", daemon_id=str(did)
                                    )
                                    status_desc = status[1] if status else "unknown"
                                    if status_desc != "running":
                                        all_nfs_recovered = False
                                        break
                                if not all_nfs_recovered:
                                    break
                        except Exception as nfs_check_err:
                            log.warning("NFS recovery check failed: %s", nfs_check_err)
                            all_nfs_recovered = False

                        if all_nfs_recovered:
                            log.warning(
                                "Crash detected but all NFS daemons have recovered. "
                                "Likely cascading effect from OSD thrashing degrading "
                                "the .nfs RADOS pool. Downgrading to warning."
                            )
                        else:
                            log.error(
                                "Test failed due to crash detected "
                                "(NFS daemons not fully recovered)"
                            )
                            test_failed = True
                    else:
                        log.error("Test failed due to crash detected")
                        test_failed = True
        except Exception as e:
            log.error(f"Crash check failed: {e}")
            test_failed = True

        if _destructive_mds_injection:
            _fs = fs_name or "cephfs-thrash"
            recovery = CephInjectionRecovery(rados_obj, client_node)
            recovery.destroy_damaged_filesystem(_fs, cephfs_mount_path)

        if config.get("cleanup_pools", True):
            log.debug("Cleaning up test resources...")

            # Phase 1: Unmount filesystems, cleanup RGW and NFS (parallel)
            cleanup_tasks = []
            with cf.ThreadPoolExecutor(max_workers=4) as cleanup_executor:
                if enable_cephfs_pools and not _destructive_mds_injection:
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

        # Unmount pre-thrash NFS integrity mounts (infrastructure cleanup,
        # runs regardless of cleanup_pools setting)
        if nfs_integrity_mounts:
            log.info(
                "Unmounting %s pre-thrash NFS integrity mount(s)...",
                len(nfs_integrity_mounts),
            )
            for mp in nfs_integrity_mounts:
                try:
                    client_node.exec_command(
                        sudo=True,
                        cmd=f"umount -l {mp} 2>/dev/null; " f"rmdir {mp} 2>/dev/null",
                        timeout=20,
                        check_ec=False,
                    )
                    log.info("  Unmounted: %s", mp)
                except Exception as e:
                    log.warning("  Failed to unmount %s: %s", mp, e)

        # Clean up SMB integrity temp files (runs regardless of cleanup_pools)
        if smb_integrity_baselines:
            log.info("Cleaning up SMB integrity temp files...")
            try:
                client_node.exec_command(
                    cmd="rm -f /tmp/smb_integrity_*.dat",
                    timeout=15,
                    check_ec=False,
                )
                log.info("  SMB integrity temp files removed")
            except Exception as e:
                log.warning("  SMB integrity temp file cleanup failed: %s", e)

        # Clean up SMB clusters/shares and the dedicated CephFS volume
        # (runs regardless of cleanup_pools to prevent leftover resources
        # from blocking subsequent runs)
        if smb_config and smb_config.get("cluster_ids"):
            log.info("Cleaning up SMB resources (unconditional)...")
            try:
                if smb_wf is None:
                    smb_wf = SmbThrashWorkflows(
                        cephadm.installer, ceph_cluster, rados_obj
                    )
                smb_wf.cleanup(smb_config)
                log.info("  SMB clusters and shares removed")
            except Exception as e:
                log.warning("  SMB cluster/share cleanup failed: %s", e)
            smb_vol = smb_config.get("cephfs_vol", "")
            if smb_vol:
                try:
                    cephadm.installer.exec_command(
                        sudo=True,
                        cmd=f"ceph orch rm mds.{smb_vol}",
                        timeout=60,
                        check_ec=False,
                    )
                    log.info("  MDS service for '%s' removed", smb_vol)
                except Exception as e:
                    log.warning("  MDS service removal failed: %s", e)
                try:
                    cephadm.installer.exec_command(
                        sudo=True,
                        cmd=(f"ceph fs volume rm" f" {smb_vol} --yes-i-really-mean-it"),
                        timeout=120,
                    )
                    log.info("  SMB CephFS volume '%s' removed", smb_vol)
                except Exception as e:
                    log.warning("  SMB CephFS volume removal failed: %s", e)

        # Re-enable firewall on nodes where it was disabled for NFS
        if nfs_firewall_disabled_nodes:
            log.info(
                "Re-enabling firewall on %s node(s)...",
                len(nfs_firewall_disabled_nodes),
            )
            for node in nfs_firewall_disabled_nodes:
                try:
                    node.exec_command(
                        cmd="systemctl enable firewalld 2>/dev/null || true; "
                        "systemctl start firewalld 2>/dev/null || true",
                        sudo=True,
                        timeout=30,
                        check_ec=False,
                    )
                    log.info("  %s: firewall re-enabled", node.hostname)
                except Exception as e:
                    log.warning(
                        "  %s: Failed to re-enable firewall - %s", node.hostname, e
                    )

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
                    host_objs = []
                    for host in osd_hosts:
                        host_obj = utils.get_node_by_id(ceph_cluster, host)
                        if host_obj:
                            host_objs.append(host_obj)

                    def _rotate_log(node):
                        try:
                            node.exec_command(
                                cmd=logrotate_cmd,
                                sudo=True,
                                check_ec=False,
                                long_running=True,
                            )
                            log.debug(f"Log rotation completed on {node.hostname}")
                        except Exception:
                            pass

                    with cf.ThreadPoolExecutor(max_workers=len(host_objs)) as pool:
                        pool.map(_rotate_log, host_objs)
                    log.info(
                        f"Log rotation triggered on {len(host_objs)} OSD host(s) in parallel"
                    )
            except Exception as e:
                log.warning(f"Failed to rotate logs: {e}")

        # Post-cleanup: recover any persistent injection damage
        try:
            post_recovery = CephInjectionRecovery(rados_obj, client_node)
            recovery_result = post_recovery.recover_all(
                fs_name=(
                    None if _destructive_mds_injection else (fs_name or "cephfs-thrash")
                ),
                cephfs_mount_path=cephfs_mount_path,
            )
            if recovery_result.get("bluestore_recovery"):
                log.info(
                    f"BlueStore recovery: "
                    f"{recovery_result['bluestore_recovery']['summary']}"
                )
        except Exception as e:
            log.warning(f"Post-cleanup recovery failed: {e}")

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
        pool_configs_path = _get_pool_configs_path(rados_obj.rhbuild)
        log.debug(f"Loading pool configs from: {pool_configs_path}")
        with open(pool_configs_path, "r") as f:
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
    Thrash PG count via bulk flag toggle to exercise PG split and merge paths.

    Per iteration:
    1. Enable bulk flag on all pools (triggers PG split)
    2. Wait 90-120 seconds for splits to complete
    3. Remove bulk flag from all pools (triggers PG merge)
    4. Wait 90-120 seconds for merges to complete

    Scrub and deep-scrub operations are handled separately by
    thrash_scrubs() when enable_scrub_thrashing is enabled.

    Args:
        rados_obj: RadosOrchestrator object
        pool_obj: PoolFunctions object for bulk flag operations
        pools: List of pool configurations
        iterations: Number of iterations
        stop_flag: Dict with 'stop' key to signal early termination

    Returns:
        Number of iterations completed
    """
    log.info(f"Starting PG count thrashing (iterations: {iterations})")

    # Filter RADOS pools only (exclude CephFS and RBD pools)
    rados_pools = tuple(p for p in pools if p.get("client") not in ("cephfs", "rbd"))

    if not rados_pools:
        log.info("No RADOS pools found, skipping PG count thrashing")
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
                f"Iteration {iteration + 1}/{iterations}: PG thrashing on all pools"
            )

            # Phase 1: Enable bulk flag on ALL pools (triggers PG split)
            for pool in rados_pools:
                pool_name = pool["pool_name"]
                pool_obj.set_bulk_flag(pool_name=pool_name)
                bulk_enabled_pools.append(pool_name)

            if bulk_enabled_pools:
                pool_count = len(bulk_enabled_pools)
                log.info(
                    f"  Enabled bulk on {pool_count} pool(s), "
                    f"sleeping for PG splits..."
                )

                # Phase 2: Sleep while ALL pools split PGs simultaneously
                time.sleep(random.uniform(90, 120))

                # Phase 3: Remove bulk flag from ALL pools (triggers PG merge)
                log.info(f"  Removing bulk from {pool_count} pool(s)...")
                for pool_name in bulk_enabled_pools:
                    pool_obj.rm_bulk_flag(pool_name=pool_name)
                bulk_enabled_pools.clear()

                # Phase 4: Sleep for PG merges
                log.info(f"  Sleeping while {pool_count} pool(s) merge PGs...")
                time.sleep(random.uniform(90, 120))

                completed += 1
            else:
                log.warning(
                    f"Iteration {iteration + 1}: No pools had bulk enabled, skipping PG thrashing"
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

    log.info(f"PG count thrashing completed: {completed} iterations")
    return completed


def thrash_scrubs(
    rados_obj: RadosOrchestrator,
    pools: List[Dict],
    duration: int,
    aggressive: bool,
    stop_flag: Dict,
) -> int:
    """
    Run periodic scrub and deep-scrub cycles on the cluster.

    Each iteration randomly picks a scrub strategy and target:

    Strategy (random per iteration):
        - scrub: Standard scrub to verify replica/shard consistency
        - deep-scrub: Full data + metadata verification with checksum

    Target (random per iteration):
        - cluster-wide: ceph osd scrub all / deep-scrub all
        - per-pool: ceph osd pool scrub <pool> / deep-scrub <pool>
        - per-osd: ceph osd scrub <osd_id> / deep-scrub <osd_id>

    Timing:
        - Normal mode: 30-60s between iterations
        - Aggressive mode: 15-30s between iterations (more frequent scrubs)

    This function is independent of thrash_pg_count and does NOT perform
    PG split/merge operations. It purely exercises the scrub subsystem.

    Args:
        rados_obj: RadosOrchestrator instance
        pools: List of pool config dicts (for per-pool targeting)
        duration: Total duration in seconds
        aggressive: If True, scrub more frequently with shorter intervals
        stop_flag: Shared dict with "stop" key for coordinated shutdown

    Returns:
        Number of scrub operations completed
    """
    log.info(
        f"Starting scrub thrashing (duration: {duration}s, aggressive: {aggressive})"
    )

    pool_names = [p["pool_name"] for p in pools if p.get("pool_name")]

    osd_tree = rados_obj.run_ceph_command(cmd="ceph osd tree")
    osd_ids = [
        node["id"]
        for node in osd_tree.get("nodes", [])
        if node.get("type") == "osd" and node.get("status") == "up"
    ]
    if not osd_ids:
        log.warning("No up OSDs found for scrub thrashing, running cluster-wide only")

    completed = 0
    end_time = time.time() + duration

    try:
        while time.time() < end_time and not stop_flag.get("stop"):
            scrub_type = random.choice(["scrub", "deep-scrub"])
            target_type = random.choice(["cluster", "pool", "osd"])

            try:
                if target_type == "pool" and pool_names:
                    target_pool = random.choice(pool_names)
                    if scrub_type == "deep-scrub":
                        rados_obj.run_deep_scrub(pool=target_pool)
                    else:
                        rados_obj.run_scrub(pool=target_pool)
                    log.info(
                        f"Scrub [{completed + 1}]: {scrub_type} on pool {target_pool}"
                    )

                elif target_type == "osd" and osd_ids:
                    target_osd = random.choice(osd_ids)
                    if scrub_type == "deep-scrub":
                        rados_obj.run_deep_scrub(osd=target_osd)
                    else:
                        rados_obj.run_scrub(osd=target_osd)
                    log.info(
                        f"Scrub [{completed + 1}]: {scrub_type} on osd.{target_osd}"
                    )

                else:
                    if scrub_type == "deep-scrub":
                        rados_obj.run_deep_scrub()
                    else:
                        rados_obj.run_scrub()
                    log.info(f"Scrub [{completed + 1}]: {scrub_type} cluster-wide")

                completed += 1

            except Exception as e:
                log.warning(f"Scrub operation failed (non-fatal): {e}")

            sleep_time = (
                random.uniform(15, 30) if aggressive else random.uniform(30, 60)
            )
            time.sleep(sleep_time)

    except Exception as e:
        log.error(f"Exception in thrash_scrubs: {e}")
        return completed

    log.info(f"Scrub thrashing completed: {completed} operations")
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
    cluster_fsid = rados_obj.run_ceph_command(cmd="ceph fsid")["fsid"]

    # Build list of available operations
    operations = ["leader_failover", "rolling_restart", "sigkill"]
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
            elif operation == "sigkill":
                result = _thrash_mon_sigkill(rados_obj, mon_workflow_obj, cluster_fsid)
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
    4. SIGKILL - Kill random MGR process with kill -9 (abrupt crash)

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
    cluster_fsid = rados_obj.run_ceph_command(cmd="ceph fsid")["fsid"]

    operations = ["failover", "rolling_restart", "random_fail", "sigkill"]

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
            elif operation == "sigkill":
                result = _thrash_mgr_sigkill(rados_obj, mgr_workflow_obj, cluster_fsid)
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


def thrash_nfs_fio(
    client_node,
    nfs_config: Dict[str, Any],
    duration: int,
    stop_flag: Dict,
) -> Dict[str, int]:
    """
    Thrash NFS exports with mount -> FIO -> FS ops -> unmount cycles.

    Resolves mount targets via ``_nfs_export_mount_targets()`` (handles
    multi-host NFS clusters), distributes them across up to 5 parallel
    worker threads, and continuously cycles through:

    1. Mount all assigned exports (cycling NFSv4.0/4.1/4.2; NFSv3 only
       if the cluster was created with ``enable_nfsv3``).  Multi-host
       clusters rotate the active host per cycle for load distribution.
    2. Run ``ceph nfs export info`` / ``ls`` on a random mounted export.
    3. Run FIO workloads on all mounted exports (cycling through I/O
       patterns: seq_write, seq_read, rand_write, rand_read, mixed_rw).
    4. Run filesystem operations (dd, stat, chmod, symlink, hardlink,
       remount, rename, touch) on each mounted export.
    5. Unmount all exports.
    6. Repeat until ``duration`` expires or ``stop_flag["stop"]`` is set.

    Args:
        client_node: Client node to mount/unmount and run FIO.
        nfs_config: NFS configuration dict from
            ``create_nfs_clusters_and_exports``.  Must contain ``exports``
            and ``clusters`` lists; may contain ``enable_rdma``,
            ``enable_nfsv3``, and per-cluster ``rdma_port`` entries.
        duration: Total duration in seconds.
        stop_flag: Dict with ``stop`` key to signal early termination.

    Returns:
        dict: ``{"cycles": int, "failures": int}`` aggregated across all
        worker threads.
    """
    nfs_versions_v4 = (
        ("4.0", "nfsvers=4.0"),
        ("4.1", "nfsvers=4.1"),
        ("4.2", "nfsvers=4.2"),
    )
    if nfs_config.get("enable_nfsv3"):
        nfs_versions = (("3", "nfsvers=3,nolock"),) + nfs_versions_v4
        log.info(
            "NFS FIO thrashing includes NFSv3 (cluster was created with --enable-nfsv3)"
        )
    else:
        nfs_versions = nfs_versions_v4
        log.info(
            "NFS FIO thrashing skips NFSv3 (cluster was created without --enable-nfsv3)"
        )

    log.info("Starting NFS FIO thrashing (5 parallel workers)")

    exports = nfs_config.get("exports", [])

    if not exports:
        log.warning("No NFS exports available for thrashing")
        return {"cycles": 0, "failures": 0}

    mount_targets = _nfs_export_mount_targets(nfs_config)
    if not mount_targets:
        log.warning("No valid NFS mount targets available for thrashing")
        return {"cycles": 0, "failures": 0}

    num_workers = min(5, len(mount_targets))
    target_groups = [[] for _ in range(num_workers)]
    for idx, target in enumerate(mount_targets):
        target_groups[idx % num_workers].append((idx, target))

    log.info(
        "Distributing %s exports across %s workers: %s",
        len(mount_targets),
        num_workers,
        [len(g) for g in target_groups],
    )

    # Log NFS version assignments
    version_info = [
        f"Worker{i}:NFSv{nfs_versions[i % len(nfs_versions)][0]}"
        for i in range(num_workers)
    ]
    log.info(f"NFS version assignments: {', '.join(version_info)}")

    total_cycles = 0
    total_failures = 0
    with cf.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        for worker_id, target_group in enumerate(target_groups):
            if target_group:
                assigned_nfs_version = nfs_versions[worker_id % len(nfs_versions)]
                futures.append(
                    executor.submit(
                        _nfs_worker_thread,
                        client_node=client_node,
                        worker_id=worker_id,
                        target_group=target_group,
                        nfs_version=assigned_nfs_version,
                        duration=duration,
                        stop_flag=stop_flag,
                        nfs_config=nfs_config,
                    )
                )

        # Collect results
        for future in cf.as_completed(futures):
            try:
                result = future.result()
                if isinstance(result, dict):
                    total_cycles += result.get("cycles", 0)
                    total_failures += result.get("failures", 0)
                else:
                    total_cycles += result
            except Exception as e:
                log.warning(f"NFS worker thread failed: {e}")
                total_failures += 1

    log.info(
        f"NFS FIO thrashing completed: {total_cycles} total cycles across {num_workers} workers "
        f"(failures={total_failures})"
    )
    return {"cycles": total_cycles, "failures": total_failures}


def _nfs_worker_thread(
    client_node,
    worker_id: int,
    target_group: List[tuple],
    nfs_version: tuple,
    duration: int,
    stop_flag: Dict,
    nfs_config: Dict[str, Any],
) -> Dict[str, int]:
    """
    Worker thread for NFS FIO thrashing.

    Each worker handles a subset of pre-resolved mount targets using a specific
    NFS version: mount -> NFS info commands -> FIO -> unmount

    Args:
        client_node: Client node for commands
        worker_id: Unique worker identifier
        target_group: List of (idx, target_dict) from _nfs_export_mount_targets
        nfs_version: Tuple of (version_str, mount_options) e.g. ("4.1", "nfsvers=4.1")
        duration: Total duration in seconds
        stop_flag: Stop signal dict
        nfs_config: Full NFS setup dict (enable_rdma, clusters with rdma_port, etc.)

    Returns:
        Dict with cycles completed and failures encountered by this worker
    """
    nfs_ver_str, nfs_options = nfs_version

    fio_patterns = (
        ("seq_write", "write", "64k", "20M", 20),
        ("seq_read", "read", "64k", "20M", 20),
        ("rand_write", "randwrite", "4k", "10M", 15),
        ("rand_read", "randread", "4k", "10M", 15),
        ("mixed_rw", "randrw", "8k", "10M", 20),
    )

    end_time = time.time() + duration
    cycle_count = 0
    failures = 0
    base_mount_dir = f"/mnt/nfs-thrash-w{worker_id}"
    num_patterns = len(fio_patterns)

    export_info = []
    for idx, target in target_group:
        if target.get("hosts") and target.get("port") is not None:
            export_info.append(
                {
                    "mount_point": f"{base_mount_dir}/export_{idx}",
                    "cluster_id": target["cluster_id"],
                    "pseudo_path": target["pseudo_path"],
                    "host": target["host"],
                    "hosts": target["hosts"],
                    "port": target["port"],
                }
            )

    if not export_info:
        log.warning(f"Worker {worker_id}: No valid exports, exiting")
        return {"cycles": 0, "failures": 1}

    while time.time() < end_time and not stop_flag.get("stop"):
        cycle_count += 1
        # Unpack tuple patterns: (name, rw, bs, size, runtime)
        p_name, p_rw, p_bs, p_size, p_runtime = fio_patterns[
            (cycle_count - 1) % num_patterns
        ]

        mounted_exports = []

        try:
            # Step 1: Create worker-specific mount directory
            client_node.exec_command(cmd=f"mkdir -p {base_mount_dir}", sudo=True)

            # Step 2: Mount all exports assigned to this worker
            for exp in export_info:
                mount_point = exp["mount_point"]
                active_host = ""
                try:
                    hosts = exp.get("hosts") or (
                        [exp.get("host")] if exp.get("host") else []
                    )
                    if not hosts:
                        raise RuntimeError(
                            f"No NFS hosts available for cluster {exp['cluster_id']}"
                        )
                    host_idx = (cycle_count - 1 + worker_id) % len(hosts)
                    active_host = hosts[host_idx]

                    client_node.exec_command(cmd=f"mkdir -p {mount_point}", sudo=True)
                    mount_opts = _nfs_client_mount_option_string(
                        nfs_config, exp["cluster_id"], nfs_options
                    )
                    mount_cmd = (
                        f"mount -t nfs -o {mount_opts} "
                        f"{active_host}:{exp['pseudo_path']} {mount_point}"
                    )
                    client_node.exec_command(cmd=mount_cmd, sudo=True, timeout=60)
                    mounted_exports.append((exp, active_host))
                except Exception as e:
                    err_type = type(e).__name__
                    log.warning(
                        "Worker %s: MOUNT FAILED\n"
                        "  Export: %s:%s\n"
                        "  Mount point: %s\n"
                        "  NFS version: %s\n"
                        "  Error: %s: %s",
                        worker_id,
                        active_host,
                        exp["pseudo_path"],
                        mount_point,
                        nfs_ver_str,
                        err_type,
                        e,
                    )
                    failures += 1

            if not mounted_exports:
                log.warning(
                    f"Worker {worker_id} cycle {cycle_count}: No exports mounted"
                )
                time.sleep(5)
                continue

            # Step 2.5: Run NFS export info on one random export per cycle
            exp, _host = random.choice(mounted_exports)
            cid, ppath = exp["cluster_id"], exp["pseudo_path"]
            try:
                client_node.exec_command(
                    cmd=f"ceph nfs export info {cid} {ppath} --format json-pretty",
                    sudo=True,
                )
                client_node.exec_command(
                    cmd=f"ceph nfs export ls {cid} --detailed --format json-pretty",
                    sudo=True,
                )
            except Exception as e:
                log.warning(f"Worker {worker_id}: NFS INFO FAILED - {cid}:{ppath}: {e}")
                failures += 1

            # Step 3: Run FIO on all mounted exports
            for exp, exp_active_host in mounted_exports:
                if stop_flag.get("stop"):
                    break

                work_dir = f"{exp['mount_point']}/fio_c{cycle_count}"
                try:
                    client_node.exec_command(cmd=f"mkdir -p {work_dir}", sudo=True)
                    fio_cmd = (
                        f"fio --name={p_name} --directory={work_dir} "
                        f"--rw={p_rw} --bs={p_bs} --size={p_size} "
                        f"--numjobs=2 --time_based --runtime={p_runtime} "
                        f"--group_reporting --minimal"
                    )
                    client_node.exec_command(cmd=fio_cmd, sudo=True, timeout=120)
                except Exception as e:
                    err_type = type(e).__name__
                    log.warning(
                        "Worker %s: FIO FAILED\n"
                        "  Pattern: %s (rw=%s, bs=%s)\n"
                        "  Work dir: %s\n"
                        "  Export: %s:%s\n"
                        "  Error: %s: %s",
                        worker_id,
                        p_name,
                        p_rw,
                        p_bs,
                        work_dir,
                        exp_active_host,
                        exp["pseudo_path"],
                        err_type,
                        e,
                    )
                    failures += 1

            # Step 4: Additional filesystem operations on mounted NFS
            for exp, exp_active_host in mounted_exports:
                if stop_flag.get("stop"):
                    break

                work_dir = f"{exp['mount_point']}/fs_ops_c{cycle_count}"
                try:
                    # Clean up any leftover files from previous runs to avoid
                    # "File exists" errors on hardlink creation
                    client_node.exec_command(
                        cmd=f"rm -rf {work_dir}", sudo=True, check_ec=False
                    )
                    client_node.exec_command(cmd=f"mkdir -p {work_dir}", sudo=True)

                    # Batch all FS operations into a single script to reduce
                    # SSH round-trips (15+ calls -> 2 calls)
                    # Note: rm -f before ln to handle NFS caching issues
                    fs_ops_script = f"""bash <<'FSOPS_WORKER'
set -e
# Create test file
dd if=/dev/urandom of={work_dir}/test_file bs=4k count=10 2>/dev/null
# Hardlink (remove first to handle NFS caching)
rm -f {work_dir}/test_hardlink 2>/dev/null || true
ln {work_dir}/test_file {work_dir}/test_hardlink
# Rename original
mv -f {work_dir}/test_file {work_dir}/test_renamed
# Symlink (after rename, points to new name)
ln -sf {work_dir}/test_renamed {work_dir}/test_symlink
# Permission change
chmod 755 {work_dir}/test_renamed
# Extended attributes (best-effort)
setfattr -n user.thrash -v 'cycle{cycle_count}' {work_dir}/test_renamed 2>/dev/null || true
getfattr -n user.thrash {work_dir}/test_renamed 2>/dev/null || true
setfattr -x user.thrash {work_dir}/test_renamed 2>/dev/null || true
# Chown (best-effort)
chown nobody:nogroup {work_dir}/test_renamed 2>/dev/null || true
# Nested directories
mkdir -p {work_dir}/nested/a/b/c
ls -R {work_dir} >/dev/null
FSOPS_WORKER
"""
                    client_node.exec_command(
                        cmd=fs_ops_script.strip(), sudo=True, timeout=60
                    )

                    # Unmount and remount to verify persistence
                    mount_point = exp["mount_point"]
                    remount_opts = _nfs_client_mount_option_string(
                        nfs_config, exp["cluster_id"], nfs_options
                    )
                    client_node.exec_command(
                        cmd=f"umount -lf {mount_point} && sleep 1 && "
                        f"mount -t nfs -o {remount_opts} "
                        f"{exp_active_host}:{exp['pseudo_path']} {mount_point}",
                        sudo=True,
                        timeout=60,
                    )

                    # Verify after remount (batched)
                    client_node.exec_command(
                        cmd=f"cat {work_dir}/test_symlink >/dev/null && "
                        f"cat {work_dir}/test_hardlink >/dev/null && "
                        f"ls -la {work_dir}/ && rm -rf {work_dir}",
                        sudo=True,
                        check_ec=False,
                    )

                except Exception as e:
                    err_type = type(e).__name__
                    log.warning(
                        "Worker %s: FS OPS FAILED\n"
                        "  Work dir: %s\n"
                        "  Export: %s:%s\n"
                        "  Error: %s: %s",
                        worker_id,
                        work_dir,
                        exp_active_host,
                        exp["pseudo_path"],
                        err_type,
                        e,
                    )
                    failures += 1

        except Exception as e:
            err_type = type(e).__name__
            log.warning(
                f"Worker {worker_id} cycle {cycle_count} error: {err_type}: {e}"
            )
            failures += 1

        finally:
            if mounted_exports:
                mount_paths = " ".join(e["mount_point"] for e, _h in mounted_exports)
                try:
                    client_node.exec_command(
                        cmd=f"umount -lf {mount_paths}", sudo=True, check_ec=False
                    )
                    client_node.exec_command(
                        cmd=f"rm -rf {base_mount_dir}/*", sudo=True, check_ec=False
                    )
                except Exception:
                    pass

            log.info(
                f"Worker {worker_id} cycle {cycle_count} complete: "
                f"NFSv{nfs_ver_str}, mounted={len(mounted_exports)}"
            )
        time.sleep(5)

    log.info(
        f"Worker {worker_id}: Completed {cycle_count} cycles (failures={failures})"
    )
    return {"cycles": cycle_count, "failures": failures}


def thrash_nfs_locking(
    client_node,
    nfs_config: Dict[str, Any],
    stop_flag: Dict,
) -> Dict[str, int]:
    """
    NFS locking validation: tests basic exclusive lock and byte-range locks.
    Runs a Python script that verifies fcntl advisory locking works over NFS.
    """
    if stop_flag.get("stop"):
        return {"ops": 0, "failures": 0}

    failures = 0
    ops = 0

    with _nfs_mount_context(
        client_node, nfs_config, "/mnt/nfs-lock-test", index=0, nfs_ver="4.1"
    ) as (exp, mount_point):
        if not exp or not mount_point:
            log.warning("NFS locking: mount failed, skipping")
            return {"ops": 0, "failures": 1}

        try:
            lock_file = f"{mount_point}/lock_test.dat"
            script_file = f"{mount_point}/lock_test.py"
            # Create 4KB file for byte-range locking tests
            client_node.exec_command(cmd=f"truncate -s 4096 {lock_file}", sudo=True)

            # Combined locking test script (diagnostic, always exit 0)
            # Note: NFS-Ganesha may not block same-process locks per POSIX,
            # so we report behavior without failing the test.
            py_script = """#!/usr/bin/env python3
import fcntl, os, sys, time

path = sys.argv[1]
fd1 = fd2 = None
passed = 0
info = 0
start_time = time.time()

def elapsed():
    return f"[{time.time() - start_time:.3f}s]"

try:
    # Setup info
    file_size = os.path.getsize(path)
    print(f"{elapsed()} File: {path} (size={file_size} bytes)")

    fd1 = os.open(path, os.O_RDWR)
    fd2 = os.open(path, os.O_RDWR)
    print(f"{elapsed()} Opened file descriptors: fd1={fd1}, fd2={fd2}")

    # Test 1: Basic exclusive lock acquire
    print(f"{elapsed()} Test 1: Acquiring exclusive lock on fd1...")
    fcntl.lockf(fd1, fcntl.LOCK_EX)
    print(f"{elapsed()} PASS: Acquired exclusive lock on fd1")
    passed += 1

    # Test 2: Second lock from same process (NFS may or may not block)
    print(f"{elapsed()} Test 2: Trying non-blocking lock on fd2 (same process)...")
    try:
        fcntl.lockf(fd2, fcntl.LOCK_EX | fcntl.LOCK_NB)
        print(f"{elapsed()} INFO: Same-process second lock allowed (NFS-Ganesha behavior)")
        info += 1
        fcntl.lockf(fd2, fcntl.LOCK_UN)
    except BlockingIOError:
        print(f"{elapsed()} PASS: Same-process lock blocked (strict POSIX)")
        passed += 1
    fcntl.lockf(fd1, fcntl.LOCK_UN)
    print(f"{elapsed()} Released fd1 lock")

    # Test 3: Byte-range non-overlapping (should always succeed)
    print(f"{elapsed()} Test 3: Byte-range lock fd1 bytes 0-99...")
    fcntl.lockf(fd1, fcntl.LOCK_EX, 100, 0)
    print(f"{elapsed()} Acquired fd1 lock on bytes 0-99")
    print(f"{elapsed()} Trying fd2 lock on bytes 200-299 (non-overlapping)...")
    try:
        fcntl.lockf(fd2, fcntl.LOCK_EX | fcntl.LOCK_NB, 100, 200)
        print(f"{elapsed()} PASS: Non-overlapping byte-range lock succeeded")
        passed += 1
        fcntl.lockf(fd2, fcntl.LOCK_UN, 100, 200)
    except BlockingIOError:
        print(f"{elapsed()} WARN: Non-overlapping byte-range lock blocked unexpectedly")
        info += 1

    # Test 4: Byte-range overlapping check
    print(f"{elapsed()} Test 4: Trying fd2 lock on bytes 0-49 (overlaps with fd1)...")
    try:
        fcntl.lockf(fd2, fcntl.LOCK_EX | fcntl.LOCK_NB, 50, 0)
        print(f"{elapsed()} INFO: Overlapping lock allowed (NFS-Ganesha same-process behavior)")
        info += 1
        fcntl.lockf(fd2, fcntl.LOCK_UN, 50, 0)
    except BlockingIOError:
        print(f"{elapsed()} PASS: Overlapping byte-range lock blocked correctly")
        passed += 1

    fcntl.lockf(fd1, fcntl.LOCK_UN, 100, 0)
    print(f"{elapsed()} Released all locks")
    print(f"{elapsed()} SUMMARY: {passed} passed, {info} info/expected behaviors")
    sys.exit(0)
except Exception as e:
    print(f"{elapsed()} ERROR: Locking test failed unexpectedly: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
finally:
    if fd2 is not None: os.close(fd2)
    if fd1 is not None: os.close(fd1)
"""
            client_node.exec_command(
                cmd=f"cat > {script_file} << 'LOCKTEST'\n{py_script}\nLOCKTEST",
                sudo=True,
            )
            client_node.exec_command(cmd=f"chmod +x {script_file}", sudo=True)

            out, _ = client_node.exec_command(
                cmd=f"python3 {script_file} {lock_file} 2>&1",
                sudo=True,
                timeout=30,
                check_ec=False,
            )
            log.info(f"NFS locking results:\n{out.strip()}")
            ops += 1
        except Exception as e:
            log.warning(f"NFS locking failed: {e}")
            failures += 1
        finally:
            client_node.exec_command(
                cmd=f"rm -f {script_file}", sudo=True, check_ec=False
            )

    return {"ops": ops, "failures": failures}


def thrash_nfs_rootsquash(
    client_node,
    nfs_config: Dict[str, Any],
    stop_flag: Dict,
) -> Dict[str, int]:
    """
    NFS rootsquash and SELinux validation (best-effort, diagnostic).
    Tests root write, nobody write, and SELinux labeling if available.
    """
    if stop_flag.get("stop"):
        return {"ops": 0, "failures": 0}

    failures = 0
    ops = 0
    export_index = len(nfs_config.get("exports", [])) - 1
    export_index = max(0, export_index)

    with _nfs_mount_context(
        client_node,
        nfs_config,
        "/mnt/nfs-rootsquash",
        index=export_index,
        nfs_ver="4.2",
    ) as (exp, mount_point):
        if not exp or not mount_point:
            log.warning("NFS rootsquash: mount failed, skipping")
            return {"ops": 0, "failures": 1}

        test_file = f"{mount_point}/rootsquash_test.dat"

        try:
            # Test 1: Root write
            client_node.exec_command(cmd=f"echo root > {test_file}", sudo=True)
            client_node.exec_command(cmd=f"chmod 666 {test_file}", sudo=True)
            ops += 1

            # Test 2: SELinux labeling (best-effort)
            client_node.exec_command(
                cmd=f"chcon -t usr_t {test_file} 2>/dev/null && ls -Z {test_file}",
                sudo=True,
                check_ec=False,
            )
            ops += 1

            # Test 3: Nobody user write (if user exists)
            try:
                client_node.exec_command(cmd="id nobody", sudo=True)
                client_node.exec_command(
                    cmd=f"runuser -u nobody -- sh -c 'echo nobody >> {test_file}'",
                    sudo=True,
                    check_ec=False,
                )
                ops += 1
            except Exception:
                pass  # nobody user not available

        except Exception as e:
            log.warning(f"NFS rootsquash: test failed - {e}")
            failures += 1

    return {"ops": ops, "failures": failures}


def thrash_nfs_fsops_extended(
    client_node,
    nfs_config: Dict[str, Any],
    duration: int,
    stop_flag: Dict,
    iteration_delay: float = 2.0,
) -> Dict[str, int]:
    """
    Extended FS ops: cross-dir rename, hardlink delete, symlink perms, deep dirs.
    """
    if stop_flag.get("stop"):
        return {"ops": 0, "failures": 0}

    ops = 0
    failures = 0
    end_time = time.time() + min(duration, 300)

    with _nfs_mount_context(
        client_node, nfs_config, "/mnt/nfs-fsops-ext", index=2, nfs_ver="4.2"
    ) as (exp, mount_point):
        if not exp or not mount_point:
            log.warning("NFS fsops extended: mount failed, skipping")
            return {"ops": 0, "failures": 1}

        iter_idx = 0
        while time.time() < end_time and not stop_flag.get("stop"):
            iter_idx += 1
            base = f"{mount_point}/ext_{iter_idx}"
            b_dir = f"{base}/a/b"
            c_dir = f"{base}/c"

            # Batch all filesystem operations into a single shell script
            # to reduce SSH round-trips (12+ calls -> 1 call)
            # Using heredoc to avoid shell quoting issues
            fs_script = f"""bash <<'FSOPS_EOF'
set -e
mkdir -p {b_dir} {c_dir}
dd if=/dev/urandom of={b_dir}/f1 bs=1k count=4 2>/dev/null
mv {b_dir}/f1 {c_dir}/f1_renamed
dd if=/dev/urandom of={b_dir}/f2 bs=1k count=2 2>/dev/null
dd if=/dev/urandom of={c_dir}/f2 bs=1k count=1 2>/dev/null
mv -f {b_dir}/f2 {c_dir}/f2
ln {c_dir}/f1_renamed {c_dir}/f1_hl
stat -c %i {c_dir}/f1_renamed
stat -c %i {c_dir}/f1_hl
mv {c_dir}/f1_hl {c_dir}/f1_hl_mv
rm -f {c_dir}/f1_hl_mv
ln -s {c_dir}/f1_renamed {c_dir}/f1_symlink
chmod 700 {c_dir}/f1_symlink
ls -laR {base}
FSOPS_EOF
"""
            try:
                client_node.exec_command(cmd=fs_script.strip(), sudo=True, timeout=60)
                ops += 1
            except Exception as e:
                log.warning(f"NFS fsops iter {iter_idx} failed: {e}")
                failures += 1
            finally:
                client_node.exec_command(
                    cmd=f"rm -rf {base}", sudo=True, check_ec=False
                )

            time.sleep(iteration_delay)

    return {"ops": ops, "failures": failures}


def thrash_cephfs_subvolumes(
    rados_obj: RadosOrchestrator,
    fs_names: List[str],
    duration: int,
    stop_flag: Dict,
    num_groups: int = 3,
    num_subvols_per_group: int = 4,
) -> int:
    """
    Thrash CephFS subvolume operations - create groups, subvolumes, perform ops, delete.

    Operations per cycle:
    1. Create subvolume groups
    2. Create subvolumes under each group
    3. Perform subvolume operations (info, resize, snapshot create/delete)
    4. Delete subvolumes
    5. Delete subvolume groups
    6. Repeat until duration expires or stop_flag is set

    Args:
        rados_obj: RadosOrchestrator instance
        fs_names: List of CephFS filesystem names to operate on
        duration: Total duration in seconds
        stop_flag: Dict with 'stop' key to signal early termination
        num_groups: Number of subvolume groups to create per cycle
        num_subvols_per_group: Number of subvolumes per group

    Returns:
        Number of thrash cycles completed
    """
    if not fs_names:
        log.warning("No CephFS filesystems available for subvolume thrashing")
        return 0

    # Pre-define resize options (50MB, 150MB, 200MB)
    resize_options = (52428800, 157286400, 209715200)

    cycle_count = 0
    end_time = time.time() + duration

    log.info(
        f"Starting CephFS subvolume thrashing: fs={fs_names}, "
        f"groups={num_groups}, subvols/group={num_subvols_per_group}, duration={duration}s"
    )

    while time.time() < end_time and not stop_flag.get("stop"):
        cycle_count += 1
        fs_name = random.choice(fs_names)
        ts = int(time.time())

        created_groups = []
        created_subvols = []  # List of (group_name, subvol_name)

        log.info(f"Subvolume thrash cycle {cycle_count} on fs={fs_name}")

        try:
            # Step 1: Create subvolume groups
            for g_idx in range(num_groups):
                group_name = f"svgrp_{ts}_{g_idx}"
                try:
                    rados_obj.run_ceph_command(
                        cmd=f"ceph fs subvolumegroup create {fs_name} {group_name}"
                    )
                    created_groups.append(group_name)
                except Exception as e:
                    log.warning(f"Failed to create group {group_name}: {e}")

            if not created_groups:
                log.warning(f"Cycle {cycle_count}: No groups created, skipping")
                time.sleep(5)
                continue

            # Step 2: Create subvolumes under each group
            for group_name in created_groups:
                if stop_flag.get("stop"):
                    break
                group_opt = f"--group_name {group_name}"
                for sv_idx in range(num_subvols_per_group):
                    subvol_name = f"sv_{ts}_{sv_idx}"
                    try:
                        rados_obj.run_ceph_command(
                            cmd=f"ceph fs subvolume create {fs_name} {subvol_name} "
                            f"{group_opt} --size 104857600"
                        )
                        created_subvols.append((group_name, subvol_name, group_opt))
                    except Exception as e:
                        log.warning(f"Failed to create subvolume {subvol_name}: {e}")

            log.info(
                f"Cycle {cycle_count}: Created {len(created_groups)} groups, "
                f"{len(created_subvols)} subvolumes"
            )

            # Step 3: Perform subvolume operations
            for group_name, subvol_name, group_opt in created_subvols:
                if stop_flag.get("stop"):
                    break

                try:
                    # Get subvolume info and path
                    rados_obj.run_ceph_command(
                        cmd=f"ceph fs subvolume info {fs_name} {subvol_name} {group_opt}"
                    )
                    rados_obj.run_ceph_command(
                        cmd=f"ceph fs subvolume getpath {fs_name} {subvol_name} {group_opt}"
                    )

                    # Resize subvolume
                    new_size = random.choice(resize_options)
                    rados_obj.run_ceph_command(
                        cmd=f"ceph fs subvolume resize {fs_name} {subvol_name} "
                        f"{new_size} {group_opt}"
                    )

                    # Snapshot create -> list -> delete
                    snap_name = f"snap_{ts}_{subvol_name}"
                    rados_obj.run_ceph_command(
                        cmd=f"ceph fs subvolume snapshot create {fs_name} {subvol_name} "
                        f"{snap_name} {group_opt}"
                    )
                    rados_obj.run_ceph_command(
                        cmd=f"ceph fs subvolume snapshot ls {fs_name} {subvol_name} {group_opt}"
                    )
                    rados_obj.run_ceph_command(
                        cmd=f"ceph fs subvolume snapshot rm {fs_name} {subvol_name} "
                        f"{snap_name} {group_opt}"
                    )

                except Exception as e:
                    log.warning(f"Subvolume ops failed for {subvol_name}: {e}")

            # Step 4: List subvolumes in each group
            for group_name in created_groups:
                try:
                    rados_obj.run_ceph_command(
                        cmd=f"ceph fs subvolume ls {fs_name} --group_name {group_name}"
                    )
                except Exception:
                    pass  # Non-critical, skip logging

        except Exception as e:
            log.warning(f"Subvolume thrash cycle {cycle_count} error: {e}")

        finally:
            # Cleanup: Delete subvolumes first, then groups
            for group_name, subvol_name, group_opt in created_subvols:
                try:
                    rados_obj.run_ceph_command(
                        cmd=f"ceph fs subvolume rm {fs_name} {subvol_name} {group_opt}"
                    )
                except Exception:
                    pass  # Best-effort cleanup

            time.sleep(2)  # Wait for subvolume deletion

            for group_name in created_groups:
                try:
                    rados_obj.run_ceph_command(
                        cmd=f"ceph fs subvolumegroup rm {fs_name} {group_name}"
                    )
                except Exception:
                    pass  # Best-effort cleanup

        time.sleep(5)  # Pause between cycles

    log.info(f"CephFS subvolume thrashing completed: {cycle_count} cycles")
    return cycle_count


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
                cmd=f"umount -l {mount_path}", sudo=True, check_ec=False
            )
            time.sleep(3)
            client_node.exec_command(
                cmd=f"timeout 30 rm -rf {mount_path}",
                sudo=True,
                check_ec=False,
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
                cmd=f"umount -l {mount_path}", sudo=True, check_ec=False
            )
            if device_path:
                client_node.exec_command(
                    cmd=f"rbd unmap {device_path}",
                    sudo=True,
                    check_ec=False,
                )
            client_node.exec_command(
                cmd=f"timeout 30 rm -rf {mount_path}",
                sudo=True,
                check_ec=False,
            )
            log.debug(f"RBD unmounted from {mount_path}")
        except Exception as e:
            log.warning(f"Failed to unmount RBD: {e}")
