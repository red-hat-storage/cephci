"""
Test Module: NFS State Lifecycle & Robustness
==============================================

This module validates NFS-Ganesha state management under controlled stress
at two independent failure layers, individually and combined:

Layer 1 (NFS Protocol): Catches bugs in Ganesha's own state machine --
    clientid management, lease handling, delegation lifecycle, export binding.
    Chaos is induced organically at the NFS protocol boundary via iptables
    port blocking, tc netem latency, mount/unmount cycles, and export CRUD.
    If something breaks here, it is a Ganesha state machine bug.

Layer 2 (MDS/FSAL Backend): Catches bugs in how Ganesha handles backend
    failures -- cap revocation, MDS session loss, RADOS timeouts.  Chaos
    is injected at the Ceph daemon layer via CephErrorInjector (MON config
    database).  If something breaks here, it is a Ganesha resilience bug
    or a Ceph bug surfaced through NFS.

Layer 1+2 (Combined): Catches interaction bugs -- the most realistic
    failure mode.  NFS client disconnects while Ganesha is simultaneously
    losing caps to MDS.  These are the hardest bugs to reproduce.

The injection layer is selected per test via ``config["injection_layer"]``:
    - ``nfs_protocol``  -- Layer 1 only (default)
    - ``mds_backend``   -- Layer 2 only
    - ``combined``      -- Both layers active

Each scenario is organized into five sequential phases:

1. **Setup** -- Create a dedicated NFS cluster (unique name per run),
   apply gRPC + STATE debug logging via apply_all_configs (single
   redeploy), create exports/mounts, optionally set Lease_Lifetime,
   archive crash state, optionally create CephErrorInjector.
2. **Pre-thrash** -- Write fio integrity baselines, snapshot state via gRPC
   (GetClientIds/GetSessionIds), snapshot Ganesha memory.  If L2: apply
   error injection AFTER baseline capture.  Abort if cluster unhealthy.
3. **Thrash** -- Launch scenario-specific chaos threads.  L1: iptables, tc,
   mount/unmount, export/admin churn.  L2: CephErrorInjector configs active.
   Background health monitor logs cluster state every 30s.
4. **Validation** -- Remove L2 injections (if active), flush iptables and
   tc netem on all nodes, wait 60s for recovery.  Heal stale NFS mounts
   via lazy-unmount + remount (repeated iptables cycling poisons the
   kernel NFS client beyond auto-recovery).  Verify: PG health, daemon
   health, fio integrity on healed mounts, gRPC state convergence,
   memory stabilization (5 RSS samples at 60s intervals for monotonic
   growth detection), negative cid_refcount, no IBMCEPH-13072 log
   flooding, no crashes (ceph crash ls-new + daemon log scan).
5. **Cleanup** -- Flush iptables and tc qdisc, unmount, delete exports,
   delete the per-scenario NFS cluster, restore Ganesha template, final
   crash check.

Failure Stack Diagram
---------------------
    NFS Client (mount/unmount/IO)
        |
        +-- [L1] iptables block/unblock port 2049
        +-- [L1] tc netem delay/jitter
        +-- [L1] mount/unmount/open/lock cycles
        +-- [L1] export CRUD (create/delete/apply)
        +-- [L1] ceph orch restart/redeploy
        |
        v
    NFS-Ganesha
        +-- State Machine (clientid, sessions, locks, delegations)
        |       ^ validated by gRPC GetClientIds/GetSessionIds
        |       ^ validated by Ganesha log counters
        |
        +-- FSAL_CEPH (libcephfs)
                |
                +-- [L2] client_inject_release_failure (cap release fails)
                +-- [L2] client_inject_fixed_oldest_tid (stale TID)
                +-- [L2] client_debug_inject_tick_delay (slow renewals)
                +-- [L2] ms_inject_socket_failures (connection drops)
                +-- [L2] ms_inject_delay_probability (message delays)
                +-- [L2] mds_inject_traceless_reply (missing traces)
                +-- [L2] mds_inject_migrator_session_race (migration)
                +-- [L2] objecter_inject_no_watch_ping (blocklist risk)
                |
                v
            MDS / OSD / RADOS

Implemented Scenarios
---------------------
client_identity_lifecycle
    Model NFSv4 client lifecycle: EXCHANGE_ID, clientid reuse, reconnect
    before/after lease expiry, rapid reconnect loops, grace reclaim.

lease_expiry_under_load
    Shortened Lease_Lifetime with active IO, locks, delegations.  Induce
    expiry via iptables (L1) or cap release failure (L2).

state_cardinality_stress
    Scale matrix: clients x exports x states.  Incremental ramp-up,
    bulk-close, cleanup ordering.  Cap pressure at extreme cardinality (L2).

client_state_churn
    Random mount/unmount/open/lock churn overlapping lease expiry, server
    restart with grace reclaim, and export reload.  BM repro pattern.

export_reorg_state_integrity
    Export apply/delete/recreate during active IO.  Same-path reuse,
    export ID reuse.  MDS subtree migration race (L2).

generative_client_behavior
    Role-based probabilistic ops: reader-heavy, lock-heavy, metadata-heavy,
    flaky (iptables L1 / N/A L2), slow-renew (tc netem L1 / tick delay L2).

admin_op_interleave
    Admin ops (export CRUD, orch restart/redeploy, config set) interleaved
    with active IO.  Grace reclaim verification.

observability_validation
    Continuous state sampling via gRPC + Ganesha log parsing.  Fail on:
    state non-convergence, negative refcounts, memory leak, IBMCEPH-13072.

rapid_reconnect_storms
    All clients simultaneously disconnect and reconnect with staggered
    timing.  Thundering herd at NFS level (L1) and RADOS level (L2).

combined_churn_failure
    Full integration: client churn + admin ops + lease stress + backend
    chaos.  Chaos budget limits concurrent generators.

CONFIGURATION OPTIONS:
======================
- test_scenario: Scenario name from _SCENARIOS dispatch table (REQUIRED)
- injection_layer: nfs_protocol | mds_backend | combined (default: nfs_protocol)
- duration: Thrash phase duration in seconds (default: 300)
- fs_name: CephFS filesystem name (default: cephfs)
- num_clients: Max client nodes to use; actual count capped by available (default: 3)
- num_exports: Number of NFS exports to create (default: 3)
- lease_lifetime: Ganesha Lease_Lifetime override in seconds (default: None, no change)
- reconnect_cycles: Rapid reconnect loop count for S1/S9 (default: 20)
- churn_rate: Client ops per 10s window for churn scenarios (default: 5)
- error_injection: CephErrorInjector config dict for L2 (default: scenario-specific)
    Supports: profile, profile_overrides, configs -- see ceph_error_injector.py
- fio_bs: FIO block size (default: 1M)
- fio_size: FIO file size (default: 128M)
- fio_runtime: FIO runtime in seconds (default: 30)

Note: NFS cluster name is auto-generated (``lc-<hex8>``) per scenario.
Each run creates and tears down its own NFS cluster.  The backing
CephFS filesystem is shared across all scenarios.
"""

import random
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Event, Thread

from nfs_state_lifecycle.nfs_state_lifecycle_workflows import (
    NfsStateLifecycleWorkflows,
    _log_phase,
    _round_robin,
    _sleep_with_stop,
)

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from cli.exceptions import ConfigError
from utility.log import Log

log = Log(__name__)


def _l2_soak_with_io(wf, cfg, assignments, stop_event):
    """Run fio on first mount during L2 injection soak period.

    Without active IO, backend error injection has nothing to disrupt
    and the test produces trivially-passing results.  Uses only the
    first mount to avoid sequential fio multiplying the soak time.
    """
    duration = cfg["duration"]
    log.info("L2 soak: running IO for %ds while injection active", duration)
    if assignments:
        wf.run_fio_on_assignments(
            assignments[:1],
            {**cfg["fio_params"], "runtime": duration},
            stop_event,
        )


def _start_background_io(wf, assignments, fio_params, stop_event):
    """Start a daemon thread running short fio cycles during L1 thrash.

    Runs 30-second fio bursts in a loop, checking ``stop_event`` between
    cycles.  Returns the Thread object.  The thread is daemonic so it
    will not block test exit even if a fio command is mid-flight when
    ``stop_event`` is set.

    Args:
        wf: NfsStateLifecycleWorkflows instance.
        assignments: Mount assignments to run fio on.
        fio_params: Base fio parameters dict (bs, size).
        stop_event: threading.Event -- loop exits when set.

    Returns:
        Thread: The started daemon thread.
    """
    if not assignments:
        return None

    def _io_loop():
        short_params = {**fio_params, "runtime": 30}
        log.info("Background IO: started on %d mount(s)", len(assignments))
        while not stop_event.is_set():
            wf.run_fio_on_assignments(assignments, short_params, stop_event)
        log.info("Background IO: stopped")

    t = Thread(target=_io_loop, daemon=True)
    t.start()
    return t


def _parse_config(config, ceph_cluster):
    """Parse and validate config, resolve cluster objects.

    Note: ``nfs_name`` is NOT read from config.  Each scenario creates
    its own NFS cluster with a unique name generated in _common_setup.
    """
    fs_name = config.get("fs_name", "cephfs")
    duration = int(config.get("duration", 300))
    num_exports = int(config.get("num_exports", 3))
    max_clients = int(config.get("num_clients", 3))

    clients = ceph_cluster.get_nodes("client")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    installers = ceph_cluster.get_nodes("installer")

    if not clients:
        raise ConfigError("At least one client node required")
    if not nfs_nodes:
        raise ConfigError("At least one NFS node required")
    if not installers:
        raise ConfigError("Installer node required")

    clients = clients[:max_clients]
    installer = installers[0]
    nfs_server = nfs_nodes[0].hostname
    nfs_ip = nfs_nodes[0].ip_address

    layer = config.get("injection_layer", "nfs_protocol")
    enable_l1 = layer in ("nfs_protocol", "combined")
    enable_l2 = layer in ("mds_backend", "combined")

    fio_params = {
        "bs": config.get("fio_bs", "1M"),
        "size": config.get("fio_size", "128M"),
        "runtime": int(config.get("fio_runtime", 30)),
    }

    return {
        "nfs_name": None,
        "fs_name": fs_name,
        "duration": duration,
        "num_exports": num_exports,
        "clients": clients,
        "nfs_nodes": nfs_nodes,
        "installer": installer,
        "nfs_server": nfs_server,
        "nfs_ip": nfs_ip,
        "enable_l1": enable_l1,
        "enable_l2": enable_l2,
        "layer": layer,
        "fio_params": fio_params,
    }


def _common_setup(config, ceph_cluster, scenario_name):
    """Common setup for all scenarios.  Returns (wf, cfg, rados_obj, injector).

    Creates a dedicated NFS cluster for this scenario run.  The cluster
    name is ``lc-<8-hex-chars>`` to keep it short and collision-free
    across parallel runs.  The backing CephFS filesystem is shared.
    """
    cfg = _parse_config(config, ceph_cluster)
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)

    wf = NfsStateLifecycleWorkflows(
        installer=cfg["installer"],
        nfs_nodes=cfg["nfs_nodes"],
        clients=cfg["clients"],
        rados_obj=rados_obj,
        ceph_cluster=ceph_cluster,
    )

    nfs_name = f"lc-{uuid.uuid4().hex[:8]}"
    cfg["nfs_name"] = nfs_name

    log.info(
        "\n%s\nNFS STATE LIFECYCLE TEST\n%s\n"
        "Scenario: %s\n"
        "Injection layer: %s (L1=%s, L2=%s)\n"
        "Clients: %d / NFS nodes: %d / Exports: %d\n"
        "Duration: %ds / NFS cluster: %s (created per-scenario)\n%s",
        "=" * 60,
        "=" * 60,
        scenario_name,
        cfg["layer"],
        cfg["enable_l1"],
        cfg["enable_l2"],
        len(cfg["clients"]),
        len(cfg["nfs_nodes"]),
        cfg["num_exports"],
        cfg["duration"],
        nfs_name,
        "=" * 60,
    )

    injector = None
    try:
        wf.create_nfs_cluster(nfs_name, cfg["nfs_server"])
        wf.apply_all_configs(nfs_name)
        if cfg["enable_l2"]:
            injector = wf.create_injector()
    except Exception:
        log.error("Setup failed; attempting cleanup for NFS cluster %s", nfs_name)
        try:
            wf.delete_nfs_cluster(nfs_name)
        except Exception:
            pass
        raise

    return wf, cfg, rados_obj, injector


def _common_validation(wf, cfg, config, injector, assignments, baseline_snap):
    """Common validation logic for all scenarios.  Returns test_failed.

    If an injector is provided and still has active injections, they are
    cleaned up here to allow the cluster to recover before validation.

    Network chaos (iptables, tc) is flushed before the recovery wait to
    ensure mounts have a clean network path.  Stale mounts are healed
    via remount before integrity checks -- repeated iptables cycling can
    poison the kernel NFS client beyond auto-recovery.
    """
    test_failed = False

    if injector:
        wf.cleanup_injection(injector)
        injector._cleaned = True

    wf.flush_iptables_all()
    for client in cfg["clients"]:
        wf.remove_client_latency(client)

    time.sleep(60)
    wf.log_cluster_health()

    stale = wf.check_mount_health(assignments)
    if stale:
        log.warning(
            "[validation] %d stale mount(s) detected, attempting recovery: %s",
            len(stale),
            stale,
        )
        still_stale = wf.heal_stale_mounts(
            assignments, stale, cfg["nfs_server"], cfg["nfs_name"]
        )
        if still_stale:
            log.error(
                "[validation] %d mount(s) still stale after recovery: %s",
                len(still_stale),
                still_stale,
            )
            test_failed = True

    if not wf.verify_data_integrity(assignments):
        log.error("[validation] Data integrity check failed")
        test_failed = True

    if baseline_snap:
        current_snap = wf.sample_state_snapshot(cfg["nfs_name"])
        if not wf.verify_state_convergence(baseline_snap, current_snap):
            test_failed = True

    if not wf.check_memory_stabilization():
        log.error("[validation] Memory leak suspected (RSS growing after I/O stopped)")
        test_failed = True

    if wf.check_cid_refcount_negative(cfg["nfs_name"]):
        log.error("[validation] Negative cid_refcount detected")
        test_failed = True

    if wf.check_ibmceph_13072_flooding(cfg["nfs_name"]):
        log.error("[validation] IBMCEPH-13072 log flooding detected")
        test_failed = True

    if wf.check_crashes("VALIDATION"):
        test_failed = True

    return test_failed


def _common_cleanup(wf, cfg, config, injector, assignments, exports):
    """Common cleanup for all scenarios.

    Tears down in reverse order: injection -> network -> mounts ->
    exports -> template restore (no redeploy) -> crash check -> NFS
    cluster deletion.  Template restore writes the original config
    back to config-key; the next scenario's apply_all_configs picks
    it up.  Crash check runs while the cluster and its daemons still
    exist so logs are accessible.  The NFS cluster created by
    _common_setup is deleted last.
    """
    if not wf or not cfg:
        return
    if injector and not getattr(injector, "_cleaned", False):
        wf.cleanup_injection(injector)

    wf.flush_iptables_all()
    for client in cfg["clients"]:
        wf.remove_client_latency(client)

    wf.unmount_all(assignments)
    if cfg["nfs_name"]:
        wf.delete_exports(cfg["nfs_name"], exports)
        wf.restore_all_config(cfg["nfs_name"])
        if wf.check_crashes("CLEANUP"):
            log.error("[cleanup] Crash detected during cleanup")
        wf.delete_nfs_cluster(cfg["nfs_name"])
    wf.log_cluster_health()


# ---------------------------------------------------------------------------
# Scenario 1: Client Identity Lifecycle
# ---------------------------------------------------------------------------


def _scenario_client_identity_lifecycle(config, ceph_cluster):
    """Validate NFSv4 client identity lifecycle through EXCHANGE_ID.

    Layer 1 (nfs_protocol):
        Reconnect tests via iptables block/unblock and mount/unmount
        cycles.  Validates clientid reuse on same-verifier reconnect,
        new clientid on different-verifier (reboot), session cleanup
        after lease expiry, and rapid reconnect loop stability.

    Layer 2 (mds_backend):
        client_inject_fixed_oldest_tid forces MDS to see Ganesha as
        stuck.  ms_inject_socket_failures causes intermittent FSAL
        disconnects.  Tests clientid state after MDS session loss.

    Combined:
        NFS clients reconnecting while Ganesha's own MDS session is
        being disrupted by backend injection.

    Config keys:
        reconnect_cycles (int): Rapid loop iterations (default: 20)
        lease_lifetime (int): Shortened lease in seconds (default: 30)
    """
    wf = cfg = rados_obj = injector = None
    assignments = []
    exports = []
    test_failed = False

    wf, cfg, rados_obj, injector = _common_setup(
        config, ceph_cluster, "client_identity_lifecycle"
    )

    try:
        _log_phase(0, "SETUP")
        wf.archive_crash_state()

        lease_lt = config.get("lease_lifetime", 30)
        wf.set_lease_lifetime(cfg["nfs_name"], lease_lt)

        exports = wf.create_exports(cfg["nfs_name"], cfg["fs_name"], cfg["num_exports"])
        assignments = wf.mount_exports_round_robin(
            exports, cfg["nfs_name"], cfg["nfs_server"]
        )
        if not assignments:
            raise ConfigError("No mounts established")
        wf.log_cluster_health()

        _log_phase(1, "PRE-THRASH BASELINE")
        wf.write_integrity_baseline(assignments)
        baseline_snap = wf.sample_state_snapshot(cfg["nfs_name"])
        log.info("Baseline: %s", baseline_snap)

        if cfg["enable_l2"] and injector:
            wf.apply_scenario_injection(injector, "client_identity_lifecycle", config)
        wf.log_cluster_health()

        _log_phase(2, "THRASH")
        stop_event = Event()
        _health_thread = wf.start_health_monitor(stop_event)  # noqa: F841
        cycles = int(config.get("reconnect_cycles", 20))

        if cfg["enable_l1"]:
            from nfs_operations import mount_retry

            _io_thread = _start_background_io(  # noqa: F841
                wf,
                assignments[1:],
                cfg["fio_params"],
                stop_event,
            )

            test_client = cfg["clients"][0]
            test_mp = assignments[0]["mount_point"]
            test_export = assignments[0]["export"]["pseudo"]

            log.info("--- Reconnect before lease expiry ---")
            for i in range(min(3, cycles)):
                test_client.exec_command(
                    sudo=True, cmd=f"umount -l {test_mp}", check_ec=False
                )
                time.sleep(2)
                mount_retry(
                    test_client,
                    test_mp,
                    "4.2",
                    "2049",
                    cfg["nfs_server"],
                    test_export,
                )
                log.info("Reconnect cycle %d/%d OK", i + 1, 3)

            log.info("--- Reconnect after lease expiry ---")
            wf.block_client_nfs_traffic(test_client, lease_lt + 10)
            test_client.exec_command(
                sudo=True, cmd=f"umount -l {test_mp}", check_ec=False
            )
            time.sleep(5)
            mount_retry(
                test_client,
                test_mp,
                "4.2",
                "2049",
                cfg["nfs_server"],
                test_export,
            )
            log.info("Post-expiry reconnect OK")

            log.info("--- Rapid reconnect loop (%d cycles) ---", cycles)
            for i in range(cycles):
                test_client.exec_command(
                    sudo=True, cmd=f"umount -l {test_mp}", check_ec=False
                )
                mount_retry(
                    test_client,
                    test_mp,
                    "4.2",
                    "2049",
                    cfg["nfs_server"],
                    test_export,
                )
            log.info("Rapid reconnect loop complete")

        if cfg["enable_l2"]:
            _l2_soak_with_io(wf, cfg, assignments, stop_event)

        stop_event.set()

        _log_phase(3, "VALIDATION")
        test_failed = _common_validation(
            wf, cfg, config, injector, assignments, baseline_snap
        )

        if not test_failed:
            log.info("*** SCENARIO client_identity_lifecycle: PASSED ***")
        else:
            log.error("*** SCENARIO client_identity_lifecycle: FAILED ***")

    except Exception as e:
        log.error("SCENARIO FAILED with exception: %s", e)
        log.exception(e)
        test_failed = True

    finally:
        _log_phase(4, "CLEANUP")
        _common_cleanup(wf, cfg, config, injector, assignments, exports)

    return 1 if test_failed else 0


# ---------------------------------------------------------------------------
# Scenario 2: Lease Expiry Under Load
# ---------------------------------------------------------------------------


def _scenario_lease_expiry_under_load(config, ceph_cluster):
    """Validate NFS state cleanup when leases expire under realistic load.

    Layer 1 (nfs_protocol):
        Blocks NFS traffic on one client via iptables INPUT+OUTPUT on
        port 2049 for lease_time + 5s.  Other clients continue IO.

    Layer 2 (mds_backend):
        Applies client_session_stress profile: cap release failures
        cause caps to accumulate, risking MDS session eviction.

    Combined:
        NFS clients blocked while Ganesha's caps are failing to release.

    Config keys:
        lease_lifetime (int): Shortened lease in seconds (default: 15)
    """
    wf = cfg = rados_obj = injector = None
    assignments = []
    exports = []
    test_failed = False

    wf, cfg, rados_obj, injector = _common_setup(
        config, ceph_cluster, "lease_expiry_under_load"
    )

    try:
        _log_phase(0, "SETUP")
        wf.archive_crash_state()

        lease_lt = int(config.get("lease_lifetime", 15))
        wf.set_lease_lifetime(cfg["nfs_name"], lease_lt)

        exports = wf.create_exports(cfg["nfs_name"], cfg["fs_name"], cfg["num_exports"])
        assignments = wf.mount_exports_round_robin(
            exports, cfg["nfs_name"], cfg["nfs_server"]
        )
        wf.log_cluster_health()

        _log_phase(1, "PRE-THRASH BASELINE")
        wf.write_integrity_baseline(assignments)
        baseline_snap = wf.sample_state_snapshot(cfg["nfs_name"])

        wf.run_fio_on_assignments(assignments, cfg["fio_params"])

        if cfg["enable_l2"] and injector:
            wf.apply_scenario_injection(injector, "lease_expiry_under_load", config)
        wf.log_cluster_health()

        _log_phase(2, "THRASH")
        stop_event = Event()
        _health_thread = wf.start_health_monitor(stop_event)  # noqa: F841

        if cfg["enable_l1"]:
            _io_thread = _start_background_io(  # noqa: F841
                wf,
                assignments,
                cfg["fio_params"],
                stop_event,
            )

            delays = [
                ("before_expiry", max(1, lease_lt - 5)),
                ("at_expiry", lease_lt + 5),
                ("well_past", lease_lt * 2),
            ]
            for label, block_time in delays:
                target_client = _round_robin(cfg["clients"], 0)
                log.info(
                    "--- Lease test: %s (block %ds, lease %ds) ---",
                    label,
                    block_time,
                    lease_lt,
                )
                wf.block_client_nfs_traffic(target_client, block_time)
                time.sleep(10)

        if cfg["enable_l2"]:
            _l2_soak_with_io(wf, cfg, assignments, stop_event)

        stop_event.set()

        _log_phase(3, "VALIDATION")
        test_failed = _common_validation(
            wf, cfg, config, injector, assignments, baseline_snap
        )

        if not test_failed:
            log.info("*** SCENARIO lease_expiry_under_load: PASSED ***")
        else:
            log.error("*** SCENARIO lease_expiry_under_load: FAILED ***")

    except Exception as e:
        log.error("SCENARIO FAILED: %s", e)
        log.exception(e)
        test_failed = True

    finally:
        _log_phase(4, "CLEANUP")
        _common_cleanup(wf, cfg, config, injector, assignments, exports)

    return 1 if test_failed else 0


# ---------------------------------------------------------------------------
# Scenario 3: State Cardinality Stress
# ---------------------------------------------------------------------------


def _scenario_state_cardinality_stress(config, ceph_cluster):
    """Scale matrix: clients x exports x states per client.

    Layer 1 (nfs_protocol):
        Incremental ramp-up of file opens and locks across all clients
        and exports.  Tests refcount correctness at scale.

    Layer 2 (mds_backend):
        client_inject_release_failure at extreme cardinality pushes
        cap pressure toward MDS eviction threshold.

    Config keys:
        num_exports (int): Export count axis (default: 3)
        states_per_client (str): low | medium | extreme (default: medium)
    """
    wf = cfg = rados_obj = injector = None
    assignments = []
    exports = []
    test_failed = False

    wf, cfg, rados_obj, injector = _common_setup(
        config, ceph_cluster, "state_cardinality_stress"
    )

    try:
        _log_phase(0, "SETUP")
        wf.archive_crash_state()

        exports = wf.create_exports(cfg["nfs_name"], cfg["fs_name"], cfg["num_exports"])
        assignments = wf.mount_exports_round_robin(
            exports, cfg["nfs_name"], cfg["nfs_server"]
        )
        wf.log_cluster_health()

        _log_phase(1, "PRE-THRASH BASELINE")
        baseline_snap = wf.sample_state_snapshot(cfg["nfs_name"])
        wf.write_integrity_baseline(assignments)

        if cfg["enable_l2"] and injector:
            wf.apply_scenario_injection(injector, "state_cardinality_stress", config)
        wf.log_cluster_health()

        _log_phase(2, "THRASH")
        cardinality = config.get("states_per_client", "medium")
        file_counts = {"low": 5, "medium": 20, "extreme": 50}
        num_files = file_counts.get(cardinality, 20)
        stop_event = Event()
        _health_thread = wf.start_health_monitor(stop_event)  # noqa: F841

        if cfg["enable_l1"]:
            log.info(
                "--- Ramp-up: %d files per mount, %d mounts ---",
                num_files,
                len(assignments),
            )
            for step in range(1, num_files + 1, max(1, num_files // 5)):
                actual = min(step, num_files)
                for a in assignments:
                    client = a["client"]
                    mp = a["mount_point"]
                    for f_idx in range(actual):
                        fname = f"{mp}/card_{f_idx}"
                        try:
                            client.exec_command(
                                sudo=True,
                                cmd=f"dd if=/dev/zero of={fname} bs=4k count=1 2>/dev/null",
                                check_ec=False,
                                timeout=30,
                            )
                        except Exception:
                            log.warning(
                                "[cardinality] dd timed out on %s (expected under combined injection)",
                                fname,
                            )
                snap = wf.sample_state_snapshot(cfg["nfs_name"])
                log.info("Step %d/%d state: %s", actual, num_files, snap)

            log.info("--- Bulk close (unlink all) ---")
            for a in assignments:
                try:
                    a["client"].exec_command(
                        sudo=True,
                        cmd=f"rm -f {a['mount_point']}/card_*",
                        check_ec=False,
                        timeout=60,
                    )
                except Exception:
                    log.warning(
                        "[cardinality] rm timed out on %s (expected under combined injection)",
                        a["mount_point"],
                    )
            time.sleep(30)

        if cfg["enable_l2"]:
            _l2_soak_with_io(wf, cfg, assignments, stop_event)

        stop_event.set()

        _log_phase(3, "VALIDATION")
        test_failed = _common_validation(
            wf, cfg, config, injector, assignments, baseline_snap
        )

        if not test_failed:
            log.info("*** SCENARIO state_cardinality_stress: PASSED ***")
        else:
            log.error("*** SCENARIO state_cardinality_stress: FAILED ***")

    except Exception as e:
        log.error("SCENARIO FAILED: %s", e)
        log.exception(e)
        test_failed = True

    finally:
        _log_phase(4, "CLEANUP")
        _common_cleanup(wf, cfg, config, injector, assignments, exports)

    return 1 if test_failed else 0


# ---------------------------------------------------------------------------
# Scenario 4: Client State Churn
# ---------------------------------------------------------------------------


def _scenario_client_state_churn(config, ceph_cluster):
    """Random client state churn overlapping with lease expiry and restarts.

    Layer 1 (nfs_protocol):
        Per-client churn threads: random file create/write/read/lock
        cycles.  Overlaps with shortened lease and orch restart.

    Layer 2 (mds_backend):
        network_chaos profile: socket failures + message delays amplify
        churn-induced state instability at the FSAL layer.

    Config keys:
        churn_rate (int): Ops per 10s window per mount (default: 5)
        lease_lifetime (int): Shortened lease (default: 30)
    """
    wf = cfg = rados_obj = injector = None
    assignments = []
    exports = []
    test_failed = False

    wf, cfg, rados_obj, injector = _common_setup(
        config, ceph_cluster, "client_state_churn"
    )

    try:
        _log_phase(0, "SETUP")
        wf.archive_crash_state()

        lease_lt = config.get("lease_lifetime")
        if lease_lt:
            wf.set_lease_lifetime(cfg["nfs_name"], int(lease_lt))

        exports = wf.create_exports(cfg["nfs_name"], cfg["fs_name"], cfg["num_exports"])
        assignments = wf.mount_exports_round_robin(
            exports, cfg["nfs_name"], cfg["nfs_server"]
        )
        wf.log_cluster_health()

        _log_phase(1, "PRE-THRASH BASELINE")
        wf.write_integrity_baseline(assignments)
        baseline_snap = wf.sample_state_snapshot(cfg["nfs_name"])

        if cfg["enable_l2"] and injector:
            wf.apply_scenario_injection(injector, "client_state_churn", config)
        wf.log_cluster_health()

        _log_phase(2, "THRASH")
        stop_event = Event()
        _health_thread = wf.start_health_monitor(stop_event)  # noqa: F841
        churn_rate = int(config.get("churn_rate", 5))

        futures = []
        if cfg["enable_l1"]:
            with ThreadPoolExecutor(max_workers=min(len(assignments), 5)) as executor:
                for a in assignments:
                    futures.append(
                        executor.submit(
                            wf.run_churn_on_mount,
                            a["client"],
                            a["mount_point"],
                            stop_event,
                            churn_rate,
                        )
                    )
                _sleep_with_stop(stop_event, cfg["duration"], step=10)
                stop_event.set()
                for f in as_completed(futures, timeout=60):
                    try:
                        result = f.result()
                        log.info("[churn] Thread result: %s", result)
                    except Exception as e:
                        log.warning("[churn] Thread error: %s", e)
        else:
            _l2_soak_with_io(wf, cfg, assignments, stop_event)
            stop_event.set()

        _log_phase(3, "VALIDATION")
        test_failed = _common_validation(
            wf, cfg, config, injector, assignments, baseline_snap
        )

        if not test_failed:
            log.info("*** SCENARIO client_state_churn: PASSED ***")
        else:
            log.error("*** SCENARIO client_state_churn: FAILED ***")

    except Exception as e:
        log.error("SCENARIO FAILED: %s", e)
        log.exception(e)
        test_failed = True

    finally:
        _log_phase(4, "CLEANUP")
        _common_cleanup(wf, cfg, config, injector, assignments, exports)

    return 1 if test_failed else 0


# ---------------------------------------------------------------------------
# Scenario 5: Export Reorg & State Integrity
# ---------------------------------------------------------------------------


def _scenario_export_reorg_state_integrity(config, ceph_cluster):
    """Export lifecycle: create/delete/recreate with active IO.

    Layer 1 (nfs_protocol):
        Export apply to modify access_type, delete with active IO,
        recreate with same pseudo path, different-pseudo same-path.

    Layer 2 (mds_backend):
        mds_chaos profile: traceless replies + migration race during
        export reorganization.
    """
    wf = cfg = rados_obj = injector = None
    assignments = []
    exports = []
    test_failed = False

    wf, cfg, rados_obj, injector = _common_setup(
        config, ceph_cluster, "export_reorg_state_integrity"
    )

    try:
        _log_phase(0, "SETUP")
        wf.archive_crash_state()
        exports = wf.create_exports(
            cfg["nfs_name"], cfg["fs_name"], max(3, cfg["num_exports"])
        )
        assignments = wf.mount_exports_round_robin(
            exports, cfg["nfs_name"], cfg["nfs_server"]
        )
        wf.log_cluster_health()

        _log_phase(1, "PRE-THRASH BASELINE")
        wf.write_integrity_baseline(assignments)
        baseline_snap = wf.sample_state_snapshot(cfg["nfs_name"])

        if cfg["enable_l2"] and injector:
            wf.apply_scenario_injection(
                injector, "export_reorg_state_integrity", config
            )
        wf.log_cluster_health()

        _log_phase(2, "THRASH")
        stop_event = Event()
        _health_thread = wf.start_health_monitor(stop_event)  # noqa: F841

        if cfg["enable_l1"] and len(exports) >= 2:
            from cli.ceph.ceph import Ceph

            _io_thread = _start_background_io(  # noqa: F841
                wf,
                assignments[:1],
                cfg["fio_params"],
                stop_event,
            )

            target_export = exports[1]
            pseudo = target_export["pseudo"]

            log.info("--- Delete export %s during IO ---", pseudo)
            try:
                Ceph(cfg["installer"]).nfs.export.delete(cfg["nfs_name"], pseudo)
                log.info("Export %s deleted", pseudo)
                time.sleep(10)

                log.info("--- Recreate export %s ---", pseudo)
                Ceph(cfg["installer"]).nfs.export.create(
                    fs_name=cfg["fs_name"],
                    nfs_name=cfg["nfs_name"],
                    nfs_export=pseudo,
                    fs=cfg["fs_name"],
                )
                log.info("Export %s recreated", pseudo)
                time.sleep(10)
            except Exception as e:
                log.warning("Export reorg failed: %s", e)

        if cfg["enable_l2"]:
            _l2_soak_with_io(wf, cfg, assignments, stop_event)

        stop_event.set()

        _log_phase(3, "VALIDATION")
        if cfg["enable_l1"] and len(exports) >= 2:
            surviving = [a for a in assignments if a["export"] != exports[1]]
        else:
            surviving = assignments
        test_failed = _common_validation(
            wf, cfg, config, injector, surviving, baseline_snap
        )

        if not test_failed:
            log.info("*** SCENARIO export_reorg_state_integrity: PASSED ***")
        else:
            log.error("*** SCENARIO export_reorg_state_integrity: FAILED ***")

    except Exception as e:
        log.error("SCENARIO FAILED: %s", e)
        log.exception(e)
        test_failed = True

    finally:
        _log_phase(4, "CLEANUP")
        _common_cleanup(wf, cfg, config, injector, assignments, exports)

    return 1 if test_failed else 0


# ---------------------------------------------------------------------------
# Scenario 6: Generative Client Behavior
# ---------------------------------------------------------------------------


def _scenario_generative_client_behavior(config, ceph_cluster):
    """Role-based probabilistic client behavior generation.

    Layer 1 (nfs_protocol):
        Flaky client: iptables block/unblock cycles (3-10s blocks).
        Slow-renew client: tc netem delay 5000ms +/- 3000ms jitter.
        All roles: probabilistic file ops.

    Layer 2 (mds_backend):
        client_debug_inject_tick_delay=5 for FSAL-level slow renewal.
        network_latency profile for delayed daemon messages.

    Config keys:
        duration (int): How long to run roles (default: 300)
    """
    wf = cfg = rados_obj = injector = None
    assignments = []
    exports = []
    test_failed = False

    wf, cfg, rados_obj, injector = _common_setup(
        config, ceph_cluster, "generative_client_behavior"
    )

    try:
        _log_phase(0, "SETUP")
        wf.archive_crash_state()
        exports = wf.create_exports(cfg["nfs_name"], cfg["fs_name"], cfg["num_exports"])
        assignments = wf.mount_exports_round_robin(
            exports, cfg["nfs_name"], cfg["nfs_server"]
        )
        wf.log_cluster_health()

        _log_phase(1, "PRE-THRASH BASELINE")
        wf.write_integrity_baseline(assignments)
        baseline_snap = wf.sample_state_snapshot(cfg["nfs_name"])

        if cfg["enable_l2"] and injector:
            wf.apply_scenario_injection(injector, "generative_client_behavior", config)
        wf.log_cluster_health()

        _log_phase(2, "THRASH")
        stop_event = Event()
        _health_thread = wf.start_health_monitor(stop_event)  # noqa: F841
        all_roles = [
            "reader_heavy",
            "lock_heavy",
            "metadata_heavy",
            "flaky",
            "slow_renew",
        ]
        n_assign = len(assignments)
        if n_assign < len(all_roles):
            roles = all_roles[:n_assign]
            if n_assign >= 2:
                roles[-1] = "flaky"
            if n_assign >= 3:
                roles[-2] = "slow_renew"
        else:
            roles = [all_roles[i % len(all_roles)] for i in range(n_assign)]

        if cfg["enable_l1"]:
            with ThreadPoolExecutor(max_workers=min(n_assign, 5)) as executor:
                futures = []
                for i, a in enumerate(assignments):
                    role = roles[i]
                    client = a["client"]
                    mp = a["mount_point"]

                    if role == "flaky":

                        def _flaky_loop(c, se):
                            while not se.is_set():
                                block_dur = random.randint(3, 10)
                                wf.block_client_nfs_traffic(c, block_dur)
                                _sleep_with_stop(se, random.randint(15, 30))

                        futures.append(executor.submit(_flaky_loop, client, stop_event))
                    else:
                        if role == "slow_renew":
                            wf.add_client_latency(client, 5000, 3000)
                        futures.append(
                            executor.submit(
                                wf.run_churn_on_mount,
                                client,
                                mp,
                                stop_event,
                                3,
                                role=role,
                            )
                        )
                    log.info("Assigned role '%s' to %s", role, client.hostname)

                _sleep_with_stop(stop_event, cfg["duration"], step=10)
                stop_event.set()
                for f in as_completed(futures, timeout=60):
                    try:
                        f.result()
                    except Exception:
                        pass
        else:
            _l2_soak_with_io(wf, cfg, assignments, stop_event)
            stop_event.set()

        _log_phase(3, "VALIDATION")
        test_failed = _common_validation(
            wf, cfg, config, injector, assignments, baseline_snap
        )

        if not test_failed:
            log.info("*** SCENARIO generative_client_behavior: PASSED ***")
        else:
            log.error("*** SCENARIO generative_client_behavior: FAILED ***")

    except Exception as e:
        log.error("SCENARIO FAILED: %s", e)
        log.exception(e)
        test_failed = True

    finally:
        _log_phase(4, "CLEANUP")
        _common_cleanup(wf, cfg, config, injector, assignments, exports)

    return 1 if test_failed else 0


# ---------------------------------------------------------------------------
# Scenario 7: Admin Op Interleave
# ---------------------------------------------------------------------------


def _scenario_admin_op_interleave(config, ceph_cluster):
    """Admin operations interleaved with active client IO.

    Layer 1 (nfs_protocol):
        Export CRUD, orch restart, orch redeploy, config set -- all
        while fio runs on clients.

    Layer 2 (mds_backend):
        mds_chaos profile + ms_inject_delay during admin ops.
    """
    wf = cfg = rados_obj = injector = None
    assignments = []
    exports = []
    test_failed = False

    wf, cfg, rados_obj, injector = _common_setup(
        config, ceph_cluster, "admin_op_interleave"
    )

    try:
        _log_phase(0, "SETUP")
        wf.archive_crash_state()
        exports = wf.create_exports(cfg["nfs_name"], cfg["fs_name"], cfg["num_exports"])
        assignments = wf.mount_exports_round_robin(
            exports, cfg["nfs_name"], cfg["nfs_server"]
        )
        wf.log_cluster_health()

        _log_phase(1, "PRE-THRASH BASELINE")
        wf.write_integrity_baseline(assignments)
        baseline_snap = wf.sample_state_snapshot(cfg["nfs_name"])

        if cfg["enable_l2"] and injector:
            wf.apply_scenario_injection(injector, "admin_op_interleave", config)
        wf.log_cluster_health()

        _log_phase(2, "THRASH")
        stop_event = Event()
        _health_thread = wf.start_health_monitor(stop_event)  # noqa: F841

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = []
            futures.append(
                executor.submit(
                    wf.run_fio_on_assignments,
                    assignments,
                    {**cfg["fio_params"], "runtime": cfg["duration"]},
                    stop_event,
                )
            )
            if cfg["enable_l1"]:
                futures.append(
                    executor.submit(
                        wf.run_admin_ops,
                        cfg["nfs_name"],
                        cfg["fs_name"],
                        cfg["duration"],
                        stop_event,
                    )
                )

            _sleep_with_stop(stop_event, cfg["duration"], step=10)
            stop_event.set()
            for f in as_completed(futures, timeout=120):
                try:
                    f.result()
                except Exception as e:
                    log.warning("[admin_interleave] Thread error: %s", e)

        _log_phase(3, "VALIDATION")
        test_failed = _common_validation(
            wf, cfg, config, injector, assignments, baseline_snap
        )

        if not test_failed:
            log.info("*** SCENARIO admin_op_interleave: PASSED ***")
        else:
            log.error("*** SCENARIO admin_op_interleave: FAILED ***")

    except Exception as e:
        log.error("SCENARIO FAILED: %s", e)
        log.exception(e)
        test_failed = True

    finally:
        _log_phase(4, "CLEANUP")
        _common_cleanup(wf, cfg, config, injector, assignments, exports)

    return 1 if test_failed else 0


# ---------------------------------------------------------------------------
# Scenario 8: Observability Validation
# ---------------------------------------------------------------------------


def _scenario_observability_validation(config, ceph_cluster):
    """Metrics-driven state tracking with convergence fail conditions.

    Layer 1 (nfs_protocol):
        Moderate churn with periodic state sampling every 10s.

    Layer 2 (mds_backend):
        client_session_stress with periodic state sampling.

    Fail conditions (all layers):
        - gRPC client count does not converge after churn stops
        - Negative cid_refcount detected
        - RSS monotonically growing after I/O stops (leak detection
          via check_memory_stabilization: 5 samples at 60s intervals)
        - IBMCEPH-13072 log flooding
    Informational (logged, not a failure):
        - State add vs deleted delta (active objects expected while
          clients are mounted)
    """
    wf = cfg = rados_obj = injector = None
    assignments = []
    exports = []
    test_failed = False

    wf, cfg, rados_obj, injector = _common_setup(
        config, ceph_cluster, "observability_validation"
    )

    try:
        _log_phase(0, "SETUP")
        wf.archive_crash_state()

        exports = wf.create_exports(cfg["nfs_name"], cfg["fs_name"], cfg["num_exports"])
        assignments = wf.mount_exports_round_robin(
            exports, cfg["nfs_name"], cfg["nfs_server"]
        )
        wf.log_cluster_health()

        _log_phase(1, "PRE-THRASH BASELINE")
        wf.write_integrity_baseline(assignments)
        baseline_snap = wf.sample_state_snapshot(cfg["nfs_name"])
        log.info("Baseline snapshot: %s", baseline_snap)

        if cfg["enable_l2"] and injector:
            wf.apply_scenario_injection(injector, "observability_validation", config)
        wf.log_cluster_health()

        _log_phase(2, "THRASH")
        churn_stop = Event()
        sampler_stop = Event()
        samples = [baseline_snap]

        def _sampler():
            while not sampler_stop.is_set():
                _sleep_with_stop(sampler_stop, 10, step=2)
                if sampler_stop.is_set():
                    break
                snap = wf.sample_state_snapshot(cfg["nfs_name"])
                samples.append(snap)
                log.info("[sampler] %s", snap)

        wf.start_health_monitor(sampler_stop, interval=30)
        _sample_thread = Thread(target=_sampler, daemon=True)  # noqa: F841
        _sample_thread.start()

        if cfg["enable_l1"]:
            with ThreadPoolExecutor(max_workers=3) as executor:
                churn_futures = []
                for a in assignments:
                    churn_futures.append(
                        executor.submit(
                            wf.run_churn_on_mount,
                            a["client"],
                            a["mount_point"],
                            churn_stop,
                            2,
                        )
                    )
                _sleep_with_stop(churn_stop, cfg["duration"], step=10)
                churn_stop.set()
        else:
            _l2_soak_with_io(wf, cfg, assignments, churn_stop)
            churn_stop.set()

        log.info("Churn stopped, sampling continues for 120s convergence window...")
        time.sleep(120)
        sampler_stop.set()

        _log_phase(3, "VALIDATION")
        final_snap = wf.sample_state_snapshot(cfg["nfs_name"])
        samples.append(final_snap)
        log.info("Final snapshot: %s", final_snap)
        log.info("Total samples collected: %d", len(samples))

        test_failed = _common_validation(
            wf, cfg, config, injector, assignments, baseline_snap
        )

        if not test_failed:
            log.info("*** SCENARIO observability_validation: PASSED ***")
        else:
            log.error("*** SCENARIO observability_validation: FAILED ***")

    except Exception as e:
        log.error("SCENARIO FAILED: %s", e)
        log.exception(e)
        test_failed = True

    finally:
        _log_phase(4, "CLEANUP")
        _common_cleanup(wf, cfg, config, injector, assignments, exports)

    return 1 if test_failed else 0


# ---------------------------------------------------------------------------
# Scenario 9: Rapid Reconnect Storms
# ---------------------------------------------------------------------------


def _scenario_rapid_reconnect_storms(config, ceph_cluster):
    """Simultaneous multi-client disconnect/reconnect thundering herd.

    Layer 1 (nfs_protocol):
        All clients iptables block simultaneously, staggered unblock
        (3s/5s/8s per client).  Repeat 10 times.

    Layer 2 (mds_backend):
        ms_inject_socket_failures + objecter_inject_no_watch_ping
        cause RADOS-level connection storms during reconnect.

    Config keys:
        reconnect_cycles (int): Number of storm repetitions (default: 10)
    """
    wf = cfg = rados_obj = injector = None
    assignments = []
    exports = []
    test_failed = False

    wf, cfg, rados_obj, injector = _common_setup(
        config, ceph_cluster, "rapid_reconnect_storms"
    )

    try:
        _log_phase(0, "SETUP")
        wf.archive_crash_state()

        exports = wf.create_exports(cfg["nfs_name"], cfg["fs_name"], cfg["num_exports"])
        assignments = wf.mount_exports_round_robin(
            exports, cfg["nfs_name"], cfg["nfs_server"]
        )
        wf.log_cluster_health()

        _log_phase(1, "PRE-THRASH BASELINE")
        wf.write_integrity_baseline(assignments)
        baseline_snap = wf.sample_state_snapshot(cfg["nfs_name"])

        if cfg["enable_l2"] and injector:
            wf.apply_scenario_injection(injector, "rapid_reconnect_storms", config)
        wf.log_cluster_health()

        _log_phase(2, "THRASH")
        stop_event = Event()
        _health_thread = wf.start_health_monitor(stop_event)  # noqa: F841
        storms = int(config.get("reconnect_cycles", 10))

        if cfg["enable_l1"]:
            _io_thread = _start_background_io(  # noqa: F841
                wf,
                assignments[:1],
                cfg["fio_params"],
                stop_event,
            )

            stagger_delays = [3, 5, 8]
            for storm_idx in range(storms):
                log.info("--- Storm %d/%d ---", storm_idx + 1, storms)
                stagger_map = {}
                for i in range(len(cfg["clients"])):
                    stagger_map[i] = stagger_delays[i % len(stagger_delays)]

                wf.block_all_clients_simultaneously(stagger_map)
                time.sleep(30)

        if cfg["enable_l2"]:
            _l2_soak_with_io(wf, cfg, assignments, stop_event)

        stop_event.set()

        _log_phase(3, "VALIDATION")
        test_failed = _common_validation(
            wf, cfg, config, injector, assignments, baseline_snap
        )

        if not test_failed:
            log.info("*** SCENARIO rapid_reconnect_storms: PASSED ***")
        else:
            log.error("*** SCENARIO rapid_reconnect_storms: FAILED ***")

    except Exception as e:
        log.error("SCENARIO FAILED: %s", e)
        log.exception(e)
        test_failed = True

    finally:
        _log_phase(4, "CLEANUP")
        _common_cleanup(wf, cfg, config, injector, assignments, exports)

    return 1 if test_failed else 0


# ---------------------------------------------------------------------------
# Scenario 10: Combined Churn + Failure
# ---------------------------------------------------------------------------


def _scenario_combined_churn_failure(config, ceph_cluster):
    """Full integration: churn + admin ops + lease stress + backend chaos.

    Layer 1 (nfs_protocol):
        Client churn + admin ops + iptables delayed renewals with
        chaos budget (max 2 active L1 generators).

    Layer 2 (mds_backend):
        multi_layer_chaos profile: network delays + storage errors +
        OSD dispatch delays.

    Combined:
        All chaos axes with chaos budget (max 3 active generators).

    Config keys:
        churn_rate (int): Ops per 10s window (default: 3)
        lease_lifetime (int): Shortened lease (default: 30)
        duration (int): Thrash duration in seconds (default: 600)
    """
    wf = cfg = rados_obj = injector = None
    assignments = []
    exports = []
    test_failed = False

    wf, cfg, rados_obj, injector = _common_setup(
        config, ceph_cluster, "combined_churn_failure"
    )

    try:
        _log_phase(0, "SETUP")
        wf.archive_crash_state()

        lease_lt = config.get("lease_lifetime", 30)
        wf.set_lease_lifetime(cfg["nfs_name"], int(lease_lt))

        exports = wf.create_exports(cfg["nfs_name"], cfg["fs_name"], cfg["num_exports"])
        assignments = wf.mount_exports_round_robin(
            exports, cfg["nfs_name"], cfg["nfs_server"]
        )
        wf.log_cluster_health()

        _log_phase(1, "PRE-THRASH BASELINE")
        wf.write_integrity_baseline(assignments)
        baseline_snap = wf.sample_state_snapshot(cfg["nfs_name"])

        if cfg["enable_l2"] and injector:
            wf.apply_scenario_injection(injector, "combined_churn_failure", config)
        wf.log_cluster_health()

        _log_phase(2, "THRASH")
        stop_event = Event()
        _health_thread = wf.start_health_monitor(stop_event)  # noqa: F841
        churn_rate = int(config.get("churn_rate", 3))

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []

            futures.append(
                executor.submit(
                    wf.run_fio_on_assignments,
                    assignments,
                    {**cfg["fio_params"], "runtime": cfg["duration"]},
                    stop_event,
                )
            )

            if cfg["enable_l1"]:
                for a in assignments[:2]:
                    futures.append(
                        executor.submit(
                            wf.run_churn_on_mount,
                            a["client"],
                            a["mount_point"],
                            stop_event,
                            churn_rate,
                        )
                    )

                futures.append(
                    executor.submit(
                        wf.run_admin_ops,
                        cfg["nfs_name"],
                        cfg["fs_name"],
                        cfg["duration"],
                        stop_event,
                    )
                )

            _sleep_with_stop(stop_event, cfg["duration"], step=10)
            stop_event.set()

            for f in as_completed(futures, timeout=120):
                try:
                    result = f.result()
                    log.info("[combined] Thread result: %s", result)
                except Exception as e:
                    log.warning("[combined] Thread error: %s", e)

        _log_phase(3, "VALIDATION")
        test_failed = _common_validation(
            wf, cfg, config, injector, assignments, baseline_snap
        )

        if not test_failed:
            log.info("*** SCENARIO combined_churn_failure: PASSED ***")
        else:
            log.error("*** SCENARIO combined_churn_failure: FAILED ***")

    except Exception as e:
        log.error("SCENARIO FAILED: %s", e)
        log.exception(e)
        test_failed = True

    finally:
        _log_phase(4, "CLEANUP")
        _common_cleanup(wf, cfg, config, injector, assignments, exports)

    return 1 if test_failed else 0


# ---------------------------------------------------------------------------
# Scenario dispatch table
# ---------------------------------------------------------------------------

_SCENARIOS = {
    "client_identity_lifecycle": _scenario_client_identity_lifecycle,
    "lease_expiry_under_load": _scenario_lease_expiry_under_load,
    "state_cardinality_stress": _scenario_state_cardinality_stress,
    "client_state_churn": _scenario_client_state_churn,
    "export_reorg_state_integrity": _scenario_export_reorg_state_integrity,
    "generative_client_behavior": _scenario_generative_client_behavior,
    "admin_op_interleave": _scenario_admin_op_interleave,
    "observability_validation": _scenario_observability_validation,
    "rapid_reconnect_storms": _scenario_rapid_reconnect_storms,
    "combined_churn_failure": _scenario_combined_churn_failure,
}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def run(ceph_cluster, **kw):
    """Dispatch to the appropriate NFS state lifecycle scenario.

    The ``test_scenario`` config key selects which scenario to run.
    The ``injection_layer`` config key selects the chaos layer:
        - nfs_protocol  (Layer 1, default)
        - mds_backend   (Layer 2)
        - combined      (Layer 1 + Layer 2)

    See module docstring for full configuration reference.
    """
    config = kw.get("config", {})
    if not config:
        raise ConfigError("No config provided")

    scenario = config.get("test_scenario")
    if not scenario:
        raise ConfigError("test_scenario not specified in config")

    handler = _SCENARIOS.get(scenario)
    if not handler:
        valid = ", ".join(sorted(_SCENARIOS.keys()))
        log.error("Unknown test_scenario '%s'. Valid: %s", scenario, valid)
        return 1

    layer = config.get("injection_layer", "nfs_protocol")
    log.info("=== NFS State Lifecycle: %s [%s] ===", scenario, layer)
    return handler(config, ceph_cluster)
