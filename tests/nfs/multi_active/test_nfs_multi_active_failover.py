from datetime import datetime
from threading import Thread
from time import sleep

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.lib.multi_active import (
    NfsMultiActiveCleanup,
    NfsMultiActiveClient,
    NfsMultiActiveConfig,
    NfsMultiActiveDeploy,
    NfsMultiActiveFailover,
    NfsMultiActiveHaproxy,
    init_crash_monitoring,
    session_setup,
    session_teardown,
    wait_for_ceph_health,
)
from tests.nfs.lib.multi_active.placement import (
    FAILOVER_PLACEMENT,
    resolve_placement_hosts,
)
from utility.log import Log

log = Log(__name__)

"""
NFS Multi-Active failover suite (baremetal).

Four sessions (one deploy/teardown each); phases reuse the same cluster within a session.

  Session 1 ``label_single_client`` (TC-1)
    - label_dual_failover: NFS then ingress label drain during single-client background IO

  Session 2 ``label_dual_client`` (TC-2, TC-3)
    - Phase 1 label_dual_failover_dual_io: dual-client IO; primary NFS + VIP ingress drain
    - restore baseline placement + remount
    - Phase 2 label_secondary_failover_dual_io: secondary NFS + standby ingress drain

  Session 3 ``roundtrip_reboot`` (TC-4, TC-5)
    - Phase 1 label_roundtrip_failover: NFS and ingress label out-and-back
    - remount
    - Phase 2 reboot_failover: reboot pinned NFS backend then VIP-holder ingress

  Session 4 ``dual_cluster`` (TC-6)
    - dual_cluster_vip_failover: two HA clusters; sequential ingress failover A then B

Suite config ``io_tool``: ``dd`` (default) or ``fio`` for background IO during failover.
"""

from tests.nfs.lib.multi_active.constants import (
    BG_IO_JOIN_TIMEOUT,
    BG_IO_RESUME_TIMEOUT,
    BG_IO_RUNTIME,
    BG_IO_START_DELAY,
    DEPLOY_TIMEOUT,
    EXPORT_NAME,
    HEALTH_CHECK_INTERVAL,
    MARKER_IO_TIMEOUT,
    MIN_NFS_NODES,
    NFS_MOUNT,
    NFS_MOUNT_A,
    NFS_MOUNT_B,
    NFS_VERSION,
    POLL_INTERVAL_SEC,
    SERVICE_ID_PREFIX,
)

NFS_SERVICE_ID_PREFIX = SERVICE_ID_PREFIX["failover"]

# Failover phase definitions (TC-1..TC-6). Each phase runs inside a session.
_PLACEMENT = FAILOVER_PLACEMENT
_SPARE = FAILOVER_PLACEMENT["spare"]

# Suite config ``vips`` list index (1-based) per session.
_VIP1 = 1
_VIP2 = 2

FAILOVER_WORKFLOWS = {
    "label_dual_failover": {
        "tc": "TC-1",
        "desc": (
            "Single-client background IO with sequential NFS backend then "
            "keepalived VIP-holder ingress label failover."
        ),
        "steps": (
            "Start background IO on the stick-table-pinned primary client",
            "NFS label drain: pinned backend -> spare; orch apply nfs spec; wait IO resume",
            "Ingress label drain: VIP holder -> spare; orch apply ingress spec; wait IO resume",
            "Validate cluster info, orch ps, stick table, and post-failover backends",
            "Cross-client post-failover marker write/read",
        ),
        "ports": (15601, 15701, 19851),
        "placement": _PLACEMENT,
        "failover": {"target": "dual", "spare": _SPARE},
        "vip": _VIP1,
    },
    "label_dual_failover_dual_io": {
        "tc": "TC-2",
        "desc": (
            "Dual-client background IO on distinct stick-table backends; sequential "
            "primary NFS backend then VIP-holder ingress label failover."
        ),
        "steps": (
            "Start background IO on both clients (distinct stick-table backends)",
            "NFS label drain: primary client's pinned backend -> spare; wait IO resume",
            "Ingress label drain: keepalived VIP holder -> spare; wait IO resume",
            "Validate cluster info, orch ps, stick table, and backends",
            "Cross-client post-failover marker write/read",
        ),
        "dual_client_io": True,
        "ports": (16201, 16301, 20001),
        "placement": _PLACEMENT,
        "failover": {"target": "dual", "spare": _SPARE},
        "vip": _VIP2,
    },
    "label_secondary_failover_dual_io": {
        "tc": "TC-3",
        "desc": (
            "Dual-client background IO; drain non-primary NFS backend and "
            "non-VIP-holder (standby) ingress to spare."
        ),
        "steps": (
            "Start background IO on both clients",
            "NFS label drain: secondary (non-primary) backend -> spare; wait IO resume",
            "Ingress label drain: standby ingress (not VIP holder) -> spare; wait IO resume",
            "Validate cluster info, orch ps, stick table, and backends",
            "Cross-client post-failover marker write/read",
        ),
        "dual_client_io": True,
        "ports": (16401, 16501, 20051),
        "placement": _PLACEMENT,
        "failover": {"target": "secondary_nfs_standby_ingress", "spare": _SPARE},
        "vip": _VIP1,
    },
    "label_roundtrip_failover": {
        "tc": "TC-4",
        "desc": (
            "Single-client background IO; NFS backend and ingress VIP-holder "
            "each label-drain to spare and return to the original host."
        ),
        "steps": (
            "Start background IO on stick-table-pinned primary client",
            "NFS roundtrip: pinned backend -> spare -> original; IO resume after each apply",
            "Ingress roundtrip: VIP holder -> spare -> original; IO resume after each apply",
            "Validate cluster info, orch ps, stick table, and backends",
            "Cross-client post-failover marker write/read",
        ),
        "ports": (16601, 16701, 20101),
        "placement": _PLACEMENT,
        "failover": {"target": "roundtrip", "spare": _SPARE},
        "vip": _VIP2,
    },
    "reboot_failover": {
        "tc": "TC-5",
        "desc": (
            "Single-client background IO; reboot stick-table-pinned NFS backend "
            "then reboot keepalived VIP-holder ingress node."
        ),
        "steps": (
            "Start background IO on stick-table-pinned primary client",
            "Reboot pinned NFS backend; poll IO resume from reboot issue; validate cluster",
            "Reboot VIP-holder ingress node; poll IO resume from reboot issue; validate cluster",
            "Cross-client post-failover marker write/read",
        ),
        "ports": (16801, 16901, 20151),
        "placement": _PLACEMENT,
        "failover": {"target": "reboot", "spare": _SPARE},
        "vip": _VIP1,
    },
    "dual_cluster_vip_failover": {
        "tc": "TC-6",
        "desc": (
            "Two NFS multi-active clusters (VIP A and VIP B); client1 IO on cluster A "
            "and client2 IO on cluster B; sequential VIP-holder ingress label failover."
        ),
        "steps": (
            "Deploy cluster A (VIP A) and cluster B (VIP B); mount and start IO on each client",
            "Cluster A: ingress label drain VIP holder -> spare; wait IO resume on client1",
            "Assert cluster B IO still running (peer isolation)",
            "Cluster B: ingress label drain VIP holder -> host drained from cluster A; wait IO resume",
            "Assert cluster A IO still running; complete IO and post-failover markers on both",
        ),
        "dual_cluster": True,
        "ports_a": (17001, 17101, 20201),
        "ports_b": (17201, 17301, 20251),
        "placement": _PLACEMENT,
        "failover": {"target": "dual_cluster_ingress", "spare": _SPARE},
    },
}

# Failover sessions: one deploy/mount/teardown per session; phases share the cluster.
FAILOVER_SESSIONS = [
    {
        "name": "label_single_client",
        "tc": "TC-1",
        "desc": "Single-client sequential NFS + ingress label failover.",
        "vip": _VIP1,
        "ports": (15601, 15701, 19851),
        "phases": ["label_dual_failover"],
    },
    {
        "name": "label_dual_client",
        "tc": "TC-2, TC-3",
        "desc": (
            "Dual-client primary NFS + ingress failover, then secondary NFS + "
            "standby ingress failover (baseline restore + remount between phases)."
        ),
        "vip": _VIP2,
        "ports": (16201, 16301, 20001),
        "phases": [
            "label_dual_failover_dual_io",
            "label_secondary_failover_dual_io",
        ],
        "restore_between_phases": True,
        "remount_between_phases": True,
    },
    {
        "name": "roundtrip_reboot",
        "tc": "TC-4, TC-5",
        "desc": (
            "Label roundtrip (NFS + ingress out-and-back), then reboot pinned "
            "NFS backend and VIP-holder ingress (remount between phases)."
        ),
        "vip": _VIP1,
        "ports": (16801, 16901, 20151),
        "phases": ["label_roundtrip_failover", "reboot_failover"],
        "remount_between_phases": True,
    },
    {
        "name": "dual_cluster",
        "tc": "TC-6",
        "desc": "Two HA clusters with sequential ingress VIP failover on each.",
        "dual_cluster": True,
        "workflow": "dual_cluster_vip_failover",
    },
]


def _poll_interval(config):
    return int(config.get("poll_interval_sec", POLL_INTERVAL_SEC))


def _io_runtime(config):
    return NfsMultiActiveConfig.io_runtime(config, default=BG_IO_RUNTIME)


def _io_tool_label(config):
    return NfsMultiActiveConfig.resolve_io_tool(config)


def _io_mode_label(config, dual_client_io=False, dual_cluster=False):
    tool = _io_tool_label(config)
    if dual_cluster:
        return f"dual-cluster {tool} (client1 on VIP A, client2 on VIP B)"
    if dual_client_io:
        return f"dual-client {tool}"
    return f"single-client {tool}"


def _deploy_kwargs(config, is_first_session):
    """Build deploy kwargs; skip one-time setup on subsequent sessions."""
    kwargs = {
        "deploy_timeout": DEPLOY_TIMEOUT,
        "ingress_poll_interval": _poll_interval(config),
    }
    if not is_first_session:
        kwargs.update(
            {
                "skip_mgr_enable": True,
            }
        )
    return kwargs


def _format_steps(steps):
    return "\n".join(f"    {idx}. {step}" for idx, step in enumerate(steps, start=1))


def _phase_workflow_label(workflow_key):
    wf = FAILOVER_WORKFLOWS[workflow_key]
    return f"{wf.get('tc', '?')} ({workflow_key})"


def _session_phase_summary(session):
    if session.get("dual_cluster"):
        workflow = session["workflow"]
        return _phase_workflow_label(workflow)
    return " -> ".join(_phase_workflow_label(p) for p in session.get("phases", []))


def _log_phase_banner(
    workflow_key, phase_step, phase_total, nfs_count, ingress_count, target, config=None
):
    wf = FAILOVER_WORKFLOWS[workflow_key]
    config = config or {}
    io_mode = _io_mode_label(
        config,
        dual_client_io=wf.get("dual_client_io", False),
        dual_cluster=wf.get("dual_cluster", False),
    )
    fail_types = {
        "dual": "sequential NFS backend then VIP-holder ingress label drain",
        "secondary_nfs_standby_ingress": (
            "non-primary NFS backend then standby ingress label drain"
        ),
        "roundtrip": "NFS and ingress label roundtrip (out to spare and back)",
        "reboot": "stick-table NFS backend reboot then VIP-holder ingress reboot",
        "dual_cluster_ingress": (
            "cluster A VIP-holder ingress drain, then cluster B VIP-holder ingress drain"
        ),
    }
    steps = wf.get("steps") or ()
    steps_block = f"\nSteps:\n{_format_steps(steps)}\n" if steps else "\n"
    log.info(
        "\n" + "=" * 70 + "\n"
        "Phase %d of %d: %s\n"
        "%s\n"
        "IO: %s\n"
        "NFS daemons: %s | Ingress hosts: %s\n"
        "Failover: %s\n"
        f"{steps_block}" + "=" * 70,
        phase_step,
        phase_total,
        _phase_workflow_label(workflow_key),
        wf.get("desc", ""),
        io_mode,
        nfs_count,
        ingress_count,
        fail_types.get(target, target),
    )


def _log_workflow_banner(
    workflow_key, phase_step, phase_total, nfs_count, ingress_count, target, config=None
):
    """Backward-compatible alias for dual-cluster session entry."""
    _log_phase_banner(
        workflow_key, phase_step, phase_total, nfs_count, ingress_count, target, config
    )


def _resolve_placement_hosts(config, nfs_nodes, spare_slice=None):
    """Return hosts for the standard failover-session placement."""
    kwargs = {}
    if spare_slice is not None:
        kwargs["spare_slice"] = spare_slice
    return resolve_placement_hosts(nfs_nodes, FAILOVER_PLACEMENT, config, **kwargs)


def _ingress_hosts_after_failover(ingress_hosts, remove_host, spare_host):
    return [host for host in ingress_hosts if host != remove_host] + [spare_host]


def _client_backend_mappings(haproxy, mounted_clients, spare_host):
    """Return [(client, pinned_backend), ...] for clients not pinned to spare."""
    mappings = []
    for client in mounted_clients:
        backend = haproxy.get_client_backend_hostname(client)
        if backend == spare_host:
            continue
        mappings.append((client, backend))
    if not mappings:
        raise ConfigError(
            f"No mounted client pinned to a non-spare backend (spare={spare_host!r})"
        )
    return mappings


def _log_stick_table_nfs_target(mappings, primary_client, pinned, spare_host):
    """Log NFS failover host derived from HAProxy stick-table pinning."""
    log.info(
        "NFS failover target (HAProxy stick table): primary client %s pinned to "
        "backend %s; label drain %s -> %s",
        primary_client.hostname,
        pinned,
        pinned,
        spare_host,
    )
    for client, backend in mappings:
        if client == primary_client:
            continue
        log.info(
            "  Stick table: client %s pinned to backend %s",
            client.hostname,
            backend,
        )


def _log_vip_ingress_target(vip, active_ingress, spare_host):
    """Log ingress failover host derived from keepalived VIP ownership."""
    log.info(
        "Ingress failover target (keepalived VIP %s): VIP holder %s; "
        "label drain %s -> %s",
        vip,
        active_ingress,
        active_ingress,
        spare_host,
    )


def _log_colocated_nfs_ingress_note(pinned, active_ingress):
    if pinned == active_ingress:
        log.info(
            "Colocated node %s: stick-table NFS backend and keepalived VIP holder "
            "are the same host (selected independently)",
            pinned,
        )


def _resolve_failover_plan(
    haproxy,
    failover,
    ceph_cluster,
    vip,
    mounted_clients,
    spare_host,
    ingress_hosts,
    target,
    dual_client_io=False,
):
    """
    Build the failover plan before starting background IO.

    Primary client (mappings[0]) is stick-table-pinned to the NFS backend being
    removed for nfs/dual. Ingress/dual: ingress target is the keepalived VIP holder
    (``ip addr`` on ingress pool hosts). When ``dual_client_io`` is set, IO runs
    on every mapped client.

    Returns (io_clients, primary_io_client, pinned_backend, removed_nfs_host,
    removed_ingress_host, expected_ingress).
    """
    mappings = _client_backend_mappings(haproxy, mounted_clients, spare_host)
    primary_client, pinned = mappings[0]
    io_clients = (
        [client for client, _ in mappings] if dual_client_io else [primary_client]
    )
    io_hosts = ", ".join(
        f"{client.hostname}->{backend}" for client, backend in mappings
    )

    if target == "nfs":
        _log_stick_table_nfs_target(mappings, primary_client, pinned, spare_host)
        log.info(
            "Failover plan (nfs): background IO on [%s]; NFS-only label drain",
            io_hosts,
        )
        return io_clients, primary_client, pinned, pinned, None, list(ingress_hosts)

    if target == "ingress":
        _log_stick_table_nfs_target(mappings, primary_client, pinned, spare_host)
        active_ingress = failover.get_active_ingress_host(
            ceph_cluster, vip, ingress_hosts, exclude_hosts={spare_host}
        )
        expected_ingress = _ingress_hosts_after_failover(
            ingress_hosts, active_ingress, spare_host
        )
        _log_vip_ingress_target(vip, active_ingress, spare_host)
        log.info(
            "Failover plan (ingress): background IO on [%s]; ingress-only label drain",
            io_hosts,
        )
        return (
            io_clients,
            primary_client,
            pinned,
            None,
            active_ingress,
            expected_ingress,
        )

    if target == "dual":
        _log_stick_table_nfs_target(mappings, primary_client, pinned, spare_host)
        active_ingress = failover.get_active_ingress_host(
            ceph_cluster, vip, ingress_hosts, exclude_hosts={spare_host}
        )
        expected_ingress = _ingress_hosts_after_failover(
            ingress_hosts, active_ingress, spare_host
        )
        _log_vip_ingress_target(vip, active_ingress, spare_host)
        _log_colocated_nfs_ingress_note(pinned, active_ingress)
        log.info(
            "Failover plan (dual): background IO on [%s]; sequential NFS then ingress "
            "label drain (IO resume required between steps)",
            io_hosts,
        )
        return (
            io_clients,
            primary_client,
            pinned,
            pinned,
            active_ingress,
            expected_ingress,
        )

    if target == "secondary_nfs_standby_ingress":
        if not dual_client_io or len(mappings) < 2:
            raise ConfigError(
                "secondary_nfs_standby_ingress requires dual_client_io and "
                "two clients on distinct backends"
            )
        secondary_client, secondary_backend = mappings[1]
        if secondary_backend == pinned:
            raise ConfigError(
                "Both clients pinned to the same NFS backend; need distinct backends"
            )
        standby_ingress = failover.get_standby_ingress_host(
            ceph_cluster, vip, ingress_hosts, exclude_hosts={spare_host}
        )
        expected_ingress = _ingress_hosts_after_failover(
            ingress_hosts, standby_ingress, spare_host
        )
        _log_stick_table_nfs_target(
            mappings, primary_client, secondary_backend, spare_host
        )
        _log_vip_ingress_target(vip, standby_ingress, spare_host)
        log.info(
            "Failover plan (secondary): background IO on both clients; sequential non-primary "
            "NFS then standby ingress label drain (IO resume required between steps)"
        )
        return (
            [client for client, _ in mappings],
            primary_client,
            secondary_backend,
            secondary_backend,
            standby_ingress,
            expected_ingress,
        )

    if target == "roundtrip":
        active_ingress = failover.get_active_ingress_host(
            ceph_cluster, vip, ingress_hosts, exclude_hosts={spare_host}
        )
        log.info(
            "Failover plan (roundtrip): background IO on %s; nfs %s <-> %s; ingress %s <-> %s",
            primary_client.hostname,
            pinned,
            spare_host,
            active_ingress,
            spare_host,
        )
        return (
            [primary_client],
            primary_client,
            pinned,
            pinned,
            active_ingress,
            list(ingress_hosts),
        )

    if target == "reboot":
        active_ingress = failover.get_active_ingress_host(
            ceph_cluster, vip, ingress_hosts, exclude_hosts={spare_host}
        )
        log.info(
            "Failover plan (reboot): background IO on %s; reboot nfs %s then ingress %s",
            primary_client.hostname,
            pinned,
            active_ingress,
        )
        return (
            [primary_client],
            primary_client,
            pinned,
            pinned,
            active_ingress,
            list(ingress_hosts),
        )

    raise ConfigError(f"Unsupported failover target {target!r}")


def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _duration_since(trigger_time):
    """Return elapsed seconds from a ``%Y-%m-%d %H:%M:%S`` timestamp string."""
    if not trigger_time:
        return None
    try:
        started = datetime.strptime(trigger_time, "%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return None
    return (datetime.now() - started).total_seconds()


def _log_workflow_timings(timings):
    log.info(
        "\n" + "*" * 70 + "\n"
        "Background IO start time: %s\n"
        "Failover triggered at (NFS orch apply): %s\n"
        "IO resumed at (after NFS orch apply): %s\n"
        "IO recovery time after NFS orch apply: %s\n"
        "NFS daemons ready after failover: %s\n"
        "Failover triggered at (ingress orch apply): %s\n"
        "IO resumed at (after ingress orch apply): %s\n"
        "IO recovery time after ingress orch apply: %s\n"
        "Ingress daemons ready after failover: %s\n"
        "Failover triggered at (final orch apply): %s\n"
        "Stick table ready after failover: %s\n"
        "IO resume wait time (final): %s\n"
        "Background IO end time: %s\n" + "*" * 70,
        timings.get("io_start", "N/A"),
        timings.get("nfs_failover_triggered_at", "N/A"),
        timings.get("io_resume_after_nfs_at", "N/A"),
        timings.get("io_resume_after_nfs", "N/A"),
        timings.get("nfs_ready", "N/A"),
        timings.get("ingress_failover_triggered_at", "N/A"),
        timings.get("io_resume_at", "N/A"),
        timings.get("io_resume", "N/A"),
        timings.get("ingress_ready", "N/A"),
        timings.get("failover_triggered_at", "N/A"),
        timings.get("stick_table", "N/A"),
        timings.get("io_resume_final", timings.get("io_resume", "N/A")),
        timings.get("io_end", "N/A"),
    )


def _wait_io_resume(
    config,
    dual_client_io,
    io_sessions,
    io_thread,
    io_error_box,
    primary_io_client,
    io_dir,
    timings,
    timings_key="io_resume",
    trigger_timing_key=None,
    stage="failover",
):
    """Wait for background IO (dd or fio) to resume immediately after orch apply."""
    io_tool = _io_tool_label(config)
    log.info(
        "Verifying background %s IO has resumed before continuing (%s)",
        io_tool,
        stage,
    )
    resume_timings = {}
    triggered_at = (
        timings.get(trigger_timing_key) if timings and trigger_timing_key else None
    )
    if dual_client_io:
        if not io_sessions:
            log.info(
                "No dual-client %s sessions; skipping IO resume wait (%s)",
                io_tool,
                stage,
            )
            if timings is not None:
                timings[timings_key] = f"skipped (no {io_tool} sessions)"
            return
        NfsMultiActiveClient.wait_for_background_io_resume(
            io_sessions,
            NFS_MOUNT,
            config,
            timeout=BG_IO_RESUME_TIMEOUT,
            timings=resume_timings,
            triggered_at=triggered_at,
        )
    elif io_thread is not None and io_thread.is_alive():
        NfsMultiActiveClient.wait_for_background_io_resume(
            [(primary_io_client, io_thread, io_error_box, io_dir)],
            NFS_MOUNT,
            config,
            timeout=BG_IO_RESUME_TIMEOUT,
            timings=resume_timings,
            triggered_at=triggered_at,
        )
    else:
        log.info(
            "Background %s not running; skipping IO resume wait (%s)",
            io_tool,
            stage,
        )
        if timings is not None:
            timings[timings_key] = f"skipped ({io_tool} not running)"
        return

    if timings is None:
        return

    resumed_at = _now()
    timings[f"{timings_key}_at"] = resumed_at
    trigger_time = timings.get(trigger_timing_key) if trigger_timing_key else None
    recovery_seconds = _duration_since(trigger_time)
    if recovery_seconds is not None:
        timings[timings_key] = f"{recovery_seconds:.1f}s"
        log.info(
            "IO resumed at %s; recovery time since trigger (%s): %.1fs",
            resumed_at,
            trigger_timing_key or "trigger",
            recovery_seconds,
        )
    else:
        timings[timings_key] = resume_timings.get("io_resume", "N/A")


def _record_failover_trigger(timings, trigger_timing_key):
    if timings is not None and trigger_timing_key:
        timings[trigger_timing_key] = _now()


def _run_reboot_failover_with_io_resume(
    config,
    dual_client_io,
    io_sessions,
    io_thread,
    io_error_box,
    primary_io_client,
    io_dir,
    timings,
    timings_key,
    trigger_timing_key,
    stage,
    reboot_action,
):
    """Poll IO resume from reboot issue while waiting for node/daemon recovery."""
    _record_failover_trigger(timings, trigger_timing_key)
    io_resume_error = {"exc": None}

    def _io_wait():
        try:
            _wait_io_resume(
                config,
                dual_client_io,
                io_sessions,
                io_thread,
                io_error_box,
                primary_io_client,
                io_dir,
                timings,
                timings_key=timings_key,
                trigger_timing_key=trigger_timing_key,
                stage=stage,
            )
        except Exception as exc:
            io_resume_error["exc"] = exc

    io_wait_thread = Thread(target=_io_wait, daemon=True)
    io_wait_thread.start()
    try:
        reboot_action()
    finally:
        io_wait_thread.join(timeout=BG_IO_RESUME_TIMEOUT + 120)
        if io_wait_thread.is_alive():
            raise OperationFailedError(
                f"IO resume wait did not finish within budget after {stage}"
            )
        if io_resume_error["exc"] is not None:
            raise io_resume_error["exc"]


def _run_reboot_failover_sequence(
    ctx,
    config,
    dual_client_io,
    io_sessions,
    io_thread,
    io_error_box,
    primary_io_client,
    io_dir,
    removed_nfs_host,
    removed_ingress_host,
    expected_ingress,
    timings,
):
    """Reboot NFS backend then ingress with parallel IO resume and per-step validation."""
    _run_reboot_failover_with_io_resume(
        config,
        dual_client_io,
        io_sessions,
        io_thread,
        io_error_box,
        primary_io_client,
        io_dir,
        timings,
        timings_key="io_resume_after_nfs",
        trigger_timing_key="nfs_failover_triggered_at",
        stage="NFS backend reboot",
        reboot_action=lambda: ctx["failover"].reboot_and_wait_nfs(
            ctx["ceph_cluster"],
            ctx["deploy"].validator,
            ctx["nfs_service_id"],
            removed_nfs_host,
            ctx["nfs_count"],
            deploy_timeout=DEPLOY_TIMEOUT,
            timings=timings,
        ),
    )
    _validate_failover_cluster_state(
        ctx["deploy"],
        ctx["haproxy"],
        ctx["nfs_spec"],
        ctx["ingress_spec"],
        ctx["nfs_service_id"],
        expected_ingress,
        ctx["mounted_clients"],
        "reboot",
        ctx["spare_host"],
        removed_nfs_host,
        timings,
        stick_kwargs=ctx["stick_kwargs"],
    )

    _run_reboot_failover_with_io_resume(
        config,
        dual_client_io,
        io_sessions,
        io_thread,
        io_error_box,
        primary_io_client,
        io_dir,
        timings,
        timings_key="io_resume",
        trigger_timing_key="ingress_failover_triggered_at",
        stage="ingress reboot",
        reboot_action=lambda: ctx["failover"].reboot_and_wait_ingress(
            ctx["ceph_cluster"],
            ctx["deploy"].validator,
            ctx["nfs_service_id"],
            removed_ingress_host,
            ctx["vip"],
            ctx["ingress_hosts"],
            len(ctx["ingress_hosts"]),
            deploy_timeout=DEPLOY_TIMEOUT,
            timings=timings,
        ),
    )
    _validate_failover_cluster_state(
        ctx["deploy"],
        ctx["haproxy"],
        ctx["nfs_spec"],
        ctx["ingress_spec"],
        ctx["nfs_service_id"],
        expected_ingress,
        ctx["mounted_clients"],
        "reboot",
        ctx["spare_host"],
        removed_nfs_host,
        timings,
        stick_kwargs=ctx["stick_kwargs"],
    )


def _io_resume_callback(
    config,
    dual_client_io,
    io_sessions,
    io_thread,
    io_error_box,
    primary_io_client,
    io_dir,
    timings,
    timings_key,
    trigger_timing_key,
    stage,
):
    """Return a post-orch-apply callback that waits for IO to resume."""

    def _callback():
        _wait_io_resume(
            config,
            dual_client_io,
            io_sessions,
            io_thread,
            io_error_box,
            primary_io_client,
            io_dir,
            timings,
            timings_key=timings_key,
            trigger_timing_key=trigger_timing_key,
            stage=stage,
        )

    return _callback


def _validate_failover_cluster_state(
    deploy,
    haproxy,
    nfs_spec,
    ingress_spec,
    nfs_service_id,
    expected_ingress,
    mounted_clients,
    target,
    spare_host,
    removed_nfs_host,
    timings,
    stick_kwargs=None,
):
    """Run post-IO-resume cluster, orch, stick-table, and backend validations."""
    cluster_info = deploy.validator.get_cluster_info(nfs_service_id)
    deploy.validator.assert_cluster_info(cluster_info, nfs_spec, ingress_spec)
    deploy.validator.assert_orch_ps(
        nfs_spec,
        ingress_spec,
        expected_ingress_hosts=expected_ingress,
        timeout=DEPLOY_TIMEOUT,
    )
    haproxy.validate_entries(
        mounted_clients=mounted_clients,
        timings=timings,
        **(stick_kwargs or {}),
    )
    backend_hostnames = {
        backend.get("hostname")
        for backend in cluster_info.get("backend") or []
        if backend.get("hostname")
    }
    _assert_post_failover_backends(
        target, spare_host, removed_nfs_host, backend_hostnames
    )
    return cluster_info


def _run_failover_steps(
    target,
    failover,
    deploy,
    ceph_cluster,
    vip,
    orch_client,
    nfs_spec,
    ingress_spec,
    nfs_service_id,
    deploy_nodes,
    nfs_count,
    nfs_label,
    ingress_label,
    ingress_hosts,
    spare_host,
    deploy_timeout,
    removed_nfs_host,
    removed_ingress_host,
    expected_ingress,
    config,
    timings=None,
    dual_client_io=False,
    io_sessions=None,
    io_thread=None,
    io_error_box=None,
    primary_io_client=None,
    io_dir=None,
):
    """Run nfs/ingress label failover, roundtrip, secondary, or reboot steps."""
    io_resume_after_nfs = _io_resume_callback(
        config,
        dual_client_io,
        io_sessions,
        io_thread,
        io_error_box,
        primary_io_client,
        io_dir,
        timings,
        timings_key="io_resume_after_nfs",
        trigger_timing_key="nfs_failover_triggered_at",
        stage="NFS orch apply",
    )
    io_resume_after_ingress = _io_resume_callback(
        config,
        dual_client_io,
        io_sessions,
        io_thread,
        io_error_box,
        primary_io_client,
        io_dir,
        timings,
        timings_key="io_resume",
        trigger_timing_key="ingress_failover_triggered_at",
        stage="ingress orch apply",
    )
    io_resume_after_apply = _io_resume_callback(
        config,
        dual_client_io,
        io_sessions,
        io_thread,
        io_error_box,
        primary_io_client,
        io_dir,
        timings,
        timings_key="io_resume",
        trigger_timing_key="failover_triggered_at",
        stage="orch apply",
    )

    if target == "secondary_nfs_standby_ingress":
        log.info(
            "Starting NFS label failover (secondary backend): drain %s -> %s",
            removed_nfs_host,
            spare_host,
        )
        failover.failover_nfs_by_label(
            orch_client,
            deploy,
            nfs_spec,
            deploy_nodes,
            removed_nfs_host,
            spare_host,
            nfs_label,
            nfs_count,
            deploy_timeout=deploy_timeout,
            timings=timings,
            post_apply_callback=io_resume_after_nfs,
        )
        log.info(
            "Starting ingress label failover (standby): drain %s -> %s",
            removed_ingress_host,
            spare_host,
        )
        failover.failover_ingress_by_label(
            orch_client,
            deploy,
            ingress_spec,
            nfs_service_id,
            deploy_nodes,
            removed_ingress_host,
            spare_host,
            ingress_label,
            len(expected_ingress),
            deploy_timeout=deploy_timeout,
            timings=timings,
            post_apply_callback=io_resume_after_ingress,
        )
        return removed_nfs_host, removed_ingress_host, expected_ingress

    if target == "roundtrip":
        failover.failover_nfs_roundtrip(
            orch_client,
            deploy,
            nfs_spec,
            deploy_nodes,
            removed_nfs_host,
            spare_host,
            nfs_label,
            nfs_count,
            deploy_timeout=deploy_timeout,
            timings=timings,
            post_apply_callback=io_resume_after_apply,
        )
        failover.failover_ingress_roundtrip(
            orch_client,
            deploy,
            ingress_spec,
            nfs_service_id,
            deploy_nodes,
            removed_ingress_host,
            spare_host,
            ingress_label,
            len(ingress_hosts),
            deploy_timeout=deploy_timeout,
            timings=timings,
            post_apply_callback=io_resume_after_apply,
        )
        return removed_nfs_host, removed_ingress_host, list(ingress_hosts)

    if target in ("nfs", "dual"):
        log.info(
            "Starting NFS label failover (stick-table backend): drain %s -> %s",
            removed_nfs_host,
            spare_host,
        )
        failover.failover_nfs_by_label(
            orch_client,
            deploy,
            nfs_spec,
            deploy_nodes,
            removed_nfs_host,
            spare_host,
            nfs_label,
            nfs_count,
            deploy_timeout=deploy_timeout,
            timings=timings,
            post_apply_callback=(
                io_resume_after_nfs if target == "dual" else io_resume_after_apply
            ),
        )

    if target == "dual":
        # Re-resolve VIP holder after NFS failover before ingress drain.
        active_ingress = failover.get_active_ingress_host(
            ceph_cluster, vip, ingress_hosts, exclude_hosts={spare_host}
        )
        expected_ingress = _ingress_hosts_after_failover(
            ingress_hosts, active_ingress, spare_host
        )
        log.info(
            "Starting ingress label failover (VIP holder): drain %s -> %s",
            active_ingress,
            spare_host,
        )
        failover.failover_ingress_by_label(
            orch_client,
            deploy,
            ingress_spec,
            nfs_service_id,
            deploy_nodes,
            active_ingress,
            spare_host,
            ingress_label,
            len(expected_ingress),
            deploy_timeout=deploy_timeout,
            timings=timings,
            post_apply_callback=io_resume_after_ingress,
        )
        removed_ingress_host = active_ingress

    elif target == "ingress":
        log.info(
            "Starting ingress label failover (VIP holder): drain %s -> %s",
            removed_ingress_host,
            spare_host,
        )
        failover.failover_ingress_by_label(
            orch_client,
            deploy,
            ingress_spec,
            nfs_service_id,
            deploy_nodes,
            removed_ingress_host,
            spare_host,
            ingress_label,
            len(expected_ingress),
            deploy_timeout=deploy_timeout,
            timings=timings,
            post_apply_callback=io_resume_after_apply,
        )

    return removed_nfs_host, removed_ingress_host, expected_ingress


def _assert_post_failover_backends(
    target, spare_host, removed_nfs_host, backend_hostnames
):
    if target in ("nfs", "dual", "secondary_nfs_standby_ingress"):
        if spare_host not in backend_hostnames:
            raise ConfigError(
                f"Spare host {spare_host!r} not in cluster info backends: "
                f"{backend_hostnames}"
            )
        if removed_nfs_host in backend_hostnames:
            raise ConfigError(
                f"Failed-out NFS host {removed_nfs_host!r} still in backends"
            )
    elif target == "roundtrip":
        if spare_host in backend_hostnames:
            raise ConfigError(
                f"Spare host {spare_host!r} still in NFS backends after roundtrip"
            )
    elif target == "ingress":
        if spare_host in backend_hostnames:
            raise ConfigError(f"Spare host {spare_host!r} unexpectedly in NFS backends")


def _assert_io_still_running(session, label, config):
    client, thread, error_box, _io_dir = session
    io_tool = _io_tool_label(config)
    if not thread.is_alive():
        exc = error_box.get("exc")
        raise ConfigError(
            f"{label} {io_tool} on {client.hostname} stopped unexpectedly"
            + (f": {exc}" if exc else "")
        )
    log.info(
        "%s %s still running on %s after peer-cluster failover",
        label,
        io_tool,
        client.hostname,
    )


def _validate_cluster_after_ingress_failover(
    deploy,
    nfs_spec,
    ingress_spec,
    nfs_service_id,
    expected_ingress,
    haproxy,
    mounted_clients,
    timings,
):
    cluster_info = deploy.validator.get_cluster_info(nfs_service_id)
    deploy.validator.assert_cluster_info(cluster_info, nfs_spec, ingress_spec)
    deploy.validator.assert_orch_ps(
        nfs_spec,
        ingress_spec,
        expected_ingress_hosts=expected_ingress,
        timeout=DEPLOY_TIMEOUT,
    )
    haproxy.validate_entries(mounted_clients=mounted_clients, timings=timings)


def _log_session_banner(session, session_step, session_total, phase_workflow=None):
    phase_line = ""
    if phase_workflow:
        wf = FAILOVER_WORKFLOWS[phase_workflow]
        phase_line = (
            f"Starting phase: {_phase_workflow_label(phase_workflow)}\n"
            f"{wf.get('desc', '')}\n"
        )
    between = []
    if session.get("restore_between_phases"):
        between.append("restore baseline placement")
    if session.get("remount_between_phases"):
        between.append("remount clients via VIP")
    between_line = ""
    if between and phase_workflow:
        between_line = f"Between-phase actions: {', '.join(between)}\n"
    vip_slot = session.get("vip", "n/a")
    ports = session.get("ports", "dual-cluster ports")
    log.info(
        "\n" + "=" * 70 + "\n"
        "Failover session %d of %d: %s (%s)\n"
        "%s\n"
        "Phases: %s\n"
        "VIP slot: vip%s | ports: %s\n"
        f"{between_line}"
        f"{phase_line}" + "=" * 70,
        session_step,
        session_total,
        session["name"],
        session.get("tc", ""),
        session.get("desc", ""),
        _session_phase_summary(session),
        vip_slot,
        ports,
    )


def _log_suite_start(sessions, config):
    io_tool = _io_tool_label(config or {})
    log.info(
        "\n" + "#" * 70 + "\n"
        "NFS Multi-Active failover suite (%d sessions) | io_tool=%s\n"
        + "\n".join(
            f"  {idx}. {s['name']} ({s.get('tc', '')}): {_session_phase_summary(s)}"
            for idx, s in enumerate(sessions, start=1)
        )
        + "\n"
        + "#" * 70,
        len(sessions),
        io_tool,
    )


def _stop_phase_io(
    config,
    dual_client_io,
    io_sessions,
    io_thread,
    io_error_box,
    primary_io_client,
    io_dir,
):
    """Stop background IO after a failover phase completes."""
    if dual_client_io and io_sessions:
        NfsMultiActiveClient.stop_background_io_on_clients(io_sessions, config)
        for _, thread, _, _ in io_sessions:
            thread.join(timeout=30)
    elif (
        io_thread is not None and io_thread.is_alive() and primary_io_client and io_dir
    ):
        NfsMultiActiveClient.stop_background_io(
            primary_io_client, io_dir, config, io_error_box
        )
        io_thread.join(timeout=30)


def _session_setup(
    ceph_cluster, config, session_name, vip_slot, ports, is_first_workflow
):
    """Deploy NFS multi-active once for a failover session."""
    return session_setup(
        ceph_cluster,
        config,
        session_name,
        vip_slot,
        ports,
        is_first_workflow,
        service_id_prefix=NFS_SERVICE_ID_PREFIX,
        placement=_PLACEMENT,
        spare_slice=_SPARE,
    )


def _session_teardown(ctx):
    session_teardown(ctx)


def _run_failover_phase(ctx, workflow_key, phase_step=1, phase_total=1):
    """Run one failover phase on an existing session cluster (IO + failover + validate)."""
    workflow_cfg = FAILOVER_WORKFLOWS[workflow_key]
    failover_cfg = workflow_cfg["failover"]
    dual_client_io = workflow_cfg.get("dual_client_io", False)
    target = failover_cfg["target"]
    config = ctx["config"]
    io_runtime = _io_runtime(config)
    timings = {}

    _log_phase_banner(
        workflow_key,
        phase_step,
        phase_total,
        ctx["nfs_count"],
        len(ctx["ingress_hosts"]),
        target,
        config,
    )
    log.info(
        "Session %s | phase %s/%s | %s | service=%s | VIP=%s | io_tool=%s",
        ctx["session_name"],
        phase_step,
        phase_total,
        workflow_cfg.get("tc", workflow_key),
        ctx["nfs_service_id"],
        ctx["vip"],
        _io_tool_label(config),
    )

    (
        io_clients,
        primary_io_client,
        _pinned_backend,
        removed_nfs_host,
        removed_ingress_host,
        expected_ingress,
    ) = _resolve_failover_plan(
        ctx["haproxy"],
        ctx["failover"],
        ctx["ceph_cluster"],
        ctx["vip"],
        ctx["mounted_clients"],
        ctx["spare_host"],
        ctx["ingress_hosts"],
        target,
        dual_client_io=dual_client_io,
    )

    io_sessions = None
    io_thread = None
    io_error_box = None
    io_dir = None
    io_subdir = f"{ctx['session_name']}/{workflow_key}"

    if dual_client_io:
        io_sessions = NfsMultiActiveClient.run_background_io_on_clients(
            io_clients,
            NFS_MOUNT,
            config,
            io_subdir=io_subdir,
            runtime=io_runtime,
        )
        io_dir = next(
            session_io_dir
            for client, _, _, session_io_dir in io_sessions
            if client == primary_io_client
        )
    else:
        io_thread, io_error_box, io_dir = NfsMultiActiveClient.run_background_io(
            primary_io_client,
            NFS_MOUNT,
            config,
            io_subdir=io_subdir,
            runtime=io_runtime,
        )
    timings["io_start"] = _now()
    sleep(BG_IO_START_DELAY)

    if target == "reboot":
        _run_reboot_failover_sequence(
            ctx,
            config,
            dual_client_io,
            io_sessions,
            io_thread,
            io_error_box,
            primary_io_client,
            io_dir,
            removed_nfs_host,
            removed_ingress_host,
            expected_ingress,
            timings,
        )
    else:
        removed_nfs_host, removed_ingress_host, expected_ingress = _run_failover_steps(
            target,
            ctx["failover"],
            ctx["deploy"],
            ctx["ceph_cluster"],
            ctx["vip"],
            primary_io_client,
            ctx["nfs_spec"],
            ctx["ingress_spec"],
            ctx["nfs_service_id"],
            ctx["deploy_nodes"],
            ctx["nfs_count"],
            ctx["labels"]["nfs"],
            ctx["labels"]["ingress"],
            ctx["ingress_hosts"],
            ctx["spare_host"],
            DEPLOY_TIMEOUT,
            removed_nfs_host,
            removed_ingress_host,
            expected_ingress,
            config,
            timings=timings,
            dual_client_io=dual_client_io,
            io_sessions=io_sessions,
            io_thread=io_thread,
            io_error_box=io_error_box,
            primary_io_client=primary_io_client,
            io_dir=io_dir,
        )
        _validate_failover_cluster_state(
            ctx["deploy"],
            ctx["haproxy"],
            ctx["nfs_spec"],
            ctx["ingress_spec"],
            ctx["nfs_service_id"],
            expected_ingress,
            ctx["mounted_clients"],
            target,
            ctx["spare_host"],
            removed_nfs_host,
            timings,
            stick_kwargs=ctx["stick_kwargs"],
        )

    if dual_client_io:
        NfsMultiActiveClient.assert_io_completed_on_clients(
            io_sessions,
            nfs_mount=NFS_MOUNT,
            config=config,
            wait_io_resume_timeout=0,
            join_timeout=BG_IO_JOIN_TIMEOUT,
            timings=timings,
        )
    else:
        NfsMultiActiveClient.assert_io_completed(
            io_thread,
            io_error_box,
            config=config,
            stop_client=primary_io_client,
            io_dir=io_dir,
            nfs_mount=NFS_MOUNT,
            wait_io_resume_timeout=0,
            join_timeout=BG_IO_JOIN_TIMEOUT,
            timings=timings,
        )
    timings["io_end"] = _now()

    marker = f"{io_dir}/post-failover-marker"
    marker_client = next(c for c in ctx["mounted_clients"] if c != primary_io_client)
    marker_client.exec_command(
        sudo=True,
        cmd=(
            f"timeout {MARKER_IO_TIMEOUT} sh -c "
            f"'echo failover-ok-{workflow_key} > {marker}'"
        ),
        timeout=MARKER_IO_TIMEOUT + 10,
    )
    out, _ = primary_io_client.exec_command(
        sudo=True,
        cmd=f"timeout {MARKER_IO_TIMEOUT} cat {marker}",
        timeout=MARKER_IO_TIMEOUT + 10,
    )
    if "failover-ok" not in (out or ""):
        raise ConfigError("Cross-client marker read failed after failover")

    _stop_phase_io(
        config,
        dual_client_io,
        io_sessions,
        io_thread,
        io_error_box,
        primary_io_client,
        io_dir,
    )

    log.info(
        "Phase %s (%s) completed in session %s",
        workflow_key,
        workflow_cfg.get("tc", workflow_key),
        ctx["session_name"],
    )
    _log_workflow_timings(timings)
    return 0


def _run_failover_session(
    ceph_cluster, config, session, session_step, session_total, is_first_suite_deploy
):
    """Run one failover session (deploy once, one or more phases)."""
    if session.get("dual_cluster"):
        _log_session_banner(session, session_step, session_total)
        rc = _run_dual_cluster_vip_failover_workflow(
            ceph_cluster,
            config,
            session["workflow"],
            step=session_step,
            total=session_total,
            is_first_workflow=is_first_suite_deploy,
        )
        if rc != 0:
            return rc
        try:
            phase_tc = FAILOVER_WORKFLOWS[session["workflow"]]["tc"]
            wait_for_ceph_health(ceph_cluster, config, context=f"post-{phase_tc}")
        except ConfigError as e:
            log.error("Post-%s health check failed: %s", phase_tc, e)
            return 1
        return 0

    _log_session_banner(session, session_step, session_total)
    ctx = None
    try:
        ctx = _session_setup(
            ceph_cluster,
            config,
            session["name"],
            session["vip"],
            session["ports"],
            is_first_suite_deploy,
        )
        phases = session["phases"]
        for phase_idx, phase_workflow in enumerate(phases):
            if phase_idx > 0:
                if session.get("restore_between_phases"):
                    log.info(
                        "Between phases in session %s: restoring baseline placement",
                        session["name"],
                    )
                    ctx["failover"].restore_baseline_placement(
                        ctx["client1"],
                        ctx["deploy"],
                        ctx["nfs_nodes"],
                        ctx["nfs_spec"],
                        ctx["ingress_spec"],
                        ctx["nfs_service_id"],
                        _PLACEMENT,
                        ctx["labels"],
                        ctx["spare_host"],
                        ctx["nfs_count"],
                        ctx["ingress_hosts"],
                        ctx["deploy_nodes"],
                        deploy_timeout=DEPLOY_TIMEOUT,
                    )
                if session.get("remount_between_phases"):
                    log.info(
                        "Between phases in session %s: remounting clients via VIP",
                        session["name"],
                    )
                    NfsMultiActiveClient.remount_clients_via_vip(
                        ctx["mounted_clients"],
                        ctx["vip"],
                        NFS_MOUNT,
                        EXPORT_NAME,
                        ctx["ingress_frontend_port"],
                        NFS_VERSION,
                    )
                    ctx["haproxy"].validate_entries(
                        mounted_clients=ctx["mounted_clients"],
                        **ctx["stick_kwargs"],
                    )

            _log_session_banner(
                session,
                session_step,
                session_total,
                phase_workflow=phase_workflow,
            )
            _run_failover_phase(
                ctx,
                phase_workflow,
                phase_step=phase_idx + 1,
                phase_total=len(phases),
            )
            try:
                phase_tc = FAILOVER_WORKFLOWS[phase_workflow]["tc"]
                wait_for_ceph_health(ceph_cluster, config, context=f"post-{phase_tc}")
            except ConfigError as e:
                log.error("Post-%s health check failed: %s", phase_tc, e)
                return 1

        log.info("Failover session %s completed successfully", session["name"])
        return 0
    except Exception as e:
        log.error("Failover session %s failed: %s", session["name"], e)
        return 1
    finally:
        if ctx is not None:
            _session_teardown(ctx)


def _run_failover_suite(ceph_cluster, config):
    """Run all failover sessions."""
    sessions = config.get("sessions") or FAILOVER_SESSIONS
    _log_suite_start(sessions, config)
    for session_step, session in enumerate(sessions, start=1):
        if session_step > 1:
            try:
                _stabilize_between_sessions(ceph_cluster, config)
            except ConfigError as e:
                log.error("Inter-session stabilization failed: %s", e)
                return 1
        rc = _run_failover_session(
            ceph_cluster,
            config,
            session,
            session_step,
            len(sessions),
            is_first_suite_deploy=(session_step == 1),
        )
        if rc != 0:
            return rc
    try:
        wait_for_ceph_health(ceph_cluster, config, context="post-suite")
    except ConfigError as e:
        log.error("Post-suite health check failed: %s", e)
        return 1
    return 0


def _run_dual_cluster_vip_failover_workflow(
    ceph_cluster, config, workflow, step=1, total=1, is_first_workflow=True
):
    """Deploy two NFS clusters; fail ingress on A then B with background IO validation."""
    client1, client2 = ceph_cluster.get_nodes("client")[:2]
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    workflow_cfg = FAILOVER_WORKFLOWS[workflow]

    vips = NfsMultiActiveConfig.get_vips(config)
    if len(vips) < 2:
        raise ConfigError(
            "dual_cluster_vip_failover requires at least 2 VIPs in suite config"
        )
    vip_a, vip_b = vips[0], vips[1]

    nfs_hosts, ingress_hosts, deploy_nodes, nfs_count, labels, spare_host = (
        _resolve_placement_hosts(config, nfs_nodes)
    )
    nfs_backend_port_a, ingress_frontend_port_a, ingress_monitor_port_a = workflow_cfg[
        "ports_a"
    ]
    nfs_backend_port_b, ingress_frontend_port_b, ingress_monitor_port_b = workflow_cfg[
        "ports_b"
    ]
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    nfs_service_id_a = f"{NFS_SERVICE_ID_PREFIX}-vip-a-{ts}"
    nfs_service_id_b = f"{NFS_SERVICE_ID_PREFIX}-vip-b-{ts}"
    nfs_label = labels["nfs"]
    ingress_label = labels["ingress"]

    mounted_a = []
    mounted_b = []
    export_a_created = export_b_created = False
    clusters_deployed = False
    labels_applied = False
    io_session_a = io_session_b = None
    io_dir_a = io_dir_b = None
    active_ingress_a = None
    b_failover_spare = None
    timings = {}

    _log_workflow_banner(
        workflow,
        step,
        total,
        nfs_count,
        len(ingress_hosts),
        workflow_cfg["failover"]["target"],
        config,
    )
    log.info(
        "Dual cluster: A=%s VIP=%s mount=%s | B=%s VIP=%s mount=%s | spare=%s | io_tool=%s",
        nfs_service_id_a,
        vip_a,
        NFS_MOUNT_A,
        nfs_service_id_b,
        vip_b,
        NFS_MOUNT_B,
        spare_host,
        _io_tool_label(config),
    )

    try:
        NfsMultiActiveConfig.apply_placement_labels(
            client1,
            nfs_nodes,
            _PLACEMENT["nfs"],
            _PLACEMENT["ingress"],
            labels,
        )
        labels_applied = True

        nfs_spec_a = NfsMultiActiveConfig.build_nfs_spec(
            nfs_hosts,
            nfs_count,
            nfs_service_id_a,
            nfs_backend_port_a,
            label=nfs_label,
        )
        ingress_spec_a = NfsMultiActiveConfig.build_ingress_spec(
            vip_a,
            nfs_service_id_a,
            ingress_frontend_port_a,
            ingress_monitor_port_a,
            config.get("health_check_interval", HEALTH_CHECK_INTERVAL),
            label=ingress_label,
        )
        nfs_spec_b = NfsMultiActiveConfig.build_nfs_spec(
            nfs_hosts,
            nfs_count,
            nfs_service_id_b,
            nfs_backend_port_b,
            label=nfs_label,
        )
        ingress_spec_b = NfsMultiActiveConfig.build_ingress_spec(
            vip_b,
            nfs_service_id_b,
            ingress_frontend_port_b,
            ingress_monitor_port_b,
            config.get("health_check_interval", HEALTH_CHECK_INTERVAL),
            label=ingress_label,
        )
        all_specs = [nfs_spec_a, ingress_spec_a, nfs_spec_b, ingress_spec_b]

        deploy_a = NfsMultiActiveDeploy(
            ceph_cluster, nfs_service_id_a, vip_a, client=client1
        )
        deploy_b = NfsMultiActiveDeploy(
            ceph_cluster, nfs_service_id_b, vip_b, client=client1
        )
        haproxy_a = NfsMultiActiveHaproxy(
            ceph_cluster, nfs_service_id_a, info_client=client1
        )
        haproxy_b = NfsMultiActiveHaproxy(
            ceph_cluster, nfs_service_id_b, info_client=client1
        )
        failover = NfsMultiActiveFailover()
        deploy_kwargs = _deploy_kwargs(config, is_first_workflow)
        stick_kwargs = {"stick_table_interval": _poll_interval(config)}

        clusters_deployed = True
        deploy_a.deploy(
            all_specs,
            deploy_nodes,
            ingress_wait_targets=[
                (nfs_service_id_a, vip_a),
                (nfs_service_id_b, vip_b),
            ],
            cluster_validations=[
                (nfs_spec_a, ingress_spec_a, ingress_hosts),
                (nfs_spec_b, ingress_spec_b, ingress_hosts),
            ],
            **deploy_kwargs,
        )
        haproxy_a.validate_entries(**stick_kwargs)
        haproxy_b.validate_entries(**stick_kwargs)

        for nfs_name in (nfs_service_id_a, nfs_service_id_b):
            Ceph(client1).nfs.export.create(
                fs_name="cephfs",
                nfs_name=nfs_name,
                nfs_export=EXPORT_NAME,
                fs="cephfs",
            )
            NfsMultiActiveConfig.wait_until_export_visible(
                client1,
                nfs_name,
                EXPORT_NAME,
                interval=_poll_interval(config),
            )
        export_a_created = export_b_created = True

        NfsMultiActiveClient.mount_via_vip(
            client1,
            vip_a,
            NFS_MOUNT_A,
            EXPORT_NAME,
            str(ingress_frontend_port_a),
            NFS_VERSION,
        )
        mounted_a.append(client1)
        haproxy_a.validate_entries(mounted_clients=mounted_a, **stick_kwargs)

        NfsMultiActiveClient.mount_via_vip(
            client2,
            vip_b,
            NFS_MOUNT_B,
            EXPORT_NAME,
            str(ingress_frontend_port_b),
            NFS_VERSION,
        )
        mounted_b.append(client2)
        haproxy_b.validate_entries(mounted_clients=mounted_b, **stick_kwargs)

        io_runtime = _io_runtime(config)
        thread_a, error_box_a, io_dir_a = NfsMultiActiveClient.run_background_io(
            client1, NFS_MOUNT_A, config, io_subdir=f"{workflow}/a", runtime=io_runtime
        )
        thread_b, error_box_b, io_dir_b = NfsMultiActiveClient.run_background_io(
            client2, NFS_MOUNT_B, config, io_subdir=f"{workflow}/b", runtime=io_runtime
        )
        io_session_a = (client1, thread_a, error_box_a, io_dir_a)
        io_session_b = (client2, thread_b, error_box_b, io_dir_b)
        client_a, thread_a, error_box_a, io_dir_a = io_session_a
        client_b, thread_b, error_box_b, io_dir_b = io_session_b
        timings["io_start"] = _now()
        sleep(BG_IO_START_DELAY)

        # --- Cluster A ingress failover ---
        active_ingress_a = failover.get_active_ingress_host(
            ceph_cluster, vip_a, ingress_hosts, exclude_hosts={spare_host}
        )
        expected_ingress_a = _ingress_hosts_after_failover(
            ingress_hosts, active_ingress_a, spare_host
        )
        log.info(
            "Cluster A ingress failover (TC-6 step 2): VIP %s holder %s -> spare %s",
            vip_a,
            active_ingress_a,
            spare_host,
        )
        failover.failover_ingress_by_label(
            client1,
            deploy_a,
            ingress_spec_a,
            nfs_service_id_a,
            deploy_nodes,
            active_ingress_a,
            spare_host,
            ingress_label,
            len(expected_ingress_a),
            deploy_timeout=DEPLOY_TIMEOUT,
            timings=timings,
        )
        NfsMultiActiveClient.wait_for_background_io_resume(
            [io_session_a],
            NFS_MOUNT_A,
            config,
            timeout=BG_IO_RESUME_TIMEOUT,
            timings=timings,
        )
        _assert_io_still_running(io_session_b, "Cluster B peer", config)
        _validate_cluster_after_ingress_failover(
            deploy_a,
            nfs_spec_a,
            ingress_spec_a,
            nfs_service_id_a,
            expected_ingress_a,
            haproxy_a,
            mounted_a,
            timings,
        )

        # --- Cluster B ingress failover (spare = host drained from cluster A) ---
        b_failover_spare = active_ingress_a
        active_ingress_b = failover.get_active_ingress_host(
            ceph_cluster,
            vip_b,
            expected_ingress_a,
            exclude_hosts={b_failover_spare},
        )
        expected_ingress_b = _ingress_hosts_after_failover(
            expected_ingress_a, active_ingress_b, b_failover_spare
        )
        log.info(
            "Cluster B ingress failover (TC-6 step 4): VIP %s holder %s -> %s",
            vip_b,
            active_ingress_b,
            b_failover_spare,
        )
        failover.failover_ingress_by_label(
            client1,
            deploy_b,
            ingress_spec_b,
            nfs_service_id_b,
            deploy_nodes,
            active_ingress_b,
            b_failover_spare,
            ingress_label,
            len(expected_ingress_b),
            deploy_timeout=DEPLOY_TIMEOUT,
            timings=timings,
        )
        NfsMultiActiveClient.wait_for_background_io_resume(
            [io_session_b],
            NFS_MOUNT_B,
            config,
            timeout=BG_IO_RESUME_TIMEOUT,
            timings=timings,
        )
        _assert_io_still_running(io_session_a, "Cluster A peer", config)
        _validate_cluster_after_ingress_failover(
            deploy_b,
            nfs_spec_b,
            ingress_spec_b,
            nfs_service_id_b,
            expected_ingress_b,
            haproxy_b,
            mounted_b,
            timings,
        )

        NfsMultiActiveClient.assert_io_completed(
            thread_a,
            error_box_a,
            config=config,
            stop_client=client_a,
            io_dir=io_dir_a,
            nfs_mount=NFS_MOUNT_A,
            wait_io_resume_timeout=BG_IO_RESUME_TIMEOUT,
            join_timeout=BG_IO_JOIN_TIMEOUT,
            timings=timings,
        )
        NfsMultiActiveClient.assert_io_completed(
            thread_b,
            error_box_b,
            config=config,
            stop_client=client_b,
            io_dir=io_dir_b,
            nfs_mount=NFS_MOUNT_B,
            wait_io_resume_timeout=BG_IO_RESUME_TIMEOUT,
            join_timeout=BG_IO_JOIN_TIMEOUT,
            timings=timings,
        )
        timings["io_end"] = _now()

        for marker_client, marker_dir, marker_tag in (
            (client_a, io_dir_a, "a"),
            (client_b, io_dir_b, "b"),
        ):
            marker = f"{marker_dir}/post-failover-marker"
            marker_client.exec_command(
                sudo=True,
                cmd=(
                    f"timeout {MARKER_IO_TIMEOUT} sh -c "
                    f"'echo failover-ok-{workflow}-{marker_tag} > {marker}'"
                ),
                timeout=MARKER_IO_TIMEOUT + 10,
            )
            out, _ = marker_client.exec_command(
                sudo=True,
                cmd=f"timeout {MARKER_IO_TIMEOUT} cat {marker}",
                timeout=MARKER_IO_TIMEOUT + 10,
            )
            if "failover-ok" not in (out or ""):
                raise ConfigError(
                    f"Post-failover marker read failed on cluster {marker_tag.upper()}"
                )

        log.info(
            "Failover session dual_cluster (%s) completed successfully",
            workflow_cfg.get("tc", "TC-6"),
        )
        _log_workflow_timings(timings)
        return 0
    except Exception as e:
        log.error(
            "Failover session dual_cluster (%s) failed: %s",
            workflow_cfg.get("tc", "TC-6"),
            e,
        )
        return 1
    finally:
        for session in (io_session_a, io_session_b):
            if not session:
                continue
            client, thread, error_box, io_dir = session
            try:
                if thread.is_alive():
                    NfsMultiActiveClient.stop_background_io(
                        client, io_dir, config, error_box
                    )
                    thread.join(timeout=30)
            except Exception as e:
                log.warning("Background IO stop failed on %s: %s", client.hostname, e)

        try:
            if mounted_a:
                NfsMultiActiveClient.unmount_and_delete_export(
                    mounted_a, NFS_MOUNT_A, nfs_service_id_a, EXPORT_NAME
                )
            elif export_a_created:
                Ceph(client1).nfs.export.delete(nfs_service_id_a, EXPORT_NAME)
        except Exception as e:
            log.warning("Cluster A mount/export cleanup failed: %s", e)

        try:
            if mounted_b:
                NfsMultiActiveClient.unmount_and_delete_export(
                    mounted_b, NFS_MOUNT_B, nfs_service_id_b, EXPORT_NAME
                )
            elif export_b_created:
                Ceph(client1).nfs.export.delete(nfs_service_id_b, EXPORT_NAME)
        except Exception as e:
            log.warning("Cluster B mount/export cleanup failed: %s", e)

        if clusters_deployed:
            for svc_id in (nfs_service_id_a, nfs_service_id_b):
                try:
                    NfsMultiActiveCleanup(ceph_cluster).remove_cluster(svc_id)
                except Exception as e:
                    log.warning("NFS cluster %s cleanup failed: %s", svc_id, e)

        if labels_applied:
            try:
                NfsMultiActiveConfig.remove_placement_labels(
                    client1,
                    nfs_nodes,
                    _PLACEMENT["nfs"],
                    _PLACEMENT["ingress"],
                    labels,
                )
                for spare_label in (labels["nfs"], labels["ingress"]):
                    client1.exec_command(
                        sudo=True,
                        cmd=f"ceph orch host label rm {spare_host} {spare_label}",
                        check_ec=False,
                    )
                    if b_failover_spare and b_failover_spare != spare_host:
                        client1.exec_command(
                            sudo=True,
                            cmd=(
                                f"ceph orch host label rm {b_failover_spare} "
                                f"{spare_label}"
                            ),
                            check_ec=False,
                        )
            except Exception as e:
                log.warning("Placement label cleanup failed: %s", e)


def _stabilize_between_sessions(ceph_cluster, config=None):
    """Verify Ceph health before the next failover session."""
    wait_for_ceph_health(ceph_cluster, config, context="inter-session")


def run(ceph_cluster, **kw):
    """Run NFS Multi-Active failover sessions."""
    config = kw.get("config") or {}
    init_crash_monitoring(config)
    clients = ceph_cluster.get_nodes("client")
    nfs_nodes = ceph_cluster.get_nodes("nfs")

    if len(clients) < 2:
        raise ConfigError("Failover validation requires at least 2 clients")
    if len(nfs_nodes) < MIN_NFS_NODES:
        raise ConfigError(
            f"Need at least {MIN_NFS_NODES} NFS-capable nodes, found {len(nfs_nodes)}"
        )

    return _run_failover_suite(ceph_cluster, config)
