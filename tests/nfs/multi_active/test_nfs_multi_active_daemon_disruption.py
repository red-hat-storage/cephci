"""NFS Multi-Active daemon disruption during background IO (single deploy, DR-1..DR-7)."""

from time import sleep

from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.lib.multi_active import (
    NfsMultiActiveClient,
    client_backend_mappings,
    init_crash_monitoring,
    io_runtime,
    io_tool_label,
    session_setup,
    session_teardown,
    stop_phase_io,
    wait_for_ceph_health,
)
from tests.nfs.lib.multi_active.constants import (
    BG_IO_RESUME_TIMEOUT,
    DEPLOY_TIMEOUT,
    MARKER_IO_TIMEOUT,
    MIN_NFS_NODES,
    NFS_MOUNT,
    SERVICE_ID_PREFIX,
)
from tests.nfs.lib.multi_active.daemon_disruption import NfsMultiActiveDaemonDisruption
from tests.nfs.multi_active.test_nfs_multi_active_failover import (
    _VIP2,
    _log_workflow_timings,
    _now,
    _validate_failover_cluster_state,
    _wait_io_resume,
)
from utility.log import Log

log = Log(__name__)

NFS_SERVICE_ID_PREFIX = SERVICE_ID_PREFIX["daemon_disruption"]

# Standard 2 NFS + 2 ingress placement; one cluster for all workflows.
_PORTS = (17401, 17501, 20301)
DAEMON_DISRUPTION_IO_WARMUP = 10

# Execution order: least → most disruptive; DR-1..DR-7 match phase sequence in logs.
DAEMON_DISRUPTION_WORKFLOWS = {
    "dr1_nfs_restart_secondary": {
        "tc": "DR-1",
        "desc": "Restart NFS on non-pinned backend; IO should continue.",
        "daemon": "nfs",
        "action": "restart",
        "target": "secondary_nfs",
    },
    "dr2_nfs_restart_pinned": {
        "tc": "DR-2",
        "desc": "Restart NFS on stick-table-pinned backend.",
        "daemon": "nfs",
        "action": "restart",
        "target": "pinned_nfs",
    },
    "dr3_nfs_stop_start_pinned": {
        "tc": "DR-3",
        "desc": "Stop then start NFS on pinned backend.",
        "daemon": "nfs",
        "action": "stop_start",
        "target": "pinned_nfs",
    },
    "dr4_haproxy_restart_standby": {
        "tc": "DR-4",
        "desc": "Restart haproxy on standby ingress; IO should continue.",
        "daemon": "haproxy",
        "action": "restart",
        "target": "standby_ingress",
    },
    "dr5_haproxy_restart_vip": {
        "tc": "DR-5",
        "desc": "Restart haproxy on VIP-holder ingress.",
        "daemon": "haproxy",
        "action": "restart",
        "target": "vip_ingress",
    },
    "dr6_ingress_stop_start_vip": {
        "tc": "DR-6",
        "desc": (
            "Stop then start haproxy and keepalived on VIP-holder ingress; "
            "IO should continue via standby ingress failover."
        ),
        "daemons": ("haproxy", "keepalived"),
        "stop_order": ("haproxy", "keepalived"),
        "start_order": ("keepalived", "haproxy"),
        "action": "ingress_stop_start_failover",
        "target": "vip_ingress",
    },
    "dr7_keepalived_restart_vip": {
        "tc": "DR-7",
        "desc": "Restart keepalived on VIP-holder ingress.",
        "daemon": "keepalived",
        "action": "restart",
        "target": "vip_ingress",
    },
}

DAEMON_DISRUPTION_PHASES = [
    "dr1_nfs_restart_secondary",
    "dr2_nfs_restart_pinned",
    "dr3_nfs_stop_start_pinned",
    "dr4_haproxy_restart_standby",
    "dr5_haproxy_restart_vip",
    "dr6_ingress_stop_start_vip",
    "dr7_keepalived_restart_vip",
]


def _resolve_target_host(ctx, target):
    """Return (hostname, primary_io_client) for the workflow target."""
    failover = ctx["failover"]
    mappings = client_backend_mappings(
        ctx["haproxy"], ctx["mounted_clients"], ctx["spare_host"]
    )
    primary_io_client, pinned_nfs = mappings[0]

    if target == "pinned_nfs":
        return pinned_nfs, primary_io_client

    if target == "secondary_nfs":
        cluster_info = ctx["deploy"].validator.get_cluster_info(ctx["nfs_service_id"])
        backends = {
            backend.get("hostname")
            for backend in cluster_info.get("backend", [])
            if backend.get("hostname")
        }
        secondary = next(
            (host for host in sorted(backends) if host != pinned_nfs),
            None,
        )
        if not secondary:
            raise ConfigError(f"No secondary NFS backend found (pinned={pinned_nfs!r})")
        return secondary, primary_io_client

    if target == "vip_ingress":
        return (
            failover.get_active_ingress_host(
                ctx["ceph_cluster"], ctx["vip"], ctx["ingress_hosts"]
            ),
            primary_io_client,
        )

    if target == "standby_ingress":
        return (
            failover.get_standby_ingress_host(
                ctx["ceph_cluster"], ctx["vip"], ctx["ingress_hosts"]
            ),
            primary_io_client,
        )

    raise ConfigError(f"Unsupported disruption target {target!r}")


def _wait_daemon_healthy(ctx, workflow, timings):
    validator = ctx["deploy"].validator
    nfs_name = ctx["nfs_service_id"]
    if workflow.get("daemon") == "nfs":
        ctx["failover"].wait_nfs_daemons(
            validator,
            nfs_name,
            ctx["nfs_count"],
            timeout=DEPLOY_TIMEOUT,
            timings=timings,
        )
    else:
        ctx["failover"].wait_ingress_daemons(
            validator,
            nfs_name,
            len(ctx["ingress_hosts"]),
            timeout=DEPLOY_TIMEOUT,
            timings=timings,
        )


def _workflow_daemon_label(workflow):
    daemon = workflow.get("daemon")
    if daemon:
        return daemon
    return "+".join(workflow.get("daemons", ()))


def _io_sessions(ctx):
    return [
        (
            ctx["primary_io_client"],
            ctx["io_thread"],
            ctx["io_error_box"],
            ctx["io_dir"],
        )
    ]


def _start_session_io(ctx):
    """Start one background IO stream for the full daemon-disruption session."""
    mappings = client_backend_mappings(
        ctx["haproxy"], ctx["mounted_clients"], ctx["spare_host"]
    )
    primary_io_client, _pinned_nfs = mappings[0]
    io_dir = f"{NFS_MOUNT.rstrip('/')}/{ctx['session_name']}"
    io_thread, io_error_box, io_dir = NfsMultiActiveClient.run_background_io(
        primary_io_client,
        NFS_MOUNT,
        ctx["config"],
        io_subdir=ctx["session_name"],
        runtime=io_runtime(ctx["config"]),
    )
    ctx["primary_io_client"] = primary_io_client
    ctx["io_thread"] = io_thread
    ctx["io_error_box"] = io_error_box
    ctx["io_dir"] = io_dir
    ctx["io_started_at"] = _now()
    log.info(
        "Session background IO started on %s at %s; warming up %ss",
        primary_io_client.hostname,
        io_dir,
        DAEMON_DISRUPTION_IO_WARMUP,
    )
    sleep(DAEMON_DISRUPTION_IO_WARMUP)


def _stop_session_io(ctx):
    """Stop session background IO during teardown."""
    if not ctx.get("io_thread"):
        return
    stop_phase_io(
        ctx["config"],
        False,
        None,
        ctx["io_thread"],
        ctx["io_error_box"],
        ctx["primary_io_client"],
        ctx["io_dir"],
    )
    ctx["io_thread"] = None


def _wait_io_resume_after_action(
    ctx, config, timings, timings_key, trigger_timing_key, stage
):
    _wait_io_resume(
        config,
        False,
        None,
        ctx["io_thread"],
        ctx["io_error_box"],
        ctx["primary_io_client"],
        ctx["io_dir"],
        timings,
        timings_key=timings_key,
        trigger_timing_key=trigger_timing_key,
        stage=stage,
    )


def _stabilize_between_phases(ceph_cluster, config, workflow_tc):
    """Verify Ceph health before the next disruption phase."""
    wait_for_ceph_health(ceph_cluster, config, context=f"post-{workflow_tc}")


def _verify_io_continues(ctx, config, timings, timings_key, stage):
    """Confirm background IO remains responsive (no stall expected)."""
    io_tool = io_tool_label(config)
    log.info(
        "Verifying background %s IO is unaffected while target daemons are stopped (%s)",
        io_tool,
        stage,
    )
    _wait_io_resume(
        config,
        False,
        None,
        ctx["io_thread"],
        ctx["io_error_box"],
        ctx["primary_io_client"],
        ctx["io_dir"],
        timings,
        timings_key=timings_key,
        trigger_timing_key=None,
        stage=stage,
    )


def _run_restart_disruption(
    ctx, workflow, timings, timings_key, trigger_timing_key, stage, target_host
):
    """Restart daemon, wait for target daemon running, wait for health, then confirm IO resume."""
    timings[trigger_timing_key] = _now()
    daemon_name, started_before = NfsMultiActiveDaemonDisruption.apply(
        ctx["client1"],
        ctx["deploy"].validator,
        ctx["nfs_service_id"],
        target_host,
        workflow["daemon"],
        workflow["action"],
    )
    NfsMultiActiveDaemonDisruption.wait_restarted(
        ctx["deploy"].validator,
        ctx["nfs_service_id"],
        target_host,
        workflow["daemon"],
        daemon_name,
        started_before,
        timeout=DEPLOY_TIMEOUT,
    )
    _wait_daemon_healthy(ctx, workflow, timings)
    _wait_io_resume_after_action(
        ctx,
        ctx["config"],
        timings,
        timings_key,
        trigger_timing_key,
        stage,
    )


def _run_stop_start_disruption(ctx, workflow, timings, timings_key, target_host, stage):
    """Stop daemon, confirm IO stall, start daemon, then confirm IO resume."""
    sessions = _io_sessions(ctx)
    start_trigger_key = f"{timings_key}_start_triggered_at"
    validator = ctx["deploy"].validator
    orch_client = ctx["client1"]
    nfs_service_id = ctx["nfs_service_id"]
    component = workflow["daemon"]

    timings[f"{timings_key}_stop_triggered_at"] = _now()
    daemon_name = NfsMultiActiveDaemonDisruption.stop(
        orch_client,
        validator,
        nfs_service_id,
        target_host,
        component,
    )

    NfsMultiActiveClient.wait_for_background_io_stall(
        sessions,
        NFS_MOUNT,
        ctx["config"],
        timeout=BG_IO_RESUME_TIMEOUT,
        timings=timings,
    )
    timings[f"{timings_key}_stalled_at"] = _now()

    timings[start_trigger_key] = _now()
    NfsMultiActiveDaemonDisruption.start(
        orch_client, daemon_name, target_host, component
    )
    NfsMultiActiveDaemonDisruption.wait_running(
        validator,
        nfs_service_id,
        target_host,
        component,
        daemon_name=daemon_name,
        timeout=DEPLOY_TIMEOUT,
    )
    _wait_daemon_healthy(ctx, workflow, timings)
    _wait_io_resume_after_action(
        ctx,
        ctx["config"],
        timings,
        timings_key,
        start_trigger_key,
        f"{stage} resume",
    )


def _run_ingress_failover_stop_start_disruption(
    ctx, workflow, timings, timings_key, target_host, stage
):
    """Stop VIP-holder ingress daemons, confirm stopped + IO continues, then start both."""
    start_trigger_key = f"{timings_key}_start_triggered_at"
    validator = ctx["deploy"].validator
    orch_client = ctx["client1"]
    nfs_service_id = ctx["nfs_service_id"]
    stop_order = workflow["stop_order"]
    start_order = workflow["start_order"]

    timings[f"{timings_key}_stop_triggered_at"] = _now()
    daemon_names = {}
    for component in stop_order:
        daemon_names[component] = NfsMultiActiveDaemonDisruption.stop(
            orch_client,
            validator,
            nfs_service_id,
            target_host,
            component,
        )
        NfsMultiActiveDaemonDisruption.wait_stopped(
            validator,
            nfs_service_id,
            target_host,
            component,
            daemon_name=daemon_names[component],
            timeout=DEPLOY_TIMEOUT,
        )

    _verify_io_continues(
        ctx,
        ctx["config"],
        timings,
        f"{timings_key}_while_stopped",
        f"{stage} while stopped",
    )

    timings[start_trigger_key] = _now()
    for component in start_order:
        daemon_name = daemon_names[component]
        NfsMultiActiveDaemonDisruption.start(
            orch_client, daemon_name, target_host, component
        )
        NfsMultiActiveDaemonDisruption.wait_running(
            validator,
            nfs_service_id,
            target_host,
            component,
            daemon_name=daemon_name,
            timeout=DEPLOY_TIMEOUT,
        )
    _wait_daemon_healthy(ctx, workflow, timings)
    _wait_io_resume_after_action(
        ctx,
        ctx["config"],
        timings,
        timings_key,
        start_trigger_key,
        f"{stage} resume",
    )


def _validate_disruption_cluster_state(ctx, target_host, timings):
    """Post-disruption validation: cluster health, stick table, no label-drain checks."""
    _validate_failover_cluster_state(
        ctx["deploy"],
        ctx["haproxy"],
        ctx["nfs_spec"],
        ctx["ingress_spec"],
        ctx["nfs_service_id"],
        list(ctx["ingress_hosts"]),
        ctx["mounted_clients"],
        "reboot",
        ctx["spare_host"],
        target_host,
        timings,
        stick_kwargs=ctx["stick_kwargs"],
    )


def _run_disruption_phase(ctx, workflow_key, phase_step, phase_total):
    workflow = DAEMON_DISRUPTION_WORKFLOWS[workflow_key]
    config = ctx["config"]
    timings = {"io_start": ctx.get("io_started_at", _now())}
    target_host, _primary_io_client = _resolve_target_host(ctx, workflow["target"])

    if not ctx.get("io_thread") or not ctx["io_thread"].is_alive():
        raise OperationFailedError(
            f"Session background IO is not running before {workflow_key}"
        )

    log.info(
        "\n" + "=" * 70 + "\n"
        "Daemon disruption phase %s/%s | %s | %s on %s | io_tool=%s\n"
        "%s\n" + "=" * 70,
        phase_step,
        phase_total,
        workflow["tc"],
        _workflow_daemon_label(workflow),
        target_host,
        io_tool_label(config),
        workflow["desc"],
    )

    trigger_key = f"disruption_{workflow_key}_triggered_at"
    timings_key = f"io_resume_{workflow_key}"
    stage = f"{workflow['tc']} {_workflow_daemon_label(workflow)} {workflow['action']}"

    if workflow["action"] == "ingress_stop_start_failover":
        _run_ingress_failover_stop_start_disruption(
            ctx, workflow, timings, timings_key, target_host, stage
        )
    elif workflow["action"] == "stop_start":
        _run_stop_start_disruption(
            ctx, workflow, timings, timings_key, target_host, stage
        )
    else:
        _run_restart_disruption(
            ctx, workflow, timings, timings_key, trigger_key, stage, target_host
        )

    _validate_disruption_cluster_state(ctx, target_host, timings)

    marker = f"{ctx['io_dir']}/post-disruption-marker-{workflow_key}"
    marker_client = next(
        c for c in ctx["mounted_clients"] if c != ctx["primary_io_client"]
    )
    marker_client.exec_command(
        sudo=True,
        cmd=(
            f"timeout {MARKER_IO_TIMEOUT} sh -c "
            f"'echo disruption-ok-{workflow_key} > {marker}'"
        ),
        timeout=MARKER_IO_TIMEOUT + 10,
    )
    out, _ = ctx["primary_io_client"].exec_command(
        sudo=True,
        cmd=f"timeout {MARKER_IO_TIMEOUT} cat {marker}",
        timeout=MARKER_IO_TIMEOUT + 10,
    )
    if "disruption-ok" not in (out or ""):
        raise ConfigError(f"Cross-client marker read failed after {workflow_key}")

    timings["io_end"] = _now()
    _log_workflow_timings(timings)
    log.info("Phase %s (%s) completed", workflow_key, workflow["tc"])


def run(ceph_cluster, **kw):
    """Deploy once, run DR-1..DR-7 daemon disruptions, then cleanup."""
    config = kw.get("config") or {}
    init_crash_monitoring(config)
    clients = ceph_cluster.get_nodes("client")
    nfs_nodes = ceph_cluster.get_nodes("nfs")

    if len(clients) < 2:
        raise ConfigError("Daemon disruption requires at least 2 clients")
    if len(nfs_nodes) < MIN_NFS_NODES:
        raise ConfigError(
            f"Need at least {MIN_NFS_NODES} NFS-capable nodes, found {len(nfs_nodes)}"
        )

    phases = config.get("workflows") or DAEMON_DISRUPTION_PHASES
    for workflow_key in phases:
        if workflow_key not in DAEMON_DISRUPTION_WORKFLOWS:
            raise ConfigError(f"Unknown daemon disruption workflow {workflow_key!r}")

    log.info(
        "NFS Multi-Active daemon disruption | %d phase(s) on one cluster | io_tool=%s",
        len(phases),
        io_tool_label(config),
    )

    ctx = None
    try:
        ctx = session_setup(
            ceph_cluster,
            config,
            f"{NFS_SERVICE_ID_PREFIX}-daemon-disruption",
            _VIP2,
            _PORTS,
            is_first_workflow=True,
            service_id_prefix=NFS_SERVICE_ID_PREFIX,
        )
        _start_session_io(ctx)
        for phase_idx, workflow_key in enumerate(phases, start=1):
            _run_disruption_phase(ctx, workflow_key, phase_idx, len(phases))
            workflow_tc = DAEMON_DISRUPTION_WORKFLOWS[workflow_key]["tc"]
            try:
                _stabilize_between_phases(ceph_cluster, config, workflow_tc)
            except ConfigError as e:
                log.error("Post-%s health check failed: %s", workflow_tc, e)
                return 1
        log.info("Daemon disruption session completed successfully")
        return 0
    except Exception as e:
        log.error("Daemon disruption session failed: %s", e)
        return 1
    finally:
        if ctx is not None:
            _stop_session_io(ctx)
            session_teardown(ctx)
        wait_for_ceph_health(ceph_cluster, config, context="post-suite")
