"""
Shared ops for cephadm-agent scale regression TCs.

Keep case logic in dedicated test_agent_scale_*.py modules; put reusable
host/OSD/service helpers and the per-case monitor harness here so new TCs
only add a thin module + suite entry.
"""

from __future__ import annotations

import json
import os
import time
from typing import Any, Callable, Dict, List, Optional

from utility.collate_scale_timings import (
    TIMINGS_AGGREGATE_NAME,
    load_run_timings,
    publish_timings_to_share,
    write_case_timings,
    write_html_report,
)
from cephadm_agent.helpers import log, setup_run, shell
from cephadm_agent.scale_helpers import (
    BackgroundMonitor,
    TimingRecord,
    WorkloadPump,
    list_cluster_hosts,
    online_hostnames,
    osd_up_count,
    utc_now,
)

ExecuteFn = Callable[[Any, Any, Dict[str, Any], List[TimingRecord]], Dict[str, Any]]


def merge_scale_cfg(config: Dict[str, Any]) -> Dict[str, Any]:
    """Merge suite config.scale with lab defaults (~50 OSD hosts, protocol pressure)."""
    scale = dict(config.get("scale") or {})
    defaults = {
        "initial_hosts": 40,
        "initial_osds": 80,
        "expand_hosts": 10,
        "expand_osds": 20,
        "allow_fewer_hosts": True,
        "host_add_mode": "burst",  # burst | sequential
        "host_labels": ["osd", "mgr", "node-exporter", "crash"],
        "osd_service_id": "agent-scale-all",
        "osd_all_available_devices": True,
        "host_timeout": 7200,
        "agent_timeout": 7200,
        "osd_timeout": 10800,
        "mgr_failover_timeout": 180,
        "retarget_timeout": 1800,
        "retarget_sample_limit": 0,  # 0 = all agents
        "health_poll_sec": 30,
        "mgr_rss_poll_sec": 30,
        "fail_on_agent_down": True,
        "max_mgr_rss_growth_kb": 0,  # 0 = report only
        "ensure_monitoring": True,
        "deploy_rgw": True,
        "deploy_nfs": True,
        "deploy_mds": True,
        # Protocol pressure on a modest OSD-host count (see CONTEXT.md).
        "rgw_count": 16,
        "nfs_clusters": 8,
        "mds_count": 8,
        # Optional already-mounted directory (CephFS/NFS) for timing artifacts.
        "timing_share": "",
        "skip_tc1_bringup": False,
        "skip_tc3_if_no_hosts": True,
        # TC4–TC9 knobs (TC7 firewall covered by tier2; not in scale suite)
        "agent_restart_sample": 1,
        "mgr_failover_iterations": 3,
        "auth_check_sample": 3,
        "cert_store_max_entry_bytes": 65536,
        "agent_down_timeout": 180,
        "agent_recover_timeout": 300,
        "workloads": {
            "enable_rados": True,
            "enable_rgw": True,
            "enable_nfs": True,
            "rados_bench_sec": 30,
        },
    }
    defaults.update(scale)
    if "workloads" in scale and isinstance(scale["workloads"], dict):
        w = dict(defaults["workloads"])
        w.update(scale["workloads"])
        defaults["workloads"] = w
    return defaults


def add_hosts_burst(installer, nodes: List, labels: List[str]) -> None:
    """Single orch apply host-spec for all nodes (scale burst)."""
    if not nodes:
        return
    log.info("Applying host burst spec for {0} hosts".format(len(nodes)))
    lines = []
    for node in nodes:
        lines.append("---")
        lines.append("service_type: host")
        lines.append("hostname: {0}".format(node.hostname))
        lines.append("addr: {0}".format(node.ip_address))
        lines.append("labels:")
        for lab in labels:
            lines.append("  - {0}".format(lab))
    content = "\n".join(lines) + "\n"
    remote = "/tmp/agent_scale_hosts.yaml"
    # Real newlines required so remote bash sees a proper heredoc (not literal \n / EOF).
    installer.exec_command(
        sudo=True, cmd="cat > {0} <<'EOF'\n{1}EOF".format(remote, content)
    )
    installer.exec_command(
        sudo=True,
        cmd="cephadm shell --mount {0}:{1} -- ceph orch apply -i {2}".format(
            remote, remote, remote
        ),
    )


def add_hosts_sequential(installer, nodes: List, labels: List[str]) -> None:
    for node in nodes:
        label_args = " ".join(labels) if labels else ""
        cmd = "ceph orch host add {0} {1}".format(node.hostname, node.ip_address)
        if label_args:
            cmd = "{0} {1}".format(cmd, label_args)
        log.info("Adding host {0} ({1})".format(node.hostname, node.ip_address))
        shell(installer, cmd)


def add_hosts(installer, nodes: List, scale: Dict[str, Any]) -> None:
    labels = scale.get("host_labels") or []
    if scale.get("host_add_mode", "burst") == "sequential":
        add_hosts_sequential(installer, nodes, labels)
    else:
        add_hosts_burst(installer, nodes, labels)


def apply_osd_drivegroup(installer, service_id: str, all_devices: bool = True) -> None:
    if not all_devices:
        raise AssertionError("Only all-available-devices OSD DG supported currently")
    spec = (
        "service_type: osd\n"
        "service_id: {0}\n"
        "placement:\n"
        '  host_pattern: "*"\n'
        "spec:\n"
        "  data_devices:\n"
        "    all: true\n"
    ).format(service_id)
    remote = "/tmp/agent_scale_osd_{0}.yaml".format(service_id)
    installer.exec_command(
        sudo=True, cmd="cat > {0} <<'EOF'\n{1}EOF".format(remote, spec)
    )
    log.info("Applying OSD DriveGroup service_id={0}".format(service_id))
    installer.exec_command(
        sudo=True,
        cmd="cephadm shell --mount {0}:{1} -- ceph orch apply -i {2}".format(
            remote, remote, remote
        ),
    )


def ensure_monitoring_stack(installer) -> None:
    """Re-apply monitoring so node-exporter/crash track '*' as the cluster grows."""
    log.info(
        "Ensuring monitoring stack placements "
        "(prometheus/grafana/alertmanager/node-exporter/crash)"
    )
    cmds = [
        "ceph orch apply prometheus --placement=count:1",
        "ceph orch apply grafana --placement=count:1",
        "ceph orch apply alertmanager --placement=count:1",
        'ceph orch apply node-exporter --placement="*"',
        'ceph orch apply crash --placement="*"',
    ]
    for cmd in cmds:
        try:
            shell(installer, cmd)
        except Exception as exc:
            log.warning("monitoring apply failed ({0}): {1}".format(cmd, exc))


def _publish_share(log_dir: Optional[str], scale: Dict[str, Any]) -> None:
    share = (
        scale.get("timing_share") or os.environ.get("CEPHCI_SCALE_TIMING_SHARE") or ""
    )
    share = str(share).strip()
    if not log_dir or not share:
        return
    try:
        copied = publish_timings_to_share(log_dir, share)
        log.info("Copied {0} timing artifact(s) to {1}".format(len(copied), share))
    except Exception as exc:
        log.warning("timing share copy failed ({0}): {1}".format(share, exc))


def _apply_yaml(installer, remote: str, content: str) -> None:
    installer.exec_command(
        sudo=True, cmd="cat > {0} <<'EOF'\n{1}EOF".format(remote, content)
    )
    installer.exec_command(
        sudo=True,
        cmd="cephadm shell --mount {0}:{1} -- ceph orch apply -i {2}".format(
            remote, remote, remote
        ),
    )


def deploy_rgw_nfs(installer, scale: Dict[str, Any]) -> None:
    """Deploy RGW daemons, NFS clusters, and MDS to pressure non-OSD protocols."""
    if scale.get("deploy_rgw", True):
        rgw_count = int(scale.get("rgw_count") or 2)
        try:
            shell(
                installer,
                "ceph orch apply rgw agent-scale --placement=count:{0}".format(
                    rgw_count
                ),
            )
            log.info("Requested RGW agent-scale count={0}".format(rgw_count))
        except Exception as exc:
            log.warning("RGW deploy skipped/failed: {0}".format(exc))
    if scale.get("deploy_nfs", True):
        nfs_clusters = int(scale.get("nfs_clusters") or 1)
        try:
            shell(installer, "ceph osd pool create .nfs 32")
        except Exception:
            pass
        for idx in range(nfs_clusters):
            spec = (
                "service_type: nfs\n"
                "service_id: agent-scale-{0}\n"
                "placement:\n"
                "  count: 1\n"
                "spec:\n"
                "  port: {1}\n"
            ).format(idx, 2049 + idx)
            remote = "/tmp/agent_scale_nfs_{0}.yaml".format(idx)
            try:
                _apply_yaml(installer, remote, spec)
            except Exception as exc:
                log.warning(
                    "NFS cluster agent-scale-{0} skipped/failed: {1}".format(idx, exc)
                )
        log.info("Requested {0} NFS cluster(s)".format(nfs_clusters))
    if scale.get("deploy_mds", True):
        mds_count = int(scale.get("mds_count") or 2)
        try:
            shell(installer, "ceph fs volume create agent-scale")
        except Exception as exc:
            log.warning("cephfs volume create: {0}".format(exc))
        try:
            shell(
                installer,
                "ceph orch apply mds agent-scale --placement=count:{0}".format(
                    mds_count
                ),
            )
            log.info("Requested MDS agent-scale count={0}".format(mds_count))
        except Exception as exc:
            log.warning("MDS deploy skipped/failed: {0}".format(exc))


def ensure_standby_mgr(installer, timeout: float = 300) -> None:
    mgr_out, _ = shell(installer, "ceph orch ps --daemon-type mgr -f json")
    mgrs = [m for m in json.loads(mgr_out) if m.get("status_desc") == "running"]
    if len(mgrs) >= 2:
        log.info("Already have {0} running mgrs".format(len(mgrs)))
        return
    log.info("Applying mgr placement=2 for failover readiness")
    shell(installer, "ceph orch apply mgr --placement=2")
    end = time.time() + timeout
    while time.time() < end:
        mgr_out, _ = shell(installer, "ceph orch ps --daemon-type mgr -f json")
        mgrs = [m for m in json.loads(mgr_out) if m.get("status_desc") == "running"]
        if len(mgrs) >= 2:
            log.info("Standby mgr ready ({0} running)".format(len(mgrs)))
            return
        time.sleep(10)
    raise AssertionError("Could not get 2 running mgrs for failover test")


def cap_host_count(
    requested: int, available: int, allow_fewer: bool, label: str
) -> int:
    if available >= requested:
        return requested
    if allow_fewer and available > 0:
        log.warning(
            "{0}: requested {1} hosts but only {2} available — using {3} (allow_fewer_hosts=true)".format(
                label, requested, available, available
            )
        )
        return available
    raise AssertionError(
        "{0}: need {1} hosts to add, only {2} unused in conf "
        "(set scale.allow_fewer_hosts=true for smaller labs)".format(
            label, requested, available
        )
    )


def _resolve_log_dir(kw: Dict[str, Any]) -> Optional[str]:
    run_config = kw.get("run_config") or {}
    log_dir = run_config.get("log_dir") or kw.get("log_dir")
    if log_dir and os.path.isdir(log_dir):
        return log_dir
    # Fallback: CEPHCI / launcher often leave cwd or explicit env
    env_dir = os.environ.get("CEPHCI_LOG_DIR") or os.environ.get("LOGDIR")
    if env_dir and os.path.isdir(env_dir):
        return env_dir
    return None


def print_report(
    case_id: str,
    timings: List[TimingRecord],
    monitor: BackgroundMonitor,
    results: Dict[str, Any],
    log_dir: Optional[str] = None,
) -> None:
    log.info("=" * 72)
    log.info("CEPHADM AGENT SCALE REGRESSION [{0}] — TIMING REPORT".format(case_id))
    log.info("=" * 72)
    for t in timings:
        log.info("  {0:40s} {1:10.2f}s  meta={2}".format(t.name, t.seconds, t.meta))
    log.info("MGR RSS summary: {0}".format(monitor.rss_summary()))
    downs = monitor.agent_down_events()
    log.info("CEPHADM_AGENT_DOWN samples: {0}".format(len(downs)))
    for d in downs[:20]:
        log.info(
            "  {0} status={1} summary={2}".format(d.ts, d.status, d.agent_down_summary)
        )
    log.info("Results dict: {0}".format(json.dumps(results, indent=2, default=str)))
    log.info("=" * 72)
    if log_dir:
        try:
            case_payload = {
                "case": case_id,
                "started_at": results.get("started_at"),
                "finished_at": results.get("finished_at"),
                "error": results.get("error"),
                "mgr_rss": results.get("mgr_rss") or monitor.rss_summary(),
                "agent_down_events": results.get("agent_down_events", len(downs)),
                "host_count_final": results.get("host_count_final"),
                "osds_final": results.get("osds_final"),
                "timings": [t.as_dict() for t in timings],
            }
            path = write_case_timings(log_dir, case_payload)
            agg = load_run_timings(log_dir)
            html_path = write_html_report(log_dir, agg)
            log.info(
                "Wrote timing artifacts: {0} and {1}/{2}".format(
                    path, log_dir, TIMINGS_AGGREGATE_NAME
                )
            )
            log.info("Timing HTML report: {0}".format(html_path))
        except Exception as exc:
            log.warning("Failed to write timing artifacts: {0}".format(exc))


def run_scale_case(
    ceph_cluster,
    kw: Dict[str, Any],
    *,
    case_id: str,
    execute: ExecuteFn,
    watch_health: bool = False,
    start_workloads: bool = False,
    check_agent_down: bool = False,
) -> int:
    """
    Common CephCI entry harness for one scale TC.

    New TCs: implement execute(...) in a dedicated module and call this from run().
    """
    config, installer = setup_run(ceph_cluster, kw)
    scale = merge_scale_cfg(config)
    timings: List[TimingRecord] = []
    results: Dict[str, Any] = {
        "started_at": utc_now(),
        "case": case_id,
    }
    log_dir = _resolve_log_dir(kw)

    monitor = BackgroundMonitor(
        installer,
        ceph_cluster,
        health_interval=float(scale["health_poll_sec"]),
        rss_interval=float(scale["mgr_rss_poll_sec"]),
        watch_health=True,
        watch_rss=True,
        health_enabled=watch_health,
    )
    pump: Optional[WorkloadPump] = None
    if start_workloads:
        pump = WorkloadPump(installer, ceph_cluster, scale.get("workloads"))

    monitor.start()
    if pump:
        pump.start()
    try:
        results[case_id] = execute(ceph_cluster, installer, scale, timings)

        results["finished_at"] = utc_now()
        results["mgr_rss"] = monitor.rss_summary()
        results["agent_down_events"] = len(monitor.agent_down_events())
        results["hosts_final"] = list(online_hostnames(installer))
        results["host_count_final"] = len(results["hosts_final"])
        results["osds_final"] = osd_up_count(installer)
        results["inventory"] = [
            {
                "hostname": h.get("hostname"),
                "status": h.get("status"),
                "labels": h.get("labels"),
            }
            for h in list_cluster_hosts(installer)
        ]

        print_report(case_id, timings, monitor, results, log_dir=log_dir)
        _publish_share(log_dir, scale)

        downs = monitor.agent_down_events()
        if check_agent_down and scale.get("fail_on_agent_down", True) and downs:
            raise AssertionError(
                "CEPHADM_AGENT_DOWN observed {0} time(s) (poll every {1}s). First: {2}".format(
                    len(downs), scale["health_poll_sec"], downs[0]
                )
            )

        max_growth = int(scale.get("max_mgr_rss_growth_kb") or 0)
        rss = monitor.rss_summary()
        if max_growth > 0 and rss.get("rss_kb_delta", 0) > max_growth:
            raise AssertionError(
                "Active mgr RSS grew by {0} kB (limit {1} kB): {2}".format(
                    rss["rss_kb_delta"], max_growth, rss
                )
            )

        log.info("PASS: cephadm agent scale regression [{0}]".format(case_id))
        return 0
    except Exception as exc:
        results["error"] = str(exc)
        try:
            print_report(case_id, timings, monitor, results, log_dir=log_dir)
            _publish_share(log_dir, scale)
        except Exception:
            pass
        log.error(
            "FAIL: agent scale regression [{0}]: {1}".format(case_id, exc),
            exc_info=True,
        )
        return 1
    finally:
        if pump:
            pump.stop()
        monitor.stop()
