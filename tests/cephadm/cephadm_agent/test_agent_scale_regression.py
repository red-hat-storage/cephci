"""
Cephadm agent scale regression suite (TC1–TC3).

TC1 — Bootstrap already done (1 host, use_agent=true). Add N hosts + M OSDs;
      time host-add→agent-UP and OSD-create→UP; start RGW/NFS workloads.
TC2 — Mgr failover; time agent retarget to new active mgr.
TC3 — Expand by Δ hosts + Δ OSDs; time host-add→agent-UP and OSD→UP.

Throughout TC2+TC3 (not TC1): poll ceph health every 30s for CEPHADM_AGENT_DOWN.
Throughout: sample active mgr RSS.
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, List

from cephadm_agent.helpers import log, setup_run, shell
from cephadm_agent.scale_helpers import (
    BackgroundMonitor,
    TimingRecord,
    WorkloadPump,
    ensure_use_agent,
    get_active_mgr_ip,
    get_active_mgr_name,
    list_cluster_hosts,
    online_hostnames,
    osd_up_count,
    running_agent_hostnames,
    select_nodes_for_add,
    utc_now,
    wait_for_agents_retarget,
    wait_for_agents_running,
    wait_for_hosts_online,
    wait_for_mgr_active_change,
    wait_for_osds_up,
)


def _cfg(config: Dict[str, Any]) -> Dict[str, Any]:
    """Merge suite config with scale defaults (100 hosts / 500 OSDs lab target)."""
    scale = dict(config.get("scale") or {})
    defaults = {
        "initial_hosts": 100,  # additional hosts after bootstrap installer
        "initial_osds": 500,
        "expand_hosts": 50,
        "expand_osds": 250,
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


def _add_hosts_burst(installer, nodes: List, labels: List[str]) -> None:
    """Single orch apply host-spec for all nodes (scale burst)."""
    if not nodes:
        return
    log.info(f"Applying host burst spec for {len(nodes)} hosts")
    lines = []
    for node in nodes:
        lines.append("---")
        lines.append("service_type: host")
        lines.append(f"hostname: {node.hostname}")
        lines.append(f"addr: {node.ip_address}")
        lines.append("labels:")
        for lab in labels:
            lines.append(f"  - {lab}")
    content = "\n".join(lines) + "\n"
    remote = "/tmp/agent_scale_hosts.yaml"
    installer.exec_command(sudo=True, cmd=f"cat > {remote} <<'EOF'\n{content}EOF")
    # Mount /tmp so cephadm shell can read the spec
    installer.exec_command(
        sudo=True,
        cmd=f"cephadm shell --mount {remote}:{remote} -- ceph orch apply -i {remote}",
    )


def _add_hosts_sequential(installer, nodes: List, labels: List[str]) -> None:
    for node in nodes:
        label_args = " ".join(labels) if labels else ""
        cmd = f"ceph orch host add {node.hostname} {node.ip_address}"
        if label_args:
            cmd = f"{cmd} {label_args}"
        log.info(f"Adding host {node.hostname} ({node.ip_address})")
        shell(installer, cmd)


def _apply_osd_drivegroup(installer, service_id: str, all_devices: bool = True) -> None:
    if all_devices:
        spec = f"""service_type: osd
service_id: {service_id}
placement:
  host_pattern: "*"
spec:
  data_devices:
    all: true
"""
    else:
        raise AssertionError("Only all-available-devices OSD DG supported currently")
    remote = f"/tmp/agent_scale_osd_{service_id}.yaml"
    installer.exec_command(sudo=True, cmd=f"cat > {remote} <<'EOF'\n{spec}EOF")
    log.info(f"Applying OSD DriveGroup service_id={service_id}")
    installer.exec_command(
        sudo=True,
        cmd=f"cephadm shell --mount {remote}:{remote} -- ceph orch apply -i {remote}",
    )


def _ensure_monitoring_stack(installer) -> None:
    """
    Keep monitoring stack deployed and covering new hosts.

    Bootstrap (without --skip-monitoring-stack) seeds prometheus/grafana/
    alertmanager/node-exporter. Re-apply so node-exporter/crash track "*" as
    the cluster grows during TC1/TC3.
    """
    log.info(
        "Ensuring monitoring stack placements (prometheus/grafana/alertmanager/node-exporter/crash)"
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
            log.warning(f"monitoring apply failed ({cmd}): {exc}")


def _deploy_rgw_nfs(installer, scale: Dict[str, Any]) -> None:
    if scale.get("deploy_rgw", True):
        try:
            shell(installer, "ceph orch apply rgw agent-scale --placement=count:2")
            log.info("Requested RGW service agent-scale placement=2")
        except Exception as exc:
            log.warning(f"RGW deploy skipped/failed: {exc}")
    if scale.get("deploy_nfs", True):
        try:
            shell(installer, "ceph osd pool create .nfs 32")
        except Exception:
            pass
        nfs_spec = """service_type: nfs
service_id: agent-scale
placement:
  count: 1
spec:
  port: 2049
"""
        remote = "/tmp/agent_scale_nfs.yaml"
        try:
            installer.exec_command(
                sudo=True, cmd=f"cat > {remote} <<'EOF'\n{nfs_spec}EOF"
            )
            installer.exec_command(
                sudo=True,
                cmd=(
                    f"cephadm shell --mount {remote}:{remote} -- "
                    f"ceph orch apply -i {remote}"
                ),
            )
            log.info("Requested NFS service agent-scale")
        except Exception as exc:
            log.warning(f"NFS deploy skipped/failed: {exc}")


def _ensure_standby_mgr(installer, timeout: float = 300) -> None:
    mgr_out, _ = shell(installer, "ceph orch ps --daemon-type mgr -f json")
    mgrs = [m for m in json.loads(mgr_out) if m.get("status_desc") == "running"]
    if len(mgrs) >= 2:
        log.info(f"Already have {len(mgrs)} running mgrs")
        return
    log.info("Applying mgr placement=2 for failover readiness")
    shell(installer, "ceph orch apply mgr --placement=2")
    end = time.time() + timeout
    while time.time() < end:
        mgr_out, _ = shell(installer, "ceph orch ps --daemon-type mgr -f json")
        mgrs = [m for m in json.loads(mgr_out) if m.get("status_desc") == "running"]
        if len(mgrs) >= 2:
            log.info(f"Standby mgr ready ({len(mgrs)} running)")
            return
        time.sleep(10)
    raise AssertionError("Could not get 2 running mgrs for failover test")


def _cap_host_count(
    requested: int, available: int, allow_fewer: bool, label: str
) -> int:
    if available >= requested:
        return requested
    if allow_fewer and available > 0:
        log.warning(
            f"{label}: requested {requested} hosts but only {available} available — "
            f"using {available} (allow_fewer_hosts=true)"
        )
        return available
    raise AssertionError(
        f"{label}: need {requested} hosts to add, only {available} unused in conf "
        f"(set scale.allow_fewer_hosts=true for smaller labs)"
    )


def run_tc1_scale_bringup(
    ceph_cluster, installer, scale: Dict[str, Any], timings: List
):
    log.info("=== TC1: Host + OSD scale bring-up (use_agent) ===")
    ensure_use_agent(installer)

    before_hosts = len(online_hostnames(installer))
    before_osds = osd_up_count(installer)
    log.info(f"Baseline: hosts={before_hosts}, osds_up={before_osds}")

    want = int(scale["initial_hosts"])
    candidates = select_nodes_for_add(ceph_cluster, installer, 0)
    n_add = _cap_host_count(want, len(candidates), scale["allow_fewer_hosts"], "TC1")
    nodes = candidates[:n_add]
    target_hosts = before_hosts + n_add

    # Host add wall clock includes apply + online + agents UP
    host_rec = TimingRecord(name="tc1_host_add_and_agents_up", started_at=utc_now())
    t_host = time.time()

    if scale.get("host_add_mode", "burst") == "sequential":
        _add_hosts_sequential(installer, nodes, scale["host_labels"])
    else:
        _add_hosts_burst(installer, nodes, scale["host_labels"])

    h_online = wait_for_hosts_online(
        installer, target_hosts, timeout=scale["host_timeout"]
    )
    timings.append(h_online)

    a_run = wait_for_agents_running(
        installer, target_hosts, timeout=scale["agent_timeout"]
    )
    timings.append(a_run)
    host_rec.finish(
        t_host,
        hosts_added=n_add,
        hosts_total=target_hosts,
        hosts_online_sec=h_online.seconds,
        agents_up_sec=a_run.seconds,
    )
    timings.append(host_rec)

    # OSD apply + UP
    want_osds = int(scale["initial_osds"])
    if scale["allow_fewer_hosts"] and n_add < want:
        # scale OSD expectation roughly with host fraction
        fraction = n_add / float(want) if want else 1.0
        adjusted = max(before_osds + 1, int(want_osds * fraction))
        log.warning(
            f"TC1: adjusting OSD target {want_osds} -> {adjusted} for reduced host count"
        )
        want_osds = adjusted

    osd_rec = TimingRecord(name="tc1_osd_create_and_up", started_at=utc_now())
    t_osd = time.time()
    _apply_osd_drivegroup(
        installer,
        scale["osd_service_id"],
        all_devices=scale.get("osd_all_available_devices", True),
    )
    o_up = wait_for_osds_up(installer, want_osds, timeout=scale["osd_timeout"])
    timings.append(o_up)
    osd_rec.finish(t_osd, osds_target=want_osds, osds_up_sec=o_up.seconds)
    timings.append(osd_rec)

    _ensure_standby_mgr(installer)
    if scale.get("ensure_monitoring", True):
        _ensure_monitoring_stack(installer)
    _deploy_rgw_nfs(installer, scale)

    log.info(
        f"PASS TC1: hosts={len(online_hostnames(installer))} "
        f"agents={len(running_agent_hostnames(installer))} "
        f"osds_up={osd_up_count(installer)} "
        f"host+agent={host_rec.seconds}s osd={osd_rec.seconds}s"
    )
    return {
        "hosts": len(online_hostnames(installer)),
        "osds": osd_up_count(installer),
        "host_agent_sec": host_rec.seconds,
        "osd_sec": osd_rec.seconds,
    }


def run_tc2_mgr_failover(ceph_cluster, installer, scale: Dict[str, Any], timings: List):
    log.info("=== TC2: Mgr failover + agent retarget ===")
    _ensure_standby_mgr(installer)

    old_active = get_active_mgr_name(installer)
    assert old_active, "No active mgr before failover"
    log.info(f"Failing active mgr: {old_active}")

    shell(installer, f"ceph mgr fail {old_active}")
    fo = wait_for_mgr_active_change(
        installer, old_active, timeout=scale["mgr_failover_timeout"]
    )
    timings.append(fo)

    new_ip = get_active_mgr_ip(installer)
    assert new_ip, "Could not resolve new active mgr IP"
    log.info(f"New active mgr IP: {new_ip}")

    hosts = sorted(running_agent_hostnames(installer))
    limit = int(scale.get("retarget_sample_limit") or 0)
    if limit > 0:
        hosts = hosts[:limit]

    ret = wait_for_agents_retarget(
        installer,
        ceph_cluster,
        new_ip,
        sample_hosts=hosts,
        timeout=scale["retarget_timeout"],
    )
    timings.append(ret)

    # Combined retarget wall from fail command
    log.info(
        f"PASS TC2: mgr {fo.meta.get('old')} -> {fo.meta.get('new')} "
        f"active_in={fo.seconds}s retarget={ret.seconds}s"
    )
    return {"mgr_failover_sec": fo.seconds, "retarget_sec": ret.seconds}


def run_tc3_node_expansion(
    ceph_cluster, installer, scale: Dict[str, Any], timings: List
):
    log.info("=== TC3: Node expansion (+hosts/+OSDs) ===")
    before_hosts = len(online_hostnames(installer))
    before_osds = osd_up_count(installer)

    want = int(scale["expand_hosts"])
    candidates = select_nodes_for_add(ceph_cluster, installer, 0)
    n_add = _cap_host_count(want, len(candidates), scale["allow_fewer_hosts"], "TC3")
    nodes = candidates[:n_add]
    target_hosts = before_hosts + n_add

    host_rec = TimingRecord(name="tc3_host_add_and_agents_up", started_at=utc_now())
    t_host = time.time()
    if scale.get("host_add_mode", "burst") == "sequential":
        _add_hosts_sequential(installer, nodes, scale["host_labels"])
    else:
        _add_hosts_burst(installer, nodes, scale["host_labels"])

    h_online = wait_for_hosts_online(
        installer, target_hosts, timeout=scale["host_timeout"]
    )
    timings.append(h_online)
    a_run = wait_for_agents_running(
        installer, target_hosts, timeout=scale["agent_timeout"]
    )
    timings.append(a_run)
    host_rec.finish(
        t_host,
        hosts_added=n_add,
        hosts_total=target_hosts,
        hosts_online_sec=h_online.seconds,
        agents_up_sec=a_run.seconds,
    )
    timings.append(host_rec)

    want_osds = before_osds + int(scale["expand_osds"])
    if scale["allow_fewer_hosts"] and n_add < want:
        fraction = n_add / float(want) if want else 1.0
        delta = max(1, int(int(scale["expand_osds"]) * fraction))
        want_osds = before_osds + delta
        log.warning(f"TC3: adjusting OSD target to {want_osds}")

    osd_rec = TimingRecord(name="tc3_osd_create_and_up", started_at=utc_now())
    t_osd = time.time()
    # Re-apply same DG so new hosts pick up OSDs
    _apply_osd_drivegroup(
        installer,
        scale["osd_service_id"],
        all_devices=scale.get("osd_all_available_devices", True),
    )
    o_up = wait_for_osds_up(installer, want_osds, timeout=scale["osd_timeout"])
    timings.append(o_up)
    osd_rec.finish(t_osd, osds_target=want_osds, osds_up_sec=o_up.seconds)
    timings.append(osd_rec)

    if scale.get("ensure_monitoring", True):
        _ensure_monitoring_stack(installer)

    log.info(
        f"PASS TC3: hosts={len(online_hostnames(installer))} "
        f"osds_up={osd_up_count(installer)} "
        f"host+agent={host_rec.seconds}s osd={osd_rec.seconds}s"
    )
    return {
        "hosts": len(online_hostnames(installer)),
        "osds": osd_up_count(installer),
        "host_agent_sec": host_rec.seconds,
        "osd_sec": osd_rec.seconds,
    }


def _print_report(
    timings: List[TimingRecord], monitor: BackgroundMonitor, results: Dict
):
    log.info("=" * 72)
    log.info("CEPHADM AGENT SCALE REGRESSION — TIMING REPORT")
    log.info("=" * 72)
    for t in timings:
        log.info(f"  {t.name:40s} {t.seconds:10.2f}s  meta={t.meta}")
    rss = monitor.rss_summary()
    log.info(f"MGR RSS summary: {rss}")
    downs = monitor.agent_down_events()
    log.info(f"CEPHADM_AGENT_DOWN samples (post-TC1): {len(downs)}")
    for d in downs[:20]:
        log.info(f"  {d.ts} status={d.status} summary={d.agent_down_summary}")
    log.info(f"Results dict: {json.dumps(results, indent=2, default=str)}")
    log.info("=" * 72)


def run(ceph_cluster, **kw):
    """Entry point for CephCI suite."""
    config, installer = setup_run(ceph_cluster, kw)
    scale = _cfg(config)
    timings: List[TimingRecord] = []
    results: Dict[str, Any] = {"started_at": utc_now()}

    monitor = BackgroundMonitor(
        installer,
        ceph_cluster,
        health_interval=float(scale["health_poll_sec"]),
        rss_interval=float(scale["mgr_rss_poll_sec"]),
        watch_health=True,
        watch_rss=True,
        health_enabled=False,  # exclude TC1
    )
    pump = WorkloadPump(installer, ceph_cluster, scale.get("workloads"))

    monitor.start()
    try:
        # --- TC1 ---
        results["tc1"] = run_tc1_scale_bringup(ceph_cluster, installer, scale, timings)

        # Enable AGENT_DOWN watch + start RGW/NFS (and rados) for remaining cases
        monitor.enable_health_watch()
        pump.start()

        # --- TC2 ---
        results["tc2"] = run_tc2_mgr_failover(ceph_cluster, installer, scale, timings)

        # --- TC3 ---
        results["tc3"] = run_tc3_node_expansion(ceph_cluster, installer, scale, timings)

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

        _print_report(timings, monitor, results)

        downs = monitor.agent_down_events()
        if scale.get("fail_on_agent_down", True) and downs:
            raise AssertionError(
                f"CEPHADM_AGENT_DOWN observed {len(downs)} time(s) after TC1 "
                f"(poll every {scale['health_poll_sec']}s). First: {downs[0]}"
            )

        max_growth = int(scale.get("max_mgr_rss_growth_kb") or 0)
        rss = monitor.rss_summary()
        if max_growth > 0 and rss.get("rss_kb_delta", 0) > max_growth:
            raise AssertionError(
                f"Active mgr RSS grew by {rss['rss_kb_delta']} kB "
                f"(limit {max_growth} kB): {rss}"
            )

        log.info("PASS: cephadm agent scale regression suite")
        return 0
    except Exception as exc:
        results["error"] = str(exc)
        try:
            _print_report(timings, monitor, results)
        except Exception:
            pass
        log.error(f"FAIL: agent scale regression: {exc}", exc_info=True)
        return 1
    finally:
        pump.stop()
        monitor.stop()
