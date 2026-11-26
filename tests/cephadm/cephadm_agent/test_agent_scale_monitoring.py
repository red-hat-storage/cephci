"""
TC8 — Monitoring stack health after scale (prometheus/grafana/node-exporter/…).
"""

from __future__ import annotations

import json
from typing import Any, Dict, List

from cephadm_agent.helpers import log, shell
from cephadm_agent.scale_helpers import online_hostnames
from cephadm_agent.scale_regression_ops import ensure_monitoring_stack, run_scale_case

REQUIRED_TYPES = ("prometheus", "grafana", "alertmanager", "node-exporter")


def execute_tc8(ceph_cluster, installer, scale: Dict[str, Any], timings: List):
    log.info("=== TC8: Monitoring stack coverage ===")
    if scale.get("ensure_monitoring", True):
        ensure_monitoring_stack(installer)

    out, _ = shell(installer, "ceph orch ps -f json")
    daemons = json.loads(out) if out.strip() else []
    by_type: Dict[str, List] = {}
    for d in daemons:
        by_type.setdefault(
            d.get("daemon_type") or d.get("daemon_name", "").split(".", 1)[0], []
        ).append(d)

    # orch ps uses daemon_type field
    typed: Dict[str, List] = {}
    for d in daemons:
        dt = d.get("daemon_type")
        if dt:
            typed.setdefault(dt, []).append(d)

    hosts = online_hostnames(installer)
    summary = {}
    missing_required = []
    for dtype in REQUIRED_TYPES:
        running = [d for d in typed.get(dtype, []) if d.get("status_desc") == "running"]
        summary[dtype] = {
            "running": len(running),
            "total": len(typed.get(dtype, [])),
        }
        if dtype == "node-exporter":
            ne_hosts = {d.get("hostname") for d in running}
            missing = sorted(hosts - ne_hosts)
            summary[dtype]["missing_hosts"] = missing[:20]
            summary[dtype]["missing_count"] = len(missing)
            if missing:
                missing_required.append(
                    "node-exporter missing on {0} hosts".format(len(missing))
                )
        elif not running:
            missing_required.append("{0} has 0 running".format(dtype))

    log.info("Monitoring summary: {0}".format(json.dumps(summary)))
    if missing_required:
        raise AssertionError("; ".join(missing_required))
    log.info("PASS TC8: monitoring stack present")
    return summary


def run(ceph_cluster, **kw):
    return run_scale_case(
        ceph_cluster,
        kw,
        case_id="tc8",
        execute=execute_tc8,
        watch_health=True,
        start_workloads=False,
        check_agent_down=True,
    )
