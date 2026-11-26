"""
TC2 — Mgr failover + agent retarget.

Requires TC1 bring-up (or an already-scaled cluster). Fails active mgr, times
new active election and agent HTTPS retarget under background RGW/NFS/rados IO.
Fails on CEPHADM_AGENT_DOWN samples during the case.
"""

from __future__ import annotations

from typing import Any, Dict, List

from cephadm_agent.helpers import log, shell
from cephadm_agent.scale_helpers import (
    get_active_mgr_ip,
    get_active_mgr_name,
    running_agent_hostnames,
    wait_for_agents_retarget,
    wait_for_mgr_active_change,
)
from cephadm_agent.scale_regression_ops import ensure_standby_mgr, run_scale_case


def execute_tc2(ceph_cluster, installer, scale: Dict[str, Any], timings: List):
    log.info("=== TC2: Mgr failover + agent retarget ===")
    ensure_standby_mgr(installer)

    old_active = get_active_mgr_name(installer)
    assert old_active, "No active mgr before failover"
    log.info("Failing active mgr: {0}".format(old_active))

    shell(installer, "ceph mgr fail {0}".format(old_active))
    fo = wait_for_mgr_active_change(
        installer, old_active, timeout=scale["mgr_failover_timeout"]
    )
    timings.append(fo)

    new_ip = get_active_mgr_ip(installer)
    assert new_ip, "Could not resolve new active mgr IP"
    log.info("New active mgr IP: {0}".format(new_ip))

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

    log.info(
        "PASS TC2: mgr {0} -> {1} active_in={2}s retarget={3}s".format(
            fo.meta.get("old"), fo.meta.get("new"), fo.seconds, ret.seconds
        )
    )
    return {"mgr_failover_sec": fo.seconds, "retarget_sec": ret.seconds}


def run(ceph_cluster, **kw):
    """CephCI entry: TC2 mgr failover + agent retarget under IO + health watch."""
    return run_scale_case(
        ceph_cluster,
        kw,
        case_id="tc2",
        execute=execute_tc2,
        watch_health=True,
        start_workloads=True,
        check_agent_down=True,
    )
