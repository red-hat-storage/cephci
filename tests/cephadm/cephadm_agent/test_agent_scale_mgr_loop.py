"""
TC5 — Repeated mgr failover + agent retarget under IO.
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


def execute_tc5(ceph_cluster, installer, scale: Dict[str, Any], timings: List):
    log.info("=== TC5: Repeated mgr failover + retarget ===")
    ensure_standby_mgr(installer)
    iters = max(1, int(scale.get("mgr_failover_iterations") or 3))
    rounds = []

    for i in range(1, iters + 1):
        old = get_active_mgr_name(installer)
        assert old, "No active mgr"
        log.info("TC5 round {0}/{1}: failing {2}".format(i, iters, old))
        shell(installer, "ceph mgr fail {0}".format(old))
        fo = wait_for_mgr_active_change(
            installer, old, timeout=scale["mgr_failover_timeout"]
        )
        fo.name = "tc5_mgr_failover_{0}".format(i)
        timings.append(fo)

        new_ip = get_active_mgr_ip(installer)
        assert new_ip, "No new active mgr IP"
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
        ret.name = "tc5_retarget_{0}".format(i)
        timings.append(ret)
        rounds.append(
            {
                "round": i,
                "failover_sec": fo.seconds,
                "retarget_sec": ret.seconds,
                "old": fo.meta.get("old"),
                "new": fo.meta.get("new"),
            }
        )
        log.info(
            "TC5 round {0}: FO={1}s retarget={2}s {3} -> {4}".format(
                i, fo.seconds, ret.seconds, fo.meta.get("old"), fo.meta.get("new")
            )
        )

    log.info("PASS TC5: {0} failover round(s)".format(iters))
    return {"rounds": rounds}


def run(ceph_cluster, **kw):
    return run_scale_case(
        ceph_cluster,
        kw,
        case_id="tc5",
        execute=execute_tc5,
        watch_health=True,
        start_workloads=True,
        check_agent_down=True,
    )
