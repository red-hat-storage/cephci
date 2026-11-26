"""
TC4 — Agent stop/start: CEPHADM_AGENT_DOWN raise and clear at scale.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List

from cephadm_agent.helpers import (
    AGENT_HEALTH_WARNING,
    agent_service_name,
    get_agent_daemons,
    get_fsid,
    get_node_for_host,
    log,
    wait_for_health_warning,
)
from cephadm_agent.scale_helpers import TimingRecord, utc_now
from cephadm_agent.scale_regression_ops import run_scale_case


def _sample_hosts(installer, n: int) -> List[str]:
    agents = [
        a["hostname"]
        for a in get_agent_daemons(installer)
        if a.get("status_desc") == "running"
    ]
    agents = sorted(set(agents))
    assert agents, "No running agents"
    return agents[: max(1, n)]


def execute_tc4(ceph_cluster, installer, scale: Dict[str, Any], timings: List):
    log.info("=== TC4: Agent restart — AGENT_DOWN raise/clear ===")
    sample = _sample_hosts(installer, int(scale.get("agent_restart_sample") or 1))
    fsid = get_fsid(installer)
    down_to = float(scale.get("agent_down_timeout") or 180)
    recover_to = float(scale.get("agent_recover_timeout") or 300)

    results = []
    for hostname in sample:
        node = get_node_for_host(ceph_cluster, hostname)
        assert node, "No CephNode for {0}".format(hostname)
        svc = agent_service_name(fsid, hostname)
        rec = TimingRecord(
            name="tc4_restart_{0}".format(hostname), started_at=utc_now()
        )
        t0 = time.time()

        log.info("Stopping agent on {0}".format(hostname))
        node.exec_command(sudo=True, cmd="systemctl stop {0}".format(svc))
        assert wait_for_health_warning(
            installer, AGENT_HEALTH_WARNING, timeout=down_to, interval=10
        ), "CEPHADM_AGENT_DOWN not raised after stop on {0}".format(hostname)

        log.info("Starting agent on {0}".format(hostname))
        node.exec_command(sudo=True, cmd="systemctl start {0}".format(svc))
        assert wait_for_health_warning(
            installer,
            AGENT_HEALTH_WARNING,
            timeout=recover_to,
            interval=10,
            expect_present=False,
        ), "CEPHADM_AGENT_DOWN did not clear after start on {0}".format(hostname)

        rec.finish(t0, hostname=hostname)
        timings.append(rec)
        results.append({"hostname": hostname, "sec": rec.seconds})

    log.info("PASS TC4: restarted {0} agent(s)".format(len(results)))
    return {"hosts": results}


def run(ceph_cluster, **kw):
    return run_scale_case(
        ceph_cluster,
        kw,
        case_id="tc4",
        execute=execute_tc4,
        watch_health=False,  # intentional downs during the case
        start_workloads=True,
        check_agent_down=False,
    )
