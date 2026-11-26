"""
TC3 — Node expansion (+hosts / +OSDs).

Requires TC1 bring-up. Adds Δ hosts + Δ OSDs; times host-add→agent-UP and
OSD→UP under background IO. Skips cleanly when no unused conf nodes remain
(scale.skip_tc3_if_no_hosts). Fails on CEPHADM_AGENT_DOWN samples.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List

from cephadm_agent.helpers import log
from cephadm_agent.scale_helpers import (
    TimingRecord,
    online_hostnames,
    osd_up_count,
    select_nodes_for_add,
    utc_now,
    wait_for_agents_running,
    wait_for_hosts_online,
    wait_for_osds_up,
)
from cephadm_agent.scale_regression_ops import (
    add_hosts,
    apply_osd_drivegroup,
    cap_host_count,
    ensure_monitoring_stack,
    run_scale_case,
)


def execute_tc3(ceph_cluster, installer, scale: Dict[str, Any], timings: List):
    log.info("=== TC3: Node expansion (+hosts/+OSDs) ===")
    before_hosts = len(online_hostnames(installer))
    before_osds = osd_up_count(installer)

    want = int(scale["expand_hosts"])
    candidates = select_nodes_for_add(ceph_cluster, installer, 0)
    if (
        not candidates
        and scale.get("skip_tc3_if_no_hosts", True)
        and scale.get("allow_fewer_hosts", True)
    ):
        log.warning(
            "TC3: no unused hosts left in conf — skipping expansion "
            "(skip_tc3_if_no_hosts=true)"
        )
        host_rec = TimingRecord(name="tc3_host_add_and_agents_up", started_at=utc_now())
        host_rec.finish(
            time.time(), hosts_added=0, hosts_total=before_hosts, skipped=True
        )
        timings.append(host_rec)
        osd_rec = TimingRecord(name="tc3_osd_create_and_up", started_at=utc_now())
        osd_rec.finish(time.time(), osds_target=before_osds, skipped=True)
        timings.append(osd_rec)
        return {
            "hosts": before_hosts,
            "osds": before_osds,
            "host_agent_sec": host_rec.seconds,
            "osd_sec": osd_rec.seconds,
            "skipped": True,
            "reason": "no_unused_hosts",
        }

    n_add = cap_host_count(want, len(candidates), scale["allow_fewer_hosts"], "TC3")
    nodes = candidates[:n_add]
    target_hosts = before_hosts + n_add

    host_rec = TimingRecord(name="tc3_host_add_and_agents_up", started_at=utc_now())
    t_host = time.time()
    add_hosts(installer, nodes, scale)

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
        log.warning("TC3: adjusting OSD target to {0}".format(want_osds))

    osd_rec = TimingRecord(name="tc3_osd_create_and_up", started_at=utc_now())
    t_osd = time.time()
    apply_osd_drivegroup(
        installer,
        scale["osd_service_id"],
        all_devices=scale.get("osd_all_available_devices", True),
    )
    o_up = wait_for_osds_up(installer, want_osds, timeout=scale["osd_timeout"])
    timings.append(o_up)
    osd_rec.finish(t_osd, osds_target=want_osds, osds_up_sec=o_up.seconds)
    timings.append(osd_rec)

    if scale.get("ensure_monitoring", True):
        ensure_monitoring_stack(installer)

    log.info(
        "PASS TC3: hosts={0} osds_up={1} host+agent={2}s osd={3}s".format(
            len(online_hostnames(installer)),
            osd_up_count(installer),
            host_rec.seconds,
            osd_rec.seconds,
        )
    )
    return {
        "hosts": len(online_hostnames(installer)),
        "osds": osd_up_count(installer),
        "host_agent_sec": host_rec.seconds,
        "osd_sec": osd_rec.seconds,
    }


def run(ceph_cluster, **kw):
    """CephCI entry: TC3 expansion under IO + health watch."""
    return run_scale_case(
        ceph_cluster,
        kw,
        case_id="tc3",
        execute=execute_tc3,
        watch_health=True,
        start_workloads=True,
        check_agent_down=True,
    )
