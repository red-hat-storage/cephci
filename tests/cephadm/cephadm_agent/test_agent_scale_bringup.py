"""
TC1 — Host + OSD scale bring-up (use_agent).

Bootstrap already done (1 host). Add N hosts + M OSDs; time host-add→agent-UP
and OSD-create→UP; ensure standby mgr + monitoring; deploy RGW/NFS.

Suite: abort-on-fail so TC2/TC3 are skipped if bring-up fails.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List

from cephadm_agent.helpers import log
from cephadm_agent.scale_helpers import (
    TimingRecord,
    ensure_use_agent,
    online_hostnames,
    osd_up_count,
    running_agent_hostnames,
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
    deploy_rgw_nfs,
    ensure_monitoring_stack,
    ensure_standby_mgr,
    run_scale_case,
)


def execute_tc1(ceph_cluster, installer, scale: Dict[str, Any], timings: List):
    log.info("=== TC1: Host + OSD scale bring-up (use_agent) ===")
    ensure_use_agent(installer)

    before_hosts = len(online_hostnames(installer))
    before_osds = osd_up_count(installer)
    log.info("Baseline: hosts={0}, osds_up={1}".format(before_hosts, before_osds))

    skip_bringup = bool(scale.get("skip_tc1_bringup"))
    if skip_bringup:
        log.warning(
            "TC1: skip_tc1_bringup=true — skipping host/OSD add; "
            "using current cluster scale as TC1 baseline"
        )
        host_rec = TimingRecord(name="tc1_host_add_and_agents_up", started_at=utc_now())
        host_rec.finish(
            time.time(),
            hosts_added=0,
            hosts_total=before_hosts,
            skipped=True,
        )
        timings.append(host_rec)
        osd_rec = TimingRecord(name="tc1_osd_create_and_up", started_at=utc_now())
        osd_rec.finish(time.time(), osds_target=before_osds, skipped=True)
        timings.append(osd_rec)
    else:
        want = int(scale["initial_hosts"])
        candidates = select_nodes_for_add(ceph_cluster, installer, 0)
        n_add = cap_host_count(want, len(candidates), scale["allow_fewer_hosts"], "TC1")
        nodes = candidates[:n_add]
        target_hosts = before_hosts + n_add

        host_rec = TimingRecord(name="tc1_host_add_and_agents_up", started_at=utc_now())
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

        want_osds = int(scale["initial_osds"])
        if scale["allow_fewer_hosts"] and n_add < want:
            fraction = n_add / float(want) if want else 1.0
            adjusted = max(before_osds + 1, int(want_osds * fraction))
            log.warning(
                "TC1: adjusting OSD target {0} -> {1} for reduced host count".format(
                    want_osds, adjusted
                )
            )
            want_osds = adjusted

        osd_rec = TimingRecord(name="tc1_osd_create_and_up", started_at=utc_now())
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

    ensure_standby_mgr(installer)
    if scale.get("ensure_monitoring", True):
        ensure_monitoring_stack(installer)
    deploy_rgw_nfs(installer, scale)

    log.info(
        "PASS TC1: hosts={0} agents={1} osds_up={2} host+agent={3}s osd={4}s{5}".format(
            len(online_hostnames(installer)),
            len(running_agent_hostnames(installer)),
            osd_up_count(installer),
            host_rec.seconds,
            osd_rec.seconds,
            " (bringup skipped)" if skip_bringup else "",
        )
    )
    return {
        "hosts": len(online_hostnames(installer)),
        "osds": osd_up_count(installer),
        "host_agent_sec": host_rec.seconds,
        "osd_sec": osd_rec.seconds,
        "bringup_skipped": skip_bringup,
    }


def run(ceph_cluster, **kw):
    """CephCI entry: TC1 host/OSD bring-up (no AGENT_DOWN gate during scale-up)."""
    return run_scale_case(
        ceph_cluster,
        kw,
        case_id="tc1",
        execute=execute_tc1,
        watch_health=False,
        start_workloads=False,
        check_agent_down=False,
    )
