"""
TC6 — Registry auth presence (podman-auth) on agent/OSD hosts.

Guards the empty-inventory / incomplete OSD bug: hosts missing
/etc/ceph/podman-auth.json fail private-registry pulls for Volume Gatherer.
"""

from __future__ import annotations

from typing import Any, Dict, List

from cephadm_agent.helpers import get_node_for_host, log
from cephadm_agent.scale_helpers import online_hostnames, running_agent_hostnames
from cephadm_agent.scale_regression_ops import run_scale_case

AUTH_PATH = "/etc/ceph/podman-auth.json"


def execute_tc6(ceph_cluster, installer, scale: Dict[str, Any], timings: List):
    log.info("=== TC6: Registry auth (podman-auth) presence ===")
    hosts = sorted(running_agent_hostnames(installer) or online_hostnames(installer))
    sample_n = int(scale.get("auth_check_sample") or 0)
    if sample_n > 0:
        hosts = hosts[:sample_n]
    assert hosts, "No hosts to check"

    missing = []
    present = []
    for hostname in hosts:
        node = get_node_for_host(ceph_cluster, hostname)
        if node is None:
            log.warning("No CephNode for {0}; skip".format(hostname))
            continue
        out, _ = node.exec_command(
            sudo=True,
            cmd="test -s {0} && echo PRESENT || echo MISSING".format(AUTH_PATH),
            check_ec=False,
        )
        status = (out or "").strip().splitlines()[-1] if out else "MISSING"
        if status == "PRESENT":
            present.append(hostname)
        else:
            missing.append(hostname)
            log.error("MISSING {0} on {1}".format(AUTH_PATH, hostname))

    log.info(
        "podman-auth present={0} missing={1} sampled={2}".format(
            len(present), len(missing), len(hosts)
        )
    )
    # No auth anywhere: public-image labs — nothing to enforce.
    if not present and missing:
        log.warning(
            "TC6: no host has podman-auth — skipping strict check "
            "(expected for public-image runs without registry-login)"
        )
        return {
            "checked": 0,
            "missing": missing,
            "skipped": True,
            "reason": "no_auth_anywhere",
        }
    # Partial coverage is the filed bug mode — fail hard.
    if missing:
        raise AssertionError(
            "{0} missing on {1} host(s) while present on {2}: {3}".format(
                AUTH_PATH, len(missing), len(present), missing[:10]
            )
        )
    log.info("PASS TC6: podman-auth present on sampled hosts")
    return {"checked": len(present), "missing": missing}


def run(ceph_cluster, **kw):
    return run_scale_case(
        ceph_cluster,
        kw,
        case_id="tc6",
        execute=execute_tc6,
        watch_health=True,
        start_workloads=False,
        check_agent_down=True,
    )
