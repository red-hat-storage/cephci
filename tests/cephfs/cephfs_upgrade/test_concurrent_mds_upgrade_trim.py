"""
IBMCEPH-19132 / IBMCEPH-16204 — Concurrent MDS upgrade (near-term 10.0).

Apply the agreed MDS cache-trim config set before MDS upgrade, run staggered
MGR → MON → MDS upgrades while capturing per-component and MDS phase timings,
then restore the trim configs.
"""

import json
import time
import traceback
from datetime import datetime

from ceph.ceph_admin.orch import Orch
from cli.cephadm.cephadm import CephAdm
from utility.log import Log
from utility.utils import resolve_registry_host, resolve_registry_login

log = Log(__name__)

# Frozen 10.0 trim set from QE Test Plan §2 (IBMCEPH-16487).
DEFAULT_MDS_TRIM_CONFIGS = {
    "mds_heartbeat_grace": "300",
    "mds_cache_reservation": "0.95",
    "mds_cache_trim_threshold": "524288",
    "mds_recall_global_max_decay_threshold": "1000000",
    "mds_recall_max_decay_threshold": "420715",
    "mds_session_cap_acquisition_throttle": "100",
    "mds_max_caps_per_client": "10000",
}

DEFAULT_OSD_FLAGS = ["noout", "noscrub", "nodeep-scrub"]


class ConcurrentMdsUpgradeError(Exception):
    """Raised when concurrent MDS upgrade trim flow fails."""


def _now():
    return time.time()


def _fmt_duration(seconds):
    if seconds is None:
        return "n/a"
    seconds = float(seconds)
    mins, secs = divmod(seconds, 60)
    if mins:
        return f"{int(mins)}m {secs:.1f}s ({seconds:.1f}s)"
    return f"{seconds:.1f}s"


def _login_upgrade_registry(orch, ceph_cluster, config):
    overrides = config.get("overrides") or {}
    registry_host = resolve_registry_host(
        overrides=overrides,
        image=config.get("container_image"),
        key="upgrade-registry",
    )
    if not registry_host:
        log.warning("No upgrade registry host resolved; skipping registry login")
        return
    reg_args = resolve_registry_login(registry_host)
    log.info(
        "Logging into upgrade registry %s (image=%s)",
        registry_host,
        config.get("container_image"),
    )
    for node in ceph_cluster.get_nodes(ignore="client"):
        orch.registry_login(node=node, args=reg_args)


def _exec(client, cmd, check_ec=True):
    out, rc = client.exec_command(sudo=True, cmd=cmd, check_ec=check_ec)
    return (out or "").strip(), rc


def _ceph_health(client):
    out, _ = _exec(client, "ceph health", check_ec=False)
    return out


def _wait_health_ok(client, timeout=600, interval=10, allow_warn=True):
    """Wait until health is not ERR. WARN is allowed during upgrade windows."""
    end = _now() + timeout
    while _now() < end:
        health = _ceph_health(client)
        log.info("Cluster health: %s", health)
        if "HEALTH_ERR" in health:
            time.sleep(interval)
            continue
        if "HEALTH_OK" in health:
            return True
        if allow_warn and "HEALTH_WARN" in health:
            return True
        time.sleep(interval)
    raise ConcurrentMdsUpgradeError(
        f"Cluster did not leave HEALTH_ERR within {timeout}s"
    )


def _get_fs_status(client, fs_name):
    out, _ = _exec(client, f"ceph fs status {fs_name} --format json", check_ec=False)
    if not out:
        return {}
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        log.warning("Unable to parse fs status json: %s", out[:200])
        return {}


def _mds_rank_summary(fs_status):
    """Return {rank: {state,name}} and active / non-active counts."""
    ranks = {}
    counts = {"active": 0, "non_active": 0, "total": 0}
    for entry in fs_status.get("mdsmap", []) or []:
        rank = entry.get("rank", -1)
        if rank is None or int(rank) < 0:
            continue
        state = entry.get("state", "unknown")
        name = entry.get("name", "")
        ranks[str(rank)] = {"state": state, "name": name}
        counts["total"] += 1
        if state == "up:active":
            counts["active"] += 1
        else:
            counts["non_active"] += 1
    return ranks, counts


def _dns_caps_snapshot(client, fs_name):
    """Best-effort MDS perf dump for cache pressure hints."""
    snap = {"fs": fs_name, "mds": {}}
    fs_status = _get_fs_status(client, fs_name)
    for entry in fs_status.get("mdsmap", []) or []:
        name = entry.get("name")
        if not name:
            continue
        out, _ = _exec(
            client,
            f"ceph tell mds.{name} perf dump 2>/dev/null || true",
            check_ec=False,
        )
        dns = caps = None
        try:
            data = json.loads(out) if out else {}
            if isinstance(data, dict):
                mds_perf = data.get("mds", {}) or {}
                dns = mds_perf.get("inodes") or mds_perf.get("inodes_expired")
                if dns is None:
                    dns = (data.get("mds_mem") or {}).get("ino")
                session = data.get("mds_sessions") or {}
                caps = session.get("total_caps") or session.get("session_count")
        except (json.JSONDecodeError, TypeError, AttributeError):
            pass
        snap["mds"][name] = {
            "state": entry.get("state"),
            "rank": entry.get("rank"),
            "dns_hint": dns,
            "caps_hint": caps,
        }
    return snap


def apply_mds_trim_configs(client, configs, verify=True):
    applied = {}
    for key, value in configs.items():
        cmd = f"ceph config set mds {key} {value}"
        log.info("Applying MDS trim config: %s", cmd)
        _exec(client, cmd)
        if verify:
            got, _ = _exec(client, f"ceph config get mds {key}", check_ec=False)
            applied[key] = {"expected": str(value), "actual": got}
            if str(value) not in got:
                raise ConcurrentMdsUpgradeError(
                    f"Config {key} expected {value}, got {got}"
                )
    dump, _ = _exec(client, "ceph config dump | grep mds_ || true", check_ec=False)
    log.info("MDS-related config dump after apply:\n%s", dump)
    return applied


def restore_mds_trim_configs(client, configs):
    restored = []
    for key in configs:
        cmd = f"ceph config rm mds {key}"
        log.info("Restoring (removing) MDS trim config: %s", cmd)
        _exec(client, cmd, check_ec=False)
        restored.append(key)
    dump, _ = _exec(client, "ceph config dump | grep mds_ || true", check_ec=False)
    log.info("MDS-related config dump after restore:\n%s", dump)
    return restored


def wait_for_cache_trim(client, fs_name, wait_seconds=120, poll=15):
    """Wait a fixed window for trim to take effect; capture before/after hints."""
    before = _dns_caps_snapshot(client, fs_name)
    log.info("Pre-trim DNS/caps snapshot: %s", json.dumps(before, default=str))
    end = _now() + wait_seconds
    while _now() < end:
        time.sleep(poll)
        mid = _dns_caps_snapshot(client, fs_name)
        log.info("Trim wait snapshot: %s", json.dumps(mid, default=str))
    after = _dns_caps_snapshot(client, fs_name)
    log.info("Post-trim DNS/caps snapshot: %s", json.dumps(after, default=str))
    return {"before": before, "after": after, "wait_seconds": wait_seconds}


def flush_mds_journals(client, fs_name):
    fs_status = _get_fs_status(client, fs_name)
    flushed = []
    for entry in fs_status.get("mdsmap", []) or []:
        name = entry.get("name")
        state = entry.get("state", "")
        if not name or "active" not in state:
            continue
        cmd = f"ceph tell mds.{name} flush journal"
        log.info("Flushing journal: %s", cmd)
        out, _ = _exec(client, cmd, check_ec=False)
        flushed.append({"mds": name, "out": out[:200]})
    return flushed


def set_fail_fs(client, value=True):
    val = "true" if value else "false"
    cmd = f"ceph config set mgr mgr/orchestrator/fail_fs {val}"
    log.info("%s", cmd)
    _exec(client, cmd)
    got, _ = _exec(
        client, "ceph config get mgr mgr/orchestrator/fail_fs", check_ec=False
    )
    return got


def _set_osd_flags(node, flags, unset=False):
    for flag in flags:
        if unset:
            rc = CephAdm(node).ceph.osd.unset(flag)
        else:
            rc = CephAdm(node).ceph.osd.set(flag)
        if rc:
            action = "unset" if unset else "set"
            raise ConcurrentMdsUpgradeError(f"Unable to {action} osd flag {flag}")


def _upgrade_status_dict(orch):
    out = orch.upgrade_status()
    if isinstance(out, dict):
        return out
    if not out:
        return {"in_progress": False}
    try:
        return json.loads(out)
    except (json.JSONDecodeError, TypeError):
        text = str(out)
        return {
            "raw": text,
            "in_progress": "true" in text.lower() and "in_progress" in text.lower(),
        }


def monitor_upgrade_with_mds_phases(
    client, orch, fs_name, timeout=3600, poll=7
):
    """
    Monitor orch upgrade and record MDS-centric phase timings.

    Phases:
      - upgrade_start
      - first_mds_not_active (fail / restart trigger)
      - all_mds_active_again (fail → active window)
      - upgrade_complete
    """
    timings = {
        "upgrade_start_ts": datetime.utcnow().isoformat() + "Z",
        "upgrade_start": _now(),
        "first_mds_degraded_at": None,
        "first_mds_degraded_elapsed_s": None,
        "all_mds_active_at": None,
        "mds_down_to_active_s": None,
        "upgrade_complete_at": None,
        "upgrade_total_s": None,
        "samples": [],
    }
    fs0 = _get_fs_status(client, fs_name)
    _, counts0 = _mds_rank_summary(fs0)
    expected_active = max(counts0.get("active", 0), 1)
    log.info("Expecting %s MDS ranks to return to up:active", expected_active)

    end = _now() + timeout
    saw_degraded = False
    recovered = False
    degrade_wall = None

    while _now() < end:
        time.sleep(poll)
        status = _upgrade_status_dict(orch)
        fs_status = _get_fs_status(client, fs_name)
        ranks, counts = _mds_rank_summary(fs_status)
        sample = {
            "t_elapsed_s": round(_now() - timings["upgrade_start"], 1),
            "upgrade_in_progress": bool(status.get("in_progress")),
            "mds_counts": counts,
            "mds_ranks": ranks,
        }
        timings["samples"].append(sample)
        log.info(
            "Upgrade monitor t=+%.1fs in_progress=%s mds_active=%s/%s ranks=%s",
            sample["t_elapsed_s"],
            sample["upgrade_in_progress"],
            counts["active"],
            expected_active,
            ranks,
        )

        degraded = counts["active"] < expected_active
        if degraded and not saw_degraded:
            saw_degraded = True
            degrade_wall = _now()
            timings["first_mds_degraded_at"] = datetime.utcnow().isoformat() + "Z"
            timings["first_mds_degraded_elapsed_s"] = round(
                degrade_wall - timings["upgrade_start"], 1
            )
            log.info(
                "MDS degrade/fail first observed at +%ss",
                timings["first_mds_degraded_elapsed_s"],
            )

        if (
            saw_degraded
            and not recovered
            and counts["active"] >= expected_active
            and degrade_wall is not None
        ):
            recovered = True
            timings["all_mds_active_at"] = datetime.utcnow().isoformat() + "Z"
            timings["mds_down_to_active_s"] = round(_now() - degrade_wall, 1)
            log.info(
                "All MDS ranks active again. fail→active window=%ss",
                timings["mds_down_to_active_s"],
            )

        if not status.get("in_progress", False):
            timings["upgrade_complete_at"] = datetime.utcnow().isoformat() + "Z"
            timings["upgrade_total_s"] = round(_now() - timings["upgrade_start"], 1)
            log.info("Upgrade complete in %ss", timings["upgrade_total_s"])
            break
    else:
        raise ConcurrentMdsUpgradeError(
            f"Upgrade did not complete within {timeout}s; last={timings['samples'][-1:]}"
        )

    fs_status = _get_fs_status(client, fs_name)
    ranks, counts = _mds_rank_summary(fs_status)
    timings["final_mds_counts"] = counts
    timings["final_mds_ranks"] = ranks
    # Drop verbose samples from summary log size but keep count
    timings["sample_count"] = len(timings["samples"])
    timings["samples"] = timings["samples"][-5:]
    return timings


def start_daemon_upgrade(
    node, target_image, daemon_types=None, services=None, limit=None
):
    kwargs = {"image": target_image}
    if daemon_types:
        kwargs["daemon_types"] = daemon_types
    if services:
        kwargs["services"] = services
    if limit is not None:
        kwargs["limit"] = limit
    log.info("Starting staggered upgrade: %s", kwargs)
    rc = CephAdm(node).ceph.orch.upgrade.start(**kwargs)
    if rc:
        raise ConcurrentMdsUpgradeError(f"Unable to start upgrade: {kwargs}")


def upgrade_component(client, node, orch, config, component, timings_report):
    """Upgrade one component (mgr|mon|mds) and record timing."""
    target_image = config.get("container_image")
    if not target_image:
        raise ConcurrentMdsUpgradeError("container_image is required in test config")

    fs_name = config.get("fs_name", "cephfs")
    osd_flags = config.get("osd_flags", DEFAULT_OSD_FLAGS)
    timeout = int(config.get("upgrade_timeout", 3600))

    if component == "mgr":
        daemon_types, services = "mgr", None
    elif component == "mon":
        daemon_types, services = "mon", None
    elif component == "mds":
        daemon_types = None
        services = config.get("mds_service", f"mds.{fs_name}")
    else:
        raise ConcurrentMdsUpgradeError(f"Unknown component: {component}")

    entry = {
        "component": component,
        "daemon_types": daemon_types,
        "services": services,
        "started_at": datetime.utcnow().isoformat() + "Z",
    }
    t0 = _now()
    _set_osd_flags(node, osd_flags, unset=False)
    try:
        start_daemon_upgrade(
            node,
            target_image,
            daemon_types=daemon_types,
            services=services,
            limit=config.get("limit"),
        )
        if component == "mds":
            phase = monitor_upgrade_with_mds_phases(
                client, orch, fs_name, timeout=timeout
            )
            entry["mds_phases"] = phase
            entry["duration_s"] = phase.get("upgrade_total_s")
        else:
            orch.monitor_upgrade_status(timeout=timeout)
            entry["duration_s"] = round(_now() - t0, 1)
            entry["completed_at"] = datetime.utcnow().isoformat() + "Z"
    finally:
        _set_osd_flags(node, osd_flags, unset=True)

    entry["duration_human"] = _fmt_duration(entry.get("duration_s"))
    timings_report["components"].append(entry)
    log.info(
        "Component %s upgrade finished in %s",
        component,
        entry["duration_human"],
    )
    _wait_health_ok(client, timeout=600)
    return entry


def print_timing_report(report):
    log.info("=" * 72)
    log.info("IBMCEPH-19132 Concurrent MDS Upgrade — Timing Report")
    log.info("=" * 72)
    log.info("Overall start: %s", report.get("flow_start"))
    log.info("Overall end:   %s", report.get("flow_end"))
    log.info("Overall:       %s", _fmt_duration(report.get("flow_total_s")))
    log.info("-" * 72)
    for c in report.get("components", []):
        log.info(
            "Component %-4s  duration=%s  selector=%s",
            c.get("component"),
            c.get("duration_human"),
            c.get("daemon_types") or c.get("services"),
        )
        phases = c.get("mds_phases") or {}
        if phases:
            log.info(
                "  MDS first degrade at +%ss",
                phases.get("first_mds_degraded_elapsed_s"),
            )
            log.info(
                "  MDS fail/degrade → all active: %s",
                _fmt_duration(phases.get("mds_down_to_active_s")),
            )
            log.info(
                "  MDS upgrade total: %s",
                _fmt_duration(phases.get("upgrade_total_s")),
            )
            log.info("  Final MDS ranks: %s", phases.get("final_mds_ranks"))
    log.info("-" * 72)
    log.info("Trim configs applied: %s", report.get("trim_applied"))
    log.info("Trim configs restored: %s", report.get("trim_restored"))
    log.info("fail_fs: %s", report.get("fail_fs"))
    log.info("=" * 72)
    log.info("TIMING_REPORT_JSON=%s", json.dumps(report, default=str))


def run(ceph_cluster, **kw):
    """
    Concurrent MDS upgrade trim procedure with staggered MGR/MON/MDS timings.

    Suite config example:
      module: cephfs_upgrade.test_concurrent_mds_upgrade_trim.py
      config:
        container_image: <target>
        fs_name: cephfs
        components: [mgr, mon, mds]
        apply_trim_before: mds
        trim_wait_seconds: 180
        flush_journal: true
        fail_fs: true
        restore_trim_after: true
    """
    report = {
        "flow_start": datetime.utcnow().isoformat() + "Z",
        "components": [],
        "trim_applied": False,
        "trim_restored": False,
        "fail_fs": None,
        "trim_snapshots": {},
    }
    flow_t0 = _now()
    client = None
    configs = dict(DEFAULT_MDS_TRIM_CONFIGS)

    try:
        config = kw.get("config") or {}
        config["overrides"] = kw.get("test_data", {}).get("custom_config_dict")
        clients = ceph_cluster.get_ceph_objects("client")
        if not clients:
            log.error("Need at least one client")
            return 1
        client = clients[0]
        node = ceph_cluster.get_nodes(role="mon")[0]
        orch = Orch(cluster=ceph_cluster, **config)

        fs_name = config.get("fs_name", "cephfs")
        components = config.get("components", ["mgr", "mon", "mds"])
        apply_before = config.get("apply_trim_before", "mds")
        trim_wait = int(config.get("trim_wait_seconds", 180))
        flush_journal = config.get("flush_journal", True)
        use_fail_fs = config.get("fail_fs", True)
        restore_after = config.get("restore_trim_after", True)
        user_configs = config.get("mds_trim_configs")
        if user_configs:
            configs.update({k: str(v) for k, v in user_configs.items()})

        log.info("Components upgrade order: %s", components)
        log.info("Trim configs will be applied before component: %s", apply_before)

        _wait_health_ok(client, timeout=300)
        _login_upgrade_registry(orch, ceph_cluster, config)

        target_image = config.get("container_image")
        if not target_image:
            raise ConcurrentMdsUpgradeError(
                "config.container_image is required (pass via suite/CLI override)"
            )
        if CephAdm(node).ceph.orch.upgrade.check(image=target_image):
            raise ConcurrentMdsUpgradeError("Upgrade image check failed")

        for component in components:
            if component == apply_before and not report["trim_applied"]:
                log.info("Applying MDS trim configs before %s upgrade", component)
                apply_mds_trim_configs(client, configs)
                report["trim_applied"] = True
                report["trim_configs"] = configs
                report["trim_snapshots"]["wait"] = wait_for_cache_trim(
                    client, fs_name, wait_seconds=trim_wait
                )
                if flush_journal:
                    report["journal_flush"] = flush_mds_journals(client, fs_name)
                if use_fail_fs:
                    report["fail_fs"] = set_fail_fs(client, True)

            upgrade_component(client, node, orch, config, component, report)

        if restore_after and report["trim_applied"]:
            log.info("Restoring MDS trim configs after upgrade flow")
            restore_mds_trim_configs(client, configs)
            report["trim_restored"] = True
            if use_fail_fs:
                set_fail_fs(client, False)
                report["fail_fs"] = "restored_false"

        _wait_health_ok(client, timeout=600)
        report["flow_end"] = datetime.utcnow().isoformat() + "Z"
        report["flow_total_s"] = round(_now() - flow_t0, 1)
        print_timing_report(report)
        log.info(
            "TEST PASSED: concurrent MDS upgrade trim flow completed in %s",
            _fmt_duration(report["flow_total_s"]),
        )
        return 0

    except Exception as exc:  # noqa: BLE001
        log.error("Concurrent MDS upgrade trim flow failed: %s", exc)
        log.error(traceback.format_exc())
        try:
            if client is not None and configs:
                log.info("Attempting trim config restore after failure")
                restore_mds_trim_configs(client, configs)
                report["trim_restored"] = True
        except Exception as cleanup_exc:  # noqa: BLE001
            log.error("Cleanup restore failed: %s", cleanup_exc)
        report["flow_end"] = datetime.utcnow().isoformat() + "Z"
        report["flow_total_s"] = round(_now() - flow_t0, 1)
        report["error"] = str(exc)
        print_timing_report(report)
        return 1
