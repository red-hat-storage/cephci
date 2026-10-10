"""
Ceph Upgrade Thrashing Test - Main Orchestrator

7-phase upgrade test that exercises all client types (RGW, RBD, CephFS, NFS,
SMB) with features enabled, collects performance baselines, performs
an N-1 to N upgrade under IO load, and validates data integrity, service health,
feature persistence, and known bug regressions.

Phase flow (suite YAML bootstraps cluster at N-1):
  P1: Client & service setup
  P2: Feature enablement + integrity baselines
  P2.5: Cluster pre-fill
  P3: Stabilization + pre-flight + baseline stats
  P4: Upgrade with monitoring
  P5: Post-upgrade stabilization + stats
  P6: Verification (functional, integrity, bugs)
  P7: Report generation
"""

import concurrent.futures
import json
import os
import re
import statistics
import time
import traceback
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from datetime import datetime, timedelta, timezone

from looseversion import LooseVersion
from upgrade_thrashing.lifecycle_log import (
    _GRACE_LIFECYCLE_EVENTS,
    DAEMON_LOG_PATTERNS,
    LOGFILE_PARALLEL_PER_HOST,
    LOGFILE_SSH_TIMEOUT_LARGE_DTYPES,
    LOGFILE_SSH_TIMEOUT_LARGE_SEC,
    LOGFILE_SSH_TIMEOUT_SEC,
    _build_logfile_scrape_cmd,
    _build_type_lifecycle_summary,
    _daemon_logfile_path,
    _entry_lifecycle_window,
    _grep_pattern_for_dtype,
    _host_deploy_name_index,
    _merge_daemon_lifecycle_events,
    _normalize_log_daemon_name,
    _parse_lifecycle_lines,
    analyze_nfs_grace,
    daemon_running_count_mismatches,
    remote_exec_failure_tag,
    summarize_orch_ps_running_counts,
)
from upgrade_thrashing.upgrade_cephx_rotation import (
    cephx_failure_reasons,
    cephx_remediation_enabled,
    remediate_cephx_auth_warnings,
)
from upgrade_thrashing.upgrade_health_monitor import (
    HealthWarningTracker,
    classify_upgrade_error,
)

from ceph.ceph import CommandFailed
from ceph.ceph_admin import CephAdmin
from ceph.ceph_admin.orch import Orch
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.utils import get_cluster_timestamp
from ceph.utils import (
    get_daemon_versions,
    get_node_by_id,
    mgr_accept_license,
    remove_repos,
)
from cephci.utils.build_info import CephTestManifest
from cli.utilities.configure import setup_ibm_licence
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log
from utility.utils import get_cephci_config

log = Log(__name__)

_RGW_TEST_USER = "upgrade-test-user"
_RGW_TEST_ACCESS_KEY = "testkey"
_RGW_TEST_SECRET_KEY = "testsecret"
_RGW_IO_USER = "upgrade-io-user"
_RGW_IO_ACCESS_KEY = "iokey"
_RGW_IO_SECRET_KEY = "iosecret"

DEFAULT_CONFIG = {
    "skip_service_setup": False,
    "skip_client_cleanup": False,
    "skip_cluster_cleanup": False,
    "mon_max_pg_per_osd": 350,
    "phase_timing": {
        "pre_upgrade_baseline_sec": 600,
        "post_upgrade_baseline_sec": 600,
        "upgrade_timeout_sec": 43200,
        "warmup_discard_sec": 60,
        "stabilization_timeout_sec": 600,
        "post_upgrade_stabilization_sec": 600,
        "io_quiesce_sec": 30,
        "upgrade_stall_threshold_sec": 5400,
        "max_pause_retries": 3,
        "auto_resume_on_pause": False,
        "disk_warning_percent": 60,
        "io_kill_timeout_sec": 30,
        "skip_monitoring_upgrade_failures": False,
    },
    "performance_regression": {
        "iops_drop_threshold_percent": 30,
        "latency_increase_threshold_percent": 50,
        "throughput_drop_threshold_percent": 30,
    },
    "prometheus_step_sec": 10,
    "integrity": {
        "rados_objects_per_pool": 1000,
        "fio_baseline_size": "1G",
    },
    # bg_params and fill_params are populated by apply_io_tier() when an
    # io_tier is set, or directly by YAML overrides.  Kept empty here so
    # that tier defaults can fill them in without conflict.  When empty,
    # every command builder falls back to its own .get(key, default) which
    # matches the pre-tiering hardcoded values.
    "bg_params": {},
    "fill_params": {},
    "cluster_fill": {
        "enabled": True,
        "target_percent": 35,
        "abort_at_percent": 75,
        "fill_timeout_sec": 0,
        "poll_interval_sec": 30,
    },
    "upgrade_params": {
        "relax_signature_policy": False,
    },
    "services": {},
    "features": {},
    "bug_validations": {},
    "nfs_grace_validation": {
        "enabled": False,
        "mode": "baseline",
        "max_observed_grace_sec": 120,
    },
    "io_tools": {},
    "io_patterns": {
        "rwmixread": 70,
        "tail_latency_percentiles": "50:90:95:99:99.9:99.99",
    },
}


def _deep_merge(base, override):
    """Recursively merge override dict into base, returning merged result."""
    merged = deepcopy(base)
    for key, val in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(val, dict):
            merged[key] = _deep_merge(merged[key], val)
        else:
            merged[key] = deepcopy(val)
    return merged


def _apply_ibm_smb_overrides(config):
    """Enable SMB service, IO tools, features, and bug checks for IBM builds.

    SMB (Samba via cephadm) is IBM-only. This auto-enables it for IBM builds
    so a single YAML works for both IBM and RH. Set ``disable_smb: true`` in
    YAML to suppress.
    """
    if config.get("disable_smb"):
        log.info("SMB explicitly disabled via disable_smb flag -- skipping")
        return

    ibm_build = config.get("ibm_build") or config.get("product") == "ibm"
    if not ibm_build:
        return

    log.info("IBM build detected: enabling SMB service, IO, features, and bug checks")
    smb_overlay = {
        "services": {"smb": True},
        "io_tools": {"smb": {"smbclient": True}},
        "features": {"smb": {"multiple_shares": True}},
        "bug_validations": {"a17_smb_all_down": True},
    }
    for key, val in smb_overlay.items():
        if isinstance(val, dict):
            config.setdefault(key, {}).update(val)


def _parse_size(value) -> int | None:
    """Convert a human-readable size string (e.g. '24G', '4096M') to bytes."""
    if not value:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    value = str(value).strip().upper()
    if value.endswith("B") and len(value) > 1 and value[-2] in "KMGT":
        value = value[:-1]
    multipliers = {"K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}
    for suffix, mult in multipliers.items():
        if value.endswith(suffix):
            try:
                return int(float(value[:-1]) * mult)
            except ValueError:
                return None
    try:
        return int(value)
    except ValueError:
        return None


def _report_filename(
    version_hint: str, start_time: str, emergency: bool = False
) -> str:
    """Build a short, readable report filename.

    *version_hint* is typically ``upgrade_path`` ("19.2.2-80.el9cp -> 20.2.1-290.el9cp")
    or ``hop_name`` ("Upgrade 8.1 -> 9.0").  The function extracts the first two
    ``X.Y`` or ``X.Y.Z`` version patterns and forms a ``_to_`` tag.

    Example: upgrade_report_19.2.2_to_20.2.1_20260712.html
    """
    versions = re.findall(r"(\d+\.\d+(?:\.\d+)*)", version_hint or "")
    if len(versions) >= 2:
        hop_tag = f"{versions[0]}_to_{versions[1]}"
    elif len(versions) == 1:
        hop_tag = versions[0]
    else:
        hop_tag = ""

    try:
        ts_fixed = re.sub(r"([+-]\d{2})(\d{2})$", r"\1:\2", start_time)
        date_tag = datetime.fromisoformat(ts_fixed.replace("Z", "+00:00")).strftime(
            "%Y%m%d"
        )
    except Exception:
        date_tag = re.sub(r"[^0-9]", "", str(start_time))[:8]

    parts = ["upgrade_report"]
    if hop_tag:
        parts.append(hop_tag)
    parts.append(date_tag)
    if emergency:
        parts.append("emergency")
    return "_".join(parts) + ".html"


def _run_cooldown_actions(actions, mon_cfg, rados_obj):
    """Execute cooldown actions (set_config, rm_config, run_command)."""
    for action in actions:
        atype = action.get("type")
        if atype == "set_config":
            target = action.get("target", "global")
            for key, value in action.get("configs", {}).items():
                mon_cfg.set_config(section=target, name=key, value=value, no_delay=True)
                log.info(f"  Cooldown action: set {target}/{key} = {value}")
        elif atype == "rm_config":
            target = action.get("target", "global")
            for key in action.get("keys", []):
                mon_cfg.remove_config(section=target, name=key, verify_rm=False)
                log.info(f"  Cooldown action: rm {target}/{key}")
        elif atype == "run_command":
            cmd = action.get("command", "")
            if cmd:
                log.info(f"  Cooldown action: run '{cmd}'")
                try:
                    rados_obj.node.shell([cmd])
                except Exception as e:
                    if action.get("ignore_errors", False):
                        log.warning(f"  Cooldown command returned error (ignored): {e}")
                    else:
                        raise
        else:
            log.warning(f"  Unknown cooldown action type: {atype}")


def _wait_for_monitoring_stable(rados_obj, timeout=120):
    """Wait until all monitoring daemons are running after an upgrade phase.

    Polls ``ceph orch ps`` and filters to monitoring daemon types. Returns
    when all are in ``running`` state or timeout expires.
    """
    MONITORING_TYPES = {"node-exporter", "grafana", "prometheus", "alertmanager"}
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            daemons = rados_obj.run_ceph_command(cmd="ceph orch ps", timeout=30)
            if isinstance(daemons, list):
                mon_daemons = [
                    d for d in daemons if d.get("daemon_type") in MONITORING_TYPES
                ]
                if mon_daemons and all(
                    d.get("status_desc") == "running" for d in mon_daemons
                ):
                    log.info("All monitoring daemons confirmed running")
                    return
        except Exception as e:
            log.debug(f"Monitoring daemon check failed: {e}")
        time.sleep(10)
    log.warning(f"Monitoring daemons not all running after {timeout}s timeout")


_MONITORING_IMAGE_MAP = {
    "prometheus_image": "mgr/cephadm/container_image_prometheus",
    "grafana_image": "mgr/cephadm/container_image_grafana",
    "alertmanager_image": "mgr/cephadm/container_image_alertmanager",
    "node_exporter_image": "mgr/cephadm/container_image_node_exporter",
}

_MONITORING_SERVICES = ("prometheus", "grafana", "alertmanager", "node-exporter")


def _resolve_target_rhcs_version(config):
    """Return (rhcs_version, build_type) from the upgrade target in config."""
    args = config.get("args", {})
    rhcs_version = args.get("rhcs-version")
    build_type = args.get("release", "released")
    if rhcs_version:
        return rhcs_version, build_type
    return None, build_type


def _fetch_monitoring_images(
    config, cephadm_obj=None, rhcs_version=None, build_type=None
):
    """Load monitoring container images from qe-ceph-manifest for a RHCS release."""
    product = config.get("product", "ibm")
    platform = config.get("platform", "rhel-9")

    if not rhcs_version:
        rhcs_version, build_type = _resolve_target_rhcs_version(config)
    if not build_type:
        build_type = config.get("args", {}).get("release", "released")
    if not rhcs_version and cephadm_obj is not None:
        rhcs_version, build_type = _resolve_cluster_rhcs_version(cephadm_obj)
    if not rhcs_version:
        return None

    major = rhcs_version.split(".")[0]
    versions_to_try = [rhcs_version]
    alt = f"{major}.0" if rhcs_version.endswith(".1") else f"{major}.1"
    if alt not in versions_to_try:
        versions_to_try.append(alt)

    for ver in versions_to_try:
        try:
            ctm = CephTestManifest(
                product=product,
                release=ver,
                build_type=build_type,
                platform=platform,
            )
            images = ctm.custom_images
            if images:
                log.info(f"Fetched monitoring images from manifest: {ver}/{build_type}")
                return images
        except Exception as e:
            log.debug(f"Manifest fetch failed for {ver}/{build_type}: {e}")
    return None


def _apply_monitoring_images(
    cephadm_obj,
    rados_obj,
    config,
    *,
    redeploy=True,
    rhcs_version=None,
    build_type=None,
):
    """Set mgr monitoring image configs from manifest and optionally redeploy stack.

    Non-fatal: logs warnings and returns False on failure.
    """
    images = _fetch_monitoring_images(
        config,
        cephadm_obj=cephadm_obj,
        rhcs_version=rhcs_version,
        build_type=build_type,
    )
    if not images:
        log.warning("No monitoring images found in manifest")
        return False

    configured = []
    configured_types = []
    for manifest_key, config_key in _MONITORING_IMAGE_MAP.items():
        image = images.get(manifest_key)
        if not image:
            continue
        try:
            cephadm_obj.shell(args=[f"ceph config set mgr {config_key} {image}"])
            configured.append(f"{config_key}={image}")
            # container_image_node_exporter -> node-exporter
            dtype = config_key.rsplit("container_image_", 1)[-1].replace("_", "-")
            configured_types.append(dtype)
        except Exception as e:
            log.warning(f"Failed to set {config_key}: {e}")

    if not configured:
        log.warning("No monitoring images configured from manifest")
        return False
    log.info(f"Monitoring images configured: {configured}")
    # Report classifier uses this to treat monitoring as image upgrades even
    # when orch later refreshes them without "Setting container_image for all".
    config["_monitoring_image_upgrade_types"] = configured_types

    if redeploy:
        for svc in _MONITORING_SERVICES:
            try:
                cephadm_obj.shell(args=[f"ceph orch redeploy {svc}"])
            except Exception as e:
                log.warning(f"Failed to redeploy {svc}: {e}")
        _wait_for_monitoring_stable(rados_obj, timeout=300)

    return True


def _annotate_monitoring_manifest_upgrades(timeline_result, config):
    """Mark monitoring types as image upgrades after proactive manifest apply.

    Orch may redeploy monitoring without logging Setting container_image (images
    already at target). Without this, the waterfall mislabels them as
    "redeploy / same image".
    """
    types = set(config.get("_monitoring_image_upgrade_types") or [])
    if not types or not isinstance(timeline_result, dict):
        return timeline_result

    flipped = 0
    for entry in timeline_result.get("daemon_timeline", []):
        if entry.get("daemon_type") in types and entry.get("is_redeploy"):
            entry["is_redeploy"] = False
            flipped += 1
    if flipped:
        log.info(
            "Reclassified %d monitoring timeline entries as image upgrades "
            "(manifest apply): %s",
            flipped,
            sorted(types),
        )
    return timeline_result


def _apply_target_monitoring_images(cephadm_obj, rados_obj, config):
    """Apply target-release monitoring images during upgrade prep (before orch upgrade)."""
    target_ver, target_rel = _resolve_target_rhcs_version(config)
    if not target_ver:
        log.info(
            "No target rhcs-version in config; skipping proactive monitoring image apply"
        )
        return False

    log.info(
        f"Applying target monitoring stack images for upgrade "
        f"({target_ver}/{target_rel})"
    )
    return _apply_monitoring_images(
        cephadm_obj,
        rados_obj,
        config,
        redeploy=True,
        rhcs_version=target_ver,
        build_type=target_rel,
    )


def _resolve_cluster_rhcs_version(cephadm_obj):
    """Determine RHCS version and build channel from the running cluster.

    Queries the configured container image to extract the RHCS major.minor
    version and whether the cluster uses staging (CI/nightly) or production
    (released) images.

    Returns:
        (rhcs_version, build_type) -- e.g. ("8.1", "released") or ("9.1", "rc").
        rhcs_version is None if detection fails.
    """
    try:
        out, _ = cephadm_obj.shell(args=["ceph config get mgr container_image"])
        image = out.strip()
    except Exception:
        return None, "released"

    build_type = "rc" if "stg.icr.io" in image else "released"

    try:
        image_name = image.split("/")[-1].split(":")[0]
        major = image_name.split("-")[1]
    except (IndexError, ValueError):
        return None, build_type

    try:
        tag = image.split(":")[-1]
        ver = tag.split("-")[0].lstrip("v")
        if ver.startswith(f"{major}.") and ver.count(".") == 1:
            return ver, build_type
    except Exception:
        pass

    return f"{major}.1", build_type


def _ensure_monitoring_healthy(cephadm_obj, rados_obj, config):
    """Check Prometheus reachability; fix monitoring images from manifest if broken.

    Reactive fallback only: proactive apply runs in upgrade prep via
    ``_apply_target_monitoring_images``.  When Prometheus is unreachable,
    applies images from the upgrade target manifest when available, otherwise
    from the running cluster version.

    Non-fatal: logs warnings on failure and returns False so the test can
    continue with CLI-based stats collection as a fallback.
    """
    from upgrade_thrashing.upgrade_stats_collector import _PrometheusClient

    if _PrometheusClient(rados_obj).discover():
        log.info("Prometheus reachable -- monitoring stack healthy")
        return True

    log.warning("Prometheus not reachable -- attempting monitoring image fix")

    target_ver, target_rel = _resolve_target_rhcs_version(config)
    if not _apply_monitoring_images(
        cephadm_obj,
        rados_obj,
        config,
        redeploy=True,
        rhcs_version=target_ver,
        build_type=target_rel,
    ):
        return False

    if _PrometheusClient(rados_obj).discover():
        log.info("Prometheus now healthy after monitoring image fix")
        return True

    log.warning("Prometheus still not reachable after fix -- will use CLI fallbacks")
    return False


def _record_health_snapshot(rados_obj, health_tracker):
    """Take a point-in-time health snapshot (best-effort, never raises)."""
    try:
        _h = rados_obj.run_ceph_command(cmd="ceph health detail", timeout=30)
        health_tracker.record_snapshot(get_cluster_timestamp(rados_obj.node), _h)
    except Exception:
        pass


def _build_and_generate_report(
    report,
    stats,
    health_tracker,
    config,
    daemon_timeline,
    snapshot,
    ceph_cluster,
    rados_obj,
    log_dir,
    start_time_str,
    upgrade_aborted,
    abort_reason,
    failure_reasons,
    emergency=False,
    feature_results=None,
    pre_upgrade_feature_results=None,
    bug_results=None,
    integrity_results=None,
    mount_health=None,
    io_tool_data=None,
    start_time=None,
    nfs_grace_analysis=None,
):
    """Finalize stats, populate report, and write HTML output.

    Returns (report_path, report_filename) on success, (None, None) on failure.
    """
    feature_results = feature_results or {}
    pre_upgrade_feature_results = pre_upgrade_feature_results or {}
    bug_results = bug_results or []
    integrity_results = integrity_results or {}
    mount_health = mount_health or {}
    io_tool_data = io_tool_data or {}
    nfs_grace_analysis = nfs_grace_analysis or {}

    try:
        if stats:
            _start_ts = start_time.timestamp() if start_time else time.time()
            if emergency:
                try:
                    stats.record_health_tracker(health_tracker.to_dict())
                except Exception:
                    pass
                try:
                    stats.finalize(_start_ts, time.time())
                except Exception as fin_err:
                    log.warning(f"Emergency finalize failed: {fin_err}")
            else:
                stats.record_health_tracker(health_tracker.to_dict())
                stats.finalize(_start_ts, time.time())

            stats_dump = stats.get_all_data()
            report.set_stats_data(stats_dump)

            if not emergency:
                stats_dump_path = os.path.join(log_dir, "upgrade_stats_raw.json")
                try:
                    with open(stats_dump_path, "w") as f:
                        json.dump(stats_dump, f, default=str)
                    log.info(
                        f"Raw stats saved to {stats_dump_path} "
                        f"({len(stats_dump.get('samples', []))} samples)"
                    )
                except Exception as e:
                    log.warning(f"Failed to save raw stats: {e}")
        else:
            stats_dump = {}

        report.set_feature_results(feature_results)
        report.set_pre_upgrade_feature_results(pre_upgrade_feature_results)
        report.set_bug_results(bug_results)
        report.set_integrity_results(integrity_results)
        report.set_mount_health(mount_health)
        report.set_io_tool_usage(io_tool_data)
        report.set_nfs_grace_analysis(nfs_grace_analysis)
        if isinstance(daemon_timeline, dict):
            report.set_daemon_timeline(daemon_timeline.get("daemon_timeline", []))
            report.set_upgrade_events(daemon_timeline.get("upgrade_events", []))
        else:
            report.set_daemon_timeline(daemon_timeline)
        report.set_test_start_time(start_time_str)

        for s in stats_dump.get("samples", []):
            if s.get("collector") == "health_tracker" and isinstance(
                s.get("metrics"), dict
            ):
                report.set_health_warnings(s["metrics"])
                break

        try:
            cluster_details = _collect_cluster_hardware_info(ceph_cluster, rados_obj)
            report.set_cluster_details(cluster_details)
        except Exception as e:
            if not emergency:
                log.warning(f"Cluster hardware info collection failed (non-fatal): {e}")

        if emergency:
            outcome_result = "ABORT" if upgrade_aborted else "FAIL"
            test_outcome = {
                "result": outcome_result,
                "crash_detected": bool(
                    snapshot.get("crash_found") if snapshot else False
                ),
                "upgrade_aborted": upgrade_aborted,
                "abort_reason": abort_reason if upgrade_aborted else None,
                "failure_reasons": failure_reasons or [],
                "emergency_report": True,
            }
        else:
            if upgrade_aborted:
                outcome_result = "ABORT"
            elif failure_reasons:
                outcome_result = "FAIL"
            else:
                outcome_result = "PASS"
            test_outcome = {
                "result": outcome_result,
                "crash_detected": bool(snapshot.get("crash_found")),
                "upgrade_aborted": upgrade_aborted,
                "abort_reason": abort_reason,
                "failure_reasons": failure_reasons,
                "emergency_report": False,
            }

        report.set_test_outcome(test_outcome)
        if snapshot:
            report.set_crash_details(snapshot.get("crash_details") or {})
        report.generate_log_report()

        report_filename = _report_filename(
            config.get("upgrade_path", config.get("hop_name", "")),
            start_time_str,
            emergency=upgrade_aborted if emergency else False,
        )
        report_path = os.path.join(log_dir, report_filename)
        report.generate_html_report(report_path)
        log.info(f"{'Emergency r' if emergency else 'R'}eport written to {report_path}")
        return report_path, report_filename
    except Exception as e:
        if emergency:
            log.warning(f"Emergency report generation failed: {e}")
        else:
            raise
        return None, None


def _recover_nfs_after_fail_fs(rados_obj):
    """Check NFS daemon health after fail_fs cycle and redeploy errored daemons.

    Returns the count of error/stopped daemons found.
    """
    try:
        nfs_daemons = (
            rados_obj.run_ceph_command(cmd="ceph orch ps --daemon-type nfs") or []
        )
        error_daemons = [
            d
            for d in nfs_daemons
            if d.get("status_desc", "").lower() in ("error", "stopped")
        ]
        if error_daemons:
            log.warning(
                f"NFS daemons in error state after fail_fs: "
                f"{[d.get('daemon_name') for d in error_daemons]}"
            )
            for d in error_daemons:
                daemon_name = d.get("daemon_name", "")
                log.info(f"Redeploying NFS daemon: {daemon_name}")
                try:
                    rados_obj.node.shell([f"ceph orch daemon redeploy {daemon_name}"])
                except Exception as redep_err:
                    log.warning(f"NFS redeploy {daemon_name}: {redep_err}")
            time.sleep(30)
        else:
            log.info(
                f"All {len(nfs_daemons)} NFS daemons healthy " f"after fail_fs cycle"
            )
        return len(error_daemons)
    except Exception as nfs_err:
        log.warning(f"NFS health check after fail_fs: {nfs_err}")
        return 0


def _collect_upgrade_timeline(ceph_cluster, rados_obj, stats) -> dict:
    """Collect per-daemon upgrade timeline from MGR cephadm logs."""
    try:
        _all_stats = stats.get_all_data()
        boundaries = _all_stats.get("phase_boundaries", [])
        _upg_start = ""
        _upg_end = ""
        for b in boundaries:
            if b.get("name") == "upgrade_start":
                _upg_start = b.get("timestamp", "")
            elif b.get("name") == "upgrade_end":
                _upg_end = b.get("timestamp", "")
        if not _upg_start or not _upg_end:
            return {}
        result = _collect_mgr_upgrade_timeline(
            ceph_cluster, rados_obj, _upg_start, _upg_end
        )
        if isinstance(result, dict):
            timeline = result.get("daemon_timeline", [])
            log.info(f"Daemon timeline collected: {len(timeline)} daemon types")
            return result
        # Legacy list return (shouldn't happen)
        log.info(f"Daemon timeline collected: {len(result)} daemon types")
        return result
    except Exception as e:
        log.warning(f"Daemon log collection failed (non-fatal): {e}")
        return {}


def _get_existing_nfs_clusters(rados_obj):
    """Return set of NFS cluster names that actually exist on the cluster."""
    try:
        result = rados_obj.run_ceph_command(cmd="ceph nfs cluster ls", timeout=30)
        if isinstance(result, list):
            return set(result)
    except Exception:
        pass
    return set()


def run(ceph_cluster, **kw) -> int:
    """Ceph Upgrade Thrashing Test orchestrator.

    Executes a 7-phase upgrade validation workflow across all client types
    (RGW, RBD, CephFS, NFS, SMB, NVMe-oF). The cluster must already be
    bootstrapped at N-1 version by the suite YAML.

    Phases:
        1. Client and service setup (pools, mounts, services)
        2. Feature enablement + integrity baselines (60 features, CRC writes)
        3. Stabilization + pre-flight + baseline IO collection (20 min)
        3.5. Cluster pre-fill (tops up to target % after baseline IO)
        4. Upgrade with monitoring (per-daemon timing, IO drop tracking)
        5. Post-upgrade stabilization + stats (10 min)
        6. Verification (functional tests, CRC verify, 21 bug checks)
        7. Report generation (log tables + interactive HTML)

    Args:
        ceph_cluster: Ceph cluster object from the test framework.
        **kw: Keyword arguments containing:
            config (dict): Test configuration with keys for features,
                io_tools, monitoring_intervals, cluster_fill,
                bug_validations, performance_regression thresholds,
                and phase timing. See suite YAML for all toggles.

    Returns:
        0 on success, 1 on failure (crash detected, integrity mismatch,
        upgrade aborted, or critical feature verification failure).
    """
    raw_config = kw.get("config", {})

    # Apply IO tier before deep-merge so tier defaults sit between
    # DEFAULT_CONFIG (lowest priority) and explicit YAML (highest).
    io_tier = raw_config.get("io_tier")
    if io_tier:
        from upgrade_thrashing.upgrade_io_manager import apply_io_tier

        apply_io_tier(raw_config, io_tier)
        log.info("IO tier '%s' applied to config", io_tier)

    config = _deep_merge(DEFAULT_CONFIG, raw_config)
    _apply_ibm_smb_overrides(config)
    timing = config.get("phase_timing") or {}
    run_config = kw.get("run_config", {})
    log_dir = run_config.get("log_dir", "/tmp")

    cephadm_obj = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm_obj)
    mon_cfg = MonConfigMethods(rados_obj=rados_obj)
    orch_obj = Orch(cluster=ceph_cluster, **config)

    start_time_str = None
    start_time = None
    upgrade_aborted = False
    abort_reason = None
    abort_info = {}

    from upgrade_thrashing.upgrade_feature_manager import UpgradeFeatureManager
    from upgrade_thrashing.upgrade_io_manager import UpgradeIOManager
    from upgrade_thrashing.upgrade_report import UpgradeReportGenerator
    from upgrade_thrashing.upgrade_stats_collector import UpgradeStatsCollector

    stats = UpgradeStatsCollector(rados_obj, config)
    io_mgr = UpgradeIOManager(ceph_cluster, rados_obj, config)
    report = UpgradeReportGenerator(config)
    deployed_services = set()
    feat_mgr = None
    feature_results = {}
    pre_upgrade_feature_results = {}
    bug_results = []
    integrity_results = {}
    report_generated = False
    io_tool_data = {}
    daemon_timeline = {}
    nfs_grace_analysis = {}
    nfs_client_errors = {}
    health_tracker = HealthWarningTracker()
    mount_health = {}
    snapshot = {}
    # rc is returned after finally so cleanup crash detection can still fail
    # the hop (a return inside try cannot be overridden by finally logging alone).
    rc = 1
    cleanup_crash_found = False

    try:
        start_time_str = get_cluster_timestamp(rados_obj.node)
        start_time = datetime.fromisoformat(
            re.sub(r"([+-]\d{2})(\d{2})$", r"\1:\2", start_time_str)
        )
        log.debug("Test start time: %s (raw: %s)", start_time, start_time_str)

        # ============================================================
        # Phase 1: Service Setup + IO Tool Installation
        # ============================================================
        # Ordering: services first, then tools.  Pool creation during
        # service setup starts PG distribution immediately.  The slow
        # IO tool compilation (mdtest, fsstress — 5-15 min per
        # client) then runs in parallel across all clients while PGs
        # rebalance in the background, saving ~60-75 min vs the old
        # serial-then-sequential approach.
        #
        # boto3/s3cmd are pre-installed in UpgradeIOManager.__init__
        # because _setup_rgw needs boto3 to create buckets.
        # ============================================================
        log.info("=" * 60)

        # Enable daemon logging to file for lifecycle event parsing
        if not rados_obj.enable_file_logging():
            log.warning(
                "Failed to enable file logging, lifecycle events may be incomplete"
            )
        else:
            log.info("Daemon file logging enabled (log_to_file=true)")

        pg_per_osd_limit = config.get("mon_max_pg_per_osd", 350)
        if pg_per_osd_limit:
            mon_cfg.set_config(
                section="global",
                name="mon_max_pg_per_osd",
                value=pg_per_osd_limit,
            )
            log.info(f"mon_max_pg_per_osd set to {pg_per_osd_limit}")

        if config.get("skip_service_setup"):
            log.info("Phase 1: SKIPPED (multi-hop continuation)")
            log.info("Detecting services deployed by previous hop...")
            deployed_services = _detect_deployed_services(rados_obj, config)
        else:
            log.info("Phase 1: Client and Service Setup")
            deployed_services = _setup_services(ceph_cluster, rados_obj, config)
        log.info(f"Active services: {deployed_services}")
        log.info("=" * 60)

        # -- IO tool installation (parallel across all clients) ------
        # PGs from pool creation rebalance concurrently during this.
        io_mgr.install_io_tools()

        # -- Ensure monitoring stack is healthy for stats collection --
        _ensure_monitoring_healthy(cephadm_obj, rados_obj, config)

        # -- Apply MDS configuration overrides --
        mds_config = config.get("mds_config", {})
        cache_limit_raw = mds_config.get("mds_cache_memory_limit")
        if cache_limit_raw:
            cache_limit_bytes = _parse_size(cache_limit_raw)
            if cache_limit_bytes:
                mon_cfg.set_config(
                    section="mds",
                    name="mds_cache_memory_limit",
                    value=cache_limit_bytes,
                )
                log.info(
                    f"MDS cache memory limit set to {cache_limit_raw} "
                    f"({cache_limit_bytes} bytes)"
                )
            else:
                log.warning(
                    f"Could not parse mds_cache_memory_limit: {cache_limit_raw}"
                )

        upgrade_phases = config.get("upgrade_phases")

        feat_mgr = UpgradeFeatureManager(
            rados_obj, ceph_cluster, config, deployed_services
        )

        clients = ceph_cluster.get_nodes(role="client")
        if not clients:
            log.error("No client nodes found in cluster")
            return 1

        # ============================================================
        # Phase 2: Feature Enablement + Integrity Baselines
        # ============================================================
        log.info("=" * 60)
        log.info("Phase 2: Feature Enablement + Integrity Baselines")
        log.info("=" * 60)

        feat_mgr.enable_all_features()
        log.info("All enabled features configured successfully")

        pre_upgrade_feature_results = feat_mgr.verify_all_features({})
        _counts = Counter(r["result"] for r in pre_upgrade_feature_results.values())
        pre_pass, pre_fail, pre_skip = (
            _counts["pass"],
            _counts["fail"],
            _counts["skip"],
        )
        log.info(
            f"Pre-upgrade feature verification: "
            f"{pre_pass} pass, {pre_fail} fail, {pre_skip} skip"
        )
        if pre_fail > 0:
            failed_features = [
                k
                for k, v in pre_upgrade_feature_results.items()
                if v["result"] == "fail"
            ]
            log.error(
                f"Features failed enablement verification: {failed_features}. "
                "These will be flagged in the report for root-cause analysis."
            )
            for feat in failed_features:
                log.error(f"  {feat}: {pre_upgrade_feature_results[feat]['details']}")

        io_mgr.write_baseline_with_integrity(clients, deployed_services)
        log.info("Integrity baselines written through active feature paths")

        # Pin PG counts on ALL pools to prevent autoscaler splits/merges.
        # Runs unconditionally (both Hop 1 and Hop 2) and after feature
        # enablement so feature-manager pools are also caught.
        _pin_all_pool_pg_counts(rados_obj)
        _wait_for_stabilization(rados_obj, timing["stabilization_timeout_sec"])

        # ============================================================
        # Phase 3: Stabilization + Pre-flight + Baseline Stats
        # ============================================================
        log.info("=" * 60)
        log.info("Phase 3: Stabilization + Pre-flight + Baseline Stats")
        log.info("=" * 60)

        _wait_for_stabilization(rados_obj, timing["stabilization_timeout_sec"])

        _run_preflight_checks(rados_obj, orch_obj, config)

        feat_mgr.save_pre_upgrade_snapshot()
        feat_mgr.capture_daemon_state("pre")
        log.info("Pre-upgrade state snapshot saved")

        # -- Set fail_fs if configured (MDS upgrade strategy) --
        if mds_config.get("fail_fs"):
            log.info(
                "Setting mgr/orchestrator/fail_fs=true -- cephadm will fail all "
                "CephFS filesystems and upgrade MDS simultaneously"
            )
            mon_cfg.set_config(
                section="mgr",
                name="mgr/orchestrator/fail_fs",
                value="true",
            )

        pre_ver, _ = cephadm_obj.shell(args=["ceph version --format json"])
        pre_ver_str = _parse_ceph_version(pre_ver)
        config["pre_version"] = pre_ver_str

        upgrade_orch = _prepare_upgrade_context(ceph_cluster, cephadm_obj, config)
        log.info("Upgrade context prepared (image resolved, repos set, RPMs upgraded)")

        _apply_target_monitoring_images(cephadm_obj, rados_obj, config)

        target_ver = config.get("args", {}).get("rhcs-version", "latest")
        target_rel = config.get("args", {}).get("release", "")
        config["post_version"] = (
            f"{target_ver}-{target_rel}" if target_rel else target_ver
        )
        config["upgrade_path"] = f"{pre_ver_str} -> {config['post_version']}"

        hop_name = config.get("hop_name", "")
        if not hop_name:
            hop_name = f"Upgrade {config['pre_version']} -> {config['post_version']}"
            config["hop_name"] = hop_name
            config["_hop_name_auto"] = True

        all_nodes = ceph_cluster.get_nodes()
        client_nodes = ceph_cluster.get_nodes(role="client")
        config["cluster_nodes"] = len(all_nodes) - len(client_nodes) if all_nodes else 0
        config["client_count"] = len(client_nodes)

        log.info(
            f"Report metadata: {config['upgrade_path']} | "
            f"{config['cluster_nodes']} nodes | hop: {hop_name}"
        )

        fill_config = config.get("cluster_fill", {})

        if config.get("io_tier") == "none":
            log.info("IO tier is 'none' -- skipping background IO")
        else:
            io_mgr.start_background_io(clients, deployed_services)
            log.info("Background IO started on all clients")
            io_mgr.start_capacity_guard(
                max_percent=fill_config.get("bg_io_max_percent", 70),
                check_interval=fill_config.get("bg_io_check_interval_sec", 120),
            )

        _record_health_snapshot(rados_obj, health_tracker)

        stats.begin_phase("baseline")
        baseline_sec = timing["pre_upgrade_baseline_sec"]
        log.info(f"Baseline phase: sleeping {baseline_sec}s for IO stabilization")
        _sleep_with_io_sampling(stats, baseline_sec, interval=60)

        # ============================================================
        # Phase 3.5: Cluster Pre-Fill (after baseline, before upgrade)
        # ============================================================
        # Runs AFTER baseline so that data written by background IO during
        # baseline collection counts toward the fill target.  The fill
        # function checks current usage and only tops up the difference.
        if config.get("io_tier") == "none":
            log.info("Phase 3.5: IO tier is 'none' -- skipping cluster fill")
        elif fill_config.get("enabled", False):
            log.info("=" * 60)
            log.info("Phase 3.5: Cluster Pre-Fill")
            log.info("=" * 60)
            io_mgr.fill_cluster(fill_config, deployed_services)
            log.info("Cluster pre-fill complete")
        else:
            log.info("Phase 3.5: Cluster pre-fill disabled, skipping")

        # ============================================================
        # Phase 4: Upgrade with Monitoring
        # ============================================================
        log.info("=" * 60)
        log.info("Phase 4: Upgrade with Monitoring")
        log.info("=" * 60)

        stats.begin_phase("upgrade")
        stats.tag_phase_boundary(
            "upgrade_start", datetime.now(timezone.utc).isoformat()
        )

        pre_upgrade_versions = _get_version_snapshot(rados_obj)
        log.info(f"Pre-upgrade version snapshot: {pre_upgrade_versions}")

        target_ver = config.get("_target_ceph_version", "")
        if target_ver and pre_upgrade_versions:
            running_versions = set()
            for daemon_type, ver_dict in pre_upgrade_versions.items():
                running_versions.update(ver_dict.keys())
            if running_versions and all(
                _normalize_ver(v) == _normalize_ver(target_ver)
                for v in running_versions
            ):
                raise RuntimeError(
                    f"All daemons already at target version '{target_ver}'. "
                    f"Wrong test entry or manifest? Refusing to start no-op upgrade."
                )

        if upgrade_phases:
            num_phases = len(upgrade_phases)
            for phase_idx, phase_spec in enumerate(upgrade_phases):
                phase_num = phase_idx + 1
                is_last_phase = phase_idx == num_phases - 1
                phase_daemon_types = phase_spec.get("daemon_types")

                original_args = None
                if phase_daemon_types:
                    log.info(f"Phase 4.{phase_num}: Upgrading {phase_daemon_types}")
                    original_args = config["args"]
                    config["args"] = {
                        **original_args,
                        "daemon_types": phase_daemon_types,
                    }
                else:
                    log.info(f"Phase 4.{phase_num}: Upgrading all remaining daemons")

                stats.tag_phase_boundary(
                    f"upgrade_phase_{phase_num}_start",
                    datetime.now(timezone.utc).isoformat(),
                )
                _start_upgrade(upgrade_orch, config)
                upgrade_aborted = _monitor_upgrade(
                    orch_obj,
                    rados_obj,
                    stats,
                    config,
                    pre_upgrade_versions,
                    health_tracker=health_tracker,
                    skip_noop_check=(
                        phase_daemon_types is not None and not is_last_phase
                    ),
                    abort_info=abort_info,
                )

                if original_args is not None:
                    config["args"] = original_args

                stats.tag_phase_boundary(
                    f"upgrade_phase_{phase_num}_end",
                    datetime.now(timezone.utc).isoformat(),
                )

                if upgrade_aborted:
                    break

                cooldown_sec = phase_spec.get("cooldown_sec", 0)
                cooldown_actions = phase_spec.get("cooldown_actions", [])

                _wait_for_monitoring_stable(rados_obj)

                if cooldown_sec > 0 or cooldown_actions:
                    stats.tag_phase_boundary(
                        f"upgrade_cooldown_{phase_num}_start",
                        datetime.now(timezone.utc).isoformat(),
                    )

                    if cooldown_actions:
                        _run_cooldown_actions(cooldown_actions, mon_cfg, rados_obj)

                    if cooldown_sec > 0:
                        log.info(f"Cooldown: waiting {cooldown_sec}s")
                        time.sleep(cooldown_sec)

                    stats.tag_phase_boundary(
                        f"upgrade_cooldown_{phase_num}_end",
                        datetime.now(timezone.utc).isoformat(),
                    )
        else:
            _start_upgrade(upgrade_orch, config)

            upgrade_aborted = _monitor_upgrade(
                orch_obj,
                rados_obj,
                stats,
                config,
                pre_upgrade_versions,
                health_tracker=health_tracker,
                abort_info=abort_info,
            )

        abort_reason = abort_info.get("reason")

        stats.tag_phase_boundary("upgrade_end", datetime.now(timezone.utc).isoformat())

        if abort_reason and abort_reason.startswith("MONITORING_SKIP:"):
            log.info(
                "Monitoring daemon blocked upgrade -- skipped " "(not a test failure)"
            )
        elif upgrade_aborted:
            log.warning("Upgrade was aborted -- entering degraded mode")
        else:
            log.info("Upgrade completed successfully")
            _wait_for_monitoring_stable(rados_obj)
            _handle_post_upgrade_license(ceph_cluster, cephadm_obj, config)

        # Unset fail_fs regardless of success/abort to avoid leaving FS stuck.
        # Duplicated in `finally` as a silent safety net; kept here for diagnostic
        # logging and because NFS recovery below depends on fail_fs being unset.
        if mds_config.get("fail_fs"):
            _label = "after upgrade" if not upgrade_aborted else "after abort"
            log.info(f"Unsetting mgr/orchestrator/fail_fs {_label}")
            try:
                mon_cfg.set_config(
                    section="mgr",
                    name="mgr/orchestrator/fail_fs",
                    value="false",
                    no_delay=True,
                )
            except Exception as e:
                log.error(f"Failed to unset fail_fs: {e}")

        # Remove cooldown action configs. Duplicated in `finally` as a silent
        # safety net; kept here for per-key success/failure logging.
        if upgrade_phases:
            _label = "after upgrade" if not upgrade_aborted else "after abort"
            log.info(f"Removing cooldown action configs {_label}")
            for phase_spec in upgrade_phases:
                for action in phase_spec.get("cooldown_actions", []):
                    if action.get("type") == "set_config":
                        target = action.get("target", "global")
                        for key in action.get("configs", {}):
                            try:
                                mon_cfg.remove_config(
                                    section=target, name=key, verify_rm=False
                                )
                                log.info(f"  Removed {target}/{key}")
                            except Exception as e:
                                log.warning(f"  Failed to remove {target}/{key}: {e}")

        if mds_config.get("fail_fs"):
            _recover_nfs_after_fail_fs(rados_obj)

        post_ver, _ = cephadm_obj.shell(args=["ceph version --format json"])
        post_ver_str = _parse_ceph_version(post_ver)
        config["post_version"] = post_ver_str
        config["upgrade_path"] = (
            f"{config.get('pre_version', 'unknown')} -> {post_ver_str}"
        )
        if config.get("_hop_name_auto"):
            config["hop_name"] = (
                f"Upgrade {config.get('pre_version', 'unknown')} -> {post_ver_str}"
            )
        log.info(f"Post-upgrade version: {post_ver_str}")

        post_versions = _get_version_snapshot(rados_obj)
        if post_versions:
            mixed_types = [
                dtype
                for dtype, ver_dict in post_versions.items()
                if isinstance(ver_dict, dict)
                and len(ver_dict) > 1
                and dtype in ("mon", "mgr", "osd", "mds", "rgw")
            ]
            if mixed_types:
                config["_mixed_version_cluster"] = True
                log.warning(
                    f"Mixed-version cluster detected: {mixed_types}. "
                    f"Phase 6 CephFS/NFS feature checks will be skipped."
                )

        # daemon_timeline + NFS grace analysis collected in Phase 6 Step 6

        # Restore default cephadm module logging
        for cfg_cmd in [
            "ceph config rm mgr mgr/cephadm/log_to_cluster_level",
            "ceph config rm mgr mgr/cephadm/log_level",
        ]:
            try:
                rados_obj.node.shell([cfg_cmd])
            except Exception:
                pass
        # Keep log_to_cluster=true (harmless, useful for future diagnostics)

        if not upgrade_aborted and cephx_remediation_enabled(config):
            log.info("=" * 60)
            log.info("CephX post-upgrade key rotation remediation")
            log.info("=" * 60)
            cephx_result = remediate_cephx_auth_warnings(
                cephadm_obj,
                rados_obj,
                ceph_cluster,
                io_mgr=io_mgr,
                clients=clients,
                deployed_services=deployed_services,
                config=config,
                stats=stats,
            )
            if cephx_result.get("status") == "failed":
                log.error(
                    "CephX remediation failed: %s",
                    cephx_result.get("errors"),
                )
                return 1

        # ============================================================
        # Phase 5: Post-Upgrade Stabilization + Stats
        # ============================================================
        log.info("=" * 60)
        log.info("Phase 5: Post-Upgrade Stabilization + Stats")
        log.info("=" * 60)

        _record_health_snapshot(rados_obj, health_tracker)

        if not upgrade_aborted:
            _wait_for_stabilization(rados_obj, timing["post_upgrade_stabilization_sec"])

        stats.begin_phase("post_upgrade")
        post_baseline_sec = timing["post_upgrade_baseline_sec"]
        log.info(
            f"Post-upgrade phase: sleeping {post_baseline_sec}s for IO stabilization"
        )
        _sleep_with_io_sampling(stats, post_baseline_sec, interval=60)
        stats.begin_phase("complete")

        # ============================================================
        # Phase 6: Verification
        # ============================================================
        log.info("=" * 60)
        log.info("Phase 6: Verification")
        log.info("=" * 60)

        _record_health_snapshot(rados_obj, health_tracker)

        io_mgr.stop_capacity_guard()

        # Step 1: Stop IO and collect client-side tool outputs
        io_mgr.stop_io_processes()
        quiesce_sec = timing["io_quiesce_sec"]
        log.info(f"IO stopped, quiescing for {quiesce_sec}s")
        time.sleep(quiesce_sec)
        io_tool_data = io_mgr.collect_io_outputs()

        # Step 1.5: Verify mount health before integrity checks
        mount_health = io_mgr.check_mount_health(clients, deployed_services)
        remounted = sum(
            len(v.get("stale_remounted", [])) for v in mount_health.values()
        )
        unrecoverable = sum(
            len(v.get("stale_unrecoverable", [])) for v in mount_health.values()
        )
        stale_count = sum(len(v.get("stale", [])) for v in mount_health.values())
        if remounted:
            log.warning(
                f"{remounted} stale mount(s) recovered via remount "
                "-- integrity checks will proceed on recovered mounts"
            )
        if unrecoverable:
            log.error(
                f"{unrecoverable} stale mount(s) could NOT be remounted "
                "-- integrity results may be unreliable"
            )
        if stale_count:
            log.error(
                f"{stale_count} stale/broken mount(s) detected (SMB or unknown) "
                "-- integrity results may be unreliable"
            )

        if not upgrade_aborted:
            _wait_for_daemon_recovery(
                feat_mgr,
                rados_obj,
                timing.get("daemon_recovery_timeout_sec", 600),
            )

        # Step 2: Collect state snapshot
        snapshot = _collect_state_snapshot(rados_obj, start_time_str)
        feat_mgr.capture_daemon_state("post")

        # Step 3: Parallel non-destructive verification
        feature_results = feat_mgr.verify_all_features(snapshot)
        integrity_results = io_mgr.verify_all_integrity(clients, deployed_services)

        # Step 4: Deep scrub (initiate and wait)
        _run_deep_scrub(rados_obj)

        # Step 5: mClock functional test (destructive, skip in degraded)
        if not upgrade_aborted:
            _run_mclock_functional_test(rados_obj, config)

        # Step 6: Bug validations (timeline + grace analysis before validators)
        daemon_timeline = _collect_upgrade_timeline(ceph_cluster, rados_obj, stats)
        _annotate_monitoring_manifest_upgrades(daemon_timeline, config)
        grace_cfg = config.get("nfs_grace_validation") or {}
        if grace_cfg.get("enabled"):
            nfs_grace_analysis = analyze_nfs_grace(daemon_timeline, config)
            nfs_client_errors = io_mgr.scrape_nfs_io_errors()
        else:
            nfs_grace_analysis = {}
            nfs_client_errors = {"hits": [], "count": 0}
        monitoring_data = stats.get_all_data()
        monitoring_data["mount_health"] = mount_health
        monitoring_data["nfs_client_errors"] = nfs_client_errors
        monitoring_data["nfs_grace_analysis"] = nfs_grace_analysis
        monitoring_data["daemon_timeline"] = daemon_timeline
        bug_results = feat_mgr.validate_known_bugs(
            monitoring_data, upgrade_completed=not upgrade_aborted
        )

        # Post-upgrade checks
        _run_post_upgrade_checks(rados_obj, orch_obj, snapshot)

        # ============================================================
        # Phase 7: Report Generation
        # ============================================================
        log.info("=" * 60)
        log.info("Phase 7: Report Generation")
        log.info("=" * 60)

        _record_health_snapshot(rados_obj, health_tracker)

        has_failures, failure_reasons = _check_for_failures(
            feature_results,
            integrity_results,
            bug_results,
            snapshot,
            pre_upgrade_feature_results=pre_upgrade_feature_results,
            mount_health=mount_health,
        )

        report_path, report_filename = _build_and_generate_report(
            report=report,
            stats=stats,
            health_tracker=health_tracker,
            config=config,
            daemon_timeline=daemon_timeline,
            snapshot=snapshot,
            ceph_cluster=ceph_cluster,
            rados_obj=rados_obj,
            log_dir=log_dir,
            start_time_str=start_time_str,
            upgrade_aborted=upgrade_aborted,
            abort_reason=abort_reason,
            failure_reasons=failure_reasons,
            emergency=False,
            feature_results=feature_results,
            pre_upgrade_feature_results=pre_upgrade_feature_results,
            bug_results=bug_results,
            integrity_results=integrity_results,
            mount_health=mount_health,
            io_tool_data=io_tool_data,
            start_time=start_time,
            nfs_grace_analysis=nfs_grace_analysis,
        )
        report_generated = True

        hop_label = config.get("upgrade_path", config.get("hop_name", "Upgrade"))
        raw_config["artifacts"] = f"Upgrade Report ({hop_label}): {report_filename}"

        perf_regression = report.check_performance_regression()
        if perf_regression.get("gate") == "warn":
            log.warning(
                "Performance regression detected (WARN only): "
                f"{json.dumps(perf_regression, indent=2)}"
            )

        # Final crash check -- catches crashes during cleanup or late in the run
        try:
            new_crashes = rados_obj.run_ceph_command(cmd="ceph crash ls-new")
            if new_crashes:
                log.error(
                    f"Post-cleanup crash check: {len(new_crashes)} new crash(es) detected"
                )
                for c in new_crashes:
                    log.error(
                        f"  Crash: {c.get('crash_id', '?')} on {c.get('entity_name', '?')}"
                    )
                has_failures = True
        except Exception as crash_err:
            log.warning(f"Could not check for new crashes: {crash_err}")

        if upgrade_aborted:
            log.error("Test completed in degraded mode (upgrade aborted)")
            rc = 1
        elif has_failures:
            rc = 1
        else:
            rc = 0

    except Exception as e:
        log.error(f"Upgrade thrash test failed with exception: {e}")
        log.error(traceback.format_exc())
        rc = 1

    finally:
        # Safety net: always unset fail_fs if it was set
        try:
            _mds_cfg = config.get("mds_config", {})
            if _mds_cfg.get("fail_fs"):
                mon_cfg.set_config(
                    section="mgr",
                    name="mgr/orchestrator/fail_fs",
                    value="false",
                    no_delay=True,
                )
        except Exception:
            pass

        # Safety net: always remove cooldown action configs
        try:
            for ps in config.get("upgrade_phases", []):
                for act in ps.get("cooldown_actions", []):
                    if act.get("type") == "set_config":
                        for _k in act.get("configs", {}):
                            mon_cfg.remove_config(
                                section=act.get("target", "global"),
                                name=_k,
                                verify_rm=False,
                            )
        except Exception:
            pass

        skip_client_cleanup = config.get("skip_client_cleanup", False)

        log.info("=" * 60)
        if skip_client_cleanup:
            log.info(
                "Cleanup: skip_client_cleanup=True (multi-hop), "
                "preserving mounts and maps for next hop"
            )
        else:
            log.info("Cleanup: Restoring cluster state")
        log.info("=" * 60)

        if not report_generated:
            _, emerg_filename = _build_and_generate_report(
                report=report,
                stats=stats,
                health_tracker=health_tracker,
                config=config,
                daemon_timeline=daemon_timeline,
                snapshot=snapshot,
                ceph_cluster=ceph_cluster,
                rados_obj=rados_obj,
                log_dir=log_dir,
                start_time_str=start_time_str,
                upgrade_aborted=upgrade_aborted,
                abort_reason=abort_reason,
                failure_reasons=cephx_failure_reasons(
                    config.get("cephx_remediation_result")
                ),
                emergency=True,
                feature_results=feature_results,
                pre_upgrade_feature_results=pre_upgrade_feature_results,
                bug_results=bug_results,
                integrity_results=integrity_results,
                mount_health=mount_health,
                io_tool_data=io_tool_data,
                start_time=start_time,
                nfs_grace_analysis=nfs_grace_analysis,
            )
            if emerg_filename:
                raw_config["artifacts"] = (
                    f"Upgrade Report (Emergency): {emerg_filename}"
                )

        # Always stop IO processes -- these are transient and will
        # be recreated by the next hop.
        try:
            io_mgr.stop_io_processes()
        except Exception as e:
            log.warning(f"IO process stop failed: {e}")

        try:
            io_mgr.kill_all_registered_processes()
        except Exception as e:
            log.warning(f"Force-kill failed: {e}")

        # Mount/map cleanup: skip when skip_client_cleanup=True so the next
        # hop can reuse existing CephFS, NFS, SMB mounts and RBD maps.
        if not skip_client_cleanup:
            if config.get("mon_max_pg_per_osd"):
                try:
                    mon_cfg.remove_config(
                        section="global",
                        name="mon_max_pg_per_osd",
                        verify_rm=False,
                    )
                    log.info("mon_max_pg_per_osd reset to default (250)")
                except Exception as e:
                    log.warning(f"mon_max_pg_per_osd reset failed: {e}")

            try:
                client_teardown_ok = io_mgr.cleanup_mounts_and_connections()
            except Exception as e:
                log.warning(f"Mount cleanup failed: {e}")
                client_teardown_ok = False
        else:
            log.info("Skipping mount/map cleanup -- mounts persist for next hop")
            client_teardown_ok = True

        # Never delete backends while a reachable client still has storage
        # mounts (deleting under stuck mounts wedged cali015).
        if not config.get("skip_cluster_cleanup", False):
            if not skip_client_cleanup and not client_teardown_ok:
                log.error(
                    "Skipping cluster resource cleanup: client mounts remain "
                    "(see [cleanup] logs above)"
                )
            else:
                try:
                    _cleanup_cluster_resources(ceph_cluster, rados_obj, config)
                except Exception as e:
                    log.warning(f"Cluster resource cleanup failed: {e}")

        # Restore container signature policy if it was relaxed
        if config.get("upgrade_params", {}).get("relax_signature_policy"):
            try:
                _restore_container_signature_policy(ceph_cluster.get_nodes())
            except Exception as e:
                log.warning(f"Signature policy restore failed: {e}")

        end_time = get_cluster_timestamp(rados_obj.node)
        try:
            if rados_obj.check_crash_status(
                start_time=start_time_str,
                end_time=end_time,
                check_logs=False,
            ):
                log.error("Crashes detected during test execution")
                cleanup_crash_found = True
        except Exception as e:
            log.warning(f"Final crash check failed: {e}")

        # Give residual Paramiko SSH sessions time to drain before run.py
        # continues into collect_recipe() on the same cluster transports.
        # 30s is sufficient for TCP graceful close; crash check and cleanup are done.
        log.info(
            "Sleeping 30s to let residual SSH connections settle "
            "before returning to run.py"
        )
        time.sleep(30)

    hop = config.get("hop_name", config.get("upgrade_path", "?"))
    services = sorted(deployed_services)
    if cleanup_crash_found:
        log.error(
            "TEST FAILED - upgrade thrash hop=%s services=%s crashes_in_cleanup=true",
            hop,
            services,
        )
        return 1
    if rc == 0:
        log.info(
            "TEST PASSED - upgrade thrash hop=%s services=%s",
            hop,
            services,
        )
    else:
        log.error(
            "TEST FAILED - upgrade thrash hop=%s services=%s",
            hop,
            services,
        )
    return rc


# ================================================================
# Phase Helper Functions
# ================================================================


def _parse_ceph_version(raw_json_str: str) -> str:
    """Extract short version string from ``ceph version --format json`` output.

    Returns just the version+build token (e.g. ``19.2.1-375.el9cp``)
    rather than the full ``ceph version ... (hash) release (stability)`` string.
    Falls back to the raw stripped string on parse failure.
    """
    try:
        ver = json.loads(raw_json_str.strip()).get("version", "unknown")
        if ver.startswith("ceph version "):
            ver = ver[len("ceph version ") :]
        return ver.split()[0]
    except (json.JSONDecodeError, AttributeError):
        return raw_json_str.strip() if raw_json_str else "unknown"


_VERSION_RE = re.compile(r"(\d+\.\d+\.\d+)")


def _extract_version_number(text: str) -> str:
    """Extract X.Y.Z version from ceph version strings including tentacle format.

    Handles strings like 'ceph version 20.1.0-221.el9cp (hash) tentacle (stable)'
    without triggering framework extract_version() warnings.
    """
    m = _VERSION_RE.search(text)
    return m.group(1) if m else ""


def _ceph_version_tuple(ver_str: str) -> tuple:
    """Return ``(major, minor, patch)`` from a version string like ``19.2.1-375``."""
    m = _VERSION_RE.search(ver_str)
    if not m:
        return (0, 0, 0)
    parts = m.group(1).split(".")
    return tuple(int(p) for p in parts)


def _get_nfs_eligible_hosts(rados_obj) -> list:
    """Return hostnames eligible for NFS daemon placement.

    Prefers hosts with the ``nfs`` label.  Falls back to all schedulable
    hosts (excluding ``_no_schedule``) only if no ``nfs``-labelled hosts
    exist.  Returns an empty list on failure.
    """
    try:
        hosts = rados_obj.run_ceph_command(cmd="ceph orch host ls")
        if not isinstance(hosts, list):
            return []
        nfs_labelled = [
            h["hostname"]
            for h in hosts
            if "nfs" in h.get("labels", [])
            and "_no_schedule" not in h.get("labels", [])
            and h.get("hostname")
        ]
        if nfs_labelled:
            return nfs_labelled
        return [
            h["hostname"]
            for h in hosts
            if "_no_schedule" not in h.get("labels", []) and h.get("hostname")
        ]
    except Exception:
        pass
    return []


def _setup_services(ceph_cluster, rados_obj, config):
    """
    Phase 1: Set up pools and services based on config toggles.
    Returns set of successfully deployed service names.
    """
    deployed = set()
    services_config = config.get("services", {})

    # Always create pools -- they're needed by all services
    try:
        _create_pools(rados_obj, config)
        deployed.add("rados")
    except Exception as e:
        log.error(f"Pool creation failed: {e}")
        raise

    service_setup_map = {
        "rbd": _setup_rbd,
        "cephfs": _setup_cephfs,
        "rgw": _setup_rgw,
        "nfs": _setup_nfs,
        "smb": _setup_smb,
    }

    for svc_name, setup_fn in service_setup_map.items():
        if not services_config.get(svc_name, False):
            log.info(f"Service {svc_name} disabled in config, skipping")
            continue
        try:
            setup_fn(ceph_cluster, rados_obj, config)
            deployed.add(svc_name)
            log.info(f"Service {svc_name} deployed successfully")
        except Exception as e:
            log.warning(
                f"Service {svc_name} failed to deploy: {e}. " "Continuing without it."
            )

    return deployed


def _detect_deployed_services(rados_obj, config):
    """Discover already-deployed services by querying the live cluster.

    Used in multi-hop mode (``skip_service_setup: true``) where Phase 1 is
    skipped because a previous hop already set up all services.  Returns the
    same ``set[str]`` that ``_setup_services`` would have returned.

    Makes 3 commands: list_pools, rbd ls, ceph orch ls.
    """
    deployed = set()
    services_config = config.get("services", {})

    # 1. RADOS: check for our test pool
    try:
        if "rep_pool" in rados_obj.list_pools():
            deployed.add("rados")
    except Exception:
        log.warning("Could not list pools during service detection")

    # 2. RBD: check for test images in rep_pool
    try:
        out, _ = rados_obj.node.shell(["rbd ls rep_pool --format json"])
        images = json.loads(out) if out.strip() else []
        if images:
            deployed.add("rbd")
    except Exception:
        pass

    # 3. Orch services: single ceph orch ls call covers cephfs, rgw, nfs,
    #    smb, nvmeof by checking service_type and running count.
    _SVC_TYPE_MAP = {
        "mds": "cephfs",
        "rgw": "rgw",
        "nfs": "nfs",
        "smb": "smb",
        "nvmeof": "nvmeof",
    }
    try:
        orch_services = rados_obj.run_ceph_command(cmd="ceph orch ls")
        if isinstance(orch_services, list):
            for svc_entry in orch_services:
                svc_type = svc_entry.get("service_type", "")
                mapped = _SVC_TYPE_MAP.get(svc_type)
                if not mapped:
                    continue
                if not services_config.get(mapped, mapped == "cephfs"):
                    continue
                running = svc_entry.get("status", {}).get("running", 0)
                if running > 0:
                    deployed.add(mapped)
    except Exception:
        log.warning("Could not query orch services during service detection")

    log.info(f"Multi-hop: detected services = {deployed}")
    return deployed


def _compute_pool_pg_num(
    num_osds: int,
    pool_type: str = "replicated",
    repl_size: int = 3,
    ec_k: int = 0,
    ec_m: int = 0,
    target_pgs_per_osd: int = 100,
    num_pools: int = 25,
) -> int:
    """Compute a cluster-appropriate pg_num as a power of 2.

    Uses the upstream formula:
        total_pgs = num_osds * target_pgs_per_osd
        per_pool  = total_pgs / data_copies / num_pools
        pg_num    = next_power_of_2(per_pool), clamped to [32, 512]
    """
    data_copies = (ec_k + ec_m) if pool_type == "erasure" else repl_size
    if data_copies <= 0:
        data_copies = repl_size
    num_pools = max(num_pools, 1)
    raw = (num_osds * target_pgs_per_osd) / data_copies / num_pools
    pg_num = 1
    while pg_num < raw:
        pg_num <<= 1
    return max(32, min(pg_num, 512))


def _create_pools(rados_obj, config):
    """Create replicated, EC, and compressed pools for the upgrade test.

    Pools created:
        - rep_pool, rep_quota_pool, rep_compress_snappy, rep_compress_zstd,
          upgrade_integrity_pool (replicated, dynamically sized PGs)
        - ec_k2m2_pool (erasure k=2 m=2 jerasure)
        - ec_k4m2_pool (erasure k=4 m=2 isa)

    PG counts default to 512 (overridable via pool_pg_num config).
    pg_num_min is set at creation to anchor the PG floor.
    """
    num_osds = len(rados_obj.run_ceph_command(cmd="ceph osd ls"))
    default_pg = 512 if num_osds >= 30 else _compute_pool_pg_num(num_osds)
    pg_num = config.get("pool_pg_num", default_pg)
    pg_max = pg_num * 2
    log.info(f"Pool pg_num: {pg_num}, pg_num_max: {pg_max} ({num_osds} OSDs)")

    pool_specs = [
        {
            "pool_name": "rep_pool",
            "pg_num": pg_num,
            "pg_num_min": pg_num,
            "pg_num_max": pg_max,
            "pool_type": "replicated",
            "app_name": "rbd",
        },
        {
            "pool_name": "rep_quota_pool",
            "pg_num": pg_num,
            "pg_num_min": pg_num,
            "pg_num_max": pg_max,
            "pool_type": "replicated",
        },
        {
            "pool_name": "rep_compress_snappy",
            "pg_num": pg_num,
            "pg_num_min": pg_num,
            "pg_num_max": pg_max,
            "pool_type": "replicated",
        },
        {
            "pool_name": "rep_compress_zstd",
            "pg_num": pg_num,
            "pg_num_min": pg_num,
            "pg_num_max": pg_max,
            "pool_type": "replicated",
        },
        {
            "pool_name": "upgrade_integrity_pool",
            "pg_num": pg_num,
            "pg_num_min": pg_num,
            "pg_num_max": pg_max,
            "pool_type": "replicated",
        },
    ]

    for spec in pool_specs:
        try:
            rados_obj.create_pool(**spec)
        except Exception as e:
            log.warning(f"Pool {spec['pool_name']} creation: {e}")

    ec_pool_specs = [
        {
            "pool_name": "ec_k2m2_pool",
            "profile_name": "ec_k2m2",
            "plugin": "jerasure",
            "k": 2,
            "m": 2,
            "app_name": "rbd",
        },
        {
            "pool_name": "ec_k4m2_pool",
            "profile_name": "ec_k4m2",
            "plugin": "isa",
            "k": 4,
            "m": 2,
        },
    ]
    for ep in ec_pool_specs:
        try:
            rados_obj.create_erasure_pool(**ep)
            # pg_num_min set after creation to avoid CLI flag-ordering
            # issues with the `erasure` positional arg.  node.shell()
            # used because pool-set returns plain text.
            pool_name = ep["pool_name"]
            ec_default = (
                512
                if num_osds >= 30
                else _compute_pool_pg_num(
                    num_osds,
                    pool_type="erasure",
                    ec_k=ep["k"],
                    ec_m=ep["m"],
                )
            )
            ec_pg = config.get("pool_pg_num", ec_default)
            _set_pool_pg_params(rados_obj, pool_name, ec_pg, ec_pg * 2)
        except Exception as e:
            log.warning(f"EC pool {ep['pool_name']}: {e}")

    log.info(f"Pool creation verified: {len(rados_obj.list_pools())} pools exist")


def _set_pool_pg_params(rados_obj, pool_name, pg_target, pg_ceiling):
    """Set PG num/min/max/pgp on a pool. Returns count of successful sets."""
    try:
        out, _ = rados_obj.node.shell(
            [f"ceph osd pool get {pool_name} pg_num --format json"]
        )
        current_pg = json.loads(out.strip()).get("pg_num", 0)
        if current_pg and current_pg > pg_ceiling:
            log.info(
                f"{pool_name}: autoscaler already set pg_num={current_pg}, "
                f"adjusting pg_num_max from {pg_ceiling} to {current_pg * 2}"
            )
            pg_ceiling = current_pg * 2
    except Exception:
        pass

    ok = 0
    for param, val in [
        ("pg_num_max", pg_ceiling),
        ("pg_num", pg_target),
        ("pgp_num", pg_target),
        ("pg_num_min", pg_target),
    ]:
        try:
            rados_obj.node.shell([f"ceph osd pool set {pool_name} {param} {val}"])
            ok += 1
        except Exception as e:
            log.warning(f"{pool_name}: {param}={val} failed: {e}")
    return ok


def _setup_rbd(ceph_cluster, rados_obj, config):
    """Phase 1: Create RBD images on rep and EC data pools.

    Scale is driven by ``config["scale"]["rbd"]``:
        image_count (int): Total images to create (default 100).
        image_size (str): Size per image (default "1G").
    Special-purpose images (integrity, luks, qos, etc.) are always created.
    The remaining quota is filled with numbered workload images.
    """
    log.info("Setting up RBD images")
    clients = ceph_cluster.get_nodes(role="client")
    if not clients:
        raise RuntimeError("No client nodes for RBD setup")

    rbd_scale = config.get("scale", {}).get("rbd", {})
    total_images = rbd_scale.get("image_count", 100)
    img_size = rbd_scale.get("image_size", "1G")

    integrity_baseline = config.get("integrity", {}).get("fio_baseline_size", "1G")
    _unit = integrity_baseline.lstrip("0123456789.")
    _val = float(integrity_baseline[: len(integrity_baseline) - len(_unit)] or "1")
    block_img_size = f"{max(4, int(_val * 2))}{_unit or 'G'}"

    special_images = [
        f"rbd create rep_pool/integrity_img --size {block_img_size}",
        f"rbd create rep_pool/background_img --size {block_img_size}",
        "rbd create rep_pool/luks_img --size 2G",
        "rbd create rep_pool/cache_img --size 2G",
        "rbd create rep_pool/qos_img --size 2G",
        f"rbd create rep_pool/group_img1 --size {img_size}",
        f"rbd create rep_pool/group_img2 --size {img_size}",
        "rbd namespace create rep_pool/test_ns",
        f"rbd create rep_pool/test_ns/ns_img --size {img_size}",
        "rbd create rep_pool/ec_img --size 2G --data-pool ec_k2m2_pool",
    ]
    remaining = max(0, total_images - len(special_images))
    log.info(
        f"Creating {len(special_images)} special + {remaining} workload "
        f"RBD images (batched)"
    )
    batch_lines = ["#!/bin/bash", "set +e", "OK=0", "FAIL=0"]
    for cmd in special_images:
        batch_lines.append(f"{cmd} 2>/dev/null && OK=$((OK+1)) || FAIL=$((FAIL+1))")
    for i in range(remaining):
        dp_flag = " --data-pool ec_k2m2_pool" if i % 4 == 0 else ""
        batch_lines.append(
            f"rbd create rep_pool/workload_img_{i:04d} --size {img_size}"
            f"{dp_flag} 2>/dev/null && OK=$((OK+1)) || FAIL=$((FAIL+1))"
        )
    batch_lines.append('echo "RBD_IMAGES ok=$OK fail=$FAIL"')
    rbd_script = "\n".join(batch_lines)
    try:
        out, _ = rados_obj.node.shell(
            [f"bash -s <<'EOFRBD'\n{rbd_script}\nEOFRBD"],
            timeout=max(300, (len(special_images) + remaining) * 3),
        )
        summary = ""
        for ln in (out or "").strip().splitlines():
            if ln.startswith("RBD_IMAGES"):
                summary = ln
                break
        log.info(f"RBD image creation: {summary or 'batch complete'}")
    except Exception as e:
        log.warning(f"Batch RBD image creation failed: {e}")

    for cl in clients:
        hostname = getattr(cl, "hostname", str(cl))
        client_images = [
            f"rep_pool/integrity_img_{hostname}",
            f"rep_pool/background_img_{hostname}",
        ]
        for img_name in client_images:
            try:
                cl.exec_command(
                    sudo=True,
                    cmd=f"rbd create {img_name} --size {block_img_size} || true",
                    timeout=30,
                )
                out, _ = cl.exec_command(
                    sudo=True, cmd=f"rbd map {img_name}", timeout=30
                )
                log.info(f"RBD mapped {img_name} -> {out.strip()} on {hostname}")
            except Exception as e:
                log.warning(f"RBD per-client image {img_name} on {hostname}: {e}")

    fill_count = rbd_scale.get("fill_image_count", 0)
    fill_size = rbd_scale.get("fill_image_size", "100G")
    if fill_count > 0:
        log.info(
            f"Creating {fill_count} RBD fill images per client "
            f"({fill_size} each, thin-provisioned)"
        )
        for idx, cl in enumerate(clients):
            cmds = []
            for i in range(fill_count):
                img = f"rep_pool/fill_img_{idx:02d}_{i:03d}"
                cmds.append(f"rbd create {img} --size {fill_size} && rbd map {img}")
            script = "; ".join(cmds)
            try:
                cl.exec_command(sudo=True, cmd=script, timeout=fill_count * 15)
                log.info(f"Mapped {fill_count} fill images on {cl.hostname}")
            except Exception as e:
                log.warning(f"RBD fill image setup on {cl.hostname}: {e}")


def _cleanup_stale_cephfs_mounts(clients):
    """Remove stale CephFS kernel and FUSE mounts from all clients.

    Prevents 'mkdir: cannot stat ... Permission denied' when a prior run
    left dead kernel or FUSE mounts on /mnt/cephfs* paths.  Runs in
    parallel across all clients.  Safe to call unconditionally -- only
    touches /mnt/cephfs* paths.
    """
    cleanup_cmd = (
        "set +e; "
        "for mp in $(mount -t ceph 2>/dev/null | awk '{print $3}' "
        "  | grep '^/mnt/cephfs'); do "
        '  umount -lf "$mp" 2>/dev/null; '
        "done; "
        "for mp in $(mount -t fuse.ceph-fuse 2>/dev/null | awk '{print $3}' "
        "  | grep '^/mnt/cephfs'); do "
        '  fusermount -u "$mp" 2>/dev/null || umount -lf "$mp" 2>/dev/null; '
        "done; "
        "pkill -9 ceph-fuse 2>/dev/null; "
        "sleep 1; "
        "timeout 10 bash -c 'rm -rf /mnt/cephfs* 2>/dev/null' || true; "
        "exit 0"
    )

    def _clean_one(client):
        hostname = getattr(client, "hostname", str(client))
        try:
            client.exec_command(sudo=True, cmd=cleanup_cmd, timeout=30)
            log.info(f"Stale CephFS mounts cleaned on {hostname}")
        except Exception as e:
            log.warning(f"Stale mount cleanup on {hostname}: {e}")

    with ThreadPoolExecutor(max_workers=len(clients)) as pool:
        list(pool.map(_clean_one, clients))


def _batch_remove_subvolumes(rados_obj, fs_name, svg_name, sv_names, batch_size=100):
    """Remove subvolumes in batched shell scripts to reduce SSH calls."""
    if not sv_names:
        return
    for i in range(0, len(sv_names), batch_size):
        batch = sv_names[i : i + batch_size]
        lines = ["#!/bin/bash", "set +e"]
        for sv in batch:
            lines.append(
                f"ceph fs subvolume rm {fs_name} {sv} "
                f"--group_name {svg_name} --force 2>/dev/null || true"
            )
        lines.append(f'echo "REMOVED {len(batch)} subvols from {fs_name}/{svg_name}"')
        script = "\n".join(lines)
        try:
            rados_obj.node.shell(
                [f"bash -s <<'EOFRM'\n{script}\nEOFRM"],
                timeout=max(120, len(batch) * 3),
            )
        except Exception as e:
            log.warning(
                f"Batch subvol rm {fs_name}/{svg_name} " f"[{i}:{i + len(batch)}]: {e}"
            )


def _cleanup_cluster_resources(ceph_cluster, rados_obj, config):
    """Delete all cluster-side resources created by the upgrade thrashing test.

    Phases execute in reverse-dependency order:
      1. SMB shares/cluster/subvolumes
      2. NFS exports/clusters/subvolumes
      3. RGW buckets/users/service
      4. RBD images/snaps/clones/groups/namespace
      5. CephFS subvolumes/SVGs/snapshots/filesystems/MDS services
      6. RADOS pools + EC profiles + CRUSH rules
      7. Cluster config cleanup
    """
    log.info("=" * 60)
    log.info("Cluster Cleanup: Removing all test resources")
    log.info("=" * 60)

    clients = ceph_cluster.get_nodes(role="client")
    client = clients[0] if clients else None
    cephfs_scale = config.get("scale", {}).get("cephfs", {})
    filesystems = cephfs_scale.get("filesystems", ["cephfs_direct", "cephfs_nfs"])
    nfs_scale = config.get("scale", {}).get("nfs", {})
    cluster_count = nfs_scale.get("cluster_count", 1)
    _mon_cfg = MonConfigMethods(rados_obj=rados_obj)

    # --- Phase 1: SMB ---
    try:
        log.info("[cluster-cleanup] Phase 1: SMB teardown")
        smb_scale = config.get("scale", {}).get("smb", {})
        share_count = smb_scale.get("share_count", 500)
        smb_cluster_id = "upgradesmb"
        try:
            rados_obj.node.shell(
                ["ceph smb cluster ls 2>/dev/null"],
                timeout=15,
            )
        except Exception:
            log.info("[cluster-cleanup] Phase 1: SMB not deployed, skipping")
        else:
            lines = ["#!/bin/bash", "set +e"]
            for i in range(share_count):
                lines.append(
                    f"ceph smb share rm {smb_cluster_id} share{i:04d} "
                    f"2>/dev/null || true"
                )
            lines.append(f"ceph smb cluster rm {smb_cluster_id} 2>/dev/null || true")
            lines.append('echo "SMB_DONE"')
            script = "\n".join(lines)
            try:
                rados_obj.node.shell(
                    [f"bash -s <<'EOFSMB'\n{script}\nEOFSMB"],
                    timeout=max(120, share_count * 2),
                )
            except Exception:
                pass
            smb_fs = (
                config.get("scale", {}).get("smb", {}).get("filesystem", "cephfs_smb")
            )
            _batch_remove_subvolumes(
                rados_obj,
                smb_fs,
                "smb_svg",
                [f"smbvol{i:04d}" for i in range(share_count)],
            )
            try:
                rados_obj.node.shell(
                    [f"ceph fs subvolumegroup rm {smb_fs} smb_svg --force"]
                )
            except Exception:
                pass
            # Remove the SMB filesystem (created on-demand by _setup_smb)
            if smb_fs not in filesystems:
                try:
                    rados_obj.node.shell([f"ceph fs fail {smb_fs} 2>/dev/null || true"])
                    rados_obj.node.shell(
                        [
                            f"ceph fs rm {smb_fs} "
                            f"--yes-i-really-mean-it 2>/dev/null || true"
                        ]
                    )
                    log.info(f"[cluster-cleanup] Removed SMB filesystem {smb_fs}")
                except Exception:
                    pass
            log.info("[cluster-cleanup] Phase 1: SMB teardown complete")
    except Exception as e:
        log.warning(f"[cluster-cleanup] SMB cleanup: {e}")

    # --- Phase 2: NFS ---
    try:
        log.info("[cluster-cleanup] Phase 2: NFS teardown")
        cluster_names = (
            [f"upgrade_nfs{i + 1}" for i in range(cluster_count)]
            if cluster_count > 1
            else ["upgrade_nfs"]
        )

        # Skip non-existent NFS clusters to avoid ENOENT errors
        existing_nfs = _get_existing_nfs_clusters(rados_obj)

        for cluster_id in cluster_names:
            if cluster_id not in existing_nfs:
                log.debug(f"Skipping cleanup of non-existent NFS cluster {cluster_id}")
                continue
            try:
                exports = rados_obj.run_ceph_command(
                    cmd=f"ceph nfs export ls {cluster_id}"
                )
                if exports:
                    rm_lines = ["#!/bin/bash", "set +e"]
                    for pseudo in exports:
                        rm_lines.append(f"ceph nfs export rm {cluster_id} {pseudo}")
                    rm_script = "\n".join(rm_lines)
                    rados_obj.node.shell(
                        [f"bash -s <<'EOFNFSRM'\n{rm_script}\nEOFNFSRM"],
                        timeout=max(60, len(exports) * 10),
                    )
            except Exception as nfs_err:
                log.warning(f"NFS export cleanup {cluster_id}: {nfs_err}")

        # Batch cluster removal: only remove clusters that exist.
        clusters_to_remove = [c for c in cluster_names if c in existing_nfs]
        if clusters_to_remove:
            cluster_rm_lines = ["#!/bin/bash", "set +e"]
            for cluster_id in clusters_to_remove:
                cluster_rm_lines.append(f"ceph nfs cluster rm {cluster_id}")
            cluster_rm_script = "\n".join(cluster_rm_lines)
            try:
                rados_obj.node.shell(
                    [
                        f"bash -s <<'EOFNFSCLEANUP'\n"
                        f"{cluster_rm_script}\nEOFNFSCLEANUP"
                    ],
                    timeout=max(120, len(clusters_to_remove) * 15),
                )
            except Exception as e:
                log.warning(f"NFS batch cluster cleanup: {e}")
        for ci in range(len(cluster_names)):
            svg = f"nfs_svg{ci + 1}"
            for fs_name in filesystems:
                try:
                    svs = rados_obj.run_ceph_command(
                        cmd=f"ceph fs subvolume ls {fs_name} --group_name {svg}"
                    )
                    sv_names = [s["name"] for s in (svs or [])]
                    _batch_remove_subvolumes(rados_obj, fs_name, svg, sv_names)
                    rados_obj.node.shell(
                        [f"ceph fs subvolumegroup rm {fs_name} {svg} --force"]
                    )
                except Exception as svg_err:
                    log.warning(f"NFS SVG cleanup {fs_name}/{svg}: {svg_err}")
        log.info("[cluster-cleanup] Phase 2: NFS teardown complete")
    except Exception as e:
        log.warning(f"[cluster-cleanup] NFS cleanup: {e}")

    # --- Phase 3: RGW ---
    try:
        log.info("[cluster-cleanup] Phase 3: RGW teardown")
        rgw_daemons = []
        try:
            rgw_daemons = (
                rados_obj.run_ceph_command(cmd="ceph orch ps --daemon-type rgw") or []
            )
        except Exception:
            pass
        if client and rgw_daemons:
            rgw_endpoint = _resolve_rgw_endpoint(rados_obj)
            delete_action = (
                f"s3r = boto3.resource('s3', endpoint_url='http://{rgw_endpoint}',"
                f" aws_access_key_id='{_RGW_TEST_ACCESS_KEY}',"
                f" aws_secret_access_key='{_RGW_TEST_SECRET_KEY}');"
                "bs = [b['Name'] for b in "
                "(s3.list_buckets().get('Buckets') or [])];"
                "print(f'Deleting {len(bs)} buckets');"
                "errs=0;"
                "exec('''\nfor bn in bs:\n"
                " b = s3r.Bucket(bn)\n"
                " try: b.object_versions.delete()\n"
                " except: pass\n"
                " try: b.objects.delete()\n"
                " except: pass\n"
                " try: b.delete()\n"
                " except: errs+=1\n''')\n"
                "print(f'Done, errors={errs}')"
            )
            delete_script = _s3_script(rgw_endpoint, delete_action)
            client.exec_command(sudo=True, cmd=delete_script, timeout=600)

            io_delete_action = (
                f"s3r = boto3.resource('s3', endpoint_url='http://{rgw_endpoint}',"
                f" aws_access_key_id='{_RGW_IO_ACCESS_KEY}',"
                f" aws_secret_access_key='{_RGW_IO_SECRET_KEY}');"
                "bs = [b['Name'] for b in "
                "(s3.list_buckets().get('Buckets') or [])];"
                "print(f'Deleting {len(bs)} iokey buckets');"
                "errs=0;"
                "exec('''\nfor bn in bs:\n"
                " b = s3r.Bucket(bn)\n"
                " try: b.object_versions.delete()\n"
                " except: pass\n"
                " try: b.objects.delete()\n"
                " except: pass\n"
                " try: b.delete()\n"
                " except: errs+=1\n''')\n"
                "print(f'Done iokey, errors={errs}')"
            )
            io_delete_script = _s3_script(
                rgw_endpoint,
                io_delete_action,
                access_key=_RGW_IO_ACCESS_KEY,
                secret_key=_RGW_IO_SECRET_KEY,
            )
            client.exec_command(sudo=True, cmd=io_delete_script, timeout=600)
        rados_obj.node.shell(
            [
                f"radosgw-admin user rm --uid={_RGW_TEST_USER} --purge-data ; "
                f"radosgw-admin user rm --uid={_RGW_IO_USER} --purge-data"
            ],
            timeout=120,
        )
        rados_obj.node.shell(["ceph orch rm rgw.upgrade_rgw"])
        time.sleep(10)
        log.info("[cluster-cleanup] Phase 3: RGW teardown complete")
    except Exception as e:
        log.warning(f"[cluster-cleanup] RGW cleanup: {e}")

    # --- Phase 4: RBD ---
    try:
        log.info("[cluster-cleanup] Phase 4: RBD teardown")
        rbd_lines = ["#!/bin/bash", "set +e"]
        rbd_lines.extend(
            [
                "rbd trash purge schedule remove -p rep_pool 2>/dev/null || true",
                "rbd group snap rm rep_pool/upgrade_group "
                "@pre_upgrade_group_snap 2>/dev/null || true",
                "rbd group image rm rep_pool/upgrade_group "
                "rep_pool/group_img1 2>/dev/null || true",
                "rbd group image rm rep_pool/upgrade_group "
                "rep_pool/group_img2 2>/dev/null || true",
                "rbd group image rm rep_pool/upgrade_group "
                "rep_pool/group_test_img 2>/dev/null || true",
                "rbd group rm rep_pool/upgrade_group 2>/dev/null || true",
                "rbd rm rep_pool/integrity_clone 2>/dev/null || true",
                "rbd snap unprotect rep_pool/integrity_img"
                "@pre_upgrade 2>/dev/null || true",
                "rbd snap rm rep_pool/integrity_img" "@pre_upgrade 2>/dev/null || true",
            ]
        )
        rbd_lines.append(
            "for img in $(rbd ls rep_pool 2>/dev/null); do "
            'rbd rm "rep_pool/$img" 2>/dev/null || true; done'
        )
        rbd_lines.extend(
            [
                "rbd rm rep_pool/test_ns/ns_img 2>/dev/null || true",
                "rbd namespace rm rep_pool/test_ns 2>/dev/null || true",
                'echo "RBD_CLEANUP_DONE"',
            ]
        )
        rbd_script = "\n".join(rbd_lines)
        try:
            rados_obj.node.shell(
                [f"bash -s <<'EOFRBD'\n{rbd_script}\nEOFRBD"],
                timeout=600,
            )
        except Exception:
            pass
        log.info("[cluster-cleanup] Phase 4: RBD teardown complete")
    except Exception as e:
        log.warning(f"[cluster-cleanup] RBD cleanup: {e}")

    # --- Phase 5: CephFS ---
    try:
        log.info("[cluster-cleanup] Phase 5: CephFS teardown")
        svg_count = cephfs_scale.get("subvolume_groups_per_fs", 6)

        for fs_name in filesystems:
            try:
                rados_obj.node.shell(
                    [f"ceph fs snap-schedule remove /{fs_name} 2>/dev/null || true"]
                )
            except Exception:
                pass

            try:
                rados_obj.node.shell(
                    [
                        f"ceph fs subvolume snapshot rm {fs_name} "
                        f"upgrade_test_subvol pre_upgrade_snap 2>/dev/null || true"
                    ]
                )
                rados_obj.node.shell(
                    [
                        f"ceph fs subvolume rm {fs_name} upgrade_test_subvol "
                        f"--force 2>/dev/null || true"
                    ]
                )
            except Exception:
                pass

            all_svgs = [f"svg_{g:02d}" for g in range(svg_count)]
            all_svgs.append("svg_charmap")
            for svg_name in all_svgs:
                try:
                    svs = rados_obj.run_ceph_command(
                        cmd=(
                            f"ceph fs subvolume ls {fs_name} "
                            f"--group_name {svg_name}"
                        )
                    )
                    sv_names = [s["name"] for s in (svs or [])]
                    if sv_names:
                        _batch_remove_subvolumes(rados_obj, fs_name, svg_name, sv_names)
                except Exception:
                    pass
                try:
                    rados_obj.node.shell(
                        [
                            f"ceph fs subvolumegroup rm {fs_name} "
                            f"{svg_name} --force 2>/dev/null || true"
                        ]
                    )
                except Exception:
                    pass

            try:
                rados_obj.node.shell([f"ceph fs fail {fs_name}"])
                time.sleep(3)
            except Exception as e:
                log.warning(f"CephFS fail {fs_name}: {e}")

            try:
                rados_obj.node.shell([f"ceph fs rm {fs_name} --yes-i-really-mean-it"])
            except Exception as e:
                log.warning(f"CephFS rm {fs_name}: {e}")

            # Fail orphaned standby MDS daemons still in the MDS map
            # (IBMCEPH-11851: stale standby entries persist after fs rm)
            stale_hosts = set()
            try:
                mds_meta = rados_obj.run_ceph_command(cmd="ceph mds metadata")
                if isinstance(mds_meta, list):
                    stale = [
                        m
                        for m in mds_meta
                        if m.get("name", "").startswith(f"{fs_name}.")
                    ]
                    if stale:
                        stale_hosts = {
                            m.get("hostname") for m in stale if m.get("hostname")
                        }
                        fail_cmds = "; ".join(
                            f"ceph mds fail {m['name']}" for m in stale
                        )
                        rados_obj.node.shell(
                            [f"bash -c '{fail_cmds}'"],
                            timeout=60,
                        )
                        log.info(
                            f"Failed {len(stale)} orphaned MDS "
                            f"daemon(s) for {fs_name}"
                        )
            except Exception as e:
                log.warning(f"MDS fail cleanup for {fs_name}: {e}")

            try:
                rados_obj.node.shell([f"ceph orch rm mds.{fs_name}"])
            except Exception:
                pass

            # Rescan affected hosts to clear cephadm daemon cache
            if stale_hosts:
                rescan_cmds = "; ".join(
                    f"ceph orch host rescan {h}" for h in stale_hosts
                )
                try:
                    rados_obj.node.shell(
                        [f"bash -c '{rescan_cmds}'"],
                        timeout=max(60, len(stale_hosts) * 15),
                    )
                except Exception:
                    pass

            # Poll until MDS metadata is fully cleared
            deadline = time.time() + 90
            consecutive_errors = 0
            while time.time() < deadline:
                time.sleep(5)
                try:
                    mds_meta = rados_obj.run_ceph_command(cmd="ceph mds metadata")
                    consecutive_errors = 0
                    leftover = [
                        m.get("name", "?")
                        for m in (mds_meta if isinstance(mds_meta, list) else [])
                        if m.get("name", "").startswith(f"{fs_name}.")
                    ]
                    if not leftover:
                        log.info(f"All MDS daemons for {fs_name} " f"fully removed")
                        break
                    log.info(
                        f"Waiting for {len(leftover)} stale MDS "
                        f"entry(ies): {leftover}"
                    )
                    for n in leftover:
                        try:
                            rados_obj.node.shell([f"ceph mds fail {n}"])
                        except Exception:
                            pass
                except Exception as e:
                    consecutive_errors += 1
                    log.warning(
                        f"MDS metadata query failed " f"({consecutive_errors}): {e}"
                    )
                    if consecutive_errors >= 3:
                        break
            else:
                log.warning(f"Timed out waiting for MDS cleanup of " f"{fs_name} (90s)")

        log.info("[cluster-cleanup] Phase 5: CephFS teardown complete")
    except Exception as e:
        log.warning(f"[cluster-cleanup] CephFS cleanup: {e}")

    # --- Phase 6: Pool deletion ---
    try:
        log.info("[cluster-cleanup] Phase 6: Pool deletion")
        pools_to_delete = [
            "rep_pool",
            "rep_quota_pool",
            "rep_compress_snappy",
            "rep_compress_zstd",
            "upgrade_integrity_pool",
            "ec_k2m2_pool",
            "ec_k4m2_pool",
            "rep_quota_obj_pool",
            "crush_class_test_pool",
            "ec_opt_verify",
        ]
        for fs_name in filesystems:
            pools_to_delete.append(f"cephfs.{fs_name}.data")
            pools_to_delete.append(f"cephfs.{fs_name}.meta")

        try:
            _mon_cfg.set_config(
                section="mon",
                name="mon_allow_pool_delete",
                value="true",
                no_delay=True,
            )
        except Exception:
            pass

        try:
            existing, _ = rados_obj.node.shell(["ceph osd pool ls"])
            existing_set = set((existing or "").strip().splitlines())
        except Exception:
            existing_set = set()

        to_delete = [p for p in pools_to_delete if p in existing_set]
        if to_delete:
            pool_lines = ["#!/bin/bash", "set +e", "OK=0", "FAIL=0"]
            for pool in to_delete:
                pool_lines.append(
                    f"ceph osd pool delete {pool} {pool} "
                    f"--yes-i-really-really-mean-it 2>/dev/null "
                    f"&& OK=$((OK+1)) || FAIL=$((FAIL+1))"
                )
            pool_lines.append('echo "POOLS_DELETED ok=$OK fail=$FAIL"')
            pool_script = "\n".join(pool_lines)
            try:
                out, _ = rados_obj.node.shell(
                    [f"bash -s <<'EOFPOOL'\n{pool_script}\nEOFPOOL"],
                    timeout=max(120, len(to_delete) * 10),
                )
                for ln in (out or "").strip().splitlines():
                    if ln.startswith("POOLS_DELETED"):
                        log.info(f"[cluster-cleanup] {ln}")
                        break
            except Exception as e:
                log.warning(f"Batch pool deletion: {e}")
        else:
            log.info("[cluster-cleanup] No matching pools to delete")

        for profile in ["ec_k2m2", "ec_k4m2", "ecp_ec_opt_verify"]:
            try:
                rados_obj.node.shell([f"ceph osd erasure-code-profile rm {profile}"])
            except Exception:
                pass

        try:
            rules_out, _ = rados_obj.node.shell(["ceph osd crush rule ls"])
            for rule in (rules_out or "").splitlines():
                if rule.strip().startswith("rule_") and "upgrade_test" in rule:
                    rados_obj.node.shell([f"ceph osd crush rule rm {rule.strip()}"])
        except Exception:
            pass

        log.info("[cluster-cleanup] Phase 6: Pool deletion complete")
    except Exception as e:
        log.warning(f"[cluster-cleanup] Pool deletion: {e}")

    # --- Phase 7: Config cleanup ---
    try:
        log.info("[cluster-cleanup] Phase 7: Config cleanup")
        config_keys_to_rm = [
            ("mds", "mds_cache_memory_limit"),
            ("mds", "mds_cache_reservation"),
            ("mds", "mds_cache_trim_threshold"),
            ("mds", "mds_recall_global_max_decay_threshold"),
            ("mds", "mds_session_cap_acquisition_throttle"),
            ("mds", "mds_max_caps_per_client"),
            ("mds", "mds_bal_fragment_dirs"),
            ("mds", "mds_bal_fragment_size_max"),
            ("client", "rbd_persistent_cache_mode"),
            ("client", "rbd_persistent_cache_path"),
            ("client", "rbd_plugins"),
            ("client.rgw", "rgw_crypt_sse_s3_backend"),
            ("client.rgw", "rgw_dynamic_resharding"),
            ("client.rgw", "rgw_s3_auth_use_sts"),
            ("client.rgw", "rgw_sts_key"),
            ("osd", "osd_mclock_override_recovery_settings"),
            ("osd", "osd_mclock_profile"),
            ("osd", "osd_memory_target_autotune"),
            ("osd", "osd_scrub_auto_repair"),
            ("global", "mon_max_pg_per_osd"),
        ]
        for target, key in config_keys_to_rm:
            try:
                _mon_cfg.remove_config(section=target, name=key, verify_rm=False)
            except Exception:
                pass

        try:
            _mon_cfg.set_config(
                section="mon",
                name="mon_allow_pool_delete",
                value="false",
                no_delay=True,
            )
        except Exception:
            pass

        log.info("[cluster-cleanup] Phase 7: Config cleanup complete")
    except Exception as e:
        log.warning(f"[cluster-cleanup] Config cleanup: {e}")

    log.info("=" * 60)
    log.info("Cluster cleanup finished")
    log.info("=" * 60)


def _pin_subvolumegroup(rados_obj, fs_name, svg_name, pin_policy):
    """SVG-level distributed pin (not per-subvolume). Warn-only on failure."""
    if pin_policy != "distributed":
        return
    try:
        rados_obj.node.shell(
            [f"ceph fs subvolumegroup pin {fs_name} {svg_name} distributed 1"]
        )
    except Exception as e:
        log.warning(f"SVG pin {fs_name}/{svg_name}: {e}")


def _setup_cephfs(ceph_cluster, rados_obj, config):
    """Phase 1: Create CephFS filesystems with scaled MDS, SVGs, and subvolumes.

    Scale is driven by ``config["scale"]["cephfs"]``:
        filesystems (list[str]): FS names to create (default ["cephfs_direct", "cephfs_nfs"]).
        active_mds (int): max_mds per filesystem (default 6).
        mds_count_per_host (int): MDS daemons per host (default 2).
        subvolume_groups_per_fs (int): SVGs on the direct filesystem (default 6).
        subvolumes_per_group (int): Subvolumes per SVG (default 200).
        pin_policy (str): "distributed" | "none" (default "distributed").
            SVG-level pin via ``ceph fs subvolumegroup pin``; NFS/SMB reuse the same knob.
    """
    log.info("Setting up CephFS filesystems")
    cephfs_scale = config.get("scale", {}).get("cephfs", {})
    filesystems = cephfs_scale.get("filesystems", ["cephfs_direct", "cephfs_nfs"])
    direct_fs = cephfs_scale.get("direct_filesystem", filesystems[0])
    active_mds = cephfs_scale.get("active_mds", 6)
    count_per_host = cephfs_scale.get("mds_count_per_host", 2)
    svg_count = cephfs_scale.get("subvolume_groups_per_fs", 6)
    subvol_count = cephfs_scale.get("subvolumes_per_group", 200)
    pin_policy = cephfs_scale.get("pin_policy", "distributed")

    data_pool_pg_target = cephfs_scale.get("data_pool_pg_num_max", 512)
    data_pool_pg_ceiling = data_pool_pg_target * 2

    for fs_name in filesystems:
        try:
            # mutating — do not use run_ceph_command (empty stdout => {})
            rados_obj.node.shell([f"ceph fs volume create {fs_name}"])
        except Exception as e:
            log.warning(f"CephFS volume create {fs_name}: {e}")

        data_pool = f"cephfs.{fs_name}.data"
        try:
            _set_pool_pg_params(
                rados_obj, data_pool, data_pool_pg_target, data_pool_pg_ceiling
            )
            if data_pool_pg_target <= 512:
                time.sleep(5)
            log.info(
                f"{data_pool}: pg_num={data_pool_pg_target}, "
                f"pg_num_min={data_pool_pg_target}, "
                f"pg_num_max={data_pool_pg_ceiling}"
            )
        except Exception as e:
            log.warning(f"PG setup for {data_pool}: {e}")

        # Scale MDS via JSON service spec piped to stdin.
        # --count-per-host is not a valid CLI flag on older Ceph versions.
        try:
            spec = {
                "service_type": "mds",
                "service_id": fs_name,
                "placement": {"label": "mds", "count_per_host": count_per_host},
            }
            spec_str = json.dumps(spec)
            rados_obj.node.shell(
                [
                    f"bash -s <<'EOFMDS'\n"
                    f"echo '{spec_str}' | ceph orch apply -i -\n"
                    f"EOFMDS"
                ],
            )
            log.info(f"MDS for {fs_name}: count_per_host={count_per_host}")
        except Exception as e:
            log.warning(f"MDS apply for {fs_name}: {e}")

        # Set max_mds for active standby scaling
        try:
            rados_obj.node.shell([f"ceph fs set {fs_name} max_mds {active_mds}"])
            log.info(f"{fs_name}: max_mds set to {active_mds}")
        except Exception as e:
            log.warning(f"max_mds set for {fs_name}: {e}")

        _wait_for_active_mds(rados_obj, fs_name)

    # Create the special test subvolume (used by feature manager)
    try:
        # mutating — do not use run_ceph_command (empty stdout => {})
        rados_obj.node.shell(
            [f"ceph fs subvolume create {direct_fs} upgrade_test_subvol"]
        )
    except Exception as e:
        log.warning(f"upgrade_test_subvol create: {e}")

    # Create scaled SVGs/subvolumes on the direct filesystem only.
    # NFS/SMB create and pin their own SVGs. SVG-level distributed pin.
    created_svgs: list = []
    for g in range(svg_count):
        svg_name = f"svg_{g:02d}"
        try:
            rados_obj.node.shell(
                [f"ceph fs subvolumegroup create {direct_fs} {svg_name}"]
            )
        except Exception as e:
            log.warning(f"SVG {direct_fs}/{svg_name}: {e}")
            continue

        _pin_subvolumegroup(rados_obj, direct_fs, svg_name, pin_policy)

        lines = ["#!/bin/bash", "set +e", "FAIL=0"]
        for s in range(subvol_count):
            sv = f"subvol_{s:04d}"
            lines.append(
                f"ceph fs subvolume create {direct_fs} {sv} "
                f"--group_name {svg_name} 2>/dev/null || FAIL=$((FAIL+1))"
            )
        lines.append('echo "DONE fail=$FAIL"')
        script = "\n".join(lines)
        try:
            out, _ = rados_obj.node.shell(
                [f"bash -s <<'EOFSV'\n{script}\nEOFSV"],
                timeout=max(600, subvol_count * 3),
            )
            out_lines = out.strip().splitlines() if out else []
            summary = out_lines[-1] if out_lines else "(no output)"
            log.info(
                f"{direct_fs}/{svg_name}: {subvol_count} subvolumes "
                f"(pin_policy={pin_policy}) -- {summary}"
            )
            created_svgs.append(svg_name)
        except Exception as e:
            log.warning(f"Batch subvolume create {direct_fs}/{svg_name}: {e}")
            continue

    clients = ceph_cluster.get_nodes(role="client")
    mounts_per_client = cephfs_scale.get("mounts_per_client", 10)
    total_needed = mounts_per_client * len(clients) if clients else 0
    max_available = len(created_svgs) * subvol_count
    total_needed = min(total_needed, max_available) if max_available else 0

    # Stride across SVGs so mounts are not all from svg_00/subvol_0000.
    mount_targets: list = []
    if created_svgs and total_needed:
        for i in range(total_needed):
            svg_name = created_svgs[i % len(created_svgs)]
            sv_idx = i // len(created_svgs)
            if sv_idx >= subvol_count:
                break
            mount_targets.append((svg_name, f"subvol_{sv_idx:04d}"))

    selected_paths: list = []
    if mount_targets:
        gp_lines = ["#!/bin/bash"]
        for svg_name, sv in mount_targets:
            gp_lines.append(
                f'echo "SV:{direct_fs}:{svg_name}:{sv}:$(ceph fs subvolume getpath '
                f'{direct_fs} {sv} --group_name {svg_name} 2>/dev/null)"'
            )
        try:
            gp_out, _ = rados_obj.node.shell(
                ["bash -s <<'EOFGP'\n" + "\n".join(gp_lines) + "\nEOFGP"],
                timeout=max(300, len(mount_targets) * 2),
            )
            for line in (gp_out or "").strip().splitlines():
                if not line.startswith("SV:"):
                    continue
                parts = line.split(":", 4)
                if len(parts) != 5:
                    continue
                sv_path = parts[4].strip()
                if sv_path and sv_path.startswith("/"):
                    selected_paths.append((direct_fs, sv_path))
        except Exception as e:
            log.warning(f"Batch getpath for mounts on {direct_fs}: {e}")

    log.info(
        f"CephFS setup complete: {len(filesystems)} FS, "
        f"max_mds={active_mds}, count_per_host={count_per_host}, "
        f"{len(created_svgs)} SVGs x {subvol_count} subvols on {direct_fs}, "
        f"{len(selected_paths)}/{total_needed} mount paths resolved"
    )

    if not clients:
        log.warning("No client nodes available for CephFS mounts")
        return

    _cleanup_stale_cephfs_mounts(clients)

    log.info("Waiting for PG creation to complete before CephFS mounts...")
    _wait_for_pg_creation(rados_obj, timeout=600)

    mon_nodes = ceph_cluster.get_nodes(role="mon")
    mon_addr = ""
    if mon_nodes:
        mon_addr = getattr(mon_nodes[0], "ip_address", "")

    if not selected_paths:
        log.warning("No subvolume paths available for CephFS mounts, skipping")
        return

    # Sequential mounts with per-mount retry (do not batch — preserves 30s/60s retry).
    for sv_idx, (fs_name, sv_path) in enumerate(selected_paths):
        client = clients[sv_idx % len(clients)]
        hostname = getattr(client, "hostname", str(client))
        mount_idx = sv_idx // len(clients)

        if mount_idx % 2 == 0:
            mnt = f"/mnt/{fs_name}_sv{mount_idx}_kernel"
            cmd = (
                f"mkdir -p {mnt} && "
                f"mount -t ceph {mon_addr}:{sv_path} {mnt} "
                f"-o name=admin,fs={fs_name}"
            )
        else:
            mnt = f"/mnt/{fs_name}_sv{mount_idx}_fuse"
            cmd = (
                f"mkdir -p {mnt} && "
                f"ceph-fuse -n client.admin --client_fs {fs_name} "
                f"-r {sv_path} {mnt}"
            )

        for attempt, tmo in enumerate([30, 60], start=1):
            try:
                client.exec_command(sudo=True, cmd=cmd, timeout=tmo)
                log.info(f"CephFS subvol mount: {mnt} on {hostname}")
                break
            except Exception as e:
                if attempt == 1:
                    try:
                        client.exec_command(
                            sudo=True,
                            cmd=f"umount -lf {mnt} 2>/dev/null; rm -rf {mnt}",
                            timeout=10,
                            check_ec=False,
                        )
                    except Exception:
                        pass
                    log.info(f"CephFS mount retry (60s): {mnt} on {hostname}")
                else:
                    log.warning(f"CephFS subvol mount {mnt} on {hostname}: {e}")


def _resolve_rgw_endpoint(rados_obj):
    """Resolve a reachable RGW endpoint (host:port) from running daemons.

    Returns the hostname:port of the first running RGW daemon, falling
    back to localhost:80 if resolution fails.
    """
    try:
        daemons = rados_obj.run_ceph_command(cmd="ceph orch ps --daemon-type rgw")
        if isinstance(daemons, list):
            for d in daemons:
                if d.get("status_desc") == "running":
                    hostname = d.get("hostname", "")
                    ports = d.get("ports", [80])
                    port = ports[0] if ports else 80
                    if hostname:
                        return f"{hostname}:{port}"
    except Exception as e:
        log.warning(f"RGW endpoint resolution failed: {e}")
    return "localhost:80"


def _setup_rgw(ceph_cluster, rados_obj, config):
    """Phase 1: Deploy RGW service and create scaled buckets.

    Scale is driven by ``config["scale"]["rgw"]``:
        versioned_buckets (int): Buckets with versioning enabled (default 50).
        non_versioned_buckets (int): Plain buckets (default 50).
    """
    log.info("Setting up RGW")
    rgw_scale = config.get("scale", {}).get("rgw", {})
    daemons = rgw_scale.get("daemons_per_service", 2)
    rados_obj.node.shell([f"ceph orch apply rgw upgrade_rgw --placement={daemons}"])
    _wait_for_service(rados_obj, "rgw.upgrade_rgw", timeout=max(120, daemons * 30))

    try:
        rados_obj.node.shell(
            [
                f"radosgw-admin user create --uid={_RGW_TEST_USER} "
                f"--display-name='Upgrade Test User' "
                f"--access-key={_RGW_TEST_ACCESS_KEY} "
                f"--secret={_RGW_TEST_SECRET_KEY}"
            ]
        )
    except Exception as e:
        log.warning(f"RGW user create: {e}")

    try:
        rados_obj.node.shell(
            [
                f"radosgw-admin user create --uid={_RGW_IO_USER} "
                f"--display-name='Upgrade IO User' "
                f"--access-key={_RGW_IO_ACCESS_KEY} "
                f"--secret={_RGW_IO_SECRET_KEY}"
            ]
        )
    except Exception as e:
        log.warning(f"RGW IO user create: {e}")

    for quota_cmd in [
        "radosgw-admin quota set --quota-scope=user "
        f"--uid={_RGW_IO_USER} --max-objects=-1 --max-size=-1",
        f"radosgw-admin quota disable --quota-scope=user --uid={_RGW_IO_USER}",
        f"radosgw-admin ratelimit disable --ratelimit-scope=user "
        f"--uid={_RGW_IO_USER}",
    ]:
        try:
            rados_obj.node.shell([quota_cmd])
        except Exception:
            pass

    rgw_endpoint = _resolve_rgw_endpoint(rados_obj)
    log.info(f"Using RGW endpoint: {rgw_endpoint}")

    versioned_count = rgw_scale.get("versioned_buckets", 50)
    non_versioned_count = rgw_scale.get("non_versioned_buckets", 50)

    clients = ceph_cluster.get_nodes(role="client")
    if not clients:
        raise RuntimeError("No client nodes for RGW bucket setup (boto3 required)")
    client = clients[0]

    _create_rgw_bucket(client, rgw_endpoint, "upgrade-test-bucket")

    batch_script = _s3_script(
        rgw_endpoint,
        "import traceback;"
        f"nv={non_versioned_count};v={versioned_count};"
        "errs=0;"
        "exec('''\nfor i in range(nv):\n"
        " try: s3.create_bucket(Bucket=f'bucket-nv-{i:04d}')\n"
        " except: errs+=1\n"
        "for i in range(v):\n"
        " try: s3.create_bucket(Bucket=f'bucket-ver-{i:04d}')\n"
        " except: errs+=1\n"
        "for i in range(v):\n"
        " try: s3.put_bucket_versioning(Bucket=f'bucket-ver-{i:04d}',"
        "VersioningConfiguration={'Status':'Enabled'})\n"
        " except: errs+=1\n''')\n"
        f"print(f'Buckets: {{nv}}nv+{{v}}ver, errors={{errs}}')",
        access_key=_RGW_IO_ACCESS_KEY,
        secret_key=_RGW_IO_SECRET_KEY,
    )
    try:
        client.exec_command(sudo=True, cmd=batch_script, timeout=300)
    except Exception as e:
        log.warning(f"Batch bucket creation: {e}")
    log.info(
        f"Created {non_versioned_count} non-versioned + "
        f"{versioned_count} versioned buckets (batched)"
    )

    # Force creation of default.rgw.buckets.data by writing a seed object.
    # Bucket creation is metadata-only; the data pool is lazily created on
    # the first S3 PUT.
    seed_script = _s3_script(
        rgw_endpoint,
        "s3.put_object(Bucket='upgrade-test-bucket', "
        "Key='_pg_seed', Body=b'x'); "
        "print('SEED_OK')",
    )
    try:
        out, _ = client.exec_command(sudo=True, cmd=seed_script, timeout=60)
        if "SEED_OK" not in (out or ""):
            log.warning("RGW seed object write did not confirm success")
    except Exception as e:
        log.warning(f"RGW seed object write failed: {e}")

    rgw_data_pool = "default.rgw.buckets.data"
    rgw_data_pg_target = rgw_scale.get("data_pool_pg_num", 512)
    rgw_data_pg_ceiling = rgw_data_pg_target * 2
    pool_ready = False
    for _attempt in range(6):
        try:
            pools_out, _ = rados_obj.node.shell(["ceph osd pool ls"])
            if rgw_data_pool in (pools_out or "").splitlines():
                pool_ready = True
                break
        except Exception:
            pass
        time.sleep(5)

    if not pool_ready:
        log.warning(
            f"{rgw_data_pool} not found after seed write -- "
            "PG config skipped; pool will use Ceph defaults"
        )
    else:
        ok = _set_pool_pg_params(
            rados_obj, rgw_data_pool, rgw_data_pg_target, rgw_data_pg_ceiling
        )
        if ok == 4:
            log.info(
                f"{rgw_data_pool}: pg_num={rgw_data_pg_target}, "
                f"pg_num_min={rgw_data_pg_target}, "
                f"pg_num_max={rgw_data_pg_ceiling}"
            )
        else:
            log.warning(f"{rgw_data_pool}: only {ok}/4 PG settings applied")

    log.info(
        f"RGW setup complete: {versioned_count} versioned + "
        f"{non_versioned_count} non-versioned buckets"
    )


def _s3_script(
    endpoint, action, access_key=_RGW_TEST_ACCESS_KEY, secret_key=_RGW_TEST_SECRET_KEY
):
    """Build a python3 one-liner that creates a boto3 S3 client and runs *action*."""
    return (
        'python3 -c "'
        "import boto3;"
        "s3 = boto3.client('s3', "
        f"endpoint_url='http://{endpoint}', "
        f"aws_access_key_id='{access_key}', "
        f"aws_secret_access_key='{secret_key}');"
        f"{action}"
        '"'
    )


def _create_rgw_bucket(node, endpoint, bucket_name):
    """Create a single S3 bucket via boto3."""
    script = _s3_script(endpoint, f"s3.create_bucket(Bucket='{bucket_name}')")
    try:
        node.exec_command(sudo=True, cmd=script, timeout=30)
    except Exception as e:
        log.warning(f"Bucket create {bucket_name}: {e}")


def _discover_nfs_daemon_nodes(rados_obj, ceph_cluster, cluster_names):
    """Yield ``(hostname, node)`` for each unique NFS daemon host."""
    seen = set()
    for cluster_id in cluster_names:
        try:
            daemons = rados_obj.run_ceph_command(
                cmd=f"ceph orch ps --service-name nfs.{cluster_id}"
            )
            if not isinstance(daemons, list):
                continue
            for d in daemons:
                hostname = d.get("hostname", "")
                if not hostname or hostname in seen:
                    continue
                node = None
                for n in ceph_cluster.get_nodes():
                    if getattr(n, "hostname", "") == hostname or (
                        hasattr(n, "shortname") and n.shortname == hostname
                    ):
                        node = n
                        break
                if not node:
                    continue
                seen.add(hostname)
                yield hostname, node
        except Exception as e:
            log.warning(f"NFS daemon discovery for {cluster_id}: {e}")


def _open_nfs_firewall_ports(rados_obj, ceph_cluster, cluster_names):
    """Open firewall ports required for NFSv3 on every NFS-Ganesha host.

    Ganesha registers mountd with rpcbind on a random ephemeral port
    (not the standard 20048).  This function discovers the actual port
    and opens it alongside the rpc-bind service in a single SSH call.
    """
    for hostname, node in _discover_nfs_daemon_nodes(
        rados_obj, ceph_cluster, cluster_names
    ):
        try:
            script = (
                "yum install -y rpcbind 2>/dev/null; "
                "systemctl start rpcbind 2>/dev/null; "
                "firewall-cmd --add-service=rpc-bind --permanent 2>/dev/null; "
                "firewall-cmd --add-service=nfs --permanent 2>/dev/null; "
                "firewall-cmd --add-service=mountd --permanent 2>/dev/null; "
                "MPORT=$(rpcinfo -p localhost 2>/dev/null"
                " | awk '/mountd.*tcp/{print $4}'"
                " | sort -un | head -1); "
                'if [ -z "$MPORT" ]; then '
                "  MPORT=$(ss -tlnp | grep ganesha | grep -v 2049"
                " | awk '{print $4}' | grep -oP '\\d+$'"
                " | sort -un | head -1); "
                "fi; "
                'if [ -n "$MPORT" ] && [ "$MPORT" != "20048" ]; then '
                "  firewall-cmd --add-port=${MPORT}/tcp --permanent 2>/dev/null; "
                '  echo "mountd_port=$MPORT"; '
                "fi; "
                "firewall-cmd --reload; "
                "sleep 3"
            )
            out, _ = node.exec_command(sudo=True, cmd=script, timeout=60)
            port_info = ""
            for line in out.splitlines():
                if line.startswith("mountd_port="):
                    port_info = f" (mountd port {line.split('=')[1]})"
            log.info(
                f"NFS firewall: opened rpc-bind+mountd on" f" {hostname}{port_info}"
            )
        except Exception as e:
            log.warning(f"NFS firewall on {hostname}: {e}")


def _open_nfs_port_range(
    rados_obj, ceph_cluster, cluster_names, base_port, cluster_count
):
    """Open firewall ports for all NFS clusters on every NFS daemon host.

    When multiple NFS clusters use sequential ports (base_port through
    base_port + cluster_count - 1), the non-default ports may be blocked
    by firewalld on hosts with restrictive zones (e.g. public).  This
    function opens the full port range unconditionally — it is harmless
    on hosts using permissive zones (e.g. trusted).
    """
    if cluster_count <= 1:
        return

    end_port = base_port + cluster_count - 1
    port_range = f"{base_port}-{end_port}/tcp"

    opened = []
    for hostname, node in _discover_nfs_daemon_nodes(
        rados_obj, ceph_cluster, cluster_names
    ):
        try:
            node.exec_command(
                sudo=True,
                cmd=(
                    f"firewall-cmd --add-port={port_range} --permanent"
                    " 2>/dev/null; firewall-cmd --reload 2>/dev/null"
                ),
                timeout=30,
            )
            opened.append(hostname)
            log.info(f"NFS firewall: opened {port_range} on {hostname}")
        except Exception as e:
            log.warning(f"NFS firewall port range on {hostname}: {e}")

    if opened:
        log.info(f"NFS firewall: opened {port_range} on {len(opened)} host(s)")


def _setup_nfs(ceph_cluster, rados_obj, config):
    """Phase 1: Deploy NFS Ganesha clusters with versioned CephFS exports.

    Scale is driven by ``config["scale"]["nfs"]``:
        cluster_count (int): Number of NFS clusters to deploy (default 1).
        daemons_per_cluster (int): Daemon count per cluster (default 1).
        nfs_versions (list): NFS protocol versions to mount (default [3,4.1,4.2]).
        mounts_per_version (int): Mounts per version per cluster (default 2).
    Each cluster gets a dedicated subvolume group (``nfs_svg<N>``). Exports are
    created for every version x mount combination and mounted on all clients with
    the correct ``-t nfs``/``-t nfs4 -o vers=X`` flags.
    """
    log.info("Setting up NFS")
    nfs_config = config.get("features", {}).get("nfs", {})
    nfs_scale = config.get("scale", {}).get("nfs", {})
    cluster_count = nfs_scale.get("cluster_count", 1)
    daemons_per_cluster = nfs_scale.get("daemons_per_cluster", 1)

    # Detect Ceph version once for all feature-flag decisions below.
    ceph_ver = ""
    ver_tuple = (0, 0, 0)
    enable_nfsv3 = False
    try:
        ver_out, _ = rados_obj.node.shell(
            ["bash -c \"ceph version | awk '{print $3}'\""]
        )
        ceph_ver = ver_out.strip()
        ver_tuple = _ceph_version_tuple(ceph_ver)
        if LooseVersion(ceph_ver) > LooseVersion("19.2.1-292"):
            enable_nfsv3 = True
            log.info("NFS cluster: --enable-nfsv3 (Ceph >= 19.2.1-292)")
        else:
            log.info(f"NFS cluster: skipping --enable-nfsv3 (Ceph {ceph_ver})")
    except Exception:
        if nfs_config.get("nfsv3", False):
            enable_nfsv3 = True

    # NFS daemon colocation: the NFS-Ganesha Prometheus metrics exporter
    # binds to a hardcoded port (9587) per host, so only one NFS daemon
    # per host works on versions before 9.1 (IBMCEPH-11322).
    #   < 9.1 (ver < 20.1.0): cap cluster_count to host count and use
    #       explicit per-host placement to prevent cephadm colocation
    #   >= 9.1 (ver >= 20.1.0): use spec-based creation with monitoring_port
    colocation_supported = ver_tuple >= (20, 1, 0)
    nfs_hosts = []
    if not colocation_supported and cluster_count > 1:
        nfs_hosts = _get_nfs_eligible_hosts(rados_obj)
        if nfs_hosts:
            try:
                nfs_daemons = rados_obj.run_ceph_command(
                    cmd="ceph orch ps --daemon-type nfs"
                )
                occupied = {
                    d["hostname"]
                    for d in nfs_daemons
                    if isinstance(d, dict) and d.get("hostname")
                }
                if occupied:
                    before = len(nfs_hosts)
                    nfs_hosts = [h for h in nfs_hosts if h not in occupied]
                    log.info(
                        f"Excluded {before - len(nfs_hosts)} host(s) with "
                        f"existing NFS daemons: {occupied}"
                    )
            except Exception:
                pass
        if nfs_hosts and cluster_count > len(nfs_hosts):
            log.warning(
                f"NFS colocation not supported on Ceph {ceph_ver} "
                f"(IBMCEPH-11322). Capping cluster_count from "
                f"{cluster_count} to {len(nfs_hosts)} (1 per host)."
            )
            cluster_count = len(nfs_hosts)
        elif not nfs_hosts:
            log.warning(
                "Could not determine NFS-eligible hosts via "
                "'ceph orch host ls'; proceeding with cluster_count=%d "
                "(colocation crashes may occur)",
                cluster_count,
            )

    cluster_names = (
        [f"upgrade_nfs{i + 1}" for i in range(cluster_count)]
        if cluster_count > 1
        else ["upgrade_nfs"]
    )

    # Port assignment: each cluster gets a unique port.
    base_port = nfs_scale.get("base_port", 2049)
    cluster_ports = {name: base_port + i for i, name in enumerate(cluster_names)}

    # Build a multi-document YAML spec for all NFS clusters.
    # On < 9.1: pin each cluster to a specific host (no colocation).
    # On >= 9.1: add monitoring_port so colocation is safe.
    base_monitoring_port = 9587
    spec_lines = []
    for idx, cluster_id in enumerate(cluster_names):
        port = cluster_ports[cluster_id]
        if nfs_hosts and idx < len(nfs_hosts):
            placement = f"  hosts:\n  - {nfs_hosts[idx]}\n"
        else:
            placement = f"  count: {daemons_per_cluster}\n"
        spec_block = (
            f"service_type: nfs\n"
            f"service_id: {cluster_id}\n"
            f"placement:\n"
            f"{placement}"
            f"spec:\n"
            f"  port: {port}\n"
        )
        if colocation_supported:
            spec_block += f"  monitoring_port: {base_monitoring_port + idx}\n"
        if enable_nfsv3:
            spec_block += "  enable_nlm: true\n"
        spec_lines.append(spec_block)

    combined_spec = "---\n".join(spec_lines)

    # IBMCEPH-17794: Use CLI to create the first NFS cluster so that Ceph
    # auto-creates the .nfs RADOS pool with correct application tag.  Direct
    # pool creation via `rados_obj.create_pool(".nfs", ...)` produces a pool
    # that NFS-Ganesha cannot use (missing internal RADOS objects).  After the
    # first cluster exists (and .nfs pool is ready), deploy all clusters via
    # spec -- orch apply is idempotent for the already-created first cluster.
    if ".nfs" not in rados_obj.list_pools():
        first_name = cluster_names[0]
        first_port = cluster_ports[first_name]
        if nfs_hosts:
            first_placement = nfs_hosts[0]
        else:
            first_placement = str(daemons_per_cluster)
        cli_cmd = (
            f"ceph nfs cluster create {first_name} "
            f'"{first_placement}" --port {first_port}'
        )
        if enable_nfsv3:
            cli_cmd += " --enable-nfsv3"
        log.info(f"Creating first NFS cluster via CLI (triggers .nfs pool): {cli_cmd}")
        rados_obj.node.shell([cli_cmd], timeout=120)
        time.sleep(5)
        if ".nfs" not in rados_obj.list_pools():
            raise RuntimeError(
                "IBMCEPH-17794: .nfs pool not created after "
                f"'ceph nfs cluster create {first_name}'"
            )
        log.info(".nfs pool created successfully via CLI")
    else:
        log.info(".nfs pool already exists")

    log.info(
        f"Deploying {len(cluster_names)} NFS cluster(s) via spec "
        f"(colocation={'yes' if colocation_supported else 'no'}, "
        f"pinned={'yes' if nfs_hosts else 'no'})"
    )
    rados_obj.node.shell(
        [
            f"bash -c 'cat > /tmp/nfs_spec.yaml <<EOFSPEC\n{combined_spec}\nEOFSPEC\n"
            f"ceph orch apply -i /tmp/nfs_spec.yaml'"
        ],
        timeout=max(120, len(cluster_names) * 15),
    )

    # Wait for all NFS services concurrently.
    def _wait_nfs_service(cid):
        _wait_for_service(rados_obj, f"nfs.{cid}", timeout=180)
        return cid

    with ThreadPoolExecutor(max_workers=min(10, len(cluster_names))) as executor:
        futures = {
            executor.submit(_wait_nfs_service, cid): cid for cid in cluster_names
        }
        for future in as_completed(futures):
            cid = futures[future]
            try:
                future.result()
                log.info(f"NFS cluster '{cid}' created on port {cluster_ports[cid]}")
            except Exception as e:
                log.warning(f"NFS cluster '{cid}' wait failed: {e}")

    # Open firewall ports for NFSv3 on all NFS server nodes.
    # NFSv3 needs rpcbind (111) + the actual mountd port.  Ganesha binds mountd
    # on a random ephemeral port (not 20048), so the firewalld 'mountd' service
    # alone is insufficient.  We discover and open the real port in a single
    # atomic SSH call to avoid races and minimise connections.
    if enable_nfsv3:
        _open_nfs_firewall_ports(rados_obj, ceph_cluster, cluster_names)

    # Open firewall ports for multi-cluster port range (unconditional).
    _open_nfs_port_range(
        rados_obj, ceph_cluster, cluster_names, base_port, cluster_count
    )

    # Validate which NFS clusters actually deployed
    existing_nfs = _get_existing_nfs_clusters(rados_obj)
    expected_count = cluster_count
    if len(existing_nfs) < expected_count:
        log.warning(
            f"Only {len(existing_nfs)} of {expected_count} NFS clusters "
            f"deployed successfully: {sorted(existing_nfs)}"
        )

    # --delegations is only available in Ceph Tentacle (20.x / RHCS 9.x+).
    delegations_flag = ""
    if nfs_config.get("delegations", False):
        if ceph_ver and (
            ceph_ver.startswith("20.")
            or LooseVersion(ceph_ver) >= LooseVersion("20.0.0")
        ):
            delegations_flag = " --delegations rw"
            log.info("NFS exports: --delegations rw (Ceph >= 20.x)")
        elif ceph_ver:
            log.info(f"NFS exports: skipping --delegations (Ceph {ceph_ver} < 20.x)")
        else:
            log.info("NFS exports: skipping --delegations (version unknown)")

    cephfs_scale = config.get("scale", {}).get("cephfs", {})
    filesystems = cephfs_scale.get("filesystems", ["cephfs_direct", "cephfs_nfs"])
    nfs_fs = nfs_scale.get("filesystem", "cephfs_nfs")
    if nfs_fs not in filesystems:
        log.warning(
            f"NFS filesystem '{nfs_fs}' not in filesystems list {filesystems}; "
            f"falling back to {filesystems[-1]}"
        )
        nfs_fs = filesystems[-1]
    # Ganesha Minor_Versions defaults to "1, 2" -- NFSv4.0 is not served.
    nfs_versions = nfs_scale.get("nfs_versions", ["3", "4.1", "4.2"])
    if not enable_nfsv3 and "3" in nfs_versions:
        log.info(
            "Removing NFSv3 from mount list -- --enable-nfsv3 not available "
            "on this Ceph version; NFSv3 mounts would always time out"
        )
        nfs_versions = [v for v in nfs_versions if v != "3"]
    mounts_per_version = nfs_scale.get("mounts_per_version", 2)

    log.info(f"Waiting for active MDS on {nfs_fs} before NFS subvolumes...")
    _wait_for_active_mds(rados_obj, nfs_fs)

    # Per-cluster SVG groups on the NFS filesystem (batched)
    pin_policy = cephfs_scale.get("pin_policy", "distributed")
    svg_lines = ["#!/bin/bash", "set +e"]
    for ci in range(len(cluster_names)):
        svg = f"nfs_svg{ci + 1}"
        svg_lines.append(
            f"ceph fs subvolumegroup create {nfs_fs} {svg} 2>/dev/null || true"
        )
    svg_lines.append('echo "NFS_SVG_DONE"')
    try:
        rados_obj.node.shell(
            ["bash -s <<'EOFSVG'\n" + "\n".join(svg_lines) + "\nEOFSVG"],
            timeout=60,
        )
    except Exception as e:
        log.warning(f"NFS SVG batch create: {e}")

    for ci in range(len(cluster_names)):
        _pin_subvolumegroup(rados_obj, nfs_fs, f"nfs_svg{ci + 1}", pin_policy)

    # Versioned exports: mounts_per_version per NFS version per cluster.
    # Layout: 20 clusters x 2 versions x 2 mounts = 80 exports (default).
    # Parallelized: per-cluster (subvol create+getpath -> export create)
    # pipelines run concurrently across 8 threads.
    versioned_exports: dict[str, list[dict]] = {}

    def _create_cluster_exports(ci, cluster_id):
        """Per-cluster export pipeline: create subvols, resolve paths, exports."""
        svg = f"nfs_svg{ci + 1}"
        cluster_exports = []
        sv_entries = []
        for ver in nfs_versions:
            ver_safe = ver.replace(".", "_")
            for midx in range(mounts_per_version):
                sv_name = f"nfsvol_c{ci + 1}_v{ver_safe}_{midx}"
                pseudo = f"/export/c{ci + 1}/v{ver}/{midx}"
                squash = ""
                sv_mode = None
                if midx == 1 and ver in ("3", "4.1"):
                    squash = " --squash=root_squash"
                    sv_mode = 777
                sv_entries.append(
                    {
                        "fs_name": nfs_fs,
                        "sv_name": sv_name,
                        "svg": svg,
                        "pseudo": pseudo,
                        "ver": ver,
                        "midx": midx,
                        "squash": squash,
                        "sv_mode": sv_mode,
                    }
                )

        # Fuse create + getpath into one SSH round-trip.
        sv_lines = ["#!/bin/bash", "set +e"]
        for idx, entry in enumerate(sv_entries):
            mode_arg = f" --mode={entry['sv_mode']}" if entry["sv_mode"] else ""
            sv_lines.append(
                f"ceph fs subvolume create {entry['fs_name']}"
                f" {entry['sv_name']}"
                f" --group_name {entry['svg']} --earmark nfs{mode_arg}"
            )
            sv_lines.append(
                f'echo "NFS:{idx}:$(ceph fs subvolume getpath '
                f'{entry["fs_name"]} {entry["sv_name"]} '
                f'--group_name {entry["svg"]} 2>/dev/null)"'
            )
        sv_paths = {}
        try:
            out, _ = rados_obj.node.shell(
                ["bash -s <<'EOFSVCREATE'\n" + "\n".join(sv_lines) + "\nEOFSVCREATE"],
                timeout=max(300, len(sv_entries) * 12),
            )
            for line in (out or "").strip().splitlines():
                if not line.startswith("NFS:"):
                    continue
                parts = line.split(":", 2)
                if len(parts) == 3 and parts[2].strip().startswith("/"):
                    sv_paths[int(parts[1])] = parts[2].strip()
        except Exception as e:
            log.warning(f"NFS batch subvol create+getpath {cluster_id}: {e}")

        export_lines = ["#!/bin/bash", "set +e"]
        for idx, entry in enumerate(sv_entries):
            sv_path = sv_paths.get(idx)
            if not sv_path:
                log.warning(f"NFS subvolume path {entry['sv_name']}: empty/missing")
                cluster_exports.append(
                    {
                        "pseudo": entry["pseudo"],
                        "ver": entry["ver"],
                        "midx": entry["midx"],
                        "ok": False,
                    }
                )
                continue
            export_lines.append(
                f"ceph nfs export create cephfs"
                f" --cluster-id {cluster_id}"
                f" --pseudo-path {entry['pseudo']}"
                f" --fsname {entry['fs_name']}"
                f" --path {sv_path}"
                f"{delegations_flag}{entry['squash']}"
            )
            cluster_exports.append(
                {
                    "pseudo": entry["pseudo"],
                    "ver": entry["ver"],
                    "midx": entry["midx"],
                    "ok": True,
                }
            )

        if len(export_lines) > 2:
            try:
                rados_obj.node.shell(
                    [
                        "bash -s <<'EOFEXPORT'\n"
                        + "\n".join(export_lines)
                        + "\nEOFEXPORT"
                    ],
                    timeout=max(120, len(sv_entries) * 10),
                )
            except Exception as e:
                log.warning(f"NFS batch export create {cluster_id}: {e}")
                for exp in cluster_exports:
                    if exp.get("ok"):
                        exp["ok"] = False

        return cluster_exports

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        futures = {
            pool.submit(_create_cluster_exports, ci, cid): cid
            for ci, cid in enumerate(cluster_names)
        }
        for future in concurrent.futures.as_completed(futures):
            cid = futures[future]
            try:
                versioned_exports[cid] = future.result()
            except Exception as e:
                log.warning(f"NFS export pipeline failed for {cid}: {e}")
                versioned_exports[cid] = []

    total_exports = sum(
        1 for exps in versioned_exports.values() for e in exps if e["ok"]
    )
    log.info(
        f"NFS setup: {total_exports} versioned exports across "
        f"{len(cluster_names)} cluster(s) "
        f"({len(nfs_versions)} versions x {mounts_per_version} mounts each)"
    )

    # Set export defaults on each cluster (requires Ceph >= 20.2.0 / RHCS 9.1+)
    if nfs_config.get("export_default", False):
        export_default_ok = ver_tuple >= (20, 2, 0)
        if export_default_ok:
            deleg_default = "rw" if nfs_config.get("delegations", False) else "none"
            for cluster_id in cluster_names:
                try:
                    rados_obj.node.shell(
                        [
                            f"ceph nfs cluster set-export-default "
                            f"{cluster_id} {deleg_default}"
                        ]
                    )
                except Exception as e:
                    log.warning(
                        f"NFS export default on {cluster_id} "
                        f"(may not be available on this version): {e}"
                    )
        else:
            log.info("NFS export default: skipped (requires Ceph >= 20.2.0 / 9.1+)")

    # Mount versioned exports on client nodes with overlap factor.
    # Each export is mounted on `overlap` clients (round-robin assignment).
    # Batched: all mount commands are grouped per client into a single SSH
    # script to eliminate per-mount SSH overhead (~1.6s each).
    clients = ceph_cluster.get_nodes(role="client")
    overlap = min(3, len(clients))

    for client in clients:
        try:
            client.exec_command(
                sudo=True,
                cmd="systemctl start rpcbind 2>/dev/null",
                timeout=15,
            )
        except Exception:
            pass

    # Pre-resolve NFS server IPs for all clusters (avoids per-mount lookup).
    cluster_ips: dict = {}
    for ci, cluster_id in enumerate(cluster_names):
        nfs_ip = _get_nfs_server_ip(rados_obj, cluster_id, ceph_cluster)
        if nfs_ip:
            cluster_ips[cluster_id] = (ci, nfs_ip)
        else:
            log.warning(
                f"Could not resolve NFS server IP for {cluster_id}, "
                "skipping client mounts"
            )

    # Build batched mount scripts per client.
    mount_cmds_by_client: dict = {c: [] for c in clients}
    for cluster_id, (ci, nfs_ip) in cluster_ips.items():
        port = cluster_ports[cluster_id]
        ok_exports = [
            exp for exp in versioned_exports.get(cluster_id, []) if exp.get("ok")
        ]
        for exp_idx, exp in enumerate(ok_exports):
            pseudo = exp["pseudo"]
            ver = exp["ver"]
            midx = exp["midx"]
            mnt = f"/mnt/nfs_c{ci + 1}_v{ver}_{midx}"
            tmo = 90 if ver == "3" else 30
            if ver == "3":
                mcmd = (
                    f"mount -t nfs -o vers=3,noresvport,port={port} "
                    f"{nfs_ip}:{pseudo} {mnt}"
                )
            else:
                mcmd = (
                    f"mount -t nfs4 -o vers={ver},port={port} "
                    f"{nfs_ip}:{pseudo} {mnt}"
                )
            line = f"mkdir -p {mnt} && timeout {tmo} {mcmd} && echo 'OK:{mnt}'"
            for j in range(overlap):
                target_client = clients[(exp_idx + j) % len(clients)]
                mount_cmds_by_client[target_client].append(line)

    # Execute one batched script per client.
    for client in clients:
        cmds = mount_cmds_by_client.get(client, [])
        if not cmds:
            continue
        hostname = getattr(client, "hostname", str(client))
        script = (
            "#!/bin/bash\nset +e\nOK=0; FAIL=0\n"
            + "\n".join(f"{{ {c}; }} && OK=$((OK+1)) || FAIL=$((FAIL+1))" for c in cmds)
            + '\necho "NFS_MOUNT_DONE ok=$OK fail=$FAIL"'
        )
        try:
            out, _ = client.exec_command(
                sudo=True,
                cmd=f"bash -s <<'EOFMNT'\n{script}\nEOFMNT",
                timeout=max(600, len(cmds) * 3),
            )
            summary = ""
            for ln in (out or "").strip().splitlines():
                if ln.startswith("NFS_MOUNT_DONE"):
                    summary = ln
            log.info(
                f"NFS mounts on {hostname}: {len(cmds)} attempted -- "
                f"{summary or '(no summary)'}"
            )
        except Exception as e:
            log.warning(f"NFS batch mount on {hostname}: {e}")


def _resolve_daemon_host_ip(rados_obj, service_prefix, cluster_id, ceph_cluster=None):
    """Resolve the IP of a daemon host for *service_prefix*.*cluster_id*.

    4-step resolution:
      1. ``ceph orch ps --service-name {service_prefix}.{cluster_id}``
      2. cephci cluster node list (inventory)
      3. ``ceph orch host ls`` (orchestrator registry)
      4. ``getent hosts`` on admin node (DNS / /etc/hosts)

    Returns the IP string or "" on failure.
    """
    try:
        daemons = rados_obj.run_ceph_command(
            cmd=(f"ceph orch ps --service-name {service_prefix}.{cluster_id}")
        )
        if not isinstance(daemons, list) or not daemons:
            log.warning(
                f"No {service_prefix} daemons found via orch ps for {cluster_id}"
            )
            return ""

        hostname = ""
        for d in daemons:
            if d.get("status_desc") == "running":
                hostname = d.get("hostname", "")
                break
        if not hostname:
            hostname = daemons[0].get("hostname", "")
        if not hostname:
            return ""

        # 1. cephci cluster node list (authoritative inventory)
        if ceph_cluster:
            for node in ceph_cluster.get_nodes():
                if getattr(node, "hostname", "") == hostname:
                    ip = getattr(node, "ip_address", "")
                    if ip:
                        log.info(
                            f"{service_prefix} {cluster_id}: {hostname} -> {ip} "
                            "(via cluster nodes)"
                        )
                        return ip

        # 2. ceph orch host ls (host addr registered with orchestrator)
        try:
            hosts = rados_obj.run_ceph_command(cmd="ceph orch host ls")
            if isinstance(hosts, list):
                for h in hosts:
                    if h.get("hostname") == hostname:
                        addr = h.get("addr", "")
                        if addr:
                            log.info(
                                f"{service_prefix} {cluster_id}: "
                                f"{hostname} -> {addr} (via orch host ls)"
                            )
                            return addr
        except Exception as e:
            log.debug(f"orch host ls lookup failed: {e}")

        # 3. DNS / /etc/hosts on admin node
        try:
            result, _ = rados_obj.node.shell(
                ['bash -c "getent hosts ' f"{hostname} | awk '{{print $1}}'\""]
            )
            ip = result.strip()
            if ip:
                log.info(
                    f"{service_prefix} {cluster_id}: {hostname} -> {ip} "
                    "(via getent hosts)"
                )
                return ip
        except Exception:
            pass
    except Exception as e:
        log.warning(f"{service_prefix} orch ps lookup for {cluster_id}: {e}")

    return ""


def _get_nfs_server_ip(rados_obj, cluster_id, ceph_cluster=None):
    """Resolve the IP of an NFS daemon host for *cluster_id*.

    Delegates to ``_resolve_daemon_host_ip`` for the standard 4-step
    resolution and falls back to ``ceph nfs cluster info`` as a last
    resort (known to mis-resolve IPs).
    """
    ip = _resolve_daemon_host_ip(rados_obj, "nfs", cluster_id, ceph_cluster)
    if ip:
        return ip

    # Last resort: ceph nfs cluster info (known to mis-resolve IPs)
    try:
        info = rados_obj.run_ceph_command(cmd=f"ceph nfs cluster info {cluster_id}")
        cluster_data = info.get(cluster_id, info) if isinstance(info, dict) else {}
        backends = cluster_data.get("backend", [])
        if backends:
            ip = backends[0].get("ip", "")
            if ip:
                log.warning(
                    f"NFS {cluster_id}: using cluster-info fallback IP {ip} "
                    "(orch-based resolution failed)"
                )
                return ip
    except Exception as e:
        log.warning(f"NFS cluster info fallback for {cluster_id}: {e}")

    return ""


def _setup_smb(ceph_cluster, rados_obj, config):
    """Phase 1: Deploy SMB cluster with scaled shares on a dedicated CephFS.

    Scale is driven by ``config["scale"]["smb"]``:
        share_count (int): Total SMB shares to create (default 500).
        filesystem (str): Dedicated FS (default ``cephfs_smb``).
    Creates its own ``smb_svg`` / ``smbvol*`` subvolumes (does not reuse
    ``_setup_cephfs`` scale volumes). Uses ``scale.cephfs.pin_policy`` for
    SVG-level distributed pin.

    Raises on hard failures (cluster create, zero shares) so the caller
    does not add a non-functional SMB to deployed_services.
    """
    log.info("Setting up SMB")
    try:
        # mutating — do not use run_ceph_command (empty stdout => {})
        rados_obj.node.shell(["ceph mgr module enable smb"])
    except Exception as e:
        log.warning(f"SMB module enable: {e}")

    smb_scale = config.get("scale", {}).get("smb", {})
    share_count = smb_scale.get("share_count", 500)
    pin_policy = (
        config.get("scale", {}).get("cephfs", {}).get("pin_policy", "distributed")
    )

    fs_name = smb_scale.get("filesystem", "cephfs_smb")
    svg_name = "smb_svg"

    # Create the SMB filesystem if it doesn't already exist (it may not
    # be in the default cephfs filesystems list since SMB is off by default).
    existing_fs = []
    try:
        existing_fs = [
            v.get("name", "") for v in rados_obj.run_ceph_command("ceph fs ls")
        ]
    except Exception:
        pass
    if fs_name not in existing_fs:
        try:
            # mutating — do not use run_ceph_command (empty stdout => {})
            rados_obj.node.shell([f"ceph fs volume create {fs_name}"])
            log.info(f"Created SMB filesystem: {fs_name}")
            cephfs_scale = config.get("scale", {}).get("cephfs", {})
            active_mds = cephfs_scale.get("active_mds", 6)
            count_per_host = cephfs_scale.get("mds_count_per_host", 2)
            spec = {
                "service_type": "mds",
                "service_id": fs_name,
                "placement": {
                    "label": "mds",
                    "count_per_host": count_per_host,
                },
            }
            rados_obj.node.shell(
                [
                    f"bash -s <<'EOFMDS'\n"
                    f"echo '{json.dumps(spec)}' | "
                    f"ceph orch apply -i -\n"
                    f"EOFMDS"
                ],
            )
            rados_obj.node.shell([f"ceph fs set {fs_name} max_mds {active_mds}"])
            _wait_for_active_mds(rados_obj, fs_name)
        except Exception as e:
            log.warning(f"SMB filesystem setup for {fs_name}: {e}")

    smb_cluster_id = "upgradesmb"
    rados_obj.node.shell(
        [
            f"ceph smb cluster create {smb_cluster_id} "
            f"user --define-user-pass smbuser%smbpass"
        ]
    )

    try:
        rados_obj.node.shell([f"ceph fs subvolumegroup create {fs_name} {svg_name}"])
    except Exception as e:
        log.warning(f"SMB subvolumegroup create: {e}")

    _pin_subvolumegroup(rados_obj, fs_name, svg_name, pin_policy)

    # Fuse create + getpath (share create needs every path).
    sv_lines = ["#!/bin/bash", "set +e", "OK=0", "FAIL=0"]
    for i in range(share_count):
        sv_name = f"smbvol{i:04d}"
        sv_lines.append(
            f"ceph fs subvolume create {fs_name} {sv_name} "
            f"--group_name {svg_name} --earmark smb 2>/dev/null "
            f"&& OK=$((OK+1)) || FAIL=$((FAIL+1))"
        )
        sv_lines.append(
            f'echo "SMBGP:{sv_name}:$(ceph fs subvolume getpath '
            f'{fs_name} {sv_name} --group_name {svg_name} 2>/dev/null)"'
        )
    sv_lines.append('echo "SMB_SV_DONE ok=$OK fail=$FAIL"')
    sv_script = "\n".join(sv_lines)
    sv_paths = []
    try:
        sv_out, _ = rados_obj.node.shell(
            [f"bash -s <<'EOFSMB_SV'\n{sv_script}\nEOFSMB_SV"],
            timeout=max(600, share_count * 5),
        )
        for ln in (sv_out or "").strip().splitlines():
            if ln.startswith("SMB_SV_DONE"):
                log.info(f"SMB subvolume batch: {ln}")
            if not ln.startswith("SMBGP:"):
                continue
            parts = ln.split(":", 2)
            if len(parts) == 3 and parts[2].strip().startswith("/"):
                sv_paths.append((parts[1], parts[2].strip()))
    except Exception as e:
        log.warning(f"SMB batch subvolume create+getpath failed: {e}")

    if not sv_paths:
        raise RuntimeError(
            f"SMB setup failed: 0/{share_count} subvolume paths resolved "
            f"on {smb_cluster_id}"
        )

    # --- Batch share creates ---
    sh_lines = ["#!/bin/bash", "set +e", "OK=0", "FAIL=0"]
    for idx, (sv_name, sv_path) in enumerate(sv_paths):
        share_name = f"share{idx:04d}"
        sh_lines.append(
            f"ceph smb share create {smb_cluster_id} "
            f"{share_name} {fs_name} {sv_path} 2>/dev/null "
            f"&& OK=$((OK+1)) || FAIL=$((FAIL+1))"
        )
    sh_lines.append('echo "SMB_SHARE_DONE ok=$OK fail=$FAIL"')
    sh_script = "\n".join(sh_lines)
    created = 0
    try:
        sh_out, _ = rados_obj.node.shell(
            [f"bash -s <<'EOFSMB_SH'\n{sh_script}\nEOFSMB_SH"],
            timeout=max(600, len(sv_paths) * 3),
        )
        for ln in (sh_out or "").strip().splitlines():
            if ln.startswith("SMB_SHARE_DONE"):
                log.info(f"SMB share batch: {ln}")
                for token in ln.split():
                    if token.startswith("ok="):
                        try:
                            created = int(token.split("=", 1)[1])
                        except ValueError:
                            pass
                break
    except Exception as e:
        log.warning(f"SMB batch share create failed: {e}")

    if created == 0:
        raise RuntimeError(
            f"SMB setup failed: 0/{share_count} shares created on {smb_cluster_id}"
        )

    log.info(
        f"SMB setup complete: {created}/{len(sv_paths)} shares "
        f"on cluster {smb_cluster_id}"
    )

    _wait_for_smb_daemon(rados_obj, smb_cluster_id)

    smb_ip = _resolve_daemon_host_ip(rados_obj, "smb", smb_cluster_id, ceph_cluster)
    if smb_ip:
        clients = ceph_cluster.get_nodes(role="client")
        for client in clients:
            mnt = "/mnt/smb_share0000"
            try:
                client.exec_command(sudo=True, cmd=f"mkdir -p {mnt}", timeout=15)
                client.exec_command(
                    sudo=True,
                    cmd=(
                        f"mount -t cifs //{smb_ip}/share0000 {mnt} "
                        f"-o username=smbuser,password=smbpass"
                    ),
                    timeout=60,
                )
                log.info(
                    f"SMB mount: share0000 -> {mnt} on "
                    f"{getattr(client, 'hostname', client)}"
                )
            except Exception as e:
                log.warning(f"SMB mount share0000: {e}")
    else:
        log.warning("Could not resolve SMB server IP, skipping client mounts")


def _wait_for_smb_daemon(rados_obj, cluster_id, timeout=120):
    """Wait for at least one SMB daemon to reach 'running' status."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            daemons = rados_obj.run_ceph_command(
                cmd=f"ceph orch ps --service-name smb.{cluster_id}"
            )
            if isinstance(daemons, list):
                for d in daemons:
                    if d.get("status_desc") == "running":
                        log.info(
                            f"SMB daemon {d.get('daemon_name', '?')} running "
                            f"on {d.get('hostname', '?')}"
                        )
                        return
        except Exception:
            pass
        time.sleep(10)
    log.warning(
        f"No SMB daemon reached 'running' for smb.{cluster_id} "
        f"within {timeout}s -- mounts may fail"
    )


def _wait_for_active_mds(rados_obj, fs_name, attempts=30, poll_sec=10):
    """Poll until at least one active MDS exists for *fs_name*.

    Returns True if an active MDS was detected within *attempts* x *poll_sec*.
    """
    for attempt in range(attempts):
        try:
            out = rados_obj.run_ceph_command(cmd=f"ceph fs status {fs_name}")
            mds_list = out.get("mdsmap", [])
            active_count = sum(
                1
                for m in mds_list
                if m.get("state", "").startswith("active")
                or m.get("state", "") == "up:active"
            )
            if active_count > 0:
                log.info(f"{fs_name}: {active_count} active MDS daemon(s) ready")
                return True
            log.debug(
                f"{fs_name}: waiting for active MDS ({attempt + 1}/{attempts})..."
            )
        except Exception:
            pass
        time.sleep(poll_sec)

    log.warning(
        f"{fs_name}: no active MDS after {attempts * poll_sec}s "
        "-- subvolume creation may fail"
    )
    return False


def _wait_for_pg_creation(rados_obj, timeout=180, poll_interval=10):
    """Wait until no PGs are in 'creating' state.

    Blocks up to *timeout* seconds polling ``ceph pg stat``.  Does NOT
    wait for full 'active+clean' -- only for PG creation to finish so OSD
    bandwidth pressure drops enough for CephFS mount operations.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            pg_stat = rados_obj.run_ceph_command(cmd="ceph pg stat")
            state_map = pg_stat.get("pg_summary", {}).get(
                "num_pg_by_state", []
            ) or pg_stat.get("num_pg_by_state", [])
            creating = sum(
                s.get("num", 0) for s in state_map if "creating" in s.get("name", "")
            )
            if creating == 0:
                log.info("PG creation complete -- no PGs in 'creating' state")
                return True
            log.info(f"Waiting for PG creation: {creating} PGs still creating...")
        except Exception as e:
            log.warning(f"PG stat check: {e}")
        time.sleep(poll_interval)
    log.warning(f"PG creation wait timed out after {timeout}s, proceeding with mounts")
    return False


def _wait_for_service(rados_obj, service_name, timeout=120):
    """Wait for a ceph orch service to become available."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            out = rados_obj.run_ceph_command(
                cmd=f"ceph orch ls --service-name {service_name}"
            )
            if isinstance(out, list) and out:
                svc = out[0]
                running = svc.get("status", {}).get("running", 0)
                expected = svc.get("status", {}).get("size", 0)
                if running > 0 and running >= expected:
                    return
        except Exception:
            pass
        time.sleep(5)
    log.warning(f"Service {service_name} not fully ready after {timeout}s")


def _pin_all_pool_pg_counts(rados_obj):
    """Pin pg_num_min and pg_num_max on every pool to lock PG counts during upgrade.

    Discovers all pools via ``ceph osd pool ls detail``, reads each pool's
    current ``pg_num``, and sets:
      - pg_num_max = current * 2  (for pg_num > 4; else current)
      - pg_num_min = current

    Order: pg_num_max is set first (always >= current, safe).
    Then pg_num_min (equals current, safe).

    For small pools like ``.mgr`` (pg_num=1), pg_num_max is set equal to
    current to keep them compact.
    """
    try:
        pools = rados_obj.run_ceph_command(cmd="ceph osd pool ls detail")
    except Exception as e:
        log.warning(f"Cannot list pools for PG pinning: {e}")
        return

    # Batch all pg_num_max and pg_num_min sets into one shell script.
    # Order preserved: pg_num_max first (>= current, safe), then pg_num_min.
    # && chaining per pool ensures pg_num_min is only set if pg_num_max succeeded.
    pin_lines = ["#!/bin/bash", "set +e", "OK=0", "FAIL=0"]
    for pool_info in pools:
        pool_name = pool_info["pool_name"]
        current_pg = pool_info["pg_num"]
        pg_max = current_pg if current_pg <= 4 else current_pg * 2
        pg_min = current_pg
        pin_lines.append(
            f"ceph osd pool set {pool_name} pg_num_max {pg_max} 2>&1 && "
            f"ceph osd pool set {pool_name} pg_num_min {pg_min} 2>&1 && "
            f"OK=$((OK+1)) || FAIL=$((FAIL+1))"
        )
    pin_lines.append('echo "PINNED ok=$OK fail=$FAIL"')
    pin_script = "\n".join(pin_lines)

    try:
        pin_out, _ = rados_obj.node.shell(
            [f"bash -s <<'EOFPG'\n{pin_script}\nEOFPG"],
            timeout=max(120, len(pools) * 5),
        )
        summary = ""
        for line in (pin_out or "").strip().splitlines():
            if line.startswith("PINNED"):
                summary = line
        log.info(f"PG pin batch: {summary or f'{len(pools)} pools submitted'}")
    except Exception as e:
        log.warning(f"PG pin batch failed: {e}")


def _wait_for_daemon_recovery(feat_mgr, rados_obj, timeout_sec, interval=30):
    """Poll until orch daemon running counts match the pre-upgrade snapshot."""
    pre = (
        feat_mgr._daemon_states.get("pre", {})
        if hasattr(feat_mgr, "_daemon_states")
        else {}
    )
    if not pre:
        log.info("No pre-upgrade daemon snapshot; skipping daemon recovery wait")
        return

    log.info(
        f"Waiting for daemon running counts to match pre-upgrade "
        f"snapshot (timeout={timeout_sec}s)"
    )
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            orch_ps = rados_obj.run_ceph_command(cmd="ceph orch ps")
            current = summarize_orch_ps_running_counts(orch_ps)
        except Exception as exc:
            log.warning(f"Daemon recovery poll failed: {exc}")
            time.sleep(interval)
            continue

        mismatches = daemon_running_count_mismatches(pre, current)
        if not mismatches:
            log.info("Daemon running counts match pre-upgrade snapshot")
            return

        log.info(
            f"Daemon recovery pending ({len(mismatches)} type(s)): "
            f"{', '.join(mismatches[:5])}"
        )
        time.sleep(interval)

    log.warning(
        f"Daemon recovery wait timed out after {timeout_sec}s; "
        "proceeding with post-upgrade snapshot"
    )


def _wait_for_stabilization(rados_obj, timeout_sec):
    """Wait for clean PGs and acceptable health.

    Increases osd_max_backfills and osd_recovery_max_active (default 18)
    before waiting so that backfill and recovery proceed at full speed.
    Restores defaults after PGs are clean.
    """
    log.info(f"Waiting for cluster stabilization (timeout={timeout_sec}s)")

    try:
        rados_obj.change_recovery_threads(config={}, action="set")
        log.info("Recovery threads increased -- backfill/recovery at full speed")
    except Exception as e:
        log.warning(f"Could not increase recovery threads: {e}")

    try:
        rados_obj.wait_for_clean_pg_sets(
            timeout=timeout_sec,
            sleep_interval=15,
        )
        log.info("Cluster PGs are clean")
    except Exception as e:
        log.warning(f"PG stabilization incomplete: {e}")

    try:
        rados_obj.change_recovery_threads(config={}, action="rm")
        log.info("Recovery threads restored to defaults")
    except Exception as e:
        log.warning(f"Could not restore recovery threads: {e}")

    try:
        health = rados_obj.run_ceph_command(cmd="ceph health")
        status = (
            str(health) if not isinstance(health, dict) else health.get("status", "")
        )
        if "HEALTH_OK" in status:
            log.info("Cluster is HEALTH_OK")
        else:
            log.warning(f"Cluster health: {status}")
    except Exception as e:
        log.warning(f"Health check during stabilization: {e}")


def _sleep_with_io_sampling(stats, total_sec, interval=60):
    """Sleep while taking periodic IO snapshots for chart data."""
    elapsed = 0
    while elapsed < total_sec:
        chunk = min(interval, total_sec - elapsed)
        time.sleep(chunk)
        elapsed += chunk
        if elapsed < total_sec:
            stats.io_stats_snapshot()


def _run_preflight_checks(rados_obj, orch_obj, config):
    """Phase 3: Pre-flight resource and readiness checks."""
    log.info("Running pre-flight checks")

    # Host reachability
    try:
        out = rados_obj.run_ceph_command(cmd="ceph orch host ls")
        if isinstance(out, list):
            offline = [
                h["hostname"]
                for h in out
                if h.get("status", "").lower() != "online" and h.get("status", "") != ""
            ]
            if offline:
                log.warning(f"Offline hosts detected: {offline}")
    except Exception as e:
        log.warning(f"Host reachability check failed: {e}")

    # Disk usage
    try:
        df_out = rados_obj.run_ceph_command(cmd="ceph df")
        if isinstance(df_out, dict):
            stats = df_out.get("stats", {})
            total = stats.get("total_bytes", 1)
            used = stats.get("total_used_bytes", 0)
            pct = (used / total) * 100 if total > 0 else 0
            abort_at = config.get("cluster_fill", {}).get("abort_at_percent", 75)
            warn_at = (config.get("phase_timing") or {}).get("disk_warning_percent", 60)
            if pct >= abort_at:
                raise RuntimeError(
                    f"Disk usage {pct:.1f}% exceeds abort " f"threshold {abort_at}%"
                )
            if pct >= warn_at:
                log.warning(
                    f"Disk usage {pct:.1f}% exceeds warning " f"threshold {warn_at}%"
                )
            else:
                log.info(f"Disk usage: {pct:.1f}%")
    except RuntimeError:
        raise
    except Exception as e:
        log.warning(f"Disk usage check failed: {e}")

    # Upgrade check (informational -- full check with --image runs post-context)
    try:
        status = orch_obj.upgrade_status(timeout=60)
        if status.get("in_progress"):
            log.warning(
                "Upgrade already in progress at preflight: %s",
                status.get("progress"),
            )
        else:
            log.info("No upgrade currently in progress (preflight OK)")
    except Exception as e:
        log.warning(f"Upgrade status check: {e}")

    # IBMCEPH-17825: Enable cephadm module debug logging for detailed
    # per-daemon upgrade timeline extraction from MGR logs.
    for cfg_cmd in [
        "ceph config set mgr mgr/cephadm/log_to_cluster true",
        "ceph config set mgr mgr/cephadm/log_to_cluster_level debug",
        "ceph config set mgr mgr/cephadm/log_level debug",
    ]:
        try:
            rados_obj.node.shell([cfg_cmd])
            log.info(f"Set: {cfg_cmd}")
        except Exception as e:
            log.warning(f"Failed to set cephadm config: {cfg_cmd} -- {e}")


def _parallel_remove_repos(nodes):
    if not nodes:
        return
    with ThreadPoolExecutor(max_workers=len(nodes)) as executor:
        futures = {
            executor.submit(remove_repos, ceph_node=node): node for node in nodes
        }
        for future in as_completed(futures):
            node = futures[future]
            try:
                future.result()
                log.debug(f"remove_repos OK: {getattr(node, 'hostname', node)}")
            except Exception as e:
                log.warning(
                    f"remove_repos failed on " f"{getattr(node, 'hostname', node)}: {e}"
                )


def _parallel_set_repo(nodes, config):
    if not nodes:
        return
    cloud_type = config.get("cloud-type", "openstack")
    hotfix_repo = config.get("hotfix_repo")
    base_url = config["base_url"]

    def _set_repo_on_node(node):
        if hotfix_repo:
            node.exec_command(
                sudo=True,
                cmd=(f"curl -o /etc/yum.repos.d/rh_hotfix_repo.repo " f"{hotfix_repo}"),
            )
            node.exec_command(sudo=True, cmd="yum makecache", check_ec=False)
        elif base_url.endswith(".repo"):
            node.exec_command(
                sudo=True,
                cmd=f"yum-config-manager --add-repo {base_url}",
            )
        else:
            url = base_url
            if not url.endswith("/"):
                url += "/"
            if cloud_type == "ibmc":
                url += "Tools"
            else:
                url += "compose/Tools/x86_64/os/"
            node.exec_command(
                sudo=True,
                cmd=f"yum-config-manager --add-repo {url}",
            )

    with ThreadPoolExecutor(max_workers=len(nodes)) as executor:
        futures = {executor.submit(_set_repo_on_node, node): node for node in nodes}
        for future in as_completed(futures):
            node = futures[future]
            try:
                future.result()
                log.debug(f"set_repo OK: {getattr(node, 'hostname', node)}")
            except Exception as e:
                log.warning(
                    f"set_repo failed on " f"{getattr(node, 'hostname', node)}: {e}"
                )


def _parallel_rpm_install(nodes, config, rpm_version=None):
    if not nodes:
        return
    ibm_build = config.get("ibm_build") or config.get("product") == "ibm"
    build_type = config.get("args", {}).get("release", config.get("build_type"))

    def _install_on_node(node):
        if ibm_build:
            setup_ibm_licence(node, build_type=build_type)
        node.exec_command(sudo=True, cmd="yum makecache", check_ec=False)
        upd_cmd = "yum update --nogpgcheck -y 'ceph*'"
        if rpm_version:
            upd_cmd = f"{upd_cmd}-{rpm_version}"
        node.exec_command(sudo=True, cmd=upd_cmd)
        node.exec_command(cmd="rpm -qa | grep ceph")

    with ThreadPoolExecutor(max_workers=len(nodes)) as executor:
        futures = {executor.submit(_install_on_node, node): node for node in nodes}
        for future in as_completed(futures):
            node = futures[future]
            try:
                future.result()
                log.debug(f"rpm_install OK: {getattr(node, 'hostname', node)}")
            except Exception as e:
                log.warning(
                    f"rpm_install failed on " f"{getattr(node, 'hostname', node)}: {e}"
                )


def _detect_registry_tier(registry):
    """Return credential tier (cdn or stage) based on registry hostname.

    Stage checks come first because cp.stg.icr.io contains 'cp.icr.io'.
    """
    if not registry:
        return "cdn"
    if "stage" in registry or "stg" in registry or "quay" in registry:
        return "stage"
    if "registry.redhat.io" in registry or "cp.icr.io" in registry:
        return "cdn"
    return "cdn"


def _detect_registry_vendor(config):
    """Return credential vendor key (ibm or rh) based on product config."""
    product = config.get("product", "redhat").lower()
    return "ibm" if "ibm" in product else "rh"


def _load_registry_credentials(registry, config):
    """Load registry credentials from ~/.cephci.yaml with fallback.

    Credential resolution order (mirrors bootstrap.py construct_registry):
      1. credentials.registry.<vendor>.<tier>  (structured nested path)
      2. <vendor>_registry_credentials          (legacy vendor-specific flat key)
      3. cdn_credentials                        (legacy catch-all)

    Returns:
        (username, password, resolved_registry_url) or raises RuntimeError.
    """
    cephci_cfg = get_cephci_config()
    vendor = _detect_registry_vendor(config)
    tier = _detect_registry_tier(registry)

    log.debug(
        "Registry credential lookup: registry=%r vendor=%r tier=%r",
        registry,
        vendor,
        tier,
    )

    cred = (
        cephci_cfg.get("credentials", {}).get("registry", {}).get(vendor, {}).get(tier)
    )

    if not cred:
        cred = cephci_cfg.get(
            f"{vendor}_registry_credentials",
            cephci_cfg.get("cdn_credentials"),
        )

    if not cred:
        raise RuntimeError(
            f"No registry credentials found for vendor={vendor} tier={tier}. "
            "Check ~/.cephci.yaml credentials.registry section."
        )

    username = cred.get("username")
    password = cred.get("password")
    if not username or not password:
        raise RuntimeError(
            f"Registry credentials for vendor={vendor} tier={tier} are "
            f"incomplete (username={bool(username)}, password={bool(password)}). "
            "Check ~/.cephci.yaml."
        )

    return (username, password, cred.get("registry", registry))


def _ensure_target_registry_auth(ceph_cluster, cluster_obj, config):
    """Ensure podman + cephadm credentials exist for the target registry.

    Detects the target registry from config['container_image'], determines
    the credential tier (cdn vs stage) using the same logic as bootstrap.py's
    _detect_registry_tier, loads credentials from ~/.cephci.yaml, and runs:
      - podman login <registry> on every non-client node
      - cephadm registry-login on every non-client node (cephadm uses its
        own authfile at /etc/ceph/podman-auth.json, separate from podman's
        default credential store)
    """
    container_image = config.get("container_image", "")
    if not container_image:
        log.debug("No container_image in config; skipping registry auth")
        return

    # Extract registry host from image URL (e.g., "registry.stage.redhat.io/...")
    registry = container_image.split("/")[0]
    if not registry:
        log.debug("Could not parse registry from container_image; skipping")
        return

    try:
        username, password, reg_url = _load_registry_credentials(registry, config)
    except RuntimeError as exc:
        log.warning(f"Registry credential load failed: {exc}")
        return

    log.info(f"Ensuring registry auth for {reg_url} on all nodes")

    non_client_nodes = ceph_cluster.get_nodes(ignore="client")

    def _auth_on_node(node):
        hostname = getattr(node, "hostname", node)
        try:
            node.exec_command(
                sudo=True,
                cmd=(
                    f"podman login --username {username}"
                    f" --password-stdin {reg_url}"
                    f" <<< '{password}'"
                ),
            )
            log.debug(f"podman login OK on {hostname}")
        except Exception as exc:
            log.warning(f"podman login failed on {hostname}: {exc}")

        try:
            cluster_obj.registry_login(
                node=node,
                args={
                    "registry-url": reg_url,
                    "registry-username": username,
                    "registry-password": password,
                },
            )
            log.debug(f"cephadm registry-login OK on {hostname}")
        except Exception as exc:
            log.warning(f"cephadm registry-login failed on {hostname}: {exc}")

    with ThreadPoolExecutor(max_workers=len(non_client_nodes)) as executor:
        futures = {
            executor.submit(_auth_on_node, node): node for node in non_client_nodes
        }
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                node = futures[future]
                log.warning(
                    f"registry auth failed on "
                    f"{getattr(node, 'hostname', node)}: {exc}"
                )


def _relax_container_signature_policy(nodes, registry="registry.redhat.io"):
    """Temporarily set insecureAcceptAnything for a registry in policy.json.

    On each node:
      1. Backs up /etc/containers/policy.json to policy.json.upgrade-bak
      2. Reads the JSON, adds/overwrites the transports.docker entry for the
         given registry to [{"type": "insecureAcceptAnything"}], writes it back

    This allows pulling images whose signatures have not yet propagated to
    Red Hat's sigstore (common with freshly published 'released' builds).
    """
    policy_path = "/etc/containers/policy.json"
    backup_path = f"{policy_path}.upgrade-bak"

    patch_script = (
        'python3 -c "'
        "import json, sys; "
        f"p = json.load(open('{policy_path}')); "
        "t = p.setdefault('transports', {}).setdefault('docker', {}); "
        f"t['{registry}'] = [{{'type': 'insecureAcceptAnything'}}]; "
        f"json.dump(p, open('{policy_path}', 'w'), indent=2)"
        '"'
    )

    def _relax_on_node(node):
        hostname = getattr(node, "hostname", node)
        try:
            node.exec_command(sudo=True, cmd=f"cp {policy_path} {backup_path}")
            node.exec_command(sudo=True, cmd=patch_script)
            log.info(f"Relaxed signature policy for {registry} on {hostname}")
        except Exception as exc:
            log.warning(f"Failed to relax signature policy on {hostname}: {exc}")

    with ThreadPoolExecutor(max_workers=len(nodes)) as executor:
        list(executor.map(_relax_on_node, nodes))


def _restore_container_signature_policy(nodes):
    """Restore the original /etc/containers/policy.json from backup."""
    policy_path = "/etc/containers/policy.json"
    backup_path = f"{policy_path}.upgrade-bak"

    def _restore_on_node(node):
        hostname = getattr(node, "hostname", node)
        try:
            node.exec_command(
                sudo=True,
                cmd=f"test -f {backup_path} && mv {backup_path} {policy_path}",
            )
            log.debug(f"Restored signature policy on {hostname}")
        except Exception as exc:
            log.warning(f"Failed to restore signature policy on {hostname}: {exc}")

    with ThreadPoolExecutor(max_workers=len(nodes)) as executor:
        list(executor.map(_restore_on_node, nodes))


def _prepare_upgrade_context(ceph_cluster, cephadm_obj, config):
    """Build upgrade context: resolve image, set repos, install RPMs.

    Mirrors the workflow from test_upgrade_warn.py:
      1. Resolve target version/release via CephTestManifest (or use custom image)
      2. Convert node IDs to hostnames (for staggered upgrades with hosts= arg)
      3. Remove old repos on all nodes
      4. Set new tool repos
      5. Install upgraded cephadm RPMs
      6. Run upgrade_check to verify readiness

    Supports staggered upgrades via args:
      - daemon_types: "mon,mgr"    -- upgrade only these daemon types
      - hosts: "node1,node2"       -- upgrade only daemons on these hosts
      - services: "mon,mgr"        -- upgrade only these services

    Updates ``config`` in place with container_image, registry, and repo info.
    Returns a fresh Orch object configured for the target version.
    """
    args = config.get("args", {})
    _rhcs_version = args.get("rhcs-version", None)
    _rhcs_release = args.get("release", None)
    _platform = args.get("platform", config.get("platform", "rhel-9"))
    _custom_image = args.get("custom_image", None)
    _custom_repo = args.get("custom_repo", None)
    product = config.get("product", "redhat")

    if "args" not in config:
        config["args"] = {}
    config["args"]["image"] = "latest"

    _rpm_version = None

    if _rhcs_version and _rhcs_release:
        curr_ver, _ = cephadm_obj.shell(
            args=["bash -c \"ceph version | awk '{print $3}'\""]
        )
        log.info(
            f"Resolving upgrade target: {product} {_rhcs_version}-{_rhcs_release} "
            f"({_platform}) -- current version: {curr_ver.strip()}"
        )
        ctm = CephTestManifest(
            product=product,
            release=_rhcs_version,
            build_type=_rhcs_release,
            platform=_platform,
        )
        _base_url = ctm.repository
        _registry = ctm.ceph_image_dtr
        _image_name = ctm.ceph_image_path
        _image_tag = ctm.ceph_image_tag
        _ver = ctm.ceph_version

        config["base_url"] = _base_url
        config["container_image"] = f"{_registry}/{_image_name}:{_image_tag}"
        config["ceph_docker_registry"] = _registry
        config["ceph_docker_image"] = _image_name
        config["ceph_docker_image_tag"] = _image_tag
        ceph_cluster.rhcs_version = _rhcs_version
        config["rhbuild"] = f"{_rhcs_version}-{_platform}"
        config["args"]["rhcs-version"] = _rhcs_version
        config["args"]["release"] = _rhcs_release
        config["args"]["image"] = config["container_image"]

        os_ver = _platform.split("-")[-1]
        _rpm_version = f"2:{_ver}.el{os_ver}cp"

        log.info("=" * 60)
        log.info("Manifest Resolution Results")
        log.info(
            f"  Manifest URL : {ctm.URI}{product}/{_rhcs_version}.yaml"
            f" -> [{_rhcs_release}]"
        )
        log.info(f"  Ceph version : {_ver}")
        log.info(f"  Container    : {config['container_image']}")
        log.info(f"  Registry     : {_registry}")
        log.info(f"  Image path   : {_image_name}")
        log.info(f"  Image tag    : {_image_tag}")
        log.info(f"  Repository   : {_base_url}")
        log.info(f"  RPM version  : {_rpm_version}")
        log.info("=" * 60)

    elif _custom_image and _custom_repo:
        log.info(f"Using custom image: {_custom_image}")
        # Parse image ref: host[:port]/path:tag
        img_ref, _image_tag = _custom_image.rsplit(":", 1)
        _registry, _image_name = img_ref.split("/", 1)

        config["base_url"] = _custom_repo
        config["container_image"] = f"{_registry}/{_image_name}:{_image_tag}"
        config["ceph_docker_registry"] = _registry
        config["ceph_docker_image"] = _image_name
        config["ceph_docker_image_tag"] = _image_tag
        config["args"]["image"] = config["container_image"]

        log.info(
            f"Custom image resolved: container={config['container_image']}, "
            f"repo={_custom_repo}"
        )

    elif config.get("container_image"):
        curr_ver, _ = cephadm_obj.shell(
            args=["bash -c \"ceph version | awk '{print $3}'\""]
        )
        curr_ver = curr_ver.strip()
        candidate = config["container_image"]
        log.info(
            f"Checking pre-configured container_image: {candidate} "
            f"(current cluster version: {curr_ver})"
        )
        try:
            from packaging.version import Version

            tag = candidate.rsplit(":", 1)[-1]
            # Extract version from tag before the build separator
            # e.g. "v8.1-20191" -> "8.1", "8.1" -> "8.1"
            tag_ver = tag.lstrip("v").split("-", 1)[0]
            curr_clean = curr_ver.split("-")[0]
            if tag_ver and Version(tag_ver) < Version(curr_clean):
                log.warning(
                    f"Inherited container_image {candidate} (tag version "
                    f"{tag_ver}) is OLDER than the running cluster "
                    f"({curr_clean}). This is likely a stale image from "
                    f"bootstrap/CLI -- ignoring it. Upgrade will auto-resolve "
                    f"to the latest available image."
                )
                config.pop("container_image", None)
            else:
                log.info(
                    f"Using pre-configured container_image: {candidate} "
                    f"(cluster={curr_ver})"
                )
                config["args"]["image"] = candidate
        except Exception as err:
            log.debug(f"Version comparison failed ({err}); using image as-is")
            log.info(f"Using pre-configured container_image (unchecked): {candidate}")
            config["args"]["image"] = candidate

    else:
        log.warning(
            "No rhcs-version/release, custom_image, or container_image specified. "
            "Upgrade will use whatever image the orchestrator resolves."
        )

    # Log selective upgrade parameters if provided
    if args.get("daemon_types"):
        log.info(f"Staggered upgrade: daemon_types={args['daemon_types']}")
    if args.get("hosts"):
        log.info(f"Staggered upgrade: target hosts={args['hosts']}")
    if args.get("services"):
        log.info(f"Staggered upgrade: services={args['services']}")

    # Convert node IDs to hostnames if hosts parameter is provided
    # (suite YAML uses node IDs like "node1", but ceph orch expects hostnames)
    if args.get("hosts"):
        node_ids = [nid.strip() for nid in args["hosts"].split(",")]
        hostnames = []
        for node_id in node_ids:
            node_obj = get_node_by_id(ceph_cluster, node_id)
            if node_obj:
                hostnames.append(node_obj.hostname)
                log.debug(f"Converted node ID '{node_id}' -> '{node_obj.hostname}'")
            else:
                log.warning(f"Could not resolve node ID '{node_id}', skipping")
        if hostnames:
            config["args"]["hosts"] = ",".join(hostnames)
            log.info(f"Resolved host list: {hostnames}")

    cluster_obj = Orch(cluster=ceph_cluster, **config)

    all_nodes = ceph_cluster.get_nodes()
    log.info(f"Removing old repos on {len(all_nodes)} nodes (parallel)")
    _parallel_remove_repos(all_nodes)

    if config.get("base_url"):
        log.info(f"Setting new tool repos on {len(all_nodes)} nodes (parallel)")
        _parallel_set_repo(all_nodes, config)
        time.sleep(5)

        log.info(
            f"Installing upgraded cephadm RPMs on " f"{len(all_nodes)} nodes (parallel)"
        )
        _parallel_rpm_install(all_nodes, config, rpm_version=_rpm_version)
        time.sleep(5)
    else:
        log.info("No base_url configured; skipping repo setup and RPM install")

    # Ensure target registry credentials are configured on all nodes
    if config.get("container_image"):
        _ensure_target_registry_auth(ceph_cluster, cluster_obj, config)

    # Auto-relax container signature policy for non-released builds (nightly,
    # testing, rc, etc.) whose images are on staging registries without
    # signed manifests.  Explicit YAML override takes precedence.
    upgrade_params = config.setdefault("upgrade_params", {})
    _build_type = args.get("release", "")
    if "relax_signature_policy" not in upgrade_params:
        should_relax = bool(_build_type and _build_type != "released")
        if should_relax:
            log.info(
                f"Auto-enabling relax_signature_policy for "
                f"non-released build type '{_build_type}'"
            )
        upgrade_params["relax_signature_policy"] = should_relax

    if upgrade_params.get("relax_signature_policy", False):
        target_registry = config.get("container_image", "").split("/")[0]
        _relax_container_signature_policy(
            ceph_cluster.get_nodes(), registry=target_registry or "registry.redhat.io"
        )

    target_image = config.get("container_image")
    if target_image:
        log.info(f"Running upgrade check against {target_image}")
        for attempt in range(3):
            try:
                result = cluster_obj.upgrade_check(image=target_image)
                log.info(f"upgrade_check passed: {result}")
                target_ver = result.get("target_version", "")
                if target_ver:
                    config["_target_ceph_version"] = target_ver
                    log.info(f"Target Ceph version from upgrade_check: {target_ver}")
                needs_update = result.get("needs_update", {})
                up_to_date = result.get("up_to_date", [])
                log.info(
                    f"upgrade_check: {len(up_to_date)} up-to-date, "
                    f"{len(needs_update)} need update"
                )
                break
            except Exception as err:
                log.warning(f"upgrade_check attempt {attempt + 1}/3 failed: {err}")
                if attempt < 2:
                    time.sleep(15)
                else:
                    raise Exception(
                        f"upgrade_check failed after 3 attempts for "
                        f"{target_image}: {err}"
                    )
    else:
        log.info(
            "Skipping upgrade_check (no explicit target image; "
            "framework/CI will resolve at upgrade start)"
        )

    return cluster_obj


def _start_upgrade(cluster_obj, config):
    """Initiate the ceph orch upgrade via the prepared Orch object.

    Passes through staggered upgrade args (daemon_types, hosts, services)
    from the user config to ``ceph orch upgrade start``. When these are
    absent, all daemons on all hosts are upgraded (default behavior).

    The ``image`` key is only included when an explicit container_image
    is available.  Without it, ``ceph orch upgrade start`` uses whatever
    image the orchestrator resolves from the configured repository (the
    standard CI auto-resolve behavior).

    Args:
        cluster_obj: Orch object returned by ``_prepare_upgrade_context``,
            already configured with the target image, repos, and RPMs.
        config: Test config with optional ``container_image`` and
            ``args.daemon_types``, ``args.hosts``, ``args.services``.
    """
    args = config.get("args", {})
    upgrade_args = {}

    # Only include --image when we have an explicit target; without it,
    # ceph orch upgrade start auto-resolves from the configured repo.
    if config.get("container_image"):
        upgrade_args["image"] = config["container_image"]

    # Forward staggered upgrade parameters to ceph orch upgrade start
    for key in ("daemon_types", "hosts", "services"):
        if args.get(key):
            upgrade_args[key] = args[key]

    upgrade_config = {
        "args": upgrade_args,
    }

    # Always pass product/release for IBM license handling inside start_upgrade.
    # product is set by run.py framework; release comes from the upgrade target.
    upgrade_config["product"] = config.get("product", "redhat")
    if args.get("rhcs-version"):
        upgrade_config["release"] = args["rhcs-version"]
    elif config.get("rhbuild"):
        upgrade_config["release"] = config["rhbuild"].split("-")[0]

    # For IBM builds during staggered upgrades, config["rhbuild"] reflects the
    # *source* version (e.g. "9.0"), but the active MGR may already be at 9.1+
    # after an earlier phase.  upgrade.py gates --automatically-accept-license
    # behind release >= "9.1", so we detect the running MGR version and override
    # release accordingly.
    if upgrade_config.get("product") == "ibm":
        try:
            from packaging.version import Version

            mgr_common, mgr_downstream = get_daemon_versions(
                node=cluster_obj, daemon_type="mgr"
            )
            mgr_is_91_plus = (
                mgr_common and Version(mgr_common) >= Version("20.2.1")
            ) or (mgr_downstream and Version(mgr_downstream) >= Version("9.9.1.0"))
            if mgr_is_91_plus:
                log.info(
                    "Active MGR is 9.1+ (common=%s, downstream=%s); "
                    "setting release=9.1 for license flag",
                    mgr_common,
                    mgr_downstream,
                )
                upgrade_config["release"] = "9.1"
        except Exception as e:
            log.warning(f"Could not detect MGR version for license override: {e}")

    log.info(
        f"Starting upgrade: image={upgrade_args.get('image', '<auto-resolve>')}, "
        f"daemon_types={upgrade_args.get('daemon_types', 'all')}"
    )
    log.info(f"Calling ceph orch upgrade start with args: {upgrade_args}")

    out = cluster_obj.start_upgrade(upgrade_config)
    stdout = str(out[0]).strip() if isinstance(out, tuple) and out[0] else ""

    if "Initiating" in stdout:
        log.info(f"Upgrade initiated: {stdout[-200:]}")
        return

    log.warning(
        f"Upgrade start response did not contain 'Initiating upgrade'. "
        f"Response (last 300 chars): {stdout[-300:]}"
    )

    time.sleep(5)
    try:
        status = cluster_obj.upgrade_status()
    except Exception as e:
        log.warning(f"upgrade_status() check failed: {e}")
        status = {}

    if status.get("in_progress", False):
        log.info(
            "Upgrade IS in progress despite missing 'Initiating' in response. "
            "First attempt succeeded silently."
        )
        return

    log.info("Upgrade not in progress. Retrying upgrade start in 10s...")
    time.sleep(10)
    try:
        out = cluster_obj.start_upgrade(upgrade_config)
        stdout = str(out[0]).strip() if isinstance(out, tuple) and out[0] else ""
    except CommandFailed as e:
        stdout = str(e)

    if "Initiating" in stdout:
        log.info(f"Upgrade initiated on retry: {stdout[-200:]}")
    elif "already in progress" in stdout.lower() or "cannot set" in stdout.lower():
        log.info("Upgrade already in progress -- first attempt succeeded.")
    else:
        log.error(
            "Upgrade start failed on retry. "
            "The upgrade may not have been initiated. "
            f"Response: {stdout[:300]}"
        )


def _handle_post_upgrade_license(ceph_cluster, cephadm_obj, config):
    """Accept IBM Storage Ceph license if required (9.1+).

    Resolves the target version from (in order): args.rhcs-version,
    rhbuild (set by _prepare_upgrade_context), or live MGR daemon version
    check. The MGR version check is the most reliable fallback for Hop 2
    auto-resolve scenarios where no explicit version is specified.

    Args:
        ceph_cluster: Cluster object.
        cephadm_obj: CephAdmin node for license command.
        config: Test config with product and container_image.
    """
    product = config.get("product", "redhat")

    if product != "ibm":
        return

    needs_license = False
    release_str = str(config.get("args", {}).get("rhcs-version", ""))
    if not release_str:
        rhbuild = config.get("rhbuild", "")
        if rhbuild:
            release_str = rhbuild.split("-")[0]

    if release_str and LooseVersion(release_str) >= LooseVersion("9.1"):
        needs_license = True
    else:
        # Fallback: extract MGR version directly from ceph versions output
        # to avoid framework's extract_version() warnings on tentacle format
        # (e.g., "ceph version 20.1.0-221.el9cp ... tentacle").
        try:
            ver_raw, _ = cephadm_obj.shell(args=["ceph versions --format json"])
            versions = json.loads(ver_raw.strip()) if ver_raw else {}
            mgr_versions = versions.get("mgr", {})
            for ver_str in mgr_versions:
                mgr_ver_common = _extract_version_number(ver_str)
                if mgr_ver_common:
                    from packaging.version import Version

                    if Version(mgr_ver_common) >= Version("20.2.0"):
                        needs_license = True
                        log.info(
                            f"MGR version check indicates 9.1+ "
                            f"(common={mgr_ver_common})"
                        )
                    break
        except Exception as e:
            log.warning(f"Could not determine MGR version for license check: {e}")

    if needs_license:
        img = config.get("container_image")
        if not img:
            try:
                img_out, _ = cephadm_obj.shell(
                    args=["ceph config get mgr container_image"]
                )
                img = img_out.strip() if img_out else ""
            except Exception as e:
                log.debug(f"container_image query from mgr config failed: {e}")
        if not img:
            log.warning("Cannot accept IBM license: no container_image resolvable")
            return
        log.info(f"Accepting IBM license for image: {img}")
        mgr_accept_license(node=cephadm_obj, img=img)


def _get_version_snapshot(rados_obj):
    """Return ``ceph versions`` output as a dict, without the ``overall`` key.

    The returned dict maps daemon type to {version_string: count}, e.g.::

        {"mon": {"ceph version 20.1.0-221.el9cp (...) tentacle (stable)": 3},
         "osd": {"ceph version 20.1.0-221.el9cp (...) tentacle (stable)": 35}}

    The version strings use the same format as ``target_version`` from
    ``ceph orch upgrade check --image``, so they are directly comparable.

    Returns ``None`` on failure.
    """
    try:
        raw = rados_obj.run_ceph_command(cmd="ceph versions")
    except Exception as err:
        log.warning(f"Could not get daemon versions: {err}")
        return None
    if not isinstance(raw, dict):
        return None
    raw.pop("overall", None)
    return raw


def _normalize_ver(v):
    """Strip build-type suffixes and downstream release tags for comparison.

    IBM debug builds report ``(stable - RelWithDebInfo)`` in ``ceph versions``
    while ``ceph orch upgrade check`` returns just ``(stable)``.  Downstream
    9.1+ images also append ``release X.Y.Z.W`` after the stability tag.
    This normalizes both forms so they can be compared reliably.
    """
    v = re.sub(r"\((\w+)\s*-\s*[^)]+\)", r"(\1)", v)
    v = re.sub(r"\s+release\s+[\d.]+\s*$", "", v)
    return v


def _verify_upgrade_not_noop(rados_obj, pre_versions, config=None):
    """Confirm the upgrade actually changed daemon versions.

    Two checks, in order:

    1. **Target match** -- ``_target_ceph_version`` (from ``ceph orch upgrade
       check --image``) gives the expected version string.  Uses normalized
       comparison to handle build-type suffixes (e.g. ``RelWithDebInfo``).

    2. **Before/after diff** -- if no target is available, compare the
       ``ceph versions`` snapshot taken before the upgrade with the current
       one. If they are identical, the upgrade was a no-op.

    Raises ``RuntimeError`` if the upgrade did not happen.
    """
    config = config or {}
    target_ver = config.get("_target_ceph_version", "")

    post = _get_version_snapshot(rados_obj)
    if post is None:
        log.warning("Could not collect post-upgrade versions; skipping check")
        return

    total = sum(c for vd in post.values() for c in vd.values())
    if total == 0:
        log.warning("ceph versions returned no daemons; skipping check")
        return

    # --- Check 1: do daemons match the target from upgrade check? ---
    if target_ver:
        norm_target = _normalize_ver(target_ver)
        on_target = sum(
            c
            for vd in post.values()
            for v, c in vd.items()
            if _normalize_ver(v) == norm_target
        )
        log.info(
            f"No-op check: {on_target}/{total} daemons on " f"target '{target_ver}'"
        )

        if on_target == 0:
            versions = {v: c for vd in post.values() for v, c in vd.items()}
            raise RuntimeError(
                f"Upgrade completed but no daemons match target version "
                f"'{target_ver}'. Current: {versions}. "
                f"The orchestrator likely aborted silently."
            )
        if on_target < total:
            log.warning(f"Partial upgrade: {on_target}/{total} on target")
        if pre_versions is not None and pre_versions == post:
            raise RuntimeError(
                f"Upgrade no-op: all {total} daemons were already at target "
                f"'{target_ver}' BEFORE the upgrade started. "
                f"Check that the correct target image/manifest was used."
            )
        return

    # --- Check 2: before/after comparison ---
    if pre_versions is None:
        log.info("No target version and no pre-upgrade snapshot; skipping")
        return

    if pre_versions == post:
        raise RuntimeError(
            f"Upgrade completed but daemon versions unchanged. "
            f"Versions: {post}. Orchestrator likely aborted silently."
        )

    log.info(
        f"Versions changed: pre={set().union(*pre_versions.values())} "
        f"-> post={set().union(*post.values())}"
    )


def _monitor_upgrade(
    orch_obj,
    rados_obj,
    stats,
    config,
    pre_upgrade_versions=None,
    health_tracker=None,
    skip_noop_check=False,
    abort_info=None,
):
    """Phase 4: Monitor upgrade progress with abort logic.

    Returns True if upgrade was aborted, False if completed normally.
    """
    timing = config.get("phase_timing") or {}
    timeout = timing["upgrade_timeout_sec"]
    stall_threshold = timing["upgrade_stall_threshold_sec"]
    max_retries = timing["max_pause_retries"]
    auto_resume = timing.get("auto_resume_on_pause", False)

    deadline = time.time() + timeout
    last_progress_time = time.time()
    last_status_msg = ""
    last_progress_str = ""
    pause_retries = 0
    was_paused = False

    def _abort(reason):
        log.error(reason)
        if abort_info is not None:
            abort_info["reason"] = reason
        _abort_upgrade(orch_obj)

    if health_tracker is None:
        health_tracker = HealthWarningTracker()
    last_health_poll = time.time()
    seen_in_progress = False
    startup_checked = False

    while time.time() < deadline:
        try:
            status = orch_obj.upgrade_status()
            stats.record_upgrade_status(status)

            now = time.time()
            if now - last_health_poll >= 15:
                last_health_poll = now
                try:
                    health = rados_obj.run_ceph_command(
                        cmd="ceph health detail", timeout=30
                    )
                    cluster_ts = get_cluster_timestamp(rados_obj.node)
                    health_tracker.record_snapshot(cluster_ts, health)
                except Exception:
                    pass

                try:
                    stats.io_stats_snapshot()
                except Exception:
                    pass

                try:
                    stats.orch_versions_snapshot()
                except Exception:
                    pass

            if status.get("in_progress", False):
                seen_in_progress = True
            else:
                if not seen_in_progress and not startup_checked:
                    startup_checked = True
                    log.warning(
                        "Upgrade status shows not-in-progress on "
                        "first poll -- upgrade may not have started."
                        " Re-checking in 15s..."
                    )
                    time.sleep(15)
                    try:
                        status = orch_obj.upgrade_status()
                    except Exception as e:
                        log.warning(f"Re-check upgrade_status() failed: {e}")
                        continue
                    if status.get("in_progress", False):
                        seen_in_progress = True
                        log.info("Upgrade now in progress after re-check.")
                        continue
                    _abort(
                        "Upgrade never started: 'in_progress' was "
                        "never true. The upgrade start command may "
                        "have failed silently."
                    )
                    return True
                log.info("Upgrade completed")
                if not skip_noop_check:
                    _verify_upgrade_not_noop(rados_obj, pre_upgrade_versions, config)
                return False

            current_msg = status.get("message", "")
            current_progress = status.get("progress", "")
            if current_msg != last_status_msg or current_progress != last_progress_str:
                log.info(f"Upgrade status: {current_msg} ({current_progress})")
                last_status_msg = current_msg
                last_progress_str = current_progress
                last_progress_time = time.time()

            # --- Pause handling ---
            is_paused = status.get("is_paused", status.get("paused", False))
            if is_paused:
                if not was_paused:
                    pause_retries += 1
                    error_code, subcause = classify_upgrade_error(current_msg)
                    log.warning(
                        f"Upgrade paused (episode {pause_retries}/{max_retries})"
                        f" [{error_code}/{subcause}]: {current_msg[:200]}"
                    )

                if not auto_resume:
                    skip_monitoring = timing.get(
                        "skip_monitoring_upgrade_failures", False
                    )
                    _MONITORING_KEYWORDS = (
                        "grafana",
                        "prometheus",
                        "alertmanager",
                        "node-exporter",
                    )
                    if skip_monitoring and any(
                        kw in current_msg.lower() for kw in _MONITORING_KEYWORDS
                    ):
                        log.warning(
                            f"Monitoring daemon blocked upgrade "
                            f"(skip_monitoring=true, auto_resume=false): "
                            f"{current_msg[:200]}. Issuing upgrade stop."
                        )
                        config["_monitoring_upgrade_skipped"] = True
                        _abort(f"MONITORING_SKIP: {current_msg[:200]}")
                        return True
                    _abort(
                        f"Upgrade paused (auto_resume_on_pause=false). "
                        f"Reason: {current_msg}"
                    )
                    return True

                if pause_retries > max_retries:
                    _abort("Max pause episodes exhausted")
                    return True

                try:
                    orch_obj.shell(args=["ceph", "orch", "upgrade", "resume"])
                except Exception as e:
                    log.warning(f"Resume failed: {e}")
            else:
                if was_paused:
                    log.info("Upgrade resumed successfully")
                    last_progress_time = time.time()
            was_paused = is_paused

            # --- Stall detection ---
            if not is_paused:
                stall_duration = time.time() - last_progress_time
                if stall_duration > stall_threshold:
                    _abort(
                        f"Upgrade stalled for {stall_duration:.0f}s "
                        f"(threshold: {stall_threshold}s)"
                    )
                    return True

        except RuntimeError:
            raise
        except Exception as e:
            err_str = str(e)
            log.debug(f"Upgrade status poll error (may be transient): {e}")
            if "Module not found" in err_str:
                error_code, subcause = classify_upgrade_error(err_str)
                log.warning(
                    f"Orch module missing [{error_code}/{subcause}], "
                    f"attempting mgr failover"
                )
                try:
                    rados_obj.node.shell(["ceph mgr fail"], timeout=30)
                except Exception:
                    pass

        time.sleep(3)

    _abort(f"Upgrade timed out after {timeout}s")
    return True


def _abort_upgrade(orch_obj):
    """Stop the in-progress upgrade."""
    try:
        orch_obj.shell(args=["ceph", "orch", "upgrade", "stop"])
        log.info("Upgrade stop command issued")
    except Exception as e:
        log.warning(f"Upgrade stop failed: {e}")


def _extract_daemon_lifecycle_events(
    ceph_cluster,
    daemon_entries: dict,
    fsid: str = "",
) -> dict:
    """Parse daemon-specific log files for timestamped lifecycle events.

    For each redeployed daemon on each host, greps that daemon's log file
    (exact path under /var/log/ceph/{fsid}/) with dtype-specific patterns
    and per-daemon redeploy_time ±5m windows. Up to
    ``LOGFILE_PARALLEL_PER_HOST`` parallel SSH commands per host; per-daemon
    ``cephadm logs`` fallback on failure.

    Args:
        ceph_cluster: Cluster object with get_nodes().
        daemon_entries: Dict mapping daemon_type -> list of redeploy entry
            dicts (each with 'host', 'redeploy_time', etc.).
        fsid: Cluster FSID for log paths; empty skips logfile scrape.

    Returns:
        Dict mapping daemon_type -> list of lifecycle event dicts, each with
        'event' and 'timestamp' keys, sorted by timestamp. Returns empty
        dict for types where no events were found.
    """
    # Determine which daemon types need logs from which nodes
    node_dtypes: dict[str, set[str]] = {}
    for dtype, entries in daemon_entries.items():
        if dtype not in DAEMON_LOG_PATTERNS:
            continue
        for entry in entries:
            host = entry.get("host", "")
            if host:
                node_dtypes.setdefault(host, set()).add(dtype)

    if not node_dtypes:
        log.info("No daemon types with hosts for lifecycle extraction")
        return {}

    # Per-type time windows (first redeploy - 5 min to last redeploy + 5 min)
    type_windows: dict[str, tuple[datetime, datetime]] = {}
    for dtype, entries in daemon_entries.items():
        times = []
        for e in entries:
            try:
                dt = datetime.fromisoformat(e["redeploy_time"])
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                times.append(dt)
            except (ValueError, TypeError):
                continue
        if times:
            buf = timedelta(minutes=5)
            type_windows[dtype] = (min(times) - buf, max(times) + buf)

    host_index = _host_deploy_name_index(daemon_entries)
    normalize_warned: set[tuple[str, str]] = set()

    nodes = ceph_cluster.get_nodes()
    node_map = {}
    for n in nodes:
        sn = getattr(n, "shortname", "")
        hn = getattr(n, "hostname", str(n))
        if sn:
            node_map[sn] = n
        if hn and hn != sn:
            node_map[hn] = n

    unresolved = set(node_dtypes.keys()) - set(node_map.keys())
    if unresolved:
        log.warning(
            f"Lifecycle extraction: {len(unresolved)} hosts have no matching "
            f"node object: {sorted(unresolved)[:5]}"
        )

    # Raw events collected per daemon type from all nodes
    raw_events: dict[str, list[dict]] = {}

    def _collect_lifecycle_from_node(hostname):
        """Scrape lifecycle events from redeployed daemon logfiles on one host.

        Only handles ``logfile`` source types; ``cephadm_logs`` types are
        collected separately via ``_collect_lifecycle_via_cephadm``.
        """
        node = node_map.get(hostname)
        if not node:
            return {}
        dtypes = node_dtypes.get(hostname, set())
        if not dtypes:
            return {}

        logfile_dtypes = {
            dt
            for dt in dtypes
            if DAEMON_LOG_PATTERNS.get(dt, {}).get("source", "logfile") == "logfile"
        }
        if not logfile_dtypes:
            return {}

        def _merge_result(
            target: dict[str, list[dict]], result: dict[str, list[dict]]
        ) -> None:
            for rtype, evts in result.items():
                target.setdefault(rtype, []).extend(evts)

        if not fsid:
            log.warning(f"Lifecycle logfile scrape skipped on {hostname}: no FSID")
            node_events: dict[str, list[dict]] = {}
            fallback = 0
            for dt in logfile_dtypes:
                for entry in daemon_entries.get(dt, []):
                    if entry.get("host") != hostname:
                        continue
                    _merge_result(
                        node_events, _collect_lifecycle_via_cephadm(dt, entry)
                    )
                    fallback += 1
            event_count = sum(len(v) for v in node_events.values())
            log.info(
                f"Lifecycle extraction from {hostname}: "
                f"scraped=0 fallback={fallback} events={event_count}"
            )
            return node_events

        work: list[tuple] = []
        seen_paths: set[str] = set()
        for dt in logfile_dtypes:
            type_window = type_windows.get(dt)
            if not type_window:
                continue
            grep_pat = _grep_pattern_for_dtype(dt)
            if not grep_pat:
                continue
            for entry in daemon_entries.get(dt, []):
                if entry.get("host") != hostname:
                    continue
                name = entry.get("name", "")
                if not name:
                    continue
                entry_window = _entry_lifecycle_window(entry, type_window)
                if not entry_window:
                    continue
                log_path = _daemon_logfile_path(fsid, dt, name)
                if log_path in seen_paths:
                    continue
                seen_paths.add(log_path)
                since_str = entry_window[0].strftime("%Y-%m-%dT%H:%M")
                until_str = (entry_window[1] + timedelta(minutes=1)).strftime(
                    "%Y-%m-%dT%H:%M"
                )
                work.append(
                    (dt, entry, log_path, since_str, until_str, grep_pat, entry_window)
                )

        if not work:
            return {}

        node_events = {}
        scraped = 0
        fallback = 0

        def _scrape_one(item):
            dt, entry, log_path, since_str, until_str, grep_pat, window = item
            dname = entry.get("name", "")
            log_events: list[dict] = []
            cmd = _build_logfile_scrape_cmd(log_path, since_str, until_str, grep_pat)
            scrape_timeout = (
                LOGFILE_SSH_TIMEOUT_LARGE_SEC
                if dt in LOGFILE_SSH_TIMEOUT_LARGE_DTYPES
                else LOGFILE_SSH_TIMEOUT_SEC
            )
            try:
                out, _ = node.exec_command(
                    sudo=True,
                    cmd=cmd,
                    timeout=scrape_timeout,
                )
                if out and out.strip():
                    log_events = _parse_lifecycle_lines(
                        out,
                        dt,
                        hostname,
                        dname,
                        window,
                        host_index,
                        normalize_warned,
                    )
            except Exception as exc:
                tag = remote_exec_failure_tag(exc, scrape_timeout)
                log.warning(
                    f"Lifecycle log scrape failed for {dname} on {hostname} "
                    f"({tag}); will try cephadm logs"
                )

            cephadm_result = _collect_lifecycle_via_cephadm(dt, entry)
            cephadm_events = cephadm_result.get(dt, []) if cephadm_result else []
            merged = _merge_daemon_lifecycle_events(log_events, cephadm_events)
            for evt in merged:
                evt["daemon_name"] = dname
                evt["host"] = hostname

            if not merged:
                log.warning(
                    f"Lifecycle empty for {dname} on {hostname} "
                    f"(log={len(log_events)}, cephadm={len(cephadm_events)})"
                )
                return {}, True

            used_fallback = not log_events and bool(cephadm_events)
            return {dt: merged}, used_fallback

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=LOGFILE_PARALLEL_PER_HOST
        ) as inner_pool:
            futures = [inner_pool.submit(_scrape_one, w) for w in work]
            for fut in concurrent.futures.as_completed(futures):
                try:
                    result, used_fallback = fut.result()
                    if used_fallback:
                        fallback += 1
                    else:
                        scraped += 1
                    _merge_result(node_events, result)
                except Exception as exc:
                    log.warning(f"Lifecycle scrape worker failed on {hostname}: {exc}")

        event_count = sum(len(v) for v in node_events.values())
        log.info(
            f"Lifecycle extraction from {hostname}: "
            f"scraped={scraped} fallback={fallback} events={event_count}"
        )
        return node_events

    def _collect_lifecycle_via_cephadm(dtype, entry):
        """Collect lifecycle events for a single container-based daemon.

        Uses ``cephadm logs --name <daemon>`` with journalctl time
        filtering, mirroring the pattern from ``core_workflows.py``.
        """
        hostname = entry.get("host", "")
        daemon_name = entry.get("name", "")
        node = node_map.get(hostname)
        if not node or not daemon_name:
            return {}

        if dtype not in DAEMON_LOG_PATTERNS:
            return {}

        window = _entry_lifecycle_window(entry, type_windows.get(dtype))
        if not window:
            return {}

        since_str = window[0].strftime("%Y-%m-%d %H:%M:%S")
        until_str = window[1].strftime("%Y-%m-%d %H:%M:%S")

        grep_pattern = _grep_pattern_for_dtype(dtype).replace("'", "'\\''")
        if not grep_pattern:
            return {}

        # NFS: omit tail — SIGHUP/export-reload noise fills the last N lines
        # and drops early Starting/IN GRACE events (cycle_count=0 / report N/A).
        tail = "" if dtype == "nfs" else " | tail -100"
        cmd = (
            f"cephadm logs --name {daemon_name} -- "
            f"--output=short-iso --utc "
            f'--since "{since_str}" '
            f'--until "{until_str}" '
            f"--no-pager 2>/dev/null | "
            f"grep -iE '{grep_pattern}'{tail} || true"
        )

        try:
            out, _ = node.exec_command(sudo=True, cmd=cmd, timeout=120, check_ec=False)
        except Exception as e:
            log.warning(f"cephadm logs collection for {daemon_name} on {hostname}: {e}")
            return {}

        if not out or not out.strip():
            return {}

        parsed = _parse_lifecycle_lines(
            out,
            dtype,
            hostname,
            daemon_name,
            window,
            host_index,
            normalize_warned,
        )
        if parsed:
            return {dtype: parsed}
        return {}

    # Identify cephadm_logs daemon instances to collect
    cephadm_tasks = []
    for dtype, entries in daemon_entries.items():
        pat = DAEMON_LOG_PATTERNS.get(dtype)
        if not pat or pat.get("source", "logfile") != "cephadm_logs":
            continue
        for entry in entries:
            cephadm_tasks.append((dtype, entry))

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        # Logfile-based collection (one task per node)
        futures = {
            pool.submit(_collect_lifecycle_from_node, hn): hn for hn in node_dtypes
        }
        # cephadm logs collection (one task per daemon instance)
        for dtype, entry in cephadm_tasks:
            tag = f"cephadm:{entry.get('name', dtype)}"
            futures[pool.submit(_collect_lifecycle_via_cephadm, dtype, entry)] = tag

        for future in concurrent.futures.as_completed(futures):
            label = futures[future]
            try:
                result = future.result()
                for dtype, events in result.items():
                    raw_events.setdefault(dtype, []).extend(events)
            except Exception as e:
                log.warning(f"Lifecycle collection thread failed for {label}: {e}")

    # Deduplicate and sort per daemon type.
    # Group by (daemon_name, event) so each daemon instance keeps its own
    # first+last occurrences instead of collapsing across all instances.
    final: dict[str, list[dict]] = {}
    for dtype, events in raw_events.items():
        events.sort(key=lambda e: e["timestamp"])
        by_daemon_event: dict[tuple[str, str], list[dict]] = {}
        for ev in events:
            dname = _normalize_log_daemon_name(
                ev.get("daemon_name", ""),
                ev.get("host", ""),
                host_index,
                normalize_warned,
            )
            key = (dname, ev["event"])
            by_daemon_event.setdefault(key, []).append(ev)

        deduped = []
        for (_dname, _ename), occurrences in by_daemon_event.items():
            # Match _merge_daemon_lifecycle_events: keep every grace cycle.
            if _ename in _GRACE_LIFECYCLE_EVENTS:
                deduped.extend(occurrences)
            else:
                deduped.append(occurrences[0])
                if len(occurrences) > 1 and occurrences[-1] != occurrences[0]:
                    deduped.append(occurrences[-1])

        deduped.sort(key=lambda e: e["timestamp"])
        final[dtype] = []
        for e in deduped:
            item = {
                "event": e["event"],
                "timestamp": e["timestamp"],
                "daemon_name": _normalize_log_daemon_name(
                    e.get("daemon_name", ""),
                    e.get("host", ""),
                    host_index,
                    normalize_warned,
                ),
            }
            if e.get("configured_sec") is not None:
                item["configured_sec"] = e["configured_sec"]
            final[dtype].append(item)

    if final:
        total = sum(len(v) for v in final.values())
        log.info(
            f"Extracted {total} lifecycle events from daemon logs "
            f"for {len(final)} daemon types"
        )
        for dtype, events in final.items():
            dnames = {e.get("daemon_name") for e in events if e.get("daemon_name")}
            log.info(
                f"Lifecycle summary {dtype}: {len(events)} events, "
                f"{len(dnames)} daemons with log data"
            )
            if dtype == "nfs":
                counts = Counter(e.get("event") for e in events)
                log.info(
                    "Lifecycle nfs event counts: "
                    f"grace_start={counts.get('grace_start', 0)} "
                    f"grace_end={counts.get('grace_end', 0)} "
                    f"started={counts.get('started', 0)} "
                    f"shutdown={counts.get('shutdown', 0)} "
                    f"export_reload={counts.get('export_reload', 0)}"
                )
    else:
        log.warning(
            "No lifecycle events extracted from any node -- "
            "daemon-log events will be empty in the report"
        )
    return final


_CEPHADM_LOG_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+\+\d{4})\s+"
    r"(mgr\.\S+)\s+\(mgr\.\d+\)\s+\d+\s+:\s+cephadm\s+\[INF\]\s+(.*)"
)
_DEPLOYING_RE = re.compile(r"Deploying daemon (\S+) on (\S+)")
_UPDATING_BATCH_RE = re.compile(r"Upgrade: Updating \[\['(.+?)'\]\]")
_UPDATING_SINGLE_RE = re.compile(r"Upgrade: Updating (\S+) \((\d+)/(\d+)\)")
# Tentacle: type-complete is INF "Setting container_image for all <type>"
# (legacy "daemons are up to date" / "Starting with" never logged on 20.2).
_SET_IMAGE_RE = re.compile(r"Upgrade: Setting container_image for all (\S+)")
_UPGRADE_COMPLETE_RE = re.compile(r"Upgrade: Complete!")
_UPGRADE_START_RE = re.compile(r"Upgrade: Started with target (.+)")
_UPGRADE_TARGET_RE = re.compile(r"Upgrade: Target is version (.+)")
_KEYRING_ROTATE_RE = re.compile(r"Rotating keyring for (\S+)")
_KEYRING_REDEPLOY_RE = re.compile(r"Redeploying (\S+) with new keyring")
_OSD_FLAG_SET_RE = re.compile(r"Setting OSD flag (\S+)")
_OSD_FLAG_UNSET_RE = re.compile(r"Unsetting OSD flag (\S+)")
_FS_JOINABLE_RE = re.compile(r"Setting filesystem (\S+) Joinable")
_MDS_COMPLETE_RE = re.compile(r"All MDS daemons upgraded to (.+)")
_OSD_SAFE_RE = re.compile(r"(osd\.\d+) is safe to restart")
_OSD_UNSAFE_RE = re.compile(r"unsafe to stop osd\(s\) at this time")
_SELF_UPGRADE_RE = re.compile(r"Upgrade: Need to upgrade myself \((\S+)\)")
_KEY_ROTATION_DELAY_RE = re.compile(
    r"Delaying rotation of keyring for (\S+), not ok-to-stop"
)
_KEY_ROTATION_COMPLETE_RE = re.compile(r"OSD/mds daemon key rotation completed")
_KEY_ROTATION_CHECK_RE = re.compile(
    r"All osd/mds daemons upgraded, checking for keys needing rotation"
)

_UPGRADE_ORDER = [
    "mgr",
    "mon",
    "crash",
    "osd",
    "mds",
    "rgw",
    "rbd-mirror",
    "cephfs-mirror",
    "ceph-exporter",
    "iscsi",
    "nfs",
    "nvmeof",
    "smb",
    "node-exporter",
    "prometheus",
    "alertmanager",
    "grafana",
    "loki",
    "promtail",
    "alloy",
    "mgmt-gateway",
    "oauth2-proxy",
]

_IO_IMPACT_DESC = {
    "mgr": "No visible IO impact - standby MGR takes over seamlessly",
    "mon": "Minimal IO impact - MON quorum maintained throughout",
    "crash": "No IO impact - crash collectors are passive",
    "osd": "Brief IO pauses as OSDs restart node-by-node; PGs temporarily degraded",
    "mds": "CephFS IO blackout during MDS failover chain (~20-90s)",
    "rgw": "S3/Swift API briefly unavailable (~10-15s per daemon)",
    "rbd-mirror": "No client IO impact - mirroring pauses briefly",
    "cephfs-mirror": "No client IO impact - mirror sync pauses briefly",
    "ceph-exporter": "No IO impact - metric export only",
    "iscsi": "iSCSI initiator IO disruption during gateway restart",
    "nfs": "NFS client IO disruption during Ganesha restart + grace period",
    "nvmeof": "NVMe-oF gateway IO disruption during gateway restart",
    "smb": "SMB share IO disruption during Samba daemon restart",
    "node-exporter": "No IO impact - host metric export only",
    "prometheus": "No IO impact - monitoring only",
    "alertmanager": "No IO impact - alerting only",
    "grafana": "No IO impact - dashboard only",
    "loki": "No IO impact - log aggregation only",
    "promtail": "No IO impact - log shipping only",
    "alloy": "No IO impact - telemetry collector only",
    "mgmt-gateway": "No IO impact - management API proxy only",
    "oauth2-proxy": "No IO impact - auth proxy only",
}


def _host_from_daemon_name(daemon_name: str, valid_hosts: set) -> str:
    """Extract hostname from daemon name for daemons no longer in orch ps.

    Handles naming patterns: mon.<host>, mgr.<host>.X, crash.<host>,
    mds.<fs>.<host>.X, rgw.<svc>.<host>.X, nfs.<svc>.<p>.<ns>.<host>.X,
    alertmanager.<host>, grafana.<host>, prometheus.<host>,
    ceph-exporter.<host>, node-exporter.<host>.
    """
    parts = daemon_name.split(".")
    dtype = parts[0]
    candidate = ""
    if (
        dtype
        in (
            "mon",
            "crash",
            "mgr",
            "alertmanager",
            "grafana",
            "prometheus",
            "loki",
            "promtail",
        )
        and len(parts) >= 2
    ):
        candidate = parts[1]
    elif dtype in ("mds", "rgw") and len(parts) >= 3:
        candidate = parts[2]
    elif dtype == "nfs" and len(parts) >= 5:
        candidate = parts[4]
    elif dtype in ("ceph-exporter", "node-exporter") and len(parts) >= 2:
        candidate = parts[1]
    return candidate if candidate in valid_hosts else ""


def _collect_mgr_upgrade_timeline(
    ceph_cluster,
    rados_obj,
    upgrade_start: str,
    upgrade_end: str,
) -> dict:
    """Collect per-daemon upgrade timeline from MGR cephadm module logs.

    Reads ``ceph.cephadm.log`` from a MON host (cluster channel log that
    aggregates events from ALL active MGRs, surviving failovers).  Falls
    back to ``ceph log last cephadm`` CLI if the log file is unavailable.

    Returns a dict with:
      - ``daemon_timeline``: list of per-daemon-type dicts for
        ``report.set_daemon_timeline()``.
      - ``upgrade_events``: list of operational events (key rotations,
        OSD flags, FS events, safety checks, image pulls, lifecycle).
    """
    try:
        start_dt = datetime.fromisoformat(upgrade_start.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(upgrade_end.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        log.warning("Invalid upgrade timestamps for MGR log collection")
        return {"daemon_timeline": [], "upgrade_events": []}

    buffer = timedelta(minutes=5)

    # -- Step 1: Get FSID and MON hosts --
    fsid = ""
    try:
        fsid_out, _ = rados_obj.node.installer.exec_command(
            sudo=True, cmd="cephadm shell -- ceph fsid", timeout=30
        )
        fsid = fsid_out.strip()
    except Exception as e:
        log.warning(f"Could not get FSID: {e}")

    mon_nodes = ceph_cluster.get_nodes(role="mon")
    if not mon_nodes:
        mon_nodes = [rados_obj.node.installer]

    # -- Step 2: Read ceph.cephadm.log from a MON host --
    # Time-windowed + pattern-filtered read to handle multi-GB debug logs.
    # Reads BOTH rotated (.gz) and current log files to handle mid-upgrade
    # log rotation. awk filters by timestamp, grep selects upgrade-relevant
    # lines (deploy events, type boundaries, operational events).
    since_str = (start_dt - buffer).strftime("%Y-%m-%dT%H:%M")
    until_str = (end_dt + buffer).strftime("%Y-%m-%dT%H:%M")
    upgrade_grep = (
        "Deploying daemon|Deployed |"
        "Upgrade:|daemons are up to date|"
        "Redeploying .* with new keyring|"
        "noout|noscrub|nodeep-scrub|"
        "Setting container_image|"
        "Pulling image|pulled .* image|"
        "fail_fs|standby_for_fscid|"
        "Stopping .* service|Removing .* service|"
        "mclock|osd_mclock"
    )
    raw_lines = []
    for mon_node in mon_nodes:
        hostname = getattr(mon_node, "hostname", str(mon_node))
        if not fsid:
            break
        log_dir = f"/var/log/ceph/{fsid}"
        log_file = f"{log_dir}/ceph.cephadm.log"
        cmd = (
            f"{{ zcat {log_dir}/ceph.cephadm.log-*.gz 2>/dev/null; "
            f"cat {log_file} 2>/dev/null; }} | "
            f'awk \'$0 >= "{since_str}" && $0 <= "{until_str}"\' | '
            f"grep -E '{upgrade_grep}'"
        )
        try:
            out, _ = mon_node.exec_command(
                sudo=True,
                cmd=cmd,
                timeout=180,
            )
            if out and out.strip():
                raw_lines = out.strip().splitlines()
                log.info(
                    f"Read {len(raw_lines)} upgrade-relevant lines "
                    f"from cephadm logs on {hostname} "
                    f"(window: {since_str} to {until_str})"
                )
                break
        except Exception as e:
            log.debug(f"ceph.cephadm.log read from {hostname}: {e}")

    # -- Step 3: Fallback to ceph log last cephadm --
    # ponytail: The cluster log uses a 10K-line ring buffer. With debug logging
    # on a large cluster, a 49-min upgrade generates far more than 10K lines,
    # so this fallback may only capture the final minutes. The ceph.cephadm.log
    # file (Step 2) is the authoritative source.
    if not raw_lines:
        log.info("ceph.cephadm.log unavailable, trying ceph log last cephadm")
        try:
            out, _ = rados_obj.node.installer.exec_command(
                sudo=True,
                cmd="cephadm shell -- ceph log last 99999 debug cephadm",
                timeout=120,
            )
            if out and out.strip():
                raw_lines = out.strip().splitlines()
                log.info(f"ceph log last cephadm returned {len(raw_lines)} lines")
                # Warn if buffer likely truncated (first log entry far after
                # upgrade start suggests the ring buffer overflowed)
                if raw_lines:
                    first_m = _CEPHADM_LOG_RE.match(raw_lines[0])
                    if first_m:
                        try:
                            first_ts = datetime.fromisoformat(first_m.group(1))
                            if first_ts.tzinfo is None:
                                first_ts = first_ts.replace(tzinfo=timezone.utc)
                            gap = (first_ts - start_dt).total_seconds()
                            if gap > 300:
                                log.warning(
                                    f"ceph log buffer likely truncated: first entry "
                                    f"is {int(gap)}s after upgrade start. "
                                    f"Early upgrade events may be missing."
                                )
                        except (ValueError, TypeError):
                            pass
        except Exception as e:
            log.warning(f"ceph log last cephadm failed: {e}")

    if not raw_lines:
        log.warning("No MGR cephadm log data available")
        return {"daemon_timeline": [], "upgrade_events": []}

    # -- Step 4: Parse log lines into deploy events --
    deploy_events: list[dict] = []
    type_boundaries: list[dict] = []
    upgrade_events: list[dict] = []
    seen_daemons: set[str] = set()

    for line in raw_lines:
        m = _CEPHADM_LOG_RE.match(line)
        if not m:
            continue

        ts_str, _, message = m.group(1), m.group(2), m.group(3)
        try:
            ts_dt = datetime.fromisoformat(ts_str)
            if ts_dt.tzinfo is None:
                ts_dt = ts_dt.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            continue

        if ts_dt < (start_dt - buffer) or ts_dt > (end_dt + buffer):
            continue

        # Per-daemon deploy events (actual image upgrade).
        # "Deploying daemon X on Y" = first-seen candidate (may be pre-upgrade).
        # "Upgrade: Updating ..." = authoritative image-upgrade time (overwrites).
        # "Redeploying X with new keyring" is key_rotation, NOT a daemon upgrade.
        def _record_deploy(daemon_name, host, ts_iso, *, is_update=False):
            if not daemon_name:
                return
            if daemon_name in seen_daemons:
                # Image-upgrade timestamp wins over an earlier bare Deploying
                # (e.g. NFS deployed during mgr bootstrap, upgraded in phase 3).
                if is_update:
                    for e in deploy_events:
                        if e["name"] == daemon_name:
                            e["redeploy_time"] = ts_iso
                            if host:
                                e["host"] = host
                            break
                return
            seen_daemons.add(daemon_name)
            deploy_events.append(
                {
                    "name": daemon_name,
                    "daemon_type": daemon_name.split(".")[0],
                    "host": host or "",
                    "redeploy_time": ts_iso,
                }
            )

        dm = _DEPLOYING_RE.search(message)
        if dm:
            _record_deploy(dm.group(1), dm.group(2), ts_dt.isoformat())
            continue

        sm = _UPDATING_SINGLE_RE.search(message)
        if sm:
            _record_deploy(sm.group(1), "", ts_dt.isoformat(), is_update=True)
            continue

        bm = _UPDATING_BATCH_RE.search(message)
        if bm:
            for part in bm.group(1).replace("'", "").split(","):
                daemon_name = part.strip().strip("'\"[] ")
                _record_deploy(daemon_name, "", ts_dt.isoformat(), is_update=True)
            continue

        # Type-complete (Tentacle): Setting container_image for all <type>
        si = _SET_IMAGE_RE.search(message)
        if si:
            dtype = si.group(1)
            type_boundaries.append(
                {
                    "daemon_type": dtype,
                    "event": "end",
                    "timestamp": ts_dt.isoformat(),
                }
            )
            # Redeploy annotation only checks that a window "start" exists.
            type_boundaries.append(
                {
                    "daemon_type": dtype,
                    "event": "start",
                    "timestamp": ts_dt.isoformat(),
                }
            )
            continue

        # -- Operational events (key rotations, OSD flags, FS, safety) --
        kr = _KEYRING_ROTATE_RE.search(message)
        if kr:
            dname = kr.group(1)
            dtype = dname.split(".")[0]
            upgrade_events.append(
                {
                    "timestamp": ts_dt.isoformat(),
                    "category": "key_rotation",
                    "action": "rotate",
                    "daemon_type": dtype,
                    "detail": f"Rotating keyring for {dname}",
                }
            )
            continue

        krd = _KEYRING_REDEPLOY_RE.search(message)
        if krd:
            dname = krd.group(1)
            dtype = dname.split(".")[0]
            upgrade_events.append(
                {
                    "timestamp": ts_dt.isoformat(),
                    "category": "key_rotation",
                    "action": "redeploy",
                    "daemon_type": dtype,
                    "detail": f"Redeploying {dname} with new keyring",
                }
            )
            continue

        osd_set = _OSD_FLAG_SET_RE.search(message)
        if osd_set:
            upgrade_events.append(
                {
                    "timestamp": ts_dt.isoformat(),
                    "category": "osd_flag",
                    "action": "set",
                    "daemon_type": "osd",
                    "detail": f"{osd_set.group(1)} set",
                }
            )
            continue

        osd_unset = _OSD_FLAG_UNSET_RE.search(message)
        if osd_unset:
            upgrade_events.append(
                {
                    "timestamp": ts_dt.isoformat(),
                    "category": "osd_flag",
                    "action": "unset",
                    "daemon_type": "osd",
                    "detail": f"{osd_unset.group(1)} unset",
                }
            )
            continue

        fsj = _FS_JOINABLE_RE.search(message)
        if fsj:
            upgrade_events.append(
                {
                    "timestamp": ts_dt.isoformat(),
                    "category": "fs_event",
                    "action": "joinable",
                    "daemon_type": "mds",
                    "detail": f"FS {fsj.group(1)} joinable",
                }
            )
            continue

        mds_c = _MDS_COMPLETE_RE.search(message)
        if mds_c:
            upgrade_events.append(
                {
                    "timestamp": ts_dt.isoformat(),
                    "category": "fs_event",
                    "action": "mds_complete",
                    "daemon_type": "mds",
                    "detail": f"All MDS upgraded to {mds_c.group(1)}",
                }
            )
            continue

        osd_safe = _OSD_SAFE_RE.search(message)
        if osd_safe:
            upgrade_events.append(
                {
                    "timestamp": ts_dt.isoformat(),
                    "category": "safety_check",
                    "action": "safe",
                    "daemon_type": "osd",
                    "detail": f"{osd_safe.group(1)} safe to restart",
                }
            )
            continue

        osd_unsafe = _OSD_UNSAFE_RE.search(message)
        if osd_unsafe:
            upgrade_events.append(
                {
                    "timestamp": ts_dt.isoformat(),
                    "category": "safety_check",
                    "action": "unsafe",
                    "daemon_type": "osd",
                    "detail": "unsafe to stop osd(s) at this time",
                }
            )
            continue

        uc = _UPGRADE_COMPLETE_RE.search(message)
        if uc:
            upgrade_events.append(
                {
                    "timestamp": ts_dt.isoformat(),
                    "category": "upgrade_lifecycle",
                    "action": "complete",
                    "daemon_type": "",
                    "detail": "Upgrade Complete!",
                }
            )
            continue

        us = _UPGRADE_START_RE.search(message)
        if us:
            upgrade_events.append(
                {
                    "timestamp": ts_dt.isoformat(),
                    "category": "upgrade_lifecycle",
                    "action": "started",
                    "daemon_type": "",
                    "detail": f"Started with target {us.group(1)}",
                }
            )
            continue

        ut = _UPGRADE_TARGET_RE.search(message)
        if ut:
            upgrade_events.append(
                {
                    "timestamp": ts_dt.isoformat(),
                    "category": "upgrade_lifecycle",
                    "action": "target",
                    "daemon_type": "",
                    "detail": f"Target is {ut.group(1)}",
                }
            )
            continue

        su = _SELF_UPGRADE_RE.search(message)
        if su:
            upgrade_events.append(
                {
                    "timestamp": ts_dt.isoformat(),
                    "category": "upgrade_lifecycle",
                    "action": "self_upgrade",
                    "daemon_type": "mgr",
                    "detail": f"Need to upgrade myself ({su.group(1)})",
                }
            )
            continue

        krd_delay = _KEY_ROTATION_DELAY_RE.search(message)
        if krd_delay:
            dname = krd_delay.group(1)
            dtype = dname.split(".")[0]
            upgrade_events.append(
                {
                    "timestamp": ts_dt.isoformat(),
                    "category": "key_rotation",
                    "action": "delayed",
                    "daemon_type": dtype,
                    "detail": f"Delaying rotation for {dname}, not ok-to-stop",
                }
            )
            continue

        krc = _KEY_ROTATION_COMPLETE_RE.search(message)
        if krc:
            upgrade_events.append(
                {
                    "timestamp": ts_dt.isoformat(),
                    "category": "key_rotation",
                    "action": "complete",
                    "daemon_type": "osd",
                    "detail": "OSD/mds daemon key rotation completed",
                }
            )
            continue

        krcheck = _KEY_ROTATION_CHECK_RE.search(message)
        if krcheck:
            upgrade_events.append(
                {
                    "timestamp": ts_dt.isoformat(),
                    "category": "key_rotation",
                    "action": "check",
                    "daemon_type": "osd",
                    "detail": "Checking for keys needing rotation",
                }
            )
            continue

    if not deploy_events:
        log.info("No daemon deploy events found in MGR cephadm logs")
        return {"daemon_timeline": [], "upgrade_events": upgrade_events}

    log.info(
        f"Parsed {len(deploy_events)} deploy events, "
        f"{len(type_boundaries)} type boundaries, and "
        f"{len(upgrade_events)} operational events from MGR logs"
    )

    # -- Step 4b: Annotate redeploys vs upgrades using type boundaries --
    # Tentacle: a type gets a "start" boundary when cephadm logs
    # "Upgrade: Setting container_image for all <type>". If that marker
    # exists for the type, ALL deploys of that type are upgrades; otherwise
    # they are redeploys. (Timestamps vs window are not used.)
    # Monitoring may still be image-upgraded via proactive manifest apply;
    # _annotate_monitoring_manifest_upgrades() fixes those after collection.
    type_windows: dict[str, dict[str, datetime]] = {}
    for tb in type_boundaries:
        dt = tb["daemon_type"]
        evt = tb["event"]
        try:
            ts = datetime.fromisoformat(tb["timestamp"])
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            continue
        if dt not in type_windows:
            type_windows[dt] = {}
        if evt == "start":
            type_windows[dt]["start"] = ts
        elif evt == "end":
            type_windows[dt]["end"] = ts

    redeploy_count = 0
    for entry in deploy_events:
        dtype = entry["daemon_type"]
        window = type_windows.get(dtype)
        entry["is_redeploy"] = not window or "start" not in window
        if entry["is_redeploy"]:
            redeploy_count += 1

    if redeploy_count:
        log.info(
            f"Annotated {redeploy_count}/{len(deploy_events)} deploy events "
            f"as redeploys (no upgrade window for their type)"
        )

    # -- Step 4c: Build phase windows from Started/Complete lifecycle events --
    phase_windows: list[dict] = []
    _seen_lifecycle: set[tuple[str, str]] = set()
    _lifecycle_events = [
        e
        for e in upgrade_events
        if e.get("category") == "upgrade_lifecycle"
        and e.get("action") in ("started", "complete")
    ]
    _lifecycle_events.sort(key=lambda e: e["timestamp"])
    _pending_start = None
    for evt in _lifecycle_events:
        dedup_key = (evt["timestamp"], evt["action"])
        if dedup_key in _seen_lifecycle:
            continue
        _seen_lifecycle.add(dedup_key)
        if evt["action"] == "started":
            _pending_start = datetime.fromisoformat(evt["timestamp"])
        elif evt["action"] == "complete" and _pending_start is not None:
            phase_windows.append(
                {
                    "idx": len(phase_windows),
                    "start": _pending_start.isoformat(),
                    "end": datetime.fromisoformat(evt["timestamp"]).isoformat(),
                }
            )
            _pending_start = None
    if _pending_start is not None:
        phase_windows.append(
            {
                "idx": len(phase_windows),
                "start": _pending_start.isoformat(),
                "end": end_dt.isoformat(),
            }
        )
    if phase_windows:
        log.info(f"Built {len(phase_windows)} phase windows from lifecycle events")

    # Assign phase_idx to each deploy event (-1 = outside all windows)
    _pw_parsed = [
        (
            datetime.fromisoformat(pw["start"]),
            datetime.fromisoformat(pw["end"]),
            pw["idx"],
        )
        for pw in phase_windows
    ]
    for entry in deploy_events:
        entry["phase_idx"] = -1
        try:
            t = datetime.fromisoformat(entry["redeploy_time"])
            for pw_start, pw_end, pw_idx in _pw_parsed:
                if pw_start <= t <= pw_end:
                    entry["phase_idx"] = pw_idx
                    break
        except (ValueError, TypeError):
            pass

    # -- Step 4d: Backfill empty host fields via ceph orch ps --
    # The batch update log line (Upgrade: Updating [['daemon.name']]) does not
    # include the host.  Use rados_obj.run_ceph_command -- same approach as
    # core_workflows.scan_daemon_logs_for_crashes -- to resolve placement.
    # Must run BEFORE Step 5 (duration inference groups by host).
    missing_host = [e for e in deploy_events if not e.get("host")]
    if missing_host:
        log.info(
            f"Backfilling host for {len(missing_host)}/{len(deploy_events)} "
            f"deploy events via 'ceph orch ps'"
        )
        try:
            orch_daemons = rados_obj.run_ceph_command(cmd="ceph orch ps")
            orch_host_map = {}
            valid_hosts = set()
            for d in orch_daemons or []:
                dn = d.get("daemon_name") or (
                    f"{d.get('daemon_type', '')}.{d.get('daemon_id', '')}"
                )
                hn = d.get("hostname", "")
                if dn and hn:
                    orch_host_map[dn] = hn
                    valid_hosts.add(hn)
            filled = 0
            for entry in deploy_events:
                if not entry.get("host"):
                    entry["host"] = orch_host_map.get(entry["name"], "")
                    if not entry["host"]:
                        entry["host"] = _host_from_daemon_name(
                            entry["name"], valid_hosts
                        )
                    if entry["host"]:
                        filled += 1
            log.info(f"Backfilled host for {filled}/{len(missing_host)} deploy events")
        except Exception as e:
            log.warning(f"Failed to backfill hosts via 'ceph orch ps': {e}")

    # -- Step 5: Infer deploy durations from inter-event gaps --
    # Same-host gap to the next deploy is a proxy for duration, but the orch
    # often idles 10–30+ minutes between daemon types on a host. Capping that
    # idle wait at 600s produced fake exact-600s bars for crash/exporters/nfs.
    # Keep short ("trusted") gaps; replace long ones with the same-type median.
    TRUSTED_GAP_SEC = 180
    FALLBACK_DURATION_SEC = 60
    HARD_MAX_DURATION_SEC = 1800  # sanity only; real OSD/MDS spans use type window
    all_sorted = sorted(deploy_events, key=lambda d: d["redeploy_time"])

    # Build phase_end lookup for duration capping
    _phase_end_map: dict[int, datetime] = {}
    for pw in phase_windows:
        try:
            _phase_end_map[pw["idx"]] = datetime.fromisoformat(pw["end"])
        except (ValueError, TypeError):
            pass

    by_host: dict[str, list[dict]] = defaultdict(list)
    for entry in all_sorted:
        host_key = entry["host"] or "_unknown"
        by_host[host_key].append(entry)

    for _host, host_entries in by_host.items():
        for i, entry in enumerate(host_entries):
            try:
                t_cur = datetime.fromisoformat(entry["redeploy_time"])
            except (ValueError, TypeError):
                entry["individual_duration_sec"] = 0
                entry["_duration_trusted"] = True
                continue

            cur_phase = entry.get("phase_idx", -1)
            if i + 1 < len(host_entries):
                try:
                    t_next = datetime.fromisoformat(
                        host_entries[i + 1]["redeploy_time"]
                    )
                    gap = (t_next - t_cur).total_seconds()
                except (ValueError, TypeError):
                    entry["individual_duration_sec"] = 0
                    entry["_duration_trusted"] = True
                    continue
                # Next deploy on this host belongs to a later orch phase —
                # don't let the inter-phase gap inflate this daemon's bar
                # into the cooldown / next phase (e.g. grafana → mds).
                next_phase = host_entries[i + 1].get("phase_idx", -1)
                if next_phase != cur_phase and cur_phase in _phase_end_map:
                    gap = min(
                        gap, max((_phase_end_map[cur_phase] - t_cur).total_seconds(), 0)
                    )
            else:
                upper = _phase_end_map.get(cur_phase, end_dt)
                gap = (upper - t_cur).total_seconds()

            gap = max(gap, 0)
            if gap <= TRUSTED_GAP_SEC:
                entry["individual_duration_sec"] = gap
                entry["_duration_trusted"] = True
            else:
                # Orch idle / end-of-phase stretch — fill from peers below.
                entry["individual_duration_sec"] = None
                entry["_duration_trusted"] = False

    # Fill untrusted durations from same-type median of trusted gaps.
    by_type_for_fill: dict[str, list[dict]] = defaultdict(list)
    for entry in deploy_events:
        by_type_for_fill[entry["daemon_type"]].append(entry)

    for dtype, entries in by_type_for_fill.items():
        trusted_vals = [
            e["individual_duration_sec"]
            for e in entries
            if e.get("_duration_trusted")
            and isinstance(e.get("individual_duration_sec"), (int, float))
        ]
        positive = [v for v in trusted_vals if v > 0]
        if positive:
            fill = float(statistics.median(positive))
        elif trusted_vals:
            # All trusted gaps were ~0 (simultaneous deploys) — not useful fill.
            fill = float(FALLBACK_DURATION_SEC)
        else:
            fill = float(FALLBACK_DURATION_SEC)
        fill = min(fill, HARD_MAX_DURATION_SEC)

        filled = 0
        for e in entries:
            if e.get("individual_duration_sec") is None:
                e["individual_duration_sec"] = fill
                filled += 1
            else:
                e["individual_duration_sec"] = min(
                    float(e["individual_duration_sec"]), HARD_MAX_DURATION_SEC
                )
            e.pop("_duration_trusted", None)

        if filled:
            log.info(
                f"Duration fill for {dtype}: {filled}/{len(entries)} untrusted "
                f"gaps replaced with median/fallback {fill:.1f}s"
            )

    # -- Step 6: Group by type and build timeline --
    by_type: dict[str, list[dict]] = defaultdict(list)
    for entry in deploy_events:
        by_type[entry["daemon_type"]].append(entry)

    # Extract lifecycle events from daemon-specific log files
    timestamped_lifecycle: dict[str, list[dict]] = {}
    try:
        timestamped_lifecycle = _extract_daemon_lifecycle_events(
            ceph_cluster, by_type, fsid=fsid
        )
    except Exception as e:
        log.warning(f"Daemon lifecycle event extraction failed: {e}")

    _cephadm_per_daemon: dict[str, list[dict]] = {}
    for de in deploy_events:
        _cephadm_per_daemon.setdefault(de["daemon_type"], []).append(
            {
                "event": "deploying",
                "timestamp": de["redeploy_time"],
                "daemon_name": de["name"],
            }
        )
    for ue in upgrade_events:
        if ue.get("action") == "self_upgrade":
            _m = re.search(r"\((\S+)\)", ue.get("detail", ""))
            if _m:
                _cephadm_per_daemon.setdefault("mgr", []).append(
                    {
                        "event": "scheduled_upgrade",
                        "timestamp": ue["timestamp"],
                        "daemon_name": _m.group(1),
                    }
                )
        elif ue.get("action") == "safe" and ue.get("category") == "safety_check":
            _m = re.search(r"(osd\.\d+)", ue.get("detail", ""))
            if _m:
                _cephadm_per_daemon.setdefault("osd", []).append(
                    {
                        "event": "safe_to_restart",
                        "timestamp": ue["timestamp"],
                        "daemon_name": _m.group(1),
                    }
                )

    for dtype, fb_events in _cephadm_per_daemon.items():
        if not timestamped_lifecycle.get(dtype):
            timestamped_lifecycle[dtype] = sorted(
                fb_events, key=lambda e: e["timestamp"]
            )
        else:
            existing_dnames = {
                e.get("daemon_name")
                for e in timestamped_lifecycle[dtype]
                if e.get("daemon_name")
            }
            for fb in fb_events:
                if (
                    not fb.get("daemon_name")
                    or fb["daemon_name"] not in existing_dnames
                ):
                    timestamped_lifecycle[dtype].append(fb)
            timestamped_lifecycle[dtype].sort(key=lambda e: e["timestamp"])

    def _build_timeline_entry(daemons_list, dtype, is_redeploy=False):
        """Build a single timeline entry from a sorted list of daemon deploys."""
        from upgrade_thrashing.lifecycle_log import compute_deploy_group_span

        daemons_sorted = sorted(daemons_list, key=lambda d: d["redeploy_time"])
        duration, first_time, window_end = compute_deploy_group_span(daemons_sorted)

        timestamped_for_type = timestamped_lifecycle.get(dtype, [])

        # Split timestamped events: real daemon-log events vs orch
        # operational events.  _ORCH_EVENT_NAMES must stay in sync with
        # event names produced by the _cephadm_per_daemon merge above.
        _ORCH_EVENT_NAMES = {
            "deploying",
            "safe_to_restart",
            "scheduled_upgrade",
            "redeployed",
        }
        per_daemon_lifecycle: dict[str, list[dict]] = {}
        per_daemon_orch_events: dict[str, list[dict]] = {}
        for lc_evt in timestamped_for_type:
            if not isinstance(lc_evt, dict):
                continue
            dname_key = lc_evt.get("daemon_name", "")
            evt_name = lc_evt.get("event", "")
            ts = lc_evt.get("timestamp", "")
            if not dname_key:
                continue
            target = (
                per_daemon_orch_events
                if evt_name in _ORCH_EVENT_NAMES
                else per_daemon_lifecycle
            )
            item = {
                "event": evt_name,
                "timestamp": ts,
            }
            if lc_evt.get("configured_sec") is not None:
                item["configured_sec"] = lc_evt["configured_sec"]
            target.setdefault(dname_key, []).append(item)

        per_daemon_merged: dict[str, list[dict]] = {}
        for d in daemons_sorted:
            dname = d["name"]
            per_daemon_merged[dname] = _merge_daemon_lifecycle_events(
                per_daemon_lifecycle.get(dname, []),
                per_daemon_orch_events.get(dname, []),
            )
        type_lifecycle = _build_type_lifecycle_summary(list(per_daemon_merged.values()))
        log_event_count = sum(len(v) for v in per_daemon_lifecycle.values())
        lifecycle_source = "log+orch" if log_event_count else "orch_only"

        return {
            "daemon_type": dtype,
            "count": len(daemons_sorted),
            "start_time": first_time,
            "end_time": window_end,
            "duration_sec": round(duration, 1),
            "phase_idx": daemons_sorted[0].get("phase_idx", -1),
            "is_redeploy": is_redeploy,
            "lifecycle_source": lifecycle_source,
            "individual_daemons": [
                {
                    "name": d["name"],
                    "host": d["host"],
                    "redeploy_time": d["redeploy_time"],
                    "duration_sec": round(d.get("individual_duration_sec", 0), 1),
                    "lifecycle_events": per_daemon_merged.get(d["name"], [])
                    or [
                        {
                            "event": "redeployed",
                            "timestamp": d["redeploy_time"],
                        }
                    ],
                }
                for d in daemons_sorted
            ],
            "lifecycle_events": type_lifecycle,
            "io_impact": {
                "description": _IO_IMPACT_DESC.get(dtype, ""),
            },
        }

    from upgrade_thrashing.lifecycle_log import group_deploy_events_for_timeline

    timeline: list[dict] = []
    type_sort_index = {t: i for i, t in enumerate(_UPGRADE_ORDER)}
    grouped = group_deploy_events_for_timeline(deploy_events)
    grouped.sort(
        key=lambda g: (
            type_sort_index.get(g["daemon_type"], 999),
            g["phase_idx"],
            g["is_redeploy"],
        )
    )
    for group in grouped:
        timeline.append(
            _build_timeline_entry(
                group["deploys"],
                group["daemon_type"],
                is_redeploy=group["is_redeploy"],
            )
        )

    log.info(
        f"Built daemon upgrade timeline: {len(timeline)} daemon types, "
        f"{len(deploy_events)} total deploys"
    )
    return {
        "daemon_timeline": timeline,
        "upgrade_events": upgrade_events,
    }


def _collect_cluster_hardware_info(ceph_cluster, rados_obj):
    """Collect hardware specs and daemon placement from every cluster node.

    Runs a single batched SSH command per node to gather CPU, RAM, disk,
    and network info.  Queries ``ceph orch host ls`` and ``ceph orch ps``
    on the installer node for daemon placement.

    Failures on individual nodes are logged and skipped so that hardware
    collection never crashes the test.

    Args:
        ceph_cluster: Cluster object with get_nodes().
        rados_obj: RadosOrchestrator instance (for ceph CLI access).

    Returns:
        Dict with ``hosts`` and ``daemon_placement`` lists.
    """
    all_nodes = ceph_cluster.get_nodes()
    client_nodes = set()
    for c in ceph_cluster.get_nodes(role="client"):
        client_nodes.add(getattr(c, "hostname", str(c)))

    cluster_nodes = [
        n for n in all_nodes if getattr(n, "hostname", str(n)) not in client_nodes
    ]

    hostnames = [getattr(n, "hostname", str(n)) for n in cluster_nodes]
    common_prefix = _find_common_prefix(hostnames)

    hw_script = (
        "echo '---CPU_START---';"
        "lscpu 2>/dev/null | grep -E '^(CPU\\(s\\)|Model name|Architecture)';"
        "echo '---RAM_START---';"
        "free -h 2>/dev/null | grep Mem | awk '{print $2}';"
        "echo '---DISK_START---';"
        "lsblk -d -o NAME,SIZE,ROTA,TYPE --noheadings 2>/dev/null | grep disk;"
        "echo '---NET_START---';"
        "for iface in $(ls /sys/class/net/ | grep -v lo); do "
        "  addr=$(ip -br addr show dev $iface 2>/dev/null | awk '{print $3}');"
        "  spd=$(cat /sys/class/net/$iface/speed 2>/dev/null);"
        '  if [ -z "$spd" ] || [ "$spd" = "-1" ]; then '
        "    spd=$(ethtool $iface 2>/dev/null | grep Speed | awk '{print $2}');"
        "  else "
        '    spd="${spd}Mb/s";'
        "  fi;"
        '  case "$spd" in ""|"Unknown!"*) spd="N/A";; esac;'
        "  mtu=$(cat /sys/class/net/$iface/mtu 2>/dev/null || echo '?');"
        '  echo "$iface $addr ${spd} mtu:$mtu";'
        "done;"
        "echo '---END---'"
    )

    def _gather_from_node(node):
        hostname = getattr(node, "hostname", str(node))
        short = _shorten_hostname(hostname, common_prefix)
        info = {
            "hostname": hostname,
            "short_name": short,
            "roles": [],
            "cpu_count": "N/A",
            "cpu_model": "N/A",
            "ram_total": "N/A",
            "disks": [],
            "disk_summary": "N/A",
            "network": "N/A",
        }
        try:
            out, _ = node.exec_command(sudo=True, cmd=hw_script, timeout=30)
        except Exception as e:
            log.debug(f"Hardware collection from {hostname} failed: {e}")
            return info

        if not out:
            return info

        _parse_hardware_output(out, info)
        return info

    hosts = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_gather_from_node, n): n for n in cluster_nodes}
        for future in concurrent.futures.as_completed(futures):
            try:
                hosts.append(future.result())
            except Exception as e:
                node = futures[future]
                log.debug(
                    f"Hardware thread failed for "
                    f"{getattr(node, 'hostname', node)}: {e}"
                )

    daemon_placement = _collect_daemon_placement(rados_obj, hosts)

    hosts.sort(key=lambda h: h.get("hostname", ""))

    hw_info = {"hosts": hosts, "daemon_placement": daemon_placement}

    # RGW daemon details
    try:
        rgw_daemons = rados_obj.run_ceph_command(
            cmd="ceph orch ps --daemon-type rgw", timeout=60
        )
        if isinstance(rgw_daemons, list) and rgw_daemons:
            hw_info["rgw"] = {
                "daemon_count": len(rgw_daemons),
                "service_names": sorted(
                    {d.get("service_name", "") for d in rgw_daemons}
                ),
                "hosts": sorted({d.get("hostname", "") for d in rgw_daemons}),
                "ports": sorted(
                    {d.get("ports", [80])[0] for d in rgw_daemons if d.get("ports")}
                ),
                "version": rgw_daemons[0].get("version", ""),
            }
    except Exception as e:
        log.debug(f"RGW daemon collection failed: {e}")

    return hw_info


def _parse_hardware_output(raw_output, info):
    """Parse the batched hardware script output into the info dict."""
    sections = {
        "cpu": "",
        "ram": "",
        "disk": "",
        "net": "",
    }
    current = None
    for line in raw_output.splitlines():
        stripped = line.strip()
        if stripped == "---CPU_START---":
            current = "cpu"
            continue
        elif stripped == "---RAM_START---":
            current = "ram"
            continue
        elif stripped == "---DISK_START---":
            current = "disk"
            continue
        elif stripped == "---NET_START---":
            current = "net"
            continue
        elif stripped == "---END---":
            break
        if current:
            sections[current] += line + "\n"

    for line in sections["cpu"].strip().splitlines():
        if line.startswith("CPU(s):"):
            info["cpu_count"] = line.split(":", 1)[1].strip()
        elif line.startswith("Model name:"):
            info["cpu_model"] = line.split(":", 1)[1].strip()

    ram = sections["ram"].strip()
    if ram:
        info["ram_total"] = ram.splitlines()[0].strip()

    ssd_count = 0
    hdd_count = 0
    disks = []
    for line in sections["disk"].strip().splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        name = parts[0]
        size = parts[1]
        rota = parts[2]
        dtype = parts[3] if len(parts) > 3 else "disk"
        is_rotational = rota == "1"
        if is_rotational:
            hdd_count += 1
        else:
            ssd_count += 1
        disks.append(
            {
                "name": name,
                "size": size,
                "rotational": is_rotational,
                "type": dtype,
            }
        )
    info["disks"] = disks

    summary_parts = []
    if ssd_count:
        summary_parts.append(f"{ssd_count} SSD")
    if hdd_count:
        summary_parts.append(f"{hdd_count} HDD")
    info["disk_summary"] = " + ".join(summary_parts) if summary_parts else "N/A"

    net_lines = sections["net"].strip().splitlines()
    if net_lines:
        net_entries = []
        for nl in net_lines:
            parts = nl.split()
            if len(parts) >= 3:
                iface = parts[0]
                addr = parts[1] if parts[1] else "no-addr"
                speed = parts[2] if len(parts) > 2 else "unknown"
                mtu = parts[3] if len(parts) > 3 else ""
                entry = f"{iface}: {addr}"
                if speed and speed not in ("unknown", "N/A"):
                    entry += f" ({speed})"
                if mtu:
                    entry += f" {mtu}"
                net_entries.append(entry)
        info["network"] = "; ".join(net_entries) if net_entries else "N/A"


def _collect_daemon_placement(rados_obj, hosts):
    """Query ceph orch to determine daemon placement per type.

    Populates the ``roles`` field on each host entry and returns
    a daemon_placement summary list.
    """
    daemon_placement = []
    try:
        ps_out = rados_obj.run_ceph_command(cmd="ceph orch ps")
        if not ps_out or not isinstance(ps_out, list):
            return daemon_placement

        host_roles = defaultdict(set)
        type_hosts = defaultdict(set)
        type_counts = defaultdict(int)

        for daemon in ps_out:
            if not isinstance(daemon, dict):
                continue
            dtype = daemon.get("daemon_type", "")
            dhost = daemon.get("hostname", "")
            if dtype and dhost:
                host_roles[dhost].add(dtype)
                type_hosts[dtype].add(dhost)
                type_counts[dtype] += 1

        for h in hosts:
            hostname = h.get("hostname", "")
            roles = host_roles.get(hostname, set())
            h["roles"] = sorted(roles)

        for dtype in sorted(type_counts.keys()):
            short_hosts = sorted(
                _shorten_hostname(
                    hname,
                    _find_common_prefix(list(type_hosts[dtype])),
                )
                for hname in type_hosts[dtype]
            )
            daemon_placement.append(
                {
                    "daemon_type": dtype,
                    "count": type_counts[dtype],
                    "hosts": short_hosts,
                }
            )
    except Exception as e:
        log.debug(f"Daemon placement collection failed: {e}")

    return daemon_placement


def _find_common_prefix(names):
    """Find the longest common prefix ending at a dash or dot boundary."""
    if not names or len(names) < 2:
        return ""
    prefix = os.path.commonprefix(names)
    boundary = max(prefix.rfind("-"), prefix.rfind("."))
    if boundary > 0:
        return prefix[: boundary + 1]
    return ""


def _shorten_hostname(hostname, prefix):
    """Strip a common prefix from a hostname for compact display."""
    if prefix and hostname.startswith(prefix):
        short = hostname[len(prefix) :]
        return short if short else hostname
    return hostname


def _collect_state_snapshot(rados_obj, start_time):
    """Collect a single shared state snapshot with serial command execution.

    Phase 6 Step 2: All ceph commands share the installer SSH transport, so
    they are executed serially to avoid Paramiko ChannelException under load.
    The crash check runs after (depends on current timestamp).

    Args:
        rados_obj: RadosOrchestrator instance.
        start_time: Cluster timestamp from test start (for crash window).

    Returns:
        Dict keyed by command name with parsed ceph command output.
    """
    log.info("Collecting state snapshot for verification")

    commands = {
        "health_detail": "ceph health detail",
        "versions": "ceph versions",
        "orch_ps": "ceph orch ps",
        "config_dump": "ceph config dump",
        "osd_tree": "ceph osd tree",
        "auth_ls": "ceph auth ls",
        "mgr_modules": "ceph mgr module ls",
        "balancer_status": "ceph balancer status",
        "osd_dump": "ceph osd dump",
        "pg_stat": "ceph pg stat",
        "mon_stat": "ceph mon stat",
    }

    snapshot = {}

    # All of these commands share the installer node's SSH transport.
    # Run them serially to avoid Paramiko ChannelException under load.
    try:
        installer = getattr(rados_obj, "node", None)
        if installer is not None and hasattr(installer, "reconnect"):
            installer.reconnect()
    except Exception as e:
        log.warning(f"Installer reconnect before snapshot failed: {e}")

    for key, cmd in commands.items():
        try:
            snapshot[key] = rados_obj.run_ceph_command(cmd=cmd)
        except Exception as e:
            log.warning(f"Snapshot collection failed for {key}: {e}")
            snapshot[key] = None

    # Crash check: crash-module first (no host log crawl), then one
    # scan_daemon_logs_for_crashes pass whose return value feeds the HTML report.
    end_ts = get_cluster_timestamp(rados_obj.node)
    try:
        snapshot["crash_found"] = bool(
            rados_obj.check_crash_status(
                start_time=start_time,
                end_time=end_ts,
                check_logs=False,
            )
        )
    except Exception as e:
        log.warning(f"Crash status check failed: {e}")
        snapshot["crash_found"] = None

    log_scan = None
    try:
        log_scan = rados_obj.scan_daemon_logs_for_crashes(
            start_time=start_time,
            end_time=end_ts,
            daemon_types=["mon", "mgr", "osd", "mds", "nfs", "rgw", "smb"],
        )
        if log_scan.get("crashes_found"):
            snapshot["crash_found"] = True
    except Exception as e:
        log.warning(f"Daemon log crash scan failed: {e}")

    snapshot["crash_details"] = {
        "ceph_crashes": [],
        "log_scan": log_scan,
    }

    return snapshot


def _run_deep_scrub(rados_obj):
    """Trigger deep scrub on all OSDs post-upgrade.

    Uses the existing ``run_deep_scrub()`` from core_workflows.
    Completion is not verified here -- deep scrub runs asynchronously and
    can take a long time depending on data volume.

    TODO: verify completion by checking ``ceph pg <pgid> query`` for
    last_deep_scrub_stamp before and after, on a sample of PGs.
    """
    log.info("Triggering post-upgrade deep scrub on all OSDs")
    try:
        rados_obj.run_deep_scrub()
        log.info("Deep scrub initiated on all OSDs")
    except Exception as e:
        log.warning(f"Deep scrub initiation failed: {e}")


def _run_mclock_functional_test(rados_obj, config):
    """
    Destructive mClock test: mark OSD out, verify client IO
    continues at acceptable rate during recovery.
    """
    features = config.get("features", {}).get("rados", {})
    if not features.get("mclock_balanced", False):
        return

    log.info("Running mClock functional test (destructive)")
    try:
        osd_tree = rados_obj.run_ceph_command(cmd="ceph osd tree")
        if not isinstance(osd_tree, dict):
            return

        osd_ids = [
            n["id"]
            for n in osd_tree.get("nodes", [])
            if n.get("type") == "osd" and n.get("status") == "up"
        ]
        if not osd_ids:
            log.warning("No up OSDs found for mClock test")
            return

        target_osd = osd_ids[0]
        log.info(f"Marking osd.{target_osd} out for mClock test")

        # mutating — do not use run_ceph_command (empty stdout => {})
        rados_obj.node.shell([f"ceph osd out {target_osd}"])
        time.sleep(30)

        rados_obj.node.shell([f"ceph osd in {target_osd}"])
        log.info(f"osd.{target_osd} restored to in state")

        _wait_for_stabilization(rados_obj, 120)
        log.info("mClock functional test complete")

    except Exception as e:
        log.warning(f"mClock functional test failed: {e}")


def _run_post_upgrade_checks(rados_obj, orch_obj, snapshot):
    """Post-upgrade validation checks."""
    log.info("Running post-upgrade checks")

    # Upgrade completion
    try:
        status = orch_obj.upgrade_status()
        if status.get("in_progress", False):
            log.warning("Upgrade still shows in_progress")
        else:
            log.info("Upgrade completion confirmed")
    except Exception as e:
        log.warning(f"Upgrade status check: {e}")

    # Single version check
    versions = snapshot.get("versions")
    if isinstance(versions, dict):
        for dtype, ver_dict in versions.items():
            if isinstance(ver_dict, dict) and len(ver_dict) > 1:
                log.warning(f"Multiple versions for {dtype}: {ver_dict}")

    # No inactive PGs
    pg_stat = snapshot.get("pg_stat")
    if isinstance(pg_stat, dict):
        pg_summary = pg_stat.get("pg_summary", pg_stat)
        by_state = pg_summary.get("num_pg_by_state", [])
        inactive = [s for s in by_state if "active" not in s.get("name", "")]
        if inactive:
            log.warning(f"Inactive PGs detected: {inactive}")
        else:
            log.info(
                f"All {pg_summary.get('num_pgs', '?')} PGs active, "
                f"states: {[s['name'] for s in by_state]}"
            )

    # Crash check
    if snapshot.get("crash_found"):
        log.error("Crashes detected in Phase 6 snapshot")


def _check_for_failures(
    feature_results,
    integrity_results,
    bug_results,
    snapshot,
    pre_upgrade_feature_results=None,
    mount_health=None,
):
    """Check all result sets for failures.

    Only flags feature failures that are **regressions** -- features that
    passed pre-upgrade but fail post-upgrade.  Pre-existing failures are
    logged as warnings but do not count as test failures.

    Integrity ``stale_or_hung`` skips are demoted to warnings when the mount
    was already healthy in ``mount_health``, CRC mismatches are empty, and
    ``total_checked > 0``. ``cluster_not_ready:rep_pool_active+clean_timeout``
    is report-only (RBD verify skipped). Other verification errors are hard
    failures.

    Bug validation ``fail`` results are remapped to ``warn`` and do not fail
    the hop (report signal only).

    Args:
        feature_results: Dict of {feature_name: {"result": str, "details": str}}.
        integrity_results: Dict from verify_all_integrity — mismatches, errors,
            total_checked (incomplete/zero-item cases are in errors).
        bug_results: List of {"id": str, "result": str, "evidence": str} dicts.
        snapshot: State snapshot dict with "crash_found" key.
        pre_upgrade_feature_results: Pre-upgrade feature results for regression check.
        mount_health: Dict from check_mount_health (per-service healthy /
            stale_remounted / stale_unrecoverable / stale lists).

    Returns:
        Tuple of (has_failures: bool, failure_reasons: list[str]).
    """
    failures = []
    pre_results = pre_upgrade_feature_results or {}
    # Skipped-verify signals (reported in integrity.errors / HTML warns).
    _soft_integrity_errors = frozenset(
        {"cluster_not_ready:rep_pool_active+clean_timeout"}
    )

    for name, result in (feature_results or {}).items():
        if result.get("result") != "fail":
            continue
        pre = pre_results.get(name, {})
        if pre.get("result") == "fail":
            log.warning(
                f"Feature '{name}' still failing post-upgrade (pre-existing): "
                f"{result.get('details', '')}"
            )
        else:
            failures.append(f"Feature '{name}' regressed: {result.get('details', '')}")

    if integrity_results:
        mismatches = integrity_results.get("mismatches", [])
        if mismatches:
            failures.append(
                f"Integrity mismatches: {len(mismatches)} items: {mismatches[:5]}"
            )
        # Verification errors (unreachable clients, lost mounts, zero items
        # checked, read failures) are hard failures — warn-only allowed a
        # false PASS when mismatches was empty but nothing was verified.
        # Exception: stale_or_hung on mounts already healthy in mount_health,
        # when some items were verified and there are no CRC mismatches.
        errors = integrity_results.get("errors", []) or []
        healthy_mounts = set()
        for buckets in (mount_health or {}).values():
            if isinstance(buckets, dict):
                healthy_mounts.update(buckets.get("healthy") or [])
        can_demote_stale = (not mismatches) and (
            integrity_results.get("total_checked", 0) or 0
        ) > 0
        hard_errors = []
        for err in errors:
            if str(err) in _soft_integrity_errors:
                log.warning("Integrity soft error (not failing hop): %s", err)
                continue
            parts = str(err).split(":")
            detail = parts[-1] if parts else ""
            mp = parts[1] if len(parts) >= 3 else ""
            if can_demote_stale and detail == "stale_or_hung" and mp in healthy_mounts:
                log.warning(
                    "Integrity stale_or_hung demoted to warn "
                    "(mount healthy, 0 mismatches): %s",
                    err,
                )
            else:
                hard_errors.append(err)
        if hard_errors:
            failures.append(
                "Integrity verification errors: %d items: %s"
                % (len(hard_errors), hard_errors[:5])
            )
        # verification_incomplete / zero_items_checked already land in errors.

    if mount_health:
        unrecoverable = sum(
            len(v.get("stale_unrecoverable", [])) for v in mount_health.values()
        )
        stale_count = sum(len(v.get("stale", [])) for v in mount_health.values())
        if unrecoverable or stale_count:
            failures.append(
                "Unrecoverable/stale mounts: unrecoverable=%d stale=%d"
                % (unrecoverable, stale_count)
            )

    skipped_bugs = []
    warned_bugs = []
    for bug in bug_results or []:
        if bug.get("result") == "skip":
            skipped_bugs.append(bug.get("id", "?"))
            continue
        if bug.get("result") == "fail":
            # Historical bug detectors are report signals, not hop exit criteria.
            bug["result"] = "warn"
            warned_bugs.append(bug.get("id", "?"))
            log.warning(
                "Bug validation %s (warn, not failing hop): %s",
                bug.get("id", "?"),
                (bug.get("evidence", "") or "")[:200],
            )
    if skipped_bugs:
        log.info("Bug validations skipped: %s", skipped_bugs)
    if warned_bugs:
        log.info("Bug validations warned (not hop failures): %s", warned_bugs)

    if snapshot.get("crash_found"):
        failures.append("Crashes detected during upgrade")

    if failures:
        log.error(f"Test failures detected ({len(failures)}):")
        for f in failures:
            log.error(f"  - {f}")
        return True, failures

    return False, []
