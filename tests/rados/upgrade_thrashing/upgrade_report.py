"""
Report generator for the Ceph upgrade thrash test.

Produces structured log tables and an interactive HTML report with Chart.js
charts from data collected by UpgradeStatsCollector and the Phase 6
verification steps.
"""

from __future__ import annotations

import json
import math
import os
import re
from datetime import datetime, timezone

from utility.log import Log

log = Log(__name__)

_LATENCY_PERCENTILES = ("p50", "p95", "p99", "p99_9")
_CEPH_DAEMON_TYPES = {"mon", "mgr", "osd", "mds", "rgw", "crash", "rbd-mirror"}
# Defense-in-depth cap for stale raw JSON (IBMCEPH-17378); matches collector hard cap.
_THROUGHPUT_CHART_CAP_MBPS = 50_000


def _clamp_throughput_bytes(
    read_bytes: float, write_bytes: float
) -> tuple[float, float]:
    """Scale read/write bytes proportionally when total exceeds chart cap."""
    total = read_bytes + write_bytes
    cap_bytes = _THROUGHPUT_CHART_CAP_MBPS * 1024 * 1024
    if total <= cap_bytes:
        return read_bytes, write_bytes
    scale = cap_bytes / total
    return read_bytes * scale, write_bytes * scale


def _sum_pool_io(metrics: dict) -> tuple[float, float, float, float]:
    """Sum read/write ops and bytes across all pools."""
    ro = wo = rb = wb = 0.0
    for pool_data in metrics.values():
        if not isinstance(pool_data, dict):
            continue
        ro += pool_data.get("read_op_per_sec", 0)
        wo += pool_data.get("write_op_per_sec", 0)
        rb += pool_data.get("read_bytes_sec", 0)
        wb += pool_data.get("write_bytes_sec", 0)
    return ro, wo, rb, wb


_MDS_STATE_KEYS = ("active", "standby", "standby_replay")
_MDS_ACTIVITY_KEYS = ("rate", "dns", "inos", "dirs", "caps")


def _parse_mds_fs_name(name: str) -> str | None:
    """Extract filesystem name from an MDS daemon name."""
    parts = name.split(".")
    if parts and parts[0] == "mds":
        parts = parts[1:]
    fs_name = parts[0] if parts else "unknown"
    if fs_name in ("", "mds", "unknown"):
        return None
    return fs_name


def _is_active_mds_rank(mds_entry: dict) -> bool:
    """True for ranked MDS in an active state (excludes standby / standby-replay)."""
    rank = mds_entry.get("rank", -1)
    state = mds_entry.get("state", "")
    if "standby" in state.lower():
        return False
    if isinstance(rank, int):
        if rank < 0:
            return False
    elif isinstance(rank, str):
        if rank.startswith("-") or rank.endswith("-s"):
            return False
    elif rank is None:
        return False
    return "active" in state


def _mds_state_key(mds_entry: dict) -> str:
    """Classify an mdsmap entry for state-count charts."""
    state = mds_entry.get("state", "unknown")
    rank = mds_entry.get("rank", -1)
    if "standby-replay" in state or "standby_replay" in state:
        return "standby_replay"
    if rank == -1 or rank == "-1" or state == "standby":
        return "standby"
    if isinstance(rank, str) and rank.endswith("-s"):
        return "standby_replay"
    if "active" in state:
        return "active"
    return state.replace("-", "_")


def _aggregate_mds_fs_status(metrics: dict) -> dict[str, dict]:
    """Build per-filesystem MDS state counts and summed active-rank activity."""
    per_fs: dict[str, dict] = {}
    for mds_entry in metrics.get("mdsmap", []):
        if not isinstance(mds_entry, dict):
            continue
        fs_name = _parse_mds_fs_name(mds_entry.get("name", ""))
        if not fs_name:
            continue
        state_key = _mds_state_key(mds_entry)
        bucket = per_fs.setdefault(
            fs_name,
            {
                "active": 0,
                "standby": 0,
                "standby_replay": 0,
                "activity": {k: 0 for k in _MDS_ACTIVITY_KEYS},
            },
        )
        if state_key in _MDS_STATE_KEYS:
            bucket[state_key] = bucket.get(state_key, 0) + 1
        if _is_active_mds_rank(mds_entry):
            activity = bucket["activity"]
            for key in _MDS_ACTIVITY_KEYS:
                activity[key] += mds_entry.get(key, 0) or 0
    return per_fs


class UpgradeReportGenerator:
    """Aggregates upgrade test results and emits log tables + HTML report.

    Workflow:
        1. Construct with the suite config dict.
        2. Feed data via ``set_*`` methods as each phase completes.
        3. Call ``generate_log_report()`` for structured text output.
        4. Call ``generate_html_report(path)`` for the interactive HTML file.
        5. Call ``check_performance_regression()`` for WARN-only deltas.
    """

    def __init__(self, config: dict):
        """
        Args:
            config: Full test config dict from the suite YAML.
        """
        self._config = config
        self._stats_data: dict = {}
        self._feature_results: dict = {}
        self._pre_upgrade_feature_results: dict = {}
        self._bug_results: list = []
        self._integrity_results: dict = {}
        self._io_tool_usage: dict = {}
        self._daemon_timeline: list = []
        self._upgrade_events: list = []
        self._cluster_details: dict = {}
        self._test_start_time: str | None = None
        self._health_warnings: dict = {}
        self._mount_health: dict = {}
        self._nfs_grace_analysis: dict = {}
        self._test_outcome: dict | None = None
        self._crash_details: dict = {}
        self._agg_cache: dict[str, dict] = {}
        self._io_index: list[tuple] | None = None
        self._lat_index: list[tuple] | None = None

    # ------------------------------------------------------------------
    # Data setters
    # ------------------------------------------------------------------

    def set_stats_data(self, stats_data: dict) -> None:
        """Receive the complete stats dict from UpgradeStatsCollector.get_all_data()."""
        self._stats_data = stats_data or {}
        self._agg_cache.clear()
        self._io_index = None
        self._lat_index = None

    def set_feature_results(self, feature_results: dict) -> None:
        """Receive post-upgrade feature verification results from Phase 6."""
        self._feature_results = feature_results or {}

    def set_pre_upgrade_feature_results(self, pre_results: dict) -> None:
        """Receive pre-upgrade feature verification results from Phase 2."""
        self._pre_upgrade_feature_results = pre_results or {}

    def set_bug_results(self, bug_results: list) -> None:
        """Receive bug validation results list from Phase 6b."""
        self._bug_results = bug_results or []

    def set_integrity_results(self, integrity_results: dict) -> None:
        """Receive CRC/MD5 verification results from Phase 6."""
        self._integrity_results = integrity_results or {}

    def set_mount_health(self, mount_health: dict) -> None:
        """Receive mount health results from Phase 6 Step 1.5."""
        self._mount_health = mount_health or {}

    def set_nfs_grace_analysis(self, nfs_grace_analysis: dict) -> None:
        """Receive NFS grace cycle analysis from Phase 6 (before bug validations)."""
        self._nfs_grace_analysis = nfs_grace_analysis or {}

    def set_io_tool_usage(self, io_tool_usage: dict) -> None:
        """Receive tool-usage matrix from collect_io_outputs()."""
        self._io_tool_usage = io_tool_usage or {}

    def set_daemon_timeline(self, daemon_timeline_data: list) -> None:
        """Receive per-daemon redeployment timeline from cephadm log parsing.

        Args:
            daemon_timeline_data: List of per-daemon-type entries, each with:
                - daemon_type (str): e.g. "mgr", "mon", "osd"
                - count (int): number of daemons of this type
                - start_time (str): ISO timestamp of first redeploy
                - end_time (str): ISO timestamp of last redeploy completion
                - duration_sec (float): total window duration
                - individual_daemons (list): per-daemon redeploy details
                - io_impact (dict): description and metrics during window
        """
        self._daemon_timeline = daemon_timeline_data or []

    def set_upgrade_events(self, events: list) -> None:
        """Receive operational upgrade events (key rotations, OSD flags, etc.)."""
        self._upgrade_events = events or []

    def set_test_start_time(self, iso_timestamp: str) -> None:
        """Record the actual test start time (Phase 1) for total duration calculation.

        Args:
            iso_timestamp: ISO-8601 timestamp captured at the beginning of the test,
                before Phase 1 setup. This allows the total test duration to reflect
                the full Phase 1 through Phase 7 window rather than just the stats
                collection window (Phases 3-5).
        """
        self._test_start_time = iso_timestamp

    def set_health_warnings(self, health_data: dict) -> None:
        """Receive health warning tracker data for the Health Timeline chart."""
        self._health_warnings = health_data or {}

    def set_test_outcome(self, outcome: dict) -> None:
        """Set the overall test outcome for the report banner."""
        self._test_outcome = outcome

    def set_crash_details(self, crash_details: dict) -> None:
        """Optional crash dump / log-scan details for the HTML report."""
        self._crash_details = crash_details or {}

    def set_cluster_details(self, data: dict) -> None:
        """Receive cluster hardware and daemon placement info for the report."""
        self._cluster_details = data or {}

    # ------------------------------------------------------------------
    # Log report
    # ------------------------------------------------------------------

    def generate_log_report(self) -> None:
        """Print structured summary tables to the cephci log."""
        log.info("=" * 80)
        log.info("UPGRADE THRASH TEST REPORT")
        log.info("=" * 80)

        self._log_daemon_timing_table()
        self._log_performance_comparison()
        self._log_io_tools_used()
        self._log_feature_summary()
        self._log_bug_summary()
        self._log_integrity_summary()
        self._log_mount_health_summary()
        self._log_nfs_grace_summary()

        regression = self.check_performance_regression()
        self._log_regression_summary(regression)

        log.info("=" * 80)
        log.info("END OF REPORT")
        log.info("=" * 80)

    # ------------------------------------------------------------------
    # HTML report
    # ------------------------------------------------------------------

    def generate_html_report(self, output_path: str) -> None:
        """Render the interactive HTML report to *output_path*.

        Reads ``report_template.html`` from the same directory as this module,
        replaces ``DATA_PLACEHOLDER`` with the serialized JSON blob, and writes
        the final self-contained HTML file.
        """
        template_dir = os.path.dirname(os.path.abspath(__file__))
        template_path = os.path.join(template_dir, "report_template.html")

        try:
            with open(template_path, "r") as fh:
                template = fh.read()
        except FileNotFoundError:
            log.error(
                f"Report template not found at {template_path}; "
                "HTML report will not be generated"
            )
            return

        report_data = self._build_report_data()
        report_data = _sanitize_for_json(report_data)
        json_blob = json.dumps(report_data, default=_json_safe_serializer)
        json_blob = json_blob.replace("</", r"<\/")

        html = template.replace("DATA_PLACEHOLDER", json_blob)

        parent_dir = os.path.dirname(output_path)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)

        with open(output_path, "w") as fh:
            fh.write(html)

        log.info(f"HTML report written to {output_path}")

    # ------------------------------------------------------------------
    # Performance regression check
    # ------------------------------------------------------------------

    def check_performance_regression(self) -> dict:
        """Compare post-upgrade vs pre-upgrade metrics (WARN-only).

        Returns a dict with per-metric delta percentages and a ``gate`` key
        that is always ``"pass"`` or ``"warn"`` -- never ``"fail"``.
        """
        thresholds = self._config.get("performance_regression") or {}
        iops_thresh = thresholds.get("iops_drop_threshold_percent", 30)
        lat_thresh = thresholds.get("latency_increase_threshold_percent", 50)
        tp_thresh = thresholds.get("throughput_drop_threshold_percent", 30)

        pre = self._phase_aggregate("baseline")
        post = self._phase_aggregate("post_upgrade")

        result = {
            "iops_delta_pct": _pct_change(pre.get("iops"), post.get("iops")),
            "throughput_delta_pct": _pct_change(
                pre.get("throughput"), post.get("throughput")
            ),
            "gate": "pass",
        }

        for pctl in _LATENCY_PERCENTILES:
            key = f"latency_{pctl}_delta_pct"
            result[key] = _pct_change(
                pre.get(f"latency_{pctl}"),
                post.get(f"latency_{pctl}"),
            )

        warnings: list[str] = []

        iops_delta = result["iops_delta_pct"]
        if iops_delta is not None and iops_delta < -iops_thresh:
            warnings.append(
                f"IOPS dropped {abs(iops_delta):.1f}% " f"(threshold {iops_thresh}%)"
            )

        tp_delta = result["throughput_delta_pct"]
        if tp_delta is not None and tp_delta < -tp_thresh:
            warnings.append(
                f"Throughput dropped {abs(tp_delta):.1f}% " f"(threshold {tp_thresh}%)"
            )

        for pctl in _LATENCY_PERCENTILES:
            key = f"latency_{pctl}_delta_pct"
            delta = result[key]
            if delta is not None and delta > lat_thresh:
                warnings.append(
                    f"Latency {pctl} increased {delta:.1f}% "
                    f"(threshold {lat_thresh}%)"
                )

        if warnings:
            result["gate"] = "warn"
            result["warnings"] = warnings

        return result

    # ------------------------------------------------------------------
    # Private: pre-parsed sample indexes for repeated window queries
    # ------------------------------------------------------------------

    def _ensure_io_index(self) -> None:
        """Lazily pre-parse io_stats and daemon_counters samples into sorted lists.

        Avoids re-iterating all samples for every call to
        ``_get_io_metrics_for_window()`` and ``_build_daemon_impact_table()``.
        """
        if self._io_index is not None:
            return

        io_parsed: list[tuple[datetime, float, float]] = []
        lat_parsed: list[tuple[datetime, float]] = []

        for s in self._stats_data.get("samples", []):
            metrics = s.get("metrics")
            if metrics is None:
                continue
            ts_str = s.get("timestamp", "")
            try:
                ts_dt = datetime.fromisoformat(ts_str)
            except (ValueError, TypeError):
                continue

            collector = s.get("collector", "")
            if collector == "io_stats" and isinstance(metrics, dict):
                total_iops, _, total_read_bw, total_write_bw = _sum_pool_io(metrics)
                read_bw, write_bw = _clamp_throughput_bytes(
                    total_read_bw, total_write_bw
                )
                io_parsed.append((ts_dt, float(total_iops), float(read_bw + write_bw)))
            elif collector == "daemon_counters" and isinstance(metrics, dict):
                osd_data = metrics.get("osd", {})
                lats: list[float] = []
                for counters in osd_data.values():
                    if not isinstance(counters, dict):
                        continue
                    cl = counters.get("txc_commit_lat")
                    if isinstance(cl, dict):
                        avg = cl.get("avgcount", 0)
                        if avg:
                            lats.append(cl.get("sum", 0) / avg * 1000)
                    elif isinstance(cl, (int, float)):
                        lats.append(cl * 1000)
                if lats:
                    lat_parsed.append((ts_dt, max(lats)))

        io_parsed.sort(key=lambda x: x[0])
        lat_parsed.sort(key=lambda x: x[0])
        self._io_index = io_parsed
        self._lat_index = lat_parsed

    # ------------------------------------------------------------------
    # Private: build REPORT_DATA JSON
    # ------------------------------------------------------------------

    def _build_report_data(self) -> dict:
        """Assemble the complete JSON blob for the HTML template."""
        phases = self._build_phases()
        timeseries = self._build_timeseries()
        timeline = self._build_upgrade_timeline()
        regression = self.check_performance_regression()

        integrity_summary = self._build_integrity_summary()
        features_list = self._build_features_list()
        bug_list = self._build_bug_list()

        daemon_impact = self._build_daemon_impact_table()
        meta = self._build_meta(phases, daemon_impact)

        daemon_waterfall = self._build_daemon_upgrade_waterfall()

        upgrade_milestones = self._build_upgrade_milestones()
        phase_windows = self._build_phase_windows()

        return {
            "meta": meta,
            "test_outcome": self._test_outcome,
            "crash_details": self._crash_details,
            "phases": phases,
            "timeseries": timeseries,
            "upgrade_timeline": timeline,
            "upgrade_milestones": upgrade_milestones,
            "phase_windows": phase_windows,
            "daemon_impact": daemon_impact,
            "daemon_upgrade_waterfall": daemon_waterfall,
            "cluster_details": self._cluster_details,
            "results": {
                "data_integrity": integrity_summary,
                "features": features_list,
                "bug_validations": bug_list,
                "performance_regression": regression,
                "io_tools_used": self._io_tool_usage,
                "nfs_grace_analysis": self._build_nfs_grace_summary(),
            },
            "health_warnings": self._health_warnings,
            "phase_boundaries": self._stats_data.get("phase_boundaries", []),
            "config": _sanitize_config(self._config),
        }

    def _build_upgrade_milestones(self) -> list[dict]:
        """Lifecycle milestones for the Upgrade Execution strip.

        Dedupes repeated ``Need to upgrade myself`` lines that the MGR log
        emits a few milliseconds apart for the same daemon.
        """
        milestones: list[dict] = []
        seen_self: set[tuple[str, str]] = set()
        for evt in self._upgrade_events:
            if evt.get("category") != "upgrade_lifecycle":
                continue
            ts = evt.get("timestamp", "")
            label = evt.get("detail", "")
            if evt.get("action") == "self_upgrade":
                # Collapse duplicates within the same second
                key = (label, ts[:19] if ts else "")
                if key in seen_self:
                    continue
                seen_self.add(key)
            milestones.append({"ts": ts, "label": label})
        return milestones

    def _build_phase_windows(self) -> list[dict]:
        """Build orch Started→Complete windows for milestone phase placement.

        Same pairing used when assigning ``phase_idx`` to daemons. Emitted so
        the JS timeline can place milestones by orch cycle instead of by
        (possibly inflated) card end times.
        """
        windows: list[dict] = []
        pending_start = None
        seen: set[tuple[str, str]] = set()
        lifecycle = sorted(
            (
                e
                for e in self._upgrade_events
                if e.get("category") == "upgrade_lifecycle"
                and e.get("action") in ("started", "complete")
            ),
            key=lambda e: e.get("timestamp", ""),
        )
        for evt in lifecycle:
            key = (evt.get("timestamp", ""), evt.get("action", ""))
            if key in seen:
                continue
            seen.add(key)
            if evt.get("action") == "started":
                pending_start = evt.get("timestamp")
            elif evt.get("action") == "complete" and pending_start:
                windows.append(
                    {
                        "idx": len(windows),
                        "start": pending_start,
                        "end": evt.get("timestamp", ""),
                    }
                )
                pending_start = None
        return windows

    def _build_meta(self, phases: dict, daemon_impact: list) -> dict:
        """Assemble report metadata including versions and disruption."""
        pre_ver = _clean_version(self._config.get("pre_version", "unknown"))
        post_ver = _clean_version(self._config.get("post_version", "unknown"))

        total_daemons = self._count_total_daemons()

        upgrade_phase = phases.get("phase_4_upgrade", {})
        upgrade_start = upgrade_phase.get("start", "")
        upgrade_end = upgrade_phase.get("end", "")
        upgrade_duration = _duration_sec(upgrade_start, upgrade_end)

        noop_warning = None
        if upgrade_duration is not None and upgrade_duration < 60:
            noop_warning = (
                f"Upgrade completed in {upgrade_duration:.0f}s -- "
                f"possible no-op (same version?). Verify pre/post versions."
            )

        # Total test duration: from earliest phase start to latest phase end
        total_test_duration = self._compute_total_test_duration_sec(phases)

        max_iops_drop = self._compute_max_iops_drop_pct(daemon_impact)
        max_io_disruption = self._compute_max_io_disruption_sec()

        # Build scale/resource summary from config
        scale = self._config.get("scale", {})
        services = self._config.get("services", {})
        scale_summary = self._build_scale_summary(scale, services, self._config)

        # Merge runtime RGW details from cluster_details if available
        rgw_runtime = self._cluster_details.get("rgw", {})
        if rgw_runtime:
            scale_summary["rgw_daemon_count"] = rgw_runtime.get("daemon_count", 0)
            scale_summary["rgw_hosts"] = rgw_runtime.get("hosts", [])
            scale_summary["rgw_service_names"] = rgw_runtime.get("service_names", [])
            scale_summary["rgw_ports"] = rgw_runtime.get("ports", [])

        # Performance comparison: pre vs post
        try:
            perf_comparison = self._build_performance_comparison()
        except Exception:
            perf_comparison = {}

        upgrade_phases_config = self._config.get("upgrade_phases")
        if upgrade_phases_config:
            staggered_info = {
                "staggered": True,
                "num_phases": len(upgrade_phases_config),
                "phases": [],
            }
            for i, ps in enumerate(upgrade_phases_config):
                phase_info = {"phase": i + 1}
                if ps.get("daemon_types"):
                    dt = ps["daemon_types"]
                    phase_info["daemon_types"] = (
                        ",".join(dt) if isinstance(dt, list) else dt
                    )
                else:
                    phase_info["daemon_types"] = "all remaining"
                if ps.get("cooldown_sec"):
                    phase_info["cooldown_sec"] = ps["cooldown_sec"]
                staggered_info["phases"].append(phase_info)
        else:
            staggered_info = None

        return {
            "pre_version": pre_ver,
            "post_version": post_ver,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "cluster_nodes": self._config.get("cluster_nodes", 0),
            "total_daemons": total_daemons,
            "upgrade_duration_sec": (
                round(upgrade_duration, 1) if upgrade_duration else None
            ),
            "noop_warning": noop_warning,
            "total_test_duration_sec": (
                round(total_test_duration, 1) if total_test_duration else None
            ),
            "max_iops_drop_pct": max_iops_drop,
            "max_io_disruption_sec": max_io_disruption,
            "scale": scale_summary,
            "performance_comparison": perf_comparison,
            "mds_upgrade_strategy": self._build_mds_strategy_summary(),
            "staggered_upgrade": staggered_info,
            "cephx_remediation": self._config.get("cephx_remediation_result"),
        }

    def _build_performance_comparison(self) -> dict:
        """Build pre vs post performance comparison for the summary tile."""
        pre = self._phase_aggregate("baseline")
        post = self._phase_aggregate("post_upgrade")

        pre_iops = pre.get("iops")
        post_iops = post.get("iops")
        iops_change_pct = _pct_change(pre_iops, post_iops)

        return {
            "pre_iops": pre_iops,
            "post_iops": post_iops,
            "iops_change_pct": iops_change_pct,
            "pre_latency_p50": pre.get("latency_p50"),
            "post_latency_p50": post.get("latency_p50"),
            "pre_latency_p95": pre.get("latency_p95"),
            "post_latency_p95": post.get("latency_p95"),
            "pre_latency_p99": pre.get("latency_p99"),
            "post_latency_p99": post.get("latency_p99"),
            "pre_throughput": pre.get("throughput"),
            "post_throughput": post.get("throughput"),
            "throughput_change_pct": _pct_change(
                pre.get("throughput"), post.get("throughput")
            ),
        }

    def _build_mds_strategy_summary(self) -> dict:
        """Build MDS upgrade strategy metadata from mds_config."""
        mds_config = self._config.get("mds_config", {})
        if not mds_config:
            return {}
        summary = {}
        if mds_config.get("fail_fs"):
            summary["fail_fs"] = True
            summary["strategy"] = "fail_fs (simultaneous MDS upgrade)"
        else:
            summary["strategy"] = "rolling (default)"
        cache_limit = mds_config.get("mds_cache_memory_limit")
        if cache_limit:
            summary["mds_cache_memory_limit"] = str(cache_limit)
        return summary

    @staticmethod
    def _build_scale_summary(scale: dict, services: dict, config: dict) -> dict:
        """Build a summary of configured resource counts for the report."""
        rbd = scale.get("rbd", {})
        cephfs = scale.get("cephfs", {})
        rgw = scale.get("rgw", {})
        nfs = scale.get("nfs", {})
        smb = scale.get("smb", {})

        client_count = config.get("client_count", 0)
        io_tools_cfg = config.get("io_tools", {})

        filesystems = cephfs.get("filesystems", ["cephfs1"])
        fs_count = len(filesystems)
        svg_per_fs = cephfs.get("subvolume_groups_per_fs", 0)
        subvols_per_group = cephfs.get("subvolumes_per_group", 0)
        active_mds = cephfs.get("active_mds", 0)
        pin_policy = cephfs.get("pin_policy", "")

        cephfs_mounts_per_client = fs_count if services.get("cephfs") else 0
        cephfs_total_mounts = cephfs_mounts_per_client * client_count
        cephfs_kernel = client_count if fs_count > 0 and services.get("cephfs") else 0
        cephfs_fuse = client_count if fs_count > 0 and services.get("cephfs") else 0
        if fs_count == 1:
            cephfs_kernel = (client_count + 1) // 2
            cephfs_fuse = client_count // 2

        nfs_cluster_count = nfs.get("cluster_count", 0)
        nfs_versions = nfs.get("nfs_versions", [])
        mounts_per_ver = nfs.get("mounts_per_version", 0)
        daemons_per_cluster = nfs.get("daemons_per_cluster", 0)
        nfs_exports = nfs_cluster_count * len(nfs_versions) * mounts_per_ver
        nfs_mounts_per_client = nfs_exports
        nfs_total_mounts = nfs_mounts_per_client * client_count
        nfs_total_daemons = nfs_cluster_count * daemons_per_cluster

        smb_share_count = smb.get("share_count", 0)
        smb_mounts_per_client = 1 if smb_share_count > 0 and services.get("smb") else 0

        rbd_mapped = 2 if rbd.get("image_count", 0) > 0 and services.get("rbd") else 0

        io_tools_by_svc = {}
        for svc, tools in io_tools_cfg.items():
            if isinstance(tools, dict):
                enabled = sorted(t for t, v in tools.items() if v)
                if enabled:
                    io_tools_by_svc[svc] = enabled

        return {
            "client_count": client_count,
            "services_enabled": sorted(svc for svc, en in services.items() if en),
            "cephfs_filesystems": fs_count if services.get("cephfs") else 0,
            "cephfs_filesystem_names": (filesystems if services.get("cephfs") else []),
            "cephfs_active_mds": active_mds,
            # Scale SVGs/subvols are created on the direct FS only
            # (NFS/SMB own their SVGs separately).
            "cephfs_subvolume_groups": svg_per_fs,
            "cephfs_subvolumes": svg_per_fs * subvols_per_group,
            "cephfs_kernel_mounts": cephfs_kernel,
            "cephfs_fuse_mounts": cephfs_fuse,
            "cephfs_total_mounts": cephfs_total_mounts,
            "cephfs_pin_policy": pin_policy,
            "rbd_images": rbd.get("image_count", 0),
            "rbd_image_size": rbd.get("image_size", ""),
            "rbd_mapped_devices": rbd_mapped,
            "rgw_versioned_buckets": rgw.get("versioned_buckets", 0),
            "rgw_non_versioned_buckets": rgw.get("non_versioned_buckets", 0),
            "rgw_total_buckets": (
                rgw.get("versioned_buckets", 0) + rgw.get("non_versioned_buckets", 0)
            ),
            "nfs_clusters": nfs_cluster_count,
            "nfs_total_daemons": nfs_total_daemons,
            "nfs_version_list": [str(v) for v in nfs_versions],
            "nfs_exports": nfs_exports,
            "nfs_mounts_per_client": nfs_mounts_per_client,
            "nfs_total_mounts": nfs_total_mounts,
            "smb_shares": smb_share_count,
            "smb_mounts_per_client": smb_mounts_per_client,
            "io_tools_by_service": io_tools_by_svc,
        }

    def _build_phases(self) -> dict:
        """Extract phase boundaries and per-phase aggregate stats."""
        from upgrade_thrashing.lifecycle_log import resolve_phase_window_from_boundaries

        boundaries = self._stats_data.get("phase_boundaries", [])
        phases: dict = {}

        boundary_map: dict[str, list[str]] = {}
        for b in boundaries:
            name = b.get("name", "")
            ts = b.get("timestamp", "")
            boundary_map.setdefault(name, []).append(ts)

        phase_order = ("baseline", "upgrade", "post_upgrade")
        for phase_name in phase_order:
            start, end = resolve_phase_window_from_boundaries(
                phase_name, boundary_map, phase_order
            )
            agg = self._phase_aggregate(phase_name)

            entry: dict = {"start": start, "end": end, "stats": agg}

            if phase_name == "upgrade":
                entry["daemon_order"] = self._get_daemon_order()

            if phase_name == "baseline":
                key = "phase_3_baseline"
            elif phase_name == "upgrade":
                key = "phase_4_upgrade"
            else:
                key = "phase_5_post"

            phases[key] = entry

        return phases

    def _build_timeseries(self) -> dict:
        """Build time-series arrays from raw samples."""
        iops_ts: list[dict] = []
        latency_ts: list[dict] = []
        throughput_ts: list[dict] = []
        pg_state_ts: list[dict] = []
        osd_latency_ts: list[dict] = []
        versions_ts: list[dict] = []
        health_ts: list[dict] = []
        osd_state_ts: list[dict] = []
        pool_iops_ts: list[dict] = []
        mds_status_ts: list[dict] = []
        cluster_usage_ts: list[dict] = []
        osd_utilization_ts: list[dict] = []
        _old_version: str | None = None

        all_samples = self._stats_data.get("samples", [])
        sorted_samples = sorted(all_samples, key=lambda s: s.get("timestamp", ""))

        # Determine if orch_daemon_versions data is available (preferred source)
        _has_orch_versions = any(
            s.get("collector") == "orch_daemon_versions" for s in sorted_samples
        )

        for sample in sorted_samples:
            ts = sample.get("timestamp", "")
            collector = sample.get("collector", "")
            metrics = sample.get("metrics")
            if metrics is None:
                continue

            if collector == "io_stats" and isinstance(metrics, dict):
                point = {"t": ts}
                total_read_ops, total_write_ops, total_read_bw, total_write_bw = (
                    _sum_pool_io(metrics)
                )
                point["read_iops"] = total_read_ops
                point["write_iops"] = total_write_ops
                point["total_iops"] = total_read_ops + total_write_ops
                iops_ts.append(point)

                tp_point = {"t": ts}
                read_bw, write_bw = _clamp_throughput_bytes(
                    total_read_bw, total_write_bw
                )
                read_mbps = read_bw / (1024 * 1024)
                write_mbps = write_bw / (1024 * 1024)
                tp_point["read_mbps"] = round(read_mbps, 2)
                tp_point["write_mbps"] = round(write_mbps, 2)
                tp_point["total_mbps"] = round(read_mbps + write_mbps, 2)
                throughput_ts.append(tp_point)

                pool_point: dict[str, dict] = {}
                for pname, pdata in metrics.items():
                    if not isinstance(pdata, dict):
                        continue
                    pool_point[pname] = {
                        "read_iops": pdata.get("read_op_per_sec", 0),
                        "write_iops": pdata.get("write_op_per_sec", 0),
                    }
                if pool_point:
                    pool_iops_ts.append({"t": ts, "pools": pool_point})

            elif collector == "daemon_counters" and isinstance(metrics, dict):
                osd_data = metrics.get("osd", {})
                if osd_data:
                    lat_point = {"t": ts}
                    commit_lats = []
                    for osd_name, counters in osd_data.items():
                        if not isinstance(counters, dict):
                            continue
                        cl = counters.get("txc_commit_lat")
                        if isinstance(cl, dict):
                            avg = cl.get("avgcount", 0)
                            if avg:
                                lat_val = cl.get("sum", 0) / avg
                                commit_lats.append(lat_val)
                        elif isinstance(cl, (int, float)):
                            commit_lats.append(cl)
                    if commit_lats:
                        lat_point["avg_commit_lat_ms"] = round(
                            sum(commit_lats) / len(commit_lats) * 1000,
                            3,
                        )
                        latency_ts.append(lat_point)

            elif collector == "pg_stat" and isinstance(metrics, dict):
                # ``ceph pg stat -f json`` nests data under ``pg_summary``
                pg_summary = metrics.get("pg_summary", metrics)
                pg_point = {
                    "t": ts,
                    "num_pgs": pg_summary.get("num_pgs", 0),
                }
                pg_by_state = pg_summary.get("num_pg_by_state", [])
                for st in pg_by_state:
                    if not isinstance(st, dict):
                        continue
                    sname = st.get("name", "")
                    num = st.get("num", 0)
                    if sname:
                        safe_key = sname.replace("+", "_").replace("-", "_")
                        pg_point[safe_key] = pg_point.get(safe_key, 0) + num
                pg_state_ts.append(pg_point)

            elif collector == "system_metrics" and isinstance(metrics, dict):
                osd_perf = metrics.get("osd_perf")
                if isinstance(osd_perf, dict):
                    perf_stat = osd_perf.get("osdstats", osd_perf).get(
                        "osd_perf_infos", []
                    )
                    if perf_stat:
                        rss_point = {"t": ts, "osds": {}}
                        for info in perf_stat:
                            if not isinstance(info, dict):
                                continue
                            oid = info.get("id")
                            stats = info.get("perf_stats", {})
                            rss_point["osds"][f"osd.{oid}"] = {
                                "commit_latency_ms": stats.get("commit_latency_ms", 0),
                                "apply_latency_ms": stats.get("apply_latency_ms", 0),
                            }
                        osd_latency_ts.append(rss_point)

                cluster_df = metrics.get("cluster_df")
                if isinstance(cluster_df, dict):
                    total_bytes = cluster_df.get("total_bytes", 0)
                    used_bytes = cluster_df.get("total_used_bytes", 0)
                    used_pct = (used_bytes / total_bytes * 100) if total_bytes else 0.0
                    cluster_usage_ts.append(
                        {
                            "t": ts,
                            "used_pct": round(used_pct, 2),
                            "total_tb": round(total_bytes / 1e12, 3),
                            "used_tb": round(used_bytes / 1e12, 3),
                        }
                    )

            elif collector == "versions" and isinstance(metrics, dict):
                if not _has_orch_versions:
                    totals_by_ver: dict[str, int] = {}
                    for dtype, daemon_type_vers in metrics.items():
                        if dtype == "overall":
                            continue
                        if not isinstance(daemon_type_vers, dict):
                            continue
                        for ver_str, count in daemon_type_vers.items():
                            if isinstance(count, int):
                                totals_by_ver[ver_str] = (
                                    totals_by_ver.get(ver_str, 0) + count
                                )
                    if _old_version is None and totals_by_ver:
                        _old_version = max(
                            totals_by_ver,
                            key=lambda v: totals_by_ver[v],
                        )
                    old_count = (
                        totals_by_ver.get(_old_version, 0) if _old_version else 0
                    )
                    new_count = (
                        sum(c for v, c in totals_by_ver.items() if v != _old_version)
                        if _old_version
                        else 0
                    )
                    versions_ts.append(
                        {
                            "t": ts,
                            "old_version_count": old_count,
                            "new_version_count": new_count,
                            "total": old_count + new_count,
                        }
                    )

            elif collector == "upgrade_status" and isinstance(metrics, dict):
                if not _has_orch_versions:
                    progress = metrics.get("progress", "")
                    if progress and "/" in progress:
                        try:
                            nums = progress.split("/")
                            upgraded = int(nums[0])
                            total = int(nums[1].split()[0])
                            versions_ts.append(
                                {
                                    "t": ts,
                                    "old_version_count": total - upgraded,
                                    "new_version_count": upgraded,
                                    "total": total,
                                }
                            )
                        except (ValueError, IndexError):
                            pass

            elif collector == "orch_daemon_versions" and isinstance(metrics, list):
                totals_by_ver: dict[str, int] = {}
                for d in metrics:
                    if not isinstance(d, dict):
                        continue
                    dtype = d.get("daemon_type", "")
                    if dtype not in _CEPH_DAEMON_TYPES:
                        continue
                    ver = d.get("version", "")
                    if not ver:
                        continue
                    totals_by_ver[ver] = totals_by_ver.get(ver, 0) + 1
                if not totals_by_ver:
                    continue
                if _old_version is None:
                    _old_version = max(totals_by_ver, key=lambda v: totals_by_ver[v])
                old_count = totals_by_ver.get(_old_version, 0) if _old_version else 0
                new_count = (
                    sum(c for v, c in totals_by_ver.items() if v != _old_version)
                    if _old_version
                    else 0
                )
                versions_ts.append(
                    {
                        "t": ts,
                        "old_version_count": old_count,
                        "new_version_count": new_count,
                        "total": old_count + new_count,
                    }
                )

            elif collector == "health" and isinstance(metrics, dict):
                status = metrics.get("status", "UNKNOWN")
                checks = metrics.get("checks", {})
                check_names = sorted(checks.keys()) if isinstance(checks, dict) else []
                health_ts.append(
                    {
                        "t": ts,
                        "status": status,
                        "check_count": len(check_names),
                        "checks_summary": ", ".join(check_names),
                    }
                )

            elif collector == "osd_tree" and isinstance(metrics, dict):
                nodes = metrics.get("nodes", [])
                up_count = 0
                down_count = 0
                for node in nodes:
                    if not isinstance(node, dict):
                        continue
                    if node.get("type") == "osd":
                        if node.get("status") == "up":
                            up_count += 1
                        else:
                            down_count += 1
                osd_state_ts.append({"t": ts, "up": up_count, "down": down_count})

            elif collector == "osd_perf" and isinstance(metrics, dict):
                perf_stat = metrics.get("osdstats", metrics).get("osd_perf_infos", [])
                if perf_stat:
                    rss_point = {"t": ts, "osds": {}}
                    for info in perf_stat:
                        if not isinstance(info, dict):
                            continue
                        oid = info.get("id")
                        if oid is None:
                            continue
                        stats = info.get("perf_stats", {})
                        rss_point["osds"][f"osd.{oid}"] = {
                            "commit_latency_ms": stats.get("commit_latency_ms", 0),
                            "apply_latency_ms": stats.get("apply_latency_ms", 0),
                        }
                    osd_latency_ts.append(rss_point)

            elif collector == "fs_status" and isinstance(metrics, dict):
                per_fs = _aggregate_mds_fs_status(metrics)
                if not per_fs:
                    continue
                mds_point: dict = {"t": ts, "per_fs": per_fs}
                totals = {k: 0 for k in _MDS_STATE_KEYS}
                for fs_counts in per_fs.values():
                    for key in _MDS_STATE_KEYS:
                        totals[key] += fs_counts.get(key, 0)
                mds_point.update(totals)
                mds_status_ts.append(mds_point)

            elif collector == "osd_utilization" and isinstance(metrics, dict):
                point: dict = {"t": ts}
                for osd_id, vals in metrics.items():
                    if isinstance(vals, dict):
                        point[osd_id] = vals.get("used_pct", 0)
                osd_utilization_ts.append(point)

        return {
            "iops": iops_ts,
            "latency": latency_ts,
            "throughput": throughput_ts,
            "pg_state": pg_state_ts,
            "osd_latency": osd_latency_ts,
            "versions": versions_ts,
            "health": health_ts,
            "osd_state": osd_state_ts,
            "pool_iops": pool_iops_ts,
            "mds_status": mds_status_ts,
            "cluster_usage": cluster_usage_ts,
            "osd_utilization": osd_utilization_ts,
        }

    def _build_upgrade_timeline(self) -> list[dict]:
        """Build one entry per daemon type from the parsed daemon timeline.

        Sources data from ``_daemon_timeline`` (MGR log parsed), ordered
        by ``start_time``.  The ``phase`` field is preserved for the JS
        timeline renderer.  Attaches grouped ``upgrade_events`` as event
        summaries per daemon type.
        """
        if not self._daemon_timeline:
            return []

        # Group upgrade_events by daemon_type for attachment
        events_by_type: dict[str, list[dict]] = {}
        for evt in self._upgrade_events:
            dtype = evt.get("daemon_type", "")
            if dtype:
                events_by_type.setdefault(dtype, []).append(evt)

        timeline: list[dict] = []
        for entry in sorted(
            self._daemon_timeline, key=lambda e: e.get("start_time", "")
        ):
            dtype = entry.get("daemon_type", "")
            count = entry.get("count", len(entry.get("individual_daemons", [])))

            # Summarize events for this daemon type
            raw_events = events_by_type.get(dtype, [])
            event_summaries = self._summarize_events_for_type(raw_events)

            phase_idx = entry.get("phase_idx", -1)
            # JS Upgrade Execution bins by `phase` when set; leave empty
            # only when phase_idx is unknown so the end-time fallback stays
            # available for non-staggered runs.
            phase = f"4.{phase_idx + 1}" if phase_idx >= 0 else ""

            timeline.append(
                {
                    "daemon": dtype,
                    "type": dtype,
                    "host": "",
                    "start": entry.get("start_time", ""),
                    "end": entry.get("end_time", ""),
                    "phase": phase,
                    "phase_idx": phase_idx,
                    "count": count,
                    "is_redeploy": entry.get("is_redeploy", False),
                    "events": event_summaries,
                }
            )

        return timeline

    def _summarize_events_for_type(self, events: list[dict]) -> list[dict]:
        """Collapse raw events into display-friendly summaries grouped by category."""
        if not events:
            return []

        from datetime import datetime as _dt

        def _time_range(evts: list[dict]) -> str:
            times = sorted(e.get("timestamp", "") for e in evts if e.get("timestamp"))
            if len(times) < 2:
                return ""
            try:
                t0 = _dt.fromisoformat(times[0]).strftime("%H:%M")
                t1 = _dt.fromisoformat(times[-1]).strftime("%H:%M")
                return f"{t0}-{t1}" if t0 != t1 else t0
            except (ValueError, TypeError):
                return ""

        by_category: dict[str, list[dict]] = {}
        for evt in events:
            cat = evt.get("category", "unknown")
            by_category.setdefault(cat, []).append(evt)

        summaries: list[dict] = []
        for cat, cat_events in by_category.items():
            if cat == "key_rotation":
                # Prefer rotate/redeploy/delayed for names+range; fall back to all.
                rot_evts = [
                    e
                    for e in cat_events
                    if e.get("action") in ("rotate", "redeploy", "delayed")
                ] or cat_events
                # Unique daemon names from detail ("Rotating/Redeploying X ...").
                names: list[str] = []
                seen_names: set[str] = set()
                for e in rot_evts:
                    detail = e.get("detail") or ""
                    for prefix in (
                        "Rotating keyring for ",
                        "Redeploying ",
                        "Delaying rotation for ",
                    ):
                        if detail.startswith(prefix):
                            name = detail[len(prefix) :].split(" ", 1)[0].strip()
                            if name and name not in seen_names:
                                seen_names.add(name)
                                names.append(name)
                            break
                n_unique = len(names) or len(rot_evts)
                if names and len(names) <= 6:
                    who = ", ".join(names)
                elif names:
                    who = ", ".join(names[:4]) + f", +{len(names) - 4} more"
                else:
                    who = ""
                label = f"Key Rot: {n_unique} daemon{'s' if n_unique != 1 else ''}"
                if who:
                    label += f" ({who})"
                summaries.append(
                    {
                        "category": cat,
                        "summary": label,
                        "range": _time_range(rot_evts),
                        "daemons": names,
                    }
                )
            elif cat == "osd_flag":
                # Dedupe identical set/unset transitions; keep order of first seen.
                seen: set[str] = set()
                for e in cat_events:
                    detail = e.get("detail", "OSD flag")
                    if detail in seen:
                        continue
                    seen.add(detail)
                    summaries.append({"category": cat, "summary": detail})
            elif cat == "fs_event":
                joinable = [e for e in cat_events if e.get("action") == "joinable"]
                completes = [e for e in cat_events if e.get("action") == "mds_complete"]
                if joinable:
                    fs_names = sorted(
                        {
                            (e.get("detail") or "")
                            .removeprefix("FS ")
                            .removesuffix(" joinable")
                            for e in joinable
                            if e.get("detail")
                        }
                    )
                    fs_names = [n for n in fs_names if n]
                    label = (
                        f"{len(fs_names)} FS joinable ({', '.join(fs_names)})"
                        if fs_names
                        else f"{len(joinable)} FS joinable"
                    )
                    summaries.append(
                        {
                            "category": cat,
                            "summary": label,
                            "range": _time_range(joinable),
                        }
                    )
                seen_complete: set[str] = set()
                for e in completes:
                    detail = e.get("detail", "All MDS upgraded")
                    if detail in seen_complete:
                        continue
                    seen_complete.add(detail)
                    summaries.append(
                        {
                            "category": cat,
                            "summary": detail,
                        }
                    )
            elif cat == "safety_check":
                safe_count = sum(1 for e in cat_events if e.get("action") == "safe")
                unsafe_count = sum(1 for e in cat_events if e.get("action") == "unsafe")
                detail = f"{safe_count} safe"
                if unsafe_count:
                    detail += f", {unsafe_count} unsafe waits"
                summaries.append({"category": cat, "summary": detail})
            elif cat == "upgrade_lifecycle":
                # Dedupe self-upgrade by entity; keep other lifecycle once each.
                seen_self: set[str] = set()
                seen_other: set[str] = set()
                for e in cat_events:
                    detail = e.get("detail", "")
                    if e.get("action") == "self_upgrade":
                        if detail in seen_self:
                            continue
                        seen_self.add(detail)
                    else:
                        if detail in seen_other:
                            continue
                        seen_other.add(detail)
                    summaries.append({"category": cat, "summary": detail})

        return summaries

    def _build_integrity_summary(self) -> dict:
        """Format integrity results into a normalized summary for the HTML report."""
        total_checked = 0
        mismatch_list: list = []
        error_list: list = []

        if isinstance(self._integrity_results, dict):
            # Flat format: {"total_checked": N, "mismatches": [...], "errors": [...]}
            total_checked = self._integrity_results.get("total_checked", 0)
            raw_mm = self._integrity_results.get("mismatches", [])
            mismatch_list = raw_mm if isinstance(raw_mm, list) else []
            raw_err = self._integrity_results.get("errors", [])
            error_list = raw_err if isinstance(raw_err, list) else []

        total_mismatches = len(mismatch_list)

        mount_health = self._build_mount_health_summary()

        return {
            "total_checked": total_checked,
            "mismatches": total_mismatches,
            "mismatch_details": [str(m)[:200] for m in mismatch_list[:50]],
            "errors": [str(e)[:200] for e in error_list[:50]],
            "result": ("pass" if total_mismatches == 0 else "fail"),
            "mount_health": mount_health,
        }

    def _build_mount_health_summary(self) -> dict:
        """Build a normalized mount health summary from raw Phase 6 data."""
        total = 0
        healthy = 0
        recovered = 0
        unrecoverable = 0
        stale = 0
        details: list[dict] = []

        for svc, buckets in self._mount_health.items():
            if not isinstance(buckets, dict):
                continue
            for mp in buckets.get("healthy", []):
                total += 1
                healthy += 1
            for mp in buckets.get("stale_remounted", []):
                total += 1
                recovered += 1
                details.append({"service": svc, "mount": mp, "status": "recovered"})
            for mp in buckets.get("stale_unrecoverable", []):
                total += 1
                unrecoverable += 1
                details.append(
                    {
                        "service": svc,
                        "mount": mp,
                        "status": "unrecoverable",
                    }
                )
            for mp in buckets.get("stale", []):
                total += 1
                stale += 1
                details.append({"service": svc, "mount": mp, "status": "stale"})

        return {
            "total": total,
            "healthy": healthy,
            "recovered": recovered,
            "unrecoverable": unrecoverable,
            "stale": stale,
            "details": details,
        }

    def _build_nfs_grace_summary(self) -> dict:
        """Normalize nfs_grace_analysis for HTML report and log tables."""
        raw = self._nfs_grace_analysis or {}
        metrics = raw.get("metrics") or {}
        cycles: list[dict] = []
        for daemon, daemon_cycles in (raw.get("cycles_by_daemon") or {}).items():
            for c in daemon_cycles or []:
                cycles.append(
                    {
                        "daemon": c.get("daemon") or daemon,
                        "start": c.get("start", ""),
                        "end": c.get("end") or "",
                        "configured_sec": c.get("configured_sec"),
                        "observed_sec": c.get("observed_sec"),
                    }
                )
        cycles.sort(key=lambda x: x.get("start", ""))
        return {
            "mode": raw.get("mode", "baseline"),
            "metrics": metrics,
            "cycles": cycles,
            "stagger_sec": raw.get("stagger_sec"),
        }

    def _build_features_list(self) -> list[dict]:
        """Merge pre/post feature results into a unified list with regression flags."""
        features: list[dict] = []
        all_keys = set()
        if isinstance(self._feature_results, dict):
            all_keys.update(self._feature_results.keys())
        if isinstance(self._pre_upgrade_feature_results, dict):
            all_keys.update(self._pre_upgrade_feature_results.keys())

        for key in sorted(all_keys):
            parts = key.split(".", 1) if "." in key else ["general", key]
            svc = parts[0]
            fname = parts[1] if len(parts) > 1 else key

            post_data = self._feature_results.get(key, {})
            pre_data = self._pre_upgrade_feature_results.get(key, {})

            entry = {"service": svc, "name": fname}
            if isinstance(post_data, dict):
                entry["verified"] = post_data.get("result", "skip")
                entry["details"] = post_data.get("details", "")
            else:
                entry["verified"] = str(post_data) if post_data else "skip"
                entry["details"] = ""

            if isinstance(pre_data, dict):
                entry["pre_upgrade"] = pre_data.get("result", "skip")
                entry["pre_details"] = pre_data.get("details", "")
            else:
                entry["pre_upgrade"] = str(pre_data) if pre_data else "n/a"
                entry["pre_details"] = ""

            # Derive regression flag: passed pre-upgrade but failed post-upgrade
            entry["regression"] = (
                entry.get("pre_upgrade") == "pass" and entry["verified"] == "fail"
            )
            features.append(entry)
        return features

    def _build_bug_list(self) -> list[dict]:
        """Normalize bug validation results for HTML report rendering."""
        out: list[dict] = []
        for b in self._bug_results:
            if isinstance(b, dict):
                out.append(
                    {
                        "id": b.get("id", ""),
                        "name": b.get("name", ""),
                        "result": b.get("result", "unknown"),
                        "evidence": _truncate(str(b.get("evidence", "")), 500),
                    }
                )
        return out

    def _build_daemon_impact_table(self) -> list[dict]:
        """Compute per-daemon-type upgrade impact on IO."""
        if not self._daemon_timeline:
            return []
        daemon_entries = [
            {
                "daemon_type": e["daemon_type"],
                "start": e.get("start_time", ""),
                "end": e.get("end_time", ""),
                "duration_sec": e.get("duration_sec", 0),
                "is_redeploy": e.get("is_redeploy", False),
                "individual_durations": [
                    float(d["duration_sec"])
                    for d in e.get("individual_daemons", [])
                    if isinstance(d.get("duration_sec"), (int, float))
                ],
            }
            for e in self._daemon_timeline
            if e.get("start_time") and e.get("end_time")
        ]
        if not daemon_entries:
            return []

        self._ensure_io_index()
        io_parsed = self._io_index or []
        lat_parsed = self._lat_index or []

        if not io_parsed:
            return []

        baseline_avg = self._phase_aggregate("baseline").get("iops", 0.0)

        # Aggregate entries per daemon type so avg/max are computed correctly
        # even when a type has multiple start/end pairs.
        # IO window uses type start/end; avg/max duration use per-daemon gaps.
        type_agg: dict[tuple[str, bool], dict] = {}
        for entry in daemon_entries:
            dtype = entry["daemon_type"]
            is_redeploy = entry["is_redeploy"]
            agg_key = (dtype, is_redeploy)
            try:
                d_start = datetime.fromisoformat(entry["start"])
                d_end = datetime.fromisoformat(entry["end"])
            except (ValueError, TypeError):
                continue

            if d_start.tzinfo is None:
                d_start = d_start.replace(tzinfo=timezone.utc)
            if d_end.tzinfo is None:
                d_end = d_end.replace(tzinfo=timezone.utc)

            duration = entry.get("duration_sec") or 0.0

            during_iops = [
                iops
                for t, iops, _bw in io_parsed
                if d_start
                <= (t if t.tzinfo else t.replace(tzinfo=timezone.utc))
                <= d_end
            ]

            during_lats = [
                v
                for t, v in lat_parsed
                if d_start
                <= (t if t.tzinfo else t.replace(tzinfo=timezone.utc))
                <= d_end
            ]
            max_lat = max(during_lats) if during_lats else 0.0

            if agg_key not in type_agg:
                type_agg[agg_key] = {
                    "durations": [],
                    "individual_durations": [],
                    "iops_during": [],
                    "max_lat": 0.0,
                }
            type_agg[agg_key]["durations"].append(duration)
            type_agg[agg_key]["individual_durations"].extend(
                entry["individual_durations"]
            )
            type_agg[agg_key]["iops_during"].extend(during_iops)
            if max_lat > type_agg[agg_key]["max_lat"]:
                type_agg[agg_key]["max_lat"] = max_lat

        results: list[dict] = []
        for (dtype, is_redeploy), agg in type_agg.items():
            count = self._daemon_count_for_type(dtype)
            indiv = agg["individual_durations"]
            if indiv:
                avg_dur = sum(indiv) / len(indiv)
                max_dur = max(indiv)
            else:
                # Fallback when individuals missing (legacy timeline).
                total_dur = sum(agg["durations"])
                avg_dur = (total_dur / count) if count else 0.0
                max_dur = max(agg["durations"]) if agg["durations"] else 0.0
            avg_iops = (
                sum(agg["iops_during"]) / len(agg["iops_during"])
                if agg["iops_during"]
                else 0.0
            )
            change_pct = 0.0
            if baseline_avg > 0:
                change_pct = round((avg_iops - baseline_avg) / baseline_avg * 100, 2)

            results.append(
                {
                    "daemon_type": dtype,
                    "is_redeploy": is_redeploy,
                    "count": count,
                    "avg_duration_sec": round(avg_dur, 1),
                    "max_duration_sec": round(max_dur, 1),
                    "iops_drop_pct": change_pct,
                    "max_latency_spike_ms": round(agg["max_lat"], 3),
                }
            )

        return results

    def _key_rotation_spans(self) -> dict[str, dict]:
        """Per-type key-rotation window from upgrade_events (for Gantt overlay)."""
        by_type: dict[str, list[dict]] = {}
        for evt in self._upgrade_events:
            if evt.get("category") != "key_rotation":
                continue
            if evt.get("action") not in ("rotate", "redeploy", "delayed"):
                continue
            dtype = evt.get("daemon_type") or ""
            if not dtype:
                continue
            by_type.setdefault(dtype, []).append(evt)

        spans: dict[str, dict] = {}
        for dtype, evts in by_type.items():
            timed = sorted(
                (e for e in evts if e.get("timestamp")),
                key=lambda e: e.get("timestamp") or "",
            )
            if not timed:
                continue
            names: list[str] = []
            seen: set[str] = set()
            events: list[dict] = []
            for e in timed:
                detail = e.get("detail") or ""
                name = ""
                for prefix in (
                    "Rotating keyring for ",
                    "Redeploying ",
                    "Delaying rotation for ",
                ):
                    if detail.startswith(prefix):
                        name = detail[len(prefix) :].split(" ", 1)[0].strip()
                        if name and name not in seen:
                            seen.add(name)
                            names.append(name)
                        break
                events.append(
                    {
                        "timestamp": e.get("timestamp"),
                        "action": e.get("action") or "",
                        "daemon": name,
                    }
                )
            spans[dtype] = {
                "start_time": timed[0].get("timestamp"),
                "end_time": timed[-1].get("timestamp"),
                "count": len(names) or len(timed),
                "daemons": names,
                # Per-event markers for the Gantt (avoid first→last envelope bars).
                "events": events,
            }
        return spans

    def _build_daemon_upgrade_waterfall(self) -> list[dict]:
        """Build per-daemon-type waterfall data for the Gantt-style chart.

        Sources directly from ``_daemon_timeline`` (MGR log parsed),
        enriched with IO metrics for each daemon type window.
        """
        if not self._daemon_timeline:
            return []

        key_spans = self._key_rotation_spans()

        enriched: list[dict] = []
        for entry in self._daemon_timeline:
            start = entry.get("start_time", "")
            end = entry.get("end_time", "")
            io_impact = entry.get("io_impact", {})

            if not io_impact.get("iops_during") and start and end:
                io_metrics = self._get_io_metrics_for_window(start, end)
                io_metrics["description"] = io_impact.get("description", "")
                io_impact = io_metrics

            dtype = entry.get("daemon_type", "")
            enriched.append(
                {
                    "daemon_type": dtype,
                    "count": entry.get("count", 0),
                    "start_time": start,
                    "end_time": end,
                    "duration_sec": entry.get("duration_sec", 0),
                    "phase_idx": entry.get("phase_idx", -1),
                    "is_redeploy": entry.get("is_redeploy", False),
                    "individual_daemons": entry.get("individual_daemons", []),
                    "lifecycle_events": entry.get("lifecycle_events", []),
                    "lifecycle_source": entry.get("lifecycle_source", ""),
                    "key_rotation": key_spans.get(dtype),
                    "io_impact": io_impact,
                }
            )

        return enriched

    def _get_io_metrics_for_window(self, start_iso: str, end_iso: str) -> dict:
        """Extract IO metrics (IOPS, throughput) for a given time window.

        Uses the pre-parsed io_index for O(n) scan of sorted tuples instead
        of re-parsing JSON metrics from every raw sample.
        """
        try:
            w_start = datetime.fromisoformat(start_iso)
            w_end = datetime.fromisoformat(end_iso)
        except (ValueError, TypeError):
            return {}

        if w_start.tzinfo is None:
            w_start = w_start.replace(tzinfo=timezone.utc)
        if w_end.tzinfo is None:
            w_end = w_end.replace(tzinfo=timezone.utc)

        self._ensure_io_index()
        io_index = self._io_index or []

        iops_values: list[float] = []
        tp_values: list[float] = []

        for ts_dt, total_iops, total_bw in io_index:
            ts_cmp = ts_dt if ts_dt.tzinfo else ts_dt.replace(tzinfo=timezone.utc)
            if ts_cmp < w_start:
                continue
            if ts_cmp > w_end:
                break
            iops_values.append(total_iops)
            tp_values.append(total_bw / (1024 * 1024))

        if not iops_values:
            return {}

        baseline_iops = self._phase_aggregate("baseline").get("iops", 0)
        avg_iops = sum(iops_values) / len(iops_values)
        min_iops = min(iops_values)
        change_pct = 0.0
        if baseline_iops > 0:
            change_pct = round((avg_iops - baseline_iops) / baseline_iops * 100, 2)

        return {
            "iops_during": {
                "avg": round(avg_iops, 1),
                "min": round(min_iops, 1),
                "max": round(max(iops_values), 1),
                "samples": len(iops_values),
            },
            "throughput_during": {
                "avg_mbps": round(sum(tp_values) / len(tp_values), 2),
                "min_mbps": round(min(tp_values), 2),
                "max_mbps": round(max(tp_values), 2),
            },
            "iops_drop_pct": change_pct,
            "baseline_iops": round(baseline_iops, 1),
        }

    def _count_total_daemons(self) -> int:
        """Compute total daemon count from the best available source.

        Priority order:
          1. daemon_timeline (from cephadm log parsing) - has actual counts
          2. Config scale data - includes all daemon types
          3. Fallback: config scale data
        """
        # Source 1: daemon_timeline has actual per-type counts from cephadm logs
        if self._daemon_timeline:
            from upgrade_thrashing.lifecycle_log import (
                count_daemons_by_type_from_timeline,
            )

            total = sum(
                count_daemons_by_type_from_timeline(self._daemon_timeline).values()
            )
            if total > 0:
                return total

        # Source 2: config scale data (only when core counts are present)
        scale = self._config.get("scale", {})
        services = self._config.get("services", {})
        mon_count = scale.get("mon_count", 0)
        mgr_count = scale.get("mgr_count", 0)
        osd_count = scale.get("osd_count", 0)

        if mon_count or mgr_count or osd_count:
            count = mon_count + mgr_count + osd_count

            mds_hosts = scale.get("mds_hosts", 0)
            mds_per_host = scale.get("mds_count_per_host", 0)
            count += mds_hosts * mds_per_host

            if services.get("rgw"):
                count += scale.get("rgw_count", 0)

            if services.get("nfs"):
                nfs_cfg = scale.get("nfs", self._config.get("nfs", {}))
                count += nfs_cfg.get("cluster_count", 0) * nfs_cfg.get(
                    "daemons_per_cluster", 0
                )

            if services.get("smb"):
                smb_cfg = scale.get("smb", self._config.get("smb", {}))
                count += smb_cfg.get("daemon_count", smb_cfg.get("cluster_count", 0))

            if services.get("nvmeof"):
                nvmeof_cfg = scale.get("nvmeof", self._config.get("nvmeof", {}))
                count += nvmeof_cfg.get(
                    "gateway_count", nvmeof_cfg.get("daemon_count", 0)
                )

            crash_count = mon_count + osd_count + scale.get("mds_hosts", 0)
            count += crash_count

            if count > 0:
                return count

        return 0

    def _daemon_count_for_type(self, dtype: str) -> int:
        """Return the actual number of daemons for a given daemon type.

        Priority:
          1. _daemon_timeline (from cephadm log parsing) - ground truth
          2. Config scale data - static declaration
          3. Fallback: 1
        """
        # Source 1: daemon_timeline (dedupe names across split phase groups)
        if self._daemon_timeline:
            from upgrade_thrashing.lifecycle_log import (
                count_daemons_by_type_from_timeline,
            )

            by_type = count_daemons_by_type_from_timeline(self._daemon_timeline)
            if dtype in by_type and by_type[dtype] > 0:
                return by_type[dtype]

        # Source 2: config scale data
        scale = self._config.get("scale", {})
        services = self._config.get("services", {})
        nfs_cfg = scale.get("nfs", self._config.get("nfs", {}))
        smb_cfg = scale.get("smb", self._config.get("smb", {}))
        nvmeof_cfg = scale.get("nvmeof", self._config.get("nvmeof", {}))
        counts = {
            "mon": scale.get("mon_count", 0),
            "mgr": scale.get("mgr_count", 0),
            "osd": scale.get("osd_count", 0),
            "mds": scale.get("mds_hosts", 0) * scale.get("mds_count_per_host", 0),
            "rgw": scale.get("rgw_count", 0) if services.get("rgw") else 0,
            "nfs": (
                (
                    nfs_cfg.get("cluster_count", 0)
                    * nfs_cfg.get("daemons_per_cluster", 0)
                )
                if services.get("nfs")
                else 0
            ),
            "smb": (
                smb_cfg.get("daemon_count", smb_cfg.get("cluster_count", 0))
                if services.get("smb")
                else 0
            ),
            "nvmeof": (
                nvmeof_cfg.get("gateway_count", nvmeof_cfg.get("daemon_count", 0))
                if services.get("nvmeof")
                else 0
            ),
            "crash": scale.get("mon_count", 0)
            + scale.get("osd_count", 0)
            + scale.get("mds_hosts", 0),
        }
        val = counts.get(dtype, 1)
        if val == 0:
            log.debug("Daemon type %s has 0 count in config, defaulting to 1", dtype)
            return 1
        return val

    def _compute_total_test_duration_sec(self, phases: dict) -> float | None:
        """Compute total test duration from Phase 1 start to report generation.

        When ``set_test_start_time()`` has been called (with the timestamp
        captured at the very start of the test), the duration spans from that
        moment to now (report generation in Phase 7).  This covers the full
        Phase 1 through Phase 7 window.

        Falls back to the stats-collection window (Phase 3 baseline start
        through Phase 5 post-upgrade end) when no explicit test start time
        is available.
        """
        if self._test_start_time:
            try:
                ts = re.sub(r"([+-]\d{2})(\d{2})$", r"\1:\2", self._test_start_time)
                start_dt = datetime.fromisoformat(ts)
                end_dt = datetime.now(timezone.utc)
                if start_dt.tzinfo is None:
                    start_dt = start_dt.replace(tzinfo=timezone.utc)
                return (end_dt - start_dt).total_seconds()
            except (ValueError, TypeError):
                pass

        timestamps: list[str] = []
        for phase_data in phases.values():
            if not isinstance(phase_data, dict):
                continue
            if phase_data.get("start"):
                timestamps.append(phase_data["start"])
            if phase_data.get("end"):
                timestamps.append(phase_data["end"])
        if len(timestamps) < 2:
            return None
        try:
            dts = [datetime.fromisoformat(t) for t in timestamps]
            return (max(dts) - min(dts)).total_seconds()
        except (ValueError, TypeError):
            return None

    def _compute_max_io_disruption_sec(self) -> float | None:
        """Longest continuous IO drop below 50pct of baseline.

        Uses phase_boundaries to filter the pre-parsed io_index to only
        the upgrade window, avoiding a full raw-sample re-scan.

        Returns None when no IO data is available.
        """
        baseline_iops = self._phase_aggregate("baseline").get("iops", 0)
        if not baseline_iops:
            return None

        threshold = baseline_iops * 0.5

        upg_start_dt, upg_end_dt = self._get_upgrade_window()

        self._ensure_io_index()
        io_index = self._io_index or []

        upgrade_io: list[tuple] = []
        for ts_dt, total_iops, _bw in io_index:
            ts_cmp = ts_dt if ts_dt.tzinfo else ts_dt.replace(tzinfo=timezone.utc)
            if upg_start_dt and ts_cmp < upg_start_dt:
                continue
            if upg_end_dt and ts_cmp > upg_end_dt:
                break
            upgrade_io.append((ts_dt, total_iops))

        if not upgrade_io:
            return None

        max_disruption = 0.0
        streak_start: datetime | None = None
        streak_end: datetime | None = None

        for ts_dt, iops in upgrade_io:
            if iops < threshold:
                if streak_start is None:
                    streak_start = ts_dt
                streak_end = ts_dt
            else:
                if streak_start is not None and streak_end is not None:
                    dur = (streak_end - streak_start).total_seconds()
                    max_disruption = max(max_disruption, dur)
                streak_start = None
                streak_end = None

        if streak_start is not None and streak_end is not None:
            dur = (streak_end - streak_start).total_seconds()
            max_disruption = max(max_disruption, dur)

        return round(max_disruption, 1)

    def _get_upgrade_window(self) -> tuple[datetime | None, datetime | None]:
        """Return upgrade phase start/end from canonical phase boundaries."""
        from upgrade_thrashing.lifecycle_log import resolve_phase_window_from_boundaries

        boundary_map: dict[str, list[str]] = {}
        for b in self._stats_data.get("phase_boundaries", []):
            name = b.get("name", "")
            ts = b.get("timestamp", "")
            if name and ts:
                boundary_map.setdefault(name, []).append(ts)

        start_iso, end_iso = resolve_phase_window_from_boundaries(
            "upgrade", boundary_map, ("baseline", "upgrade", "post_upgrade")
        )
        if not start_iso:
            return None, None

        try:
            upg_start_dt = datetime.fromisoformat(start_iso)
            upg_end_dt = datetime.fromisoformat(end_iso) if end_iso else None
        except (ValueError, TypeError):
            return None, None

        if upg_start_dt.tzinfo is None:
            upg_start_dt = upg_start_dt.replace(tzinfo=timezone.utc)
        if upg_end_dt and upg_end_dt.tzinfo is None:
            upg_end_dt = upg_end_dt.replace(tzinfo=timezone.utc)

        return upg_start_dt, upg_end_dt

    _NON_IO_DAEMONS = frozenset(
        {
            "prometheus",
            "grafana",
            "alertmanager",
            "node-exporter",
            "ceph-exporter",
            "loki",
            "promtail",
            "alloy",
            "mgmt-gateway",
            "oauth2-proxy",
        }
    )

    def _compute_max_iops_drop_pct(self, daemon_impact: list) -> float | None:
        """Compute worst IOPS change percentage during upgrade.

        Convention: positive = IOPS increased, negative = IOPS dropped.
        Returns the most negative value (worst drop) from daemon_impact,
        or computes directly from upgrade window IO samples vs baseline.

        Returns None when no IO data is available (distinct from 0.0
        which means zero disruption was observed).
        """
        if daemon_impact:
            io_path = [
                d.get("iops_drop_pct", 0)
                for d in daemon_impact
                if d.get("daemon_type", "") not in self._NON_IO_DAEMONS
            ]
            return min(io_path) if io_path else None

        baseline_iops = self._phase_aggregate("baseline").get("iops", 0)
        if not baseline_iops:
            return None

        upg_start_dt, upg_end_dt = self._get_upgrade_window()
        if not upg_start_dt:
            return 0.0

        self._ensure_io_index()
        io_index = self._io_index or []

        upgrade_iops: list[float] = []
        for ts_dt, total_iops, _bw in io_index:
            ts_cmp = ts_dt if ts_dt.tzinfo else ts_dt.replace(tzinfo=timezone.utc)
            if ts_cmp < upg_start_dt:
                continue
            if upg_end_dt and ts_cmp > upg_end_dt:
                break
            upgrade_iops.append(total_iops)

        if not upgrade_iops:
            return None

        min_iops = min(upgrade_iops)
        change_pct = (min_iops - baseline_iops) / baseline_iops * 100
        return round(change_pct, 2)

    # ------------------------------------------------------------------
    # Private: phase aggregation
    # ------------------------------------------------------------------

    def _phase_aggregate(self, phase_name: str) -> dict:
        """Compute aggregate stats for a phase from raw samples.

        Excludes the first 60s warmup. Returns IOPS, throughput, and
        latency percentile averages. Results are cached until stats data
        is replaced via set_stats_data().

        Falls back to timestamp-based filtering if samples lack phase tags.
        """
        if phase_name in self._agg_cache:
            return self._agg_cache[phase_name]

        samples = self._samples_by_time_window(phase_name)
        if not samples:
            samples = sorted(
                (
                    s
                    for s in self._stats_data.get("samples", [])
                    if s.get("phase") == phase_name
                ),
                key=lambda s: s.get("timestamp", ""),
            )

        if not samples:
            self._agg_cache[phase_name] = {}
            return {}

        timing = self._config.get("phase_timing", {})
        warmup_sec = timing.get("warmup_discard_sec", 60)
        phase_duration = _phase_duration_sec(samples)
        if phase_duration is not None and warmup_sec >= phase_duration:
            warmup_sec = max(0, int(phase_duration * 0.1))
        samples = _discard_warmup(samples, warmup_sec)
        if not samples:
            self._agg_cache[phase_name] = {}
            return {}

        total_iops_list: list[float] = []
        total_tp_list: list[float] = []
        commit_lat_list: list[float] = []

        for s in samples:
            collector = s.get("collector", "")
            metrics = s.get("metrics")
            if metrics is None:
                continue

            if collector == "io_stats" and isinstance(metrics, dict):
                sample_iops, _, read_bw, write_bw = _sum_pool_io(metrics)
                read_bw, write_bw = _clamp_throughput_bytes(read_bw, write_bw)
                total_iops_list.append(sample_iops)
                total_tp_list.append((read_bw + write_bw) / (1024 * 1024))

            elif collector == "daemon_counters" and isinstance(metrics, dict):
                osd_data = metrics.get("osd", {})
                for counters in osd_data.values():
                    if not isinstance(counters, dict):
                        continue
                    cl = counters.get("txc_commit_lat")
                    if isinstance(cl, dict):
                        avg = cl.get("avgcount", 0)
                        if avg:
                            commit_lat_list.append(cl.get("sum", 0) / avg * 1000)
                    elif isinstance(cl, (int, float)):
                        commit_lat_list.append(cl * 1000)

        agg: dict = {}
        if total_iops_list:
            agg["iops"] = round(sum(total_iops_list) / len(total_iops_list), 1)
        if total_tp_list:
            agg["throughput"] = round(sum(total_tp_list) / len(total_tp_list), 2)

        if commit_lat_list:
            sorted_lats = sorted(commit_lat_list)
            n = len(sorted_lats)
            for pctl, frac in [
                ("p50", 0.50),
                ("p95", 0.95),
                ("p99", 0.99),
                ("p99_9", 0.999),
            ]:
                idx = min(int(frac * n), n - 1)
                agg[f"latency_{pctl}"] = round(sorted_lats[idx], 3)

        self._agg_cache[phase_name] = agg
        return agg

    def _samples_by_time_window(self, phase_name: str) -> list[dict]:
        """Filter samples by phase time window when phase tags are missing.

        Derives start/end from phase_boundaries and returns all samples
        within that window regardless of their 'phase' field.
        """
        from upgrade_thrashing.lifecycle_log import resolve_phase_window_from_boundaries

        boundaries = self._stats_data.get("phase_boundaries", [])
        boundary_map: dict[str, list[str]] = {}
        for b in boundaries:
            name = b.get("name", "")
            ts = b.get("timestamp", "")
            if name and ts:
                boundary_map.setdefault(name, []).append(ts)

        phase_order = ("baseline", "upgrade", "post_upgrade")
        start_iso, end_iso = resolve_phase_window_from_boundaries(
            phase_name, boundary_map, phase_order
        )

        if not start_iso:
            return []

        try:
            start_dt = datetime.fromisoformat(start_iso)
            end_dt = datetime.fromisoformat(end_iso) if end_iso else None
        except (ValueError, TypeError):
            return []

        filtered: list[dict] = []
        for s in self._stats_data.get("samples", []):
            ts_str = s.get("timestamp", "")
            if not ts_str:
                continue
            try:
                ts_dt = datetime.fromisoformat(ts_str)
            except (ValueError, TypeError):
                continue
            if ts_dt < start_dt:
                continue
            if end_dt and ts_dt > end_dt:
                continue
            filtered.append(s)

        return sorted(filtered, key=lambda s: s.get("timestamp", ""))

    def _get_daemon_order(self) -> list[str]:
        """Return the observed daemon upgrade order from MGR log timeline."""
        if self._daemon_timeline:
            return [
                e["daemon_type"]
                for e in sorted(
                    self._daemon_timeline,
                    key=lambda e: e.get("start_time", ""),
                )
            ]
        return []

    # ------------------------------------------------------------------
    # Private: log table printers
    # ------------------------------------------------------------------

    def _log_daemon_timing_table(self) -> None:
        """Print per-daemon-type upgrade duration table to the log."""
        log.info("")
        log.info("-" * 72)
        log.info("DAEMON UPGRADE TIMING")
        log.info("-" * 72)

        if not self._daemon_timeline:
            log.info("  No daemon timing data available")
            return

        header = (
            f"{'Daemon Type':<15} {'Duration (s)':>12} " f"{'Start':>26} {'End':>26}"
        )
        log.info(header)
        log.info("-" * 72)
        for e in self._daemon_timeline:
            dur = e.get("duration_sec")
            dur_str = f"{dur:.0f}" if dur is not None else "N/A"
            log.info(
                f"{e.get('daemon_type', '?'):<15} {dur_str:>12} "
                f"{e.get('start_time', 'N/A'):>26} "
                f"{e.get('end_time', 'N/A'):>26}"
            )

    def _log_performance_comparison(self) -> None:
        """Print pre/during/post performance metric comparison table."""
        log.info("")
        log.info("-" * 72)
        log.info("PERFORMANCE COMPARISON")
        log.info("-" * 72)

        pre = self._phase_aggregate("baseline")
        during = self._phase_aggregate("upgrade")
        post = self._phase_aggregate("post_upgrade")

        header = f"{'Metric':<25} {'Pre':>12} {'During':>12} {'Post':>12}"
        log.info(header)
        log.info("-" * 72)

        metrics = [
            ("IOPS", "iops"),
            ("Throughput (MB/s)", "throughput"),
        ]
        for pctl in _LATENCY_PERCENTILES:
            metrics.append((f"Latency {pctl} (ms)", f"latency_{pctl}"))

        for label, key in metrics:
            pre_val = _fmt_val(pre.get(key))
            dur_val = _fmt_val(during.get(key))
            post_val = _fmt_val(post.get(key))
            log.info(f"{label:<25} {pre_val:>12} " f"{dur_val:>12} {post_val:>12}")

    def _log_io_tools_used(self) -> None:
        """Print IO tool usage matrix to the log."""
        log.info("")
        log.info("-" * 72)
        log.info("IO TOOLS USED")
        log.info("-" * 72)

        tools_used = self._io_tool_usage.get("tools_used", [])
        if not tools_used:
            log.info("  No IO tool usage data collected")
            return

        by_client: dict[str, dict[str, list[str]]] = {}
        for entry in tools_used:
            host = entry.get("hostname", "?")
            svc = entry.get("service", "?")
            tool = entry.get("tool", "?")
            by_client.setdefault(host, {}).setdefault(svc, []).append(tool)

        for host, services in sorted(by_client.items()):
            svc_parts = [
                f"{svc}: {', '.join(sorted(set(tools)))}"
                for svc, tools in sorted(services.items())
            ]
            log.info(f"  {host}  |  {' | '.join(svc_parts)}")

        unique_tools = set(e.get("tool", "") for e in tools_used)
        log.info(
            f"  Total: {len(tools_used)} instances, "
            f"{len(unique_tools)} unique tools, "
            f"{len(by_client)} clients"
        )

    def _log_feature_summary(self) -> None:
        """Print feature verification table with pre/post comparison and regressions."""
        log.info("")
        log.info("-" * 90)
        log.info("FEATURE VERIFICATION SUMMARY (Pre-Upgrade vs Post-Upgrade)")
        log.info("-" * 90)

        features = self._build_features_list()
        if not features:
            log.info("  No feature results available")
            return

        header = (
            f"{'Service':<12} {'Feature':<25} "
            f"{'Pre':<8} {'Post':<8} {'Regr?':<6} {'Details':<25}"
        )
        log.info(header)
        log.info("-" * 90)
        for f in features:
            details = _truncate(str(f.get("details", "")), 25)
            regr = "YES" if f.get("regression") else ""
            log.info(
                f"{f['service']:<12} {f['name']:<25} "
                f"{f.get('pre_upgrade', 'n/a'):<8} "
                f"{f['verified']:<8} {regr:<6} {details:<25}"
            )

        regressions = [f for f in features if f.get("regression")]
        if regressions:
            log.info("")
            log.info(f"*** {len(regressions)} REGRESSION(S) DETECTED ***")
            for f in regressions:
                log.info(
                    f"  {f['service']}.{f['name']}: "
                    f"pre=pass -> post=fail: {f.get('details', '')}"
                )

    def _log_bug_summary(self) -> None:
        """Print bug validation results table to the log."""
        log.info("")
        log.info("-" * 72)
        log.info("BUG VALIDATION SUMMARY")
        log.info("-" * 72)

        if not self._bug_results:
            log.info("  No bug validation results available")
            return

        header = f"{'ID':<6} {'Name':<25} {'Result':<10} " f"{'Evidence':<31}"
        log.info(header)
        log.info("-" * 72)
        for b in self._bug_results:
            if not isinstance(b, dict):
                continue
            name = _truncate(str(b.get("name", "")), 25)
            evidence = _truncate(str(b.get("evidence", "")), 31)
            log.info(
                f"{b.get('id', ''):<6} "
                f"{name:<25} "
                f"{b.get('result', ''):<10} "
                f"{evidence:<31}"
            )

    def _log_integrity_summary(self) -> None:
        """Print data integrity verification summary to the log."""
        log.info("")
        log.info("-" * 72)
        log.info("DATA INTEGRITY SUMMARY")
        log.info("-" * 72)

        summary = self._build_integrity_summary()
        if not self._integrity_results:
            log.info("  No integrity results available")
            return

        log.info(f"  Total checked:  {summary.get('total_checked', 0)}")
        log.info(f"  Mismatches:     {summary.get('mismatches', 0)}")
        log.info(f"  Verify warns:   {len(summary.get('errors') or [])}")
        log.info(f"  Result:         {str(summary.get('result', '')).upper()}")

        for m in (summary.get("mismatch_details") or [])[:10]:
            log.error(f"    - {m}")
        for e in (summary.get("errors") or [])[:10]:
            log.warning(f"    verify warn: {e}")

    def _log_mount_health_summary(self) -> None:
        """Print mount health summary to the log."""
        log.info("")
        log.info("-" * 72)
        log.info("MOUNT HEALTH SUMMARY")
        log.info("-" * 72)

        if not self._mount_health:
            log.info("  No mount health data available")
            return

        summary = self._build_mount_health_summary()
        log.info(f"  Total mounts:    {summary['total']}")
        log.info(f"  Healthy:         {summary['healthy']}")
        log.info(f"  Recovered:       {summary['recovered']}")
        log.info(f"  Unrecoverable:   {summary['unrecoverable']}")
        log.info(f"  Stale:           {summary['stale']}")

        for d in summary.get("details", []):
            level = (
                log.error if d["status"] in ("unrecoverable", "stale") else log.warning
            )
            level(f"    {d['service']:<8} {d['mount']:<30} " f"{d['status']}")

    def _log_nfs_grace_summary(self) -> None:
        """Print NFS grace cycle summary to the log."""
        log.info("")
        log.info("-" * 72)
        log.info("NFS GRACE ANALYSIS")
        log.info("-" * 72)

        summary = self._build_nfs_grace_summary()
        metrics = summary.get("metrics") or {}
        if not metrics.get("cycle_count"):
            log.info("  No NFS grace data available")
            return

        log.info(f"  Mode:              {summary.get('mode', 'baseline')}")
        log.info(f"  Grace cycles:      {metrics.get('cycle_count', 0)}")
        log.info(f"  NFS daemons:       {metrics.get('daemon_count', 0)}")
        log.info(
            f"  Max observed:      {metrics.get('max_observed_sec', 0)}s "
            f"(threshold {metrics.get('threshold_sec', 120)}s)"
        )
        if summary.get("stagger_sec") is not None:
            log.info(f"  Stagger (median):  {summary['stagger_sec']}s")
        for item in (metrics.get("missing_grace_end") or [])[:5]:
            log.warning(
                f"  missing grace_end: {item.get('daemon', '?')} "
                f"start={item.get('start')}"
            )
        for item in (metrics.get("over_threshold") or [])[:5]:
            log.warning(
                f"  over threshold: {item.get('daemon', '?')} "
                f"observed={item.get('observed_sec')}s"
            )

    def _log_regression_summary(self, regression: dict) -> None:
        """Print performance regression gate result and warnings to the log."""
        log.info("")
        log.info("-" * 72)
        gate = regression.get("gate", "pass").upper()
        log.info(f"PERFORMANCE REGRESSION CHECK: {gate}")
        log.info("-" * 72)

        for key, val in regression.items():
            if key in ("gate", "warnings"):
                continue
            val_str = f"{val:+.1f}%" if val is not None else "N/A"
            label = key.replace("_", " ").replace("delta pct", "delta")
            log.info(f"  {label:<35} {val_str}")

        for w in regression.get("warnings", []):
            log.warning(f"  WARN: {w}")


# ======================================================================
# Module-level helpers
# ======================================================================


def _clean_version(ver: str) -> str:
    """Strip commit hashes and build tags, keeping version + codename.

    Examples:
        "ceph version 19.2.1-375.el9cp (...) squid (stable)" -> "19.2.1-375 squid"
        "ceph version 20.1.0-221.el9cp (...) tentacle (stable - RelWithDebInfo)"
            -> "20.1.0-221 tentacle"
    """
    if not ver or ver == "unknown":
        return ver
    ver = re.sub(r"^ceph\s+version\s+", "", ver, flags=re.IGNORECASE)
    ver = re.sub(r"\(?[a-f0-9]{7,}\)?", "", ver)
    ver = re.sub(r"\.el\d+cp", "", ver)
    # Remove stability / build-info tags like (stable), (stable - RelWithDebInfo)
    ver = re.sub(r"\(\s*stable\b[^)]*\)", "", ver, flags=re.IGNORECASE)
    ver = re.sub(r"\(\s*development\b[^)]*\)", "", ver, flags=re.IGNORECASE)
    ver = re.sub(r"\(\s*rc\b[^)]*\)", "", ver, flags=re.IGNORECASE)
    ver = re.sub(r"\s{2,}", " ", ver).strip()
    ver = re.sub(r"\(\s*\)", "", ver).strip()
    return ver


def _sanitize_for_json(obj):
    """Recursively replace NaN/Inf floats with None before JSON serialization."""
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize_for_json(v) for v in obj]
    return obj


def _json_safe_serializer(obj):
    """JSON serializer that handles datetime, sets, bytes, and unknown objects."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
    if isinstance(obj, set):
        return list(obj)
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    return str(obj)


def _pct_change(old, new) -> float | None:
    """Percentage change from old to new. Positive means increase."""
    if old is None or new is None:
        return None
    if old == 0:
        return None
    return round(((new - old) / abs(old)) * 100, 2)


def _duration_sec(start_iso: str, end_iso: str) -> float | None:
    """Compute duration in seconds between two ISO timestamps."""
    try:
        s = datetime.fromisoformat(start_iso)
        e = datetime.fromisoformat(end_iso)
        return (e - s).total_seconds()
    except (ValueError, TypeError):
        return None


def _phase_duration_sec(samples: list[dict]) -> float | None:
    """Return duration in seconds between first and last sample timestamps."""
    if len(samples) < 2:
        return None
    try:
        first = datetime.fromisoformat(samples[0].get("timestamp", ""))
        last = datetime.fromisoformat(samples[-1].get("timestamp", ""))
        return (last - first).total_seconds()
    except (ValueError, TypeError):
        return None


def _discard_warmup(samples: list[dict], warmup_sec: int) -> list[dict]:
    """Remove samples within the first warmup_sec of the phase.

    Samples are chronologically ordered, so we find the cutoff index and
    return a slice rather than building a new list.
    """
    if not samples or warmup_sec <= 0:
        return samples

    try:
        first_ts = datetime.fromisoformat(samples[0].get("timestamp", ""))
    except (ValueError, TypeError):
        return samples

    for i, s in enumerate(samples):
        try:
            ts = datetime.fromisoformat(s.get("timestamp", ""))
            if (ts - first_ts).total_seconds() >= warmup_sec:
                return samples[i:]
        except (ValueError, TypeError):
            continue
    return []


def _fmt_val(val) -> str:
    """Format a numeric value for table display."""
    if val is None:
        return "N/A"
    if isinstance(val, float):
        return f"{val:.2f}"
    return str(val)


def _truncate(text: str, max_len: int) -> str:
    """Truncate text with ellipsis if exceeding max_len."""
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def _sanitize_config(config: dict) -> dict:
    """Strip sensitive values from config before embedding in JSON."""
    sanitized = {}
    sensitive = {"password", "secret", "token", "key", "credential"}
    for k, v in config.items():
        if any(s in k.lower() for s in sensitive):
            sanitized[k] = "***"
        elif isinstance(v, dict):
            sanitized[k] = _sanitize_config(v)
        elif isinstance(v, list):
            sanitized[k] = _sanitize_list(v)
        else:
            sanitized[k] = v
    return sanitized


def _sanitize_list(items: list) -> list:
    """Recursively sanitize list elements that may contain dicts."""
    result = []
    for item in items:
        if isinstance(item, dict):
            result.append(_sanitize_config(item))
        elif isinstance(item, list):
            result.append(_sanitize_list(item))
        else:
            result.append(item)
    return result
