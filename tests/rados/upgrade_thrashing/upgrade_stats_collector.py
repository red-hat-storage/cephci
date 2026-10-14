"""
Stats collection engine for the Ceph upgrade thrash test.

Architecture:
    - During the test: lightweight phase/daemon boundary tagging + sparse
      CLI checkpoints (health, versions, fs_status) at key moments.
    - At report time: one-shot Prometheus ``query_range`` backfill for dense
      timeseries data (IOPS, throughput, latency, PG state, OSD state, etc.).
    - If Prometheus is unavailable: CLI checkpoint data provides sparse but
      functional coverage for all charts.

    Total SSH calls during test: ~12 (vs thousands with old approach).
    Total Prometheus HTTP calls at report time: ~10 (< 10s total).
"""

from __future__ import annotations

import json
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

from utility.log import Log

log = Log(__name__)

# Impossible cluster throughput from Prometheus rate() counter-reset artifacts.
_HARD_CAP_BYTES = 50_000 * 1024 * 1024  # 50 GB/s

_CANONICAL_PHASES = frozenset({"baseline", "upgrade", "post_upgrade", "complete"})


def _cluster_io_totals(metrics: dict) -> tuple[float, float]:
    """Return (total_iops, total_bytes_sec) summed across all pools."""
    total_iops = 0.0
    total_bw = 0.0
    for pool_vals in metrics.values():
        if not isinstance(pool_vals, dict):
            continue
        total_iops += pool_vals.get("read_op_per_sec", 0) + pool_vals.get(
            "write_op_per_sec", 0
        )
        total_bw += pool_vals.get("read_bytes_sec", 0) + pool_vals.get(
            "write_bytes_sec", 0
        )
    return total_iops, total_bw


def _is_counter_reset_spike(sample: dict) -> bool:
    """True only for impossible rate() artifacts, not real GB/s load."""
    if sample.get("source") == "cli":
        return False
    metrics = sample.get("metrics") or {}
    total_bw = 0.0
    for pool_vals in metrics.values():
        if not isinstance(pool_vals, dict):
            continue
        for field in ("read_bytes_sec", "write_bytes_sec"):
            v = pool_vals.get(field, 0)
            if v < 0 or v != v:  # NaN
                return True
        total_bw += pool_vals.get("read_bytes_sec", 0) + pool_vals.get(
            "write_bytes_sec", 0
        )
    return total_bw > _HARD_CAP_BYTES


# ---------------------------------------------------------------------------
# Prometheus HTTP API client
# ---------------------------------------------------------------------------


class _PrometheusClient:
    """Thin wrapper around the Prometheus HTTP Query API."""

    _DEFAULT_PORT = 9095
    _CONNECT_TIMEOUT = 10
    _QUERY_TIMEOUT = 30

    def __init__(self, rados_obj):
        self._rados_obj = rados_obj
        self._base_url: str | None = None
        self._ssl_ctx = None

    # -- discovery ----------------------------------------------------------

    def discover(self) -> bool:
        """Locate Prometheus via ``ceph orch ps`` and verify reachability."""
        log.debug("Prometheus discovery: starting via ceph orch ps")
        try:
            daemons = self._rados_obj.run_ceph_command(
                cmd="ceph orch ps --daemon-type prometheus"
            )
        except Exception as e:
            log.warning(f"Prometheus discovery via orch ps failed: {e}")
            return self._try_installer_fallback()

        if not isinstance(daemons, list) or not daemons:
            log.warning("No prometheus daemons found via ceph orch ps")
            return self._try_installer_fallback()

        log.debug(f"Prometheus discovery: found {len(daemons)} daemon(s)")
        for d in daemons:
            if d.get("status_desc") != "running":
                log.debug(
                    f"  skipping {d.get('hostname')}: status={d.get('status_desc')}"
                )
                continue
            hostname = d.get("hostname", "")
            if not hostname:
                continue
            ip = self._resolve_host(hostname)
            log.debug(f"  {hostname} -> resolved IP: {ip}")
            if ip and self._verify(ip):
                return True

        log.warning("Could not reach Prometheus on any discovered host")
        log.debug("Prometheus discovery: trying dashboard config fallback")
        if self._try_dashboard_config():
            return True
        log.debug("Prometheus discovery: trying installer fallback")
        return self._try_installer_fallback()

    def _try_installer_fallback(self) -> bool:
        """Try the installer node directly (Prometheus often co-located)."""
        try:
            installer = self._rados_obj.node.installer
            ip = installer.node.ip_address
            log.debug(f"Installer fallback: trying IP {ip}")
            if ip and self._verify(ip):
                return True
        except Exception as e:
            log.warning(f"Installer fallback failed: {e}")
        return False

    def _try_dashboard_config(self) -> bool:
        """Try Prometheus URL from dashboard config."""
        try:
            out, _ = self._rados_obj.node.installer.exec_command(
                sudo=True,
                cmd="cephadm shell -- ceph config get mgr mgr/dashboard/PROMETHEUS_API_HOST",
                timeout=30,
            )
            prom_url = out.strip()
            log.debug(f"Dashboard config Prometheus URL: {prom_url}")
            if not prom_url:
                return False
            parsed = urllib.parse.urlparse(prom_url)
            host = parsed.hostname
            ip = (
                self._resolve_host(host)
                if host and not host.replace(".", "").isdigit()
                else host
            )
            log.debug(f"Dashboard config: host={host} -> resolved IP={ip}")
            if ip and self._verify(ip):
                return True
        except Exception as e:
            log.warning(f"Dashboard config Prometheus discovery failed: {e}")
        return False

    def discover_by_sweep(self) -> bool:
        """Last resort: try port 9095 on every known cluster node."""
        log.debug("Prometheus discovery: sweeping all cluster node IPs")
        try:
            for node in self._rados_obj.node.cluster.get_nodes():
                ip = getattr(node, "ip_address", None)
                log.debug(f"  sweep: {getattr(node, 'hostname', '?')} -> {ip}")
                if ip and self._verify(ip):
                    return True
        except Exception as e:
            log.warning(f"IP sweep discovery failed: {e}")
        return False

    def _resolve_host(self, hostname: str) -> str | None:
        """Resolve a Ceph hostname to an IP using the framework's node list."""
        try:
            nodes = self._rados_obj.node.cluster.get_nodes()
            for node in nodes:
                if node.hostname == hostname or node.shortname == hostname:
                    log.debug(f"_resolve_host: {hostname} -> {node.ip_address}")
                    return node.ip_address
            if hostname.replace(".", "").isdigit():
                return hostname
            log.debug(
                f"_resolve_host: {hostname} not found in "
                f"{[n.hostname for n in nodes]}"
            )
        except Exception as e:
            log.warning(f"_resolve_host({hostname}) failed: {e}")
        return None

    def _verify(self, ip: str) -> bool:
        """Check that Prometheus is reachable at the given IP."""
        for scheme in ("https", "http"):
            url = f"{scheme}://{ip}:{self._DEFAULT_PORT}/api/v1/status/config"
            try:
                ctx = None
                if scheme == "https":
                    import ssl

                    ctx = ssl.create_default_context()
                    ctx.check_hostname = False
                    ctx.verify_mode = ssl.CERT_NONE
                req = urllib.request.Request(url, method="GET")
                with urllib.request.urlopen(
                    req, timeout=self._CONNECT_TIMEOUT, context=ctx
                ) as resp:
                    data = json.loads(resp.read())
                if data.get("status") == "success":
                    self._base_url = f"{scheme}://{ip}:{self._DEFAULT_PORT}"
                    self._ssl_ctx = ctx
                    log.info(f"Prometheus discovered at {self._base_url}")
                    return True
            except Exception as e:
                log.warning(
                    f"Prometheus not reachable at {scheme}://{ip}:"
                    f"{self._DEFAULT_PORT}: {e}"
                )
        return False

    # -- queries ------------------------------------------------------------

    def query_range(
        self, promql: str, start: float, end: float, step: str = "10s"
    ) -> list[dict]:
        """Execute a Prometheus range query.

        Returns the ``result`` list from the API response, or [] on failure.
        Each element has ``metric`` (labels) and ``values`` (list of [ts, val]).
        """
        if not self._base_url:
            return []
        params = urllib.parse.urlencode(
            {"query": promql, "start": start, "end": end, "step": step}
        )
        url = f"{self._base_url}/api/v1/query_range?{params}"
        try:
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(
                req, timeout=self._QUERY_TIMEOUT, context=self._ssl_ctx
            ) as resp:
                data = json.loads(resp.read())
            if data.get("status") == "success":
                return data.get("data", {}).get("result", [])
            log.warning(f"Prometheus query failed: {data}")
        except Exception as e:
            log.warning(f"Prometheus query_range error for {promql[:80]}: {e}")
        return []

    def query_instant(self, promql: str) -> list[dict]:
        """Execute a Prometheus instant query."""
        if not self._base_url:
            return []
        params = urllib.parse.urlencode({"query": promql})
        url = f"{self._base_url}/api/v1/query?{params}"
        try:
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(
                req, timeout=self._QUERY_TIMEOUT, context=self._ssl_ctx
            ) as resp:
                data = json.loads(resp.read())
            if data.get("status") == "success":
                return data.get("data", {}).get("result", [])
        except Exception as e:
            log.warning(f"Prometheus instant query error: {e}")
        return []

    def get_pool_name_map(self) -> dict[str, str]:
        """Build pool_id -> pool_name mapping from ceph_pool_metadata."""
        results = self.query_instant("ceph_pool_metadata")
        mapping: dict[str, str] = {}
        for r in results:
            pid = r.get("metric", {}).get("pool_id", "")
            name = r.get("metric", {}).get("name", "")
            if pid and name:
                mapping[pid] = name
        return mapping


# ---------------------------------------------------------------------------
# Stats collector
# ---------------------------------------------------------------------------


class UpgradeStatsCollector:
    """Stats collection engine for upgrade monitoring.

    Lifecycle::

        begin_phase("baseline")
        [test runs... cli_checkpoint / tag_phase_boundary]
        begin_phase("upgrade")
        [_monitor_upgrade calls record_upgrade_status every 3s]
        begin_phase("post_upgrade")
        [post-upgrade sleep]
        begin_phase("complete")
        finalize(start_ts, end_ts)  # Prometheus backfill
        get_all_data()  # -> report

    Data Structure (self._data)::

        {
            "samples": [
                {"timestamp": str, "phase": str,
                 "daemon_upgrading": str|None,
                 "collector": str, "metrics": dict|list|None}
            ],
            "phase_boundaries": [{"name": str, "timestamp": str}],
        }
    """

    def __init__(self, rados_obj, config: dict):
        self.rados_obj = rados_obj
        self.config = config
        self._step = str(config.get("prometheus_step_sec", 10))

        self._data: dict = {
            "samples": [],
            "phase_boundaries": [],
        }
        self._lock = threading.Lock()
        self._current_phase: str | None = None
        self._finalized = False
        self._prom_client = _PrometheusClient(rados_obj)
        self._prom_url_cached = False
        try:
            if self._prom_client.discover():
                self._prom_url_cached = True
                log.info(f"Prometheus pre-discovered: {self._prom_client._base_url}")
        except Exception:
            log.warning("Early Prometheus discovery failed, will retry at finalize")

    # -----------------------------------------------------------------
    # Public API -- phase management
    # -----------------------------------------------------------------

    def begin_phase(self, phase_name: str) -> None:
        """Transition to a new test phase.

        Tags a phase boundary and takes a CLI checkpoint.
        """
        self._current_phase = phase_name
        self.tag_phase_boundary(phase_name, datetime.now(timezone.utc).isoformat())
        self.cli_checkpoint(f"phase_{phase_name}")
        log.info(f"Stats collector: phase '{phase_name}' started")

    # -----------------------------------------------------------------
    # Public API -- data access
    # -----------------------------------------------------------------

    def get_all_data(self) -> dict:
        """Return the complete data structure for report generation."""
        with self._lock:
            return {
                "samples": list(self._data["samples"]),
                "phase_boundaries": list(self._data["phase_boundaries"]),
            }

    # -----------------------------------------------------------------
    # Public API -- tagging (called during test)
    # -----------------------------------------------------------------

    def tag_phase_boundary(self, name: str, timestamp: str) -> None:
        """Mark a phase boundary event (e.g. 'upgrade_start')."""
        with self._lock:
            self._data["phase_boundaries"].append(
                {"name": name, "timestamp": timestamp}
            )
        log.debug(f"Phase boundary tagged: {name} at {timestamp}")

    # -----------------------------------------------------------------
    # Public API -- CLI checkpoint (sparse data during test)
    # -----------------------------------------------------------------

    def cli_checkpoint(self, label: str = "checkpoint") -> None:
        """Capture a sparse CLI snapshot at a key test moment.

        Collects health, versions, and fs_status. Each is stored as a
        separate sample with the correct collector name so that
        ``_build_timeseries()`` and ``_build_monitoring_summary()``
        can consume them directly.
        """
        for collector, cmd in [
            ("health", "ceph health detail"),
            ("versions", "ceph versions"),
            ("fs_status", "ceph fs status"),
            ("orch_daemon_versions", "ceph orch ps"),
        ]:
            try:
                result = self.rados_obj.run_ceph_command(cmd=cmd, timeout=60)
                self._record_sample(collector, result)
            except Exception as e:
                log.debug(f"CLI checkpoint {collector} failed: {e}")

        log.debug(f"CLI checkpoint '{label}' recorded")

    def io_stats_snapshot(self) -> None:
        """Lightweight IO + MDS state snapshot (2 SSH calls per invocation)."""
        try:
            out, _ = self.rados_obj.node.installer.exec_command(
                sudo=True,
                cmd="cephadm shell -- ceph osd pool stats -f json",
                timeout=60,
            )
            io_data = self._parse_io_stats(out)
            if io_data:
                self._record_sample("io_stats", io_data)
        except Exception as e:
            log.debug(f"IO stats snapshot failed: {e}")

        try:
            fs_result = self.rados_obj.run_ceph_command(
                cmd="ceph fs status", timeout=60
            )
            self._record_sample("fs_status", fs_result)
        except Exception as e:
            log.debug(f"MDS status snapshot failed: {e}")

        # OSD latency CLI fallback (for when Prometheus is unavailable)
        try:
            osd_perf = self.rados_obj.run_ceph_command(cmd="ceph osd perf", timeout=60)
            if isinstance(osd_perf, dict):
                self._record_sample("osd_perf", osd_perf)
        except Exception as e:
            log.debug(f"OSD perf snapshot failed: {e}")

        # OSD state CLI fallback (for when Prometheus is unavailable)
        try:
            osd_tree = self.rados_obj.run_ceph_command(cmd="ceph osd tree", timeout=60)
            if isinstance(osd_tree, dict):
                self._record_sample("osd_tree", osd_tree)
        except Exception as e:
            log.debug(f"OSD tree snapshot failed: {e}")

    def orch_versions_snapshot(self) -> None:
        """Collect per-daemon version data from ceph orch ps for version chart."""
        try:
            daemons = self.rados_obj.run_ceph_command(cmd="ceph orch ps", timeout=60)
            if isinstance(daemons, list):
                self._record_sample("orch_daemon_versions", daemons)
        except Exception as e:
            log.debug(f"orch versions snapshot failed: {e}")

    # -----------------------------------------------------------------
    # Public API -- upgrade status recording (fills gap for bug A16)
    # -----------------------------------------------------------------

    def record_upgrade_status(self, status: dict) -> None:
        """Record upgrade status from ``ceph orch upgrade status`` poll.

        Called every 3s in ``_monitor_upgrade()``. Feeds
        ``_build_monitoring_summary()`` -> ``upgrade_status_history``.
        """
        self._record_sample(
            "upgrade_status",
            {
                "message": status.get("message", ""),
                "in_progress": status.get("in_progress", False),
                "services_complete": status.get("services_complete", []),
                "progress": status.get("progress", ""),
                "is_paused": status.get("is_paused", False),
            },
        )

    def record_health_tracker(self, tracker_data: dict) -> None:
        """Store the HealthWarningTracker export as a single sample.

        Called once at the end of _monitor_upgrade() via try/finally.
        """
        self._record_sample("health_tracker", tracker_data)

    # -----------------------------------------------------------------
    # Public API -- Prometheus backfill (called once at report time)
    # -----------------------------------------------------------------

    def finalize(self, start_ts: float, end_ts: float) -> None:
        """One-shot Prometheus backfill for dense timeseries data.

        Args:
            start_ts: Test start time as epoch seconds.
            end_ts:   Current time as epoch seconds.
        """
        if self._finalized:
            log.warning("finalize() called more than once -- skipping")
            return
        self._finalized = True

        prom = self._prom_client
        connected = False

        if self._prom_url_cached and prom._base_url:
            parsed = urllib.parse.urlparse(prom._base_url)
            if parsed.hostname and prom._verify(parsed.hostname):
                connected = True
                log.info(f"Prometheus re-verified at cached URL: {prom._base_url}")

        if not connected:
            delays = [0, 10, 20, 30]
            for attempt, delay in enumerate(delays):
                if delay > 0:
                    log.warning(
                        f"Prometheus discovery retry {attempt}/{len(delays)-1} "
                        f"in {delay}s..."
                    )
                    time.sleep(delay)
                if prom.discover():
                    connected = True
                    break
            if not connected and prom.discover_by_sweep():
                connected = True

        if connected:
            count_before = len(self._data["samples"])
            self._backfill_from_prometheus(prom, start_ts, end_ts)
            count_after = len(self._data["samples"])
            added = count_after - count_before
            log.info(
                f"Prometheus backfill added {added} samples " f"(total: {count_after})"
            )
            io_count = sum(
                1
                for s in self._data["samples"][count_before:]
                if s.get("collector") == "io_stats"
            )
            if io_count == 0:
                log.warning(
                    "Prometheus returned no io_stats data despite being "
                    "reachable. Running CLI fallback for basic chart coverage."
                )
                self._cli_full_snapshot()
        else:
            log.warning(
                "Prometheus unreachable after all retries -- report will use "
                f"CLI checkpoint data only ({len(self._data['samples'])} samples)"
            )
            self._cli_full_snapshot()

        self._filter_upgrade_spikes()

    # -----------------------------------------------------------------
    # Internal -- Prometheus counter-reset spike filter
    # -----------------------------------------------------------------

    def _filter_upgrade_spikes(self) -> None:
        """Drop impossible Prometheus rate() counter-reset spikes in io_stats.

        Only removes samples with cluster throughput above a hard sanity cap
        (or invalid NaN/negative rates). CLI ``ceph osd pool stats`` samples
        are never filtered. Does not median-replace legitimate GB/s load.

        Tracked as IBMCEPH-17378.
        """
        samples = self._data["samples"]
        io_indices = sorted(
            (i for i, s in enumerate(samples) if s.get("collector") == "io_stats"),
            key=lambda i: samples[i]["timestamp"],
        )
        if not io_indices:
            self._log_peak_throughput()
            return

        drop_indices = [
            idx for idx in io_indices if _is_counter_reset_spike(samples[idx])
        ]
        if drop_indices:
            if len(drop_indices) > 0.1 * len(io_indices):
                log.warning(
                    "Dropping %d/%d io_stats samples (>10%%) — high "
                    "counter-reset rate",
                    len(drop_indices),
                    len(io_indices),
                )
            for idx in sorted(drop_indices, reverse=True):
                iops, bw = _cluster_io_totals(samples[idx]["metrics"])
                log.debug(
                    f"_filter_upgrade_spikes: dropping spike at "
                    f"{samples[idx]['timestamp']} totals=({iops}, {bw})"
                )
                del samples[idx]
            log.info("Dropped %d io_stats counter-reset spike(s)", len(drop_indices))
        else:
            log.debug("_filter_upgrade_spikes: no spikes detected")

        self._log_peak_throughput()

    def _log_peak_throughput(self) -> None:
        """Log peak cluster throughput after spike filtering."""
        peak = 0.0
        for s in self._data["samples"]:
            if s.get("collector") != "io_stats":
                continue
            metrics = s.get("metrics") or {}
            if not isinstance(metrics, dict):
                continue
            _, total_bw = _cluster_io_totals(metrics)
            peak = max(peak, total_bw)
        log.info("io_stats peak cluster throughput: %.2f GB/s", peak / 1024**3)
        tier = self.config.get("io_tier", "")
        if tier == "saturation" and peak < 1 * 1024**3:
            log.warning(
                "Peak throughput %.2f GB/s below saturation expectation (~GB/s)",
                peak / 1024**3,
            )

    # -----------------------------------------------------------------
    # Internal -- sample recording
    # -----------------------------------------------------------------

    def _record_sample(self, collector: str, metrics) -> None:
        """Append a sample to the internal data store."""
        sample = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "phase": self._current_phase,
            "daemon_upgrading": None,
            "collector": collector,
            "source": "cli",
            "metrics": metrics,
        }
        with self._lock:
            self._data["samples"].append(sample)

    def _record_backfill_sample(
        self, collector: str, metrics, timestamp: float
    ) -> None:
        """Append a Prometheus-backfilled sample with a specific timestamp."""
        ts_str = datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
        phase = self._resolve_phase(timestamp)
        sample = {
            "timestamp": ts_str,
            "phase": phase,
            "daemon_upgrading": None,
            "collector": collector,
            "source": "prometheus",
            "metrics": metrics,
        }
        with self._lock:
            self._data["samples"].append(sample)

    def _resolve_phase(self, epoch: float) -> str | None:
        """Map a timestamp to a canonical test phase."""
        boundaries = sorted(
            self._data.get("phase_boundaries", []),
            key=lambda b: b.get("timestamp", ""),
        )
        canonical = None
        for b in boundaries:
            try:
                bt = datetime.fromisoformat(b["timestamp"]).timestamp()
            except (ValueError, KeyError):
                continue
            if epoch >= bt:
                name = b["name"]
                if name in _CANONICAL_PHASES:
                    canonical = name
                elif name.startswith("upgrade"):
                    canonical = "upgrade"
            else:
                break
        return canonical

    # -----------------------------------------------------------------
    # Internal -- Prometheus backfill
    # -----------------------------------------------------------------

    def _backfill_from_prometheus(
        self, prom: _PrometheusClient, start: float, end: float
    ) -> None:
        """Query all metric types from Prometheus and create samples."""
        step = f"{self._step}s"

        pool_names = prom.get_pool_name_map()
        log.info(f"Pool name map: {pool_names}")

        self._backfill_io_stats(prom, start, end, step, pool_names)
        self._backfill_pg_stat(prom, start, end, step)
        self._backfill_system_metrics(prom, start, end, step)
        self._backfill_osd_state(prom, start, end, step)
        self._backfill_daemon_counters(prom, start, end, step)
        self._backfill_osd_utilization(prom, start, end, step)
        # MDS status: CLI fs_status only (io_stats_snapshot ~15s during upgrade).
        # Prom ceph_mds_metadata has rank but not state — would mislabel
        # standby-replay as active.
        self._backfill_health(prom, start, end, step)

    def _backfill_io_stats(self, prom, start, end, step, pool_names):
        """Backfill io_stats (per-pool IOPS and throughput)."""
        metrics_map = {
            "read_op_per_sec": "rate(ceph_pool_rd[30s])",
            "write_op_per_sec": "rate(ceph_pool_wr[30s])",
            "read_bytes_sec": "rate(ceph_pool_rd_bytes[30s])",
            "write_bytes_sec": "rate(ceph_pool_wr_bytes[30s])",
        }

        # Collect all per-pool timeseries keyed by (timestamp, pool_id)
        pool_data: dict[float, dict[str, dict]] = {}

        for field, promql in metrics_map.items():
            results = prom.query_range(promql, start, end, step)
            for series in results:
                pool_id = series.get("metric", {}).get("pool_id", "")
                pool_name = pool_names.get(pool_id, f"pool_{pool_id}")
                for ts_val in series.get("values", []):
                    ts = float(ts_val[0])
                    val = float(ts_val[1]) if ts_val[1] != "NaN" else 0.0
                    pool_data.setdefault(ts, {}).setdefault(pool_name, {})
                    pool_data[ts][pool_name][field] = val

        count = 0
        for ts in sorted(pool_data):
            pools = pool_data[ts]
            # Fill missing fields with 0
            io_sample: dict[str, dict] = {}
            for pname, pvals in pools.items():
                io_sample[pname] = {
                    "read_bytes_sec": pvals.get("read_bytes_sec", 0),
                    "write_bytes_sec": pvals.get("write_bytes_sec", 0),
                    "read_op_per_sec": pvals.get("read_op_per_sec", 0),
                    "write_op_per_sec": pvals.get("write_op_per_sec", 0),
                }
            self._record_backfill_sample("io_stats", io_sample, ts)
            count += 1

        log.info(f"  io_stats: {count} samples backfilled")

    def _backfill_pg_stat(self, prom, start, end, step):
        """Backfill pg_stat (all PG state counts from Prometheus)."""
        queries = {
            "total": "sum(ceph_pg_total)",
            "active": "sum(ceph_pg_active)",
            "clean": "sum(ceph_pg_clean)",
            "degraded": "sum(ceph_pg_degraded)",
            "recovering": "sum(ceph_pg_recovering)",
            "undersized": "sum(ceph_pg_undersized)",
            "remapped": "sum(ceph_pg_remapped)",
            "backfilling": "sum(ceph_pg_backfilling)",
            "backfill_wait": "sum(ceph_pg_backfill_wait)",
            "peering": "sum(ceph_pg_peering)",
            "stale": "sum(ceph_pg_stale)",
            "creating": "sum(ceph_pg_creating)",
            "recovery_wait": "sum(ceph_pg_recovery_wait)",
            "scrubbing": "sum(ceph_pg_scrubbing)",
            "deep": "sum(ceph_pg_deep)",
            "snaptrim": "sum(ceph_pg_snaptrim)",
            "snaptrim_wait": "sum(ceph_pg_snaptrim_wait)",
            "repair": "sum(ceph_pg_repair)",
            "failed_repair": "sum(ceph_pg_failed_repair)",
            "down": "sum(ceph_pg_down)",
            "incomplete": "sum(ceph_pg_incomplete)",
            "inconsistent": "sum(ceph_pg_inconsistent)",
            "unknown": "sum(ceph_pg_unknown)",
            "forced_backfill": "sum(ceph_pg_forced_backfill)",
            "forced_recovery": "sum(ceph_pg_forced_recovery)",
            "backfill_toofull": "sum(ceph_pg_backfill_toofull)",
            "recovery_toofull": "sum(ceph_pg_recovery_toofull)",
            "wait": "sum(ceph_pg_wait)",
            "laggy": "sum(ceph_pg_laggy)",
            "activating": "sum(ceph_pg_activating)",
            "peered": "sum(ceph_pg_peered)",
            "snaptrim_error": "sum(ceph_pg_snaptrim_error)",
            "recovery_unfound": "sum(ceph_pg_recovery_unfound)",
            "backfill_unfound": "sum(ceph_pg_backfill_unfound)",
            "premerge": "sum(ceph_pg_premerge)",
        }

        all_ts: dict[float, dict[str, float]] = {}
        for key, promql in queries.items():
            results = prom.query_range(promql, start, end, step)
            if not results:
                continue
            for ts_val in results[0].get("values", []):
                ts = float(ts_val[0])
                val = float(ts_val[1]) if ts_val[1] != "NaN" else 0.0
                all_ts.setdefault(ts, {})[key] = val

        skip_keys = {"total", "active", "clean"}
        count = 0
        for ts in sorted(all_ts):
            vals = all_ts[ts]
            total = int(vals.get("total", 0))
            active_clean = min(int(vals.get("active", 0)), int(vals.get("clean", 0)))
            state_list = [{"name": "active+clean", "num": active_clean}]
            for key, val in vals.items():
                if key in skip_keys:
                    continue
                num = int(val)
                if num > 0:
                    state_list.append({"name": key, "num": num})
            pg_sample = {
                "pg_summary": {
                    "num_pgs": total,
                    "num_pg_by_state": state_list,
                }
            }
            self._record_backfill_sample("pg_stat", pg_sample, ts)
            count += 1

        log.info(f"  pg_stat: {count} samples backfilled")

    def _backfill_system_metrics(self, prom, start, end, step):
        """Backfill system_metrics (per-OSD latency + cluster usage).

        Produces the nested structure that ``_build_timeseries()`` expects:
        ``{"osd_perf": {"osdstats": {"osd_perf_infos": [...]}}, "cluster_df": {...}}``
        """
        # Per-OSD commit latency
        commit_results = prom.query_range(
            "ceph_osd_commit_latency_ms", start, end, step
        )
        # Per-OSD apply latency
        apply_results = prom.query_range("ceph_osd_apply_latency_ms", start, end, step)

        # Cluster usage
        total_results = prom.query_range("ceph_cluster_total_bytes", start, end, step)
        used_results = prom.query_range(
            "ceph_cluster_total_used_raw_bytes", start, end, step
        )

        # Index commit/apply by (timestamp, osd_daemon_name)
        commit_by_ts: dict[float, dict[str, float]] = {}
        for series in commit_results:
            daemon = series.get("metric", {}).get("ceph_daemon", "")
            for ts_val in series.get("values", []):
                ts = float(ts_val[0])
                val = float(ts_val[1]) if ts_val[1] != "NaN" else 0.0
                commit_by_ts.setdefault(ts, {})[daemon] = val

        apply_by_ts: dict[float, dict[str, float]] = {}
        for series in apply_results:
            daemon = series.get("metric", {}).get("ceph_daemon", "")
            for ts_val in series.get("values", []):
                ts = float(ts_val[0])
                val = float(ts_val[1]) if ts_val[1] != "NaN" else 0.0
                apply_by_ts.setdefault(ts, {})[daemon] = val

        # Index cluster usage by timestamp
        total_by_ts: dict[float, float] = {}
        for ts_val in total_results[0].get("values", []) if total_results else []:
            total_by_ts[float(ts_val[0])] = (
                float(ts_val[1]) if ts_val[1] != "NaN" else 0.0
            )
        used_by_ts: dict[float, float] = {}
        for ts_val in used_results[0].get("values", []) if used_results else []:
            used_by_ts[float(ts_val[0])] = (
                float(ts_val[1]) if ts_val[1] != "NaN" else 0.0
            )

        all_timestamps = sorted(set(commit_by_ts) | set(total_by_ts))

        count = 0
        for ts in all_timestamps:
            # Build per-OSD perf info list
            perf_infos = []
            commit_osds = commit_by_ts.get(ts, {})
            apply_osds = apply_by_ts.get(ts, {})
            for daemon in sorted(commit_osds):
                # daemon is like "osd.5"
                try:
                    osd_id = int(daemon.split(".")[1])
                except (IndexError, ValueError):
                    continue
                perf_infos.append(
                    {
                        "id": osd_id,
                        "perf_stats": {
                            "commit_latency_ms": commit_osds.get(daemon, 0),
                            "apply_latency_ms": apply_osds.get(daemon, 0),
                        },
                    }
                )

            total_bytes = total_by_ts.get(ts, 0)
            used_bytes = used_by_ts.get(ts, 0)

            metrics = {
                "osd_perf": {"osdstats": {"osd_perf_infos": perf_infos}},
                "cluster_df": {
                    "total_bytes": total_bytes,
                    "total_used_bytes": used_bytes,
                    "total_avail_bytes": total_bytes - used_bytes,
                },
            }
            self._record_backfill_sample("system_metrics", metrics, ts)
            count += 1

        log.info(f"  system_metrics: {count} samples backfilled")

    def _backfill_osd_state(self, prom, start, end, step):
        """Backfill osd_tree (OSD up/down counts)."""
        results = prom.query_range("ceph_osd_up", start, end, step)
        if not results:
            log.info("  osd_tree: 0 samples (no ceph_osd_up data)")
            return

        # Group by timestamp: count up vs down
        ts_counts: dict[float, dict[str, int]] = {}
        for series in results:
            for ts_val in series.get("values", []):
                ts = float(ts_val[0])
                val = float(ts_val[1]) if ts_val[1] != "NaN" else 0.0
                ts_counts.setdefault(ts, {"up": 0, "down": 0})
                if val == 1.0:
                    ts_counts[ts]["up"] += 1
                else:
                    ts_counts[ts]["down"] += 1

        count = 0
        for ts in sorted(ts_counts):
            counts = ts_counts[ts]
            nodes = []
            for _ in range(counts["up"]):
                nodes.append({"type": "osd", "status": "up"})
            for _ in range(counts["down"]):
                nodes.append({"type": "osd", "status": "down"})
            self._record_backfill_sample("osd_tree", {"nodes": nodes}, ts)
            count += 1

        log.info(f"  osd_tree: {count} samples backfilled")

    def _backfill_osd_utilization(self, prom, start, end, step):
        """Backfill per-OSD utilization from Prometheus.

        Queries ceph_osd_stat_bytes and ceph_osd_stat_bytes_used per OSD,
        computes utilization percentage, and records as osd_utilization samples.
        Uses a larger step (60s) to keep data volume manageable with many OSDs.
        """
        osd_step = "60s"
        stat_results = prom.query_range("ceph_osd_stat_bytes", start, end, osd_step)
        used_results = prom.query_range(
            "ceph_osd_stat_bytes_used", start, end, osd_step
        )
        if not stat_results and not used_results:
            log.info("  osd_utilization: 0 samples (no data)")
            return

        stat_by_ts: dict[float, dict[str, float]] = {}
        for series in stat_results:
            daemon = series.get("metric", {}).get("ceph_daemon", "")
            if not daemon:
                continue
            for ts_val in series.get("values", []):
                ts = float(ts_val[0])
                val = float(ts_val[1]) if ts_val[1] != "NaN" else 0.0
                stat_by_ts.setdefault(ts, {})[daemon] = val

        used_by_ts: dict[float, dict[str, float]] = {}
        for series in used_results:
            daemon = series.get("metric", {}).get("ceph_daemon", "")
            if not daemon:
                continue
            for ts_val in series.get("values", []):
                ts = float(ts_val[0])
                val = float(ts_val[1]) if ts_val[1] != "NaN" else 0.0
                used_by_ts.setdefault(ts, {})[daemon] = val

        all_timestamps = sorted(set(stat_by_ts) | set(used_by_ts))
        count = 0
        for ts in all_timestamps:
            stat_osds = stat_by_ts.get(ts, {})
            used_osds = used_by_ts.get(ts, {})
            osd_metrics: dict[str, dict] = {}
            for daemon in sorted(set(stat_osds) | set(used_osds)):
                total = stat_osds.get(daemon, 0)
                used = used_osds.get(daemon, 0)
                pct = (used / total * 100) if total > 0 else 0.0
                osd_metrics[daemon] = {
                    "used_pct": round(pct, 2),
                    "used_bytes": used,
                    "total_bytes": total,
                }
            if osd_metrics:
                self._record_backfill_sample("osd_utilization", osd_metrics, ts)
                count += 1

        log.info(f"  osd_utilization: {count} samples backfilled")

    def _backfill_daemon_counters(self, prom, start, end, step):
        """Backfill daemon_counters using MGR-exported OSD latency.

        Uses ceph_osd_commit_latency_ms as a proxy for bluestore
        txc_commit_lat since the MGR module covers all OSDs reliably.
        """
        results = prom.query_range("avg(ceph_osd_commit_latency_ms)", start, end, step)
        if not results:
            log.info("  daemon_counters: 0 samples (no latency data)")
            return

        count = 0
        for ts_val in results[0].get("values", []):
            ts = float(ts_val[0])
            val = float(ts_val[1]) if ts_val[1] != "NaN" else 0.0
            # Convert from ms to the format _build_timeseries expects:
            # txc_commit_lat as {sum, avgcount} where avg = sum/avgcount
            val_sec = val / 1000.0
            counters = {
                "osd": {
                    "osd.avg": {
                        "txc_commit_lat": {
                            "sum": val_sec,
                            "avgcount": 1,
                        }
                    }
                },
                "mds": {},
                "rgw": {},
            }
            self._record_backfill_sample("daemon_counters", counters, ts)
            count += 1

        log.info(f"  daemon_counters: {count} samples backfilled")

    def _backfill_health(self, prom, start, end, step):
        """Backfill health status and active checks from Prometheus.

        Uses ceph_health_status (0=OK, 1=WARN, 2=ERR) and
        ceph_health_detail (per-check 0/1 with ``name`` label).
        Note: ceph_health_detail only exposes the fixed subset of
        check codes registered by the MGR Prometheus module (~14).
        The HealthWarningTracker CLI polling captures all codes.
        """
        status_results = prom.query_range("ceph_health_status", start, end, step)
        detail_results = prom.query_range("ceph_health_detail", start, end, step)

        detail_by_ts: dict[float, dict[str, bool]] = {}
        for series in detail_results:
            name = series.get("metric", {}).get("name", "")
            if not name:
                continue
            for ts_val in series.get("values", []):
                ts = float(ts_val[0])
                active = float(ts_val[1]) > 0
                detail_by_ts.setdefault(ts, {})[name] = active

        status_by_ts: dict[float, str] = {}
        if status_results:
            for ts_val in status_results[0].get("values", []):
                ts = float(ts_val[0])
                val = float(ts_val[1]) if ts_val[1] != "NaN" else 0
                status_by_ts[ts] = (
                    "HEALTH_OK"
                    if val == 0
                    else "HEALTH_WARN" if val == 1 else "HEALTH_ERR"
                )

        all_ts = sorted(set(list(status_by_ts.keys()) + list(detail_by_ts.keys())))
        count = 0
        for ts in all_ts:
            status = status_by_ts.get(ts, "UNKNOWN")
            checks = detail_by_ts.get(ts, {})
            active_checks = {k: {} for k, v in checks.items() if v}
            self._record_backfill_sample(
                "health", {"status": status, "checks": active_checks}, ts
            )
            count += 1

        log.info(f"  health: {count} samples backfilled")

    # -----------------------------------------------------------------
    # Internal -- CLI full snapshot (fallback when Prometheus unavailable)
    # -----------------------------------------------------------------

    def _cli_full_snapshot(self) -> None:
        """Take a comprehensive CLI snapshot as last resort."""
        commands = [
            ("health", "ceph health detail"),
            ("versions", "ceph versions"),
            ("fs_status", "ceph fs status"),
        ]

        # Also try io_stats and osd_tree for basic chart data
        try:
            out, _ = self.rados_obj.node.installer.exec_command(
                sudo=True,
                cmd="cephadm shell -- ceph osd pool stats -f json",
                timeout=60,
            )
            io_data = self._parse_io_stats(out)
            if io_data:
                self._record_sample("io_stats", io_data)
        except Exception as e:
            log.debug(f"CLI fallback io_stats failed: {e}")

        try:
            osd_tree = self.rados_obj.run_ceph_command(cmd="ceph osd tree")
            self._record_sample("osd_tree", osd_tree)
        except Exception as e:
            log.debug(f"CLI fallback osd_tree failed: {e}")

        try:
            osd_df = self.rados_obj.run_ceph_command(cmd="ceph osd df", timeout=60)
            if isinstance(osd_df, dict):
                nodes = osd_df.get("nodes", [])
                osd_metrics = {}
                for node in nodes:
                    if not isinstance(node, dict):
                        continue
                    osd_id = node.get("id")
                    if osd_id is None:
                        continue
                    kb = node.get("kb", 0)
                    kb_used = node.get("kb_used", 0)
                    util = node.get("utilization", 0)
                    osd_metrics[f"osd.{osd_id}"] = {
                        "used_pct": round(util, 2),
                        "used_bytes": kb_used * 1024,
                        "total_bytes": kb * 1024,
                    }
                if osd_metrics:
                    self._record_sample("osd_utilization", osd_metrics)
        except Exception as e:
            log.debug(f"CLI fallback osd_utilization failed: {e}")

        # pg_stat fallback
        try:
            pg_out = self.rados_obj.run_ceph_command(cmd="ceph pg stat", timeout=60)
            if isinstance(pg_out, dict):
                self._record_sample("pg_stat", pg_out)
        except Exception as e:
            log.debug(f"CLI fallback pg_stat failed: {e}")

        # system_metrics fallback (OSD perf + cluster df)
        try:
            osd_perf = self.rados_obj.run_ceph_command(cmd="ceph osd perf", timeout=60)
            cluster_df = self.rados_obj.run_ceph_command(cmd="ceph df", timeout=60)
            df_stats = (
                cluster_df.get("stats", {}) if isinstance(cluster_df, dict) else {}
            )
            self._record_sample(
                "system_metrics",
                {
                    "osd_perf": osd_perf if isinstance(osd_perf, dict) else {},
                    "cluster_df": {
                        "total_bytes": df_stats.get("total_bytes", 0),
                        "total_used_bytes": df_stats.get("total_used_raw_bytes", 0),
                        "total_avail_bytes": df_stats.get("total_avail_bytes", 0),
                    },
                },
            )
        except Exception as e:
            log.debug(f"CLI fallback system_metrics failed: {e}")

        for collector, cmd in commands:
            try:
                result = self.rados_obj.run_ceph_command(cmd=cmd, timeout=60)
                self._record_sample(collector, result)
            except Exception as e:
                log.debug(f"CLI fallback {collector} failed: {e}")

        log.info("CLI full snapshot taken as Prometheus fallback")

    # -----------------------------------------------------------------
    # Internal -- parsing helpers (used by CLI fallback)
    # -----------------------------------------------------------------

    @staticmethod
    def _parse_io_stats(raw: str) -> dict | None:
        """Parse ``ceph osd pool stats`` JSON into per-pool IO dict."""
        if not raw or raw.strip() == "null":
            return None
        try:
            out = json.loads(raw)
        except (json.JSONDecodeError, TypeError, ValueError):
            return None
        pools = out
        if isinstance(out, dict):
            pools = out.get("pool_stats", out.get("pools", []))
        if not isinstance(pools, list):
            return None
        pool_io: dict = {}
        for pool in pools:
            if not isinstance(pool, dict):
                continue
            pool_name = pool.get("pool_name", "unknown")
            client_io = pool.get("client_io_rate", {})
            pool_io[pool_name] = {
                "read_bytes_sec": client_io.get("read_bytes_sec", 0),
                "write_bytes_sec": client_io.get("write_bytes_sec", 0),
                "read_op_per_sec": client_io.get("read_op_per_sec", 0),
                "write_op_per_sec": client_io.get("write_op_per_sec", 0),
            }
        return pool_io
