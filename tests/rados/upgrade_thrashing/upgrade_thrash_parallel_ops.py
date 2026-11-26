"""
Customer-like CephFS/NFS admin ops running in parallel during upgrade.

Suite key: ``upgrade_parallel_ops`` (see suite CONFIG REFERENCE).

Behavior
  - Soft-fail only: command / cycle problems are logged (warning) and counted
    for the report — they **never** abort the upgrade hop (including during
    ``fail_fs`` / MDS-down when CephFS cmds are expected to fail).
  - No phase gate: workers start after background IO and run through upgrade
    until teardown (after fail_fs NFS recover when that path is used).
  - Isolated resources: SVG ``par_ops`` (default), seed subvols ``par_svN``,
    stable NFS ``par_nfs_stable``, throwaway ``par_nfs_<id>``, client mounts
    under ``mount_prefix`` (default ``/mnt/par_``). UpgradeIOManager excludes
    that prefix from bg IO / integrity / fill mount discovery.

Concurrency (multi-customer)
  - Every enabled cephfs/nfs flag starts its own daemon thread.
  - Volume-bound ops (snap/clone, snap-schedule, snapshot_visibility,
    export lifecycle, client remount/links/attrs) fan out **one thread
    per seed subvol** (``seed_subvol_count``, default 3) so distinct volumes
    are churned at once.
  - Cluster-scoped ops (SVG churn, NFS cluster create/rm, cluster config)
    stay single-threaded.
"""

from __future__ import annotations

import itertools
import json
import re
import shlex
import threading
import time
import traceback
from typing import Any, Callable, Dict, List, Optional, Tuple

from utility.log import Log

log = Log(__name__)

_DEFAULT_MOUNT_PREFIX = "/mnt/par_"
_STABLE_NFS = "par_nfs_stable"  # underscore — safer for service_id
_DEFAULT_SEED_COUNT = 3  # distinct subvols ≈ distinct customers
_DUR_CAP = 200  # per-worker durations kept for p50/p95
# Per-command rc marker — installer.exit_status is racy across worker threads.
_RC_RE = re.compile(r"__PAR_OPS_RC__=(\d+)\s*$")
# Minute aggregates for phase bucketing (full-hop accurate; no ring wrap).

# worker-name prefix → (category, logical_op, description, suite_flag)
_OP_CATALOG = {
    "svg_lifecycle": (
        "cephfs",
        "svg_lifecycle",
        "Create/resize/pin/rm sibling SVG (never deletes stable SVG)",
        "cephfs.subvolumegroup_lifecycle",
    ),
    "subvol_lifecycle": (
        "cephfs",
        "subvol_lifecycle",
        "Create/info/resize/pin/metadata/rm throwaway subvolume",
        "cephfs.subvolume_lifecycle",
    ),
    "snap_clone": (
        "cephfs",
        "snap_clone",
        "Subvolume snapshot (+ clone when enabled); group snap on first seed",
        "cephfs.subvolume_snapshot|clone|group_snapshot",
    ),
    "snap_schedule": (
        "cephfs",
        "snap_schedule",
        "snap-schedule add/retention/activate on seed subvol",
        "cephfs.snap_schedule",
    ),
    "snap_visibility": (
        "cephfs",
        "snap_visibility",
        "snapshot_visibility set/get on seed subvol",
        "cephfs.snapshot_visibility",
    ),
    "cephfs_readonly": (
        "cephfs",
        "readonly_status",
        "fs ls/status, mds versions/metadata",
        "cephfs.readonly_status",
    ),
    "nfs_cluster": (
        "nfs",
        "cluster_lifecycle",
        "Create/info/rm throwaway NFS cluster",
        "nfs.cluster_lifecycle",
    ),
    "nfs_config": (
        "nfs",
        "cluster_config",
        "NFS cluster config set/get/reset + get-export-default",
        "nfs.cluster_config|export_default",
    ),
    "nfs_export": (
        "nfs",
        "export_lifecycle",
        "Create/ls/info/rm throwaway NFS export on stable cluster",
        "nfs.export_lifecycle",
    ),
    "nfs_remount": (
        "nfs",
        "client_remount_snap",
        "NFS mount/umount seed export + .snap probe",
        "nfs.client_remount_snap",
    ),
    "nfs_links": (
        "nfs",
        "client_links",
        "Soft/hard links same dir + cross-dir; softlink across seed subvols",
        "nfs.client_links",
    ),
    "nfs_attrs": (
        "nfs",
        "client_attrs",
        "NFS chmod/mtime/stat only (user xattrs unsupported over NFS — IBMCEPH-1752)",
        "nfs.client_attrs",
    ),
    "nfs_readonly": (
        "nfs",
        "readonly_status",
        "nfs cluster ls/info",
        "nfs.readonly_status",
    ),
}


def _catalog_for_worker(worker: str) -> Tuple[str, str, str, str]:
    """Return (category, op, target, description) for a worker thread name."""
    # Longest prefix first so nfs_export_par_sv0 never matches a shorter key.
    for prefix, (category, op, desc, _flag) in sorted(
        _OP_CATALOG.items(), key=lambda kv: len(kv[0]), reverse=True
    ):
        if worker == prefix:
            return category, op, "—", desc
        tagged = f"{prefix}_"
        if worker.startswith(tagged):
            return category, op, worker[len(tagged) :], desc
    return "other", worker, "—", worker


def _percentile(values: List[int], pct: float) -> Optional[int]:
    """Nearest-rank percentile; values in milliseconds."""
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (pct / 100.0) * (len(ordered) - 1)
    lo = int(rank)
    hi = min(lo + 1, len(ordered) - 1)
    frac = rank - lo
    return int(round(ordered[lo] * (1 - frac) + ordered[hi] * frac))


def _looks_like_ceph_error(out: str) -> bool:
    low = (out or "").lstrip().lower()
    return (
        low.startswith("error ")
        or low.startswith("error:")
        or low.startswith("traceback")
    )


class UpgradeThrashParallelOps:
    """Long-running CephFS/NFS churn loops driven by suite YAML flags."""

    def __init__(
        self,
        rados_obj,
        config: Dict[str, Any],
        clients: Optional[List[Any]] = None,
    ):
        self.rados_obj = rados_obj
        self.config = config or {}
        self.clients = clients or []
        self.cfg = dict(self.config.get("upgrade_parallel_ops") or {})
        self.enabled = bool(self.cfg.get("enabled", False))

        self.svg = self.cfg.get("svg", "par_ops")
        self.churn_svg = f"{self.svg}_churn"
        self.nfs_prefix = self.cfg.get("nfs_prefix", "par_nfs")
        self.base_port = int(self.cfg.get("base_port", 2100))
        self.max_live_nfs = int(self.cfg.get("max_live_nfs_clusters", 2))
        self.mount_prefix = self.cfg.get("mount_prefix", _DEFAULT_MOUNT_PREFIX)
        self.snap_sched_interval = self.cfg.get("snap_schedule_interval", "1h")
        self.seed_count = max(
            1, int(self.cfg.get("seed_subvol_count", _DEFAULT_SEED_COUNT))
        )
        rates = self.cfg.get("rate_limits") or {}
        self.rate_snap = int(rates.get("manual_snap_interval_s", 60))
        self.rate_subvol = int(rates.get("subvol_interval_s", 15))
        self.rate_nfs = int(rates.get("nfs_create_interval_s", 60))
        self.rate_remount = int(rates.get("remount_interval_s", 90))
        self.rate_client_fs = int(rates.get("client_fs_interval_s", self.rate_remount))

        self.cephfs_flags = dict(self.cfg.get("cephfs") or {})
        self.nfs_flags = dict(self.cfg.get("nfs") or {})
        self.fs_name = self._resolve_fs()

        self._stop = threading.Event()
        self._threads: List[threading.Thread] = []
        self._lock = threading.Lock()
        self._nfs_init_lock = threading.Lock()
        self._tls = threading.local()  # per-worker cmd-fail tally for _record
        self._id_counter = itertools.count(1)
        self.stats: Dict[str, Dict[str, Any]] = {}
        self.error_samples: Dict[str, List[str]] = {}
        # minute_epoch -> {ok, fail, dur_sum_ms, n} for phase table (no ring loss)
        self._time_buckets: Dict[int, Dict[str, int]] = {}
        self._nfs_created: List[str] = []
        self._nfs_endpoint: Optional[Tuple[str, int]] = None
        self._seed_subvols = [f"par_sv{i}" for i in range(self.seed_count)]
        self._started = False
        self._torn_down = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> None:
        if not self.enabled:
            log.info("[par_ops] disabled in suite config")
            return
        if not self.fs_name:
            log.warning("[par_ops] no CephFS found; skipping")
            return

        log.info(
            "[par_ops] starting on fs=%s svg=%s seeds=%s mount_prefix=%s",
            self.fs_name,
            self.svg,
            self._seed_subvols,
            self.mount_prefix,
        )
        try:
            self._bootstrap()
        except Exception as e:
            log.error("[par_ops] bootstrap failed: %s\n%s", e, traceback.format_exc())

        workers = self._enabled_workers()
        self._stop.clear()
        # Stagger first kick so N customers do not all hit the cluster at t=0
        for i, (name, fn, interval) in enumerate(workers):
            t = threading.Thread(
                target=self._loop,
                args=(name, fn, interval, i * 2),
                name=f"par_ops-{name}",
                daemon=True,
            )
            self._threads.append(t)
            t.start()
        self._started = True
        log.info(
            "[par_ops] started %d concurrent workers: %s",
            len(workers),
            [w[0] for w in workers],
        )

    def stop(self) -> List[str]:
        """Signal workers and join past the longest in-flight cmd (~180s).

        Returns names of threads still alive after the join deadline (rare).
        """
        if not self._threads:
            return []
        log.info("[par_ops] stopping workers")
        self._stop.set()
        # NFS create / clone wait use timeout=180; give join headroom beyond that.
        deadline = time.monotonic() + 240
        for t in self._threads:
            t.join(timeout=max(1.0, deadline - time.monotonic()))
        alive = [t.name for t in self._threads if t.is_alive()]
        if alive:
            log.warning("[par_ops] workers still running after join: %s", alive)
        self._threads = []
        return alive

    def teardown(self) -> None:
        """Umount → export/cluster rm → subvols → SVGs. Idempotent."""
        if self._torn_down or (not self.enabled and not self._started):
            return
        log.info("[par_ops] teardown begin")
        stragglers = self.stop()
        cleanup = (
            ("umount", self._umount_all),
            ("nfs", self._teardown_nfs),
            ("cephfs", self._teardown_cephfs),
        )
        for label, fn in cleanup:
            try:
                fn()
            except Exception as e:
                log.warning("[par_ops] teardown %s: %s", label, e)
        # If a worker outlived the join window, one more sweep after it drains.
        if stragglers:
            log.warning(
                "[par_ops] second teardown sweep after stragglers: %s", stragglers
            )
            time.sleep(30)
            for label, fn in cleanup:
                try:
                    fn()
                except Exception as e:
                    log.warning("[par_ops] teardown-2 %s: %s", label, e)
        self._started = False
        self._torn_down = True
        totals = self.get_summary().get("totals") or {}
        log.info(
            "[par_ops] teardown done | workers=%s ok=%s fail=%s cycles=%s",
            totals.get("workers", 0),
            totals.get("ok", 0),
            totals.get("fail", 0),
            totals.get("cycles", 0),
        )

    def get_summary(self) -> Dict[str, Any]:
        """Counters for the HTML report (full hop: start → teardown).

        Includes per-worker latency (avg/p50/p95/max) and minute aggregates
        for phase bucketing in the report builder (full-hop accurate).
        """
        with self._lock:
            stats = {
                k: {
                    "ok": v.get("ok", 0),
                    "fail": v.get("fail", 0),
                    "durs": list(v.get("durs") or []),
                }
                for k, v in self.stats.items()
            }
            samples = {k: list(v) for k, v in self.error_samples.items()}
            time_buckets = [
                {
                    "t": minute,
                    "ok": int(b.get("ok", 0)),
                    "fail": int(b.get("fail", 0)),
                    "dur_sum_ms": int(b.get("dur_sum_ms", 0)),
                    "n": int(b.get("n", 0)),
                }
                for minute, b in sorted(self._time_buckets.items())
            ]
        total_ok = sum(s.get("ok", 0) for s in stats.values())
        total_fail = sum(s.get("fail", 0) for s in stats.values())
        operations = []
        for worker in sorted(stats.keys()):
            category, op, target, desc = _catalog_for_worker(worker)
            s = stats[worker]
            durs = s.get("durs") or []
            n = s.get("ok", 0) + s.get("fail", 0)
            # Store ms; report converts to seconds for display.
            operations.append(
                {
                    "worker": worker,
                    "category": category,
                    "op": op,
                    "target": target,
                    "description": desc,
                    "ok": s.get("ok", 0),
                    "fail": s.get("fail", 0),
                    "latency_ms": {
                        "avg": (int(round(sum(durs) / len(durs))) if durs else None),
                        "p50": _percentile(durs, 50),
                        "p95": _percentile(durs, 95),
                        "max": max(durs) if durs else None,
                        "samples": len(durs),
                        "cycles": n,
                    },
                    "error_samples": samples.get(worker, [])[:5],
                }
            )
        return {
            "enabled": self.enabled,
            "fs": self.fs_name,
            "seeds": list(self._seed_subvols),
            "window": "full_hop",
            "totals": {
                "ok": total_ok,
                "fail": total_fail,
                "cycles": total_ok + total_fail,
                "workers": len(stats),
            },
            "operations": operations,
            "time_buckets": time_buckets,
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _next_id(self) -> int:
        with self._lock:
            return next(self._id_counter)

    def _resolve_fs(self) -> Optional[str]:
        override = self.cfg.get("filesystem")
        if override:
            return override
        cephfs_scale = self.config.get("scale", {}).get("cephfs", {})
        direct = cephfs_scale.get("direct_filesystem")
        if direct:
            return direct
        fs_list = cephfs_scale.get("filesystems") or []
        if fs_list:
            return fs_list[0]
        try:
            data = self._shell_json("ceph fs ls -f json", default=[])
            if data:
                return data[0].get("name")
        except Exception:
            pass
        return None

    def _shell(self, cmd: str, timeout: int = 120, check: bool = True) -> str:
        """Run via cephadm shell. Never raises for ceph CLI failures.

        Non-zero exit / Error output → ``log.info`` (tagged ``[soft]`` when
        ``check=False``). Expected during fail_fs / MDS-down; never gates the hop.
        Transport/timeout exceptions → warning + empty string (still non-gating).

        Exit status is echoed into stdout (``__PAR_OPS_RC__=N``) so concurrent
        workers do not race the shared ``installer.exit_status`` attribute.
        """
        wrapped = f"bash -c {shlex.quote(cmd + '; echo __PAR_OPS_RC__=$?')}"
        try:
            result = self.rados_obj.node.shell(
                [wrapped], timeout=timeout, check_status=False
            )
        except Exception as e:
            self._note_cmd_fail()
            log.warning("[par_ops] cmd exception (non-gating): %s (%s)", cmd[:100], e)
            return ""
        if isinstance(result, tuple):
            out = result[0]
        else:
            out = result
        out = (out if isinstance(out, str) else str(out or "")).strip()
        m = _RC_RE.search(out)
        rc = int(m.group(1)) if m else None
        if m:
            out = out[: m.start()].rstrip()
        failed = rc not in (None, 0) or _looks_like_ceph_error(out)
        if failed:
            self._note_cmd_fail()
            # info only — expected under fail_fs / MDS-down; never gates the hop.
            # (warning reserved for rare uncaught cycle exceptions in _loop)
            detail = out[:200] if out else f"exit={rc}"
            log.info(
                "[par_ops] cmd failed (non-gating)%s: %s | %s",
                "" if check else " [soft]",
                cmd[:100],
                detail,
            )
        return out

    def _note_cmd_fail(self) -> None:
        """Bump this worker thread's soft cmd-fail count (see ``_loop``)."""
        self._tls.cmd_fail = getattr(self._tls, "cmd_fail", 0) + 1

    def _nfs_export_create(self, cluster: str, pseudo: str, path: str) -> str:
        """Flag-form export create (matches upgrade thrashing NFS setup)."""
        return (
            f"ceph nfs export create cephfs"
            f" --cluster-id {cluster}"
            f" --pseudo-path {pseudo}"
            f" --fsname {self.fs_name}"
            f" --path {path}"
        )

    def _shell_ok(self, cmd: str, timeout: int = 120) -> str:
        """Best-effort shell — never raises; failures logged at info."""
        return self._shell(cmd, timeout=timeout, check=False)

    @staticmethod
    def _shq(path: str) -> str:
        """Shell-quote a client path (mount_prefix may be suite-overridden)."""
        return shlex.quote(path)

    def _client_exec(self, client, cmd: str, timeout: int = 45) -> None:
        """Client SSH exec; on failure raise with cmd + limit + wall time.

        ``ceph.TimeoutException`` / ``CommandFailed`` only say
        "Command exceed the allocated execution time." — useless in HTML samples.
        Keep the message short: ``_record`` caps samples at 480 chars.
        """
        t0 = time.monotonic()
        try:
            client.exec_command(sudo=True, cmd=cmd, timeout=timeout)
        except Exception as e:
            wall = time.monotonic() - t0
            shown = cmd if len(cmd) <= 120 else cmd[:117] + "..."
            raise RuntimeError(
                f"client_exec limit_s={timeout} wall_s={wall:.1f} "
                f"cmd={shown!r}: {e}"
            ) from e

    def _shell_json(self, cmd: str, default=None):
        out = self._shell_ok(cmd)
        if not out:
            return default if default is not None else {}
        try:
            return json.loads(out)
        except (json.JSONDecodeError, ValueError):
            return default if default is not None else {}

    def _record(
        self,
        name: str,
        ok: bool,
        err: str = "",
        dur_ms: int = 0,
    ) -> None:
        with self._lock:
            s = self.stats.setdefault(name, {"ok": 0, "fail": 0, "durs": []})
            s["ok" if ok else "fail"] += 1
            durs = s.setdefault("durs", [])
            durs.append(int(dur_ms))
            if len(durs) > _DUR_CAP:
                del durs[: len(durs) - _DUR_CAP]
            if err:
                samples = self.error_samples.setdefault(name, [])
                if len(samples) < 20:
                    # Enough for cmd + seed/mnt + cycle_wall; was 300 and
                    # truncated mid-cmd before the useful bits landed.
                    samples.append(err[:480])
            # Full-hop minute aggregate (no ring wrap → phase table stays accurate)
            minute = int(time.time()) // 60 * 60
            b = self._time_buckets.setdefault(
                minute, {"ok": 0, "fail": 0, "dur_sum_ms": 0, "n": 0}
            )
            b["ok" if ok else "fail"] += 1
            b["dur_sum_ms"] += int(dur_ms)
            b["n"] += 1

    def _loop(
        self,
        name: str,
        fn: Callable[[], None],
        interval: int,
        initial_delay: int = 0,
    ) -> None:
        if initial_delay and self._stop.wait(initial_delay):
            return
        while not self._stop.is_set():
            t0 = time.monotonic()
            self._tls.cmd_fail = 0
            try:
                fn()
                dur_ms = int((time.monotonic() - t0) * 1000)
                cmd_fail = getattr(self._tls, "cmd_fail", 0)
                # Soft-fail still never gates the hop; report must reflect cmd fails.
                err = f"{cmd_fail} ceph cmd(s) failed" if cmd_fail else ""
                self._record(name, cmd_fail == 0, err=err, dur_ms=dur_ms)
            except Exception as e:
                # Client mount / unexpected Python errors. Still non-gating.
                dur_ms = int((time.monotonic() - t0) * 1000)
                # cycle_wall first so a truncated sample still keeps timing.
                err = f"cycle_wall_s={dur_ms / 1000.0:.1f} worker={name} | {e}"
                self._record(name, False, err, dur_ms=dur_ms)
                log.warning(
                    "[par_ops] %s cycle error (non-gating, %.1fs): %s",
                    name,
                    dur_ms / 1000.0,
                    e,
                )
            self._stop.wait(interval)

    def _pick_seed(self) -> str:
        """Fallback when a worker is invoked without an explicit seed."""
        return self._seed_subvols[self._next_id() % len(self._seed_subvols)]

    def _seed_export(self, subvol: str) -> str:
        return f"/par_export_{subvol}"

    def _nfs_host(self) -> Optional[str]:
        try:
            hosts = self.rados_obj.run_ceph_command(cmd="ceph orch host ls")
            if not isinstance(hosts, list):
                return None
            for h in hosts:
                labels = h.get("labels") or []
                if "nfs" in labels and "_no_schedule" not in labels:
                    return h.get("hostname")
            for h in hosts:
                if "_no_schedule" not in (h.get("labels") or []):
                    return h.get("hostname")
        except Exception:
            pass
        return None

    def _track_nfs(self, name: str, add: bool = True) -> None:
        with self._lock:
            if add:
                if name not in self._nfs_created:
                    self._nfs_created.append(name)
            elif name in self._nfs_created:
                self._nfs_created.remove(name)

    def _live_throwaway_nfs(self) -> List[str]:
        with self._lock:
            return [n for n in self._nfs_created if n != _STABLE_NFS]

    # ------------------------------------------------------------------
    # Bootstrap
    # ------------------------------------------------------------------

    def _bootstrap(self) -> None:
        # Idempotent creates (re-create after hop / prior partial run)
        self._shell_ok(f"ceph fs subvolumegroup create {self.fs_name} {self.svg}")
        for sv in self._seed_subvols:
            self._shell_ok(
                f"ceph fs subvolume create {self.fs_name} {sv} "
                f"--group_name {self.svg}"
            )
        if self.cephfs_flags.get("snap_schedule"):
            self._shell_ok("ceph mgr module enable snap_schedule")
        if self._needs_stable_nfs():
            self._ensure_stable_nfs()

    def _needs_stable_nfs(self) -> bool:
        return any(
            self.nfs_flags.get(k)
            for k in (
                "export_lifecycle",
                "cluster_config",
                "export_default",
                "client_remount_snap",
                "client_links",
                "client_attrs",
            )
        )

    def _ensure_stable_nfs(self) -> None:
        """Create stable NFS + per-seed exports. Serialized (multi-worker safe)."""
        name = _STABLE_NFS
        with self._nfs_init_lock:
            existing = self._shell_ok("ceph nfs cluster ls")
            # ls may be JSON array or newline/space separated
            names = set()
            try:
                parsed = json.loads(existing) if existing.startswith("[") else None
                if isinstance(parsed, list):
                    names = {str(x) for x in parsed}
            except Exception:
                names = set(existing.replace(",", " ").split())
            if name not in names:
                host = self._nfs_host()
                placement = host or "1"
                # Pin monitoring_port so throwaways (9600+) never collide with
                # stable/default 9587 (CEPHADM_DAEMON_PLACE_FAIL on Dell).
                self._shell(
                    f'ceph nfs cluster create {name} "{placement}" '
                    f"--port {self.base_port} --monitoring_port 9587",
                    timeout=180,
                )
                self._nfs_endpoint = None  # force re-resolve after create
                time.sleep(5)
            self._track_nfs(name, True)
            # One export per seed — remount/export workers share the cluster
            # but hit different volumes (multi-customer).
            for sv in self._seed_subvols:
                path = self._shell_ok(
                    f"ceph fs subvolume getpath {self.fs_name} {sv} "
                    f"--group_name {self.svg}"
                )
                if path:
                    self._shell_ok(
                        self._nfs_export_create(name, self._seed_export(sv), path)
                    )

    def _enabled_workers(self) -> List[Tuple[str, Callable[[], None], int]]:
        """One thread per enabled op. Volume-bound ops fan out per seed subvol
        so multiple 'customers' churn distinct volumes concurrently."""
        cf, nf = self.cephfs_flags, self.nfs_flags
        out: List[Tuple[str, Callable[[], None], int]] = []
        if cf.get("subvolumegroup_lifecycle"):
            out.append(("svg_lifecycle", self._op_svg_lifecycle, self.rate_subvol))
        if cf.get("subvolume_lifecycle"):
            out.append(
                ("subvol_lifecycle", self._op_subvol_lifecycle, self.rate_subvol)
            )
        if (
            cf.get("subvolume_snapshot")
            or cf.get("subvolume_clone")
            or cf.get("group_snapshot")
        ):
            # Per-seed when volume snap/clone enabled; else one SVG group-snap worker
            seeds = (
                self._seed_subvols
                if (cf.get("subvolume_snapshot") or cf.get("subvolume_clone"))
                else self._seed_subvols[:1]
            )
            for sv in seeds:
                out.append(
                    (
                        f"snap_clone_{sv}",
                        lambda s=sv: self._op_snap_clone(s),
                        self.rate_snap,
                    )
                )
        if cf.get("snap_schedule"):
            for sv in self._seed_subvols:
                out.append(
                    (
                        f"snap_schedule_{sv}",
                        lambda s=sv: self._op_snap_schedule(s),
                        max(120, self.rate_snap),
                    )
                )
        if cf.get("snapshot_visibility"):
            for sv in self._seed_subvols:
                out.append(
                    (
                        f"snap_visibility_{sv}",
                        lambda s=sv: self._op_snap_visibility(s),
                        self.rate_snap,
                    )
                )
        if cf.get("readonly_status"):
            out.append(("cephfs_readonly", self._op_cephfs_readonly, 60))
        if nf.get("cluster_lifecycle"):
            out.append(("nfs_cluster", self._op_nfs_cluster, self.rate_nfs))
        if nf.get("cluster_config") or nf.get("export_default"):
            out.append(("nfs_config", self._op_nfs_config, self.rate_nfs))
        if nf.get("export_lifecycle"):
            for sv in self._seed_subvols:
                out.append(
                    (
                        f"nfs_export_{sv}",
                        lambda s=sv: self._op_nfs_export(s),
                        self.rate_subvol,
                    )
                )
        if self.clients:
            for flag, prefix, fn, rate in (
                (
                    "client_remount_snap",
                    "nfs_remount",
                    self._op_nfs_remount,
                    self.rate_remount,
                ),
                ("client_links", "nfs_links", self._op_nfs_links, self.rate_client_fs),
                ("client_attrs", "nfs_attrs", self._op_nfs_attrs, self.rate_client_fs),
            ):
                if nf.get(flag):
                    for sv in self._seed_subvols:
                        out.append(
                            (
                                f"{prefix}_{sv}",
                                lambda s=sv, f=fn: f(s),
                                rate,
                            )
                        )
        if nf.get("readonly_status"):
            out.append(("nfs_readonly", self._op_nfs_readonly, 60))
        return out

    # ------------------------------------------------------------------
    # CephFS workers
    # ------------------------------------------------------------------

    def _op_svg_lifecycle(self) -> None:
        """Churn sibling SVG only — never delete stable par_ops mid-run."""
        g = self.churn_svg
        # Soft create: SVG may already exist if prior cycle failed mid-rm
        self._shell_ok(f"ceph fs subvolumegroup create {self.fs_name} {g}")
        self._shell_ok(f"ceph fs subvolumegroup ls {self.fs_name}")
        self._shell_ok(f"ceph fs subvolumegroup info {self.fs_name} {g}")
        self._shell_ok(f"ceph fs subvolumegroup resize {self.fs_name} {g} 1073741824")
        # Matches existing thrashing: pin_type + value
        self._shell_ok(f"ceph fs subvolumegroup pin {self.fs_name} {g} distributed 1")
        self._shell_ok(f"ceph fs subvolumegroup rm {self.fs_name} {g} --force")

    def _op_subvol_lifecycle(self) -> None:
        name = f"par_sv_{self._next_id()}"
        g = self.svg
        self._shell(
            f"ceph fs subvolume create {self.fs_name} {name} --group_name {g} "
            f"--size 268435456"
        )
        try:
            self._shell_ok(f"ceph fs subvolume ls {self.fs_name} --group_name {g}")
            self._shell(
                f"ceph fs subvolume info {self.fs_name} {name} --group_name {g}"
            )
            self._shell_ok(
                f"ceph fs subvolume resize {self.fs_name} {name} 536870912 "
                f"--group_name {g}"
            )
            self._shell_ok(
                f"ceph fs subvolume pin {self.fs_name} {name} distributed 1 "
                f"--group_name {g}"
            )
            self._shell_ok(
                f"ceph fs subvolume metadata set {self.fs_name} {name} "
                f"par_key par_val --group_name {g}"
            )
            self._shell_ok(
                f"ceph fs subvolume metadata get {self.fs_name} {name} par_key "
                f"--group_name {g}"
            )
        finally:
            self._shell(
                f"ceph fs subvolume rm {self.fs_name} {name} --group_name {g} --force"
            )

    def _op_snap_clone(self, seed: Optional[str] = None) -> None:
        g = self.svg
        sv = seed or self._pick_seed()
        # Include time so hop/restart cannot collide with leftover par_clone_N
        tag = f"{self._next_id()}_{int(time.time()) % 100000}"
        snap = f"par_snap_{tag}"
        clone = f"par_clone_{tag}"
        cf = self.cephfs_flags

        # Group snapshot once per cycle on first seed only (shared SVG)
        if cf.get("group_snapshot") and sv == self._seed_subvols[0]:
            gs = f"par_gsnap_{tag}"
            self._shell_ok(
                f"ceph fs subvolumegroup snapshot create {self.fs_name} {g} {gs}"
            )
            self._shell_ok(f"ceph fs subvolumegroup snapshot ls {self.fs_name} {g}")
            self._shell_ok(
                f"ceph fs subvolumegroup snapshot rm {self.fs_name} {g} {gs} --force"
            )

        if not (cf.get("subvolume_snapshot") or cf.get("subvolume_clone")):
            return

        self._shell(
            f"ceph fs subvolume snapshot create {self.fs_name} {sv} {snap} "
            f"--group_name {g}"
        )
        try:
            self._shell_ok(
                f"ceph fs subvolume snapshot ls {self.fs_name} {sv} --group_name {g}"
            )
            self._shell_ok(
                f"ceph fs subvolume snapshot info {self.fs_name} {sv} {snap} "
                f"--group_name {g}"
            )
            self._shell_ok(
                f"ceph fs subvolume snapshot metadata set {self.fs_name} {sv} {snap} "
                f"par_m k --group_name {g}"
            )
            if cf.get("subvolume_clone"):
                # Best-effort clear leftover same name (restart / prior soft-fail)
                self._shell_ok(
                    f"ceph fs subvolume rm {self.fs_name} {clone} "
                    f"--group_name {g} --force"
                )
                self._shell(
                    f"ceph fs subvolume snapshot clone {self.fs_name} {sv} {snap} "
                    f"{clone} --group_name {g} --target_group_name {g}"
                )
                self._wait_clone(clone, g)
                self._shell_ok(
                    f"ceph fs subvolume info {self.fs_name} {clone} --group_name {g}"
                )
                self._shell_ok(
                    f"ceph fs subvolume getpath {self.fs_name} {clone} "
                    f"--group_name {g}"
                )
                self._shell_ok(
                    f"ceph fs subvolume rm {self.fs_name} {clone} "
                    f"--group_name {g} --force"
                )
        finally:
            self._shell_ok(
                f"ceph fs subvolume snapshot rm {self.fs_name} {sv} {snap} "
                f"--group_name {g} --force"
            )

    def _wait_clone(self, clone: str, group: str, timeout: int = 180) -> None:
        """Poll clone status via ``_shell_ok`` (soft-fail, respects ``_stop``).

        Kept local on purpose: ``FsUtils.validate_clone_state`` needs a CephFS
        client, raises on timeout, and does not honor worker stop — wrong fit
        for non-gating par_ops. Same CLI as cephfs_utilsV1 clone status.
        """
        deadline = time.time() + timeout
        while time.time() < deadline and not self._stop.is_set():
            out = self._shell_ok(
                f"ceph fs clone status {self.fs_name} {clone} "
                f"--group_name {group} --format json"
            )
            state = ""
            try:
                data = json.loads(out) if out.startswith("{") else {}
                state = (
                    (data.get("status") or {}).get("state") or data.get("state") or ""
                ).lower()
            except Exception:
                state = ""
            if state in ("complete", "completed"):
                return
            if state in ("failed", "canceled", "cancelled"):
                log.warning("[par_ops] clone %s ended in state=%s", clone, state)
                self._note_cmd_fail()
                self._shell_ok(
                    f"ceph fs clone cancel {self.fs_name} {clone} "
                    f"--group_name {group}"
                )
                return
            # Fallback: clone finished and is a normal subvolume
            info = self._shell_ok(
                f"ceph fs subvolume info {self.fs_name} {clone} --group_name {group}"
            )
            if info and "ENOENT" not in info and "Error" not in info:
                try:
                    j = json.loads(info) if info.startswith("{") else {}
                    if j.get("state") == "complete" or "path" in j or "bytes_used" in j:
                        return
                except Exception:
                    if "path" in info:
                        return
            time.sleep(5)
        if self._stop.is_set():
            return
        log.warning(
            "[par_ops] clone %s not complete after %ss; cancelling", clone, timeout
        )
        self._note_cmd_fail()
        self._shell_ok(
            f"ceph fs clone cancel {self.fs_name} {clone} --group_name {group}"
        )

    def _op_snap_schedule(self, seed: Optional[str] = None) -> None:
        """Schedule on a seed subvol via --subvol/--group — not FeatureManager `/`."""
        sv = seed or self._pick_seed()
        interval = self.snap_sched_interval
        # Prefer --subvol/--group: raw path add can schedule on path/.. and
        # leave schedules that block subvolume rm at teardown.
        subvol_args = f"--fs {self.fs_name} --subvol {sv} --group {self.svg}"
        added = self._shell_ok(f"ceph fs snap-schedule add / {interval} {subvol_args}")
        if not added or "Error" in added or "EINVAL" in added:
            path = self._shell_ok(
                f"ceph fs subvolume getpath {self.fs_name} {sv} "
                f"--group_name {self.svg}"
            )
            if path:
                self._shell_ok(
                    f"ceph fs snap-schedule add {path} {interval} "
                    f"--fs {self.fs_name}"
                )
        self._shell_ok(f"ceph fs snap-schedule retention add / h 10 {subvol_args}")
        self._shell_ok(f"ceph fs snap-schedule activate / {subvol_args}")
        self._shell_ok(f"ceph fs snap-schedule status / {subvol_args}")
        self._shell_ok(f"ceph fs snap-schedule list / {subvol_args}")
        if int(time.time()) % 2 == 0:
            self._shell_ok(f"ceph fs snap-schedule deactivate / {subvol_args}")
            self._shell_ok(f"ceph fs snap-schedule activate / {subvol_args}")

    def _op_snap_visibility(self, seed: Optional[str] = None) -> None:
        # Correct CLI (cephfs_snap_utils): set uses --value, not positional
        g = self.svg
        sv = seed or self._pick_seed()
        self._shell(
            f"ceph fs subvolume snapshot_visibility set {self.fs_name} {sv} "
            f"--group_name {g} --value true"
        )
        self._shell_ok(
            f"ceph fs subvolume snapshot_visibility get {self.fs_name} {sv} "
            f"--group_name {g}"
        )

    def _op_cephfs_readonly(self) -> None:
        self._shell_ok("ceph fs ls")
        self._shell_ok(f"ceph fs status {self.fs_name}")
        self._shell_ok("ceph mds versions")
        # Avoid inventing daemon id "a" — just list metadata
        self._shell_ok("ceph mds metadata")

    # ------------------------------------------------------------------
    # NFS workers
    # ------------------------------------------------------------------

    def _op_nfs_cluster(self) -> None:
        n = self._next_id()
        name = f"{self.nfs_prefix}_{n}"
        host = self._nfs_host()
        port = self.base_port + 10 + (n % 40)
        # Unique Ganesha prometheus port — default 9587 is host-global and
        # collides when >1 NFS lands on the same node (MX74F5 PLACE_FAIL).
        mon_port = 9600 + (n % 40)
        placement = host or "1"
        # Cap live throwaways (stable not counted)
        while len(self._live_throwaway_nfs()) >= self.max_live_nfs:
            live = self._live_throwaway_nfs()
            if not live:
                break
            old = live[0]
            self._shell_ok(f"ceph nfs cluster rm {old}")
            self._track_nfs(old, False)
        self._shell(
            f'ceph nfs cluster create {name} "{placement}" '
            f"--port {port} --monitoring_port {mon_port}",
            timeout=180,
        )
        self._track_nfs(name, True)
        try:
            self._shell_ok("ceph nfs cluster ls")
            self._shell_ok(f"ceph nfs cluster info {name}")
        finally:
            self._shell_ok(f"ceph nfs cluster rm {name}")
            self._track_nfs(name, False)

    def _op_nfs_config(self) -> None:
        self._ensure_stable_nfs()
        name = _STABLE_NFS
        if self.nfs_flags.get("cluster_config"):
            # Single-line LOG block — avoids fragile heredoc through cephadm shell
            conf = "LOG { COMPONENTS { FSAL = EVENT; } }"
            self._shell(
                f"bash -c \"printf '%s\\n' '{conf}' > /tmp/par_nfs_cfg.conf && "
                f'ceph nfs cluster config set {name} -i /tmp/par_nfs_cfg.conf"'
            )
            self._shell_ok(f"ceph nfs cluster config get {name}")
            self._shell_ok(f"ceph nfs cluster config reset {name}")
        if self.nfs_flags.get("export_default"):
            # get only — set needs a real export-default payload
            self._shell_ok(f"ceph nfs cluster get-export-default {name}")

    def _op_nfs_export(self, seed: Optional[str] = None) -> None:
        self._ensure_stable_nfs()
        name = _STABLE_NFS
        sv = seed or self._pick_seed()
        pseudo = f"/par_exp_{self._next_id()}"
        path = self._shell_ok(
            f"ceph fs subvolume getpath {self.fs_name} {sv} " f"--group_name {self.svg}"
        )
        if not path:
            log.warning(
                "[par_ops] nfs_export skip (non-gating): getpath empty for %s", sv
            )
            return
        self._shell(self._nfs_export_create(name, pseudo, path))
        try:
            self._shell_ok(f"ceph nfs export ls {name}")
            self._shell_ok(f"ceph nfs export info {name} {pseudo}")
        finally:
            self._shell_ok(f"ceph nfs export rm {name} {pseudo}")

    def _op_nfs_remount(self, seed: Optional[str] = None) -> None:
        def body(client, mnt, _sv, _idx, _extra):
            q = self._shq(mnt)
            self._client_exec(
                client,
                (
                    f"touch {q}/par_ops_touch 2>/dev/null || true; "
                    f"ls {q}/.snap 2>/dev/null || true"
                ),
                timeout=30,
            )

        self._nfs_client_cycle(seed, "nfs_sv", body, snap_vis=True)

    def _op_nfs_links(self, seed: Optional[str] = None) -> None:
        """Soft/hard links same+cross dir; softlink across a peer seed subvol."""

        def body(client, mnt, _sv, seed_idx, extra):
            q = self._shq(mnt)
            self._client_exec(
                client,
                (
                    f"timeout 30 rm -rf {q}/a {q}/b && mkdir -p {q}/a {q}/b && "
                    f"echo par_ops_link > {q}/a/src && "
                    f"ln -sf src {q}/a/soft_same && "
                    f"ln {q}/a/src {q}/a/hard_same && "
                    f"ln -sf ../a/src {q}/b/soft_cross_dir && "
                    f"ln {q}/a/src {q}/b/hard_cross_dir && "
                    f"test -f {q}/a/soft_same && test -f {q}/a/hard_same && "
                    f"test -f {q}/b/soft_cross_dir && test -f {q}/b/hard_cross_dir"
                ),
                timeout=45,
            )
            if len(self._seed_subvols) <= 1:
                return
            peer_idx = (seed_idx + 1) % len(self._seed_subvols)
            # Same client so absolute softlink resolves locally
            _pc, peer_mnt, _ = self._nfs_mount_seed(
                self._seed_subvols[peer_idx],
                peer_idx,
                mnt_tag="nfs_links_peer_sv",
                client=client,
            )
            if not peer_mnt:
                return
            extra.append(peer_mnt)
            pq = self._shq(peer_mnt)
            self._client_exec(
                client,
                (
                    f"echo peer > {pq}/peer_src && "
                    f"ln -sfn {pq}/peer_src {q}/a/soft_cross_sv && "
                    f"test -f {q}/a/soft_cross_sv && "
                    # Expected fail across exports — soft OK if EXDEV
                    f"(ln {pq}/peer_src {q}/a/hard_cross_sv 2>/dev/null || true)"
                ),
                timeout=45,
            )

        self._nfs_client_cycle(
            seed,
            "nfs_links_sv",
            body,
            cleanup="timeout 30 rm -rf {mnt}/a {mnt}/b 2>/dev/null || true",
        )

    def _op_nfs_attrs(self, seed: Optional[str] = None) -> None:
        """chmod/mtime/stat — no setfattr (IBMCEPH-1752: xattrs unsupported over NFS)."""

        def body(client, mnt, _sv, _idx, _extra):
            path = self._shq(f"{mnt}/par_ops_attr")
            self._client_exec(
                client,
                (
                    f"echo attr > {path} && "
                    f"chmod 640 {path} && "
                    f"touch -d '2020-01-02 03:04:05' {path} && "
                    f"test \"$(stat -c '%a' {path})\" = 640"
                ),
                timeout=45,
            )

        self._nfs_client_cycle(
            seed,
            "nfs_attrs_sv",
            body,
            cleanup="timeout 30 rm -f {mnt}/par_ops_attr 2>/dev/null || true",
        )

    def _nfs_client_cycle(
        self,
        seed: Optional[str],
        mnt_tag: str,
        body: Callable,
        *,
        snap_vis: bool = False,
        cleanup: Optional[str] = None,
    ) -> None:
        """Mount seed → run body → optional cleanup → umount.

        Body failures propagate to ``_loop`` (fail counter). Mount miss is a
        soft skip (no raise) so unavailable NFS does not spam fails.
        """
        if not self.clients:
            return
        sv = seed or self._pick_seed()
        seed_idx = self._seed_subvols.index(sv)
        client, mnt, _export = self._nfs_mount_seed(
            sv, seed_idx, mnt_tag=mnt_tag, snap_vis=snap_vis
        )
        if not client:
            return
        extra_mnts: List[str] = []
        client_id = getattr(client, "hostname", None) or getattr(
            client, "ip_address", "?"
        )
        try:
            body(client, mnt, sv, seed_idx, extra_mnts)
        except Exception as e:
            # Bare TimeoutException ("Command exceed the allocated…") is useless
            # in the HTML samples column — rewrap with seed/mount/client.
            raise RuntimeError(
                f"{mnt_tag} seed={sv} mnt={mnt} client={client_id}: {e}"
            ) from e
        finally:
            if cleanup:
                try:
                    client.exec_command(
                        sudo=True,
                        cmd=cleanup.format(mnt=self._shq(mnt)),
                        timeout=30,
                    )
                except Exception:
                    pass
            for em in extra_mnts:
                self._nfs_umount(client, em)
            self._nfs_umount(client, mnt)

    def _stable_nfs_endpoint(self) -> Tuple[Optional[str], Optional[int]]:
        """Cached backend IP/port for the stable NFS cluster."""
        if self._nfs_endpoint:
            return self._nfs_endpoint
        self._ensure_stable_nfs()
        info = self._shell_ok(f"ceph nfs cluster info {_STABLE_NFS} --format json")
        host_ip, port = self._parse_nfs_endpoint(info)
        if host_ip:
            self._nfs_endpoint = (host_ip, int(port or self.base_port))
            return self._nfs_endpoint
        log.warning(
            "[par_ops] nfs mount skip (non-gating): no endpoint for %s",
            _STABLE_NFS,
        )
        return None, None

    def _nfs_mount_seed(
        self,
        sv: str,
        seed_idx: int,
        mnt_tag: str = "nfs_work_sv",
        snap_vis: bool = False,
        client=None,
    ):
        """Mount seed export. Returns (client, mnt, export) or (None, None, None)."""
        host_ip, port = self._stable_nfs_endpoint()
        if not host_ip:
            return None, None, None
        export = self._seed_export(sv)
        if client is None:
            client = self.clients[seed_idx % len(self.clients)]
        mnt = f"{self.mount_prefix}{mnt_tag}{seed_idx}"
        if snap_vis:
            self._shell_ok(
                f"ceph fs subvolume snapshot_visibility set {self.fs_name} {sv} "
                f"--group_name {self.svg} --value true"
            )
        q = self._shq(mnt)
        src = self._shq(f"{host_ip}:{export}")
        try:
            client.exec_command(sudo=True, cmd=f"mkdir -p {q}", timeout=15)
            client.exec_command(
                sudo=True,
                cmd=f"timeout 30 umount -l {q} 2>/dev/null || true",
                timeout=30,
            )
            mount_cmd = (
                f"timeout 45 mount -t nfs "
                f"-o vers=4.1,port={port},soft,timeo=50,retrans=2 "
                f"{src} {q}"
            )
            client.exec_command(sudo=True, cmd=mount_cmd, timeout=45)
            return client, mnt, export
        except Exception as e:
            log.warning(
                "[par_ops] nfs mount (non-gating): %s on %s: %s", mnt, export, e
            )
            self._nfs_umount(client, mnt)
            return None, None, None

    def _nfs_umount(self, client, mnt: str) -> None:
        if not client or not mnt:
            return
        try:
            client.exec_command(
                sudo=True,
                cmd=f"timeout 30 umount -l {self._shq(mnt)} 2>/dev/null || true",
                timeout=30,
            )
        except Exception:
            pass

    def _parse_nfs_endpoint(self, info_raw: str):
        try:
            data = json.loads(info_raw) if info_raw else {}
            block = data.get(_STABLE_NFS) or (
                next(iter(data.values()), {}) if data else {}
            )
            backends = block.get("backend") or []
            if backends:
                b0 = backends[0]
                return b0.get("ip") or b0.get("hostname"), int(
                    b0.get("port") or self.base_port
                )
        except Exception:
            pass
        return None, None

    def _op_nfs_readonly(self) -> None:
        self._shell_ok("ceph nfs cluster ls")
        with self._lock:
            names = list(self._nfs_created)
        if names:
            self._shell_ok(f"ceph nfs cluster info {names[0]}")

    # ------------------------------------------------------------------
    # Teardown
    # ------------------------------------------------------------------

    def _umount_all(self) -> None:
        prefix = self.mount_prefix  # e.g. /mnt/par_
        pref_q = self._shq(prefix)
        for client in self.clients:
            try:
                client.exec_command(
                    sudo=True,
                    cmd=(
                        f"for m in $(mount | awk '{{print $3}}' "
                        f"| grep -F -- {pref_q}); do "
                        f'timeout 30 umount -l "$m" 2>/dev/null || true; done'
                    ),
                    timeout=60,
                )
            except Exception as e:
                log.debug("[par_ops] umount: %s", e)

    def _teardown_nfs(self) -> None:
        with self._lock:
            names = list(dict.fromkeys(self._nfs_created + [_STABLE_NFS]))
        for name in names:
            exports = self._shell_json(
                f"ceph nfs export ls {name} --format json", default=[]
            )
            if isinstance(exports, list):
                for pseudo in exports:
                    if isinstance(pseudo, str) and pseudo.startswith("/"):
                        self._shell_ok(f"ceph nfs export rm {name} {pseudo}")
            # orch delete is async — retry until gone or budget exhausted
            gone = False
            for _ in range(12):
                self._shell_ok(f"ceph nfs cluster rm {name}")
                ls = self._shell_ok("ceph nfs cluster ls")
                if name not in ls.split():
                    gone = True
                    break
                time.sleep(5)
            if not gone:
                log.warning(
                    "[par_ops] nfs cluster %s still present after teardown "
                    "retries (leak)",
                    name,
                )
        with self._lock:
            self._nfs_created = []
        self._nfs_endpoint = None

    def _purge_subvol_snaps(self, subvol: str, group: str) -> None:
        """Remove snap-schedules and snapshots so subvolume rm can succeed."""
        # --subvol form schedules land on getpath/.. ; clear both forms
        subvol_args = f"--fs {self.fs_name} --subvol {subvol} --group {group}"
        self._shell_ok(f"ceph fs snap-schedule deactivate / {subvol_args}")
        self._shell_ok(f"ceph fs snap-schedule remove / {subvol_args}")
        path = self._shell_ok(
            f"ceph fs subvolume getpath {self.fs_name} {subvol} "
            f"--group_name {group}"
        )
        if path:
            self._shell_ok(
                f"ceph fs snap-schedule deactivate {path} --fs {self.fs_name}"
            )
            self._shell_ok(f"ceph fs snap-schedule remove {path} --fs {self.fs_name}")
        snaps = self._shell_json(
            f"ceph fs subvolume snapshot ls {self.fs_name} {subvol} "
            f"--group_name {group} --format json",
            default=[],
        )
        if not isinstance(snaps, list):
            snaps = []
        for snap in snaps:
            sname = snap.get("name") if isinstance(snap, dict) else str(snap)
            if sname:
                self._shell_ok(
                    f"ceph fs subvolume snapshot rm {self.fs_name} {subvol} "
                    f"{sname} --group_name {group} --force"
                )

    def _teardown_cephfs(self) -> None:
        if not self.fs_name:
            return
        for g in (self.svg, self.churn_svg):
            items = self._shell_json(
                f"ceph fs subvolume ls {self.fs_name} --group_name {g} "
                f"--format json",
                default=[],
            )
            if not isinstance(items, list):
                items = []
            for item in items:
                name = item.get("name") if isinstance(item, dict) else str(item)
                if not name:
                    continue
                self._purge_subvol_snaps(name, g)
                self._shell_ok(
                    f"ceph fs subvolume rm {self.fs_name} {name} "
                    f"--group_name {g} --force"
                )
            # Group-level snaps (if any)
            gsnaps = self._shell_json(
                f"ceph fs subvolumegroup snapshot ls {self.fs_name} {g} "
                f"--format json",
                default=[],
            )
            if isinstance(gsnaps, list):
                for gs in gsnaps:
                    sname = gs.get("name") if isinstance(gs, dict) else str(gs)
                    if sname:
                        self._shell_ok(
                            f"ceph fs subvolumegroup snapshot rm "
                            f"{self.fs_name} {g} {sname} --force"
                        )
            gone = False
            for _ in range(12):
                self._shell_ok(f"ceph fs subvolumegroup rm {self.fs_name} {g} --force")
                groups = self._shell_json(
                    f"ceph fs subvolumegroup ls {self.fs_name} --format json",
                    default=[],
                )
                names = {
                    (item.get("name") if isinstance(item, dict) else str(item))
                    for item in (groups if isinstance(groups, list) else [])
                }
                if g not in names:
                    gone = True
                    break
                time.sleep(5)
            if not gone:
                log.warning(
                    "[par_ops] subvolumegroup %s still present after teardown "
                    "retries (leak)",
                    g,
                )
