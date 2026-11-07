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
    export lifecycle, client remount) fan out **one thread per seed subvol**
    (``seed_subvol_count``, default 3) so distinct volumes are churned at once.
  - Cluster-scoped ops (SVG churn, NFS cluster create/rm, cluster config)
    stay single-threaded.
"""

from __future__ import annotations

import itertools
import json
import threading
import time
import traceback
from typing import Any, Callable, Dict, List, Optional, Tuple

from utility.log import Log

log = Log(__name__)

_DEFAULT_MOUNT_PREFIX = "/mnt/par_"
_STABLE_NFS = "par_nfs_stable"  # underscore — safer for service_id
_DEFAULT_SEED_COUNT = 3  # distinct subvols ≈ distinct customers
_EVENT_CAP = 500  # ring buffer of cycle events for phase bucketing
_DUR_CAP = 200  # per-worker durations kept for p50/p95

# worker-name prefix → (category, logical_op, description) for HTML report
_OP_CATALOG = {
    "svg_lifecycle": (
        "cephfs",
        "svg_lifecycle",
        "Create/resize/pin/rm sibling SVG (never deletes stable SVG)",
    ),
    "subvol_lifecycle": (
        "cephfs",
        "subvol_lifecycle",
        "Create/info/resize/pin/metadata/rm throwaway subvolume",
    ),
    "snap_clone": (
        "cephfs",
        "snap_clone",
        "Subvolume snapshot (+ clone when enabled); group snap on first seed",
    ),
    "snap_schedule": (
        "cephfs",
        "snap_schedule",
        "snap-schedule add/retention/activate on seed subvol",
    ),
    "snap_visibility": (
        "cephfs",
        "snap_visibility",
        "snapshot_visibility set/get on seed subvol",
    ),
    "cephfs_readonly": (
        "cephfs",
        "readonly_status",
        "fs ls/status, mds versions/metadata",
    ),
    "nfs_cluster": (
        "nfs",
        "cluster_lifecycle",
        "Create/info/rm throwaway NFS cluster",
    ),
    "nfs_config": (
        "nfs",
        "cluster_config",
        "NFS cluster config set/get/reset + get-export-default",
    ),
    "nfs_export": (
        "nfs",
        "export_lifecycle",
        "Create/ls/info/rm throwaway NFS export on stable cluster",
    ),
    "nfs_qos": (
        "nfs",
        "qos",
        "NFS cluster/export QoS enable/get/disable",
    ),
    "nfs_remount": (
        "nfs",
        "client_remount_snap",
        "NFS mount/umount seed export + .snap probe",
    ),
    "nfs_readonly": (
        "nfs",
        "readonly_status",
        "nfs cluster ls/info",
    ),
}


def _catalog_for_worker(worker: str) -> Tuple[str, str, str, str]:
    """Return (category, op, target, description) for a worker thread name."""
    # Longest prefix first so nfs_export_par_sv0 never matches a shorter key.
    for prefix, (category, op, desc) in sorted(
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

        self.cephfs_flags = dict(self.cfg.get("cephfs") or {})
        self.nfs_flags = dict(self.cfg.get("nfs") or {})
        self.fs_name = self._resolve_fs()

        self._stop = threading.Event()
        self._threads: List[threading.Thread] = []
        self._lock = threading.Lock()
        self._nfs_init_lock = threading.Lock()
        self._id_counter = itertools.count(1)
        self.stats: Dict[str, Dict[str, Any]] = {}
        self.error_samples: Dict[str, List[str]] = {}
        self._events: List[Dict[str, Any]] = []
        self._nfs_created: List[str] = []
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

    def stop(self) -> None:
        if not self._threads:
            return
        log.info("[par_ops] stopping workers")
        self._stop.set()
        for t in self._threads:
            t.join(timeout=45)
        self._threads = []

    def teardown(self) -> None:
        """Umount → export/cluster rm → subvols → SVGs. Idempotent."""
        if self._torn_down or (not self.enabled and not self._started):
            return
        log.info("[par_ops] teardown begin")
        self.stop()
        for label, fn in (
            ("umount", self._umount_all),
            ("nfs", self._teardown_nfs),
            ("cephfs", self._teardown_cephfs),
        ):
            try:
                fn()
            except Exception as e:
                log.warning("[par_ops] teardown %s: %s", label, e)
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

        Includes per-worker latency (avg/p50/p95/max) and a capped cycle
        event stream for phase bucketing in the report builder.
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
            events = list(self._events)
        total_ok = sum(s.get("ok", 0) for s in stats.values())
        total_fail = sum(s.get("fail", 0) for s in stats.values())
        operations = []
        for worker in sorted(stats.keys()):
            category, op, target, desc = _catalog_for_worker(worker)
            s = stats[worker]
            durs = s.get("durs") or []
            n = s.get("ok", 0) + s.get("fail", 0)
            avg_ms = int(round(sum(durs) / len(durs))) if durs else None
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
                        "avg": avg_ms,
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
            "events": events,
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

    def _last_exit(self) -> Optional[int]:
        """Exit code from the most recent cephadm shell on the installer node."""
        installer = getattr(self.rados_obj.node, "installer", None)
        if installer is None:
            return None
        host = getattr(installer, "node", installer)
        rc = getattr(host, "exit_status", None)
        try:
            return int(rc) if rc is not None else None
        except (TypeError, ValueError):
            return None

    def _shell(self, cmd: str, timeout: int = 120, check: bool = True) -> str:
        """Run via cephadm shell. Never raises for ceph CLI failures.

        Non-zero exit / Error output → ``log.info`` (tagged ``[soft]`` when
        ``check=False``). Expected during fail_fs / MDS-down; never gates the hop.
        Transport/timeout exceptions → warning + empty string (still non-gating).
        """
        try:
            result = self.rados_obj.node.shell(
                [cmd], timeout=timeout, check_status=False
            )
        except Exception as e:
            log.warning("[par_ops] cmd exception (non-gating): %s (%s)", cmd[:100], e)
            return ""
        if isinstance(result, tuple):
            out = result[0]
        else:
            out = result
        out = (out if isinstance(out, str) else str(out or "")).strip()
        rc = self._last_exit()
        failed = rc not in (None, 0) or _looks_like_ceph_error(out)
        if failed:
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
                    samples.append(err[:300])
            self._events.append(
                {
                    "t": time.time(),
                    "w": name,
                    "ok": bool(ok),
                    "ms": int(dur_ms),
                }
            )
            if len(self._events) > _EVENT_CAP:
                self._events = self._events[-_EVENT_CAP:]

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
            try:
                fn()
                dur_ms = int((time.monotonic() - t0) * 1000)
                self._record(name, True, dur_ms=dur_ms)
            except Exception as e:
                # Client mount / unexpected Python errors only — ceph CLI no
                # longer raises. Still non-gating: count + warn, never abort hop.
                dur_ms = int((time.monotonic() - t0) * 1000)
                self._record(name, False, str(e), dur_ms=dur_ms)
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
                "qos_bandwidth",
                "qos_iops",
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
                self._shell(
                    f'ceph nfs cluster create {name} "{placement}" '
                    f"--port {self.base_port}",
                    timeout=180,
                )
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
        if nf.get("qos_bandwidth") or nf.get("qos_iops"):
            out.append(("nfs_qos", self._op_nfs_qos, self.rate_nfs))
        if nf.get("client_remount_snap") and self.clients:
            for sv in self._seed_subvols:
                out.append(
                    (
                        f"nfs_remount_{sv}",
                        lambda s=sv: self._op_nfs_remount(s),
                        self.rate_remount,
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
        # cephfs_utilsV1: ``ceph fs clone status VOL CLONE --group_name G -f json``
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
        self._shell_ok(
            f"ceph fs clone cancel {self.fs_name} {clone} --group_name {group}"
        )

    def _op_snap_schedule(self, seed: Optional[str] = None) -> None:
        """Schedule on a seed subvol via --subvol/--group — not FeatureManager `/`."""
        sv = seed or self._pick_seed()
        interval = self.snap_sched_interval
        self._shell_ok("ceph mgr module enable snap_schedule")
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
            f'ceph nfs cluster create {name} "{placement}" --port {port}',
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

    def _op_nfs_qos(self) -> None:
        """Code path ready; suite flags default false.

        CLI matches UpgradeFeatureManager / nfs qos tests (PerShare + flags).
        """
        self._ensure_stable_nfs()
        name = _STABLE_NFS
        export = self._seed_export(self._seed_subvols[0])
        if self.nfs_flags.get("qos_bandwidth"):
            self._shell_ok(
                f"ceph nfs cluster qos enable bandwidth_control {name} "
                f"PerShare --combined-rw-bw-ctrl "
                f"--max_export_combined_bw 104857600"
            )
            self._shell_ok(f"ceph nfs cluster qos get {name}")
            self._shell_ok(f"ceph nfs cluster qos disable {name}")
            self._shell_ok(
                f"ceph nfs export qos enable bandwidth_control {name} {export} "
                f"PerShare --combined-rw-bw-ctrl "
                f"--max_export_combined_bw 52428800"
            )
            self._shell_ok(f"ceph nfs export qos disable {name} {export}")
        if self.nfs_flags.get("qos_iops"):
            self._shell_ok(
                f"ceph nfs cluster qos enable ops_control {name} PerShare 1000"
            )
            self._shell_ok(f"ceph nfs cluster qos get {name}")
            self._shell_ok(f"ceph nfs cluster qos disable {name}")

    def _op_nfs_remount(self, seed: Optional[str] = None) -> None:
        if not self.clients:
            return
        self._ensure_stable_nfs()
        info = self._shell_ok(f"ceph nfs cluster info {_STABLE_NFS} --format json")
        host_ip, port = self._parse_nfs_endpoint(info)
        if not host_ip:
            log.warning(
                "[par_ops] nfs_remount skip (non-gating): no endpoint for %s",
                _STABLE_NFS,
            )
            return
        sv = seed or self._pick_seed()
        export = self._seed_export(sv)
        # Prefer a distinct client per seed when available (multi-client customers)
        seed_idx = self._seed_subvols.index(sv)
        client = self.clients[seed_idx % len(self.clients)]
        mnt = f"{self.mount_prefix}nfs_sv{seed_idx}"  # e.g. /mnt/par_nfs_sv0
        self._shell_ok(
            f"ceph fs subvolume snapshot_visibility set {self.fs_name} {sv} "
            f"--group_name {self.svg} --value true"
        )
        try:
            client.exec_command(sudo=True, cmd=f"mkdir -p {mnt}", timeout=15)
            client.exec_command(
                sudo=True, cmd=f"umount -l {mnt} 2>/dev/null || true", timeout=30
            )
            # soft+timeo so a wedged Ganesha cannot hang the remount worker forever
            mount_cmd = (
                f"mount -t nfs -o vers=4.1,port={port},soft,timeo=50,retrans=2 "
                f"{host_ip}:{export} {mnt}"
            )
            client.exec_command(sudo=True, cmd=mount_cmd, timeout=45)
            client.exec_command(
                sudo=True,
                cmd=(
                    f"touch {mnt}/par_ops_touch 2>/dev/null || true; "
                    f"ls {mnt}/.snap 2>/dev/null || true"
                ),
                timeout=30,
            )
        except Exception as e:
            log.warning(
                "[par_ops] nfs_remount (non-gating): %s on %s: %s", mnt, export, e
            )
        finally:
            try:
                client.exec_command(
                    sudo=True, cmd=f"umount -l {mnt} 2>/dev/null || true", timeout=30
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
        for client in self.clients:
            try:
                client.exec_command(
                    sudo=True,
                    cmd=(
                        f"for m in $(mount | awk '{{print $3}}' "
                        f"| grep -E '^{prefix}'); do "
                        f'umount -l "$m" 2>/dev/null || true; done'
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
            for _ in range(12):
                self._shell_ok(f"ceph nfs cluster rm {name}")
                ls = self._shell_ok("ceph nfs cluster ls")
                if name not in ls:
                    break
                time.sleep(5)
        with self._lock:
            self._nfs_created = []

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
            self._shell_ok(f"ceph fs subvolumegroup rm {self.fs_name} {g} --force")
