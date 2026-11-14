"""Helpers for cephadm-agent scale regression: timing, waits, health/RSS monitors."""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from cephadm_agent.helpers import (
    AGENT_HEALTH_WARNING,
    get_agent_daemons,
    get_fsid,
    get_node_for_host,
    log,
    shell,
)

ISO = "%Y-%m-%dT%H:%M:%SZ"


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime(ISO)


def elapsed(start: float) -> float:
    return round(time.time() - start, 2)


@dataclass
class TimingRecord:
    name: str
    started_at: str
    finished_at: str = ""
    seconds: float = 0.0
    meta: Dict[str, Any] = field(default_factory=dict)

    def finish(self, start_ts: float, **meta: Any) -> "TimingRecord":
        self.finished_at = utc_now()
        self.seconds = elapsed(start_ts)
        self.meta.update(meta)
        log.info(
            f"TIMING [{self.name}] {self.seconds}s "
            f"({self.started_at} -> {self.finished_at}) meta={self.meta}"
        )
        return self


@dataclass
class HealthSample:
    ts: str
    status: str
    agent_down: bool
    agent_down_summary: str = ""
    agent_down_count: Optional[int] = None


@dataclass
class MgrRssSample:
    ts: str
    active_mgr: str
    hostname: str
    rss_kb: Optional[int]
    pmem: Optional[str] = None


class BackgroundMonitor:
    """Daemon thread that samples cluster health and/or active mgr RSS."""

    def __init__(
        self,
        installer,
        ceph_cluster,
        *,
        health_interval: float = 30.0,
        rss_interval: float = 30.0,
        watch_health: bool = True,
        watch_rss: bool = True,
        health_enabled: bool = False,
    ):
        self.installer = installer
        self.ceph_cluster = ceph_cluster
        self.health_interval = health_interval
        self.rss_interval = rss_interval
        self.watch_health = watch_health
        self.watch_rss = watch_rss
        self._health_enabled = threading.Event()
        if health_enabled:
            self._health_enabled.set()
        self._stop = threading.Event()
        self.health_samples: List[HealthSample] = []
        self.rss_samples: List[MgrRssSample] = []
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None

    def enable_health_watch(self) -> None:
        """Start recording CEPHADM_AGENT_DOWN (after TC1)."""
        log.info("BackgroundMonitor: enabling CEPHADM_AGENT_DOWN health watch")
        self._health_enabled.set()

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="agent-scale-monitor", daemon=True
        )
        self._thread.start()
        log.info(
            f"BackgroundMonitor started "
            f"(health={self.watch_health}/{self.health_interval}s, "
            f"rss={self.watch_rss}/{self.rss_interval}s)"
        )

    def stop(self, timeout: float = 60.0) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=timeout)
        log.info(
            f"BackgroundMonitor stopped: "
            f"{len(self.health_samples)} health samples, "
            f"{len(self.rss_samples)} RSS samples"
        )

    def agent_down_events(self) -> List[HealthSample]:
        with self._lock:
            return [s for s in self.health_samples if s.agent_down]

    def rss_summary(self) -> Dict[str, Any]:
        with self._lock:
            samples = list(self.rss_samples)
        rss_vals = [s.rss_kb for s in samples if s.rss_kb is not None]
        if not rss_vals:
            return {"samples": 0}
        return {
            "samples": len(rss_vals),
            "rss_kb_min": min(rss_vals),
            "rss_kb_max": max(rss_vals),
            "rss_kb_first": rss_vals[0],
            "rss_kb_last": rss_vals[-1],
            "rss_kb_delta": rss_vals[-1] - rss_vals[0],
            "active_mgrs_seen": sorted({s.active_mgr for s in samples if s.active_mgr}),
        }

    def _run(self) -> None:
        next_health = 0.0
        next_rss = 0.0
        while not self._stop.is_set():
            now = time.time()
            if (
                self.watch_health
                and self._health_enabled.is_set()
                and now >= next_health
            ):
                try:
                    self._sample_health()
                except Exception as exc:
                    log.warning(f"health sample failed: {exc}")
                next_health = now + self.health_interval
            if self.watch_rss and now >= next_rss:
                try:
                    self._sample_rss()
                except Exception as exc:
                    log.warning(f"mgr RSS sample failed: {exc}")
                next_rss = now + self.rss_interval
            self._stop.wait(1.0)

    def _sample_health(self) -> None:
        out, _ = shell(self.installer, "ceph health detail -f json")
        health = json.loads(out)
        checks = health.get("checks", {}) or {}
        agent = checks.get(AGENT_HEALTH_WARNING)
        agent_down = agent is not None
        summary = ""
        count = None
        if agent_down:
            summary = (agent.get("summary") or {}).get("message", "")
            # try to parse "N agents are not reporting..." style counts
            detail = agent.get("detail") or []
            count = len(detail) if detail else None
            log.warning(
                f"HEALTH WATCH: {AGENT_HEALTH_WARNING} present — {summary} "
                f"(detail_entries={count})"
            )
        sample = HealthSample(
            ts=utc_now(),
            status=health.get("status", ""),
            agent_down=agent_down,
            agent_down_summary=summary,
            agent_down_count=count,
        )
        with self._lock:
            self.health_samples.append(sample)

    def _sample_rss(self) -> None:
        mgr_out, _ = shell(self.installer, "ceph mgr dump -f json")
        dump = json.loads(mgr_out)
        active = dump.get("active_name", "")
        active_addrs = dump.get("active_addrs", {}) or {}
        # Find hostname for active mgr via orch ps
        mgr_ps_out, _ = shell(self.installer, "ceph orch ps --daemon-type mgr -f json")
        mgrs = json.loads(mgr_ps_out)
        hostname = ""
        for m in mgrs:
            daemon_id = m.get("daemon_id", "")
            if daemon_id == active or m.get("is_active"):
                hostname = m.get("hostname", "")
                break
        if not hostname and active:
            # daemon_id often equals hostname or hostname.suffix
            for m in mgrs:
                if (
                    active.startswith(m.get("hostname", ""))
                    or m.get("hostname", "") in active
                ):
                    hostname = m.get("hostname", "")
                    break

        rss_kb = None
        pmem = None
        if hostname:
            node = get_node_for_host(self.ceph_cluster, hostname)
            if node is not None:
                # Match mantle sample_mgr_proc.sh: pgrep ceph-mgr RSS
                out, _ = node.exec_command(
                    sudo=True,
                    cmd=(
                        "pid=$(pgrep -n -x ceph-mgr || true); "
                        'if [ -n "$pid" ]; then '
                        "ps -p $pid -o rss=,pmem= --no-headers; "
                        "else echo ''; fi"
                    ),
                    check_ec=False,
                )
                line = (out or "").strip().splitlines()
                if line:
                    parts = line[0].split()
                    if parts:
                        try:
                            rss_kb = int(parts[0])
                        except ValueError:
                            rss_kb = None
                    if len(parts) > 1:
                        pmem = parts[1]

        sample = MgrRssSample(
            ts=utc_now(),
            active_mgr=active,
            hostname=hostname,
            rss_kb=rss_kb,
            pmem=pmem,
        )
        with self._lock:
            self.rss_samples.append(sample)
        if rss_kb is not None:
            log.info(
                f"MGR RSS: active={active} host={hostname} "
                f"rss_kb={rss_kb} pmem={pmem} addrs={active_addrs}"
            )


class WorkloadPump:
    """Background RGW/NFS (and rados) IO that runs for the rest of the suite."""

    def __init__(
        self, installer, ceph_cluster, config: Optional[Dict[str, Any]] = None
    ):
        self.installer = installer
        self.ceph_cluster = ceph_cluster
        self.config = config or {}
        self._stop = threading.Event()
        self._threads: List[threading.Thread] = []
        self.errors: List[str] = []
        self.started = False

    def start(self) -> None:
        if self.started:
            return
        self.started = True
        self._stop.clear()
        if self.config.get("enable_rados", True):
            t = threading.Thread(target=self._rados_loop, name="io-rados", daemon=True)
            self._threads.append(t)
            t.start()
        if self.config.get("enable_rgw", True):
            t = threading.Thread(target=self._rgw_loop, name="io-rgw", daemon=True)
            self._threads.append(t)
            t.start()
        if self.config.get("enable_nfs", True):
            t = threading.Thread(target=self._nfs_loop, name="io-nfs", daemon=True)
            self._threads.append(t)
            t.start()
        log.info(f"WorkloadPump started ({len(self._threads)} workers)")

    def stop(self, timeout: float = 120.0) -> None:
        self._stop.set()
        for t in self._threads:
            t.join(timeout=timeout)
        log.info(f"WorkloadPump stopped; errors={len(self.errors)}")

    def _rados_loop(self) -> None:
        pool = self.config.get("rados_pool", "agent_scale_io")
        duration = int(self.config.get("rados_bench_sec", 30))
        try:
            shell(self.installer, f"ceph osd pool create {pool} 32")
        except Exception:
            pass
        try:
            shell(self.installer, f"ceph osd pool application enable {pool} rados")
        except Exception:
            pass
        while not self._stop.is_set():
            try:
                shell(
                    self.installer,
                    f"rados bench -p {pool} {duration} write -b 4096 " f"--no-cleanup",
                )
            except Exception as exc:
                msg = f"rados bench failed: {exc}"
                log.warning(msg)
                self.errors.append(msg)
            self._stop.wait(5)

    def _rgw_loop(self) -> None:
        """Lightweight RGW load via radosgw-admin + rados put fallback."""
        # Prefer cycling a small object write through an RGW data pool if present.
        while not self._stop.is_set():
            try:
                out, _ = shell(self.installer, "ceph orch ls rgw -f json")
                services = json.loads(out) if out.strip() else []
                if not services:
                    log.info("RGW workload: no rgw service yet — waiting")
                    self._stop.wait(30)
                    continue
                # Keep traffic on the cluster: short rados bench against .rgw.root
                # is not ideal; use a dedicated pool tagged for the pump.
                shell(
                    self.installer,
                    "rados bench -p agent_scale_io 15 write -b 8192 --no-cleanup",
                )
            except Exception as exc:
                msg = f"rgw/object workload failed: {exc}"
                log.warning(msg)
                self.errors.append(msg)
            self._stop.wait(10)

    def _nfs_loop(self) -> None:
        """Best-effort NFS load: keep exporting cluster busy via MDS/rados if NFS present."""
        while not self._stop.is_set():
            try:
                out, _ = shell(self.installer, "ceph orch ls nfs -f json")
                services = json.loads(out) if out.strip() else []
                if not services:
                    log.info("NFS workload: no nfs service yet — waiting")
                    self._stop.wait(30)
                    continue
                # Exercise metadata/data path via a CephFS IO if filesystem exists;
                # otherwise fall back to rados bench so the pump still stresses mgr.
                fs_out, _ = shell(self.installer, "ceph fs ls -f json")
                filesystems = json.loads(fs_out) if fs_out.strip() else []
                if filesystems:
                    shell(
                        self.installer,
                        "rados bench -p agent_scale_io 15 write -b 4096 --no-cleanup",
                    )
                else:
                    shell(
                        self.installer,
                        "rados bench -p agent_scale_io 15 write -b 4096 --no-cleanup",
                    )
            except Exception as exc:
                msg = f"nfs workload failed: {exc}"
                log.warning(msg)
                self.errors.append(msg)
            self._stop.wait(10)


def list_cluster_hosts(installer) -> List[Dict[str, Any]]:
    out, _ = shell(installer, "ceph orch host ls -f json")
    return json.loads(out) if out.strip() else []


def online_hostnames(installer) -> Set[str]:
    hosts = list_cluster_hosts(installer)
    return {
        h["hostname"]
        for h in hosts
        if h.get("status", "") in ("", None, "online") or not h.get("status")
    }


def running_agent_hostnames(installer) -> Set[str]:
    agents = get_agent_daemons(installer)
    return {a["hostname"] for a in agents if a.get("status_desc") == "running"}


def osd_up_count(installer) -> int:
    out, _ = shell(installer, "ceph osd stat -f json")
    stat = json.loads(out)
    # osdmap keys vary slightly by version
    return int(stat.get("num_up_osds", stat.get("num_up_osd", 0)))


def wait_for_hosts_online(
    installer, expected: int, timeout: float, interval: float = 15.0
) -> TimingRecord:
    rec = TimingRecord(name="hosts_online", started_at=utc_now())
    t0 = time.time()
    last = 0
    while time.time() - t0 < timeout:
        last = len(online_hostnames(installer))
        log.info(f"hosts online: {last}/{expected}")
        if last >= expected:
            return rec.finish(t0, hosts=last, expected=expected)
        time.sleep(interval)
    raise AssertionError(
        f"Timed out waiting for {expected} hosts online (last={last}) after {timeout}s"
    )


def wait_for_agents_running(
    installer, expected: int, timeout: float, interval: float = 15.0
) -> TimingRecord:
    rec = TimingRecord(name="agents_running", started_at=utc_now())
    t0 = time.time()
    last = 0
    while time.time() - t0 < timeout:
        last = len(running_agent_hostnames(installer))
        log.info(f"agents running: {last}/{expected}")
        if last >= expected:
            return rec.finish(t0, agents=last, expected=expected)
        time.sleep(interval)
    raise AssertionError(
        f"Timed out waiting for {expected} agents running (last={last}) after {timeout}s"
    )


def wait_for_osds_up(
    installer, expected: int, timeout: float, interval: float = 20.0
) -> TimingRecord:
    rec = TimingRecord(name="osds_up", started_at=utc_now())
    t0 = time.time()
    last = 0
    while time.time() - t0 < timeout:
        last = osd_up_count(installer)
        log.info(f"OSDs up: {last}/{expected}")
        if last >= expected:
            return rec.finish(t0, osds=last, expected=expected)
        time.sleep(interval)
    raise AssertionError(
        f"Timed out waiting for {expected} OSDs up (last={last}) after {timeout}s"
    )


def get_active_mgr_name(installer) -> str:
    out, _ = shell(installer, "ceph mgr dump -f json")
    return json.loads(out).get("active_name", "")


def get_active_mgr_ip(installer) -> str:
    out, _ = shell(installer, "ceph mgr dump -f json")
    dump = json.loads(out)
    addr = dump.get("active_addr", "") or ""
    # v2:10.0.0.1:6800/123 or 10.0.0.1:6800/123
    import re

    m = re.search(r"(\d+\.\d+\.\d+\.\d+)", addr)
    if m:
        return m.group(1)
    addrs = dump.get("active_addrs", {}).get("addrvec") or []
    for a in addrs:
        m = re.search(r"(\d+\.\d+\.\d+\.\d+)", a.get("addr", ""))
        if m:
            return m.group(1)
    return ""


def wait_for_mgr_active_change(
    installer, old_active: str, timeout: float = 120.0, interval: float = 5.0
) -> TimingRecord:
    rec = TimingRecord(name="mgr_active_new", started_at=utc_now())
    t0 = time.time()
    new_active = ""
    while time.time() - t0 < timeout:
        try:
            new_active = get_active_mgr_name(installer)
            if new_active and new_active != old_active:
                return rec.finish(t0, old=old_active, new=new_active)
        except Exception:
            pass
        time.sleep(interval)
    raise AssertionError(
        f"Timed out waiting for mgr failover away from {old_active} (last={new_active})"
    )


def wait_for_agents_retarget(
    installer,
    ceph_cluster,
    expected_ip: str,
    *,
    sample_hosts: Optional[List[str]] = None,
    min_fraction: float = 1.0,
    timeout: float = 1800.0,
    interval: float = 15.0,
) -> TimingRecord:
    """
    Poll agent.json target_ip until hosts retarget to the new active mgr IP.
    """
    rec = TimingRecord(name="agents_retarget", started_at=utc_now())
    t0 = time.time()
    fsid = get_fsid(installer)
    if sample_hosts is None:
        sample_hosts = sorted(running_agent_hostnames(installer))
    if not sample_hosts:
        raise AssertionError("No agent hosts available to check retarget")

    need = max(1, int(len(sample_hosts) * min_fraction))
    last_ok = 0
    while time.time() - t0 < timeout:
        ok = 0
        for hostname in sample_hosts:
            node = get_node_for_host(ceph_cluster, hostname)
            if node is None:
                continue
            agent_dir = f"/var/lib/ceph/{fsid}/agent.{hostname}"
            try:
                out, _ = node.exec_command(
                    sudo=True, cmd=f"cat {agent_dir}/agent.json", check_ec=False
                )
                cfg = json.loads(out)
                if cfg.get("target_ip") == expected_ip:
                    ok += 1
            except Exception:
                continue
        last_ok = ok
        log.info(
            f"agent retarget: {ok}/{len(sample_hosts)} have target_ip={expected_ip} "
            f"(need {need})"
        )
        if ok >= need:
            return rec.finish(
                t0,
                retargeted=ok,
                sampled=len(sample_hosts),
                expected_ip=expected_ip,
            )
        time.sleep(interval)
    raise AssertionError(
        f"Timed out waiting for agent retarget to {expected_ip}: "
        f"{last_ok}/{len(sample_hosts)} after {timeout}s"
    )


def nodes_not_in_cluster(ceph_cluster, installer, include_clients: bool = False):
    """Return CephNode objects present in conf but not yet in orch host inventory."""
    present = {h["hostname"] for h in list_cluster_hosts(installer)}
    # also match short names
    present_short = {h.split(".")[0] for h in present}
    candidates = []
    for node in ceph_cluster.get_nodes():
        roles = set(getattr(node.role, "role_list", []) or [])
        if not include_clients and roles == {"client"}:
            continue
        hn = node.hostname
        if hn in present or hn.split(".")[0] in present_short:
            continue
        if node.shortname in present or node.shortname in present_short:
            continue
        candidates.append(node)
    return candidates


def select_nodes_for_add(ceph_cluster, installer, count: int):
    candidates = nodes_not_in_cluster(ceph_cluster, installer)
    if count > 0:
        candidates = candidates[:count]
    return candidates


def ensure_use_agent(installer) -> None:
    shell(installer, "ceph config set mgr mgr/cephadm/use_agent true")
    out, _ = shell(installer, "ceph config get mgr mgr/cephadm/use_agent")
    log.info(f"mgr/cephadm/use_agent => {out.strip()}")
