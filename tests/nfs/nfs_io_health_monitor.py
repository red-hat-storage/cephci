"""
NFS mount I/O health monitor for upgrade and long-running NFS test suites.

Deploys a shell script on each client that discovers all NFS mounts, runs
``touch`` + ``stat`` heartbeats, and writes timestamped logs locally. The test
runner tails those logs every 20s into the main CephCI log, copies full logs at
the end. **Stale mounts fail the test immediately.** **Stalls are tolerated** while the
client script polls for recovery for up to ``stall_recovery_timeout_s`` (default 100s);
only ``STALL_FAILED`` after that window marks the test failed.

Suite config keys:
    io_health_monitor (bool): enable monitoring (default False)
    stall_threshold_s (float): per-probe timeout / stall threshold (default 5)
    stall_recovery_timeout_s (float): poll after stall before failure (default 100)
    probe_interval_s (float): client script probe round interval (default 1)
    heartbeat_tail_interval_s (float): tail remote logs into main log (default 20)
    heartbeat_log_tail_lines (int): lines shown per client tail in main log (default 20)
    heartbeat_filename (str): heartbeat file under each mount (default .nfs_io_heartbeat)
    io_health_monitor_mount_prefix (str): probe mounts under this path (default /mnt)
"""

from __future__ import annotations

import os
import re
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

_REPO_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from cli.exceptions import OperationFailedError
from utility.config import TestMetaData
from utility.log import Log

log = Log(__name__)

DEFAULT_HEARTBEAT_FILENAME = ".nfs_io_heartbeat"
DEFAULT_STALL_THRESHOLD_S = 5.0
DEFAULT_STALL_RECOVERY_TIMEOUT_S = 100.0
DEFAULT_PROBE_INTERVAL_S = 1.0
DEFAULT_HEARTBEAT_TAIL_INTERVAL_S = 20.0
DEFAULT_HEALTH_POLL_INTERVAL_S = 2.0
DEFAULT_LOG_TAIL_LINES = 20
DEFAULT_MOUNT_PATH_PREFIX = "/mnt"
REMOTE_SCRIPT_PATH = "/tmp/cephci_nfs_io_heartbeat.sh"
REMOTE_LOG_PATH = "/tmp/cephci_nfs_io_heartbeat.log"
REMOTE_CONTROL_PATH = "/tmp/cephci_nfs_io_heartbeat.control"
REMOTE_PID_PATH = "/tmp/cephci_nfs_io_heartbeat.pid"
HEARTBEAT_LOG_SUBDIR = "nfs_heartbeat"

_SCRIPT_LOCAL_PATH = os.path.join(
    os.path.dirname(__file__), "shell_scripts", "nfs_io_heartbeat.sh"
)

_TRUTHY_STRINGS = frozenset(("true", "1", "yes", "on"))
_STALE_LINE_RE = re.compile(r"\bSTALE\b")
_STALL_FAILED_LINE_RE = re.compile(r"\bSTALL_FAILED\b")
_STALL_LINE_RE = re.compile(r"\bSTALL mount=")


def parse_config_bool(config: Optional[dict], key: str, default: bool = False) -> bool:
    """Parse a suite config flag without treating arbitrary strings as True."""
    if not config:
        return default
    value = config.get(key, default)
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in _TRUTHY_STRINGS
    if isinstance(value, (int, float)):
        return value != 0
    return bool(value)


def io_health_monitor_enabled(config: Optional[dict] = None) -> bool:
    """Return True only when ``io_health_monitor`` is explicitly enabled in config."""
    return parse_config_bool(config, "io_health_monitor", default=False)


class NfsIoHealthSlaBreached(OperationFailedError):
    """Raised when NFS mount I/O health checks fail SLA (legacy alias)."""


class NfsIoStaleMountError(OperationFailedError):
    """Raised immediately when a stale NFS mount is detected."""


class NfsIoStallFailedError(OperationFailedError):
    """Raised when a mount stall does not recover within the recovery timeout."""


@dataclass
class MountProbeTarget:
    """One NFS mount tracked for reporting (probing runs on the client script)."""

    client: Any
    mount_path: str
    sudo: bool = True
    label: Optional[str] = None

    @property
    def hostname(self) -> str:
        return getattr(self.client, "hostname", str(self.client))

    @property
    def identity(self) -> str:
        if self.label:
            return self.label
        return f"{self.hostname}:{self.mount_path}"


@dataclass
class _MonitorState:
    ok_count: int = 0
    stall_count: int = 0
    stale_count: int = 0
    stall_failed_count: int = 0
    error_count: int = 0
    client_ok_counts: Dict[str, int] = field(default_factory=dict)


def resolve_log_dir(
    log_dir: Optional[str] = None,
    config: Optional[dict] = None,
    run_config: Optional[dict] = None,
) -> str:
    """Resolve the directory for heartbeat log artifacts."""
    if log_dir:
        return log_dir
    if config and config.get("log_dir"):
        return str(config["log_dir"])
    if run_config and run_config.get("log_dir"):
        return str(run_config["log_dir"])
    run_meta = TestMetaData()
    for key in ("run_dir", "log_dir"):
        value = run_meta.get(key)
        if value:
            return str(value)
    return os.getcwd()


def settings_from_config(
    config: Optional[dict] = None,
    run_config: Optional[dict] = None,
) -> Dict[str, Any]:
    """Parse monitor settings from a CephCI test ``config`` dict."""
    config = config or {}
    return {
        "enabled": io_health_monitor_enabled(config),
        "stall_threshold_s": float(
            config.get("stall_threshold_s", DEFAULT_STALL_THRESHOLD_S)
        ),
        "stall_recovery_timeout_s": float(
            config.get("stall_recovery_timeout_s", DEFAULT_STALL_RECOVERY_TIMEOUT_S)
        ),
        "probe_interval_s": float(
            config.get("probe_interval_s", DEFAULT_PROBE_INTERVAL_S)
        ),
        "heartbeat_filename": str(
            config.get("heartbeat_filename", DEFAULT_HEARTBEAT_FILENAME)
        ),
        "heartbeat_tail_interval_s": float(
            config.get("heartbeat_tail_interval_s", DEFAULT_HEARTBEAT_TAIL_INTERVAL_S)
        ),
        "health_poll_interval_s": float(
            config.get("health_poll_interval_s", DEFAULT_HEALTH_POLL_INTERVAL_S)
        ),
        "log_tail_lines": int(
            config.get("heartbeat_log_tail_lines", DEFAULT_LOG_TAIL_LINES)
        ),
        "mount_path_prefix": str(
            config.get("io_health_monitor_mount_prefix", DEFAULT_MOUNT_PATH_PREFIX)
        ),
        "log_dir": resolve_log_dir(config=config, run_config=run_config),
    }


def build_probe_targets_from_export_mount_dict(
    client_export_mount_dict: dict,
    clients: Sequence[Any],
    sudo: bool = True,
) -> List[MountProbeTarget]:
    """Build probe targets from ``exports_mounts_perclient`` layout."""
    targets: List[MountProbeTarget] = []
    for client in clients:
        mounts = client_export_mount_dict.get(client, {}).get("mount", [])
        for mount_path in mounts:
            targets.append(
                MountProbeTarget(client=client, mount_path=mount_path, sudo=sudo)
            )
    return targets


def build_probe_targets_from_pairs(
    client_mount_pairs: Sequence[tuple],
    sudo: bool = True,
) -> List[MountProbeTarget]:
    """Build targets from ``(client, mount_path)`` or ``(client, mount_path, sudo)`` tuples."""
    targets: List[MountProbeTarget] = []
    for entry in client_mount_pairs:
        if len(entry) == 2:
            client, mount_path = entry
            entry_sudo = sudo
        elif len(entry) >= 3:
            client, mount_path, entry_sudo = entry[0], entry[1], entry[2]
        else:
            raise ValueError(
                "Each pair must be (client, mount_path) or (client, mount_path, sudo)"
            )
        targets.append(
            MountProbeTarget(client=client, mount_path=mount_path, sudo=entry_sudo)
        )
    return targets


def _normalize_mount_path(mount_path: str) -> str:
    return mount_path.rstrip("/") or mount_path


def _mount_matches_prefix(mount_path: str, mount_path_prefix: Optional[str]) -> bool:
    if not mount_path_prefix:
        return True
    prefix = mount_path_prefix.rstrip("/")
    path = _normalize_mount_path(mount_path)
    return path == prefix or path.startswith(prefix + "/")


def discover_nfs_mount_paths_on_client(
    client: Any,
    sudo: bool = True,
    mount_path_prefix: Optional[str] = DEFAULT_MOUNT_PATH_PREFIX,
) -> List[str]:
    """Discover active NFS/NFSv4 mount points on a client via ``findmnt``."""
    mounts: List[str] = []
    for fstype in ("nfs", "nfs4"):
        try:
            out, _ = client.exec_command(
                sudo=sudo,
                cmd=f"findmnt -rn -t {fstype} -o TARGET 2>/dev/null",
                timeout=30,
            )
        except Exception as err:
            log.debug(
                "findmnt -t %s failed on %s: %s",
                fstype,
                getattr(client, "hostname", client),
                err,
            )
            continue
        raw = out if isinstance(out, str) else (out[0] if out else "")
        for line in raw.splitlines():
            path = _normalize_mount_path(line.strip())
            if not path:
                continue
            if _mount_matches_prefix(path, mount_path_prefix):
                mounts.append(path)
    return sorted(set(mounts))


def discover_all_cluster_nfs_mount_targets(
    clients: Sequence[Any],
    sudo: bool = True,
    mount_path_prefix: Optional[str] = DEFAULT_MOUNT_PATH_PREFIX,
    client_export_mount_dict: Optional[dict] = None,
) -> List[MountProbeTarget]:
    """Build targets for every NFS mount on clients (for reporting / discovery)."""
    targets: List[MountProbeTarget] = []
    seen: set[str] = set()

    if client_export_mount_dict:
        for target in build_probe_targets_from_export_mount_dict(
            client_export_mount_dict, clients, sudo=sudo
        ):
            if target.identity not in seen:
                seen.add(target.identity)
                targets.append(target)

    for client in clients:
        for mount_path in discover_nfs_mount_paths_on_client(
            client, sudo=sudo, mount_path_prefix=mount_path_prefix
        ):
            target = MountProbeTarget(client=client, mount_path=mount_path, sudo=sudo)
            if target.identity not in seen:
                seen.add(target.identity)
                targets.append(target)

    return targets


def refresh_cluster_mount_targets(
    monitor: Optional["NfsIoHealthMonitor"],
    clients: Sequence[Any],
    sudo: bool = True,
    client_export_mount_dict: Optional[dict] = None,
    mount_path_prefix: Optional[str] = DEFAULT_MOUNT_PATH_PREFIX,
) -> int:
    """
    Re-discover NFS mounts and ensure client heartbeat scripts are running.

    The client shell script discovers mounts each round; this refreshes the
    reported target list and starts scripts if needed.

    Returns:
        Number of discovered mounts.
    """
    if monitor is None:
        return 0

    prefix = mount_path_prefix or monitor.mount_path_prefix
    targets = discover_all_cluster_nfs_mount_targets(
        clients,
        sudo=sudo,
        mount_path_prefix=prefix,
        client_export_mount_dict=client_export_mount_dict,
    )
    monitor.sync_mount_targets(targets)
    monitor.ensure_client_scripts()
    return len(targets)


def create_paused_upgrade_io_monitor(
    config: Optional[dict],
    clients: Sequence[Any],
    sudo: bool = True,
    mount_path_prefix: Optional[str] = None,
    run_config: Optional[dict] = None,
) -> Optional["NfsIoHealthMonitor"]:
    """
    Start a paused I/O health monitor for upgrade loops when config enables it.

    Client scripts discover NFS mounts under ``io_health_monitor_mount_prefix``.
    Call ``refresh_cluster_mount_targets`` after exports are mounted each loop.

    Returns ``None`` when ``io_health_monitor`` is not set (default).
    """
    if not io_health_monitor_enabled(config):
        return None

    settings = settings_from_config(config, run_config=run_config)
    prefix = mount_path_prefix or settings["mount_path_prefix"]
    unique_clients = list(dict.fromkeys(clients))

    monitor = NfsIoHealthMonitor(
        clients=unique_clients,
        sudo=sudo,
        log_dir=settings["log_dir"],
        stall_threshold_s=settings["stall_threshold_s"],
        stall_recovery_timeout_s=settings["stall_recovery_timeout_s"],
        probe_interval_s=settings["probe_interval_s"],
        heartbeat_filename=settings["heartbeat_filename"],
        heartbeat_tail_interval_s=settings["heartbeat_tail_interval_s"],
        mount_path_prefix=prefix,
        config=config,
    )

    log.info(
        "NFS I/O health monitor enabled on %d client(s); client scripts will "
        "discover mounts under %s; heartbeat artifacts under %s/%s/",
        len(unique_clients),
        prefix,
        monitor.log_dir,
        HEARTBEAT_LOG_SUBDIR,
    )
    monitor.start()
    monitor.pause()
    return monitor


def finalize_upgrade_io_monitor(
    monitor: Optional["NfsIoHealthMonitor"],
    raise_on_breach: bool = True,
    reason: str = "complete",
) -> bool:
    """
    Stop client scripts, tail/copy logs, and evaluate failures.

    Safe to call from ``finally`` after test errors.
    """
    if monitor is None:
        return True

    if getattr(monitor, "_finalized", False):
        return monitor.check_health(raise_on_failure=raise_on_breach)

    monitor.stop(reason=reason)
    monitor.log_status_summary(reason=reason)
    log.info(
        "NFS I/O heartbeat client logs: %s/%s/",
        monitor.log_dir,
        HEARTBEAT_LOG_SUBDIR,
    )
    monitor._finalized = True
    return monitor.check_health(raise_on_failure=raise_on_breach)


class NfsIoHealthMonitor:
    """
    Orchestrates per-client NFS heartbeat shell scripts with log tailing.

    Each client runs ``shell_scripts/nfs_io_heartbeat.sh`` which logs timestamped probe
    results locally. This class tails those logs into the main test log every
    20s and copies them into the run log directory at shutdown.
    """

    def __init__(
        self,
        clients: Sequence[Any],
        sudo: bool = True,
        log_dir: Optional[str] = None,
        stall_threshold_s: float = DEFAULT_STALL_THRESHOLD_S,
        stall_recovery_timeout_s: float = DEFAULT_STALL_RECOVERY_TIMEOUT_S,
        probe_interval_s: float = DEFAULT_PROBE_INTERVAL_S,
        heartbeat_filename: str = DEFAULT_HEARTBEAT_FILENAME,
        heartbeat_tail_interval_s: float = DEFAULT_HEARTBEAT_TAIL_INTERVAL_S,
        health_poll_interval_s: float = DEFAULT_HEALTH_POLL_INTERVAL_S,
        log_tail_lines: int = DEFAULT_LOG_TAIL_LINES,
        mount_path_prefix: str = DEFAULT_MOUNT_PATH_PREFIX,
        config: Optional[dict] = None,
    ):
        self.clients = list(clients)
        self.sudo = sudo
        self.log_dir = resolve_log_dir(log_dir=log_dir, config=config)
        self.stall_threshold_s = float(stall_threshold_s)
        self.stall_recovery_timeout_s = float(stall_recovery_timeout_s)
        self.probe_interval_s = float(probe_interval_s)
        self.heartbeat_filename = heartbeat_filename
        self.heartbeat_tail_interval_s = float(heartbeat_tail_interval_s)
        self.health_poll_interval_s = float(health_poll_interval_s)
        self.log_tail_lines = int(log_tail_lines)
        self.mount_path_prefix = str(mount_path_prefix)

        self.targets: List[MountProbeTarget] = []
        self._state = _MonitorState()
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._tail_thread: Optional[threading.Thread] = None
        self._health_thread: Optional[threading.Thread] = None
        self._started_at: Optional[datetime] = None
        self._ended_at: Optional[datetime] = None
        self._paused = False
        self._finalized = False
        self._failure: Optional[BaseException] = None
        self._scripts_deployed: set[str] = set()
        self._seen_log_lines: set[str] = set()

    @classmethod
    def from_config(
        cls,
        clients: Sequence[Any],
        config: Optional[dict] = None,
        sudo: bool = True,
    ) -> "NfsIoHealthMonitor":
        """Construct a monitor using ``settings_from_config``."""
        settings = settings_from_config(config)
        return cls(
            clients=clients,
            sudo=sudo,
            log_dir=settings["log_dir"],
            stall_threshold_s=settings["stall_threshold_s"],
            stall_recovery_timeout_s=settings["stall_recovery_timeout_s"],
            probe_interval_s=settings["probe_interval_s"],
            heartbeat_filename=settings["heartbeat_filename"],
            heartbeat_tail_interval_s=settings["heartbeat_tail_interval_s"],
            health_poll_interval_s=settings["health_poll_interval_s"],
            log_tail_lines=settings["log_tail_lines"],
            mount_path_prefix=settings["mount_path_prefix"],
            config=config,
        )

    @property
    def has_failed(self) -> bool:
        with self._lock:
            return self._failure is not None

    @property
    def is_paused(self) -> bool:
        with self._lock:
            return self._paused

    @property
    def is_running(self) -> bool:
        tail_alive = self._tail_thread is not None and self._tail_thread.is_alive()
        health_alive = self._health_thread is not None and self._health_thread.is_alive()
        return tail_alive or health_alive

    def raise_if_unhealthy(self) -> None:
        """
        Raise when the monitor recorded a test failure.

        Transient ``STALL`` events are OK (client script polls up to 100s).
        Raises only for ``STALE`` (immediate) or ``STALL_FAILED`` (after 100s).
        """
        with self._lock:
            failure = self._failure
        if failure is not None:
            raise failure

    def sync_mount_targets(self, targets: Sequence[MountProbeTarget]) -> int:
        """Update the discovered mount list used in reports."""
        self.targets = list(targets)
        log.info(
            "NfsIoHealthMonitor synced mount targets: %d mount(s) discovered",
            len(self.targets),
        )
        return len(self.targets)

    def pause(self) -> None:
        """Pause client heartbeat scripts (e.g. before unmount)."""
        with self._lock:
            self._paused = True
        for client in self.clients:
            self._set_client_control(client, "PAUSE")
        log.info("NfsIoHealthMonitor paused")

    def resume(self) -> None:
        """Resume client heartbeat scripts after mounts are available."""
        with self._lock:
            self._paused = False
        self.ensure_client_scripts()
        for client in self.clients:
            self._set_client_control(client, "RUN")
        log.info("NfsIoHealthMonitor resumed")

    def ensure_heartbeats_initialized(self) -> None:
        """Ensure client scripts are deployed and running (heartbeats created by script)."""
        self.ensure_client_scripts()

    def ensure_client_scripts(self) -> None:
        """Deploy and start heartbeat scripts on all clients if not already running."""
        for client in self.clients:
            hostname = getattr(client, "hostname", str(client))
            if not self._is_client_script_running(client):
                self._deploy_script(client)
                self._start_client_script(client)
                self._scripts_deployed.add(hostname)
                log.info("NFS heartbeat script started on %s", hostname)

    def start(self) -> None:
        """Start the background log-tail thread."""
        if self.is_running:
            log.warning("NfsIoHealthMonitor already running; ignoring start()")
            return

        os.makedirs(self.log_dir, exist_ok=True)
        os.makedirs(os.path.join(self.log_dir, HEARTBEAT_LOG_SUBDIR), exist_ok=True)
        self._stop_event.clear()
        self._started_at = datetime.now(timezone.utc)

        self._tail_thread = threading.Thread(
            target=self._tail_loop,
            name="nfs-io-health-tail",
            daemon=True,
        )
        self._health_thread = threading.Thread(
            target=self._health_watch_loop,
            name="nfs-io-health-watch",
            daemon=True,
        )
        self._tail_thread.start()
        self._health_thread.start()
        log.info(
            "NfsIoHealthMonitor tail thread started; log_dir=%s "
            "stall_threshold_s=%s stall_recovery_timeout_s=%s "
            "probe_interval_s=%s tail_interval_s=%s health_poll_interval_s=%s "
            "log_tail_lines=%s",
            self.log_dir,
            self.stall_threshold_s,
            self.stall_recovery_timeout_s,
            self.probe_interval_s,
            self.heartbeat_tail_interval_s,
            self.health_poll_interval_s,
            self.log_tail_lines,
        )

    def stop(self, reason: str = "complete") -> None:
        """Stop tail thread, client scripts, and copy client heartbeat logs."""
        self._stop_event.set()
        join_timeout = max(
            self.heartbeat_tail_interval_s, self.health_poll_interval_s
        ) + 30
        if self._tail_thread is not None:
            self._tail_thread.join(timeout=join_timeout)
            if self._tail_thread.is_alive():
                log.warning("NfsIoHealthMonitor tail thread did not stop cleanly")
            self._tail_thread = None
        if self._health_thread is not None:
            self._health_thread.join(timeout=join_timeout)
            if self._health_thread.is_alive():
                log.warning("NfsIoHealthMonitor health thread did not stop cleanly")
            self._health_thread = None

        for client in self.clients:
            self._stop_client_script(client)
            self._tail_client_log(client, final=True, reason=reason)
            self._copy_client_log(client)

        self._ended_at = datetime.now(timezone.utc)

    def log_status_summary(self, reason: str = "complete") -> None:
        """Log heartbeat summary for triage."""
        with self._lock:
            state = self._state
            failure = self._failure
            summary = (
                "NFS I/O heartbeat status (%s): clients=%d mounts=%d ok=%d "
                "stall=%d stale=%d stall_failed=%d errors=%d failure=%s"
            )
            args = (
                reason,
                len(self.clients),
                len(self.targets),
                state.ok_count,
                state.stall_count,
                state.stale_count,
                state.stall_failed_count,
                state.error_count,
                type(failure).__name__ if failure else "none",
            )
            if failure or state.stale_count or state.stall_failed_count:
                log.error(summary, *args)
            else:
                log.info(summary, *args)
            self._log_client_heartbeat_counts()

    def _log_client_heartbeat_counts(self) -> None:
        """Log per-client OK heartbeat totals and which client recorded the most."""
        with self._lock:
            counts = dict(self._state.client_ok_counts)

        if not counts:
            log.info("NFS I/O heartbeat per client: no OK heartbeats recorded yet")
            return

        ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
        breakdown = ", ".join(f"{hostname}={count}" for hostname, count in ordered)
        leader_host, leader_count = ordered[0]
        log.info("NFS I/O heartbeat per client: %s", breakdown)
        log.info(
            "NFS I/O heartbeat leader: %s (%d OK heartbeats)",
            leader_host,
            leader_count,
        )

    def check_health(self, raise_on_failure: bool = True) -> bool:
        """
        Evaluate monitor health after stop.

        Raises:
            NfsIoStaleMountError: stale mount (immediate test failure).
            NfsIoStallFailedError: stall not recovered within ``stall_recovery_timeout_s``.
        """
        with self._lock:
            failure = self._failure
        if failure:
            log.error("NFS I/O health check failed: %s", failure)
            if raise_on_failure:
                raise failure
            return False
        return True

    def check_sla(self, raise_on_breach: bool = True) -> bool:
        """Legacy alias for ``check_health``."""
        return self.check_health(raise_on_failure=raise_on_breach)

    def _deploy_script(self, client: Any) -> None:
        with open(_SCRIPT_LOCAL_PATH, encoding="utf-8") as handle:
            script_body = handle.read()
        remote = client.remote_file(
            sudo=self.sudo,
            file_name=REMOTE_SCRIPT_PATH,
            file_mode="w",
        )
        remote.write(script_body)
        remote.flush()
        client.exec_command(
            sudo=self.sudo,
            cmd=f"chmod +x {REMOTE_SCRIPT_PATH}",
            timeout=30,
        )

    def _client_start_cmd(self) -> str:
        return (
            f"rm -f {REMOTE_CONTROL_PATH}; "
            f"echo RUN > {REMOTE_CONTROL_PATH}; "
            f"nohup env "
            f"NFS_IO_LOG_FILE={REMOTE_LOG_PATH} "
            f"NFS_IO_CONTROL_FILE={REMOTE_CONTROL_PATH} "
            f"NFS_IO_PROBE_INTERVAL_S={self.probe_interval_s} "
            f"NFS_IO_STALL_THRESHOLD_S={self.stall_threshold_s} "
            f"NFS_IO_STALL_RECOVERY_S={self.stall_recovery_timeout_s} "
            f"NFS_IO_MOUNT_PREFIX={self.mount_path_prefix} "
            f"NFS_IO_HEARTBEAT_FILE={self.heartbeat_filename} "
            f"{REMOTE_SCRIPT_PATH} >/dev/null 2>&1 & "
            f"echo $! > {REMOTE_PID_PATH}"
        )

    def _start_client_script(self, client: Any) -> None:
        client.exec_command(
            sudo=self.sudo,
            cmd=self._client_start_cmd(),
            timeout=60,
        )

    def _stop_client_script(self, client: Any) -> None:
        self._set_client_control(client, "STOP")
        client.exec_command(
            sudo=self.sudo,
            cmd=(
                f"if [ -f {REMOTE_PID_PATH} ]; then "
                f"kill $(cat {REMOTE_PID_PATH}) 2>/dev/null || true; "
                f"rm -f {REMOTE_PID_PATH}; fi"
            ),
            timeout=30,
            check_ec=False,
        )

    def _set_client_control(self, client: Any, mode: str) -> None:
        client.exec_command(
            sudo=self.sudo,
            cmd=f"echo {mode} > {REMOTE_CONTROL_PATH}",
            timeout=30,
            check_ec=False,
        )

    def _is_client_script_running(self, client: Any) -> bool:
        try:
            out, _ = client.exec_command(
                sudo=self.sudo,
                cmd=(
                    f"if [ -f {REMOTE_PID_PATH} ]; then "
                    f"pid=$(cat {REMOTE_PID_PATH}); "
                    f"if kill -0 $pid 2>/dev/null; then echo running; "
                    f"else echo stopped; fi; else echo stopped; fi"
                ),
                timeout=30,
                check_ec=False,
            )
            raw = out if isinstance(out, str) else (out[0] if out else "")
            return "running" in raw
        except Exception as err:
            log.debug(
                "Could not check heartbeat script on %s: %s",
                getattr(client, "hostname", client),
                err,
            )
            return False

    def _tail_loop(self) -> None:
        while not self._stop_event.wait(timeout=self.heartbeat_tail_interval_s):
            if self.is_paused or self.has_failed:
                continue
            for client in self.clients:
                if self._stop_event.is_set() or self.has_failed:
                    break
                self._tail_client_log(client, final=False)

    def _health_watch_loop(self) -> None:
        """Fast poll for stale mounts / script exit between periodic log tails."""
        while not self._stop_event.wait(timeout=self.health_poll_interval_s):
            if self.is_paused or self.has_failed:
                continue
            for client in self.clients:
                if self._stop_event.is_set() or self.has_failed:
                    break
                self._quick_health_check(client)

    def _quick_health_check(self, client: Any) -> None:
        hostname = getattr(client, "hostname", str(client))
        try:
            out, _ = client.exec_command(
                sudo=self.sudo,
                cmd=f"tail -n {self.log_tail_lines} {REMOTE_LOG_PATH} 2>/dev/null || true",
                timeout=60,
                check_ec=False,
            )
            raw = out if isinstance(out, str) else (out[0] if out else "")
            if raw.strip():
                self._parse_log_lines(raw, hostname)
            if self.has_failed:
                return
            self._check_client_script_exit(client)
        except Exception as err:
            log.debug("Health poll failed on %s: %s", hostname, err)

    def _tail_client_log(
        self,
        client: Any,
        final: bool = False,
        reason: str = "complete",
    ) -> None:
        hostname = getattr(client, "hostname", str(client))
        label = "final tail" if final else "tail"
        try:
            out, _ = client.exec_command(
                sudo=self.sudo,
                cmd=f"tail -n {self.log_tail_lines} {REMOTE_LOG_PATH} 2>/dev/null || true",
                timeout=120,
            )
            raw = out if isinstance(out, str) else (out[0] if out else "")
            if raw.strip():
                log.info(
                    "NFS I/O heartbeat %s [%s] (%s):\n%s",
                    label,
                    hostname,
                    reason if final else "periodic",
                    raw,
                )
                self._parse_log_lines(raw, hostname)
        except Exception as err:
            log.warning("Failed to tail heartbeat log on %s: %s", hostname, err)

    def _copy_client_log(self, client: Any) -> None:
        hostname = getattr(client, "hostname", str(client))
        dest_dir = os.path.join(self.log_dir, HEARTBEAT_LOG_SUBDIR)
        os.makedirs(dest_dir, exist_ok=True)
        dest_path = os.path.join(dest_dir, f"{hostname}.log")
        try:
            client.download_file(
                src=REMOTE_LOG_PATH,
                dst=dest_path,
                sudo=self.sudo,
            )
            log.info("NFS heartbeat log copied from %s to %s", hostname, dest_path)
        except Exception as err:
            log.warning(
                "Failed to copy heartbeat log from %s: %s",
                hostname,
                err,
            )

    def _check_client_script_exit(self, client: Any) -> None:
        if self._is_client_script_running(client):
            return

        hostname = getattr(client, "hostname", str(client))
        try:
            out, _ = client.exec_command(
                sudo=self.sudo,
                cmd=f"tail -n {self.log_tail_lines} {REMOTE_LOG_PATH} 2>/dev/null || true",
                timeout=60,
                check_ec=False,
            )
            raw = out if isinstance(out, str) else (out[0] if out else "")
            if raw.strip():
                log.error(
                    "NFS heartbeat script stopped on %s; recent log:\n%s",
                    hostname,
                    raw,
                )
                self._parse_log_lines(raw, hostname)
        except Exception as err:
            log.warning(
                "Failed to read heartbeat log after script exit on %s: %s",
                hostname,
                err,
            )

        with self._lock:
            if self._failure is None:
                log.warning(
                    "NFS heartbeat script stopped on %s without STALE/STALL_FAILED "
                    "in recent log; not failing test (stalls are tolerated up to %.0fs)",
                    hostname,
                    self.stall_recovery_timeout_s,
                )

    def _parse_log_lines(self, text: str, hostname: str) -> None:
        for line in text.splitlines():
            self._parse_log_line(line, hostname)

    def _parse_log_line(self, line: str, hostname: str) -> None:
        if not line.strip():
            return

        event_type = None
        if _STALE_LINE_RE.search(line):
            event_type = "stale"
        elif _STALL_FAILED_LINE_RE.search(line):
            event_type = "stall_failed"
        elif _STALL_LINE_RE.search(line):
            event_type = "stall"
        elif " STALL_RECOVERED " in line or " STALL_RECOVERED mount=" in line:
            event_type = "stall_recovered"
        elif " OK " in line or line.endswith(" OK"):
            event_type = "ok"
        elif " ERROR " in line:
            event_type = "error"

        if not event_type:
            return

        dedup_key = f"{hostname}:{line.strip()}"
        is_critical = event_type in ("stale", "stall_failed")
        with self._lock:
            if not is_critical and dedup_key in self._seen_log_lines:
                return
            if not is_critical:
                self._seen_log_lines.add(dedup_key)

            state = self._state
            if event_type == "ok":
                state.ok_count += 1
                state.client_ok_counts[hostname] = (
                    state.client_ok_counts.get(hostname, 0) + 1
                )
            elif event_type == "stall":
                state.stall_count += 1
                log.info(
                    "NFS mount stall on %s (tolerated up to %.0fs for recovery): %s",
                    hostname,
                    self.stall_recovery_timeout_s,
                    line,
                )
            elif event_type == "stall_recovered":
                log.info("NFS mount stall recovered on %s: %s", hostname, line)
            elif event_type == "stale":
                state.stale_count += 1
                self._fail_test_on_stale_mount(
                    NfsIoStaleMountError(
                        f"Stale NFS mount detected on {hostname}: {line}"
                    )
                )
            elif event_type == "stall_failed":
                state.stall_failed_count += 1
                self._fail_test_on_stall_timeout(
                    NfsIoStallFailedError(
                        f"NFS mount stall did not recover within "
                        f"{self.stall_recovery_timeout_s}s on {hostname}: {line}"
                    )
                )
            elif event_type == "error":
                state.error_count += 1

    def _fail_test_on_stale_mount(self, exc: NfsIoStaleMountError) -> None:
        """Stale mount — fail the whole test immediately."""
        if self._failure is not None:
            return
        self._failure = exc
        self._stop_event.set()
        log.error(
            "Stale NFS mount — failing test immediately: %s",
            exc,
        )
        self._stop_all_client_scripts()

    def _fail_test_on_stall_timeout(self, exc: NfsIoStallFailedError) -> None:
        """Stall persisted beyond recovery window — fail the test."""
        if self._failure is not None:
            return
        self._failure = exc
        self._stop_event.set()
        log.error(
            "NFS mount stall not recovered within %.0fs — failing test: %s",
            self.stall_recovery_timeout_s,
            exc,
        )
        self._stop_all_client_scripts()

    def _stop_all_client_scripts(self) -> None:
        for client in self.clients:
            try:
                self._stop_client_script(client)
            except Exception as err:
                log.debug(
                    "Failed to stop heartbeat script on %s: %s",
                    getattr(client, "hostname", client),
                    err,
                )

    def _set_failure(self, exc: BaseException) -> None:
        if self._failure is None:
            self._failure = exc


def run_monitor_self_check() -> int:
    """Offline self-check for config parsing and log classification."""
    if not io_health_monitor_enabled({"io_health_monitor": True}):
        log.error("Self-check failed: True must enable monitor")
        return 1
    if io_health_monitor_enabled(None):
        log.error("Self-check failed: None config must default to disabled")
        return 1

    class _FakeClient:
        def __init__(self, hostname: str):
            self.hostname = hostname
            self.log_lines: List[str] = []
            self.running = True
            self.deployed = False

        def remote_file(self, sudo=False, file_name="", file_mode="w"):
            self.deployed = True

            class _Writer:
                def write(self, _data):
                    return None

                def flush(self):
                    return None

            return _Writer()

        def exec_command(self, sudo=False, cmd="", timeout=None, check_ec=True):
            if "chmod" in cmd:
                return "", ""
            if "nohup env" in cmd:
                self.running = True
                return "", ""
            if "echo STOP" in cmd or "kill" in cmd:
                self.running = False
                return "", ""
            if "kill -0" in cmd or "echo running" in cmd:
                status = "running" if self.running else "stopped"
                return status, ""
            if "tail" in cmd:
                return "\n".join(self.log_lines), ""
            return "", ""

        def download_file(self, src, dst, sudo=False):
            os.makedirs(os.path.dirname(dst) or ".", exist_ok=True)
            with open(dst, "w", encoding="utf-8") as handle:
                handle.write("\n".join(self.log_lines))

    client = _FakeClient("node4")
    client.log_lines = [
        "2026-08-18T10:00:00+00:00 OK mount=/mnt/nfs_a",
        "2026-08-18T10:00:01 STALL mount=/mnt/nfs_b latency>=5s",
        "2026-08-18T10:00:02 STALL_RECOVERED mount=/mnt/nfs_b elapsed_s=2",
    ]

    log_dir = os.path.join(os.getcwd(), ".nfs_io_health_monitor_self_check")
    monitor = NfsIoHealthMonitor(
        clients=[client],
        log_dir=log_dir,
        stall_threshold_s=5,
        stall_recovery_timeout_s=100,
        probe_interval_s=1,
        heartbeat_tail_interval_s=0.05,
    )
    monitor.sync_mount_targets(
        [MountProbeTarget(client=client, mount_path="/mnt/nfs_a")]
    )
    monitor.start()
    monitor.resume()
    monitor._tail_client_log(client, final=False)
    monitor.stop(reason="self_check")

    if not client.deployed:
        log.error("Self-check failed: script was not deployed")
        return 1

    copied = os.path.join(log_dir, HEARTBEAT_LOG_SUBDIR, "node4.log")
    if not os.path.isfile(copied):
        log.error("Self-check failed: client log not copied")
        return 1

    with monitor._lock:
        if monitor._state.ok_count < 1:
            log.error("Self-check failed: expected OK events")
            return 1
        if monitor._state.client_ok_counts.get("node4", 0) < 1:
            log.error("Self-check failed: expected per-client OK counts")
            return 1

    stale_client = _FakeClient("node5")
    stale_client.log_lines = [
        "2026-08-18T10:00:00+00:00 STALE mount=/mnt/nfs_x error=Stale file handle"
    ]
    stale_monitor = NfsIoHealthMonitor(
        clients=[stale_client],
        log_dir=log_dir,
        heartbeat_tail_interval_s=0.05,
    )
    stale_monitor.start()
    stale_monitor._parse_log_lines("\n".join(stale_client.log_lines), "node5")
    try:
        stale_monitor.raise_if_unhealthy()
        log.error("Self-check failed: stale mount should raise via raise_if_unhealthy")
        return 1
    except NfsIoStaleMountError:
        pass

    if not stale_monitor.has_failed:
        log.error("Self-check failed: stale mount should set has_failed")
        return 1

    stall_ok_client = _FakeClient("node6")
    stall_ok_client.log_lines = [
        "2026-08-18T10:00:00+00:00 STALL mount=/mnt/nfs_y latency>=5s",
        "2026-08-18T10:00:03 STALL_RECOVERED mount=/mnt/nfs_y elapsed_s=2",
    ]
    stall_ok_monitor = NfsIoHealthMonitor(
        clients=[stall_ok_client],
        log_dir=log_dir,
        heartbeat_tail_interval_s=0.05,
    )
    stall_ok_monitor._parse_log_lines("\n".join(stall_ok_client.log_lines), "node6")
    if stall_ok_monitor.has_failed:
        log.error("Self-check failed: transient stall should not fail test")
        return 1

    log.info("NfsIoHealthMonitor self-check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(run_monitor_self_check())
