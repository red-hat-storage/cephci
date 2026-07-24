"""
General-purpose thrash helpers for daemon failure injection and verification.

Provides shared verification utilities and component-specific thrash workflows
for NFS and SMB. Used by test_osd_thrashing.py thread submissions.

Components:
    - Shared: data integrity (write_integrity_baseline, verify_data_integrity,
      check_mount_health) -- called in pre-thrash / post-thrash phases
    - Shared: wait_for_service_daemons_running -- daemon-type-agnostic polling
      used by both NFS and SMB wait methods
    - NFS: NfsThrashWorkflows class -- daemon kill, recovery, RADOS config
    - SMB: SmbThrashWorkflows class -- daemon lifecycle, kill methods, CTDB/VIP
    - NFS protocol state: client churn, export churn, admin op interleaving
"""

import json
import random
import re
import shlex
import time
import uuid
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Tuple

from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.mgr_workflows import MgrWorkflows
from ceph.waiter import WaitUntil
from cli.utilities.utils import reboot_node
from utility.log import Log

log = Log(__name__)


# ---------------------------------------------------------------------------
#  Shared Verification Layer
# ---------------------------------------------------------------------------


_INTEGRITY_FILE_SPECS = [
    ("integrity_4k", "4k", "4k"),
    ("integrity_64k", "64k", "4k"),
    ("integrity_256k", "256k", "4k"),
    ("integrity_1m", "1M", "16k"),
    ("integrity_4m", "4M", "64k"),
    ("integrity_8m", "8M", "64k"),
    ("integrity_16m", "16M", "128k"),
    ("integrity_32m", "32M", "128k"),
    ("integrity_48m", "48M", "256k"),
    ("integrity_64m", "64M", "256k"),
]
"""(job_name, file_size, block_size) tuples for integrity baseline files."""


def write_integrity_baseline(
    client_node,
    mount_points: List[str],
) -> Dict[str, Any]:
    """Write 10 fio files with CRC32C checksums to establish a verification baseline.

    Creates a ``thrash_integrity/`` directory under each mount point and writes
    10 files of varying sizes (4K to 64M) using ``--verify=crc32c --do_verify=0``
    (write-only). Each file uses a different block size for IO pattern diversity.
    File sizes and block sizes are defined by ``_INTEGRITY_FILE_SPECS``.

    Args:
        client_node: CephNode with the mount points accessible.
        mount_points: Absolute paths to mounted NFS or CIFS filesystems.

    Returns:
        Dict with ``files_written`` (int) and ``paths`` (list of integrity dirs
        that were successfully written).
    """
    result = {"files_written": 0, "paths": []}
    for mp in mount_points:
        integrity_dir = f"{mp}/thrash_integrity"
        try:
            client_node.exec_command(
                sudo=True, cmd=f"mkdir -p {integrity_dir}", timeout=15
            )
            files_ok = 0
            for job_name, fsize, bs in _INTEGRITY_FILE_SPECS:
                fio_cmd = (
                    f"fio --name={job_name} --directory={integrity_dir} "
                    f"--size={fsize} --bs={bs} --rw=write --numjobs=1 "
                    f"--verify=crc32c --verify_dump=1 --do_verify=0 "
                    f"--ioengine=libaio --direct=1"
                )
                client_node.exec_command(sudo=True, cmd=fio_cmd, timeout=120)
                files_ok += 1
            result["files_written"] += files_ok
            result["paths"].append(integrity_dir)
            log.info(
                "[integrity] Baseline written: %s (%s files, 4K-64M)",
                integrity_dir,
                files_ok,
            )
        except Exception as e:
            log.warning("[integrity] Failed to write baseline at %s: %s", mp, e)
    return result


def verify_data_integrity(
    client_node,
    mount_points: List[str],
) -> Dict[str, Any]:
    """Re-run fio verify pass on all integrity files written by the baseline.

    Reads each file created by ``write_integrity_baseline`` with
    ``--do_verify=1`` and checks CRC32C checksums.

    This function returns results without raising; callers decide the
    severity. Typical usage:

    * **Pre-thrash** (healthy cluster): any error → abort test.
    * **Post-thrash** (recovered cluster): ``mismatches`` → test failure
      (data corruption); ``errors`` without mismatches → warning
      (transient I/O issue).

    Args:
        client_node: CephNode with the mount points accessible.
        mount_points: Same mount paths passed to ``write_integrity_baseline``.

    Returns:
        Dict with ``files_checked`` (int), ``errors`` (int), and
        ``mismatches`` (list of mount paths with CRC32C verification failures).
    """
    result = {"files_checked": 0, "errors": 0, "mismatches": []}
    for mp in mount_points:
        integrity_dir = f"{mp}/thrash_integrity"
        mp_mismatched = False
        for job_name, fsize, bs in _INTEGRITY_FILE_SPECS:
            try:
                fio_cmd = (
                    f"fio --name={job_name} --directory={integrity_dir} "
                    f"--size={fsize} --bs={bs} --rw=read --numjobs=1 "
                    f"--verify=crc32c --do_verify=1 "
                    f"--ioengine=libaio --direct=1"
                )
                out, err = client_node.exec_command(
                    sudo=True, cmd=fio_cmd, timeout=120, check_ec=False
                )
                result["files_checked"] += 1
                err_lower = (err or "").lower()
                if "verify" in err_lower and "error" in err_lower:
                    result["errors"] += 1
                    mp_mismatched = True
                    log.error(
                        "[integrity] VERIFY MISMATCH: %s/%s (%s)",
                        integrity_dir,
                        job_name,
                        fsize,
                    )
            except Exception as e:
                log.warning(
                    "[integrity] Verify failed: %s/%s: %s", integrity_dir, job_name, e
                )
                result["errors"] += 1
        if mp_mismatched:
            result["mismatches"].append(mp)
        else:
            log.info(
                "[integrity] Verify passed: %s (%s files)",
                integrity_dir,
                len(_INTEGRITY_FILE_SPECS),
            )
    return result


def check_mount_health(
    client_node,
    mount_points: List[str],
    proto: str = "nfs",
) -> Dict[str, Any]:
    """Detect stale or hung mounts via ``stat`` and ``df`` with a 10-second timeout.

    Any mount that fails either command is classified as stale. For SMB mounts,
    "cifs vfs" is added to the stale-indicator pattern list.

    Args:
        client_node: CephNode with the mount points accessible.
        mount_points: Absolute paths to check.
        proto: ``"nfs"`` or ``"smb"`` -- controls stale-error pattern matching.

    Returns:
        Dict with ``healthy`` (list of responsive mount paths) and ``stale``
        (list of unresponsive mount paths).
    """
    result = {"healthy": [], "stale": []}
    stale_indicators = [
        "stale file handle",
        "transport endpoint",
        "no route to host",
        "connection timed out",
        "timed out",
    ]
    if proto == "smb":
        stale_indicators.append("cifs vfs")
    for mp in mount_points:
        try:
            client_node.exec_command(
                sudo=True, cmd=f"timeout 10 stat {mp}/.", timeout=15
            )
            client_node.exec_command(sudo=True, cmd=f"timeout 10 df {mp}", timeout=15)
            result["healthy"].append(mp)
        except Exception as e:
            err_str = str(e).lower()
            if any(ind in err_str for ind in stale_indicators):
                log.warning("[mount_health] Stale mount detected: %s", mp)
            else:
                log.warning("[mount_health] Mount check failed for %s: %s", mp, e)
            result["stale"].append(mp)
    return result


# ---------------------------------------------------------------------------
#  Shared Daemon Wait Helper
# ---------------------------------------------------------------------------


def wait_for_service_daemons_running(
    rados_obj,
    daemon_type: str,
    cluster_ids: List[str],
    timeout: int = 300,
) -> bool:
    """Poll until all daemons of the given type across clusters are running.

    Uses ``get_service_spec_daemons`` + ``get_daemon_status`` in a
    ``WaitUntil`` loop with a 10-second polling interval.

    Args:
        rados_obj: RadosOrchestrator instance.
        daemon_type: Daemon type (``"nfs"``, ``"smb"``, etc.).
        cluster_ids: Cluster identifiers (service name =
            ``"{daemon_type}.{cluster_id}"``).
        timeout: Max seconds to wait.

    Returns:
        True if all daemons are running within timeout, False otherwise.
    """
    svc_names = [f"{daemon_type}.{cid}" for cid in cluster_ids]
    for _ in WaitUntil(timeout=timeout, interval=10):
        try:
            all_running = True
            for svc_name in svc_names:
                daemon_ids = rados_obj.get_service_spec_daemons(service_name=svc_name)
                if not daemon_ids:
                    all_running = False
                    break
                for did in daemon_ids:
                    status = rados_obj.get_daemon_status(daemon_type, str(did))
                    if not status or status[1] != "running":
                        all_running = False
                        break
                if not all_running:
                    break
            if all_running:
                log.info(
                    "[daemon_wait] All daemons running for %s", ", ".join(svc_names)
                )
                return True
        except Exception as e:
            log.debug("[daemon_wait] Polling %s: %s", svc_names, e)
    log.error(
        "[daemon_wait] Timeout (%ss) waiting for daemons: %s",
        timeout,
        ", ".join(svc_names),
    )
    return False


# ---------------------------------------------------------------------------
#  NFS Thrash Workflows
# ---------------------------------------------------------------------------


class NfsThrashWorkflows:
    """NFS daemon failover helpers for thrash testing.

    Follows the pattern of MonitorWorkflows / MgrWorkflows. Each method has
    a single responsibility. Uses ``rados_obj`` (RadosOrchestrator) for
    daemon discovery, status polling, and kill actions instead of maintaining
    its own topology cache.
    """

    def __init__(self, installer, nfs_config, rados_obj, ceph_cluster):
        """Initialize NFS thrash workflow context.

        Args:
            installer: CephNode with ``ceph`` CLI access (typically the installer node).
            nfs_config: Dict produced by ``create_nfs_clusters_and_exports`` containing
                ``clusters`` (list of cluster dicts with ``cluster_id``).
            rados_obj: RadosOrchestrator instance for daemon/host queries.
            ceph_cluster: CephCluster object for node lookups.
        """
        self.installer = installer
        self.nfs_config = nfs_config
        self.rados_obj = rados_obj
        self.ceph_cluster = ceph_cluster

    # -- Daemon Discovery (via rados_obj) --

    def _get_cluster_ids(self) -> List[str]:
        """Extract NFS cluster IDs from ``self.nfs_config["clusters"]``.

        Returns:
            List of non-empty ``cluster_id`` strings.
        """
        clusters = self.nfs_config.get("clusters", [])
        return [c.get("cluster_id", "") for c in clusters if c.get("cluster_id")]

    def get_daemon_for_cluster(self, cluster_id: str) -> Optional[Dict]:
        """Select a random running daemon for the given NFS cluster.

        Delegates to ``RadosOrchestrator.get_service_spec_daemons``,
        ``fetch_host_node``, and ``get_daemon_status`` for discovery.

        Args:
            cluster_id: NFS cluster identifier (e.g. ``"nfs-cluster-1"``).

        Returns:
            Dict with ``daemon_name``, ``hostname``, ``host_node``, and ``status``.
            Returns ``None`` if no daemons are found or on error.
        """
        svc_name = f"nfs.{cluster_id}"
        try:
            daemon_ids = self.rados_obj.get_service_spec_daemons(service_name=svc_name)
            if not daemon_ids:
                return None
            daemon_id = str(random.choice(daemon_ids))
            daemon_name = f"nfs.{daemon_id}"
            host_node = self.rados_obj.fetch_host_node(
                daemon_type="nfs", daemon_id=daemon_id
            )
            daemon_status = self.rados_obj.get_daemon_status(
                daemon_type="nfs", daemon_id=daemon_id
            )
            status_desc = daemon_status[1] if daemon_status else "unknown"
            hostname = host_node.hostname if host_node else ""
            return {
                "daemon_name": daemon_name,
                "hostname": hostname,
                "host_node": host_node,
                "status": status_desc,
            }
        except Exception as e:
            log.warning("[nfs_topo] Failed to get daemon for %s: %s", cluster_id, e)
            return None

    # -- Kill Methods --

    def kill_daemon_pkill(self, host_node, daemon_name: str) -> bool:
        """Kill ganesha.nfsd inside its container via ``cephadm enter``.

        NFS-Ganesha runs inside a cephadm-managed container, so a host-level
        ``pkill`` won't reach the process. This method enters the container
        and sends SIGKILL to ganesha.nfsd, simulating an unexpected crash.
        Cephadm detects the dead process and restarts the container.

        Args:
            host_node: CephNode hosting the NFS daemon container.
            daemon_name: Full daemon name (e.g. ``"nfs.nfs-cluster-1.0.0.grim031.abc"``).

        Returns:
            True if kill succeeded, False on error.
        """
        try:
            host_node.exec_command(
                sudo=True,
                cmd=f"cephadm enter -n {daemon_name} -- sh -c 'kill -9 $(pidof ganesha.nfsd) 2>/dev/null || true'",
                timeout=15,
            )
            log.info(
                "[nfs_kill] Killed ganesha.nfsd inside %s on %s",
                daemon_name,
                host_node.hostname,
            )
            return True
        except Exception as e:
            log.warning(
                "[nfs_kill] pkill failed for %s on %s: %s",
                daemon_name,
                host_node.hostname,
                e,
            )
            return False

    def kill_daemon(self, host_node, daemon_name: str, method: str) -> bool:
        """Dispatch to the appropriate daemon kill method.

        Supported methods:
            - ``"pkill"``: ``kill_daemon_pkill`` (SIGKILL ganesha.nfsd).
            - ``"orch_stop"``: ``RadosOrchestrator.change_daemon_orch_state``.
            - ``"systemctl"``: ``RadosOrchestrator.change_daemon_systemctl_state``
              (builds systemd unit from FSID, performs reset-failed, stops daemon).

        Args:
            host_node: CephNode hosting the daemon (used by pkill path).
            daemon_name: Full daemon name (e.g. ``"nfs.12345"``).
            method: One of ``"pkill"``, ``"orch_stop"``, ``"systemctl"``.

        Returns:
            True if the kill action succeeded, False otherwise.
        """
        if method == "pkill":
            return self.kill_daemon_pkill(host_node, daemon_name)
        elif method in ("orch_stop", "systemctl"):
            daemon_id = daemon_name.removeprefix("nfs.")
            try:
                fn = (
                    self.rados_obj.change_daemon_orch_state
                    if method == "orch_stop"
                    else self.rados_obj.change_daemon_systemctl_state
                )
                return fn(action="stop", daemon_type="nfs", daemon_id=daemon_id)
            except Exception as e:
                log.warning(
                    "[nfs_kill] %s stop failed for %s: %s", method, daemon_name, e
                )
                return False
        else:
            log.warning("[nfs_kill] Unknown kill method: %s", method)
            return False

    # -- Recovery Checks --

    def wait_for_daemon_running(self, cluster_id: str, timeout: int = 180) -> bool:
        """Poll until all NFS daemons in the cluster report running state.

        Delegates to the shared ``wait_for_service_daemons_running`` helper.

        Args:
            cluster_id: NFS cluster identifier.
            timeout: Maximum seconds to wait before returning False.

        Returns:
            True if all daemons reach ``"running"`` within timeout, False otherwise.
        """
        return wait_for_service_daemons_running(
            self.rados_obj, "nfs", [cluster_id], timeout=timeout
        )

    def verify_vip_floated(self, vip: str, nfs_hosts: List) -> Optional[str]:
        """Check which host currently holds the virtual IP address.

        Runs ``ip addr show`` on each host and greps for the VIP (without
        CIDR prefix). Intended for post-thrash validation of NFS HA clusters
        deployed with ``--ingress --virtual_ip``.

        .. note:: **Not yet called in thrash workflows (TODO)**

           Once VIP/ingress support is added to ``create_nfs_clusters_and_exports``
           (via ``nfs_enable_ingress`` and ``nfs_virtual_ip`` config params),
           this method should be called in the post-thrash NFS verification
           phase to confirm the VIP migrated to a healthy host after daemon
           kills. The VIP address would come from ``nfs_config["clusters"][i]["virtual_ip"]``.

        Args:
            vip: Virtual IP address, optionally with CIDR suffix (e.g. ``"10.0.0.5/24"``).
            nfs_hosts: List of CephNode objects to scan.

        Returns:
            Hostname of the node holding the VIP, or ``None`` if not found.
        """
        for node in nfs_hosts:
            try:
                out = node.exec_command(
                    sudo=True,
                    cmd=f"ip addr show | grep '{vip.split('/')[0]}'",
                    timeout=10,
                    check_ec=False,
                )
                if out[0].strip():
                    log.info("[nfs_vip] VIP %s found on %s", vip, node.hostname)
                    return node.hostname
            except Exception:
                pass
        log.warning("[nfs_vip] VIP %s not found on any host", vip)
        return None

    # -- RADOS Config Check --

    def check_rados_config(self, cluster_id: str) -> Dict[str, Any]:
        """Check the NFS RADOS config object (``conf-nfs.<cluster_id>``) for corruption.

        Reads the object from the ``.nfs`` pool and flags it as invalid if the
        content is empty or fewer than 10 bytes.

        Args:
            cluster_id: NFS cluster identifier.

        Returns:
            Dict with ``valid`` (bool), ``error`` (str or None), and ``size`` (int).
        """
        result = {"valid": False, "error": None, "size": 0}
        try:
            out = self.installer.exec_command(
                sudo=True,
                cmd=f"rados get -p .nfs -N {cluster_id} "
                f"conf-nfs.{cluster_id} /dev/stdout 2>/dev/null",
                timeout=15,
            )
            content = out[0]
            result["size"] = len(content) if content else 0
            if result["size"] < 10:
                result["error"] = "empty or truncated"
                log.error(
                    "[nfs_rados] Config for %s is empty/truncated (%s bytes)",
                    cluster_id,
                    result["size"],
                )
            else:
                result["valid"] = True
                log.debug(
                    "[nfs_rados] Config for %s: %s bytes, valid",
                    cluster_id,
                    result["size"],
                )
        except Exception as e:
            result["error"] = str(e)
            log.warning("[nfs_rados] Failed to read config for %s: %s", cluster_id, e)
        return result

    # -- Composite Operations --

    def daemon_kill_cycle(
        self,
        cluster_id: str,
        method: str = "pkill",
        timeout: int = 180,
    ) -> Dict[str, Any]:
        """Execute one kill-and-recover cycle for a random daemon in the cluster.

        Selects a random daemon via ``get_daemon_for_cluster``, kills it using
        ``kill_daemon``, waits 5 seconds, then polls for recovery.

        Args:
            cluster_id: NFS cluster identifier.
            method: Kill method forwarded to ``kill_daemon``.
            timeout: Seconds to wait for daemon recovery after the kill.

        Returns:
            Dict with ``killed`` (bool) and ``recovered`` (bool).
        """
        result = {"killed": False, "recovered": False}

        target = self.get_daemon_for_cluster(cluster_id)
        if not target:
            log.warning("[nfs_cycle] No daemon found for cluster %s", cluster_id)
            return result

        host_node = target.get("host_node")
        daemon_name = target.get("daemon_name", "")
        if not host_node:
            log.warning("[nfs_cycle] No host node for daemon %s", daemon_name)
            return result

        log.info(
            "[nfs_cycle] Killing %s on %s via %s",
            daemon_name,
            host_node.hostname,
            method,
        )
        result["killed"] = self.kill_daemon(host_node, daemon_name, method)

        time.sleep(5)
        result["recovered"] = self.wait_for_daemon_running(cluster_id, timeout)

        return result


# ---------------------------------------------------------------------------
#  SMB Thrash Workflows
# ---------------------------------------------------------------------------


class SmbThrashWorkflows:
    """SMB daemon lifecycle and verification helpers for thrash testing.

    Consolidates daemon discovery, kill methods (orch restart, ctdb kill, smbd
    kill, node reboot), endpoint verification, RADOS config checks, and CTDB
    health into a single class. Uses ``rados_obj`` (RadosOrchestrator) for
    daemon discovery where applicable.
    """

    def __init__(self, installer, ceph_cluster, rados_obj):
        """Initialize SMB thrash workflow context.

        Args:
            installer: CephNode with ``ceph`` CLI access.
            ceph_cluster: CephCluster object for node lookups.
            rados_obj: RadosOrchestrator instance for daemon/host queries.
        """
        self.installer = installer
        self.ceph_cluster = ceph_cluster
        self.rados_obj = rados_obj

    # -- Daemon Info --

    def get_daemon_info(self, cluster_id: str) -> List[Dict[str, Any]]:
        """Get SMB daemon IDs/hosts/status for a cluster via rados helpers.

        Returns:
            List of dicts with daemon_name, daemon_id, hostname, status.
        """
        svc_name = f"smb.{cluster_id}"
        try:
            daemon_ids = self.rados_obj.get_service_spec_daemons(service_name=svc_name)
            result = []
            for daemon_id in daemon_ids or []:
                daemon_id = str(daemon_id)
                daemon_name = f"smb.{daemon_id}"
                host_node = self.rados_obj.fetch_host_node(
                    daemon_type="smb", daemon_id=daemon_id
                )
                daemon_status = self.rados_obj.get_daemon_status(
                    daemon_type="smb", daemon_id=daemon_id
                )
                result.append(
                    {
                        "daemon_name": daemon_name,
                        "daemon_id": daemon_id,
                        "hostname": host_node.hostname if host_node else "",
                        "status": daemon_status[1] if daemon_status else "unknown",
                    }
                )
            log.info("[smb_info] Found %s daemon(s) for %s", len(result), svc_name)
            return result
        except Exception as e:
            log.error("[smb_info] Failed to query %s: %s", svc_name, e)
            return []

    def wait_for_daemons_running(
        self,
        cluster_ids: List[str],
        timeout: int = 300,
    ) -> bool:
        """Poll until all SMB daemons across the given clusters report running.

        Delegates to the shared ``wait_for_service_daemons_running`` helper.

        Args:
            cluster_ids: List of SMB cluster identifiers to monitor.
            timeout: Maximum seconds to wait before returning False.

        Returns:
            True if every daemon in every cluster is running, False on timeout.
        """
        return wait_for_service_daemons_running(
            self.rados_obj, "smb", cluster_ids, timeout=timeout
        )

    def get_daemon_name(self, node, cluster_id: str) -> Optional[str]:
        """Resolve a single SMB daemon name for a cluster.

        Primary path uses ``get_daemon_info`` (rados-based discovery).
        Falls back to ``cephadm ls`` + ``jq`` for compatibility with older
        environments where rados-based discovery is not available.

        Args:
            node: CephNode used only by the ``cephadm ls`` fallback path.
            cluster_id: SMB cluster identifier.

        Returns:
            Daemon name string (e.g. ``"smb.12345"``) or ``None``.
        """
        daemons = self.get_daemon_info(cluster_id)
        if daemons:
            return daemons[0].get("daemon_name")

        # Fallback to cephadm ls (kept for compatibility with older flows).
        try:
            cmd = (
                f"cephadm ls --no-detail | "
                f"jq -r 'map(select(.name | startswith("
                f'"smb.{cluster_id}")))[-1].name\''
            )
            out = node.exec_command(sudo=True, cmd=cmd, timeout=15)
            name = out[0].strip()
            if name and name != "null":
                return name
        except Exception as e:
            log.warning("[smb_name] Failed to resolve daemon name: %s", e)
        return None

    # -- Kill Methods --

    def kill_orch_service_restart(self, cluster_id: str) -> bool:
        """Restart all SMB services via orch.

        Delegates to RadosOrchestrator.restart_daemon_services which restarts
        all SMB services (not scoped to a single cluster) and verifies each
        daemon has restarted. ``cluster_id`` is accepted for interface
        consistency and logging only.
        """
        try:
            return self.rados_obj.restart_daemon_services(daemon="smb")
        except Exception as e:
            log.warning("[smb_kill] orch restart failed for smb.%s: %s", cluster_id, e)
            return False

    def _kill_process_in_container(
        self, node, cluster_id: str, process_name: str, tag: str
    ) -> bool:
        """Kill a process inside its cephadm-managed SMB container.

        SMB daemons (smbd, ctdbd) run inside containers, so host-level
        ``pgrep``/``kill`` won't reach them. This method finds the SMB
        daemon name for the given node and cluster, then uses
        ``cephadm enter`` to send SIGKILL to the target process inside
        the container.

        Args:
            node: CephNode hosting the SMB container.
            cluster_id: SMB cluster identifier (to resolve daemon name).
            process_name: Process to kill inside the container (e.g.
                ``"ctdbd"``, ``"smbd"``).
            tag: Log tag prefix for messages.

        Returns:
            True if the process was killed, False otherwise.
        """
        try:
            daemons = self.get_daemon_info(cluster_id)
            daemon_name = next(
                (
                    d["daemon_name"]
                    for d in daemons
                    if d.get("hostname") == node.hostname
                ),
                None,
            )
            if not daemon_name:
                log.warning(
                    "[%s] No SMB daemon for cluster %s on %s",
                    tag,
                    cluster_id,
                    node.hostname,
                )
                return False
            node.exec_command(
                sudo=True,
                cmd=(
                    f"cephadm enter -n {daemon_name} -- "
                    f"sh -c 'kill -9 $(pidof {process_name}) 2>/dev/null || true'"
                ),
                timeout=15,
            )
            log.info(
                "[%s] Killed %s inside %s on %s",
                tag,
                process_name,
                daemon_name,
                node.hostname,
            )
            return True
        except Exception as e:
            log.warning(
                "[%s] %s kill failed on %s: %s", tag, process_name, node.hostname, e
            )
            return False

    def kill_ctdb(self, node, cluster_id: str) -> bool:
        """Kill ctdbd process inside the SMB container on a node."""
        return self._kill_process_in_container(node, cluster_id, "ctdbd", "smb_kill")

    def kill_smbd(self, node, cluster_id: str) -> bool:
        """Kill smbd process inside the SMB container on a node."""
        return self._kill_process_in_container(node, cluster_id, "smbd", "smb_kill")

    def kill_node_reboot(self, node) -> bool:
        """Reboot an SMB node via ``reboot_node`` from cli.utilities.utils.

        Args:
            node: CephNode to reboot.

        Returns:
            True if the reboot command succeeded, False on error.
        """
        try:
            reboot_node(node)
            log.info("[smb_kill] Rebooted %s", node.hostname)
            return True
        except Exception as e:
            log.warning("[smb_kill] Reboot failed for %s: %s", node.hostname, e)
            return False

    # -- Verification --

    def verify_endpoints(self, client_node, smb_config: Dict) -> Dict[str, Any]:
        """Verify SMB shares are accessible via ``smbclient -c ls``.

        Iterates over every target-IP / share combination. Uses
        ``public_addrs`` (VIPs) when present; falls back to ``smb_nodes``
        IP addresses.

        Args:
            client_node: CephNode with ``smbclient`` installed.
            smb_config: Dict with ``smb_user_name``, ``smb_user_password``,
                ``smb_shares``, ``smb_nodes``, and optional ``public_addrs``.

        Returns:
            Dict with ``accessible`` (list of ``"<ip>/<share>"`` strings) and
            ``failed`` (list of same format for unreachable shares).
        """
        result = {"accessible": [], "failed": []}
        user = smb_config.get("smb_user_name", "")
        password = smb_config.get("smb_user_password", "")
        shares = smb_config.get("smb_shares", [])
        nodes = smb_config.get("smb_nodes", [])

        public_addrs = smb_config.get("public_addrs", [])
        if public_addrs:
            targets = [addr.split("/")[0] for addr in public_addrs]
        else:
            targets = [n.ip_address for n in nodes]

        for target in targets:
            for share in shares:
                try:
                    cmd = f"smbclient -U {user}%{password} //{target}/{share} -c ls"
                    client_node.exec_command(sudo=True, cmd=cmd, timeout=30)
                    result["accessible"].append(f"{target}/{share}")
                except Exception as e:
                    log.warning("[smb_verify] Failed: //%s/%s - %s", target, share, e)
                    result["failed"].append(f"{target}/{share}")
        return result

    def verify_rados_config(self, cluster_ids: List[str]) -> Dict[str, Any]:
        """Check ``.smb`` pool metadata for corruption.

        Reads ``cluster.meta.json`` from the ``.smb`` RADOS pool for each
        cluster and verifies it parses as valid JSON.

        Args:
            cluster_ids: SMB cluster identifiers to verify.

        Returns:
            Dict with ``valid`` (list), ``corrupted`` (list), and
            ``details`` (per-cluster info or error string).
        """
        result = {"valid": [], "corrupted": [], "details": {}}
        for cid in cluster_ids:
            try:
                out = self.installer.exec_command(
                    sudo=True,
                    cmd=f"rados --pool=.smb -N {cid} get cluster.meta.json /dev/stdout",
                    timeout=15,
                )
                meta = json.loads(out[0].strip() or "{}")
                nodes = meta.get("nodes", [])
                result["valid"].append(cid)
                result["details"][cid] = f"{len(nodes)} nodes"
                log.info("[smb_rados] %s: %s nodes, valid", cid, len(nodes))
            except (json.JSONDecodeError, Exception) as e:
                result["corrupted"].append(cid)
                result["details"][cid] = str(e)
                log.error("[smb_rados] %s: config check failed: %s", cid, e)
        return result

    def verify_ctdb_health(
        self,
        smb_nodes: List,
        cluster_ids: List[str],
    ) -> Dict[str, Any]:
        """Verify CTDB cluster health via ``cephadm enter ... ctdb status``.

        Runs ``ctdb status`` inside the first daemon's container for each
        cluster. Checks all nodes report ``OK``; flags ``BANNED`` or
        ``DISCONNECTED`` nodes.

        Args:
            smb_nodes: CephNode objects in the SMB cluster (fallback host).
            cluster_ids: SMB cluster identifiers.

        Returns:
            Dict with ``healthy`` (bool), ``nodes_ok`` (int),
            ``nodes_total`` (int), and ``banned`` (list).
        """
        result = {"healthy": True, "nodes_ok": 0, "nodes_total": 0, "banned": []}
        for cid in cluster_ids:
            try:
                daemons = self.get_daemon_info(cid)
                if not daemons:
                    result["healthy"] = False
                    log.warning("[ctdb_health] No daemons for %s", cid)
                    continue

                daemon_name = daemons[0]["daemon_name"]
                hostname = daemons[0].get("hostname", "")
                ctdb_node = next(
                    (n for n in smb_nodes if n.hostname == hostname),
                    smb_nodes[0],
                )

                out = ctdb_node.exec_command(
                    sudo=True,
                    cmd=f"cephadm enter -n {daemon_name} ctdb status",
                    timeout=30,
                )
                status_text = out[0]

                num_match = re.search(r"Number of nodes:\s*(\d+)", status_text)
                total = int(num_match.group(1)) if num_match else 0
                ok_count = len(re.findall(r"pnn:\d+ \S+ *OK", status_text))
                banned = [
                    ln.strip()
                    for ln in status_text.splitlines()
                    if "pnn:" in ln and ("BANNED" in ln or "DISCONNECTED" in ln)
                ]

                result["nodes_ok"] += ok_count
                result["nodes_total"] += total
                result["banned"].extend(banned)
                if banned:
                    result["healthy"] = False
                log.info("[ctdb_health] %s: %s/%s nodes OK", cid, ok_count, total)
            except Exception as e:
                result["healthy"] = False
                log.warning("[ctdb_health] Check failed for %s: %s", cid, e)
        return result

    # -- Setup / Cleanup --

    def create_clusters_and_shares(self, config: Dict) -> Dict[str, Any]:
        """Deploy SMB cluster + CephFS + subvolumes + shares.

        Uses smb_operations imports internally.

        Args:
            config: Dict with smb_cluster_ids, smb_auth_mode, etc.

        Returns:
            smb_config dict with cluster_ids, shares, nodes, and credentials.
        """
        import sys

        if "tests/smb" not in sys.path:
            sys.path.insert(0, "tests/smb")
        from smb_operations import (
            create_smb_cluster,
            create_smb_share,
            create_vol_smb_subvol,
            enable_smb_module,
        )

        smb_config = {
            "cluster_ids": config.get("smb_cluster_ids", ["smb-thrash1"]),
            "smb_shares": [],
            "smb_nodes": [],
            "smb_user_name": config.get("smb_user_name", "smbuser"),
            "smb_user_password": config.get("smb_user_password", "smbpassword"),
            "cephfs_vol": "smb_thrash_vol",
            "smb_subvol_group": "smb_thrash_group",
        }

        # smb_nodes will be resolved after daemons are running (see below)
        smb_config["smb_nodes"] = []

        num_shares = config.get("smb_num_shares", 4)
        auth_mode = config.get("smb_auth_mode", "user")
        domain_realm = config.get("smb_domain_realm", "")
        custom_dns = config.get("smb_custom_dns", "")
        clustering = config.get("smb_clustering", "default")

        try:
            subvols = [f"smb_thrash_sv{i}" for i in range(num_shares)]
            create_vol_smb_subvol(
                self.installer,
                smb_config["cephfs_vol"],
                smb_config["smb_subvol_group"],
                subvols,
                "0777",
            )

            all_shares = []
            for cluster_id in smb_config["cluster_ids"]:
                enable_smb_module(self.installer, cluster_id)
                create_smb_cluster(
                    self.installer,
                    cluster_id,
                    auth_mode,
                    domain_realm,
                    smb_config["smb_user_name"],
                    smb_config["smb_user_password"],
                    custom_dns,
                    clustering,
                )

                share_names = [f"share{i}" for i in range(num_shares)]
                path = "/"
                create_smb_share(
                    self.installer,
                    share_names,
                    cluster_id,
                    smb_config["cephfs_vol"],
                    path,
                    smb_config["smb_subvol_group"],
                    subvols,
                )
                all_shares.extend(share_names)

            smb_config["smb_shares"] = all_shares

            # Wait for SMB daemons to be running (replaces verify_smb_service
            # which crashes on empty orch ls output before daemons deploy)
            if not self.wait_for_daemons_running(
                smb_config["cluster_ids"], timeout=300
            ):
                raise RuntimeError(
                    "[smb_setup] SMB daemons did not reach running state "
                    f"for clusters {smb_config['cluster_ids']}"
                )

            # Resolve smb_nodes from actual daemon hostnames so endpoint
            # verification targets the correct IPs (not guessed from roles).
            all_nodes = self.ceph_cluster.get_nodes()
            smb_hostnames = set()
            for cluster_id in smb_config["cluster_ids"]:
                for d in self.get_daemon_info(cluster_id):
                    if d.get("hostname"):
                        smb_hostnames.add(d["hostname"])
            smb_config["smb_nodes"] = [
                n for n in all_nodes if n.hostname in smb_hostnames
            ]
            log.info(
                "[smb_setup] SMB daemon hosts resolved: %s",
                [n.hostname for n in smb_config["smb_nodes"]],
            )

            log.info(
                "[smb_setup] Created %s cluster(s) with %s share(s) each",
                len(smb_config["cluster_ids"]),
                num_shares,
            )
        except Exception as e:
            log.error("[smb_setup] SMB setup failed: %s", e)
            raise

        return smb_config

    def cleanup(self, smb_config: Dict):
        """Tear down SMB thrash resources via ``smb_operations.smb_cleanup``.

        Args:
            smb_config: Dict returned by ``create_clusters_and_shares``.
        """
        import sys

        if "tests/smb" not in sys.path:
            sys.path.insert(0, "tests/smb")
        from smb_operations import smb_cleanup

        try:
            for cluster_id in smb_config.get("cluster_ids", []):
                smb_cleanup(
                    self.installer,
                    smb_config.get("smb_shares", []),
                    cluster_id,
                    smb_config.get("cephfs_vol", ""),
                    smb_config.get("smb_subvol_group", ""),
                )
            log.info("[smb_cleanup] SMB thrash resources cleaned up")
        except Exception as e:
            log.warning("[smb_cleanup] Cleanup failed: %s", e)


# ---------------------------------------------------------------------------
#  NFS Protocol State Thrash Threads
# ---------------------------------------------------------------------------


def _list_cluster_exports(rados_obj, cluster_id: str) -> List:
    """Fetch the NFS export list for a cluster, returning [] on non-list results."""
    exports = rados_obj.run_ceph_command(
        cmd=f"ceph nfs export ls {cluster_id}", timeout=15
    )
    return exports if isinstance(exports, list) else []


def _export_pseudo(export_entry) -> str:
    """Resolve the pseudo path from a str or dict export entry."""
    return (
        export_entry
        if isinstance(export_entry, str)
        else export_entry.get("pseudo", "")
    )


def _nfs_export_mount_targets(nfs_config: Dict) -> List[Dict[str, Any]]:
    """Build mount targets from ``nfs_config`` with multi-host fallback support.

    Merges cluster endpoint info (hosts, port) with each export's pseudo_path.
    When ``enable_rdma`` is set in ``nfs_config``, the RDMA port is selected
    instead of the TCP port.

    .. note:: **VIP / Ingress support (TODO)**

       When NFS clusters are deployed with ``--ingress --virtual_ip``, the
       client mount endpoint is the VIP, not individual daemon host IPs.
       To support this:

       1. ``create_nfs_clusters_and_exports`` should accept ``nfs_virtual_ip``
          and ``nfs_enable_ingress`` params, pass them to
          ``ceph nfs cluster create``, and store ``virtual_ip`` in the
          cluster entry dict.
       2. This function should check ``cluster.get("virtual_ip")``; when
          present, use the VIP as the sole ``host`` (overriding daemon
          hostnames) and use the ingress port for TCP mounts.
       3. ``verify_vip_floated`` in ``NfsThrashWorkflows`` can then be used
          post-thrash to confirm the VIP migrated correctly after daemon kills.

    Args:
        nfs_config: Dict with ``clusters`` (list of cluster dicts containing
            ``cluster_id``, ``hosts``/``host``, ``port``, ``rdma_port``) and
            ``exports`` (list of export dicts with ``cluster_id``,
            ``pseudo_path``).

    Returns:
        List of dicts, each with ``cluster_id``, ``host`` (primary),
        ``hosts`` (all), ``port``, and ``pseudo_path``.
    """
    use_rdma_port = bool(nfs_config.get("enable_rdma"))
    cluster_endpoint_map: Dict[str, Dict[str, Any]] = {}
    for cluster in nfs_config.get("clusters", []):
        cluster_id = cluster.get("cluster_id")
        if not cluster_id:
            continue
        # TODO: When virtual_ip is present in cluster dict (VIP/ingress
        # deployment), use it as the mount endpoint instead of daemon hosts.
        # vip = cluster.get("virtual_ip")
        # if vip:
        #     hosts = [vip.split("/")[0]]  # strip CIDR if present
        # else:
        hosts = cluster.get("hosts") or (
            [cluster.get("host")] if cluster.get("host") else []
        )
        hosts = [host for host in hosts if host]
        if not hosts:
            continue
        cluster_endpoint_map[cluster_id] = {
            "host": hosts[0],
            "hosts": hosts,
            "port": cluster.get("rdma_port") if use_rdma_port else cluster.get("port"),
        }

    targets = []
    for export in nfs_config.get("exports", []):
        cluster_id = export.get("cluster_id")
        endpoint = cluster_endpoint_map.get(cluster_id, {})
        host = endpoint.get("host")
        hosts = endpoint.get("hosts", [])
        port = endpoint.get("port")
        pseudo_path = export.get("pseudo_path")
        if cluster_id and host and pseudo_path:
            targets.append(
                {
                    "cluster_id": cluster_id,
                    "host": host,
                    "hosts": hosts,
                    "port": port,
                    "pseudo_path": pseudo_path,
                }
            )
    return targets


def _nfs_client_mount_option_string(
    nfs_config: Dict[str, Any],
    cluster_id: str,
    nfs_options_base: str,
) -> str:
    """Build a ``mount -o`` option string with port and optional RDMA flag.

    Uses ``_nfs_export_mount_targets`` (cached in ``nfs_config["_mount_targets"]``)
    as the single source of truth for port resolution, handling RDMA vs TCP
    transparently.

    Args:
        nfs_config: Full NFS config dict; ``_mount_targets`` is lazily populated.
        cluster_id: Cluster to resolve the port for.
        nfs_options_base: Base options string (e.g. ``"nfsvers=4.1"``).

    Returns:
        Comma-separated option string suitable for ``mount -o``.
        Falls back to ``nfs_options_base`` if no port is found.
    """
    if "_mount_targets" not in nfs_config:
        nfs_config["_mount_targets"] = _nfs_export_mount_targets(nfs_config)
    port = None
    for t in nfs_config["_mount_targets"]:
        if t["cluster_id"] == cluster_id and t.get("port") is not None:
            port = t["port"]
            break
    if port is None:
        log.warning(
            "_nfs_client_mount_option_string: no port for cluster_id=%s",
            cluster_id,
        )
        return nfs_options_base
    if nfs_config.get("enable_rdma"):
        return f"{nfs_options_base},rdma,port={port}"
    return f"{nfs_options_base},port={port}"


def _pick_nfs_endpoint(
    nfs_config: Dict[str, Any], index: int = 0
) -> Tuple[Optional[Dict[str, Any]], Optional[str], Optional[int]]:
    """Pick an NFS export endpoint by index with wrap-around.

    Delegates to ``_nfs_export_mount_targets`` (cached) and selects a target
    and host using modular indexing for round-robin distribution.

    Args:
        nfs_config: Full NFS config dict.
        index: Selector index; wraps around available targets and hosts.

    Returns:
        Tuple of ``(export_info, host, port)`` where ``export_info`` is a dict
        with ``cluster_id`` and ``pseudo_path``. Returns ``(None, None, None)``
        if no targets exist.
    """
    if "_mount_targets" not in nfs_config:
        nfs_config["_mount_targets"] = _nfs_export_mount_targets(nfs_config)
    targets = nfs_config["_mount_targets"]
    if not targets:
        return None, None, None
    target = targets[index % len(targets)]
    hosts = target.get("hosts", [])
    host = hosts[index % len(hosts)] if hosts else target.get("host")
    port = target.get("port")
    exp = {"cluster_id": target["cluster_id"], "pseudo_path": target["pseudo_path"]}
    return exp, host, port


@contextmanager
def _nfs_mount_context(
    client_node,
    nfs_config: Dict[str, Any],
    mount_base: str,
    index: int = 0,
    nfs_ver: str = "4.1",
):
    """Context manager that mounts an NFS export and unmounts on exit.

    Creates a unique mount point (``<mount_base>-<uuid8>``), mounts via
    ``_pick_nfs_endpoint`` and ``_nfs_client_mount_option_string``, and
    performs lazy unmount + cleanup in the finally block.

    Args:
        client_node: CephNode to mount on.
        nfs_config: Full NFS config dict.
        mount_base: Path prefix for the mount directory.
        index: Passed to ``_pick_nfs_endpoint`` for target selection.
        nfs_ver: NFS protocol version string (e.g. ``"4.1"``).

    Yields:
        ``(export_info, mount_point)`` on success, or ``(None, None)`` if
        endpoint selection or mount fails.
    """
    exp, host, port = _pick_nfs_endpoint(nfs_config, index=index)
    if not exp or not host or not port:
        yield None, None
        return

    unique_suffix = uuid.uuid4().hex[:8]
    mount_point = f"{mount_base}-{unique_suffix}"

    mount_opts = _nfs_client_mount_option_string(
        nfs_config, exp["cluster_id"], f"nfsvers={nfs_ver}"
    )
    mount_cmd = (
        f"mount -t nfs -o {mount_opts} {host}:{exp['pseudo_path']} {mount_point}"
    )
    try:
        client_node.exec_command(cmd=f"mkdir -p {mount_point}", sudo=True)
        client_node.exec_command(cmd=mount_cmd, sudo=True, timeout=60)
        yield exp, mount_point
    except Exception as e:
        log.warning(
            "NFS mount FAILED: %s:%s NFSv%s: %s", host, exp["pseudo_path"], nfs_ver, e
        )
        yield None, None
    finally:
        try:
            client_node.exec_command(
                cmd=f"umount -lf {mount_point}",
                sudo=True,
                check_ec=False,
            )
            client_node.exec_command(
                cmd=f"rm -rf {mount_point}",
                sudo=True,
                check_ec=False,
            )
        except Exception:
            pass


def _sleep_with_stop(stop_flag: Dict, total_seconds: int, step: int = 2) -> None:
    """Sleep in small increments, exiting early if ``stop_flag["stop"]`` is set.

    Args:
        stop_flag: Dict checked each iteration; returns immediately when
            ``stop_flag["stop"]`` is truthy.
        total_seconds: Total sleep duration in seconds.
        step: Maximum seconds per sleep chunk.
    """
    remaining = max(0, int(total_seconds))
    while remaining > 0 and not stop_flag.get("stop"):
        this_sleep = min(step, remaining)
        time.sleep(this_sleep)
        remaining -= this_sleep


def thrash_nfs_client_churn(
    client_node,
    nfs_config: Dict,
    duration: int,
    stop_flag: Dict,
    churn_rate: int = 5,
) -> Dict[str, int]:
    """Random client mount/unmount/open/lock cycles to stress NFS state management.

    Designed to run as a background thread alongside daemon kill threads.
    Operations are randomly chosen each iteration: mount a new NFS export,
    unmount an existing one, write a small file (open_hold), or acquire an
    ``flock`` lock. All churn mounts are lazy-unmounted in the finally block.

    Args:
        client_node: CephNode to perform mounts and I/O on.
        nfs_config: NFS config dict with ``clusters`` and ``exports``.
        duration: Total runtime in seconds.
        stop_flag: Dict with ``"stop"`` key; set to True to terminate early.
        churn_rate: Number of operations per 10-second window.

    Returns:
        Dict with ``mounts``, ``unmounts``, ``opens``, ``locks``, and
        ``errors`` counts.
    """
    result = {"mounts": 0, "unmounts": 0, "opens": 0, "locks": 0, "errors": 0}
    end_time = time.time() + duration
    export_targets = _nfs_export_mount_targets(nfs_config)
    if not export_targets:
        log.warning("[client_churn] No NFS exports configured")
        result["errors"] += 1
        return result

    churn_mounts = []
    mount_base = "/mnt/thrash_churn"
    mount_idx = 0

    try:
        while time.time() < end_time and not stop_flag.get("stop"):
            for _ in range(churn_rate):
                if stop_flag.get("stop"):
                    break

                op = random.choice(["mount", "unmount", "open_hold", "lock"])

                if op == "mount" and len(churn_mounts) < 20:
                    target = random.choice(export_targets)
                    mp = f"{mount_base}_{mount_idx}"
                    mount_idx += 1
                    hosts = target.get("hosts") or (
                        [target.get("host")] if target.get("host") else []
                    )
                    if not hosts:
                        result["errors"] += 1
                        continue
                    nfs_server = random.choice(hosts)
                    pseudo = target.get("pseudo_path", "")
                    try:
                        client_node.exec_command(
                            sudo=True, cmd=f"mkdir -p {mp}", timeout=10
                        )
                        nfs_ver = random.choice(["4.0", "4.1", "4.2"])
                        mount_opts = _nfs_client_mount_option_string(
                            nfs_config,
                            target["cluster_id"],
                            f"nfsvers={nfs_ver}",
                        )
                        client_node.exec_command(
                            sudo=True,
                            cmd=f"mount -t nfs -o {mount_opts} {nfs_server}:{pseudo} {mp}",
                            timeout=30,
                        )
                        churn_mounts.append(mp)
                        result["mounts"] += 1
                    except Exception:
                        result["errors"] += 1

                elif op == "unmount" and churn_mounts:
                    mp = random.choice(churn_mounts)
                    try:
                        client_node.exec_command(
                            sudo=True,
                            cmd=f"umount -l {mp} 2>/dev/null; rmdir {mp}",
                            timeout=15,
                            check_ec=False,
                        )
                        churn_mounts.remove(mp)
                        result["unmounts"] += 1
                    except Exception:
                        result["errors"] += 1

                elif op == "open_hold" and churn_mounts:
                    mp = random.choice(churn_mounts)
                    try:
                        client_node.exec_command(
                            sudo=True,
                            cmd=f"dd if=/dev/urandom "
                            f"of={mp}/churn_file_{random.randint(0, 999)} "
                            f"bs=4k count=1 2>/dev/null",
                            timeout=15,
                        )
                        result["opens"] += 1
                    except Exception:
                        result["errors"] += 1

                elif op == "lock" and churn_mounts:
                    mp = random.choice(churn_mounts)
                    try:
                        lock_file = f"{mp}/churn_lock_{random.randint(0, 99)}"
                        client_node.exec_command(
                            sudo=True,
                            cmd=f"touch {lock_file} && "
                            f"flock -x -w 2 {lock_file} -c 'sleep 1' "
                            f"2>/dev/null",
                            timeout=15,
                            check_ec=False,
                        )
                        result["locks"] += 1
                    except Exception:
                        result["errors"] += 1

            _sleep_with_stop(stop_flag, total_seconds=10, step=2)
    finally:
        for mp in churn_mounts:
            try:
                client_node.exec_command(
                    sudo=True,
                    cmd=f"umount -l {mp} 2>/dev/null; rmdir {mp} 2>/dev/null",
                    timeout=15,
                    check_ec=False,
                )
            except Exception:
                pass

    log.info(
        "[client_churn] Completed: mounts=%s, unmounts=%s, opens=%s, "
        "locks=%s, errors=%s",
        result["mounts"],
        result["unmounts"],
        result["opens"],
        result["locks"],
        result["errors"],
    )
    return result


def thrash_nfs_export_churn(
    installer,
    nfs_config: Dict,
    duration: int,
    stop_flag: Dict,
    rados_obj,
) -> Dict[str, int]:
    """Rapid NFS export create/delete/modify to stress RADOS config updates.

    Randomly selects one of three operations each cycle:
        - ``create``: create a new churn export then immediately delete it.
        - ``delete_recreate``: delete a non-churn export and recreate it.
        - ``modify_access``: toggle ``access_type`` between RW and RO via
          ``ceph nfs export apply``.

    Args:
        installer: CephNode with ``ceph`` CLI access.
        nfs_config: NFS config dict with ``clusters`` and ``fs_name``.
        duration: Total runtime in seconds.
        stop_flag: Dict with ``"stop"`` key for early termination.
        rados_obj: RadosOrchestrator used for ``run_ceph_command`` calls.

    Returns:
        Dict with ``creates``, ``deletes``, ``modifies``, and ``errors`` counts.
    """
    result = {"creates": 0, "deletes": 0, "modifies": 0, "errors": 0}
    end_time = time.time() + duration
    clusters = nfs_config.get("clusters", [])
    fs_name = nfs_config.get("fs_name", "nfs-cephfs")
    if not clusters:
        log.warning("[export_churn] No NFS clusters in config")
        return result

    churn_export_idx = 100

    while time.time() < end_time and not stop_flag.get("stop"):
        cluster = random.choice(clusters)
        cluster_id = cluster.get("cluster_id", "")
        op = random.choice(["create", "delete_recreate", "modify_access"])

        try:
            if op == "create":
                pseudo = f"/churn_export_{churn_export_idx}"
                churn_export_idx += 1
                installer.exec_command(
                    sudo=True,
                    cmd=f"ceph nfs export create cephfs {cluster_id} "
                    f"{pseudo} {fs_name} --path=/",
                    timeout=30,
                )
                result["creates"] += 1
                _sleep_with_stop(stop_flag, total_seconds=3, step=1)
                installer.exec_command(
                    sudo=True,
                    cmd=f"ceph nfs export rm {cluster_id} {pseudo}",
                    timeout=30,
                    check_ec=False,
                )
                result["deletes"] += 1

            elif op == "delete_recreate":
                exports = _list_cluster_exports(rados_obj, cluster_id)
                churn_exports = [e for e in exports if "churn" not in str(e)]
                if churn_exports:
                    target = random.choice(churn_exports)
                    pseudo = _export_pseudo(target)
                    if pseudo:
                        installer.exec_command(
                            sudo=True,
                            cmd=f"ceph nfs export rm {cluster_id} {pseudo}",
                            timeout=30,
                            check_ec=False,
                        )
                        result["deletes"] += 1
                        _sleep_with_stop(stop_flag, total_seconds=2, step=1)
                        installer.exec_command(
                            sudo=True,
                            cmd=f"ceph nfs export create cephfs {cluster_id} "
                            f"{pseudo} {fs_name} --path=/",
                            timeout=30,
                        )
                        result["creates"] += 1

            elif op == "modify_access":
                exports = _list_cluster_exports(rados_obj, cluster_id)
                if exports:
                    target_pseudo = _export_pseudo(exports[0])
                    if target_pseudo:
                        try:
                            export_info = rados_obj.run_ceph_command(
                                cmd=f"ceph nfs export info "
                                f"{cluster_id} {target_pseudo}",
                                timeout=15,
                            )
                            current_access = export_info.get("access_type", "RW")
                            new_access = "RO" if current_access == "RW" else "RW"
                            export_info["access_type"] = new_access
                            payload = shlex.quote(json.dumps(export_info))
                            installer.exec_command(
                                sudo=True,
                                cmd=f"printf %s {payload} | "
                                f"ceph nfs export apply {cluster_id} -i -",
                                timeout=30,
                            )
                            result["modifies"] += 1
                        except Exception:
                            result["errors"] += 1
        except Exception as e:
            log.debug("[export_churn] Operation %s failed: %s", op, e)
            result["errors"] += 1

        _sleep_with_stop(stop_flag, total_seconds=random.randint(5, 15), step=2)

    log.info(
        "[export_churn] Completed: creates=%s, deletes=%s, modifies=%s, " "errors=%s",
        result["creates"],
        result["deletes"],
        result["modifies"],
        result["errors"],
    )
    return result


def thrash_nfs_admin_ops(
    installer,
    nfs_config: Dict,
    duration: int,
    stop_flag: Dict,
    rados_obj,
) -> Dict[str, int]:
    """Interleave NFS admin operations with client I/O running in parallel.

    Randomly selects operations each cycle:
        - ``orch_restart``: restart all NFS daemons via
          ``RadosOrchestrator.restart_daemon_services``, then sleeps 30s.
        - ``export_reload``: re-apply an existing export's config unchanged
          via ``ceph nfs export apply`` to exercise config reload paths.

    Args:
        installer: CephNode with ``ceph`` CLI access.
        nfs_config: NFS config dict with ``clusters``.
        duration: Total runtime in seconds.
        stop_flag: Dict with ``"stop"`` key for early termination.
        rados_obj: RadosOrchestrator for ``restart_daemon_services`` and
            ``run_ceph_command``.

    Returns:
        Dict with ``restarts``, ``reloads``, and ``errors`` counts.
    """
    result = {"restarts": 0, "reloads": 0, "errors": 0}
    end_time = time.time() + duration
    clusters = nfs_config.get("clusters", [])
    if not clusters:
        log.warning("[admin_ops] No NFS clusters in config")
        return result

    while time.time() < end_time and not stop_flag.get("stop"):
        cluster = random.choice(clusters)
        cluster_id = cluster.get("cluster_id", "")
        op = random.choice(["orch_restart", "export_reload"])

        try:
            if op == "orch_restart":
                rados_obj.restart_daemon_services(daemon="nfs")
                result["restarts"] += 1
                _sleep_with_stop(stop_flag, total_seconds=30, step=3)

            elif op == "export_reload":
                exports = _list_cluster_exports(rados_obj, cluster_id)
                if exports:
                    target = _export_pseudo(exports[0])
                    if target:
                        try:
                            export_info = rados_obj.run_ceph_command(
                                cmd=f"ceph nfs export info {cluster_id} {target}",
                                timeout=15,
                            )
                            payload = shlex.quote(json.dumps(export_info))
                            installer.exec_command(
                                sudo=True,
                                cmd=f"printf %s {payload} | "
                                f"ceph nfs export apply {cluster_id} -i -",
                                timeout=30,
                            )
                            result["reloads"] += 1
                        except Exception:
                            result["errors"] += 1
        except Exception as e:
            log.debug("[admin_ops] Operation %s failed: %s", op, e)
            result["errors"] += 1

        _sleep_with_stop(stop_flag, total_seconds=random.randint(20, 40), step=3)

    log.info(
        "[admin_ops] Completed: restarts=%s, reloads=%s, errors=%s",
        result["restarts"],
        result["reloads"],
        result["errors"],
    )
    return result


# ---------------------------------------------------------------------------
#  Thin Orchestrator Functions (called from test_osd_thrashing.py)
# ---------------------------------------------------------------------------


def thrash_nfs_daemon_failover(
    installer,
    nfs_config: Dict,
    rados_obj,
    ceph_cluster,
    client_node,
    duration: int,
    stop_flag: Dict,
    config: Dict,
) -> Dict[str, Any]:
    """Kill NFS daemons cyclically and validate recovery.

    Called from ``test_osd_thrashing.py`` thread submissions. Instantiates
    ``NfsThrashWorkflows`` and loops ``daemon_kill_cycle`` on random clusters
    until ``duration`` expires or ``stop_flag`` is set.

    Config keys read from ``config``:
        - ``nfs_daemon_kill_method``: Kill method string (default ``"pkill"``).
        - ``nfs_daemon_thrash_interval``: Seconds between cycles (default 60).
        - ``nfs_daemon_recovery_timeout``: Seconds for recovery poll (default 180).

    Args:
        installer: CephNode with ``ceph`` CLI access.
        nfs_config: NFS config dict with cluster definitions.
        rados_obj: RadosOrchestrator instance.
        ceph_cluster: CephCluster object.
        client_node: CephNode (unused directly; reserved for future I/O checks).
        duration: Total runtime in seconds.
        stop_flag: Dict with ``"stop"`` key for early termination.
        config: Test config dict with thrash tuning parameters.

    Returns:
        Dict with ``cycles`` (int), ``kill_failures`` (int), and
        ``recovery_failures`` (int).
    """
    wf = NfsThrashWorkflows(installer, nfs_config, rados_obj, ceph_cluster)

    method = config.get("nfs_daemon_kill_method", "pkill")
    interval = config.get("nfs_daemon_thrash_interval", 60)
    timeout = config.get("nfs_daemon_recovery_timeout", 180)
    clusters = wf._get_cluster_ids()
    end_time = time.time() + duration
    results = {"cycles": 0, "kill_failures": 0, "recovery_failures": 0}

    if not clusters:
        log.warning("[nfs_failover] No NFS clusters discovered")
        return results

    while time.time() < end_time and not stop_flag.get("stop"):
        cluster_id = random.choice(clusters)
        cycle_result = wf.daemon_kill_cycle(cluster_id, method, timeout)
        results["cycles"] += 1
        if not cycle_result.get("killed"):
            results["kill_failures"] += 1
        if not cycle_result.get("recovered"):
            results["recovery_failures"] += 1

        _sleep_with_stop(stop_flag, total_seconds=interval, step=3)

    log.info(
        "[nfs_failover] Completed %s cycles, kill_failures=%s, " "recovery_failures=%s",
        results["cycles"],
        results["kill_failures"],
        results["recovery_failures"],
    )
    return results


def thrash_smb(
    installer,
    smb_config: Dict,
    ceph_cluster,
    rados_obj,
    duration: int,
    stop_flag: Dict,
) -> Dict[str, int]:
    """SMB daemon-level chaos: randomly kill/restart SMB components.

    Called from ``test_osd_thrashing.py`` thread submissions. Instantiates
    ``SmbThrashWorkflows`` and randomly selects from four operations each
    cycle: ``orch_daemon_restart``, ``orch_service_restart``, ``ctdb_kill``,
    ``smbd_kill``. Waits for daemon recovery after each kill.

    Args:
        installer: CephNode with ``ceph`` CLI access.
        smb_config: Dict with ``cluster_ids``, ``smb_nodes``, and related
            SMB configuration produced by ``create_clusters_and_shares``.
        ceph_cluster: CephCluster object.
        rados_obj: RadosOrchestrator instance.
        duration: Total runtime in seconds.
        stop_flag: Dict with ``"stop"`` key for early termination.

    Returns:
        Dict with ``cycles``, ``kills``, ``recovery_failures``, and ``errors``
        counts.
    """
    smb_wf = SmbThrashWorkflows(installer, ceph_cluster, rados_obj)
    result = {"cycles": 0, "kills": 0, "recovery_failures": 0, "errors": 0}
    end_time = time.time() + duration
    cluster_ids = smb_config.get("cluster_ids", [])
    smb_nodes = smb_config.get("smb_nodes", [])

    if not cluster_ids or not smb_nodes:
        log.warning("[smb_thrash] No clusters or nodes configured")
        return result

    operations = [
        "orch_daemon_restart",
        "orch_service_restart",
        "ctdb_kill",
        "smbd_kill",
    ]

    while time.time() < end_time and not stop_flag.get("stop"):
        cluster_id = random.choice(cluster_ids)
        op = random.choice(operations)
        node = random.choice(smb_nodes)
        result["cycles"] += 1

        log.info(
            "[smb_thrash] Iteration %s: op=%s, node=%s",
            result["cycles"],
            op,
            node.hostname,
        )

        try:
            killed = False
            if op == "orch_daemon_restart":
                daemons = smb_wf.get_daemon_info(cluster_id)
                if daemons:
                    daemon = random.choice(daemons)
                    daemon_id = daemon["daemon_name"].removeprefix("smb.")
                    killed = smb_wf.rados_obj.change_daemon_orch_state(
                        action="restart", daemon_type="smb", daemon_id=daemon_id
                    )
            elif op == "orch_service_restart":
                killed = smb_wf.kill_orch_service_restart(cluster_id)
            elif op == "ctdb_kill":
                killed = smb_wf.kill_ctdb(node, cluster_id)
            elif op == "smbd_kill":
                killed = smb_wf.kill_smbd(node, cluster_id)

            if killed:
                result["kills"] += 1

            _sleep_with_stop(stop_flag, total_seconds=10, step=2)

            if not smb_wf.wait_for_daemons_running(cluster_ids, timeout=120):
                result["recovery_failures"] += 1
                log.warning("[smb_thrash] Recovery failed after %s", op)

        except Exception as e:
            result["errors"] += 1
            log.warning("[smb_thrash] Error during %s: %s", op, e)

        _sleep_with_stop(stop_flag, total_seconds=15, step=2)

    log.info(
        "[smb_thrash] Completed %s cycles, kills=%s, " "recovery_failures=%s",
        result["cycles"],
        result["kills"],
        result["recovery_failures"],
    )
    return result


def thrash_smb_client_io(
    client_node,
    smb_config: Dict,
    duration: int,
    stop_flag: Dict,
) -> Dict[str, int]:
    """Run smbclient read/write/list loops to generate I/O during SMB daemon chaos.

    Called from ``test_osd_thrashing.py`` thread submissions. Each iteration
    randomly picks a target IP and share, then performs one of: ``smbclient ls``,
    ``smbclient put`` (4 MB file), or ``smbclient get``. Uses ``public_addrs``
    (VIPs) when present; falls back to ``smb_nodes`` IP addresses. Temp files
    are cleaned up in the finally block.

    Args:
        client_node: CephNode with ``smbclient`` installed.
        smb_config: Dict with ``smb_shares``, ``smb_user_name``,
            ``smb_user_password``, ``smb_nodes``, and optional ``public_addrs``.
        duration: Total runtime in seconds.
        stop_flag: Dict with ``"stop"`` key for early termination.

    Returns:
        Dict with ``io_ops`` (int) and ``errors`` (int) counts.
    """
    result = {"io_ops": 0, "errors": 0}
    end_time = time.time() + duration
    shares = smb_config.get("smb_shares", [])
    user = smb_config.get("smb_user_name", "")
    password = smb_config.get("smb_user_password", "")
    smb_nodes = smb_config.get("smb_nodes", [])

    if not shares or not smb_nodes:
        log.warning("[smb_io] No shares or nodes configured")
        return result

    public_addrs = smb_config.get("public_addrs", [])
    targets = (
        [addr.split("/")[0] for addr in public_addrs]
        if public_addrs
        else [n.ip_address for n in smb_nodes]
    )

    try:
        while time.time() < end_time and not stop_flag.get("stop"):
            target = random.choice(targets)
            share = random.choice(shares)
            op = random.choice(["smbclient_ls", "smbclient_write", "smbclient_read"])

            try:
                if op == "smbclient_ls":
                    client_node.exec_command(
                        sudo=True,
                        cmd=f"smbclient -U {user}%{password} "
                        f"//{target}/{share} -c ls",
                        timeout=30,
                    )
                    result["io_ops"] += 1

                elif op == "smbclient_write":
                    fname = f"thrash_io_{random.randint(0, 999)}.dat"
                    client_node.exec_command(
                        sudo=True,
                        cmd=f"dd if=/dev/urandom of=/tmp/{fname} bs=1M count=4 "
                        f"2>/dev/null && "
                        f"smbclient -U {user}%{password} "
                        f"//{target}/{share} "
                        f"-c 'put /tmp/{fname} {fname}'; "
                        f"rc=$?; rm -f /tmp/{fname}; exit $rc",
                        timeout=30,
                    )
                    result["io_ops"] += 1

                elif op == "smbclient_read":
                    fname = f"thrash_io_{random.randint(0, 999)}.dat"
                    client_node.exec_command(
                        sudo=True,
                        cmd=f"smbclient -U {user}%{password} "
                        f"//{target}/{share} "
                        f"-c 'get {fname} /dev/null' 2>/dev/null",
                        timeout=30,
                        check_ec=False,
                    )
                    result["io_ops"] += 1

            except Exception:
                result["errors"] += 1

            _sleep_with_stop(stop_flag, total_seconds=2, step=1)
    finally:
        try:
            client_node.exec_command(
                sudo=True,
                cmd="rm -f /tmp/thrash_io_*.dat 2>/dev/null",
                timeout=15,
                check_ec=False,
            )
        except Exception:
            pass

    log.info(
        "[smb_io] Completed: io_ops=%s, errors=%s", result["io_ops"], result["errors"]
    )
    return result


# ---------------------------------------------------------------------------
#  Common Daemon SIGKILL Helper
# ---------------------------------------------------------------------------


def _sigkill_daemon(
    rados_obj: RadosOrchestrator,
    daemon_type: str,
    daemon_id: str,
    cluster_fsid: str,
) -> bool:
    """Send SIGKILL to a specific Ceph daemon via ``systemctl kill``.

    Uses ``systemctl kill --signal=SIGKILL ceph-{fsid}@{type}.{id}.service``
    to precisely target a single daemon process, regardless of daemon type.
    This is safer than ``pidof | kill -9`` which can inadvertently kill all
    daemons of a type on the same host.

    Args:
        rados_obj: RadosOrchestrator for host resolution.
        daemon_type: Ceph daemon type (``"osd"``, ``"mon"``, ``"mgr"``).
        daemon_id: Daemon identifier (e.g. ``"0"`` for osd.0, hostname
            for mon).
        cluster_fsid: Pre-fetched cluster FSID (avoids per-call ``ceph fsid``
            overhead).

    Returns:
        True if the kill command succeeded, False on error.
    """
    try:
        host = rados_obj.fetch_host_node(
            daemon_type=daemon_type, daemon_id=str(daemon_id)
        )
        if not host:
            log.warning("Could not resolve host for %s.%s", daemon_type, daemon_id)
            return False

        service_name = f"ceph-{cluster_fsid}@{daemon_type}.{daemon_id}.service"

        log.info(
            "Sending SIGKILL to %s.%s via systemctl kill on %s (service: %s)",
            daemon_type,
            daemon_id,
            host.hostname,
            service_name,
        )
        host.exec_command(
            sudo=True,
            cmd=f"systemctl kill --signal=SIGKILL {service_name}",
        )
        return True

    except Exception as e:
        log.warning("Failed to SIGKILL %s.%s: %s", daemon_type, daemon_id, e)
        return False


# ---------------------------------------------------------------------------
#  OSD SIGKILL Thrashing
# ---------------------------------------------------------------------------


def thrash_osd_sigkill(
    rados_obj: RadosOrchestrator,
    osd_list: List[int],
    iterations: int,
    stop_flag: Dict,
) -> int:
    """
    Thrash OSDs by sending SIGKILL (kill -9) to simulate abrupt daemon crashes.

    Per iteration:
    1. Select 1-2 random OSDs
    2. Resolve each OSD's host via ``ceph orch ps``
    3. Send SIGKILL to the ceph-osd process for that specific OSD
    4. Wait for orchestrator to auto-restart the daemon
    5. Verify the OSD comes back up

    Unlike the graceful out/in thrashing, SIGKILL bypasses all OSD shutdown
    handlers (journal flush, PG state persistence), creating conditions that
    stress PG peering, journal replay, and BlueStore recovery on startup.

    Args:
        rados_obj: RadosOrchestrator object
        osd_list: List of OSD IDs eligible for thrashing
        iterations: Number of thrashing iterations
        stop_flag: Dict with 'stop' key to signal early termination

    Returns:
        Number of iterations completed
    """
    cluster_fsid = rados_obj.run_ceph_command(cmd="ceph fsid")["fsid"]
    log.info(
        "Starting OSD SIGKILL thrashing " "(iterations: %s, osd_list: %s, fsid: %s)",
        iterations,
        osd_list,
        cluster_fsid,
    )
    completed = 0

    for iteration in range(iterations):
        if stop_flag.get("stop"):
            break

        num_to_kill = min(random.randint(1, 2), len(osd_list))
        target_osds = random.sample(osd_list, num_to_kill)

        log.info(
            "SIGKILL iteration %s/%s: Targeting OSD(s) %s",
            iteration + 1,
            iterations,
            target_osds,
        )

        killed = []
        for osd_id in target_osds:
            if _sigkill_daemon(rados_obj, "osd", str(osd_id), cluster_fsid):
                killed.append(osd_id)

        if not killed:
            log.warning("No OSDs were killed in iteration %s", iteration + 1)
            time.sleep(5)
            continue

        # Poll for OSD recovery (up to 60s, every 5s) instead of a
        # single hard-wait — mirrors the MON quorum recovery pattern.
        all_up = False
        still_down = list(killed)
        for elapsed in range(0, 60, 5):
            time.sleep(5)
            try:
                osd_dump = rados_obj.run_ceph_command(
                    cmd="ceph osd dump", client_exec=True
                )
                up_osds = {
                    e["osd"] for e in osd_dump.get("osds", []) if e.get("up") == 1
                }
                still_down = [oid for oid in killed if oid not in up_osds]
                if not still_down:
                    all_up = True
                    break
                log.debug(
                    "OSD recovery poll %ss: still down %s",
                    elapsed + 5,
                    still_down,
                )
            except Exception:
                log.debug("OSD recovery poll %ss: osd dump failed", elapsed + 5)

        for osd_id in killed:
            if all_up or osd_id not in still_down:
                log.info("OSD.%s is up after SIGKILL", osd_id)
            else:
                log.warning(
                    "OSD.%s is NOT up after SIGKILL " "(may still be recovering)",
                    osd_id,
                )

        completed += 1
        time.sleep(random.uniform(5, 10))

    log.info("OSD SIGKILL thrashing completed: %s iterations", completed)
    return completed


# ---------------------------------------------------------------------------
#  MON / MGR SIGKILL Helpers
# ---------------------------------------------------------------------------


def _thrash_mon_sigkill(
    rados_obj: RadosOrchestrator, mon_workflow_obj, cluster_fsid: str
) -> bool:
    """
    Kill a random MON daemon with SIGKILL to simulate an abrupt crash.

    Delegates the actual kill to ``_sigkill_daemon`` which uses
    ``systemctl kill --signal=SIGKILL`` to precisely target the MON service.
    Maintains quorum safety by only killing one MON at a time and verifying
    there are enough MONs to sustain quorum before proceeding.

    Args:
        rados_obj: RadosOrchestrator instance.
        mon_workflow_obj: MonitorWorkflows instance.
        cluster_fsid: Pre-fetched cluster FSID (avoids per-call overhead).
    """
    try:
        quorum_hosts = mon_workflow_obj.get_mon_quorum_hosts()
        if len(quorum_hosts) < 3:
            log.warning(
                "Need at least 3 MONs in quorum for sigkill thrash, " "have %s",
                len(quorum_hosts),
            )
            return False

        target_mon = random.choice(quorum_hosts)

        if not _sigkill_daemon(rados_obj, "mon", target_mon, cluster_fsid):
            return False

        time.sleep(15)
        quorum_after = mon_workflow_obj.get_mon_quorum_hosts()

        if target_mon in quorum_after:
            log.info(
                "MON %s recovered and rejoined quorum after SIGKILL",
                target_mon,
            )
            return True

        log.info(
            "MON %s not yet in quorum, waiting additional 30s...",
            target_mon,
        )
        time.sleep(30)
        quorum_after = mon_workflow_obj.get_mon_quorum_hosts()

        if target_mon in quorum_after:
            log.info("MON %s rejoined quorum after extended wait", target_mon)
            return True

        log.warning(
            "MON %s did not rejoin quorum after SIGKILL. " "Current quorum: %s",
            target_mon,
            quorum_after,
        )
        return False

    except Exception as e:
        log.error("MON sigkill thrash failed: %s", e)
        return False


def _thrash_mgr_sigkill(
    rados_obj: RadosOrchestrator,
    mgr_workflow_obj: MgrWorkflows,
    cluster_fsid: str,
) -> bool:
    """
    Kill a random MGR daemon with SIGKILL to simulate an abrupt crash.

    Delegates the actual kill to ``_sigkill_daemon`` which uses
    ``systemctl kill --signal=SIGKILL`` to precisely target the MGR service.
    Selects a random MGR (active or standby) and verifies an active MGR is
    available post-kill.

    Args:
        rados_obj: RadosOrchestrator instance.
        mgr_workflow_obj: MgrWorkflows instance.
        cluster_fsid: Pre-fetched cluster FSID (avoids per-call overhead).
    """
    try:
        mgr_list = mgr_workflow_obj.get_mgr_daemon_list()
        if len(mgr_list) < 2:
            log.warning("Need at least 2 MGRs for sigkill thrash")
            return False

        target_mgr = random.choice(mgr_list)
        active_mgr = mgr_workflow_obj.get_active_mgr()
        mgr_type = "active" if target_mgr == active_mgr else "standby"
        log.info("Selected %s MGR %s for SIGKILL", mgr_type, target_mgr)

        if not _sigkill_daemon(rados_obj, "mgr", target_mgr, cluster_fsid):
            return False

        time.sleep(15)

        new_active = mgr_workflow_obj.get_active_mgr()
        if new_active:
            log.info(
                "MGR cluster healthy after SIGKILL of %s. " "Active MGR: %s",
                target_mgr,
                new_active,
            )
            return True

        log.info("No active MGR yet, waiting additional 30s for recovery...")
        time.sleep(30)
        new_active = mgr_workflow_obj.get_active_mgr()
        if new_active:
            log.info("MGR recovered after extended wait. Active: %s", new_active)
            return True

        log.warning("No active MGR after SIGKILL of %s", target_mgr)
        return False

    except Exception as e:
        log.error("MGR sigkill thrash failed: %s", e)
        return False
