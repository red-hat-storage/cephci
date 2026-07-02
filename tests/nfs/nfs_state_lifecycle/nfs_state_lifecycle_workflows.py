"""
NFS State Lifecycle Workflow Helpers
====================================

Provides the NfsStateLifecycleWorkflows class with building blocks for
dual-layer chaos testing of NFS-Ganesha state management.

Components:
    - Phase Infrastructure: health checks, crash/coredump detection,
      background health monitoring, crash archive
    - Ganesha Config Management: Lease_Lifetime override via config-key
      template modification + orch redeploy (follows set_cluster_delegation
      pattern from nfs_delegation_operations.py)
    - Client Identity: gRPC GetClientIds/GetSessionIds queries via
      grpcurl (reuses test_nfs_grpc.py patterns)
    - State Tracking: Ganesha container log parsing for _state_add_impl,
      dec_nfs4_state_ref, cid_refcount patterns at STATE=FULL_DEBUG
    - Churn Generation: Wraps thrash_nfs_client_churn with state sampling
    - Network Manipulation: iptables INPUT+OUTPUT block, tc netem delay
    - Error Injection: CephErrorInjector wrapper for L2 scenarios
    - Behavioral Roles: Probabilistic op generator per client role

All helpers are designed to work with 1..N clients via round-robin
assignment.  Scenarios degrade gracefully when fewer clients are available.
"""

import json
import random
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from threading import Thread

from ceph.rados.ceph_error_injector import CephErrorInjector
from ceph.rados.utils import get_cluster_timestamp
from utility.log import Log

log = Log(__name__)

GRPC_PORT = 50051
_PHASE_SEP = "-" * 70

_L2_DEFAULTS = {
    "client_identity_lifecycle": {
        "configs": {
            "client_inject_fixed_oldest_tid": True,
            "ms_inject_socket_failures": 500,
        }
    },
    "lease_expiry_under_load": {"profile": "client_session_stress"},
    "state_cardinality_stress": {"configs": {"client_inject_release_failure": True}},
    "client_state_churn": {"profile": "network_chaos"},
    "export_reorg_state_integrity": {"profile": "mds_chaos"},
    "generative_client_behavior": {
        "profile": "network_latency",
        "configs": {"client_debug_inject_tick_delay": 5},
    },
    "admin_op_interleave": {
        "profile": "mds_chaos",
        "configs": {"ms_inject_delay_probability": 0.03},
    },
    "observability_validation": {"profile": "client_session_stress"},
    "rapid_reconnect_storms": {
        "configs": {
            "ms_inject_socket_failures": 200,
            "objecter_inject_no_watch_ping": True,
        }
    },
    "combined_churn_failure": {"profile": "multi_layer_chaos"},
}

# Behavioral profiles for Scenario 6 (generative_client_behavior).
# Each profile controls the mix of NFS operations a client performs
# and what failure mode it simulates.
#
# Operation weights (read, write, lock, metadata) are probabilities
# passed to random.choices() inside run_churn_on_mount().
ROLE_PROFILES = {
    "reader_heavy": {
        "read": 0.80,
        "write": 0.05,
        "lock": 0.05,
        "metadata": 0.10,
    },
    "lock_heavy": {
        "read": 0.10,
        "write": 0.20,
        "lock": 0.60,
        "metadata": 0.10,
    },
    "metadata_heavy": {
        "read": 0.05,
        "write": 0.05,
        "lock": 0.05,
        "metadata": 0.85,
    },
    "flaky": {
        "read": 0.20,
        "write": 0.20,
        "lock": 0.10,
        "metadata": 0.10,
        "iptables_block_interval": (15, 30),
        "iptables_block_duration": (3, 10),
    },
    "slow_renew": {
        "read": 0.30,
        "write": 0.30,
        "lock": 0.20,
        "metadata": 0.20,
        "tc_delay_ms": 5000,
        "tc_jitter_ms": 3000,
    },
}


def _log_phase(number, title):
    """Log a phase separator banner with phase number and title."""
    log.info(f"\n{_PHASE_SEP}")
    log.info(f"  PHASE {number}: {title}")
    log.info(_PHASE_SEP)


def _sleep_with_stop(stop_event, total_seconds, step=2):
    """Sleep for ``total_seconds`` in increments of ``step``.

    Checks ``stop_event`` between each increment and returns early
    if the event is set.  Used by background threads for cooperative
    shutdown.

    Args:
        stop_event: threading.Event checked between sleep increments.
        total_seconds: Total time to sleep (seconds).
        step: Sleep increment size (seconds, default 2).
    """
    elapsed = 0
    while elapsed < total_seconds:
        if stop_event.is_set():
            return
        time.sleep(min(step, total_seconds - elapsed))
        elapsed += step


def _round_robin(items, index):
    """Select an item from ``items`` using round-robin by ``index``.

    Returns None if ``items`` is empty.  With 1 item and any index,
    always returns that item (all assignments go to the single client).

    Args:
        items: List to select from.
        index: Integer index (wraps via modulo).
    """
    if not items:
        return None
    return items[index % len(items)]


class NfsStateLifecycleWorkflows:
    """Workflow orchestrator for NFS state lifecycle scenarios.

    Instantiated once per scenario with cluster objects.  Provides
    phase-level building blocks that each scenario composes.

    All methods that accept ``clients`` work with any number of clients
    (1..N) via round-robin assignment.
    """

    def __init__(self, installer, nfs_nodes, clients, rados_obj, ceph_cluster):
        """Initialize workflows with cluster objects.

        Args:
            installer: CephNode with admin/installer role (runs ceph CLI).
            nfs_nodes: List of CephNodes with the nfs role (Ganesha hosts).
            clients: List of CephNodes with the client role (NFS clients).
            rados_obj: RadosOrchestrator instance for cluster operations.
            ceph_cluster: CephCluster object for node discovery.
        """
        self.installer = installer
        self.nfs_nodes = nfs_nodes
        self.clients = clients
        self.rados_obj = rados_obj
        self.ceph_cluster = ceph_cluster
        self._start_time = None
        self._template_backup_exists = False
        self._template_backed_up = False
        self._debug_logging_enabled = False
        self._grpc_enabled = False

    # ------------------------------------------------------------------
    #  Phase Infrastructure
    # ------------------------------------------------------------------

    def archive_crash_state(self):
        """Archive existing crashes and record start timestamp.

        Runs ``ceph crash archive-all`` to clear prior crash records,
        then captures the cluster timestamp for scoping future crash
        checks to this test run only.

        Returns:
            str: Cluster timestamp string for use with check_crash_status.
        """
        try:
            self.rados_obj.run_ceph_command(
                cmd="ceph crash archive-all", client_exec=True
            )
        except Exception as e:
            log.warning("Failed to archive crashes: %s", e)
        self._start_time = get_cluster_timestamp(self.rados_obj.node)
        log.info("Crash state archived, start_time=%s", self._start_time)
        return self._start_time

    def log_cluster_health(self):
        """Log ceph health detail + ceph -s."""
        try:
            self.rados_obj.log_cluster_health()
        except Exception as e:
            log.warning("Failed to log cluster health: %s", e)

    def check_crashes(self, phase_name):
        """Check for crashes since test start via core_workflows.

        Uses RadosOrchestrator.check_crash_status() which performs:
        1. ``ceph crash ls`` -- queries the MON crash database (reliable,
           survives daemon restarts and container redeploys).
        2. Daemon log scanning for crash patterns across mon, mgr, osd,
           mds, and nfs daemons (when check_nfs=True).

        Args:
            phase_name: Label for log messages (e.g. "VALIDATION", "CLEANUP").

        Returns:
            bool: True if any crash was found.
        """
        end_time = get_cluster_timestamp(self.rados_obj.node)
        try:
            if self.rados_obj.check_crash_status(
                start_time=self._start_time,
                end_time=end_time,
                check_nfs=True,
            ):
                log.error("[%s] Crash detected", phase_name)
                return True
        except Exception as e:
            log.warning("[%s] Crash check failed: %s", phase_name, e)
        return False

    def start_health_monitor(self, stop_event, interval=30):
        """Start a daemon thread that logs cluster health periodically.

        Logs ``ceph health detail``, ``ceph -s``, and Ganesha RSS memory
        every ``interval`` seconds until ``stop_event`` is set.

        Args:
            stop_event: threading.Event to signal shutdown.
            interval: Seconds between health snapshots (default 30).

        Returns:
            Thread: The started daemon thread (auto-cleaned on process exit).
        """

        def _monitor():
            count = 0
            while not stop_event.is_set():
                count += 1
                log.info(
                    "\n%s\nHEALTH MONITOR #%d | %s\n%s",
                    "=" * 60,
                    count,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "=" * 60,
                )
                self.log_cluster_health()
                try:
                    rss = self.get_ganesha_memory_rss()
                    log.info("[health_monitor] Ganesha RSS: %s KB", rss)
                except Exception:
                    pass
                _sleep_with_stop(stop_event, interval, step=5)

        t = Thread(target=_monitor, daemon=True)
        t.start()
        return t

    # ------------------------------------------------------------------
    #  Ganesha Config Management (template + redeploy)
    # ------------------------------------------------------------------

    def set_lease_lifetime(self, nfs_name, seconds):
        """Override Lease_Lifetime in Ganesha template and redeploy.

        Follows the backup -> modify -> config-key set -> redeploy pattern
        from nfs_delegation_operations.set_cluster_delegation().

        Alternative (less disruptive, no daemon restart / grace period):
            Modify the on-disk config directly on each NFS host and SIGHUP:

                path = /var/lib/ceph/<fsid>/nfs.<cluster>.<id>/etc/ganesha/ganesha.conf
                sed -i 's/Lease_Lifetime.*/Lease_Lifetime = N;/' <path>
                podman exec <container_id> kill -HUP 1

            This avoids the full redeploy cycle (container stop/start, grace
            period, client reconnection).  Ganesha reloads config on SIGHUP
            without dropping existing client sessions.  Useful when the test
            wants to change Lease_Lifetime mid-scenario without side effects.
            Not used here because the template+redeploy pattern is the
            established approach in the cephci delegation test infrastructure.
        """
        from nfs_delegation_operations import backup_ganesha_template, run_cephadm_shell

        if not self._template_backed_up:
            self._template_backup_exists = backup_ganesha_template(self.installer)
            self._template_backed_up = True

        work_file = "/tmp/_nfs_lease_template.conf"
        fallback_path = (
            "/usr/share/ceph/mgr/cephadm/templates/services/nfs/ganesha.conf.j2"
        )
        conf_key = "mgr/cephadm/services/nfs/ganesha.conf"

        run_cephadm_shell(
            self.installer,
            f"ceph config-key get {conf_key} > {work_file}",
            check_ec=False,
        )
        size_out, _ = self.installer.exec_command(
            sudo=True, cmd=f"wc -c < {work_file}", check_ec=False
        )
        if int(size_out.strip() or "0") < 10:
            run_cephadm_shell(
                self.installer,
                f"cat {fallback_path} > {work_file}",
            )

        lease_line = f"Lease_Lifetime = {seconds};"
        self.installer.exec_command(
            sudo=True,
            cmd=(
                f"grep -q 'Lease_Lifetime' {work_file} && "
                f"sed -i 's/Lease_Lifetime.*=.*[0-9].*;/{lease_line}/' {work_file} || "
                f"sed -i '/NFSv4 {{/a\\    {lease_line}' {work_file}"
            ),
        )

        mounted_path = "/var/lib/ceph/ganesha.conf"
        self.installer.exec_command(
            sudo=True,
            cmd=(
                f"cephadm shell --mount {work_file}:{mounted_path} "
                f"-- ceph config-key set {conf_key} -i {mounted_path}"
            ),
            timeout=60,
        )
        log.info("Lease_Lifetime set to %ds, redeploying NFS cluster...", seconds)
        self._redeploy_nfs(nfs_name)

    def restore_all_config(self, nfs_name):
        """Restore original Ganesha template (undoes both lease and debug changes).

        Writes the backed-up template to config-key but does NOT
        redeploy.  The NFS cluster is deleted immediately after this
        call in cleanup, so a redeploy would be wasted (~60s).  The
        restored template is picked up by the next scenario's
        create_nfs_cluster + apply_all_configs cycle.

        Uses the correct backup path depending on which method took the
        initial backup: enable_ganesha_debug_logging saves to
        LOG_TEMPLATE_BACKUP_PATH, backup_ganesha_template saves to
        BACKUP_TEMPLATE_PATH.
        """
        if not self._template_backed_up:
            return
        from nfs_delegation_operations import (
            BACKUP_TEMPLATE_PATH,
            LOG_TEMPLATE_BACKUP_PATH,
            restore_ganesha_template,
        )

        backup_path = (
            LOG_TEMPLATE_BACKUP_PATH
            if self._debug_logging_enabled
            else BACKUP_TEMPLATE_PATH
        )
        try:
            restore_ganesha_template(
                self.installer,
                self._template_backup_exists,
                backup_path=backup_path,
            )
            self._template_backed_up = False
            self._debug_logging_enabled = False
            log.info("Ganesha template restored in config-key (no redeploy)")
        except Exception as e:
            log.warning("Failed to restore Ganesha template: %s", e)

    def enable_state_debug_logging(self, nfs_name):
        """Enable NFS4 + STATE debug logging via template + redeploy."""
        from nfs_delegation_operations import enable_ganesha_debug_logging

        if not self._template_backed_up:
            self._template_backup_exists = enable_ganesha_debug_logging(
                cmd_host=self.installer,
            )
            self._template_backed_up = True
        else:
            enable_ganesha_debug_logging(cmd_host=self.installer)
        self._debug_logging_enabled = True
        self._redeploy_nfs(nfs_name)
        log.info("STATE+NFS4 debug logging enabled")

    def _redeploy_nfs(self, nfs_name, timeout=300):
        """Redeploy NFS cluster via ``ceph orch redeploy`` and wait.

        Recreates all NFS daemon containers for the cluster, picking up
        any template changes made via config-key set.  Waits for all
        daemons to reach ``running`` state.

        Args:
            nfs_name: NFS cluster name (e.g. ``lc-a3f2b1c4``).
            timeout: Max seconds to wait for daemons (default 300).
        """
        self.installer.exec_command(
            sudo=True,
            cmd=f"ceph orch redeploy nfs.{nfs_name}",
            timeout=60,
        )
        time.sleep(10)
        from nfs_operations import verify_nfs_ganesha_service

        verify_nfs_ganesha_service(node=self.installer, timeout=timeout)

    # ------------------------------------------------------------------
    #  gRPC Server Enablement
    # ------------------------------------------------------------------

    def enable_grpc(self, nfs_name):
        """Enable the gRPC server on the NFS-Ganesha cluster.

        Applies a ``GRPC {}`` config block via ``ceph nfs cluster config
        set``, then redeploys the cluster so Ganesha starts listening on
        port 50051.  After redeploy, verifies the port is open on the
        NFS node.

        The config is written to the ``.nfs`` RADOS pool (cluster-scoped)
        and is automatically cleaned up when the NFS cluster is deleted,
        so no explicit teardown is needed.

        The ``GRPC {}`` block was introduced in nfs-ganesha 6.5-5 (BZ
        2348728).  If the build does not support gRPC, the port check
        will fail and gRPC queries are skipped gracefully.

        Args:
            nfs_name: NFS cluster name (e.g. ``lc-a3f2b1c4``).
        """
        grpc_conf = "/tmp/_nfs_grpc_enable.conf"
        grpc_block = "GRPC {\\n    Grpc_Enable = true;\\n    Grpc_Port = 50051;\\n}"
        self.installer.exec_command(
            sudo=True,
            cmd=f"printf '{grpc_block}\\n' > {grpc_conf}",
        )
        self.installer.exec_command(
            sudo=True,
            cmd=f"ceph nfs cluster config set {nfs_name} -i {grpc_conf}",
            timeout=60,
        )
        log.info("gRPC config applied to cluster %s, redeploying...", nfs_name)
        self._redeploy_nfs(nfs_name)

        nfs_node = self.nfs_nodes[0]
        cmd = f"firewall-cmd --permanent --add-port={GRPC_PORT}/tcp"
        nfs_node.exec_command(sudo=True, cmd=cmd, check_ec=False)
        nfs_node.exec_command(sudo=True, cmd="firewall-cmd --reload", check_ec=False)

        from test_nfs_grpc import verify_grpc_port_availability

        nfs_ip = self.get_nfs_server_ip()
        for attempt in range(3):
            if verify_grpc_port_availability(nfs_node, nfs_ip):
                self._grpc_enabled = True
                log.info("gRPC server enabled on port %d", GRPC_PORT)
                return
            if attempt < 2:
                log.info(
                    "gRPC port not yet listening (attempt %d/3), retrying...",
                    attempt + 1,
                )
                time.sleep(10)
        log.warning(
            "gRPC port %d not listening after redeploy; "
            "gRPC queries will return empty results",
            GRPC_PORT,
        )

    def apply_all_configs(self, nfs_name):
        """Apply gRPC + debug logging configs with a single redeploy.

        Equivalent to calling enable_grpc() + enable_state_debug_logging()
        but performs only one ``orch redeploy`` cycle instead of two,
        saving ~60-90s per scenario.

        1. Writes ``GRPC {}`` block to the .nfs RADOS pool
        2. Opens the gRPC firewall port
        3. Applies STATE+NFS4 debug logging to the config-key template
        4. Single redeploy picks up both changes
        5. Verifies gRPC port is listening + installs grpcurl

        Args:
            nfs_name: NFS cluster name (e.g. ``lc-a3f2b1c4``).
        """
        grpc_conf = "/tmp/_nfs_grpc_enable.conf"
        grpc_block = "GRPC {\\n    Grpc_Enable = true;\\n    Grpc_Port = 50051;\\n}"
        self.installer.exec_command(
            sudo=True, cmd=f"printf '{grpc_block}\\n' > {grpc_conf}"
        )
        self.installer.exec_command(
            sudo=True,
            cmd=f"ceph nfs cluster config set {nfs_name} -i {grpc_conf}",
            timeout=60,
        )

        nfs_node = self.nfs_nodes[0]
        nfs_node.exec_command(
            sudo=True,
            cmd=f"firewall-cmd --permanent --add-port={GRPC_PORT}/tcp",
            check_ec=False,
        )
        nfs_node.exec_command(sudo=True, cmd="firewall-cmd --reload", check_ec=False)

        from nfs_delegation_operations import enable_ganesha_debug_logging

        if not self._template_backed_up:
            self._template_backup_exists = enable_ganesha_debug_logging(
                cmd_host=self.installer,
            )
            self._template_backed_up = True
        else:
            enable_ganesha_debug_logging(cmd_host=self.installer)
        self._debug_logging_enabled = True

        log.info(
            "gRPC + STATE debug logging applied to %s, single redeploy...",
            nfs_name,
        )
        self._redeploy_nfs(nfs_name)

        from test_nfs_grpc import verify_grpc_port_availability

        nfs_ip = self.get_nfs_server_ip()
        for attempt in range(3):
            if verify_grpc_port_availability(nfs_node, nfs_ip):
                self._grpc_enabled = True
                log.info("gRPC server enabled on port %d", GRPC_PORT)
                break
            if attempt < 2:
                log.info(
                    "gRPC port not yet listening (attempt %d/3), retrying...",
                    attempt + 1,
                )
                time.sleep(10)
        else:
            log.warning(
                "gRPC port %d not listening after redeploy; "
                "gRPC queries will return empty results",
                GRPC_PORT,
            )

        try:
            self.install_grpcurl()
        except Exception as e:
            log.warning("grpcurl install failed (gRPC checks will be skipped): %s", e)
        log.info("All cluster configs applied for %s", nfs_name)

    # ------------------------------------------------------------------
    #  Client Identity via gRPC
    # ------------------------------------------------------------------

    def install_grpcurl(self):
        """Install grpcurl on the first client (driver node).

        Downloads grpcurl v1.8.9 from GitHub.  Requires internet access
        on the client node.  Skips if already installed.
        """
        from test_nfs_grpc import install_grpcurl

        install_grpcurl(self.clients[0])

    def get_nfs_server_ip(self):
        """Resolve IP of the first NFS node."""
        return self.nfs_nodes[0].ip_address

    def get_client_ids(self):
        """Query active NFS client IDs via gRPC GetClientIds.

        Delegates to ``test_nfs_grpc.get_client_ids()``.

        Returns:
            tuple: (success: bool, client_ids: list of str).
            Returns (False, []) if grpcurl is not installed or gRPC fails.
        """
        from test_nfs_grpc import get_client_ids

        return get_client_ids(self.clients[0], self.get_nfs_server_ip())

    def get_session_ids(self):
        """Query active NFS session IDs via gRPC GetSessionIds.

        Delegates to ``test_nfs_grpc.get_session_ids()``.

        Returns:
            tuple: (success: bool, session_ids: list of str).
            Returns (False, []) if grpcurl is not installed or gRPC fails.
        """
        from test_nfs_grpc import get_session_ids

        return get_session_ids(self.clients[0], self.get_nfs_server_ip())

    def verify_client_convergence(self, expected_count, timeout=120):
        """Poll gRPC GetClientIds until client count matches expected.

        Args:
            expected_count: Target number of active NFS clients.
            timeout: Max seconds to wait (default 120).

        Returns:
            bool: True if count converged within timeout.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            ok, ids = self.get_client_ids()
            if ok and len(ids) == expected_count:
                log.info("Client count converged: %d == %d", len(ids), expected_count)
                return True
            time.sleep(10)
        ok, ids = self.get_client_ids()
        log.warning(
            "Client convergence timeout: got %d, expected %d",
            len(ids) if ok else -1,
            expected_count,
        )
        return False

    # ------------------------------------------------------------------
    #  State Tracking via Log Parsing
    # ------------------------------------------------------------------

    def _resolve_daemon_id(self, nfs_node, nfs_name=""):
        """Resolve NFS daemon ID for a node, with per-instance caching.

        The daemon ID is stable for the lifetime of a scenario (between
        create_nfs_cluster and delete_nfs_cluster).  Caching avoids
        redundant ``get_service_spec_daemons`` calls during the
        validation phase where multiple log-grep methods are called
        in quick succession.
        """
        cache_key = (nfs_node.hostname, nfs_name)
        if hasattr(self, "_daemon_id_cache") and cache_key in self._daemon_id_cache:
            return self._daemon_id_cache[cache_key]

        daemon_id = ""
        svc = f"nfs.{nfs_name}" if nfs_name else "nfs"
        try:
            all_ids = self.rados_obj.get_service_spec_daemons(service_name=svc)
            for did in all_ids:
                if nfs_node.hostname in str(did):
                    daemon_id = str(did)
                    break
            if not daemon_id and all_ids:
                daemon_id = str(all_ids[0])
        except Exception:
            pass

        if not hasattr(self, "_daemon_id_cache"):
            self._daemon_id_cache = {}
        if daemon_id:
            self._daemon_id_cache[cache_key] = daemon_id
        return daemon_id

    def _invalidate_daemon_cache(self):
        """Clear cached daemon IDs (call after cluster create/delete)."""
        self._daemon_id_cache = {}

    def _grep_ganesha_log(self, nfs_node, pattern, nfs_name=""):
        """Grep Ganesha logs for a pattern via cephadm logs.

        Resolves the daemon ID (cached) then uses ``cephadm logs
        --name nfs.<daemon_id>`` which goes through journalctl and
        survives container restarts/redeploys.  Returns match count.
        """
        daemon_id = self._resolve_daemon_id(nfs_node, nfs_name)
        if not daemon_id:
            log.warning("No NFS daemon found on %s", nfs_node.hostname)
            return 0

        cmd = (
            f"cephadm logs --name nfs.{daemon_id} "
            f"-- --no-pager 2>/dev/null | grep -c '{pattern}'"
        )
        out, _ = nfs_node.exec_command(sudo=True, cmd=cmd, check_ec=False)
        try:
            return int(out.strip())
        except (ValueError, TypeError):
            return 0

    def count_state_add(self, nfs_name=""):
        """Count state object creation events in Ganesha logs.

        Searches for ``_state_add_impl`` in Ganesha debug logs across all
        NFS nodes.  Requires STATE=FULL_DEBUG to be enabled.
        """
        total = 0
        for node in self.nfs_nodes:
            total += self._grep_ganesha_log(node, "_state_add_impl", nfs_name)
        return total

    def count_state_deleted(self, nfs_name=""):
        """Count state objects actually freed in Ganesha logs.

        Ganesha's log format includes the calling function name on every
        line.  ``dec_nfs4_state_ref()`` emits ``"Deleted <stateid>"``
        only when the refcount reaches 0 and the object is freed:

            :dec_nfs4_state_ref :sal_functions.c-NNN :Deleted State 0x...

        This is a 1:1 match with ``_state_add_impl`` (one log per state
        object creation).  The intermediate ``dec_nfs4_state_ref`` lines
        (refcount > 0) are NOT matched by this pattern -- those are
        temporary reference releases that occur many times per object
        lifetime.

        Requires STATE=FULL_DEBUG to be enabled (via
        enable_state_debug_logging).  Returns 0 if debug logging is
        not active.
        """
        total = 0
        for node in self.nfs_nodes:
            total += self._grep_ganesha_log(
                node, "dec_nfs4_state_ref.*Deleted", nfs_name
            )
        return total

    def check_cid_refcount_negative(self, nfs_name=""):
        """Check for negative client ID refcount values in Ganesha logs.

        A negative cid_refcount indicates a refcount underflow bug in
        Ganesha's state management.  This is always a test failure.

        Returns:
            bool: True if negative refcount detected.
        """
        for node in self.nfs_nodes:
            count = self._grep_ganesha_log(node, "cid_refcount.*= *-", nfs_name)
            if count > 0:
                log.error("Negative cid_refcount on %s (%d hits)", node.hostname, count)
                return True
        return False

    def check_ibmceph_13072_flooding(self, nfs_name="", threshold=100):
        """Check for IBMCEPH-13072: excessive expired-state log flooding.

        After lease expiry, Ganesha may log the same expired-state message
        repeatedly.  If the count exceeds ``threshold``, this indicates
        the known log flooding bug.

        Args:
            nfs_name: NFS cluster name for daemon ID resolution.
            threshold: Max acceptable expired-state log lines (default 100).

        Returns:
            bool: True if flooding detected.
        """
        for node in self.nfs_nodes:
            count = self._grep_ganesha_log(node, "expired.*state", nfs_name)
            if count > threshold:
                log.error(
                    "IBMCEPH-13072: %d expired state lines on %s (threshold %d)",
                    count,
                    node.hostname,
                    threshold,
                )
                return True
        return False

    def sample_state_snapshot(self, nfs_name=""):
        """Capture a composite state snapshot for convergence analysis.

        Collects: gRPC client/session counts (if gRPC is enabled),
        Ganesha log-based state add/deleted counters, and Ganesha
        process RSS memory.

        Returns:
            dict with keys: timestamp, client_count, session_count,
            state_add, state_deleted, rss_kb.
        """
        if self._grpc_enabled:
            ok_c, client_ids = self.get_client_ids()
            ok_s, session_ids = self.get_session_ids()
        else:
            ok_c, client_ids = False, []
            ok_s, session_ids = False, []
        return {
            "timestamp": time.time(),
            "client_count": len(client_ids) if ok_c else -1,
            "session_count": len(session_ids) if ok_s else -1,
            "state_add": self.count_state_add(nfs_name),
            "state_deleted": self.count_state_deleted(nfs_name),
            "rss_kb": self.get_ganesha_memory_rss(),
        }

    def verify_state_convergence(self, baseline, current):
        """Compare two state snapshots for convergence.

        Fails if:
        - gRPC was enabled but client query failed in the current snapshot.

        RSS growth is logged for visibility but does NOT fail the test.
        Leak detection is handled entirely by ``check_memory_stabilization``
        which monitors RSS trends after I/O ceases.

        When gRPC is not enabled (``_grpc_enabled`` is False), client/session
        counts of -1 are expected and do not cause failure.

        Args:
            baseline: Snapshot from pre-thrash phase.
            current: Snapshot from post-thrash validation.

        Returns:
            bool: True if all convergence checks pass.
        """
        issues = []
        if self._grpc_enabled and current["client_count"] < 0:
            issues.append("gRPC client query failed")
        elif not self._grpc_enabled and current["client_count"] < 0:
            log.info("[convergence] gRPC not enabled; skipping client count check")
        if baseline["rss_kb"] > 0 and current["rss_kb"] > 0:
            growth = (current["rss_kb"] - baseline["rss_kb"]) / baseline["rss_kb"]
            log.info(
                "[convergence] RSS growth: %+.1f%% (%dKB -> %dKB) "
                "[informational -- leak detection via check_memory_stabilization]",
                growth * 100,
                baseline["rss_kb"],
                current["rss_kb"],
            )
        if issues:
            for msg in issues:
                log.error("[convergence] %s", msg)
            return False
        log.info("[convergence] State converged")
        return True

    # ------------------------------------------------------------------
    #  Ganesha Memory
    # ------------------------------------------------------------------

    def get_ganesha_memory_rss(self):
        """Get RSS of the nfs-ganesha process on the first NFS node.

        Tries nfs_operations.get_nfs_pid_and_memory() first, falls back
        to ``ps -C ganesha.nfsd -o rss=`` if the import fails.

        Returns:
            int: RSS in KB, or 0 if unavailable.
        """
        try:
            from nfs_operations import get_nfs_pid_and_memory

            result = get_nfs_pid_and_memory(self.nfs_nodes[:1])
            if result:
                hostname = self.nfs_nodes[0].hostname
                if hostname in result:
                    return int(result[hostname][1])
        except Exception:
            pass
        try:
            out, _ = self.nfs_nodes[0].exec_command(
                sudo=True,
                cmd="ps -C ganesha.nfsd -o rss= | head -1",
                check_ec=False,
            )
            return int(out.strip()) if out.strip() else 0
        except Exception:
            return 0

    def check_memory_stabilization(self, samples=5, interval=60, margin_kb=2048):
        """Check whether Ganesha RSS is stabilizing or still growing.

        After I/O stops, Ganesha's RSS should flatten (warm cache, malloc
        arenas) or decline (LRU eviction, cap release).  A monotonically
        increasing trend indicates a potential memory leak.

        Collects ``samples`` RSS readings at ``interval`` second gaps.
        Computes consecutive deltas.  Fails if ALL deltas are positive
        and exceed ``margin_kb``.

        The margin accounts for background jitter (MDCACHE LRU churn,
        MDS cap renewal, malloc arena overhead).  Observed steady-state
        jitter on VMs is ~170 KB; 2 MB (2048 KB) gives ~10x headroom.
        On baremetal with more cores/NUMA, jitter may be higher; margin
        is floored at ``margin_kb`` or 1% of the first sample's RSS,
        whichever is larger, to scale with hardware.

        Args:
            samples: Number of RSS samples to collect (default 5).
            interval: Seconds between samples (default 60).
            margin_kb: Minimum growth per interval to count as
                increasing (default 2048 KB = 2 MB).

        Returns:
            bool: True if memory is stabilized (no leak).  False if a
            monotonic upward trend was detected.
        """
        readings = []
        for i in range(samples):
            rss = self.get_ganesha_memory_rss()
            readings.append(rss)
            log.info(
                "[mem_stabilization] Sample %d/%d: RSS = %d KB",
                i + 1,
                samples,
                rss,
            )
            if i < samples - 1:
                time.sleep(interval)

        if readings[0] > 0:
            effective_margin = max(margin_kb, int(readings[0] * 0.01))
        else:
            effective_margin = margin_kb

        deltas = [readings[i + 1] - readings[i] for i in range(len(readings) - 1)]
        if len(deltas) < 2:
            log.info(
                "[mem_stabilization] Too few samples (%d) for trend analysis; "
                "assuming stabilized",
                len(readings),
            )
            return True

        log.info(
            "[mem_stabilization] Deltas (KB): %s (margin: %d KB)",
            deltas,
            effective_margin,
        )

        all_growing = all(d > effective_margin for d in deltas)
        if all_growing:
            total_growth = readings[-1] - readings[0]
            log.error(
                "[mem_stabilization] LEAK SUSPECTED: RSS grew monotonically "
                "across all %d intervals (%d KB -> %d KB, +%d KB total, "
                "all deltas > %d KB margin)",
                len(deltas),
                readings[0],
                readings[-1],
                total_growth,
                effective_margin,
            )
            return False

        stable_count = sum(1 for d in deltas if d <= effective_margin)
        log.info(
            "[mem_stabilization] RSS stabilized: %d/%d intervals flat or "
            "declining (%d KB -> %d KB)",
            stable_count,
            len(deltas),
            readings[0],
            readings[-1],
        )
        return True

    # ------------------------------------------------------------------
    #  NFS Cluster Lifecycle
    # ------------------------------------------------------------------

    def create_nfs_cluster(self, nfs_name, placement=None):
        """Create an NFS-Ganesha cluster and wait for daemons to start.

        Each test scenario calls this to spin up an isolated NFS cluster
        so that state from one scenario cannot leak into the next.

        Args:
            nfs_name: Unique cluster name (e.g. ``lc-a3f2b1c4``).
            placement: Hostname(s) for daemon placement.  Accepts a
                single hostname string or a list.  Defaults to the
                first NFS node's hostname.

        Raises:
            RuntimeError: If daemons do not reach ``running`` within 300s.
        """
        if placement is None:
            placement = self.nfs_nodes[0].hostname

        cmd = f"ceph nfs cluster create {nfs_name} {placement}"
        log.info("Creating NFS cluster: %s", cmd)
        self.installer.exec_command(sudo=True, cmd=cmd, timeout=60)

        from nfs_operations import verify_nfs_ganesha_service

        verify_nfs_ganesha_service(node=self.installer, timeout=300)
        self._invalidate_daemon_cache()
        log.info("NFS cluster %s is running", nfs_name)

    def delete_nfs_cluster(self, nfs_name):
        """Delete an NFS-Ganesha cluster and wait for daemons to be removed.

        Uses ``ceph nfs cluster rm`` (non-deprecated form).  Waits up
        to 300s for ``ceph orch ls --service-type=nfs`` to confirm no
        daemons remain for this cluster.

        Args:
            nfs_name: Name of the NFS cluster to delete.
        """
        try:
            self.installer.exec_command(
                sudo=True,
                cmd=f"ceph nfs cluster rm {nfs_name}",
                timeout=30,
            )
        except Exception as e:
            log.warning("Cluster rm for %s: %s", nfs_name, e)

        for attempt in range(30):
            time.sleep(10)
            try:
                out, _ = self.installer.exec_command(
                    sudo=True,
                    cmd=f"ceph orch ps --service_name nfs.{nfs_name} -f json",
                    check_ec=False,
                )
                daemons = json.loads(out.strip()) if out.strip() else []
                if not daemons:
                    log.info("NFS cluster %s: all daemons removed", nfs_name)
                    self._invalidate_daemon_cache()
                    return
            except (json.JSONDecodeError, ValueError):
                log.info("NFS cluster %s: daemons removed", nfs_name)
                self._invalidate_daemon_cache()
                return
        log.warning("NFS cluster %s: daemons may still remain after 300s", nfs_name)
        self._invalidate_daemon_cache()

    # ------------------------------------------------------------------
    #  NFS Export & Mount Helpers (round-robin across clients)
    # ------------------------------------------------------------------

    def create_exports(self, nfs_name, fs_name, num_exports, subvol_group=None):
        """Create N NFS exports via ``ceph nfs export create cephfs``.

        Each export gets a pseudo path ``/export_lifecycle_<i>`` backed
        by the root of the CephFS filesystem.

        Args:
            nfs_name: NFS cluster name.
            fs_name: CephFS filesystem name.
            num_exports: Number of exports to create.
            subvol_group: Unused, reserved for future subvolume support.

        Returns:
            list of dict: [{pseudo, path, index}, ...] for each created export.
        """
        from cli.ceph.ceph import Ceph

        exports = []
        for i in range(num_exports):
            pseudo = f"/export_lifecycle_{i}"
            try:
                Ceph(self.installer).nfs.export.create(
                    fs_name=fs_name,
                    nfs_name=nfs_name,
                    nfs_export=pseudo,
                    fs=fs_name,
                )
                exports.append({"pseudo": pseudo, "path": "/", "index": i})
                log.info("Created export %s on %s", pseudo, nfs_name)
            except Exception as e:
                log.error("Failed to create export %s: %s", pseudo, e)
        return exports

    def mount_exports_round_robin(
        self, exports, nfs_name, nfs_server, port="2049", version="4.2"
    ):
        """Mount exports across clients round-robin.

        Works with any number of clients (1..N).  Returns list of
        assignment dicts: [{client, mount_point, export, index}, ...]
        """
        from nfs_operations import mount_retry

        assignments = []
        for i, exp in enumerate(exports):
            client = _round_robin(self.clients, i)
            mp = f"/mnt/lifecycle_{nfs_name}_{i}"
            try:
                client.exec_command(sudo=True, cmd=f"mkdir -p {mp}", timeout=10)
                mount_retry(
                    client,
                    mp,
                    version,
                    port,
                    nfs_server,
                    exp["pseudo"],
                )
                assignments.append(
                    {
                        "client": client,
                        "mount_point": mp,
                        "export": exp,
                        "index": i,
                    }
                )
                log.info(
                    "Mounted %s:%s on %s -> %s",
                    nfs_server,
                    exp["pseudo"],
                    client.hostname,
                    mp,
                )
            except Exception as e:
                log.error("Failed to mount %s: %s", exp["pseudo"], e)
        return assignments

    def unmount_all(self, assignments):
        """Lazy-unmount (``umount -l``) and remove all mount directories.

        Errors are silently ignored since mounts may already be stale.
        """
        for a in assignments:
            try:
                a["client"].exec_command(
                    sudo=True,
                    cmd=f"umount -l {a['mount_point']}",
                    check_ec=False,
                    timeout=30,
                )
                a["client"].exec_command(
                    sudo=True,
                    cmd=f"rm -rf {a['mount_point']}",
                    check_ec=False,
                    timeout=10,
                )
            except Exception:
                pass

    def delete_exports(self, nfs_name, exports):
        """Delete all NFS exports via ``ceph nfs export rm``."""
        from cli.ceph.ceph import Ceph

        for exp in exports:
            try:
                Ceph(self.installer).nfs.export.delete(nfs_name, exp["pseudo"])
            except Exception as e:
                log.debug("Export delete %s: %s", exp["pseudo"], e)

    # ------------------------------------------------------------------
    #  Data Integrity
    # ------------------------------------------------------------------

    _INTEGRITY_FILE_SPECS = [
        ("integrity_4k", "4k", "4k"),
        ("integrity_64k", "64k", "4k"),
        ("integrity_256k", "256k", "4k"),
        ("integrity_1m", "1M", "16k"),
        ("integrity_4m", "4M", "64k"),
    ]

    def write_integrity_baseline(self, assignments):
        """Write fio baseline files with CRC32C checksums on each mount.

        Creates a ``thrash_integrity/`` directory under each mount point
        and writes files of varying sizes (4K to 4M) using
        ``fio --verify=crc32c --do_verify=0`` (write-only pass).
        The verify pass is done separately by verify_data_integrity().
        """
        for a in assignments:
            client = a["client"]
            idir = f"{a['mount_point']}/thrash_integrity"
            try:
                client.exec_command(sudo=True, cmd=f"mkdir -p {idir}", timeout=15)
                for name, fsize, bs in self._INTEGRITY_FILE_SPECS:
                    client.exec_command(
                        sudo=True,
                        cmd=(
                            f"fio --name={name} --directory={idir} "
                            f"--size={fsize} --bs={bs} --rw=write --numjobs=1 "
                            f"--verify=crc32c --verify_dump=1 --do_verify=0 "
                            f"--ioengine=libaio --direct=1"
                        ),
                        timeout=120,
                    )
                log.info("[integrity] Baseline written: %s", idir)
            except Exception as e:
                log.warning("[integrity] Baseline failed at %s: %s", idir, e)

    def verify_data_integrity(self, assignments):
        """Re-read and verify CRC32C checksums on integrity baseline files.

        Runs ``fio --verify=crc32c --do_verify=1`` on each file written
        by write_integrity_baseline().  A checksum mismatch indicates
        data corruption (test failure).

        Returns:
            bool: True if all files verified OK, False on any mismatch.
        """
        all_ok = True
        for a in assignments:
            client = a["client"]
            idir = f"{a['mount_point']}/thrash_integrity"
            for name, fsize, bs in self._INTEGRITY_FILE_SPECS:
                try:
                    client.exec_command(
                        sudo=True,
                        cmd=(
                            f"fio --name={name} --directory={idir} "
                            f"--size={fsize} --bs={bs} --rw=read --numjobs=1 "
                            f"--verify=crc32c --verify_dump=1 --do_verify=1 "
                            f"--ioengine=libaio --direct=1"
                        ),
                        timeout=120,
                    )
                except Exception as e:
                    log.error(
                        "[integrity] Verify FAILED on %s/%s: %s",
                        a["mount_point"],
                        name,
                        e,
                    )
                    all_ok = False
        return all_ok

    def check_mount_health(self, assignments):
        """Check for stale mounts via stat.  Returns list of stale mount points."""
        stale = []
        for a in assignments:
            try:
                a["client"].exec_command(
                    sudo=True,
                    cmd=f"stat {a['mount_point']} && ls {a['mount_point']}",
                    timeout=10,
                )
            except Exception:
                stale.append(a["mount_point"])
                log.warning("[mount_health] Stale: %s", a["mount_point"])
        return stale

    def heal_stale_mounts(self, assignments, stale_mps, nfs_server, nfs_name):
        """Attempt to recover stale mounts via lazy unmount + remount.

        After chaos scenarios (iptables cycling, orch restarts), the
        kernel NFS client may mark mounts as stale even after the
        network and server are healthy.  A remount is the only way to
        recover -- the kernel won't auto-heal a stale NFS handle.

        Retries up to 3 times with increasing wait intervals (15s,
        30s, 45s) to handle Combined injection scenarios where the
        backend may still be draining faults when the first attempt
        runs.

        Args:
            assignments: Full list of mount assignment dicts.
            stale_mps: List of stale mount point paths (from check_mount_health).
            nfs_server: NFS server hostname for remount.
            nfs_name: NFS cluster name (for logging).

        Returns:
            list: Mount points that remain stale after all recovery attempts.
        """
        from nfs_operations import mount_retry

        still_stale = list(stale_mps)
        max_attempts = 3
        wait_intervals = [15, 30, 45]

        for attempt in range(1, max_attempts + 1):
            stale_assignments = [
                a for a in assignments if a["mount_point"] in still_stale
            ]
            if not stale_assignments:
                break

            log.info(
                "[heal_mounts] Attempt %d/%d: recovering %d stale mount(s)",
                attempt,
                max_attempts,
                len(stale_assignments),
            )
            for a in stale_assignments:
                client = a["client"]
                mp = a["mount_point"]
                pseudo = a["export"]["pseudo"]
                try:
                    client.exec_command(
                        sudo=True,
                        cmd=f"umount -l {mp}",
                        check_ec=False,
                        timeout=30,
                    )
                    time.sleep(5)
                    client.exec_command(sudo=True, cmd=f"mkdir -p {mp}", timeout=10)
                    mount_retry(client, mp, "4.2", "2049", nfs_server, pseudo)
                    log.info("[heal_mounts] Recovered: %s on %s", mp, client.hostname)
                except Exception as e:
                    log.warning("[heal_mounts] Recovery failed for %s: %s", mp, e)

            wait = wait_intervals[attempt - 1]
            log.info("[heal_mounts] Waiting %ds before recheck...", wait)
            time.sleep(wait)
            still_stale = self.check_mount_health(assignments)

            if not still_stale:
                log.info("[heal_mounts] All mounts recovered on attempt %d", attempt)
                return []

        if still_stale:
            log.warning(
                "[heal_mounts] %d mount(s) still stale after %d attempts: %s",
                len(still_stale),
                max_attempts,
                still_stale,
            )
        return still_stale

    # ------------------------------------------------------------------
    #  Network Manipulation
    # ------------------------------------------------------------------

    def block_client_nfs_traffic(self, client, seconds):
        """Block NFS traffic on a client for ``seconds``, then unblock.

        Adds iptables DROP rules on both OUTPUT (to server port 2049)
        and INPUT (from server port 2049) to simulate full network loss
        from the NFS server's perspective.  After sleeping, removes the
        rules via _flush_iptables_client().

        Args:
            client: CephNode to block.
            seconds: Duration of the block in seconds.
        """
        log.info("Blocking NFS traffic on %s for %ds", client.hostname, seconds)
        client.exec_command(
            sudo=True,
            cmd=(
                "iptables -A OUTPUT -p tcp --dport 2049 -j DROP && "
                "iptables -A INPUT -p tcp --sport 2049 -j DROP"
            ),
            check_ec=False,
        )
        time.sleep(seconds)
        self._flush_iptables_client(client)

    def block_all_clients_simultaneously(self, stagger_map):
        """Block all clients simultaneously, then unblock with stagger.

        Simulates a thundering-herd reconnect scenario.  All clients are
        blocked at the same instant, then unblocked at different times
        to test staggered reconnection behavior.

        Args:
            stagger_map: dict of {client_index: unblock_delay_seconds}.
                Delays are absolute from the block time, not relative to
                each other.  Example: {0: 3, 1: 5, 2: 8} unblocks client
                0 at +3s, client 1 at +5s, client 2 at +8s.
        """
        for client in self.clients:
            client.exec_command(
                sudo=True,
                cmd=(
                    "iptables -A OUTPUT -p tcp --dport 2049 -j DROP && "
                    "iptables -A INPUT -p tcp --sport 2049 -j DROP"
                ),
                check_ec=False,
            )
        block_time = time.time()
        log.info("All %d clients blocked", len(self.clients))

        for idx, delay in sorted(stagger_map.items(), key=lambda x: x[1]):
            elapsed = time.time() - block_time
            remaining = delay - elapsed
            if remaining > 0:
                time.sleep(remaining)
            client = _round_robin(self.clients, idx)
            if client:
                self._flush_iptables_client(client)
                log.info(
                    "Unblocked %s at +%ds",
                    client.hostname,
                    int(time.time() - block_time),
                )

    def _flush_iptables_client(self, client):
        """Remove NFS-specific iptables rules added by this test."""
        client.exec_command(
            sudo=True,
            cmd=(
                "iptables -D OUTPUT -p tcp --dport 2049 -j DROP 2>/dev/null; "
                "iptables -D INPUT -p tcp --sport 2049 -j DROP 2>/dev/null; "
                "true"
            ),
            check_ec=False,
        )

    def add_client_latency(self, client, delay_ms, jitter_ms=0):
        """Add network latency on a client via ``tc qdisc add netem delay``.

        Applied to the default network interface.  Used by the slow_renew
        role to simulate a client whose lease renewals arrive late.

        Args:
            client: CephNode to add latency on.
            delay_ms: Base delay in milliseconds.
            jitter_ms: Jitter (+/-) in ms with normal distribution (default 0).
        """
        iface = self._get_default_iface(client)
        jitter_part = f" {jitter_ms}ms distribution normal" if jitter_ms else ""
        client.exec_command(
            sudo=True,
            cmd=(
                f"tc qdisc add dev {iface} root netem "
                f"delay {delay_ms}ms{jitter_part}"
            ),
            check_ec=False,
        )
        log.info(
            "Added %dms (+/-%dms) latency on %s:%s",
            delay_ms,
            jitter_ms,
            client.hostname,
            iface,
        )

    def remove_client_latency(self, client):
        """Remove tc netem qdisc from client's default interface."""
        iface = self._get_default_iface(client)
        client.exec_command(
            sudo=True,
            cmd=f"tc qdisc del dev {iface} root",
            check_ec=False,
        )

    def _get_default_iface(self, node):
        """Get the default network interface name."""
        out, _ = node.exec_command(
            sudo=True,
            cmd="ip route | grep default | awk '{print $5}' | head -1",
            check_ec=False,
        )
        return out.strip() or "eth0"

    def flush_iptables_all(self):
        """Safety cleanup: flush all iptables rules on all test nodes.

        Uses full flush (-F) intentionally to guarantee no leftover rules
        from any code path.  Called only during test cleanup phase.
        """
        for node in self.clients + self.nfs_nodes:
            try:
                node.exec_command(sudo=True, cmd="iptables -F", check_ec=False)
            except Exception:
                pass

    # ------------------------------------------------------------------
    #  Error Injection (Layer 2)
    # ------------------------------------------------------------------

    def create_injector(self):
        """Create a CephErrorInjector for Layer 2 error injection.

        Returns:
            CephErrorInjector: Instance bound to this cluster's RadosOrchestrator.
        """
        return CephErrorInjector(rados_obj=self.rados_obj)

    def get_l2_defaults(self, scenario_name):
        """Get default Layer 2 error injection config for a scenario.

        Returns the CephErrorInjector config dict from _L2_DEFAULTS,
        or empty dict if the scenario has no default injection.
        """
        return _L2_DEFAULTS.get(scenario_name, {})

    def apply_scenario_injection(self, injector, scenario_name, config):
        """Apply Layer 2 error injection for a scenario.

        Uses the ``error_injection`` key from config if present,
        otherwise falls back to the scenario's default from _L2_DEFAULTS.

        Args:
            injector: CephErrorInjector instance.
            scenario_name: Key into _L2_DEFAULTS.
            config: Test config dict (may contain ``error_injection`` override).
        """
        ei_config = config.get("error_injection")
        if ei_config is None:
            ei_config = self.get_l2_defaults(scenario_name)
        if ei_config:
            injector.apply_from_config(ei_config)
            log.info(
                "L2 error injection active for %s: %s",
                scenario_name,
                ei_config,
            )

    def cleanup_injection(self, injector):
        """Remove all active error injections and wait for cluster recovery.

        Calls ``injector.cleanup_all()`` to remove all MON config-key
        injections, then sleeps 30s for the cluster to stabilize.
        Safe to call multiple times (idempotent).
        """
        if injector:
            try:
                injector.cleanup_all()
                log.info("L2 error injection removed, waiting 30s for recovery")
                time.sleep(30)
            except Exception as e:
                log.warning("Injection cleanup failed: %s", e)

    # ------------------------------------------------------------------
    #  Churn Generation
    # ------------------------------------------------------------------

    def run_churn_on_mount(
        self, client, mount_point, stop_event, churn_rate=5, role=None
    ):
        """Single-client churn loop: random file ops on a mount point.

        Operations weighted by role profile from ROLE_PROFILES.
        If no role specified, uses equal probability.
        Runs until stop_event is set.
        """
        ops = {"creates": 0, "writes": 0, "reads": 0, "locks": 0, "errors": 0}
        idx = 0

        op_choices = ["read", "create", "lock", "write"]
        if role and role in ROLE_PROFILES:
            p = ROLE_PROFILES[role]
            weights = [
                p.get("read", 0.25),
                p.get("write", 0.25),
                p.get("lock", 0.25),
                p.get("metadata", 0.25),
            ]
        else:
            weights = [0.25, 0.25, 0.25, 0.25]

        while not stop_event.is_set():
            for _ in range(churn_rate):
                if stop_event.is_set():
                    break
                idx += 1
                fname = f"{mount_point}/churn_{idx}"
                op = random.choices(op_choices, weights=weights, k=1)[0]
                try:
                    if op == "create":
                        client.exec_command(
                            sudo=True,
                            cmd=f"dd if=/dev/urandom of={fname} bs=4k count=1 2>/dev/null",
                            check_ec=False,
                            timeout=15,
                        )
                        ops["creates"] += 1
                    elif op == "write":
                        client.exec_command(
                            sudo=True,
                            cmd=f"echo 'data' >> {fname}",
                            check_ec=False,
                            timeout=10,
                        )
                        ops["writes"] += 1
                    elif op == "read":
                        client.exec_command(
                            sudo=True,
                            cmd=f"cat {fname} > /dev/null 2>&1",
                            check_ec=False,
                            timeout=10,
                        )
                        ops["reads"] += 1
                    elif op == "lock":
                        lock_cmd = (
                            f'python3 -c "import fcntl,time;'
                            f"f=open('{fname}','a');"
                            f"fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB);"
                            f"time.sleep(1);"
                            f'fcntl.flock(f,fcntl.LOCK_UN)"'
                        )
                        client.exec_command(
                            sudo=True,
                            cmd=lock_cmd,
                            check_ec=False,
                            timeout=15,
                        )
                        ops["locks"] += 1
                except Exception:
                    ops["errors"] += 1
            _sleep_with_stop(stop_event, 10, step=2)
        return ops

    def start_churn_threads(self, assignments, stop_event, churn_rate=5):
        """Start per-mount churn threads.  Returns (executor, futures).

        Caller must call executor.shutdown(wait=True) after setting stop_event.
        Do NOT use this with a ``with`` statement -- the executor must stay
        alive until the caller is ready to join.
        """
        executor = ThreadPoolExecutor(max_workers=min(len(assignments), 5))
        futures = []
        for a in assignments:
            f = executor.submit(
                self.run_churn_on_mount,
                a["client"],
                a["mount_point"],
                stop_event,
                churn_rate,
            )
            futures.append(f)
        return executor, futures

    # ------------------------------------------------------------------
    #  Admin Operations
    # ------------------------------------------------------------------

    def run_admin_ops(self, nfs_name, fs_name, duration, stop_event):
        """Run random NFS admin operations for ``duration`` seconds.

        Randomly selects one of three operations each cycle:
        - export_create: create a temporary export, then delete it.
        - export_delete: no-op (avoids deleting test exports).
        - restart: ``ceph orch restart nfs.<cluster>``, then wait 30s.

        Args:
            nfs_name: NFS cluster name.
            fs_name: CephFS filesystem name for new exports.
            duration: Total runtime in seconds.
            stop_event: threading.Event for early termination.

        Returns:
            dict: Operation counts {creates, deletes, restarts, errors}.
        """
        from cli.ceph.ceph import Ceph

        ops = {"creates": 0, "deletes": 0, "restarts": 0, "errors": 0}
        end_time = time.time() + duration
        churn_idx = 100

        while time.time() < end_time and not stop_event.is_set():
            op = random.choice(["export_create", "export_delete", "restart"])
            try:
                if op == "export_create":
                    pseudo = f"/churn_admin_{churn_idx}"
                    churn_idx += 1
                    Ceph(self.installer).nfs.export.create(
                        fs_name=fs_name,
                        nfs_name=nfs_name,
                        nfs_export=pseudo,
                        fs=fs_name,
                    )
                    ops["creates"] += 1
                    _sleep_with_stop(stop_event, 3, step=1)
                    Ceph(self.installer).nfs.export.delete(nfs_name, pseudo)
                    ops["deletes"] += 1

                elif op == "export_delete":
                    pass  # no-op to avoid deleting test exports

                elif op == "restart":
                    self.installer.exec_command(
                        sudo=True,
                        cmd=f"ceph orch restart nfs.{nfs_name}",
                        timeout=60,
                    )
                    ops["restarts"] += 1
                    _sleep_with_stop(stop_event, 30, step=5)

            except Exception as e:
                log.debug("[admin_ops] %s failed: %s", op, e)
                ops["errors"] += 1

            _sleep_with_stop(stop_event, 10, step=3)

        log.info("[admin_ops] Summary: %s", ops)
        return ops

    # ------------------------------------------------------------------
    #  FIO on Mounts
    # ------------------------------------------------------------------

    def run_fio_on_assignments(self, assignments, fio_params, stop_event=None):
        """Run fio sequentially on each mount assignment.

        Executes ``fio --rw=randrw`` with the given block size, file size,
        and runtime on each mount point.  Stops early if ``stop_event``
        is set.

        Args:
            assignments: List of mount assignment dicts.
            fio_params: dict with keys bs, size, runtime.
            stop_event: Optional threading.Event for early stop.

        Returns:
            bool: True if all fio runs succeeded.
        """
        all_ok = True
        for a in assignments:
            if stop_event and stop_event.is_set():
                break
            mp = a["mount_point"]
            client = a["client"]
            bs = fio_params.get("bs", "1M")
            size = fio_params.get("size", "128M")
            runtime = fio_params.get("runtime", 30)
            try:
                client.exec_command(
                    sudo=True,
                    cmd=(
                        f"fio --name=lifecycle_io --directory={mp} "
                        f"--size={size} --bs={bs} --rw=randrw "
                        f"--numjobs=2 --time_based --runtime={runtime} "
                        f"--ioengine=libaio --direct=1 --group_reporting"
                    ),
                    timeout=runtime + 120,
                )
            except Exception as e:
                log.warning("[fio] Failed on %s:%s: %s", client.hostname, mp, e)
                all_ok = False
        return all_ok
