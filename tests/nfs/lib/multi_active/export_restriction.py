"""IP-based NFS export restrictions with HAProxy protocol in multi-active HA."""

from datetime import datetime
from time import sleep

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.lib.multi_active.cleanup import NfsMultiActiveCleanup
from tests.nfs.lib.multi_active.client import NfsMultiActiveClient
from tests.nfs.lib.multi_active.config import NfsMultiActiveConfig
from tests.nfs.lib.multi_active.constants import (
    DEPLOY_TIMEOUT,
    FS_NAME,
    HEALTH_CHECK_INTERVAL,
    IO_JOIN_TIMEOUT,
    IO_RESUME_TIMEOUT,
    IO_WARMUP_SEC,
    NFS_VERSION,
    log_section,
)
from tests.nfs.lib.multi_active.deploy import NfsMultiActiveDeploy
from tests.nfs.lib.multi_active.failover import NfsMultiActiveFailover
from tests.nfs.lib.multi_active.haproxy import NfsMultiActiveHaproxy
from tests.nfs.lib.multi_active.placement import (
    NEGATIVE_PLACEMENT,
    resolve_placement_hosts,
)
from utility.log import Log

log = Log(__name__)


_MOUNT_DENIED_MARKERS = (
    "no such file or directory",
    "access denied",
    "permission denied",
    "operation not permitted",
    "not permitted",
    "mount.nfs:",
)

NEG4_EXPORT_NAME = "/export_ip_restrict"
NEG4_AUTH_MOUNT = "/mnt/nfs_neg_auth"
NEG4_UNAUTH_MOUNT = "/mnt/nfs_neg_unauth"
NEG4_IO_SUBDIR = "neg4_ip_restrict_io"


class NegExportClusterSession:
    """Cluster state shared between NEG-4 (setup/failover) and NEG-5 (Ganesha crash)."""

    def __init__(self):
        self.ceph_cluster = None
        self.config = None
        self.nfs_name = None
        self.vip = None
        self.frontend_port = None
        self.authorized_client = None
        self.unauthorized_client = None
        self.nfs_nodes = None
        self.labels = None
        self.deploy = None
        self.haproxy = None
        self.failover = None
        self.deploy_timeout = DEPLOY_TIMEOUT
        self.nfs_spec = None
        self.ingress_spec = None
        self.deploy_nodes = None
        self.spare_host = None
        self.ingress_hosts = None
        self.nfs_count = None
        self.cluster_deployed = False
        self.export_created = False
        self.labels_applied = False
        self.auth_mounted = False
        self.io_sessions = []
        self.timings = {}

    def cleanup(self):
        if self.io_sessions:
            try:
                NfsMultiActiveExportRestriction.stop_background_io(
                    self.io_sessions, NEG4_AUTH_MOUNT, self.config
                )
            except Exception as exc:
                log.warning("IO session cleanup failed: %s", exc)
            self.io_sessions = []
        if self.auth_mounted and self.authorized_client:
            try:
                NfsMultiActiveExportRestriction.umount_authorized(
                    self.authorized_client, NEG4_AUTH_MOUNT
                )
            except Exception as exc:
                log.warning("Authorized client umount failed: %s", exc)
        for client, mount_point in (
            (self.authorized_client, NEG4_AUTH_MOUNT),
            (self.unauthorized_client, NEG4_UNAUTH_MOUNT),
        ):
            if client is None:
                continue
            try:
                client.exec_command(
                    sudo=True,
                    cmd=f"rm -rf {mount_point}",
                    check_ec=False,
                )
            except Exception as exc:
                log.warning(
                    "Cleanup mount dir %s on %s failed: %s",
                    mount_point,
                    client.hostname,
                    exc,
                )
        if self.export_created and self.authorized_client and self.nfs_name:
            try:
                Ceph(self.authorized_client).nfs.export.delete(
                    self.nfs_name, NEG4_EXPORT_NAME
                )
            except Exception as exc:
                log.warning("Export delete failed: %s", exc)
        if self.cluster_deployed and self.ceph_cluster and self.nfs_name:
            try:
                NfsMultiActiveCleanup(self.ceph_cluster).remove_cluster(self.nfs_name)
            except Exception as exc:
                log.warning("Cluster cleanup failed: %s", exc)
        if (
            self.labels_applied
            and self.authorized_client
            and self.nfs_nodes
            and self.labels
        ):
            try:
                NfsMultiActiveConfig.clear_placement_labels(
                    self.authorized_client,
                    self.nfs_nodes,
                    self.labels,
                )
            except Exception as exc:
                log.warning("Placement label cleanup failed: %s", exc)


def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class NfsMultiActiveExportRestriction:
    """Validate IP-based export client restrictions over ingress VIP + HAProxy protocol."""

    @staticmethod
    def assert_haproxy_protocol_enabled(validator, nfs_spec, ingress_spec):
        cluster_info = validator.get_cluster_info(nfs_spec["service_id"])
        validator.assert_cluster_info(cluster_info, nfs_spec, ingress_spec)
        mode = cluster_info.get("ingress_mode")
        if mode != "haproxy-protocol":
            raise OperationFailedError(
                f"Expected ingress mode haproxy-protocol, got {mode!r}"
            )
        log.info("Ingress mode confirmed: haproxy-protocol")

    @staticmethod
    def assert_export_restricts_client_ip(client, nfs_name, export_name, client_ip):
        raw = Ceph(client).nfs.export.get(nfs_name, export_name)
        if client_ip not in (raw or ""):
            raise OperationFailedError(
                f"Export {export_name!r} does not list restricted client IP "
                f"{client_ip!r}:\n{raw}"
            )
        log.info(
            "Export %s restricts client IP %s as expected",
            export_name,
            client_ip,
        )

    @staticmethod
    def mount_authorized_via_vip(
        client, vip, mount_point, export_name, port, version=NFS_VERSION
    ):
        NfsMultiActiveClient.mount_via_vip(
            client,
            vip,
            mount_point,
            export_name,
            str(port),
            version,
        )
        log.info(
            "Authorized client %s mounted %s via VIP %s",
            client.hostname,
            export_name,
            vip,
        )

    @staticmethod
    def assert_mount_via_vip_denied(
        client, vip, mount_point, export_name, port, version=NFS_VERSION
    ):
        mount_server = vip.split("/")[0]
        client.create_dirs(dir_path=mount_point, sudo=True)
        cmd = (
            f"mount -t nfs -o vers={version},port={port} "
            f"{mount_server}:{export_name} {mount_point}"
        )
        out, err = client.exec_command(cmd=cmd, sudo=True, check_ec=False)
        combined = f"{out or ''}\n{err or ''}"
        mp_out, mp_err = client.exec_command(
            sudo=True,
            cmd=f"mountpoint {mount_point}",
            check_ec=False,
        )
        if "is a mountpoint" in f"{mp_out or ''}{mp_err or ''}".lower():
            raise OperationFailedError(
                f"Unauthorized client {client.hostname} mounted "
                f"{export_name} via VIP {vip}"
            )
        if not any(marker in combined.lower() for marker in _MOUNT_DENIED_MARKERS):
            raise OperationFailedError(
                f"Expected unauthorized mount denial on {client.hostname}, got:\n"
                f"{combined}"
            )
        log.info(
            "Unauthorized client %s denied mount to %s via VIP %s",
            client.hostname,
            export_name,
            vip,
        )

    @staticmethod
    def run_sync_io(authorized_client, mount_point, phase_label):
        log_section(log, f"NEG-4 SYNC IO — {phase_label}")
        NfsMultiActiveClient.run_basic_io(
            authorized_client,
            authorized_client,
            mount_point,
            file_count=2,
            io_subdir=f"neg4_sync_{phase_label.replace(' ', '_')}",
        )
        log.info("Sync IO completed for %s", phase_label)

    @staticmethod
    def start_background_io(authorized_client, mount_point, config):
        runtime = NfsMultiActiveConfig.io_runtime(config)
        thread, error_box, io_dir = NfsMultiActiveClient.run_background_io(
            authorized_client,
            mount_point,
            config,
            io_subdir=NEG4_IO_SUBDIR,
            runtime=runtime,
        )
        warmup = int(config.get("neg4_io_start_delay", IO_WARMUP_SEC))
        if warmup > 0:
            log.info("Background IO warmup %ss before failover", warmup)
            sleep(warmup)
        if error_box.get("exc"):
            raise OperationFailedError(
                f"Background IO failed during warmup on "
                f"{authorized_client.hostname}: {error_box['exc']}"
            )
        if not thread.is_alive():
            raise OperationFailedError(
                f"Background IO stopped during warmup on {authorized_client.hostname}"
            )
        return [(authorized_client, thread, error_box, io_dir)]

    @staticmethod
    def wait_io_resume(io_sessions, mount_point, config, timings, trigger_key, stage):
        if not io_sessions:
            raise OperationFailedError(f"No IO sessions to resume-check for {stage}")
        active = [session for session in io_sessions if session[1].is_alive()]
        if not active:
            raise OperationFailedError(
                f"Background IO already stopped before {stage} resume check"
            )
        log_section(log, f"NEG-4 WAIT IO RESUME — {stage}")
        NfsMultiActiveClient.wait_for_background_io_resume(
            active,
            mount_point,
            config,
            timeout=int(config.get("io_resume_timeout", IO_RESUME_TIMEOUT)),
            timings=timings,
            triggered_at=timings.get(trigger_key),
        )
        log.info("Background IO resumed after %s", stage)

    @staticmethod
    def stop_background_io(io_sessions, mount_point, config):
        for client, thread, error_box, io_dir in io_sessions:
            if thread.is_alive():
                NfsMultiActiveClient.stop_background_io(
                    client, io_dir, config, error_box=error_box
                )
            thread.join(timeout=IO_JOIN_TIMEOUT)
            if thread.is_alive():
                raise OperationFailedError(
                    f"Background IO on {client.hostname} did not stop within "
                    f"{IO_JOIN_TIMEOUT}s"
                )
            if error_box.get("exc") and not error_box.get("stopped"):
                raise OperationFailedError(
                    f"Background IO failed on {client.hostname}: {error_box['exc']}"
                )
        log.info("Background IO stopped on %s", mount_point)

    @staticmethod
    def umount_authorized(client, mount_point):
        err = NfsMultiActiveClient._umount_lazy_then_force(client, mount_point)
        if err:
            raise OperationFailedError(
                f"Failed to unmount {mount_point} on {client.hostname}: {err.strip()}"
            )
        log.info("Unmounted %s on %s", mount_point, client.hostname)

    @classmethod
    def verify_ip_restrictions(
        cls,
        nfs_name,
        vip,
        export_name,
        frontend_port,
        authorized_client,
        unauthorized_client,
        phase_label,
        *,
        mount_authorized=True,
    ):
        auth_ip = NfsMultiActiveClient.client_mount_ip(authorized_client)
        log_section(log, f"NEG-4 IP RESTRICTION CHECK — {phase_label}")
        log.info(
            "%s: authorized=%s (%s) unauthorized=%s (%s)",
            phase_label,
            authorized_client.hostname,
            auth_ip,
            unauthorized_client.hostname,
            NfsMultiActiveClient.client_mount_ip(unauthorized_client),
        )
        cls.assert_export_restricts_client_ip(
            authorized_client, nfs_name, export_name, auth_ip
        )
        cls.assert_mount_via_vip_denied(
            unauthorized_client,
            vip,
            NEG4_UNAUTH_MOUNT,
            export_name,
            frontend_port,
        )
        if mount_authorized:
            cls.mount_authorized_via_vip(
                authorized_client,
                vip,
                NEG4_AUTH_MOUNT,
                export_name,
                frontend_port,
            )

    @classmethod
    def remount_run_io_and_verify_restrictions(
        cls,
        nfs_name,
        vip,
        export_name,
        frontend_port,
        authorized_client,
        unauthorized_client,
        config,
        phase_label,
    ):
        cls.umount_authorized(authorized_client, NEG4_AUTH_MOUNT)
        cls.verify_ip_restrictions(
            nfs_name,
            vip,
            export_name,
            frontend_port,
            authorized_client,
            unauthorized_client,
            phase_label,
            mount_authorized=True,
        )
        cls.run_sync_io(authorized_client, NEG4_AUTH_MOUNT, phase_label)
        return cls.start_background_io(authorized_client, NEG4_AUTH_MOUNT, config)

    @classmethod
    def _failover_ingress_with_io(
        cls,
        failover,
        authorized_client,
        deploy,
        ingress_spec,
        nfs_name,
        deploy_nodes,
        vip_holder,
        spare_host,
        ingress_label,
        ingress_hosts,
        config,
        io_sessions,
        timings,
        deploy_timeout,
        case_id,
    ):
        log_section(log, f"{case_id} INGRESS FAILOVER WITH BACKGROUND IO")
        timings["ingress_failover_triggered_at"] = _now()

        def _wait_io_after_ingress():
            cls.wait_io_resume(
                io_sessions,
                NEG4_AUTH_MOUNT,
                config,
                timings,
                "ingress_failover_triggered_at",
                "ingress failover",
            )

        failover.failover_ingress_by_label(
            authorized_client,
            deploy,
            ingress_spec,
            nfs_name,
            deploy_nodes,
            vip_holder,
            spare_host,
            ingress_label,
            expected_ingress_count=len(ingress_hosts),
            deploy_timeout=deploy_timeout,
            timings=timings,
            timings_trigger_key="ingress_failover_triggered_at",
            post_apply_callback=_wait_io_after_ingress,
        )
        new_vip_holder = failover.get_active_ingress_host(
            deploy.ceph_cluster, deploy.vip, ingress_hosts
        )
        if new_vip_holder == vip_holder:
            raise OperationFailedError(
                f"Ingress failover did not move VIP {deploy.vip!r} off {vip_holder!r}"
            )
        log.info(
            "Ingress VIP %s moved from %s to %s",
            deploy.vip,
            vip_holder,
            new_vip_holder,
        )

    @classmethod
    def _failover_nfs_with_io(
        cls,
        failover,
        haproxy,
        authorized_client,
        deploy,
        nfs_spec,
        deploy_nodes,
        spare_host,
        nfs_label,
        nfs_count,
        config,
        io_sessions,
        timings,
        deploy_timeout,
        case_id,
    ):
        pinned_nfs = haproxy.get_client_backend_hostname(authorized_client)
        log_section(log, f"{case_id} NFS FAILOVER WITH BACKGROUND IO")
        log.info(
            "NFS failover target from stick table: drain %s -> %s",
            pinned_nfs,
            spare_host,
        )
        timings["nfs_failover_triggered_at"] = _now()

        def _wait_io_after_nfs():
            cls.wait_io_resume(
                io_sessions,
                NEG4_AUTH_MOUNT,
                config,
                timings,
                "nfs_failover_triggered_at",
                "NFS failover",
            )

        failover.failover_nfs_by_label(
            authorized_client,
            deploy,
            nfs_spec,
            deploy_nodes,
            pinned_nfs,
            spare_host,
            nfs_label,
            nfs_count,
            deploy_timeout=deploy_timeout,
            timings=timings,
            timings_trigger_key="nfs_failover_triggered_at",
            post_apply_callback=_wait_io_after_nfs,
        )

    @classmethod
    def _crash_ganesha_with_io(
        cls,
        failover,
        haproxy,
        authorized_client,
        deploy,
        nfs_name,
        config,
        io_sessions,
        timings,
        deploy_timeout,
        case_id,
    ):
        pinned_nfs = haproxy.get_client_backend_hostname(authorized_client)
        nfs_host_node = failover._node_by_hostname(deploy.ceph_cluster, pinned_nfs)
        ganesha_pid = failover.get_ganesha_host_pid(
            deploy.validator,
            nfs_name,
            pinned_nfs,
            nfs_host_node,
        )
        log_section(log, f"{case_id} GANESHA CRASH (kill -9) WITH BACKGROUND IO")
        log.info(
            "Crashing ganesha on pinned NFS backend %s (PID %s)",
            pinned_nfs,
            ganesha_pid,
        )
        timings["ganesha_crash_triggered_at"] = _now()
        failover.crash_ganesha_process(nfs_host_node, ganesha_pid, pinned_nfs)
        cls.wait_io_resume(
            io_sessions,
            NEG4_AUTH_MOUNT,
            config,
            timings,
            "ganesha_crash_triggered_at",
            "Ganesha crash",
        )
        failover.wait_nfs_daemon_on_host(
            deploy.validator,
            nfs_name,
            pinned_nfs,
            timeout=deploy_timeout,
            timings=timings,
        )
        new_pid = failover.get_ganesha_host_pid(
            deploy.validator,
            nfs_name,
            pinned_nfs,
            nfs_host_node,
        )
        if new_pid == ganesha_pid:
            raise OperationFailedError(
                f"Ganesha PID on {pinned_nfs!r} unchanged after SIGKILL ({ganesha_pid})"
            )
        log.info(
            "Ganesha restarted on %s (old PID %s, new PID %s)",
            pinned_nfs,
            ganesha_pid,
            new_pid,
        )

    @classmethod
    def bootstrap_neg_export_session(
        cls, ceph_cluster, config, case_id, workflow_key=None
    ):
        """Deploy HA cluster, IP-restricted export, mount, and start background IO."""
        session = NegExportClusterSession()
        session.ceph_cluster = ceph_cluster
        session.config = config
        clients = ceph_cluster.get_nodes("client")
        nfs_nodes = NfsMultiActiveConfig.sorted_nfs_nodes(ceph_cluster)
        if len(clients) < 2:
            raise ConfigError(
                f"{case_id} requires at least 2 client nodes for IP restriction validation"
            )
        placement = NEGATIVE_PLACEMENT
        (
            nfs_hosts,
            ingress_hosts,
            deploy_nodes,
            nfs_count,
            labels,
            spare_host,
        ) = resolve_placement_hosts(nfs_nodes, placement, config)

        authorized_client, unauthorized_client = clients[0], clients[1]
        session.authorized_client = authorized_client
        session.unauthorized_client = unauthorized_client
        session.nfs_nodes = nfs_nodes
        session.spare_host = spare_host
        session.ingress_hosts = ingress_hosts
        session.deploy_nodes = deploy_nodes
        session.labels = labels
        session.nfs_count = nfs_count
        nfs_slice = placement["nfs"]
        ingress_slice = placement["ingress"]
        log.info(
            "%s placement: nfs=%s ingress=%s spare=%s",
            case_id,
            nfs_hosts,
            ingress_hosts,
            spare_host,
        )
        workflow_key = workflow_key or config.get(
            "neg4_workflow_key", "neg4_ip_export_restriction"
        )
        frontend_port = int(config.get("neg4_frontend_port", 17801))
        monitor_port = int(config.get("neg4_monitor_port", 17901))
        backend_port = int(config.get("neg4_backend_port", 17781))
        deploy_timeout = int(config.get("deploy_timeout", DEPLOY_TIMEOUT))
        session.deploy_timeout = deploy_timeout
        vip = NfsMultiActiveConfig.get_vips(config)[0]
        session.vip = vip
        session.frontend_port = frontend_port
        nfs_name = (
            f"cephfs-nfs-neg-{workflow_key}-"
            f"{datetime.now().strftime('%Y%m%d%H%M%S')}"
        )
        session.nfs_name = nfs_name
        auth_ip = NfsMultiActiveClient.client_mount_ip(authorized_client)

        nfs_spec = NfsMultiActiveConfig.build_nfs_spec(
            nfs_hosts, nfs_count, nfs_name, backend_port, label=labels["nfs"]
        )
        ingress_spec = NfsMultiActiveConfig.build_ingress_spec(
            vip,
            nfs_name,
            frontend_port,
            monitor_port,
            config.get("health_check_interval", HEALTH_CHECK_INTERVAL),
            label=labels["ingress"],
        )
        session.nfs_spec = nfs_spec
        session.ingress_spec = ingress_spec
        deploy = NfsMultiActiveDeploy(
            ceph_cluster, nfs_name, vip, client=authorized_client
        )
        session.deploy = deploy
        session.failover = NfsMultiActiveFailover()

        log_section(log, f"{case_id} DEPLOY NFS MULTI-ACTIVE FOR IP RESTRICTIONS")
        NfsMultiActiveConfig.clear_placement_labels(
            authorized_client,
            nfs_nodes,
            labels,
        )
        NfsMultiActiveConfig.apply_placement_labels(
            authorized_client,
            nfs_nodes,
            nfs_slice,
            ingress_slice,
            labels,
        )
        session.labels_applied = True
        deploy.deploy(
            [nfs_spec, ingress_spec],
            deploy_nodes,
            deploy_timeout=deploy_timeout,
            expected_ingress_hosts=ingress_hosts,
            skip_mgr_enable=False,
        )
        session.cluster_deployed = True
        session.haproxy = NfsMultiActiveHaproxy(
            ceph_cluster, nfs_name, info_client=authorized_client
        )
        cls.assert_haproxy_protocol_enabled(deploy.validator, nfs_spec, ingress_spec)

        Ceph(authorized_client).nfs.export.create(
            fs_name=FS_NAME,
            nfs_name=nfs_name,
            nfs_export=NEG4_EXPORT_NAME,
            fs=FS_NAME,
            client_addr=auth_ip,
        )
        NfsMultiActiveConfig.wait_until_export_visible(
            authorized_client,
            nfs_name,
            NEG4_EXPORT_NAME,
        )
        session.export_created = True

        cls.verify_ip_restrictions(
            nfs_name,
            vip,
            NEG4_EXPORT_NAME,
            frontend_port,
            authorized_client,
            unauthorized_client,
            "initial mount",
            mount_authorized=True,
        )
        session.auth_mounted = True
        cls.run_sync_io(authorized_client, NEG4_AUTH_MOUNT, "initial mount")
        session.io_sessions = cls.start_background_io(
            authorized_client, NEG4_AUTH_MOUNT, config
        )
        return session

    @classmethod
    def run_neg4_ip_export_restriction_haproxy_failover(
        cls, ceph_cluster, config, case_id
    ):
        """Deploy HA ingress, verify IP restrictions, ingress/NFS failover; leave session."""
        session = cls.bootstrap_neg_export_session(ceph_cluster, config, case_id)
        failover = session.failover
        deploy = session.deploy
        timings = session.timings
        authorized_client = session.authorized_client
        unauthorized_client = session.unauthorized_client
        nfs_name = session.nfs_name
        vip = session.vip
        frontend_port = session.frontend_port
        nfs_spec = session.nfs_spec
        ingress_spec = session.ingress_spec
        deploy_nodes = session.deploy_nodes
        spare_host = session.spare_host
        ingress_hosts = session.ingress_hosts
        labels = session.labels
        nfs_count = session.nfs_count
        deploy_timeout = session.deploy_timeout
        haproxy = session.haproxy
        io_sessions = session.io_sessions

        vip_holder = failover.get_active_ingress_host(ceph_cluster, vip, ingress_hosts)
        cls._failover_ingress_with_io(
            failover,
            authorized_client,
            deploy,
            ingress_spec,
            nfs_name,
            deploy_nodes,
            vip_holder,
            spare_host,
            labels["ingress"],
            ingress_hosts,
            config,
            io_sessions,
            timings,
            deploy_timeout,
            case_id,
        )
        cls.stop_background_io(io_sessions, NEG4_AUTH_MOUNT, config)
        io_sessions = cls.remount_run_io_and_verify_restrictions(
            nfs_name,
            vip,
            NEG4_EXPORT_NAME,
            frontend_port,
            authorized_client,
            unauthorized_client,
            config,
            "after ingress failover remount",
        )

        cls._failover_nfs_with_io(
            failover,
            haproxy,
            authorized_client,
            deploy,
            nfs_spec,
            deploy_nodes,
            spare_host,
            labels["nfs"],
            nfs_count,
            config,
            io_sessions,
            timings,
            deploy_timeout,
            case_id,
        )
        cls.stop_background_io(io_sessions, NEG4_AUTH_MOUNT, config)
        session.io_sessions = cls.remount_run_io_and_verify_restrictions(
            nfs_name,
            vip,
            NEG4_EXPORT_NAME,
            frontend_port,
            authorized_client,
            unauthorized_client,
            config,
            "after NFS failover remount",
        )

        log.info("%s passed; cluster left running for NEG-5", case_id)
        return session

    @classmethod
    def run_neg5_ganesha_crash_with_io(
        cls, ceph_cluster, config, case_id, session=None
    ):
        """SIGKILL ganesha on pinned backend; IO continues and service restarts."""
        if session is None:
            workflow_key = config.get("neg5_workflow_key", "neg5_ganesha_crash")
            session = cls.bootstrap_neg_export_session(
                ceph_cluster,
                config,
                case_id,
                workflow_key=workflow_key,
            )
        cls._require_neg_export_session(session)
        if not session.auth_mounted:
            raise OperationFailedError(
                "NEG-5 requires an authorized mount on the export cluster session"
            )
        io_sessions = session.io_sessions
        if not io_sessions:
            io_sessions = cls.start_background_io(
                session.authorized_client, NEG4_AUTH_MOUNT, session.config
            )
            session.io_sessions = io_sessions

        cls._crash_ganesha_with_io(
            session.failover,
            session.haproxy,
            session.authorized_client,
            session.deploy,
            session.nfs_name,
            session.config,
            io_sessions,
            session.timings,
            session.deploy_timeout,
            case_id,
        )
        cls.stop_background_io(io_sessions, NEG4_AUTH_MOUNT, session.config)
        session.io_sessions = []
        session.io_sessions = cls.remount_run_io_and_verify_restrictions(
            session.nfs_name,
            session.vip,
            NEG4_EXPORT_NAME,
            session.frontend_port,
            session.authorized_client,
            session.unauthorized_client,
            session.config,
            "after Ganesha crash remount",
        )
        cls.stop_background_io(session.io_sessions, NEG4_AUTH_MOUNT, session.config)
        session.io_sessions = []
        log.info("%s passed", case_id)
        return session

    @staticmethod
    def _require_neg_export_session(session):
        if session is None:
            raise ConfigError("NEG export cluster session is required")
        required = (
            "ceph_cluster",
            "nfs_name",
            "deploy",
            "haproxy",
            "failover",
            "authorized_client",
        )
        missing = [name for name in required if getattr(session, name, None) is None]
        if missing:
            raise ConfigError(
                f"NEG-4 session incomplete for NEG-5 (missing: {', '.join(missing)})"
            )
