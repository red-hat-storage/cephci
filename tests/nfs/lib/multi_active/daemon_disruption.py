"""Helpers for NFS multi-active per-daemon restart and stop/start."""

from datetime import datetime

from ceph.waiter import WaitUntil
from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.lib.multi_active.validation import NfsMultiActiveValidation
from utility.log import Log

log = Log(__name__)


class NfsMultiActiveDaemonDisruption:
    @staticmethod
    def _service_name(nfs_service_id, component):
        if component == "nfs":
            return f"nfs.{nfs_service_id}"
        return f"ingress.nfs.{nfs_service_id}"

    @staticmethod
    def _daemon_name(daemon):
        name = daemon.get("daemon_name") or daemon.get("name")
        if name:
            return name
        raise OperationFailedError(f"No daemon name in orch ps record: {daemon!r}")

    @staticmethod
    def _parse_orch_timestamp(value):
        if not value:
            return None
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))

    @staticmethod
    def _find_daemon_record(
        validator, nfs_service_id, hostname, component, daemon_name=None
    ):
        service = NfsMultiActiveDaemonDisruption._service_name(
            nfs_service_id, component
        )
        for daemon in validator.get_orch_ps_daemons(service):
            if daemon.get("hostname") != hostname:
                continue
            if NfsMultiActiveValidation.daemon_component(daemon) != component:
                continue
            name = daemon.get("daemon_name") or daemon.get("name")
            if daemon_name and name != daemon_name:
                continue
            if name:
                return daemon
        raise OperationFailedError(
            f"No {component!r} daemon on {hostname!r} for {service}"
        )

    @staticmethod
    def stop(client, validator, nfs_service_id, hostname, component):
        daemon_name = NfsMultiActiveDaemonDisruption._daemon_name(
            NfsMultiActiveDaemonDisruption._find_daemon_record(
                validator, nfs_service_id, hostname, component
            )
        )
        log.info(
            "Daemon disruption: stop %s on %s (%s)",
            component,
            hostname,
            daemon_name,
        )
        client.exec_command(
            sudo=True, cmd=f"ceph orch daemon stop {daemon_name} --force"
        )
        return daemon_name

    @staticmethod
    def start(client, daemon_name, hostname, component):
        log.info(
            "Daemon disruption: start %s on %s (%s)",
            component,
            hostname,
            daemon_name,
        )
        client.exec_command(
            sudo=True, cmd=f"ceph orch daemon start {daemon_name} --force"
        )

    @staticmethod
    def _is_daemon_stopped(daemon):
        status_desc = str(daemon.get("status_desc", "")).strip().lower()
        if status_desc == "stopped":
            return True
        if daemon.get("status") == 0 and status_desc != "running":
            return True
        return False

    @staticmethod
    def wait_stopped(
        validator,
        nfs_service_id,
        hostname,
        component,
        daemon_name=None,
        timeout=300,
    ):
        """Wait until orch ps reports the target daemon as stopped."""
        last_status = None
        last_user_stopped = None
        for _ in WaitUntil(timeout=timeout, interval=10):
            daemon = NfsMultiActiveDaemonDisruption._find_daemon_record(
                validator,
                nfs_service_id,
                hostname,
                component,
                daemon_name=daemon_name,
            )
            name = NfsMultiActiveDaemonDisruption._daemon_name(daemon)
            last_status = daemon.get("status_desc")
            last_user_stopped = daemon.get("user_stopped")
            if NfsMultiActiveDaemonDisruption._is_daemon_stopped(daemon):
                log.info(
                    "%s daemon stopped on %s (%s, status_desc=%s, user_stopped=%s)",
                    component,
                    hostname,
                    name,
                    last_status,
                    last_user_stopped,
                )
                return daemon
        raise OperationFailedError(
            f"Timed out after {timeout}s waiting for {component!r} on "
            f"{hostname!r} to reach stopped (last status={last_status!r}, "
            f"user_stopped={last_user_stopped!r})"
        )

    @staticmethod
    def wait_running(
        validator,
        nfs_service_id,
        hostname,
        component,
        daemon_name=None,
        timeout=300,
    ):
        last_status = None
        for _ in WaitUntil(timeout=timeout, interval=10):
            daemon = NfsMultiActiveDaemonDisruption._find_daemon_record(
                validator,
                nfs_service_id,
                hostname,
                component,
                daemon_name=daemon_name,
            )
            name = NfsMultiActiveDaemonDisruption._daemon_name(daemon)
            last_status = daemon.get("status_desc")
            if str(last_status or "").strip().lower() == "running":
                log.info(
                    "%s daemon running on %s (%s)",
                    component,
                    hostname,
                    name,
                )
                return daemon
        raise OperationFailedError(
            f"Timed out after {timeout}s waiting for {component!r} on "
            f"{hostname!r} to reach running (last status={last_status!r})"
        )

    @staticmethod
    def wait_restarted(
        validator,
        nfs_service_id,
        hostname,
        component,
        daemon_name,
        started_before,
        timeout=300,
    ):
        """Wait until orch ps shows a newer ``started`` time than before restart."""
        started_before_ts = NfsMultiActiveDaemonDisruption._parse_orch_timestamp(
            started_before
        )
        if started_before_ts is None:
            raise ConfigError(
                f"No orch ps started timestamp before restart for {daemon_name!r}"
            )

        last_status = None
        last_started = started_before
        for _ in WaitUntil(timeout=timeout, interval=10):
            daemon = NfsMultiActiveDaemonDisruption._find_daemon_record(
                validator,
                nfs_service_id,
                hostname,
                component,
                daemon_name=daemon_name,
            )
            last_status = daemon.get("status_desc")
            last_started = daemon.get("started")
            started_ts = NfsMultiActiveDaemonDisruption._parse_orch_timestamp(
                last_started
            )
            if str(last_status or "").strip().lower() != "running":
                continue
            if started_ts and started_ts > started_before_ts:
                log.info(
                    "%s daemon restarted on %s (%s); started %s -> %s",
                    component,
                    hostname,
                    daemon_name,
                    started_before,
                    last_started,
                )
                return daemon

        raise OperationFailedError(
            f"Timed out after {timeout}s waiting for {component!r} on "
            f"{hostname!r} to restart (last status={last_status!r}, "
            f"started={last_started!r}, started_before={started_before!r})"
        )

    @staticmethod
    def apply(client, validator, nfs_service_id, hostname, component, action):
        if action != "restart":
            raise ConfigError(f"Unsupported daemon action {action!r}")

        daemon = NfsMultiActiveDaemonDisruption._find_daemon_record(
            validator, nfs_service_id, hostname, component
        )
        daemon_name = NfsMultiActiveDaemonDisruption._daemon_name(daemon)
        started_before = daemon.get("started")
        log.info(
            "Daemon disruption: %s %s on %s (%s, started=%s)",
            action,
            component,
            hostname,
            daemon_name,
            started_before,
        )
        client.exec_command(
            sudo=True, cmd=f"ceph orch daemon restart {daemon_name} --force"
        )
        return daemon_name, started_before
