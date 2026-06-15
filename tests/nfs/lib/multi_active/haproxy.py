"""HAProxy stick table validation for NFS Multi-Active ingress."""

import re
from time import monotonic, sleep

from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.lib.multi_active.client import NfsMultiActiveClient
from tests.nfs.lib.multi_active.constants import log_section
from tests.nfs.lib.multi_active.validation import NfsMultiActiveValidation
from utility.log import Log

log = Log(__name__)

STICK_TABLE_TIMEOUT = 120
STICK_TABLE_INTERVAL = 5


class NfsMultiActiveHaproxy:
    """Validate HAProxy backend stick table state."""

    def __init__(self, ceph_cluster, nfs_name, info_client=None):
        self.ceph_cluster = ceph_cluster
        self.nfs_name = nfs_name
        self.info_client = info_client
        if info_client is None:
            clients = ceph_cluster.get_nodes("client")
            info_client = (
                clients[0] if clients else ceph_cluster.get_nodes("installer")[0]
            )
        self.validator = NfsMultiActiveValidation(ceph_cluster, info_client)

    def validate_entries(
        self,
        mounted_clients=None,
        info_client=None,
        stick_table_timeout=STICK_TABLE_TIMEOUT,
        stick_table_interval=STICK_TABLE_INTERVAL,
        timings=None,
    ):
        """Validate stick table is empty or matches mounted clients (poll up to timeout)."""
        mounted_clients = mounted_clients or []
        info_client = info_client or self.info_client
        haproxy_daemons = self._running_haproxy_daemons()

        if not mounted_clients:
            log_section(
                log,
                f"HAPROXY STICK TABLE — empty ({len(haproxy_daemons)} ingress nodes)",
            )
            self._wait_for_stick_table_state(
                haproxy_daemons,
                expected_used=0,
                expected_client_ips=set(),
                info_client=info_client,
                timeout=stick_table_timeout,
                interval=stick_table_interval,
            )
            return

        if info_client is None:
            raise ConfigError(
                "info_client is required when validating stick table entries"
            )

        log_section(
            log,
            f"HAPROXY STICK TABLE — {len(mounted_clients)} client(s): "
            f"{', '.join(c.hostname for c in mounted_clients)}",
        )
        expected_client_ips = {
            NfsMultiActiveClient.client_mount_ip(client) for client in mounted_clients
        }
        self._wait_for_stick_table_state(
            haproxy_daemons,
            expected_used=len(expected_client_ips),
            expected_client_ips=expected_client_ips,
            info_client=info_client,
            timeout=stick_table_timeout,
            interval=stick_table_interval,
            mounted_clients=mounted_clients,
        )
        if timings is not None:
            timings["stick_table"] = f"ready within {stick_table_timeout}s poll"

    def _wait_for_stick_table_state(
        self,
        haproxy_daemons,
        expected_used,
        expected_client_ips,
        info_client,
        timeout,
        interval,
        mounted_clients=None,
    ):
        wait_start = monotonic()
        last_error = None
        while monotonic() - wait_start < timeout:
            try:
                if expected_used == 0:
                    self._validate_empty(haproxy_daemons, raise_on_fail=True)
                else:
                    self._validate_client_entries(
                        haproxy_daemons[0],
                        mounted_clients,
                        info_client,
                        raise_on_fail=True,
                    )
                elapsed = monotonic() - wait_start
                log.info("Stick table reached expected state after %.1fs", elapsed)
                return
            except OperationFailedError as exc:
                last_error = exc
                remaining = timeout - (monotonic() - wait_start)
                if remaining <= 0:
                    break
                log.info(
                    "Stick table not ready (expected used:%s); retrying (%.0fs remaining)",
                    expected_used,
                    remaining,
                )
                sleep(min(interval, remaining))
        if last_error is not None:
            raise last_error
        raise OperationFailedError(
            f"Timed out after {timeout}s waiting for stick table used:{expected_used}"
        )

    def _validate_empty(self, haproxy_daemons, raise_on_fail=True):
        for daemon in haproxy_daemons:
            hostname = daemon.get("hostname")
            daemon_name = daemon.get("daemon_name") or daemon.get("name")
            node = self._node_by_hostname(hostname)
            haproxy_dir = self._daemon_dir(node, daemon_name)

            log.info(
                "Checking empty stick table on %s (%s) in %s",
                hostname,
                daemon_name,
                haproxy_dir,
            )
            output = self._stick_table_output(node, haproxy_dir)
            log.info("Stick table output on %s:\n%s", hostname, output)

            used_count = self._parse_used_count(output)
            if used_count != 0:
                msg = (
                    f"Expected empty haproxy stick table on {hostname}, found "
                    f"used:{used_count}:\n{output}"
                )
                if raise_on_fail:
                    raise OperationFailedError(msg)
                log.warning(msg)
                return

            data_lines = [
                line
                for line in output.splitlines()
                if line.strip() and not line.strip().startswith("#")
            ]
            if data_lines:
                msg = f"Unexpected stick table entries on {hostname}:\n" + "\n".join(
                    data_lines
                )
                if raise_on_fail:
                    raise OperationFailedError(msg)
                log.warning(msg)
                return
            log.info("Stick table empty on %s (used:0)", hostname)

        log.info(
            "HAProxy stick table validation passed on %d node(s) (empty)",
            len(haproxy_daemons),
        )

    def _validate_client_entries(
        self, daemon, mounted_clients, info_client, raise_on_fail=True
    ):
        hostname = daemon.get("hostname")
        daemon_name = daemon.get("daemon_name") or daemon.get("name")
        node = self._node_by_hostname(hostname)
        haproxy_dir = self._daemon_dir(node, daemon_name)

        cluster_info = NfsMultiActiveValidation.fetch_cluster_info(
            info_client, self.nfs_name
        )
        allowed_backend_ips = NfsMultiActiveValidation.backend_ips(cluster_info)
        backend_map = self._backend_map(node, haproxy_dir)
        expected_client_ips = {
            NfsMultiActiveClient.client_mount_ip(client) for client in mounted_clients
        }

        log.info(
            "Checking stick table on %s (%s), expecting %d client entries",
            hostname,
            daemon_name,
            len(expected_client_ips),
        )

        output = self._stick_table_output(node, haproxy_dir)
        log.info("Stick table output on %s:\n%s", hostname, output)

        used_count = self._parse_used_count(output)
        if used_count != len(expected_client_ips):
            msg = (
                f"Expected stick table used:{len(expected_client_ips)}, "
                f"got used:{used_count}:\n{output}"
            )
            if raise_on_fail:
                raise OperationFailedError(msg)
            log.warning(msg)
            return

        entries = self._parse_entries(output)
        if len(entries) != len(expected_client_ips):
            msg = (
                f"Expected {len(expected_client_ips)} stick table entries, parsed "
                f"{len(entries)}:\n{output}"
            )
            if raise_on_fail:
                raise OperationFailedError(msg)
            log.warning(msg)
            return

        entry_keys = {entry["key"] for entry in entries}
        if entry_keys != expected_client_ips:
            msg = (
                f"Stick table client keys mismatch: expected {expected_client_ips}, "
                f"got {entry_keys}:\n{output}"
            )
            if raise_on_fail:
                raise OperationFailedError(msg)
            log.warning(msg)
            return

        for entry in entries:
            backend_ip = backend_map.get(entry["server_id"])
            if not backend_ip or backend_ip not in allowed_backend_ips:
                msg = (
                    f"Stick table entry {entry['raw']!r} maps to backend IP "
                    f"{backend_ip!r}, expected one of {allowed_backend_ips}"
                )
                if raise_on_fail:
                    raise OperationFailedError(msg)
                log.warning(msg)
                return
            log.info(
                "Client %s pinned to NFS backend %s (server_id=%s)",
                entry["key"],
                backend_ip,
                entry["server_id"],
            )

        log.info(
            "Stick table validation passed for %d client(s) on %s",
            len(mounted_clients),
            hostname,
        )

    def get_client_backend_ip(self, client, info_client=None):
        """Return NFS backend IP pinned for a mounted client in the stick table."""
        info_client = info_client or self.info_client
        client_ip = NfsMultiActiveClient.client_mount_ip(client)
        daemon = self._running_haproxy_daemons()[0]
        hostname = daemon.get("hostname")
        daemon_name = daemon.get("daemon_name") or daemon.get("name")
        node = self._node_by_hostname(hostname)
        haproxy_dir = self._daemon_dir(node, daemon_name)

        output = self._stick_table_output(node, haproxy_dir)
        entries = self._parse_entries(output)
        backend_map = self._backend_map(node, haproxy_dir)
        for entry in entries:
            if entry["key"] != client_ip:
                continue
            backend_ip = backend_map.get(entry["server_id"])
            if not backend_ip:
                raise OperationFailedError(
                    f"No backend IP for client {client.hostname} ({client_ip}) "
                    f"server_id={entry['server_id']}"
                )
            return backend_ip

        raise OperationFailedError(
            f"Client {client.hostname} ({client_ip}) not found in stick table "
            f"on {hostname}:\n{output}"
        )

    def get_client_backend_endpoint(self, client, info_client=None):
        """Return (hostname, port) for the NFS backend pinned to a mounted client."""
        info_client = info_client or self.info_client
        client_ip = NfsMultiActiveClient.client_mount_ip(client)
        daemon = self._running_haproxy_daemons()[0]
        hostname = daemon.get("hostname")
        daemon_name = daemon.get("daemon_name") or daemon.get("name")
        node = self._node_by_hostname(hostname)
        haproxy_dir = self._daemon_dir(node, daemon_name)

        output = self._stick_table_output(node, haproxy_dir)
        entries = self._parse_entries(output)
        endpoint_map = self._backend_endpoint_map(node, haproxy_dir)
        for entry in entries:
            if entry["key"] != client_ip:
                continue
            endpoint = endpoint_map.get(entry["server_id"])
            if not endpoint:
                raise OperationFailedError(
                    f"No backend endpoint for client {client.hostname} ({client_ip}) "
                    f"server_id={entry['server_id']}"
                )
            backend_ip, backend_port = endpoint
            cluster_info = NfsMultiActiveValidation.fetch_cluster_info(
                info_client, self.nfs_name
            )
            matches = [
                (backend.get("hostname"), int(backend["port"]))
                for backend in cluster_info.get("backend") or []
                if backend.get("ip") == backend_ip
                and backend.get("port") is not None
                and int(backend["port"]) == backend_port
            ]
            if len(matches) == 1:
                return matches[0]
            raise OperationFailedError(
                f"Could not map backend endpoint ({backend_ip!r}, {backend_port}) "
                f"for client {client.hostname}; backends="
                f"{cluster_info.get('backend')!r}"
            )

        raise OperationFailedError(
            f"Client {client.hostname} ({client_ip}) not found in stick table "
            f"on {hostname}:\n{output}"
        )

    def get_client_backend_hostname(self, client, info_client=None):
        """Return NFS backend hostname pinned for a mounted client."""
        return self.get_client_backend_endpoint(client, info_client=info_client)[0]

    def _running_haproxy_daemons(self):
        ingress_service = f"ingress.nfs.{self.nfs_name}"
        ingress_daemons = self.validator.get_orch_ps_daemons(ingress_service)
        haproxy_daemons = [
            daemon
            for daemon in NfsMultiActiveValidation.running_daemons(ingress_daemons)
            if NfsMultiActiveValidation.daemon_component(daemon) == "haproxy"
        ]
        if not haproxy_daemons:
            raise OperationFailedError(
                f"No running haproxy daemons found for {ingress_service}"
            )
        return haproxy_daemons

    def _node_by_hostname(self, hostname):
        for node in self.ceph_cluster.node_list:
            if getattr(node, "hostname", None) == hostname:
                return node
        raise OperationFailedError(f"Node {hostname!r} not found in cluster node list")

    @staticmethod
    def _daemon_dir(node, daemon_name):
        cmd = f"ls -d /var/lib/ceph/*/{daemon_name} 2>/dev/null | head -1"
        out, _ = node.exec_command(sudo=True, cmd=cmd, check_ec=False)
        haproxy_dir = (out or "").strip()
        if not haproxy_dir:
            raise OperationFailedError(
                f"Could not locate haproxy daemon directory for {daemon_name!r} on "
                f"{node.hostname}"
            )
        return haproxy_dir

    @staticmethod
    def _stick_table_output(node, haproxy_dir):
        cmd = (
            f'cd {haproxy_dir} && echo "show table backend" | socat stdio haproxy/stats'
        )
        out, err = node.exec_command(sudo=True, cmd=cmd, check_ec=False)
        output = f"{out or ''}{err or ''}".strip()
        if not output:
            raise OperationFailedError(
                f"Empty stick table output from haproxy on {node.hostname} "
                f"({haproxy_dir})"
            )
        return output

    @staticmethod
    def _parse_used_count(output):
        match = re.search(r"# table: backend.*used:(\d+)", output)
        if not match:
            raise OperationFailedError(
                f"Could not parse backend stick table usage from output:\n{output}"
            )
        return int(match.group(1))

    @staticmethod
    def _parse_entries(output):
        entries = []
        for line in output.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key_match = re.search(r"key=([^\s]+)", line)
            server_id_match = re.search(r"server_id=(\d+)", line)
            if key_match and server_id_match:
                entries.append(
                    {
                        "key": key_match.group(1),
                        "server_id": int(server_id_match.group(1)),
                        "raw": line,
                    }
                )
        return entries

    @staticmethod
    def _cfg_path(node, haproxy_dir):
        candidates = [
            f"{haproxy_dir}/haproxy/haproxy.cfg",
            f"{haproxy_dir}/haproxy.cfg",
        ]
        for path in candidates:
            out, _ = node.exec_command(
                sudo=True,
                cmd=f"test -f {path} && echo {path}",
                check_ec=False,
            )
            if (out or "").strip():
                return path.strip()
        raise OperationFailedError(
            f"Could not locate haproxy.cfg under {haproxy_dir} "
            f"(tried: {', '.join(candidates)})"
        )

    @classmethod
    def _backend_map(cls, node, haproxy_dir):
        haproxy_cfg = cls._cfg_path(node, haproxy_dir)
        out, _ = node.exec_command(
            sudo=True,
            cmd=f"grep -E '^[[:space:]]*server ' {haproxy_cfg}",
            check_ec=False,
        )
        backend_map = {}
        for line in (out or "").splitlines():
            match = re.search(r"server\s+\S+\s+([^\s:]+):\d+\s+id\s+(\d+)", line)
            if match:
                backend_map[int(match.group(2))] = match.group(1)
        if not backend_map:
            raise OperationFailedError(
                f"Could not parse haproxy backend servers from {haproxy_cfg}"
            )
        return backend_map

    @classmethod
    def _backend_endpoint_map(cls, node, haproxy_dir):
        haproxy_cfg = cls._cfg_path(node, haproxy_dir)
        out, _ = node.exec_command(
            sudo=True,
            cmd=f"grep -E '^[[:space:]]*server ' {haproxy_cfg}",
            check_ec=False,
        )
        endpoint_map = {}
        for line in (out or "").splitlines():
            match = re.search(
                r"server\s+\S+\s+([^\s:]+):(\d+)\s+id\s+(\d+)",
                line,
            )
            if match:
                endpoint_map[int(match.group(3))] = (
                    match.group(1),
                    int(match.group(2)),
                )
        if not endpoint_map:
            raise OperationFailedError(
                f"Could not parse haproxy backend endpoints from {haproxy_cfg}"
            )
        return endpoint_map
