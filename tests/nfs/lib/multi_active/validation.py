"""Post-deploy cluster info and orchestrator daemon validation."""

import json

from ceph.waiter import WaitUntil
from cli.ceph.ceph import Ceph
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from tests.nfs.lib.multi_active.config import NfsMultiActiveConfig
from tests.nfs.lib.multi_active.constants import log_section
from utility.log import Log

log = Log(__name__)


class NfsMultiActiveValidation:

    def __init__(self, ceph_cluster, client):
        self.ceph_cluster = ceph_cluster
        self.client = client
        self.installer = ceph_cluster.get_nodes("installer")[0]

    def get_cluster_info(self, nfs_name):
        return self.fetch_cluster_info(self.client, nfs_name)

    @staticmethod
    def fetch_cluster_info(client, nfs_name):
        info = Ceph(client).nfs.cluster.info(nfs_name)
        if not info:
            raise OperationFailedError(
                f"No cluster info returned for NFS cluster {nfs_name!r}"
            )

        cluster_info = info.get(nfs_name)
        if cluster_info is None:
            raise OperationFailedError(
                f"Cluster {nfs_name!r} not found in nfs cluster info: {info!r}"
            )

        log.info("NFS cluster info for %s:\n%s", nfs_name, json.dumps(info, indent=2))
        return cluster_info

    @staticmethod
    def assert_cluster_info(cluster_info, nfs_spec, ingress_spec):
        nfs_name = nfs_spec["service_id"]
        expected_backend_port = int(nfs_spec["spec"]["port"])

        log_section(log, "NFS CLUSTER INFO VALIDATION")

        backends = cluster_info.get("backend") or []
        if not backends:
            raise OperationFailedError(
                f"NFS cluster info for {nfs_name!r} has no backend entries: "
                f"{cluster_info!r}"
            )

        nfs_spec_body = nfs_spec.get("spec", {})
        expected_monitoring_port = None
        if nfs_spec_body.get("monitoring_port") is not None:
            expected_monitoring_port = int(nfs_spec_body["monitoring_port"])
        elif nfs_spec_body.get("enable_haproxy_protocol"):
            expected_monitoring_port = expected_backend_port + 1

        data_backends = []
        for backend in backends:
            hostname = backend.get("hostname", "unknown")
            port = backend.get("port")
            if port is None:
                raise OperationFailedError(
                    f"Backend entry for {hostname!r} is missing port: {backend!r}"
                )
            port_int = int(port)
            if port_int == expected_backend_port:
                data_backends.append(backend)
                log.info(
                    "Backend %s port %s matches NFS spec port %s",
                    hostname,
                    port,
                    expected_backend_port,
                )
            elif (
                expected_monitoring_port is not None
                and port_int == expected_monitoring_port
            ):
                log.info(
                    "Backend %s monitoring port %s accepted (data port %s)",
                    hostname,
                    port_int,
                    expected_backend_port,
                )
            else:
                raise OperationFailedError(
                    f"Backend port mismatch on {hostname}: expected "
                    f"{expected_backend_port}"
                    f"{f' or monitoring port {expected_monitoring_port}' if expected_monitoring_port else ''}, "
                    f"got {port!r}"
                )

        expected_nfs_count = int(nfs_spec["placement"]["count"])
        if len(data_backends) != expected_nfs_count:
            raise OperationFailedError(
                f"Expected {expected_nfs_count} NFS data backends for {nfs_name!r}, "
                f"found {len(data_backends)}: {data_backends!r}"
            )

        NfsMultiActiveValidation._assert_ingress_cluster_info(
            cluster_info, ingress_spec
        )
        log.info("NFS cluster info validation passed for %s", nfs_name)

    @staticmethod
    def assert_colocation_cluster_info(cluster_info, nfs_spec, ingress_spec):
        """Validate cluster info when NFS backends use distinct colocation data ports."""
        nfs_name = nfs_spec["service_id"]
        expected_ports = NfsMultiActiveConfig.expected_colocation_data_ports(nfs_spec)
        expected_count = int(nfs_spec["placement"]["count"])

        log_section(log, "NFS COLOCATION CLUSTER INFO VALIDATION")

        backends = cluster_info.get("backend") or []
        if len(backends) != expected_count:
            raise OperationFailedError(
                f"Expected {expected_count} NFS backends for {nfs_name!r}, "
                f"found {len(backends)}: {backends!r}"
            )

        seen_ports = []
        for backend in backends:
            hostname = backend.get("hostname", "unknown")
            port = backend.get("port")
            if port is None:
                raise OperationFailedError(
                    f"Backend entry for {hostname!r} is missing port: {backend!r}"
                )
            port = int(port)
            if port not in expected_ports:
                raise OperationFailedError(
                    f"Unexpected backend port on {hostname}: {port!r}, "
                    f"expected one of {sorted(expected_ports)}"
                )
            seen_ports.append(port)
            log.info(
                "Backend %s port %s is a configured colocation data port",
                hostname,
                port,
            )

        log.info(
            "Backend data ports %s are within expected set %s",
            sorted(seen_ports),
            sorted(expected_ports),
        )

        NfsMultiActiveValidation._assert_ingress_cluster_info(
            cluster_info, ingress_spec
        )
        log.info("NFS colocation cluster info validation passed for %s", nfs_name)

    def _node_by_hostname(self, hostname):
        for node in self.ceph_cluster.node_list:
            if getattr(node, "hostname", None) == hostname:
                return node
        raise OperationFailedError(f"Node {hostname!r} not found in cluster node list")

    @staticmethod
    def _assert_port_listening(node, port, label):
        out, _ = node.exec_command(
            sudo=True,
            cmd=f"ss -H -tulpn sport = :{port}",
            check_ec=False,
        )
        if not out or not str(out).strip():
            raise OperationFailedError(
                f"{label} port {port} is not listening on {node.hostname}"
            )
        log.info("%s port %s listening on %s", label, port, node.hostname)

    def assert_colocation_ports_listening(self, cluster_info, nfs_spec):
        """Verify NFS data ports from cluster info are listening on backend hosts.

        Monitoring and cluster_qos ports are not checked here: colocated daemons on
        this Ceph release may not publish spec monitoring_port values on the host
        (orch reports a single service monitoring port; per-daemon exporters may
        auto-increment or bind inside the container only).
        """
        expected_ports = NfsMultiActiveConfig.expected_colocation_data_ports(nfs_spec)
        log_section(log, "COLOCATION DATA PORT LISTENING VALIDATION")
        for backend in cluster_info.get("backend") or []:
            hostname = backend.get("hostname")
            data_port = int(backend.get("port"))
            if data_port not in expected_ports:
                raise OperationFailedError(
                    f"Backend port {data_port} on {hostname!r} not in colocation spec"
                )
            node = self._node_by_hostname(hostname)
            self._assert_port_listening(node, data_port, "NFS data")
        log.info("Colocation data port listening validation passed")

    def assert_orch_ps(
        self,
        nfs_spec,
        ingress_spec,
        expected_ingress_hosts=None,
        timeout=None,
    ):
        nfs_name = nfs_spec["service_id"]
        nfs_service = f"nfs.{nfs_name}"
        ingress_service = f"ingress.nfs.{nfs_name}"
        expected_nfs_count = int(nfs_spec["placement"]["count"])
        ingress_placement = ingress_spec["placement"]
        if "hosts" in ingress_placement:
            expected_ingress_count = len(ingress_placement["hosts"])
        elif expected_ingress_hosts is not None:
            expected_ingress_count = len(expected_ingress_hosts)
        else:
            raise OperationFailedError(
                "expected_ingress_hosts is required when ingress placement "
                "uses a label"
            )

        log_section(log, "ORCH PS DAEMON VALIDATION")

        def _check():
            nfs_daemons = self.get_orch_ps_daemons(nfs_service)
            self._validate_running_daemon_count(
                "NFS", nfs_daemons, expected_nfs_count, nfs_service
            )

            ingress_daemons = self.get_orch_ps_daemons(ingress_service)

            def _ingress_daemons(component):
                return [
                    daemon
                    for daemon in ingress_daemons
                    if self.daemon_component(daemon) == component
                ]

            haproxy_daemons = _ingress_daemons("haproxy")
            keepalived_daemons = _ingress_daemons("keepalived")
            self._validate_running_daemon_count(
                "haproxy", haproxy_daemons, expected_ingress_count, ingress_service
            )
            self._validate_running_daemon_count(
                "keepalived",
                keepalived_daemons,
                expected_ingress_count,
                ingress_service,
            )
            if expected_ingress_hosts is not None:
                running_haproxy = self.running_daemons(haproxy_daemons)
                actual_hosts = {d.get("hostname") for d in running_haproxy}
                expected_hosts = set(expected_ingress_hosts)
                if actual_hosts != expected_hosts:
                    raise OperationFailedError(
                        f"Expected ingress haproxy on {sorted(expected_hosts)}, "
                        f"found {sorted(actual_hosts)} for {ingress_service}"
                    )
            return nfs_daemons, ingress_daemons

        if timeout:
            last_error = None
            for _ in WaitUntil(timeout=timeout, interval=10):
                try:
                    nfs_daemons, ingress_daemons = _check()
                    last_error = None
                    break
                except OperationFailedError as exc:
                    last_error = exc
            if last_error is not None:
                raise last_error
        else:
            nfs_daemons, ingress_daemons = _check()

        log.info(
            "orch ps for %s:\n%s",
            nfs_service,
            json.dumps(self._format_daemon_summary(nfs_daemons), indent=2),
        )
        log.info(
            "orch ps for %s:\n%s",
            ingress_service,
            json.dumps(self._format_daemon_summary(ingress_daemons), indent=2),
        )
        log.info(
            "Orch ps validation passed: %d NFS, %d haproxy, %d keepalived daemons "
            "running",
            expected_nfs_count,
            expected_ingress_count,
            expected_ingress_count,
        )

    def get_orch_ps_daemons(self, service_name):
        raw = CephAdm(self.installer).ceph.orch.ps(
            format="json", service_name=service_name
        )
        if not raw:
            return []
        if isinstance(raw, str):
            return json.loads(raw.strip())
        return json.loads(raw.read().decode().strip())

    @staticmethod
    def running_daemons(daemons):
        return [
            daemon
            for daemon in daemons
            if str(daemon.get("status_desc", "")).strip().lower() == "running"
        ]

    @staticmethod
    def daemon_component(daemon):
        daemon_type = str(daemon.get("daemon_type", "")).lower()
        if daemon_type in ("haproxy", "keepalived", "nfs"):
            return daemon_type

        daemon_name = daemon.get("daemon_name") or daemon.get("name") or ""
        for component in ("haproxy", "keepalived", "nfs"):
            if daemon_name.startswith(f"{component}."):
                return component
        return None

    @staticmethod
    def backend_ips(cluster_info):
        backends = cluster_info.get("backend") or []
        return {backend.get("ip") for backend in backends if backend.get("ip")}

    @staticmethod
    def _expected_ingress_mode(ingress_spec):
        if ingress_spec.get("spec", {}).get("enable_haproxy_protocol"):
            return "haproxy-protocol"
        return "haproxy-standard"

    @staticmethod
    def _assert_ingress_cluster_info(cluster_info, ingress_spec):
        ingress = ingress_spec["spec"]
        ingress_checks = {
            "ingress_mode": (
                cluster_info.get("ingress_mode"),
                NfsMultiActiveValidation._expected_ingress_mode(ingress_spec),
            ),
            "monitor_port": (
                cluster_info.get("monitor_port"),
                int(ingress["monitor_port"]),
            ),
            "port": (cluster_info.get("port"), int(ingress["frontend_port"])),
            "virtual_ip": (cluster_info.get("virtual_ip"), ingress["virtual_ip"]),
        }

        for field, (actual, expected) in ingress_checks.items():
            if field == "virtual_ip":
                if not NfsMultiActiveValidation._virtual_ip_matches(expected, actual):
                    raise OperationFailedError(
                        f"Cluster info virtual_ip mismatch: expected {expected!r}, "
                        f"got {actual!r}"
                    )
            elif field in ("monitor_port", "port"):
                if actual is None:
                    raise OperationFailedError(
                        f"Cluster info missing {field!r}; expected {expected!r}"
                    )
                if int(actual) != int(expected):
                    raise OperationFailedError(
                        f"Cluster info {field} mismatch: expected {expected!r}, "
                        f"got {actual!r}"
                    )
            elif actual != expected:
                raise OperationFailedError(
                    f"Cluster info {field} mismatch: expected {expected!r}, "
                    f"got {actual!r}"
                )
            log.info("Cluster info %s=%s matches ingress spec", field, actual)

    @staticmethod
    def _virtual_ip_matches(expected, actual):
        if expected == actual:
            return True
        if expected is None or actual is None:
            return False
        return str(expected).split("/")[0] == str(actual).split("/")[0]

    def _validate_running_daemon_count(
        self, label, daemons, expected_count, service_name
    ):
        running = self.running_daemons(daemons)
        if len(running) != expected_count:
            raise OperationFailedError(
                f"Expected {expected_count} running {label} daemon(s) for "
                f"{service_name}, found {len(running)}: "
                f"{self._format_daemon_summary(daemons)}"
            )

        for daemon in running:
            log.info(
                "%s daemon running: %s on %s",
                label,
                daemon.get("daemon_name") or daemon.get("name"),
                daemon.get("hostname"),
            )
        log.info("%d %s daemon(s) running for %s", expected_count, label, service_name)

    @staticmethod
    def _format_daemon_summary(daemons):
        return [
            {
                "name": daemon.get("daemon_name") or daemon.get("name"),
                "hostname": daemon.get("hostname"),
                "status_desc": daemon.get("status_desc"),
                "daemon_type": daemon.get("daemon_type"),
            }
            for daemon in daemons
        ]
