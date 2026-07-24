"""Suite config and orchestrator spec builders."""

import json

from ceph.waiter import WaitUntil
from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.lib.multi_active.constants import (
    BG_IO_RUNTIME,
    FIO_BS,
    FIO_IODEPTH,
    FIO_SIZE,
)
from tests.nfs.lib.multi_active.placement import node_slice
from utility.log import Log

log = Log(__name__)


class NfsMultiActiveConfig:
    """Suite config and orchestrator spec builders."""

    @staticmethod
    def get_vips(config):
        if not config:
            raise ConfigError("Test config is required to resolve NFS ingress VIPs")

        vips = config.get("vips") or []
        if not vips and config.get("vip"):
            vips = [config["vip"]]
        if not vips:
            raise ConfigError(
                "No VIPs configured. Add 'vips' (or 'vip') under the test config "
                "in the suite YAML."
            )
        return vips

    @staticmethod
    def resolve_io_tool(config):
        """Return background IO tool: ``dd`` (default) or ``fio``."""
        tool = str((config or {}).get("io_tool", "dd")).lower()
        if tool not in ("dd", "fio"):
            raise ConfigError(f"io_tool must be 'dd' or 'fio', got {tool!r}")
        return tool

    @staticmethod
    def io_runtime(config, default=BG_IO_RUNTIME):
        """Return background IO runtime in seconds for the configured tool."""
        tool = NfsMultiActiveConfig.resolve_io_tool(config)
        if tool == "fio":
            return int(
                (config or {}).get(
                    "fio_runtime",
                    (config or {}).get("bg_io_runtime", default),
                )
            )
        return int((config or {}).get("bg_io_runtime", default))

    @staticmethod
    def fio_params(config):
        """Return fio CLI parameters from suite config."""
        cfg = config or {}
        return {
            "bs": str(cfg.get("fio_bs", FIO_BS)),
            "size": str(cfg.get("fio_size", FIO_SIZE)),
            "iodepth": int(cfg.get("fio_iodepth", FIO_IODEPTH)),
        }

    @staticmethod
    def _nfs_placement(nfs_hosts, nfs_count, label=None):
        placement = {"count": nfs_count}
        if label:
            placement["label"] = label
        else:
            placement["hosts"] = nfs_hosts
        return placement

    @staticmethod
    def build_nfs_spec(nfs_hosts, nfs_count, nfs_name, nfs_backend_port, label=None):
        return {
            "service_type": "nfs",
            "service_id": nfs_name,
            "placement": NfsMultiActiveConfig._nfs_placement(
                nfs_hosts, nfs_count, label
            ),
            "spec": {
                "port": nfs_backend_port,
                "enable_haproxy_protocol": True,
            },
        }

    @staticmethod
    def build_nfs_colocation_spec(
        nfs_hosts,
        nfs_count,
        nfs_name,
        port,
        monitoring_port,
        colocation_ports,
        label=None,
    ):
        """NFS spec with monitoring_port and colocation_ports for multi-daemon hosts."""
        if len(colocation_ports) != nfs_count - 1:
            raise ConfigError(
                f"colocation_ports must have count-1 ({nfs_count - 1}) entries, "
                f"got {len(colocation_ports)}"
            )
        return {
            "service_type": "nfs",
            "service_id": nfs_name,
            "placement": NfsMultiActiveConfig._nfs_placement(
                nfs_hosts, nfs_count, label
            ),
            "spec": {
                "port": port,
                "monitoring_port": monitoring_port,
                "colocation_ports": colocation_ports,
                "enable_haproxy_protocol": True,
            },
        }

    @staticmethod
    def expected_colocation_data_ports(nfs_spec):
        """Return the set of NFS data ports defined by a colocation spec."""
        spec = nfs_spec["spec"]
        ports = {int(spec["port"])}
        for entry in spec.get("colocation_ports") or []:
            ports.add(int(entry["data_port"]))
        return ports

    @staticmethod
    def build_ingress_spec(
        vip,
        nfs_name,
        ingress_frontend_port,
        ingress_monitor_port,
        health_check_interval,
        *,
        ingress_hostnames=None,
        label=None,
    ):
        """Ingress placement: hosts or label only — never count."""
        if label:
            placement = {"label": label}
        else:
            if not ingress_hostnames:
                raise ConfigError(
                    "ingress_hostnames is required when ingress placement "
                    "does not use a label"
                )
            placement = {"hosts": ingress_hostnames}
        return {
            "service_type": "ingress",
            "service_id": f"nfs.{nfs_name}",
            "placement": placement,
            "spec": {
                "backend_service": f"nfs.{nfs_name}",
                "frontend_port": ingress_frontend_port,
                "monitor_port": ingress_monitor_port,
                "virtual_ip": vip,
                "health_check_interval": health_check_interval,
                "enable_haproxy_protocol": True,
            },
        }

    @staticmethod
    def sorted_nfs_nodes(ceph_cluster):
        """Return NFS-capable nodes in stable hostname order for placement slices."""
        return sorted(
            ceph_cluster.get_nodes("nfs"),
            key=lambda node: node.hostname,
        )

    @staticmethod
    def apply_placement_labels(client, nfs_nodes, nfs_slice, ingress_slice, labels):
        nfs_label, ingress_label = labels["nfs"], labels["ingress"]
        nfs_hosts = []
        for node in node_slice(nfs_nodes, nfs_slice[0], nfs_slice[1]):
            nfs_hosts.append(node.hostname)
            client.exec_command(
                sudo=True,
                cmd=f"ceph orch host label add {node.hostname} {nfs_label}",
            )
        ingress_hosts = []
        for node in node_slice(nfs_nodes, ingress_slice[0], ingress_slice[1]):
            ingress_hosts.append(node.hostname)
            client.exec_command(
                sudo=True,
                cmd=f"ceph orch host label add {node.hostname} {ingress_label}",
            )
        log.info(
            "Applied placement labels nfs=%r on %s ingress=%r on %s",
            nfs_label,
            nfs_hosts,
            ingress_label,
            ingress_hosts,
        )

    @staticmethod
    def remove_placement_labels(client, nfs_nodes, nfs_slice, ingress_slice, labels):
        nfs_label, ingress_label = labels["nfs"], labels["ingress"]
        for node in node_slice(nfs_nodes, nfs_slice[0], nfs_slice[1]):
            client.exec_command(
                sudo=True,
                cmd=f"ceph orch host label rm {node.hostname} {nfs_label}",
                check_ec=False,
            )
        for node in node_slice(nfs_nodes, ingress_slice[0], ingress_slice[1]):
            client.exec_command(
                sudo=True,
                cmd=f"ceph orch host label rm {node.hostname} {ingress_label}",
                check_ec=False,
            )
        log.info(
            "Removed placement labels nfs=%r ingress=%r",
            nfs_label,
            ingress_label,
        )

    @staticmethod
    def clear_placement_labels(client, nfs_nodes, labels):
        """Remove placement labels from every NFS-capable host (incl. failover spares)."""
        nfs_label, ingress_label = labels["nfs"], labels["ingress"]
        for label in (nfs_label, ingress_label):
            for node in nfs_nodes:
                client.exec_command(
                    sudo=True,
                    cmd=f"ceph orch host label rm {node.hostname} {label}",
                    check_ec=False,
                )
        log.info(
            "Cleared placement labels nfs=%r ingress=%r from all NFS hosts",
            nfs_label,
            ingress_label,
        )

    @staticmethod
    def wait_until_export_visible(
        client, nfs_name, export_name, timeout=60, interval=1
    ):
        for _ in WaitUntil(timeout=timeout, interval=interval):
            raw = Ceph(client).nfs.export.ls(nfs_name)
            if not raw:
                continue
            exports = json.loads(raw.strip()) if isinstance(raw, str) else raw
            if export_name in exports:
                log.info("Export %s listed for %s", export_name, nfs_name)
                return
        raise OperationFailedError(
            f"Timed out after {timeout}s waiting for export {export_name!r} "
            f"on {nfs_name!r}"
        )
