from ceph.nvmegw_cli import NVMeGWCLI
from ceph.utils import get_node_by_id
from utility.utils import get_ceph_version_from_cluster


class NVMeGateway:
    """
    NVMe Gateway abstraction (minimal, for workflow use).
    For full gateway logic, see tests/nvmeof/workflows/nvme_gateway.py:NVMeGateway.
    Attributes:
        ceph_node: Node where the gateway is deployed
        ceph_cluster: Ceph cluster object
        gateway_daemon_name: Name of the gateway daemon
        gateway_service_name: Name of the gateway service
        nvmegwcli: NVMeGWCLI instance for this gateway (or future class for >= 9.0)
    """

    def __init__(
        self,
        ceph_node,
        ceph_cluster,
        gateway_daemon_name: str,
        gateway_service_name: str,
        mtls: bool = False,
        port: int = 5500,
        subsystem_config: dict = None,
    ):
        self.ceph_node = get_node_by_id(ceph_cluster, ceph_node)
        self.ceph_cluster = ceph_cluster
        self.gateway_daemon_name = gateway_daemon_name
        self.gateway_service_name = gateway_service_name
        self.mtls = mtls
        self.port = port
        self.subsystem_config = subsystem_config
        self._cli = None
        self._version = get_ceph_version_from_cluster(ceph_cluster)

    @property
    def nvmegwcli(self):
        current_ceph_version = get_ceph_version_from_cluster(self.ceph_cluster)
        if self._cli is None or self._version != current_ceph_version:
            self._cli = (
                NVMeGWCLI(
                    self.ceph_node,
                    port=self.port,
                    mtls=self.mtls,  # change this to new cli class for ceph >= 9.0
                )
                if current_ceph_version >= "20.0"
                else NVMeGWCLI(self.ceph_node, port=self.port, mtls=self.mtls)
            )
            self._version = current_ceph_version
        return self._cli

    def configure_listeners(self, subsystem_config: dict):
        """
        Configure listeners for this specific gateway.
        This is called per gateway since each gateway needs its own listeners.
        Args:
            subsystem_config: Configuration for the subsystem
        """
        # Configure listeners if specified
        for sub_cfg in subsystem_config:
            if sub_cfg.get("listeners"):
                listeners = sub_cfg["listeners"]
                if not isinstance(listeners, list):
                    listeners = [listeners]

                nqn = sub_cfg.get("nqn") or sub_cfg.get("subnqn")

                # for listener in listeners:
                listener_config = {
                    "args": {
                        "subsystem": nqn,
                        "traddr": getattr(self.ceph_node, "ip_address", None),
                        "trsvcid": sub_cfg.get("listener_port", 4420),
                        "host-name": getattr(
                            self.ceph_node, "hostname", str(self.ceph_node)
                        ),
                    }
                }
                self.nvmegwcli.listener.add(**listener_config)
