from json import loads
from typing import Any

from ceph.nvmeof.cli.v1 import NVMeGWCLI
from ceph.nvmeof.cli.v2 import NVMeGWCLIV2
from cli.utilities.utils import exec_command_on_container, get_running_containers
from utility.systemctl import SystemCtl


class NVMeGatewayBase:
    """Base class containing common properties & utilities."""

    def __init__(self, node, **kwargs):
        self.node = node
        self._mtls = kwargs.get("mtls", None)
        self._gw_group = kwargs.get("gw_group", None)
        self._ana_group = None
        self._ana_group_id = None
        self._daemon_name = None
        self.systemctl = SystemCtl(node)

    @property
    def mtls(self):
        return self._mtls

    @mtls.setter
    def mtls(self, value):
        self._mtls = value
        # Call CLI setter if supported
        if hasattr(self, "setter"):
            self.setter("mtls", value)

    @property
    def ana_group_id(self):
        return self._ana_group_id

    @ana_group_id.setter
    def ana_group_id(self, value):
        self._ana_group_id = value

    @property
    def ana_group(self):
        return self._ana_group

    @ana_group.setter
    def ana_group(self, value):
        self._ana_group = value

    @property
    def gateway_group(self):
        return self._gw_group

    @gateway_group.setter
    def gateway_group(self, value):
        self._gw_group = value

    @property
    def daemon_name(self):
        return self._daemon_name

    @daemon_name.setter
    def daemon_name(self, value):
        self._daemon_name = value

    @property
    def system_unit_id(self):
        return self.systemctl.get_service_unit("*@nvmeof*")

    @property
    def hostname(self):
        return self.node.hostname

    def get_io_stats(self, subsystem, namespaces):
        """Fetch I/O statistics - must be implemented in version-specific class."""
        raise NotImplementedError

    def get_nvme_container(self):
        """Fetch NVMeoF GW container id (string)."""
        out, _ = get_running_containers(
            self.node,
            expr="name=nvmeof",
            format="{{.ID}}",
            sudo=True,
        )
        container_ids = [line.strip() for line in out.splitlines() if line.strip()]
        if not container_ids:
            raise RuntimeError(f"No NVMe-oF container found on {self.node.hostname}")
        return container_ids[0]

    def get_ana_states(self, subsystem, ana_groups):
        """Fetch ANA states from NVMeoF GW container."""
        cmd = (
            f"/usr/libexec/spdk/scripts/rpc.py nvmf_subsystem_get_listeners {subsystem}"
        )
        out, _ = exec_command_on_container(
            self.node, self.get_nvme_container(), cmd, sudo=True
        )
        out = loads(out)[0]["ana_states"]

        optimized, inaccessible = [], []
        for ana_group in out:
            ana_group_id = ana_group["ana_group"]
            ana_group_state = ana_group["ana_state"]
            if ana_group_id in ana_groups:
                if ana_group_state == "optimized":
                    optimized.append(ana_group_id)
                elif ana_group_state == "inaccessible":
                    inaccessible.append(ana_group_id)

        return optimized, inaccessible

    def _rpc(self, rpc_cmd):
        """Run an SPDK rpc.py command inside the NVMe-oF gateway container."""
        cmd = f"/usr/libexec/spdk/scripts/rpc.py {rpc_cmd}"
        return exec_command_on_container(
            self.node, self.get_nvme_container(), cmd, sudo=True
        )

    def cnc_set_config(self, **params):
        """Configure CNC via ``nvmf_cnc_set_config``.

        Args:
            host_behav_support_cnc: bool (default True)
            rate_limit_bytes: int
            max_inflight: int
            chunk_nlb: int
        """
        support = params.get("host_behav_support_cnc", True)
        support_flag = (
            "--host-behav-support-cnc" if support else "--host-behav-support-cnc false"
        )
        parts = [f"nvmf_cnc_set_config {support_flag}"]
        if params.get("rate_limit_bytes") is not None:
            parts.append(f"--rate-limit-bytes {params['rate_limit_bytes']}")
        if params.get("max_inflight") is not None:
            parts.append(f"--max-inflight {params['max_inflight']}")
        if params.get("chunk_nlb") is not None:
            parts.append(f"--chunk-nlb {params['chunk_nlb']}")
        return self._rpc(" ".join(parts))

    def cnc_enable_logging(self, level="DEBUG"):
        """Enable nvmf_cnc debug logging on the gateway.

        SPDK rpc.py takes positional args: ``log_set_flag <flag>`` and
        ``log_set_level <level>`` (not ``-i``).
        """
        self._rpc("log_set_flag nvmf_cnc")
        return self._rpc(f"log_set_level {level}")

    def cnc_get_container_logs(self, lines=200):
        """Fetch recent gateway container logs for CNC diagnostics."""
        ctr = self.get_nvme_container()
        out, _ = self.node.exec_command(
            cmd=f"podman logs --tail {lines} {ctr}",
            sudo=True,
        )
        return out


class NVMeGatewayV1(NVMeGatewayBase, NVMeGWCLI):
    """NVMe Gateway (V1 CLI backend)."""

    def __init__(self, node, **kwargs):
        super().__init__(node, **kwargs)
        NVMeGWCLI.__init__(self, node, **kwargs)
        self.ana_group = self.fetch_gateway()
        self.ana_group_id = self.ana_group["load_balancing_group"]
        self.daemon_name = self.ana_group["name"].split(".", 1)[1]


class NVMeGatewayV2(NVMeGatewayBase, NVMeGWCLIV2):
    """NVMe Gateway (V2 CLI backend)."""

    def __init__(self, node, **kwargs):
        super().__init__(node, **kwargs)
        NVMeGWCLIV2.__init__(self, node, **kwargs)
        self.ana_group = self.fetch_gateway()
        self.gateway_group = self.ana_group["group"]
        self.ana_group_id = self.ana_group["load_balancing_group"]
        self.daemon_name = self.ana_group["name"].split(".", 1)[1]


def create_gateway(
    version: type, node: Any, **kwargs: dict[str, Any]
) -> NVMeGatewayBase:
    """Factory to create NVMe-oF gateway instance."""
    if version is NVMeGWCLI:
        return NVMeGatewayV1(node, **kwargs)
    elif version is NVMeGWCLIV2:
        return NVMeGatewayV2(node, **kwargs)
    raise ValueError(f"Unsupported gateway version: {version}")
