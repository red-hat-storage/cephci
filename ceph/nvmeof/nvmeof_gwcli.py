"""Ceph-NVMeoF gateway module.

- Configure spdk and start spdk.
- Configure nvme-of targets using control.cli.
"""
from json import loads

from ceph.ceph_admin.common import config_dict_to_string
from ceph.ceph_admin.orch import Orch
from cli.utilities.configs import get_registry_details
from cli.utilities.containers import Registry
from utility.log import Log

LOG = Log(__name__)


def find_client_daemon_id(cluster, pool_name):
    """Find client daemon Id."""
    orch = Orch(cluster=cluster, **{})
    daemons, _ = orch.ps(
        {"base_cmd_args": {"format": "json"}, "args": {"daemon_type": "nvmeof"}}
    )
    for daemon in loads(daemons):
        if daemon["service_name"] == f"nvmeof.{pool_name}":
            return f"client.{daemon['daemon_name']}"


class NVMeCLI:
    """NVMeCLI class for managing NVMeoF Gateway entities."""

    CEPH_NVMECLI_IMAGE = "quay.io/ceph/nvmeof-cli:0.0.3"

    def __init__(self, node, port=5500):
        self.node = node
        self.port = port
        self.check()

    def check(self):
        """Method to pre-check the pre-requisites.
        - check for podman utility
        - login to IBM container registry if it is IBM build
        """
        check_lists = ["dnf install podman -y"]
        for cmd in check_lists:
            self.node.exec_command(cmd=cmd, sudo=True)
        if self.CEPH_NVMECLI_IMAGE.startswith("cp"):
            registry_details = get_registry_details(ibm_build=True)
            url = registry_details.get("registry-url")
            username = registry_details.get("registry-username")
            password = registry_details.get("registry-password")
            Registry(self.node).login(url, username, password)

    def run_control_cli(self, action, **cmd_args):
        """Run CLI via control daemon."""
        base_cmd = f"podman run {self.CEPH_NVMECLI_IMAGE}"
        base_cmd += (
            f" --server-address {self.node.ip_address} --server-port {self.port}"
        )
        out = self.node.exec_command(
            sudo=True,
            cmd=f"{base_cmd} {action} {config_dict_to_string(cmd_args)}",
        )
        LOG.debug(out)
        return out

    def get_subsystems(self):
        """Get all subsystems."""
        return self.run_control_cli("get_subsystems")

    def create_block_device(self, name, image, pool, block_size=None):
        """Create block device using rbd image."""
        args = {"image": image, "pool": pool, "bdev": name}
        if block_size:
            args["block-size"] = block_size
        return self.run_control_cli("create_bdev", **args)

    def delete_block_device(self, name):
        """Delete block device."""
        args = {"bdev": name}
        return self.run_control_cli("delete_bdev", **args)

    def create_subsystem(self, subnqn, serial_num, max_ns=None):
        """Create subsystem."""
        args = {"subnqn": subnqn, "serial": serial_num}
        if max_ns:
            args["max-namespaces"] = max_ns
        return self.run_control_cli("create_subsystem", **args)

    def delete_subsystem(self, subnqn):
        """Delete subsystem."""
        return self.run_control_cli("delete_subsystem", **{"subnqn": subnqn})

    def add_namespace(self, subnqn, bdev, nsid=None):
        """Add namespace under subsystem."""
        args = {"subnqn": subnqn, "bdev": bdev}
        if nsid:
            args["nsid"] = nsid
        return self.run_control_cli("add_namespace", **args)

    def remove_namespace(self, subnqn, bdev):
        """Remove namespace under subsystem."""
        args = {"subnqn": subnqn}
        _, subsystems = self.get_subsystems()
        for sub in loads("\n".join(subsystems.split("\n")[1:])):
            if sub["nqn"] == subnqn:
                for namespace in sub["namespaces"]:
                    if namespace["name"] == bdev:
                        args["nsid"] = namespace["nsid"]
        return self.run_control_cli("remove_namespace", **args)

    def add_host(self, subnqn, hostnqn):
        """Add host to subsystem."""
        args = {"subnqn": subnqn, "host": repr(hostnqn)}
        return self.run_control_cli("add_host", **args)

    def remove_host(self, subnqn, hostnqn):
        """Remove host from subsystem."""
        args = {"subnqn": subnqn, "host": repr(hostnqn)}
        return self.run_control_cli("remove_host", **args)

    def create_listener(self, subnqn, port, **kwargs):
        """Create listener under subsystem.

        Args:
            subnqn: subsystem nqn
            port: transport channel port
            kwargs: other attributes
        """
        args = {
            "subnqn": subnqn,
            "trsvcid": port,
            "gateway-name": kwargs["gateway-name"],
            "traddr": kwargs["traddr"],
            "trtype": kwargs.get("trtype", False),
            "adrfam": kwargs.get("adrfam", False),
        }
        return self.run_control_cli("create_listener", **args)

    def delete_listener(self, subnqn, port, **kwargs):
        """Delete listener under subsystem.

        Args:
            subnqn: subsystem nqn
            port: transport channel port
            kwargs: other attributes
        """
        args = {
            "subnqn": subnqn,
            "trsvcid": port,
            "gateway-name": kwargs.get("gateway-name", False),
            "trtype": kwargs.get("trtype", False),
            "adrfam": kwargs.get("adrfam", False),
            "traddr": kwargs.get("traddr", False),
        }
        return self.run_control_cli("delete_listener", **args)
