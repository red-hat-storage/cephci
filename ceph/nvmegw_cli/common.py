"""Ceph-NVMeoF gateway module.

- Configure spdk and start spdk.
- Configure nvme-of targets using control.cli.
"""
from json import loads

from ceph.ceph_admin.common import config_dict_to_string
from cli.utilities.configs import get_registry_details
from cli.utilities.containers import Registry
from utility.log import Log

from . import NVMeGWCLI

LOG = Log(__name__)


def find_client_daemon_id(node, port=5500):
    """Find client daemon Id."""
    return NVMeGWCLI(node, port).fetch_gateway_client_name()


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
        base_cmd = f"podman run --rm {self.CEPH_NVMECLI_IMAGE}"
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

    def get_spdk_nvmf_log_flags_and_level(self):
        """Get nvmeoF log flags and level."""
        return self.run_control_cli("get_spdk_nvmf_log_flags_and_level")

    def disable_spdk_nvmf_logs(self):
        """Disable SPDK NVMeoF logs."""
        return self.run_control_cli("disable_spdk_nvmf_logs")

    def set_spdk_nvmf_logs(self, **kwargs):
        """Set SPDK NVMeoF logs."""
        args = {"flags": True}
        args = {**args, **kwargs}
        return self.run_control_cli("set_spdk_nvmf_logs", **args)

    def create_block_device(self, name, image, pool, **kwargs):
        """Create block device using rbd image."""
        args = {"image": image, "pool": pool, "bdev": name}
        args = {**args, **kwargs}
        return self.run_control_cli("create_bdev", **args)

    def resize_block_device(self, bdev, size):
        """Create block device using rbd image."""
        args = {"bdev": bdev, "size": size}
        return self.run_control_cli("resize_bdev", **args)

    def delete_block_device(self, name, force=None):
        """Delete block device."""
        args = {"bdev": name}
        if force:
            args["force"] = True
        return self.run_control_cli("delete_bdev", **args)

    def create_subsystem(self, subnqn, serial_num, **kwargs):
        """Create subsystem."""
        args = {"subnqn": subnqn, "serial": serial_num}
        args = {**args, **kwargs}
        return self.run_control_cli("create_subsystem", **args)

    def delete_subsystem(self, subnqn):
        """Delete subsystem."""
        return self.run_control_cli("delete_subsystem", **{"subnqn": subnqn})

    def add_namespace(self, subnqn, bdev, **kwargs):
        """Add namespace under subsystem."""
        args = {"subnqn": subnqn, "bdev": bdev}
        args = {**args, **kwargs}
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
