from ceph.ceph_admin.common import config_dict_to_string
from ceph.nvmeof.cli.v2.common import substitute_keys
from utility.log import Log

LOG = Log(__name__)


KEY_MAP = {
    "subsystem": "nqn",
    "host": "host_nqn",
    "host-nqn": "host_nqn",
    "rbd-pool": "rbd_pool",
    "rbd-image": "rbd_image_name",
    "rbd-create-image": "create-image",
    "load-balancing-group": "load_balancing_group",
    "rbd-trash-image-on-delete": "trash-image",
    "rw-ios-per-second": "rw_ios_per_second",
    "rw-megabytes-per-second": "rw_mbytes_per_second",
    "r-megabytes-per-second": "r_mbytes_per_second",
    "w-megabytes-per-second": "w_mbytes_per_second",
    "level": "log_level",
}


class BaseCLI:
    """Execute Command class runs NVMe CLI on Gateway Node."""

    BASE_CMD = "ceph nvmeof"

    def __init__(self, node, shell) -> None:
        """Initialize the Shell.

        Args:
            node: Gateway Node instance (CephNode)
            shell: Cephadm shell instance (orch.shell or cephadm.shell)
        """
        self.node = node
        self.shell = shell

    def __local_mtls_cert_path(self) -> str:
        """Currently mtls is not supported in Ceph NVMe CLI."""
        return ""

    @substitute_keys(KEY_MAP)
    def run_nvme_cli(self, entity, action, **kwargs):
        LOG.info(f"NVMeoF command - {entity} {action}")
        base_cmd_args = kwargs.get("base_cmd_args", {})

        # TODO: Currently mtls is not supported in Ceph NVMe CLI(Tentacle).
        #       Fix this once mtls is supported
        if self.mtls:
            pass

        cmd_args = kwargs.get("args", {})

        # Gateway group
        if not cmd_args.get("gw_group"):
            cmd_args["gw_group"] = self.gateway_group

        # Gateway address
        if not cmd_args.get("traddd"):
            cmd_args["traddr"] = self.node.ip_address

        command = [
            self.BASE_CMD,
            self.__local_mtls_cert_path(),
            entity,
            action,
            config_dict_to_string(cmd_args),
            config_dict_to_string(base_cmd_args),
        ]
        out, err = self.shell(args=command, pretty_print=True)
        return out, err
