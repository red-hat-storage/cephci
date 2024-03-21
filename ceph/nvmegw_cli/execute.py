from ceph.ceph_admin.common import config_dict_to_string
from utility.log import Log

LOG = Log(__name__)


class ExecuteCommandMixin:
    BASE_CMD = "podman run --rm"
    NVMEOF_CLI_IMAGE = "quay.io/ceph/nvmeof-cli:1.1.0"

    def __init__(self, node, port=5500) -> None:
        self.port = port
        self.node = node

    def run_nvme_cli(self, action, **kwargs):
        LOG.info(f"NVMe CLI command : {self.name} {action}")
        base_cmd_args = kwargs.get("base_cmd_args", {})
        base_cmd_args.update(
            {"server-address": self.node.ip_address, "server-port": self.port}
        )
        cmd_args = kwargs.get("args", {})
        command = " ".join(
            [
                self.BASE_CMD,
                self.NVMEOF_CLI_IMAGE,
                config_dict_to_string(base_cmd_args),
                self.name,
                action,
                config_dict_to_string(cmd_args),
            ]
        )
        out = self.node.exec_command(cmd=command, sudo=True)
        LOG.info(out)
        return out
