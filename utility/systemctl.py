from json import loads

from cli.utilities.utils import config_dict_to_string
from utility.log import Log

LOG = Log(__name__)


class SystemCtl:
    BASE_CMD = "systemctl"

    def __init__(self, node):
        self.node = node

    def get_service(self, regex):
        """Fetch Service info.

        Provide regex which could match required services.

        Args:
            regex: regex pattern matches single service
        Returns:
            list of unit.services matches regex
        """
        # systemctl list-units --type=service --state=running --no-legend *@nvmeof* --output json-pretty
        base_cmd = [
            self.BASE_CMD,
            "list-units",
            "--type=service",
            "--no-legend",
            "--no-pager",
            repr(regex),
            "--output=json-pretty",
        ]

        out, _ = self.node.exec_command(cmd=" ".join(base_cmd), sudo=True)
        return loads(out)

    def get_service_unit(self, regex):
        return self.get_service(regex)[0]["unit"]

    def is_active(self, unit_name):
        base_cmd = [self.BASE_CMD, "is-active", unit_name]
        out, _ = self.node.exec_command(
            cmd=" ".join(base_cmd), sudo=True, check_ec=False
        )
        LOG.info(f"{unit_name} is {out}")
        if out.strip() in ["inactive", "failed"]:
            return False
        return True

    def start(self, unit_name, **args):
        base_cmd = [self.BASE_CMD, "start", unit_name, config_dict_to_string(args)]
        out, _ = self.node.exec_command(cmd=" ".join(base_cmd), sudo=True)
        return out

    def stop(self, unit_name, **args):
        base_cmd = [self.BASE_CMD, "stop", unit_name, config_dict_to_string(args)]
        out, _ = self.node.exec_command(cmd=" ".join(base_cmd), sudo=True)
        return out

    def status(self, unit_name, **args):
        base_cmd = [self.BASE_CMD, "status", unit_name, config_dict_to_string(args)]
        out, _ = self.node.exec_command(cmd=" ".join(base_cmd), sudo=True)
        return out
