from typing import Dict

from ceph.utils import get_node_by_id

from .common import config_dict_to_string
from .typing_ import DaemonProtocol


class AddMixin:
    """Mixin Class for orch daemon add"""

    def add(self: DaemonProtocol, config: Dict):
        """
        Execute the add method using the object's service name.

        Args:
            config (Dict): Key/value pairs passed from the test suite.

        Example::

            config:
                service: osd
                command: add
                base_cmd_args:
                    verbose: true
                args:
                    method: raw
                pos_args:
                    - node1
                    - /dev/vdb

        """
        service = config.pop("service")
        base_cmd = ["ceph", "orch"]
        base_cmd.extend(["daemon", "add", service])
        if config.get("base_cmd_args"):
            base_cmd.append(config_dict_to_string(config["base_cmd_args"]))

        pos_args = config["pos_args"]
        node = pos_args[0]
        host_id = get_node_by_id(self.cluster, node)
        host = host_id.hostname

        if service == "osd":
            base_cmd.extend([f"{host}:{','.join(pos_args[1:])}"])

        else:
            if pos_args:
                base_cmd += pos_args

            base_cmd.append(host)

        if config.get("args"):
            base_cmd.append(config_dict_to_string(config["args"]))

        out, _ = self.shell(base_cmd)
