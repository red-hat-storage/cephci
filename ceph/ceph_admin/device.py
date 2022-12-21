"""CephADM orchestration Device operations."""
from time import sleep
from typing import Dict, Optional, Tuple

from ceph.utils import get_node_by_id
from utility.log import Log

from .common import config_dict_to_string
from .orch import Orch

log = Log(__name__)


class Device(Orch):
    def zap(self, config: Dict) -> None:
        """
        Zap particular device

        Args:
            config (Dict): Zap configs

        Returns:
            output (Str), error (Str)  returned by the command.

        Example::

            command: zap
                base_cmd_args:
                    verbose: true
                pos_args:
                    - "node1"
                    - "/dev/vdb"
                args:
                    force: true

        """
        base_cmd = ["ceph", "orch"]

        if config.get("base_cmd_args"):
            base_cmd_args_str = config_dict_to_string(config.get("base_cmd_args"))
            base_cmd.append(base_cmd_args_str)
        base_cmd.extend(["device", "zap"])

        pos_args = config["pos_args"]
        node = pos_args[0]
        host_id = get_node_by_id(self.cluster, node)
        host = host_id.hostname
        assert host
        base_cmd.append(host)
        base_cmd.extend(pos_args[1:])

        if config and config.get("args"):
            args = config.get("args")
            base_cmd.append(config_dict_to_string(args))
        return self.shell(args=base_cmd)

    def ls(self, config: Optional[Dict] = None) -> Tuple:
        """
        lists out devices in cluster

        Args:
            config (Dict): device list command configuration

        Configuration-Example::

            config:
                command: ls
                base_cmd_args:
                    verbose: true

        Returns:
            device_list (List): list of nodes with available devices

        Example::

            Return all available devices using "orch device ls" command.
            device_list:
                node1: ["/dev/sda", "/dev/sdb"]
                node2: ["/dev/sda"]
        """
        cmd = ["ceph", "orch"]

        if config and config.get("base_cmd_args"):
            cmd.append(config_dict_to_string(config["base_cmd_args"]))

        cmd.extend(["device", "ls"])

        if config and config.get("args"):
            args = config.get("args")
            cmd.append(config_dict_to_string(args))

        cmd.append("--refresh")
        log.info("Sleeping for 60 seconds for disks to be discovered")
        sleep(60)

        return self.shell(args=cmd)
