"""Represents the ceph orch CLI action 'host maintenance'.

This module enables the user to place a host in maintenance mode
and exit maintenance on a host already in maintenance mode.

Example:
     Any host can be placed in maintenance using the following command::
        ceph orch host maintenance enter <hostname> --force
     Any host can be exited from maintenance mode using the following command::
        ceph orch host maintenance exit <hostname>

This module is inherited where hosts are placed or exited from maintenance using orchestrator.

"""
from json import loads
from time import sleep
from typing import Dict

from ceph.utils import get_node_by_id

from .common import config_dict_to_string
from .manager import Manager
from .orch import ResourceNotFoundError
from .typing_ import ServiceProtocol


class HostMaintenanceFailure(Exception):
    pass


class MaintenanceMixin:
    """Add Orch maintenance support to the base class."""

    def check_maintenance_status(self, op, node) -> bool:
        """Check ceph orchestrator status to verify pause and resume orch operations using ceph orch status command

        Args:
            op: operation after which host status is checked. ex: enter|exit
            node: CephNode for which status needs to be checked.

        Returns:
            true if the maintenance operation has been performed successfully on the given node, else false.
        """
        status = self.get_host_status(node.hostname)
        out, _ = self.shell(args=["ceph", "fsid"])
        fsid = out.strip()
        config = {
            "command": "ps",
            "base_cmd_args": {"format": "json"},
            "args": {"hostname": node.hostname},
        }
        out, _ = self.ps(config)
        daemons = loads(out)
        daemon_names = [
            f"ceph-{fsid}-{daemon['daemon_type']}-{daemon['daemon_id'].replace('.', '-')}"
            for daemon in daemons
        ]
        # you could try daemon["hostname"]
        retry_count = len(daemons)
        count = 0

        if op == "enter" and status == "maintenance":
            active_daemon = True
            while count < retry_count:
                sleep(30)
                stdout, stderr = node.exec_command(
                    sudo=True, cmd="podman ps --format json"
                )
                container_out = stdout.replace("\n", "")
                containers = loads(container_out) if container_out else list()
                if not containers:
                    active_daemon = False
                    break
                container_names = [container["Names"] for container in containers]
                if any(daemon in container_names for daemon in daemon_names):
                    count += 1
                else:
                    active_daemon = False
                    break
            return not bool(active_daemon)
        elif op == "exit" and status != "maintenance":
            inactive_daemon = True
            while count < retry_count:
                sleep(30)
                stdout, stderr = node.exec_command(
                    sudo=True, cmd="podman ps --format json"
                )
                container_out = stdout.replace("\n", "")
                containers = loads(container_out) if container_out else list()
                if not containers:
                    count += 1
                else:
                    container_names = [
                        container["Names"][0] for container in containers
                    ]
                    all_containers_exist = all(
                        daemon in container_names for daemon in daemon_names
                    )
                    if not all_containers_exist:
                        count += 1
                    else:
                        inactive_daemon = False
                        break
            return not bool(inactive_daemon)

        return False

    def __maintenance(self: ServiceProtocol, config: Dict, op: str) -> None:
        """Perform host maintenance operations using orchestrator

        Args:
            config(Dict):     Key/value pairs passed from the test suite.
                        pos_args(Dict)        - List to be added as positional params, in this case
                                                we add name of the node on which host maintenance is to be
                                                performed as a positional parameter. (Refer example below)
        Example::

            config:
                service: host
                command: enter
                verify: true
                args:
                    node: name of the node to be placed under maintenance
            op: operation to be performed enter/exit

        """
        cmd = ["ceph", "orch"]
        if config.get("base_cmd_args"):
            cmd.append(config_dict_to_string(config["base_cmd_args"]))

        cmd.append("host")
        cmd.append("maintenance")
        cmd.append(op)

        verify = config.pop("verify", True)
        args = config["args"]
        nodename = args.pop("node")
        if not nodename:
            raise HostMaintenanceFailure(
                "Node on which maintenance mode to be configured is not provided"
            )

        node = get_node_by_id(self.cluster, nodename)

        if not node:
            raise ResourceNotFoundError(f"No matching resource found: {nodename}")

        if not self.get_host(node.hostname):
            raise HostMaintenanceFailure(
                "The node specified for maintenance is not deployed in the cluster"
            )

        cmd.append(node.hostname)

        if op == "enter":
            manager = Manager(cluster=self.cluster, **config)
            if not manager.switch_active(node):
                raise HostMaintenanceFailure(
                    "Unable to switch active mgr to a node other than the input node"
                )
            cmd.append("--force")

        self.shell(args=cmd)

        if verify and not self.check_maintenance_status(op, node):
            raise HostMaintenanceFailure(
                f"The host maintenance operation {op} was not successful on the host {node.hostname}"
            )

    def enter(self: ServiceProtocol, config: Dict) -> None:
        """Place a host under maintenance using orchestrator

        Args:
            config(Dict):     Key/value pairs passed from the test suite.
                        pos_args(Dict)        - List to be added as positional params, in this case
                                                we add name of the node on which host maintenance is to be
                                                performed as a positional parameter. (Refer example below)
        Example::

            config:
                service: host
                command: enter
                verify: true
                args:
                    node: name of the node to be placed under maintenance

        """
        self.__maintenance(config, "enter")

    def exit(self: ServiceProtocol, config: Dict) -> None:
        """Exit maintenance mode on a host under maintenance using orchestrator

        Args:
            config(Dict):     Key/value pairs passed from the test suite.
                        pos_args(Dict)        - List to be added as positional params, in this case
                                                we add name of the node on which host maintenance is to be
                                                performed as a positional parameter. (Refer example below)
        Example::

            config:
                service: host
                command: exit
                verify: true
                args:
                    node: name of the node to be exited from maintenance

        """
        self.__maintenance(config, "exit")
