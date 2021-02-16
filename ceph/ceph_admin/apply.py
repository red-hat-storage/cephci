"""
Represent the ceph orch CLI action 'apply'.

Module to deploy ceph role service(s) using orchestration command
"ceph orch apply <role> [options] --placement '<placements>' "

This module inherited where service deployed using "apply" operation and also
validation using orchestration process list response
"""
from typing import Dict

from ceph.utils import get_nodes_by_id

from .common import config_dict_to_string
from .typing_ import ServiceProtocol


class ServiceApplyFailure(Exception):
    pass


class ApplyMixin:
    """Add apply support to the base class."""

    def apply(self: ServiceProtocol, config: Dict) -> None:
        """
        Execute the apply method using the object's service name and provided input.

        Args:
            config:     Key/value pairs passed from the test suite.
                        base_cmd_args   - key/value pairs to set for base command
                        pos_args        - List to be added as positional params
                        args            - Key/value pairs as optional arguments.

        Example:
            config:
                command: apply
                service: rgw
                base_cmd_args:          # arguments to ceph orch
                    concise: true
                    verbose: true
                    input_file: <name of spec>
                pos_args:               # positional arguments
                    - india             # realm
                    - south             # zone
                args:
                    placement:
                        label: rgw_south
                        nodes:              # A list of strings that would looked up
                            - node1
                        limit: 3            # no of daemons
                        sep: " "            # separator to be used for placements
                    dry-run: true
                    unmanaged: true
        """
        base_cmd = ["ceph", "orch"]

        if config.get("base_cmd_args"):
            base_cmd_args_str = config_dict_to_string(config.get("base_cmd_args"))
            base_cmd.append(base_cmd_args_str)

        base_cmd.append("apply")
        base_cmd.append(self.SERVICE_NAME)

        pos_args = config.get("pos_args")
        if pos_args:
            base_cmd += pos_args

        args = config.get("args")

        node_names = None
        verify_service = False
        placement = args.pop("placement", {})

        if placement:
            placement_str = "--placement="

            if "label" in placement:
                placement_str += f'"label:{placement["label"]}"'
                base_cmd.append(placement_str)

            if "nodes" in placement:
                verify_service = True
                nodes = placement.get("nodes")

                if "*" in nodes:
                    placement_str += '"*"'
                    node_names = [node.shortname for node in self.cluster.node_list]
                elif "[" in nodes:
                    placement_str += '"%s"' % nodes
                    verify_service = False
                else:
                    nodes_ = get_nodes_by_id(self.cluster, nodes)
                    node_names = [node.shortname for node in nodes_]

                    sep = placement.get("sep", " ")
                    node_str = f"{sep}".join(node_names)

                    limit = placement.pop("limit", None)
                    if limit:
                        placement_str += f"'{limit}{sep}{node_str}'"
                    else:
                        placement_str += f"'{node_str}'"

                base_cmd.append(placement_str)

            # Odd scenario when limit is specified but without nodes
            if "limit" in placement:
                base_cmd.append(f"--placement={placement.get('limit')}")

        # At this junction, optional arguments are left in dict
        if args:
            base_cmd.append(config_dict_to_string(args))

        self.shell(args=base_cmd)
        if not verify_service:
            return

        if not self.check_service_exists(
            service_name=self.SERVICE_NAME, ids=node_names
        ):
            raise ServiceApplyFailure

    def remove(self: ServiceProtocol, config: Dict) -> None:
        """
        Execute the remove method using the object's service name.

        Args:
            config:     Key/value pairs passed from the test suite.
                        pos_args        - List to be added as positional params

        Example:
            config:
                command: remove
                service: rgw
                base_cmd_args:
                    service_name: rgw.realm.zone
                    verify: true
        """
        base_cmd = ["ceph", "orch", "rm"]
        cmd_args = config.pop("base_cmd_args")
        service_name = cmd_args.pop("service_name")
        base_cmd.append(service_name)

        self.shell(args=base_cmd)

        verify = cmd_args.pop("verify", True)

        if verify:
            self.check_service(
                service_name=service_name,
                exist=False,
            )
