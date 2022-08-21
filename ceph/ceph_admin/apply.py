"""
Represent the ceph orch CLI action 'apply'.

Module to deploy ceph role service(s) using orchestration command
"ceph orch apply <role> [options] --placement '<placements>' "

This is a mixin object and can be applied to the supported service classes.
"""
import re
from typing import Dict

from ceph.utils import get_nodes_by_ids

from .common import config_dict_to_string
from .helper import check_service_exists
from .typing_ import ServiceProtocol


class OrchApplyServiceFailure(Exception):
    pass


class ApplyMixin:
    """Add apply command support to child object."""

    def apply(self: ServiceProtocol, config: Dict) -> None:
        """
        Execute the apply method using the object's service name and provided input.

        Args:
            config (Dict):     Key/value pairs passed from the test suite.


        Example::

            base_cmd_args   - key/value pairs to set for base command
            pos_args        - List to be added as positional params
            args            - Key/value pairs as optional arguments.

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

        verify_service = False
        placement = args.pop("placement", {})

        if placement:
            placement_str = "--placement="
            sep = placement.get("sep", " ")
            node_names = list()
            verify_service = True

            if "label" in placement:
                node_names.append(f"label:{placement['label']}")

            if "nodes" in placement:
                nodes = placement.get("nodes")

                if "*" in nodes:
                    node_names.append("*")
                elif "[" in nodes:
                    placement_str += '"%s"' % nodes
                    verify_service = False
                else:
                    nodes_ = get_nodes_by_ids(self.cluster, nodes)
                    node_names = [node.hostname for node in nodes_]

            # Support RGW count-per-host placement option
            if "count-per-host" in placement and self.SERVICE_NAME == "rgw":
                node_names.append(f"count-per-host:{placement['count-per-host']}")

            node_str = f"{sep}".join(node_names)

            limit = placement.pop("limit", None)
            if limit:
                placement_str += f"'{limit}{sep}{node_str}'"
            else:
                placement_str += f"'{node_str}'"

            base_cmd.append(placement_str)

        # At this junction, optional arguments are left in dict
        if args:
            base_cmd.append(config_dict_to_string(args))

        out, err = self.shell(args=base_cmd)

        if not out:
            raise OrchApplyServiceFailure(self.SERVICE_NAME)

        # out value is "Scheduled <service_name> update..."
        service_name = re.search(r"Scheduled\s(.*)\supdate", out).group(1)

        if not verify_service:
            return

        if not check_service_exists(
            self.installer,
            service_name=service_name,
            service_type=self.SERVICE_NAME,
            rhcs_version=self.cluster.rhcs_version,
        ):
            raise OrchApplyServiceFailure(self.SERVICE_NAME)
