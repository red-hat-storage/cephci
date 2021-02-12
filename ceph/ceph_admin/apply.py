"""
Module to deploy ceph role service(s) using orchestration command
"ceph orch apply <role> [options] --placement '<placements>' "

this module inherited where service deployed using "apply" operation.
and also validation using orchestration process list response
"""
import logging
from copy import deepcopy
from typing import Any, List, Optional

from ceph.utils import get_nodes_by_id

from .typing_ import ServiceProtocol

logger = logging.getLogger(__name__)
BASE_CMD = ["ceph", "orch", "apply"]


class ServiceApplyFailure(Exception):
    pass


class ApplyMixin:
    """Interact with ceph orch apply CLI."""

    def placement(
        self: ServiceProtocol,
        nodes: List[str],
        prefix_args: Optional[List[str]] = None,
        limit: Optional[int] = None,
        sep: Any = " ",
    ) -> None:
        """
        Execute ceph orch apply <service> --placement command with the provided inputs.

        Args:
            nodes:  The list of the nodes participating in this operation
            prefix_args:    The list of additional arguments to be added
            limit:  The number of daemons to be deployed
            sep:    The command supports blank space, ; & , as separators

        Raises:
            ServiceApplyFailure when the service cannot be initiated
        """
        cmd = deepcopy(BASE_CMD)
        cmd.append(self.SERVICE_NAME)

        if prefix_args:
            cmd += prefix_args

        cmd.append("--placement=")

        # * refers to deploy the service on all discovered nodes
        if "*" in nodes:
            cmd += nodes
            node_names = [node.shortname for node in self.cluster.node_list]
        else:
            nodes_ = get_nodes_by_id(self.cluster, nodes)
            node_names = [node.shortname for node in nodes_]

            item = ""
            if limit is not None:
                item = f"{limit} "

            item += f"{sep}".join(node_names)
            cmd.append(item)

        self.shell(args=cmd)

        if not self.check_service_exists(
            service_name=self.SERVICE_NAME, ids=node_names
        ):
            raise ServiceApplyFailure

    def label(
        self: ServiceProtocol, label: str, prefix_args: Optional[List[str]] = None
    ) -> None:
        """
        Execute the command ceph orch apply label.

        Args:
            label:          The name of the label to be applied.
            prefix_args:    The list of additional arguments to be added.
        """
        cmd = deepcopy(BASE_CMD)
        cmd.append(self.SERVICE_NAME)

        if prefix_args:
            cmd += prefix_args

        cmd.append("label:")
        cmd.append(label)

        self.shell(args=cmd)

    def apply(self, **config):
        """
        Execute the apply method using the object's service name and passed kwargs.

        Args:
            config: Below mentioned keys are supported.

        Keys: The keys supported by the service with apply. It can contain
            prefix_args:    The list of arguments to be passed to the service.

                            For example rgw
                            config:
                                command: apply
                                service: rgw
                                prefix_args:
                                    realm: india
                                    zone: south
                                args:
                                    label: rgw_south    # either label or node.
                                    nodes:
                                        - node1
                                    limit: 3    # no of daemons
                                    sep: " "    # separator to be used for placements

        """
        args = config["args"]

        if "label" in config:
            self.label(label=args["label"], prefix_args=config.get("prefix_args"))

        if "nodes" in config:
            self.placement(
                nodes=args["nodes"],
                prefix_args=config.get("prefix_args"),
                limit=args.get("limit"),
                sep=args.get("sep", " "),
            )
