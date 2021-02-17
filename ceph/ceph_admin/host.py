"""Cephadm orchestration host operations."""
import json
import logging
from copy import deepcopy
from typing import Dict

from ceph.utils import get_node_by_id, get_nodes_by_id

from .common import config_dict_to_string
from .orch import Orch, ResourceNotFoundError

logger = logging.getLogger(__name__)


class NodeOperationFailed(Exception):
    pass


class Host(Orch):
    """Interface for executing ceph host CLI."""

    SERVICE_NAME = "host"

    def list(self):
        """
        Return the list of hosts known ceph.

        Returns:
            json output of hosts list
        """
        return self.shell(
            args=["ceph", "orch", "host", "ls", "--format=json"],
        )

    def add(self, config: Dict) -> None:
        """
        Add host(s) to cluster.

        - Attach host to cluster
        - if nodes are empty, all nodes in cluster are considered
        - add_label: host added with labels(roles assigned are considered)
        - attach_address: host added with ip address(host ip address used)
        - by default add_label and attach_address are set to True

        Args:
            config: Key/value pairs passed from the test case.

        Example:
            config:
                command: add
                service: host
                base_cmd_args:
                    concise: true
                    block: true
                args:
                    nodes:
                        - node1
                        - node2
                    attach_ip_address: true
                    labels: apply-all-labels      # To apply all the labels associated
        """
        cmd = ["ceph", "orch", "host"]

        if config.get("base_cmd_args"):
            base_cmd_args = config_dict_to_string(config["base_cmd_args"])
            cmd.append(base_cmd_args)

        args = config["args"]
        attach_address = args.get("attach_ip_address", True)

        if args.get("nodes"):
            nodes = get_nodes_by_id(self.cluster, args["nodes"])
        else:
            nodes = self.cluster.get_nodes()

        cmd.append("add")

        for node in nodes:
            _cmd = deepcopy(cmd)
            _cmd.append(node.shortname)

            if attach_address:
                _cmd.append(node.ip_address)

            _labels = args.get("labels", [])
            if _labels:
                # second position args to command must be empty if no ip address
                if not attach_address:
                    _cmd.append("")

                if isinstance(_labels, str) and _labels == "apply-all-labels":
                    # create a unique list of tags... mainly OSD
                    label_set = set(node.role.role_list)
                    _labels = list(label_set)

                _cmd += _labels

            self.shell(args=_cmd)

            # validate host existence
            if node.shortname not in self.fetch_host_names():
                raise NodeOperationFailed(f"Failed to add host: {node.shortname}")

            if attach_address:
                if node.ip_address not in self.get_addr_by_name(node.shortname):
                    raise NodeOperationFailed(
                        f"Failed to add {node.ip_address} to host {node.shortname}."
                    )

            if _labels:
                if sorted(self.fetch_labels_by_hostname(node.shortname)) == sorted(
                    _labels
                ):
                    raise NodeOperationFailed(
                        f"Failed to apply labels to {node.shortname}"
                    )

    def remove(self, config):
        """
        Remove host(s) from cluster.

        - Removal of hosts from cluster with provided nodes
        - if nodes are empty, all cluster nodes are considered
        - Otherwise all node will be considered, except Installer node

        config:
            nodes: [node1.obj, node2.obj, ...]

        Args:
            config
        """
        base_cmd_args = config.get("base_cmd_args")
        nodes = get_nodes_by_id(self.cluster, base_cmd_args.get("nodes", []))
        logger.info("Remove nodes : %s", [node.ip_address for node in nodes])

        for node in nodes:
            # Skip Installer node and if it is already removed
            if node.shortname == self.installer.node.shortname:
                continue

            if node.shortname not in self.fetch_host_names():
                continue

            cmd = ["ceph", "orch", "host", "rm", node.shortname]
            self.shell(args=cmd)

            assert node.shortname not in self.fetch_host_names()

    def label_add(self, config):
        """
        Add/Attach label to nodes

        - Attach labels to existing nodes
        - if nodes are empty, all cluster nodes are considered
        - roles defined to each node used as labels( eg., [mon, mgr])

        config:
            node: node1
            labels:
                - mon
                - osd
        Args:
            config
        """
        base_cmd_args = config.get("base_cmd_args")
        node = get_node_by_id(self.cluster, base_cmd_args.get("node"))
        if not node:
            raise ResourceNotFoundError("%s Node not found" % node.shortname)

        _labels = base_cmd_args.get("labels")
        if isinstance(_labels, str) and _labels in "node_role_list":
            _labels = node.role.role_list

        logger.info("Add label(s) %s on node %s" % (_labels, node.ip_address))
        for label in _labels:
            self.shell(
                args=[
                    "ceph",
                    "orch",
                    "host",
                    "label",
                    "add",
                    node.shortname,
                    label,
                ],
            )
            assert label in self.fetch_labels_by_hostname(node.shortname)

    def label_remove(self, config):
        """
        Removes label from nodes

        - remove labels from existing nodes
        - if nodes are empty, all cluster nodes are considered

        config:
            node: node1
            labels:
                - mon
                - osd

        Args:
            config
        """
        base_cmd_args = config.get("base_cmd_args")
        node = get_node_by_id(self.cluster, base_cmd_args.get("node"))
        if not node:
            raise ResourceNotFoundError("%s Node not found" % node.shortname)

        _labels = base_cmd_args.get("labels")

        logger.info("Remove label(s) %s on node %s" % (_labels, node.ip_address))

        if isinstance(_labels, str) and _labels in "node_role_list":
            _labels = node.role.role_list

        if not _labels:
            raise ResourceNotFoundError("labels not found/provided")

        for label in _labels:
            self.shell(
                args=["ceph", "orch", "host", "label", "rm", node.shortname, label],
            )
            # BZ-1920979(cephadm allows duplicate labels attachment to node)
            # assert label not in self.fetch_labels_by_hostname(node.shortname)

    def set_address(self, config):
        """
        Set Ip address to node

        - Attach labels to existing nodes
        - if nodes are empty, all cluster nodes are considered
        - roles defined to each node used as labels( eg., [mon, mgr])

        config:
            nodes: [node1.obj, node2.obj, ...]

        Args:
            config
        """
        base_cmd_args = config.get("base_cmd_args")
        nodes = get_nodes_by_id(self.cluster, base_cmd_args.get("nodes", []))

        logger.info(
            "Set Address on this nodes : %s", [node.ip_address for node in nodes]
        )
        for node in nodes:
            self.shell(
                args=[
                    "ceph",
                    "orch",
                    "host",
                    "set-addr",
                    node.shortname,
                    node.ip_address,
                ],
            )
            assert node.ip_address in self.get_addr_by_name(node.shortname)

    def fetch_labels_by_hostname(self, node_name):
        """
        Fetch labels attach to a node by hostname
        Returns:
            labels: list of labels
        """
        nodes, _ = self.list()
        for node in json.loads(nodes):
            if node_name in node["hostname"]:
                return node["labels"]

        return []

    def fetch_host_names(self):
        """
        Returns host-names attached to a cluster
        Returns:
            list of host names
        """
        out, _ = self.list()
        return [i["hostname"] for i in json.loads(out)]

    def get_addrs(self):
        """
        Returns IP addresses of hosts attached to a cluster
        Returns:
            list of IP addresses
        """
        out, _ = self.list()
        return [i["addr"] for i in json.loads(out)]

    def get_addr_by_name(self, node_name):
        """
        Returns ip address of attached node using hostname

        Returns:
            ip_address: IP address of host name
        """
        out, _ = self.list()
        for node in json.loads(out):
            if node_name in node["hostname"]:
                return node["addr"]

        return None
