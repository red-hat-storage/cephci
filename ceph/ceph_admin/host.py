"""Cephadm orchestration host operations."""
import json
import logging

from ceph.ceph import CephNode
from ceph.utils import get_node_by_id

from .orch import Orch, ResourceNotFoundError

logger = logging.getLogger(__name__)


class HostOpFailure(Exception):
    pass


class Host(Orch):
    """Interface for executing ceph host <options> operations."""

    SERVICE_NAME = "host"

    def list(self):
        """
        List the cluster hosts
        Returns:
            json output of hosts list
        """
        return self.shell(
            args=["ceph", "orch", "host", "ls", "--format=json"],
        )

    def add(self, config):
        """
        Add host to cluster
        - add_label: host added with labels(roles assigned are considered)
        - attach_address: host added with ip address(host ip address used)

        Args:
            config

        config:
            service: host
            command: add
            base_cmd_args:                          # arguments to ceph orch
              node: "node2"                         # node-name or object
              attach_address: bool                  # true or false
              labels: [mon, osd] or apply-all-labels

        labels are considered if list of strings are provided or all roles associated
        with node will be considered if string "apply-all-labels"

        """
        base_cmd_args = config.get("base_cmd_args", {})
        node = base_cmd_args.pop("node")

        if not isinstance(node, CephNode):
            node = get_node_by_id(self.cluster, node)

        if not node:
            raise ResourceNotFoundError("%s node not found/provided")

        attach_address = base_cmd_args.get("attach_address")
        _labels = base_cmd_args.get("labels")

        if isinstance(_labels, str) and _labels in "apply-all-labels":
            label_set = set(node.role.role_list)
            _labels = list(label_set)

        logger.info(
            "Adding node %s, (attach_address: %s, labels: %s)"
            % (node.ip_address, attach_address, _labels)
        )

        cmd = ["ceph", "orch", "host", "add", node.shortname]

        if attach_address:
            cmd.append(node.ip_address)

        if _labels:
            # To fill mandate <address> argument, In case attach_address is False
            if not attach_address:
                cmd.append("''")

            cmd += _labels

        # Add host
        self.shell(args=cmd)

        # validate host existence
        if node.shortname != self.fetch_host_names():
            raise HostOpFailure(f"Hostname verify failure. Expected {node.shortname}")

        if attach_address:
            if node.ip_address != self.get_addr_by_name(node.shortname):
                raise HostOpFailure(
                    f"IP address verify failed. Expected {node.ip_address}"
                )

        if _labels:
            assert sorted(self.fetch_labels_by_hostname(node.shortname)) == sorted(
                _labels
            )

    def add_hosts(self, config):
        """
        Add host(s) to cluster.

        - Attach host to cluster
        - if nodes are empty, all nodes in cluster are considered
        - add_label: host added with labels(roles assigned are considered)
        - attach_address: host added with ip address(host ip address used)
        - by default add_label and attach_address are set to True

        config:
            base_cmd_args:
                nodes: ["node1", "node2", ...]
                labels: [mon, osd] or apply-all-labels
                attach_address: boolean

        Args:
            config
        """
        base_cmd_args = config.get("base_cmd_args")
        nodes = base_cmd_args.pop("nodes", None)
        if not nodes:
            nodes = [node.hostname for node in self.cluster.get_nodes()]

        logger.info(
            "Adding nodes %s, (attach_address: %s, labels: %s)"
            % (nodes, base_cmd_args.get("attach_address"), base_cmd_args.get("labels"))
        )

        for node in nodes:
            config = {
                "base_cmd_args": {
                    "node": node,
                    "labels": base_cmd_args.get("labels", "apply-all-labels"),
                    "attach_address": base_cmd_args.get("attach_address", True),
                }
            }

            self.add(config)

    def remove(self, config):
        """
        Remove host from cluster

        Args:
            config

        config:
            service: host
            command: remove
            base_cmd_args:                          # arguments to ceph orch
              node: "node2"                         # node-name or object
        """
        base_cmd_args = config.get("base_cmd_args", {})
        node = base_cmd_args.pop("node")

        if not isinstance(node, CephNode):
            node = get_node_by_id(self.cluster, node)

        if not node:
            raise ResourceNotFoundError("%s node not found/provided")

        if (
            node.shortname == self.installer.node.shortname
            or node.shortname not in self.fetch_host_names()
        ):
            return

        logger.info("Removing node %s" % node.ip_address)
        cmd = ["ceph", "orch", "host", "rm", node.shortname]
        self.shell(args=cmd)
        assert node.shortname not in self.fetch_host_names()

    def remove_hosts(self, config):
        """
        Remove host(s) from cluster

        - Removal of hosts from cluster with provided nodes
        - if nodes are empty, all cluster nodes are considered
        - Otherwise all node will be considered, except Installer node

        config:
            base_cmd_args:
                nodes: ["node1", "node2", ...]

        Args:
            config
        """
        base_cmd_args = config.get("base_cmd_args", {})
        nodes = base_cmd_args.pop("nodes", None)
        if not nodes:
            nodes = [node.hostname for node in self.cluster.get_nodes()]

        logger.info("Remove nodes : %s" % nodes)

        for node in nodes:
            config = {
                "base_cmd_args": {
                    "node": node,
                }
            }
            self.remove(config)

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
        node = base_cmd_args.pop("node")

        if not isinstance(node, CephNode):
            node = get_node_by_id(self.cluster, node)

        _labels = base_cmd_args.get("labels")
        if isinstance(_labels, str) and _labels in "apply-all-labels":
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
        base_cmd_args = config.get("base_cmd_args", {})
        node = base_cmd_args.pop("node")

        if not isinstance(node, CephNode):
            node = get_node_by_id(self.cluster, node)

        _labels = base_cmd_args.get("labels")

        logger.info("Remove label(s) %s on node %s" % (_labels, node.ip_address))

        if isinstance(_labels, str) and _labels in "apply-all-labels":
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
        Set IP address to node
        - Attach address to existing nodes

        config:
            base_cmd_args:
                node: "node1.obj" or "node1"

        Args:
            config
        """
        base_cmd_args = config.get("base_cmd_args")
        node = base_cmd_args.pop("node")

        if not isinstance(node, CephNode):
            node = get_node_by_id(self.cluster, node)

        logger.info("Set Address on this node : %s" % node.ip_address)
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
