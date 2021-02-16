"""Cephadm orchestration host operations."""
import logging

from ceph.utils import get_node_by_id, get_nodes_by_id

from .orch import Orch, ResourceNotFoundError

logger = logging.getLogger(__name__)


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
        Add host(s) to cluster

        - Attach host to cluster
        - if nodes are empty, all nodes in cluster are considered
        - add_label: host added with labels(roles assigned are considered)
        - attach_address: host added with ip address(host ip address used)
        - by default add_label and attach_address are set to True

        config:
            nodes: [node1.obj, node2.obj, ...]
            add_label: boolean
            attach_address: boolean

        Args:
            config
        """
        base_cmd_args = config.get("base_cmd_args")
        nodes = get_nodes_by_id(self.cluster, base_cmd_args.get("nodes", []))

        attach_address = base_cmd_args.get("attach_address", True)
        logger.info(
            "Adding nodes %s, (attach_address: %s, labels: %s)"
            % (
                [node.ip_address for node in nodes],
                attach_address,
                base_cmd_args.get("labels"),
            )
        )

        for node in nodes:
            cmd = ["ceph", "orch", "host", "add", node.shortname]
            if attach_address:
                cmd += [node.ip_address]

            _labels = base_cmd_args.get("labels", [])
            if isinstance(_labels, str) and _labels in "node_role_list":
                _labels = node.role.role_list

            cmd += _labels

            # Add host
            self.shell(args=cmd)

            # validate host existence
            assert node.shortname in self.fetch_host_names()

            if attach_address:
                assert node.ip_address in self.get_addr_by_name(node.shortname)

            if _labels:
                assert sorted(self.fetch_labels_by_hostname(node.shortname)) == sorted(
                    _labels
                )

    def remove(self, config):
        """
        Remove host(s) from cluster

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
        nodes = self.list()
        for node in nodes:
            if node_name in node["hostname"]:
                return node["labels"]
        return []

    def fetch_host_names(self):
        """
        Returns host-names attached to a cluster
        Returns:
            list of host names
        """
        return [i["hostname"] for i in self.list()]

    def get_addrs(self):
        """
        Returns IP addresses of hosts attached to a cluster
        Returns:
            list of IP addresses
        """
        return [i["addr"] for i in self.list()]

    def get_addr_by_name(self, node_name):
        """
        Returns ip address of attached node using hostname

        Returns:
            ip_address: IP address of host name
        """
        for node in self.list():
            if node_name in node["hostname"]:
                return node["addr"]
        return None
