"""
Cephadm orchestration host operations
"""

import json
import logging

logger = logging.getLogger(__name__)


class HostMixin:
    """
    Cephadm orchestration host operations

    Supported operations
        - Host addition with ip address and labels
            (role labels will be attached)
        - Host removal
        - Attach labels to existing node
        - Set IP address to existing node
        - Listing the host with json format
    """

    def host_list(self):
        """
        List the cluster hosts
        Returns:
            json output of hosts list
        """
        out, _ = self.shell(
            remote=self.installer,
            args=["ceph", "orch", "host", "ls", "--format=json"],
        )
        return json.loads(out)

    def host_add(self, **config):
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
        nodes = config.get("nodes", self.cluster.get_nodes())
        nodes = nodes if isinstance(nodes, list) else [nodes]

        attach_address = config.get("attach_address", True)
        add_label = config.get("add_label", True)
        logger.info(
            "Adding nodes %s, (attach_address: %s, add_label: %s)"
            % ([node.ip_address for node in nodes], attach_address, add_label)
        )

        for node in nodes:
            cmd = ["ceph", "orch", "host", "add", node.shortname]
            if attach_address:
                cmd += [node.ip_address]
            if add_label:
                cmd += node.role.role_list

            # Add host
            self.shell(remote=self.installer, args=cmd)

            # validate host existence
            assert node.shortname in self.fetch_host_names()

            if attach_address:
                assert node.ip_address in self.get_addr_by_name(node.shortname)

            if add_label:
                assert sorted(self.fetch_labels_by_hostname(node.shortname)) == sorted(
                    node.role.role_list
                )

    def host_remove(self, **config):
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
        nodes = config.get("nodes", self.cluster.get_nodes())
        nodes = nodes if isinstance(nodes, list) else [nodes]
        logger.info("Remove nodes : %s", [node.ip_address for node in nodes])

        for node in nodes:
            # Skip Installer node and if it is already removed
            if node.shortname == self.installer.node.shortname:
                continue

            if node.shortname not in self.fetch_host_names():
                continue

            cmd = ["ceph", "orch", "host", "rm", node.shortname]
            self.shell(remote=self.installer, args=cmd)

            assert node.shortname not in self.fetch_host_names()

    def attach_label(self, **config):
        """
        Add/Attach label to nodes

        - Attach labels to existing nodes
        - if nodes are empty, all cluster nodes are considered
        - roles defined to each node used as labels( eg., [mon, mgr])

        config:
            nodes: [node1.obj, node2.obj, ...]

        Args:
            config
        """
        nodes = config.get("nodes", self.cluster.get_nodes())
        nodes = nodes if isinstance(nodes, list) else [nodes]
        logger.info(
            "Attach label on this nodes : %s", [node.ip_address for node in nodes]
        )

        for node in nodes:
            if node.shortname not in self.fetch_host_names():
                # Add host
                self.host_add(node)

            labels = node.role.role_list
            for label in labels:
                self.shell(
                    remote=self.installer,
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

    def remove_label(self, **config):
        """
        Removes label from nodes

        - remove labels from existing nodes
        - if nodes are empty, all cluster nodes are considered

        config:
            nodes: [node1.obj, node2.obj, ...]

        Args:
            config
        """
        nodes = config.get("nodes", self.cluster.get_nodes())
        nodes = nodes if isinstance(nodes, list) else [nodes]
        logger.info(
            "Remove label on this nodes : %s", [node.ip_address for node in nodes]
        )

        for node in nodes:
            if node.shortname not in self.fetch_host_names():
                continue

            labels = node.role.role_list
            for label in labels:
                self.shell(
                    remote=self.installer,
                    args=["ceph", "orch", "host", "label", "rm", node.shortname, label],
                )
                # BZ-1920979(cephadm allows duplicate labels attachment to node)
                # assert label not in self.fetch_labels_by_hostname(node.shortname)

    def set_address(self, **config):
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
        nodes = config.get("nodes", self.cluster.get_nodes())
        nodes = nodes if isinstance(nodes, list) else [nodes]
        logger.info(
            "Set Address on this nodes : %s", [node.ip_address for node in nodes]
        )
        for node in nodes:
            if node.shortname not in self.fetch_host_names():
                # Add host
                self.host_add(node)

            self.shell(
                remote=self.installer,
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
        nodes = self.host_list()
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
        return [i["hostname"] for i in self.host_list()]

    def get_addrs(self):
        """
        Returns IP addresses of hosts attached to a cluster
        Returns:
            list of IP addresses
        """
        return [i["addr"] for i in self.host_list()]

    def get_addr_by_name(self, node_name):
        """
        Returns ip address of attached node using hostname

        Returns:
            ip_address: IP address of host name
        """
        for node in self.host_list():
            if node_name in node["hostname"]:
                return node["addr"]
        return None
