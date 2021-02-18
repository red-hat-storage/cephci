"""Cephadm orchestration host operations."""
import json
import logging
from copy import deepcopy

from ceph.ceph import CephNode
from ceph.utils import get_node_by_id

from .common import config_dict_to_string
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
                concise: true
                block: true
            args:
                node: "node2"                         # node-name or object
                attach_ip_address: bool               # true or false
                labels: [mon, osd] or apply-all-labels

        labels are considered if list of strings are provided or all roles associated
        with node will be considered if string "apply-all-labels"

        """
        cmd = ["ceph", "orch"]
        if config.get("base_cmd_args"):
            cmd.append(config_dict_to_string(config["base_cmd_args"]))

        args = config["args"]
        node = args.pop("node")
        ceph_node = get_node_by_id(self.cluster, node_name=node)

        if not ceph_node:
            raise ResourceNotFoundError(f"No matching resource found: {node}")

        attach_address = args.get("attach_ip_address")
        _labels = args.get("labels")

        if isinstance(_labels, str) and _labels == "apply-all-labels":
            label_set = set(ceph_node.role.role_list)
            _labels = list(label_set)

        cmd.extend(["host", "add", ceph_node.shortname])

        if attach_address:
            cmd.append(ceph_node.ip_address)

        if _labels:
            # To fill mandate <address> argument, In case attach_address is False
            if not attach_address:
                cmd.append("''")

            cmd += _labels

        logger.info(
            "Adding node %s, (attach_address: %s, labels: %s)"
            % (ceph_node.ip_address, attach_address, _labels)
        )
        # Add host
        self.shell(args=cmd)

        # validate host existence
        if ceph_node.shortname not in self.fetch_host_names():
            raise HostOpFailure(
                f"Hostname verify failure. Expected {ceph_node.shortname}"
            )

        if attach_address:
            if ceph_node.ip_address != self.get_addr_by_name(ceph_node.shortname):
                raise HostOpFailure(
                    f"IP address verify failed. Expected {ceph_node.ip_address}"
                )

        if _labels:
            assert sorted(self.fetch_labels_by_hostname(ceph_node.shortname)) == sorted(
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
                concise: true
                block: true
            args:
                nodes: ["node1", "node2", ...]
                labels: [mon, osd] or apply-all-labels
                attach_ip_address: boolean

        Args:
            config
        """
        args = config.get("args")
        nodes = args.pop("nodes", None)

        if not nodes:
            nodes = [node.shortname for node in self.cluster.get_nodes()]

        for node in nodes:
            cfg = deepcopy(config)
            cfg["args"]["node"] = node

            self.add(cfg)

    def remove(self, config):
        """
        Remove host from cluster

        Args:
            config

        config:
            service: host
            command: remove
            base_cmd_args:
              verbose: true                        # arguments to ceph orch
            args:
              node: "node2"                         # node-name or object
        """
        cmd = ["ceph", "orch"]
        if config.get("base_cmd_args"):
            cmd.append(config_dict_to_string(config["base_cmd_args"]))

        args = config["args"]
        node = args.pop("node")

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
        cmd.extend(["host", "rm", node.shortname])
        self.shell(args=cmd)
        assert node.shortname not in self.fetch_host_names()

    def remove_hosts(self, config):
        """
        Remove host(s) from cluster

        - Removal of hosts from cluster with provided nodes
        - if nodes are empty, all cluster nodes are considered
        - Otherwise all node will be considered, except Installer node

        config:
            args:
                nodes: ["node1", "node2", ...]

        Args:
            config
        """
        args = config.get("args", {})
        nodes = args.pop("nodes", None)
        if not nodes:
            nodes = [node.hostname for node in self.cluster.get_nodes()]

        logger.info("Remove nodes : %s" % nodes)

        for node in nodes:
            cfg = deepcopy(config)
            cfg["args"]["node"] = node
            self.remove(cfg)

    def label_add(self, config):
        """
        Add/Attach label to nodes

        - Attach labels to existing nodes
        - if nodes are empty, all cluster nodes are considered
        - roles defined to each node used as labels( eg., [mon, mgr])

        config:
            service: host
            command: label_add
            base_cmd_args:
                verbose: true
            args:
                node: node1
                labels:
                    - mon
                    - osd
        Args:
            config
        """
        cmd = ["ceph", "orch"]
        if config.get("base_cmd_args"):
            cmd.append(config_dict_to_string(config["base_cmd_args"]))

        args = config["args"]
        node = args.pop("node")
        if not isinstance(node, CephNode):
            node = get_node_by_id(self.cluster, node)

        if not node:
            raise ResourceNotFoundError("%s node not found/provided")

        _labels = args.get("labels")
        if isinstance(_labels, str) and _labels in "apply-all-labels":
            _labels = node.role.role_list

        if not _labels:
            raise ResourceNotFoundError("labels not found/provided")

        logger.info("Add label(s) %s on node %s" % (_labels, node.ip_address))
        for label in _labels:
            _cmd = deepcopy(cmd)
            _cmd.extend(["host", "label", "add", node.shortname, label])
            self.shell(args=_cmd)
            assert label in self.fetch_labels_by_hostname(node.shortname)

    def label_remove(self, config):
        """
        Removes label from nodes

        - remove labels from existing nodes
        - if nodes are empty, all cluster nodes are considered

        Args:
            config

        config:
            service: host
            command: label_remove
            base_cmd_args:
                verbose: true
            args:
                node: node1
                labels:
                    - mon
                    - osd
        """
        cmd = ["ceph", "orch"]
        if config.get("base_cmd_args"):
            cmd.append(config_dict_to_string(config["base_cmd_args"]))

        args = config["args"]
        node = args.pop("node")

        if not isinstance(node, CephNode):
            node = get_node_by_id(self.cluster, node)

        if not node:
            raise ResourceNotFoundError("%s node not found/provided")

        _labels = args.get("labels")

        if isinstance(_labels, str) and _labels in "apply-all-labels":
            _labels = node.role.role_list

        if not _labels:
            raise ResourceNotFoundError("labels not found/provided")

        logger.info("Remove label(s) %s on node %s" % (_labels, node.ip_address))

        for label in _labels:
            _cmd = deepcopy(cmd)
            _cmd.extend(["host", "label", "rm", node.shortname, label])
            self.shell(args=_cmd)
            # BZ-1920979(cephadm allows duplicate labels attachment to node)
            # assert label not in self.fetch_labels_by_hostname(node.shortname)

    def set_address(self, config):
        """
        Set IP address to node
        - Attach address to existing nodes

        config:
            service: host
            command: set_address
            base_cmd_args:
                verbose: true
            args:
                node: node1

        Args:
            config
        """
        cmd = ["ceph", "orch"]
        if config.get("base_cmd_args"):
            cmd.append(config_dict_to_string(config["base_cmd_args"]))

        args = config["args"]
        node = args.pop("node")

        if not isinstance(node, CephNode):
            node = get_node_by_id(self.cluster, node)

        if not node:
            raise ResourceNotFoundError("%s node not found/provided")

        logger.info("Set Address on this node : %s" % node.ip_address)
        cmd.extend(["host", "set-addr", node.shortname, node.ip_address])
        self.shell(args=cmd)
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
