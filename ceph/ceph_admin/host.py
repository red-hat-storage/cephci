"""Cephadm orchestration host operations."""
import json
from copy import deepcopy

from ceph.ceph import CephNode
from ceph.utils import get_node_by_id
from utility.log import Log

from .common import config_dict_to_string
from .helper import monitoring_file_existence
from .maintenance import MaintenanceMixin
from .orch import Orch, ResourceNotFoundError

logger = Log(__name__)
DEFAULT_KEYRING_PATH = "/etc/ceph/ceph.client.admin.keyring"
DEFAULT_CEPH_CONF_PATH = "/etc/ceph/ceph.conf"


class HostOpFailure(Exception):
    pass


class Host(MaintenanceMixin, Orch):
    """Interface for executing ceph host <options> operations."""

    SERVICE_NAME = "host"

    def list(self):
        """
        List the cluster hosts

        Returns:
            json output of hosts list (List)

        """
        return self.shell(
            args=["ceph", "orch", "host", "ls", "--format=json"],
        )

    def add(self, config):
        """
        Add host to cluster

        Args:
            config (Dict):  host addition configuration

        Example::

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

            add_label: host added with labels(roles assigned are considered)
            attach_address: host added with ip address(host ip address used)

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

        # Skipping client node, if only client label is attached
        if (
            len(ceph_node.role.role_list) == 1
            and ["client"] == ceph_node.role.role_list
        ):
            return

        attach_address = args.get("attach_ip_address")
        _labels = args.get("labels")
        if isinstance(_labels, str) and _labels == "apply-all-labels":
            label_set = set(ceph_node.role.role_list)
            _labels = list(label_set)

        cmd.extend(["host", "add", ceph_node.hostname])

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
        if ceph_node.hostname not in self.fetch_host_names():
            raise HostOpFailure(
                f"Hostname verify failure. Expected {ceph_node.hostname}"
            )

        if attach_address:
            if ceph_node.ip_address != self.get_addr_by_name(ceph_node.hostname):
                raise HostOpFailure(
                    f"IP address verify failed. Expected {ceph_node.ip_address}"
                )

        if _labels:
            assert sorted(self.fetch_labels_by_hostname(ceph_node.hostname)) == sorted(
                _labels
            )

            if config.get("validate_admin_keyring") and "_admin" in _labels:
                if not monitoring_file_existence(ceph_node, DEFAULT_KEYRING_PATH):
                    raise HostOpFailure("Ceph keyring not found")
                if not monitoring_file_existence(ceph_node, DEFAULT_CEPH_CONF_PATH):
                    raise HostOpFailure("Ceph configuration file not found")
                logger.info("Ceph configuration and Keyring found")

    def add_hosts(self, config):
        """Add host(s) to cluster.

          - Attach host to cluster
          - if nodes are empty, all nodes in cluster are considered
          - add_label: host added with labels(roles assigned are considered)
          - attach_address: host added with ip address(host ip address used)
          - by default add_label and attach_address are set to True

        Args:
            config (Dict): hosts configuration

        Example::

            config:
                base_cmd_args:
                    concise: true
                    block: true
                args:
                    nodes: ["node1", "node2", ...]
                    labels: [mon, osd] or apply-all-labels
                    attach_ip_address: boolean

        """
        args = config.get("args")
        nodes = args.pop("nodes", None)

        if not nodes:
            nodes = [node.hostname for node in self.cluster.get_nodes()]

        for node in nodes:
            cfg = deepcopy(config)
            cfg["args"]["node"] = node

            self.add(cfg)

    def remove(self, config):
        """
        Remove host from cluster

        Args:
            config (Dict): Remove host configuration

        Example::

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
            node.hostname == self.installer.node.hostname
            or node.hostname not in self.fetch_host_names()
        ):
            return

        logger.info("Removing node %s" % node.ip_address)
        cmd.extend(["host", "rm", node.hostname])
        self.shell(args=cmd)
        assert node.hostname not in self.fetch_host_names()

    def remove_hosts(self, config):
        """
        Remove host(s) from cluster

          - Removal of hosts from cluster with provided nodes
          - if nodes are empty, all cluster nodes are considered
          - Otherwise all node will be considered, except Installer node

        Args:
            config (Dict): Remove hosts configuration

        Example::

            config:
                args:
                    nodes: ["node1", "node2", ...]

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

        Args:
            config (Dict): label add configuration

        Example::

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
            _cmd.extend(["host", "label", "add", node.hostname, label])
            self.shell(args=_cmd)
            assert label in self.fetch_labels_by_hostname(node.hostname)

            if config.get("validate_admin_keyring") and label == "_admin":
                logger.info("Ceph keyring - default: %s" % DEFAULT_KEYRING_PATH)
                if not monitoring_file_existence(node, DEFAULT_KEYRING_PATH):
                    raise HostOpFailure("Ceph keyring not found")
                if not monitoring_file_existence(node, DEFAULT_CEPH_CONF_PATH):
                    raise HostOpFailure("Ceph configuration file not found")
                logger.info("Ceph configuration and Keyring found on admin node...")

    def label_remove(self, config):
        """
        Removes label from nodes

          - remove labels from existing nodes
          - if nodes are empty, all cluster nodes are considered

        Args:
            config (Dict): label remove configuration

        Example::

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
            _cmd.extend(["host", "label", "rm", node.hostname, label])
            self.shell(args=_cmd)
            assert label not in self.fetch_labels_by_hostname(node.hostname)

            if config.get("validate_admin_keyring") and label == "_admin":
                logger.info("Ceph keyring - default: %s" % DEFAULT_KEYRING_PATH)
                if not monitoring_file_existence(
                    node, DEFAULT_KEYRING_PATH, file_exist=False
                ):
                    raise HostOpFailure("Ceph keyring found")
                if not monitoring_file_existence(
                    node, DEFAULT_CEPH_CONF_PATH, file_exist=False
                ):
                    raise HostOpFailure("Ceph configuration file found")
                logger.info("Ceph configuration and Keyring not found as expected")

    def set_address(self, config):
        """
        Set IP address to node

          - Attach address to existing nodes

        Args:
            config (Dict): set address configuration

        Example::

            config:
                service: host
                command: set_address
                base_cmd_args:
                    verbose: true
                args:
                    node: node1

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
        cmd.extend(["host", "set-addr", node.hostname, node.ip_address])
        self.shell(args=cmd)
        assert node.ip_address in self.get_addr_by_name(node.hostname)

    def fetch_labels_by_hostname(self, node_name):
        """
        Fetch labels attach to a node by hostname

        Returns:
            labels (List): list of labels
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
            list of host names (List)
        """
        out, _ = self.list()
        return [i["hostname"] for i in json.loads(out)]

    def get_addrs(self):
        """
        Returns IP addresses of hosts attached to a cluster

        Returns:
            list of IP addresses (List)
        """
        out, _ = self.list()
        return [i["addr"] for i in json.loads(out)]

    def get_addr_by_name(self, node_name):
        """
        Returns ip address of attached node using hostname

        Args:
            node_name (Str): node name

        Returns:
            ip_address (Str): IP address of host name
        """
        out, _ = self.list()
        for node in json.loads(out):
            if node_name in node["hostname"]:
                return node["addr"]

        return None

    def get_host(self, hostname: str):
        """Fetches the host with given hostname is deployed in the cluster.

        Args:
            hostname (str): hostname of the host to be verified

        Returns:
            host with the given hostname
        """
        out, _ = self.list()
        for node in json.loads(out):
            if hostname in node["hostname"]:
                return node
        raise AssertionError("Node not found")

    def get_host_status(self, hostname) -> str:
        """Fetches the status of the host with given hostname as present in ceph orch host ls output

        Args:
            hostname: hostname of the host whose status needs to be fetched

        Returns:
            status of the host as given in ceph orch host ls output
        """
        node = self.get_host(hostname)
        return node["status"]
