from cli.cloundproviders.openstack import Openstack
from cli.exceptions import ConfigError, ResourceNotFoundError
from utility.log import Log

log = Log(__name__)


class Node:
    """Interface to perform node operations"""

    def __init__(self, name, cloud="openstack", **config):
        """Initialize instance with provided details

        Args:
            name (str): Node name
            cloud (str): Cloud type [openstack|ibmc|baremetal]

        **kwargs:
            <key-val> for cloud credentials
        """
        self._cloud = Node._get_cloud(cloud, **config)
        self._node = self._cloud.get_node_by_name(name)
        self._id = self._node.id
        self._name = name

    def _create(self):
        """
        Creates the required set of nodes
        """
        return self._cloud.create_ceph_nodes()

    @staticmethod
    def _get_cloud(cloud, **config):
        """Get cloud object"""
        if cloud.lower() == "openstack":
            return Openstack(**config)
        elif cloud == "ibmc":
            pass
        else:
            raise ConfigError(f"Unsupported cloud provider '{cloud}'")

    @staticmethod
    def nodes(pattern, cloud="openstack", **config):
        """Get nodes with pattern"""
        _cloud = Node._get_cloud(cloud, **config)
        return _cloud.get_nodes_by_pattern(pattern)

    @property
    def name(self):
        """Node name"""
        return self._name

    @property
    def id(self):
        """Node ID"""
        return self._id

    def delete(self):
        """Delete node"""
        try:
            self._cloud.detach_node_private_ips(self._node)
        except ResourceNotFoundError as e:
            log.error(str(e))

        try:
            self._cloud.detach_node_public_ips(self._node)
        except ResourceNotFoundError as e:
            log.error(str(e))

        try:
            self._cloud.delete_node_volumes(self._node)
        except ResourceNotFoundError as e:
            log.error(str(e))

        self._cloud.delete_node(self._node)

    def volumes(self):
        """Get volumes attached to node"""
        return self._cloud.get_node_volumes(self._node)

    def state(self):
        """Get node state"""
        return self._cloud.get_node_state_by_id(self._id)

    def public_ips(self):
        """Get public attached to public IP address"""
        return self._cloud.get_node_public_ips(self._node)

    def private_ips(self):
        """Get public attached to private IP address"""
        return self._cloud.get_node_private_ips(self._node)
