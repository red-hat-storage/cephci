from cli.cloudproviders import CloudProvider
from cli.exceptions import OperationFailedError
from utility.log import Log

log = Log(__name__)


class Node(CloudProvider):
    """Interface to perform node operations"""

    def __init__(self, name, cloud="openstack", **config):
        """Initialize instance with provided details

        Args:
            name (str): Node name
            cloud (str): Cloud type [openstack|ibmc|baremetal]

        **kwargs:
            <key-val> for cloud credentials
        """
        super(Node, self).__init__(cloud, **config)

        self._node = self._cloud.get_node_by_name(name)
        self._id = self._cloud.get_node_id(self._node)

        self._name = name

    @property
    def name(self):
        """Node name"""
        return self._name

    @property
    def id(self):
        """Node ID"""
        return self._id

    @property
    def state(self):
        """Node state"""
        return self._cloud.get_volume_state_by_id(self.id) if self._node else None

    @property
    def public_ips(self):
        """Public IPs attached to node"""
        self._cloud.get_node_public_ips(self._node) if self._node else None

    @property
    def private_ips(self):
        """Private IPs attached to node"""
        self._cloud.get_node_private_ips(self._node) if self._node else None

    @property
    def volumes(self):
        """Volume names attached to node"""
        return [
            self._cloud.get_volume_name_by_id(id)
            for id in self._cloud.get_node_volumes(self._node)
        ]

    def delete(self, timeout=300, interval=10):
        """Delete node from cloud

        Args:
            timeout (int): Operation waiting time in sec
            interval (int): Operation retry time in sec
        """
        if not self._node:
            msg = f"Node with name '{self._name}' doesn't exists"
            log.error(msg)
            raise OperationFailedError(msg)

        if self.private_ips:
            log.info(
                f"Dettaching private IPs {self.private_ips} assigned to node '{self.name}'"
            )
            self._cloud.detach_node_private_ips(self._node)

        if self.public_ips:
            log.info(
                f"Dettaching public IPs {self.public_ips} assigned to node '{self.name}'"
            )
            self._cloud.detach_node_public_ips(self._node)

        self._cloud.delete_node(self._node, timeout, interval)

        self._node = None
        return True
