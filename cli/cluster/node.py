from cli.cloudproviders import CloudProvider
from cli.exceptions import OperationFailedError
from utility.log import Log

log = Log(__name__)


class Node(CloudProvider):
    """Interface to perform node operations"""

    def __init__(self, name, cloud):
        """Initialize instance with provided details

        Args:
            name (str): Node name
            cloud (CloudProvider): CloudProvider object

        **kwargs:
            <key-val> for cloud credentials
        """
        self._cloud, self._name = cloud, name

    @property
    def cloud(self):
        """Cloud provider object"""
        return self._cloud

    @property
    def name(self):
        """Node name"""
        return self._name

    @property
    def id(self):
        """Node ID"""
        return self.cloud.get_node_id(self.name)

    @property
    def state(self):
        """Node state"""
        return self.cloud.get_node_state_by_name(self.name)

    @property
    def public_ips(self):
        """Public IPs attached to node"""
        return self.cloud.get_node_public_ips(self.name)

    @property
    def private_ips(self):
        """Private IPs attached to node"""
        return self.cloud.get_node_private_ips(self.name)

    @property
    def volumes(self):
        """Volume names attached to node"""
        return self.cloud.get_node_volumes(self.name)

    def delete(self, timeout=300, interval=10):
        """Delete node from cloud

        Args:
            timeout (int): Operation waiting time in sec
            interval (int): Operation retry time in sec
        """
        if not self.id:
            msg = f"Node with name '{self.name}' doesn't exists"
            log.error(msg)
            raise OperationFailedError(msg)

        if self.private_ips:
            log.info(
                f"Dettaching private IPs {', '.join(self.private_ips)} assigned to node '{self.name}'"
            )
            self.cloud.detach_node_private_ips(self.name, self.private_ips)

        if self.public_ips:
            log.info(
                f"Dettaching public IPs {', '.join(self.public_ips)} assigned to node '{self.name}'"
            )
            self.cloud.detach_node_public_ips(self.name, self.public_ips)

        self.cloud.delete_node(self.name, timeout, interval)

        return True
