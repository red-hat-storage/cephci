from utility.log import Log

log = Log(__name__)


class Cluster:
    """Interface to perform cluster operations"""

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
