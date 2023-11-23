from cli.exceptions import OperationFailedError, ResourceNotFoundError
from utility.log import Log

log = Log(__name__)


class Node:
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

    def _get_available_network(self):
        """Get available network on cloud"""
        networks = self.cloud.get_networks()
        for n in networks:
            subnets = self.cloud.get_subnets_by_network_name(n)
            for subnet in subnets:
                free_ips = subnet.get("total_ips", 0) - subnet.get("used_ips", 0)
                if free_ips > 3:
                    return n

    def create(self, image, size, cloud_data, network=None, timeout=300, interval=10):
        """Create node on cloud

        Args:
            image (str): Image to be used for node
            size (int): Node root volume size
            cloud_data (dict): Configuration steps after deployment
            network (str): Network to be attached to node
            timeout (int): Operation waiting time in sec
            interval (int): Operation retry time in sec
        """
        if self.id:
            msg = f"Node with name '{self.name}' already exists"
            log.error(msg)
            raise OperationFailedError(msg)

        # Get cloud image
        _image = self.cloud.get_image_by_name(image)
        if not _image:
            msg = f"Image '{image}' not available on cloud"
            log.error(msg)
            raise ResourceNotFoundError(msg)

        # Get cloud vm flavor
        _size = self.cloud.get_flavor_by_name(size)
        if not _size:
            msg = f"VM size '{size}' not available on cloud"
            log.error(msg)
            raise ResourceNotFoundError(msg)

        # Get network object
        if not network:
            network = self._get_available_network()
        _network = self.cloud.get_network_by_name(network)
        if not _network:
            msg = f"Network '{network}' not available on cloud"
            log.error(msg)
            raise ResourceNotFoundError(msg)

        log.info(f"Attaching network '{network}' to node '{self.name}'")

        # Create vm on cloud
        self.cloud.create_node(
            self.name, _image, _size, cloud_data, [_network], timeout, interval
        )

        # Wait until private ips attached to node
        self.cloud.wait_for_node_private_ips(self.name, timeout, interval)

        return True

    def attach_volume(self, volume):
        """Attach node to volunme

        Args
            name (str|list|tuple): Volume name(s)
        """
        # Get node object using id
        node = self.cloud.get_node_by_id(self.id)

        # Attach volumes to node
        volumes = volume if type(volume) in (list, tuple) else [volume]
        for v in volumes:
            self.cloud.attach_volume(node, self.cloud.get_volume_by_name(v))

        return True

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
