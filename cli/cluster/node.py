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

    def _get_osp_resources(self, **config):
        """Get Openstack available resources

        Kwargs:
            image (str): Image to be used for node
            size (int): Node root volume size
            network (str): Network to be attached to node
        """
        if self.id:
            msg = f"Node with name '{self.name}' already exists"
            log.error(msg)
            raise OperationFailedError(msg)

        # Get cloud image
        _image = self.cloud.get_image_by_name(config.get("image"))
        if not _image:
            msg = f"Image {config.get('image')} not available on cloud"
            log.error(msg)
            raise ResourceNotFoundError(msg)

        # Get cloud vm flavor
        _size = self.cloud.get_flavor_by_name(config.get("size"))
        if not _size:
            msg = f"VM size {config.get('size')} not available on cloud"
            log.error(msg)
            raise ResourceNotFoundError(msg)

        # Get network object
        if not config.get("networks"):
            config["networks"] = self._get_available_network()

        _network = self.cloud.get_network_by_name(config.get("networks"))
        if not _network:
            msg = f"Network {config.get('networks')} not available on cloud"
            log.error(msg)
            raise ResourceNotFoundError(msg)

        return {"image": _image, "size": _size, "networks": [_network]}

    def create(self, timeout=300, interval=10, **configs):
        """Create node on cloud

        Args:
            timeout (int): Operation waiting time in sec
            interval (int): Operation retry time in sec

        Kwargs:
            image (str): Image to be used for node
            size (int): Node root volume size
            cloud_data (dict): Configuration steps after deployment
            network (str): Network to be attached to node
            os_type (str): OS type
            os_version (str): OS version
        """
        # Get Openstack resources from Openstack
        if str(self.cloud) == "openstack":
            configs.update(self._get_osp_resources(**configs))

        # Create vm
        self.cloud.create_node(
            **configs, name=self._name, timeout=timeout, interval=interval
        )

        # Wait until private ips attached to node
        if self.cloud == "openstack":
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
