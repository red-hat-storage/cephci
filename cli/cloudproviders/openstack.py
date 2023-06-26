import socket

from libcloud.compute.providers import get_driver
from libcloud.compute.types import Provider

from cli.exceptions import CloudProviderError, ConfigError, UnexpectedStateError
from cli.utilities.waiter import WaitUntil
from utility.log import Log

log = Log(__name__)

TIMEOUT = 280
API_VERSION = "2.2"
STATE_PENDING = "pending"
STATE_RUNNING = "running"
STATE_AVAILABLE = "available"
STATE_DESTROY = "destroy"
STATE_DELETE = None


class Openstack:
    """Insterface for Openstack operations"""

    def __init__(self, api_version=API_VERSION, timeout=TIMEOUT, **config):
        """Initialize instance using provided details

        Args:
            api_version (str): API version
            timeout (int): Socket timeout

        **Kwargs:
            username (str): Name of user to be set for the session
            password (str): Password of user
            auth-url (str): Endpoint that can authenticate the user
            auth-version (str): Version to be used for authentication
            tenant-name (str): Name of user's project
            tenant-domain_id (str): id of user's project
            service-region (str): Realm to be used
            domain (str): Authentication domain to be used
        """
        socket.setdefaulttimeout(timeout)

        try:
            self._driver = get_driver(Provider.OPENSTACK)(
                config["username"],
                config["password"],
                ex_force_auth_url=config["auth-url"],
                ex_force_auth_version=config["auth-version"],
                ex_tenant_name=config["tenant-name"],
                ex_force_service_region=config["service-region"],
                ex_domain_name=config["domain"],
                ex_tenant_domain_id=config["tenant-domain-id"],
                api_version=api_version,
            )
        except KeyError:
            msg = "Insufficient config related to Cloud provider"
            log.error(msg)
            raise ConfigError()

        except Exception as e:
            log.error(e)
            raise CloudProviderError(e)

    def get_node_by_name(self, name):
        """Get node object by name

        Args:
            name (str): Name of node
        """
        try:
            nodes = [node for node in self._driver.list_nodes() if node.name == name]
        except Exception as e:
            log.error(e)
            raise CloudProviderError(e)

        return nodes[0] if nodes else None

    def get_node_id(self, node):
        """Get node id

        Args:
            node (OpenstackNode): Openstack node object
        """
        try:
            return node.id
        except Exception as e:
            log.error(e)
            raise CloudProviderError(e)

    def get_node_state_by_id(self, id):
        """Get node status by id

        Args:
            id (str): id of node
        """
        try:
            node = self._driver.ex_get_node_details(id)
            return node.state if node else None
        except Exception as e:
            log.error(e)
            raise CloudProviderError(e)

    def get_nodes_by_prefix(self, prefix):
        """Get list of nodes by prefix

        Args:
            prefix (str): Node name prefix
        """
        try:
            return [
                (n.name, n.id) for n in self._driver.list_nodes() if prefix in n.name
            ]
        except Exception as e:
            log.error(e)
            raise CloudProviderError(e)

    def get_node_volumes(self, node):
        """Get list of storage volume ids attached to node

        Args:
            node (OpenstackNode): Openstack node object
        """
        try:
            return [v.get("id") for v in node.extra.get("volumes_attached")]
        except Exception as e:
            log.error(e)
            raise CloudProviderError(e)

    def wait_for_node_state(self, node, state, timeout, interval):
        """Wait for node to be in state

        Args:
            node (OpenstackNode): Openstack node object
            state (str): Expected node state
            timeout (int): Operation waiting time in sec
            interval (int): Operation retry time in sec
        """
        id = self.get_node_id(node)
        log.info(f"Waiting for node '{id}' to be in '{state}' state")
        for w in WaitUntil(timeout=timeout, interval=interval):
            try:
                _state = self.get_node_state_by_id(id)
            except Exception as e:
                log.error(e)
                raise CloudProviderError(e)

            if _state == state:
                log.info(f"Node '{id}' is in exptected '{_state}' state")
                break

            msg = f"Node '{id}' is not in expected '{_state}' state, retry after {interval} sec"
            log.info(msg)

        if w.expired:
            msg = f"Node '{id}' is in unexpected state '{_state}'"
            log.error(msg)
            raise UnexpectedStateError(msg)

    def delete_node(self, node, timeout=300, interval=10):
        """Delete node from project

        Args:
            node (OpenstackNode): Openstack node object
            timeout (int): Operation waiting time in sec
            interval (int): Operation retry time in sec
        """
        try:
            self._driver.destroy_node(node)
        except Exception as e:
            log.error(e)
            raise CloudProviderError(e)

        self.wait_for_node_state(node, STATE_DELETE, timeout, interval)

    def get_volume_by_name(self, name):
        """Get node object using name

        Args:
            name (str): Node name
        """
        try:
            volumes = [v for v in self._driver.list_volumes() if name == v.name]
        except Exception as e:
            log.error(e)
            raise CloudProviderError(e)

        return volumes[0] if volumes else None

    def get_volumes_by_prefix(self, prefix):
        """Get list of volumes by prefix

        Args:
            prefix (str): Volume name prefix
        """
        try:
            return [
                (v.name, v.id) for v in self._driver.list_volumes() if prefix in v.name
            ]
        except Exception as e:
            log.error(e)
            raise CloudProviderError(e)

    def get_volume_id(self, volume):
        """Get volume id

        Args:
            node (OpenstackVolume): Openstack volume object
        """
        try:
            return volume.id
        except Exception as e:
            log.error(e)
            raise CloudProviderError(e)

    def get_volume_name_by_id(self, id):
        """Get volume id

        Args:
            node (OpenstackVolume): Openstack volume object
        """
        try:
            volume = self._driver.ex_get_volume(id)
            return volume.name if volume else None
        except Exception as e:
            log.error(e)
            raise CloudProviderError(e)

    def get_volume_by_id(self, id):
        """Get volume by id

        Args:
            id (str): Node id
        """
        try:
            return self._driver.ex_get_volume(id)
        except Exception as e:
            log.error(e)
            raise CloudProviderError(e)

    def get_volume_state_by_id(self, id):
        """Get volume status by id

        Args:
            id (str): Volume id
        """
        try:
            volume = self._driver.ex_get_volume(id)
            return volume.state if volume else None
        except Exception as e:
            log.error(e)
            raise CloudProviderError(e)

    def attach_volume(self, node, volume):
        """Attach volume to node

        Args:
            node (OpenstackNode): Openstack node object
            volume (OpenstackVolume): Openstack volume object
        """
        try:
            return self._driver.attach_volume(node=node, volume=volume)
        except Exception as e:
            log.error(e)
            raise CloudProviderError(e)

    def detach_volume(self, volume, timeout=300, interval=10):
        """Detach volume from node

        Args:
            volume (OpenstackVolume): Openstack volume object
            timeout (int): Operation waiting time in sec
            interval (int): Operation retry time in sec
        """
        try:
            self._driver.detach_volume(volume)
        except Exception as e:
            log.error(e)
            raise CloudProviderError(e)

        self.wait_for_volume_state(volume, STATE_AVAILABLE, timeout, interval)

    def destroy_volume(self, volume, timeout=300, interval=10):
        """Destroy volume from node

        Args:
            volume (OpenstackVolume): Openstack volume object
            timeout (int): Operation waiting time in sec
            interval (int): Operation retry time in sec
        """
        try:
            self._driver.destroy_volume(volume)
        except Exception as e:
            log.error(e)
            raise CloudProviderError(e)

        self.wait_for_volume_state(volume, STATE_DESTROY, timeout, interval)

    def wait_for_volume_state(self, volume, state, timeout, interval):
        """Wait for volume to be in state

        Args:
            volume (OpenstackVolume): Openstack volume object
            timeout (int): Operation waiting time in sec
            interval (int): Operation retry time in sec
        """
        id = self.get_volume_id(volume)
        log.info(f"Waiting for volume '{id}' to be in '{state}' state")
        for w in WaitUntil(timeout=timeout, interval=interval):
            try:
                _state = self.get_volume_state_by_id(id)
            except Exception as e:
                if f"404 Not Found Volume {id} could not be found" in str(e):
                    _state = STATE_DESTROY
                else:
                    log.error(e)
                    raise CloudProviderError(e)

            if _state == state:
                log.info(f"Volume '{id}' is in expected '{_state}' state")
                break

            log.info(
                f"Volume '{id}' is not in '{_state}' state, retry after {interval} sec"
            )

        if w.expired:
            msg = f"Volume '{id}' is in unexpected state '{_state}'"
            log.error(msg)
            raise UnexpectedStateError(msg)

    def get_node_public_ips(self, node):
        """Detach public IP address from node

        Args:
            node (OpenstackNode): Openstack node object
        """
        try:
            return node.public_ips
        except Exception as e:
            log.error(e)
            raise CloudProviderError(e)

    def wait_for_node_public_ips(self, node, timeout=300, interval=10):
        """Wait for node public IP address

        Args:
            node (OpenstackNode): Openstack node object
            timeout (int): Operation waiting time in sec
            interval (int): Operation retry time in sec
        """
        id = self.get_node_id(node)
        log.info(f"Waiting for node '{id}' public IP")
        for w in WaitUntil(timeout=timeout, interval=interval):
            if self.get_node_public_ips(node):
                msg = f"Public IP addresses {self.get_node_public_ips(node)} are assigned to node '{id}'"
                log.info(msg)
                break

            msg = f"No IP address assigned to node '{id}', retry after {interval} sec"
            log.info(msg)

        if w.expired:
            msg = f"No IP address assigned to node '{id}'"
            log.error(msg)
            raise UnexpectedStateError(msg)

    def detach_node_public_ips(self, node, ips):
        """Detach public IP address from node

        Args:
            node (OpenstackNode): Openstack node object
        """
        try:
            [self._driver.ex_detach_floating_ip_from_node(node, ip) for ip in ips]
            return True
        except Exception as e:
            log.error(e)
            raise CloudProviderError(e)

    def get_node_private_ips(self, node):
        """Detach public IP address from node

        Args:
            node (OpenstackNode): Openstack node object
        """
        try:
            return node.private_ips
        except Exception as e:
            log.error(e)
            raise CloudProviderError(e)

    def detach_node_private_ips(self, node, ips):
        """Detach private IP address from node

        Args:
            node (OpenstackNode): Openstack node object
        """
        try:
            [self._driver.ex_detach_floating_ip_from_node(node, ip) for ip in ips]
            return True
        except Exception as e:
            log.error(e)
            raise CloudProviderError(e)

    def wait_for_node_private_ips(self, node, timeout=300, interval=10):
        """Wait for node private IP address

        Args:
            node (OpenstackNode): Openstack node object
            timeout (int): Operation waiting time in sec
            interval (int): Operation retry time in sec
        """
        id = self.get_node_id(node)
        log.info(f"Waiting for node '{id}' private IP")
        for w in WaitUntil(timeout=timeout, interval=interval):
            if self.get_node_private_ips(node):
                msg = f"Private IP addresses {self.get_node_private_ips(node)} are assigned to node '{id}'"
                log.info(msg)
                break

            msg = f"No IP address assigned to node '{id}', retry after {interval} sec"
            log.info(msg)

        if w.expired:
            msg = f"No IP address assigned to node '{id}'"
            log.error(msg)
            raise UnexpectedStateError(msg)
