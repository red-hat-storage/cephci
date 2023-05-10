import socket

from libcloud.compute.providers import get_driver
from libcloud.compute.types import Provider

from cli.exceptions import (
    CloudProviderError,
    ConfigError,
    ResourceNotFoundError,
    UnexpectedStateError,
)

TIMEOUT = 280
API_VERSION = "2.2"


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
            tenant-domain_id (str): ID of user's project
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
            raise ConfigError("Insufficient config related to Cloud provider")
        except Exception as e:
            raise CloudProviderError(e)

    def get_node_by_name(self, name):
        """Get node object by name

        Args:
            name (str): Name of node
        """
        nodes = [node for node in self._driver.list_nodes() if node.name == name]
        if nodes:
            return nodes[0]

    def get_node_state_by_id(self, id):
        """Get node status by ID

        Args:
            id (str): ID of node
        """
        node = self._driver.ex_get_node_details(id)
        if node:
            return node.state

    def get_nodes_by_prefix(self, prefix):
        """Get list of nodes by prefix

        Args:
            prefix (str): Prefix to be filtered for node name
        """
        nodes = [(n.name, n.id) for n in self._driver.list_nodes() if prefix in n.name]
        if nodes:
            return nodes

    def get_node_volumes(self, node):
        """Get list of storage volume ids attached to node

        Args:
            node (node): Node object
        """
        volumes = [v.get("id") for v in node.extra.get("volumes_attached", [])]
        if volumes:
            return volumes

    def get_node_public_ips(self, node):
        """Get public IP address of node

        Args:
            node (node): Node object
        """
        ips = node.public_ips
        if ips:
            return ips

    def get_node_private_ips(self, node):
        """Get public IP address of node

        Args:
            node (node): Node object
        """
        ips = node.private_ips
        if ips:
            return ips

    def get_volume_by_name(self, name):
        """Get node object using name

        Args:
            name (str): Name of node
        """
        volumes = [v for v in self._driver.list_volumes() if name == v.name]
        if volumes:
            return volumes[0]

    def get_volume_by_id(self, id):
        """Get volume by ID

        Args:
            id (str): ID of node
        """
        try:
            volume = self._driver.ex_get_volume(id)
        except Exception as e:
            raise CloudProviderError(
                f"Failed to get volume '{id}' with error -\n {str(e)}"
            )

        return volume

    def get_volumes_by_prefix(self, prefix):
        """Get list of volumes by prefix

        Args:
            prefix (str): Prefix to be filtered for volume name
        """
        volumes = [
            (v.name, v.id) for v in self._driver.list_volumes() if prefix in v.name
        ]
        if volumes:
            return volumes

    def delete_node_volumes(self, node):
        """Delete volumes attached to node

        Args:
            node (node): Node object
        """
        volumes = [v for v in node.extra.get("volumes_attached", [])]
        if not volumes:
            raise ResourceNotFoundError(
                f"No volumes are attached to node '{node.name}'"
            )

        for volume in volumes:
            self.delete_volume(self.get_volume_by_id(volume.get("id")))

    def detach_node_public_ips(self, node):
        """Detach public IP address from node

        Args:
            node (node): Node object
        """
        ips = self.get_node_public_ips(node)
        if not ips:
            raise ResourceNotFoundError(
                f"No Public IPs are attached to node '{node.name}'"
            )

        [self._driver.ex_detach_floating_ip_from_node(node, ip) for ip in ips]

    def detach_node_private_ips(self, node):
        """Detach private IP address from node

        Args:
            node (node): Node object
        """
        ips = self.get_node_private_ips(node)
        if not ips:
            raise ResourceNotFoundError(
                f"No Private IPs are attached to node '{node.name}'"
            )

        [self._driver.ex_detach_floating_ip_from_node(node, ip) for ip in ips]

    def delete_node(self, node):
        """Delete node

        Args:
            node (node): Node object
        """
        if node.state == "pending":
            raise UnexpectedStateError(
                f"'{node.name}' is in unexpected state 'pending'"
            )

        self._driver.destroy_node(node)

    def delete_volume(self, volume):
        """Delete volume

        Args:
            volume (volume): Volume object
        """
        try:
            self._driver.detach_volume(volume)
        except Exception as e:
            if "Cannot detach a root device volume" not in str(e):
                raise e
            return

        self._driver.destroy_volume(volume)
