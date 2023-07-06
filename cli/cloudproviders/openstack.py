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
        # Set default socket timeout
        socket.setdefaulttimeout(timeout)

        # Set default node and volume
        self._nodes, self._volumes = {}, {}

        # Connect to Openstack driver
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

    def get_nodes(self, refresh=False):
        """Get nodes available from cloud

        Args:
            refresh (bool): Option to reload
        """
        if refresh or not self._nodes:
            try:
                for n in self._driver.list_nodes():
                    self._nodes[n.name] = n.id
            except Exception as e:
                log.error(e)
                raise CloudProviderError(e)

        return self._nodes.keys()

    def get_nodes_by_prefix(self, prefix, refresh=False):
        """Get list of nodes by prefix

        Args:
            prefix (str): Node name prefix
            refresh (bool): Option to reload
        """
        if not self._nodes or refresh:
            self.get_nodes(refresh=True)

        return [n for n in self._nodes.keys() if prefix in n]

    def get_node_id(self, name):
        """Get node by id

        Args:
            name (str): Node name
        """
        if not self._nodes or name not in self._nodes.keys():
            self.get_nodes(refresh=True)

        return self._nodes.get(name)

    def get_node_by_id(self, id):
        """Get node by id

        Args:
            id (str): Node id
        """
        try:
            return self._driver.ex_get_node_details(id) if id else None
        except Exception as e:
            log.error(e)
            raise CloudProviderError(e)

    def get_node_by_name(self, name):
        """Get node object by name

        Args:
            name (str): Name of node
        """
        if not self._nodes or name not in self._nodes.keys():
            self.get_nodes(refresh=True)

        return self.get_node_by_id(self._nodes.get(name))

    def get_node_state_by_name(self, name):
        """Get node status by name

        Args:
            name (str): Name of node
        """
        if not self._nodes or name not in self._nodes.keys():
            self.get_nodes(refresh=True)

        node = self.get_node_by_id(self._nodes.get(name))

        return node.state if node else None

    def get_node_volumes(self, name):
        """Get list of storage volume ids attached to node

        Args:
            name (str): Name of node
        """
        node = self.get_node_by_name(name)
        if not node:
            return

        try:
            return [
                self.get_volume_by_id(volume.get("id")).name
                for volume in node.extra.get("volumes_attached")
            ]
        except Exception as e:
            log.error(e)
            raise CloudProviderError(e)

    def wait_for_node_state(self, name, state, timeout=300, interval=10):
        """Wait for node to be in state

        Args:
            name (str): Name of node
            state (str): Expected node state
            timeout (int): Operation waiting time in sec
            interval (int): Operation retry time in sec
        """
        id = self.get_node_id(name)

        log.info(f"Waiting for node '{name}' to be in '{state}' state")
        for w in WaitUntil(timeout, interval):
            node = self.get_node_by_id(id)
            if not node and state == STATE_DESTROY:
                _state = STATE_DESTROY

            else:
                try:
                    _state = node.state
                except Exception as e:
                    log.error(e)
                    raise CloudProviderError(e)

            if _state == state:
                log.info(f"Node '{name}' is in exptected '{state}' state")
                break

            msg = f"Node '{name}' is not in expected '{state}' state, retry after {interval} sec"
            log.info(msg)

        if w.expired:
            msg = f"Node '{name}' is in unexpected state '{_state}'"
            log.error(msg)
            raise UnexpectedStateError(msg)

    def delete_node(self, name, timeout=300, interval=10):
        """Delete node from project

        Args:
            name (str): Name of node
            timeout (int): Operation waiting time in sec
            interval (int): Operation retry time in sec
        """
        node = self.get_node_by_name(name)
        if not node:
            return

        try:
            self._driver.destroy_node(node)
        except Exception as e:
            log.error(e)
            raise CloudProviderError(e)

        self.wait_for_node_state(name, STATE_DESTROY, timeout, interval)

        del self._nodes[name]

        return True

    def get_volumes(self, refresh=False):
        """Get volumes available from cloud

        Args:
            refresh (bool): Option to reload
        """
        if refresh or not self._volumes:
            try:
                for v in self._driver.list_volumes():
                    self._volumes[v.name] = v.id
            except Exception as e:
                log.error(e)
                raise CloudProviderError(e)

        return self._volumes.keys()

    def get_volumes_by_prefix(self, prefix, refresh=False):
        """Get list of volumes by prefix

        Args:
            prefix (str): Volume name prefix
            refresh (bool): Option to reload
        """
        if not self._volumes or refresh:
            self.get_volumes(refresh=True)

        return [v for v in self._volumes.keys() if prefix in v]

    def get_volume_id(self, name):
        """Get volume by id

        Args:
            name (str): Volume name
        """
        if not self._volumes or name not in self._volumes.keys():
            self.get_volumes(refresh=True)

        return self._volumes.get(name)

    def get_volume_by_id(self, id):
        """Get volume by id

        Args:
            id (str): Volume id
        """
        try:
            return self._driver.ex_get_volume(id) if id else None
        except Exception as e:
            log.error(e)
            raise CloudProviderError(e)

    def get_volume_by_name(self, name):
        """Get volume by name

        Args:
            name (str): Name of volume
        """
        if not self._volumes or name not in self._volumes.keys():
            self.get_volumes(refresh=True)

        return self.get_volume_by_id(self._volumes.get(name))

    def get_volume_state_by_name(self, name):
        """Get volume status by name

        Args:
            name (str): Name of volume
        """
        if not self._volumes or name not in self._volumes.keys():
            self.get_volumes(refresh=True)

        volume = self.get_volume_by_id(self._volumes.get(name))

        return volume.state if volume else None

    def detach_volume(self, name, timeout=300, interval=10):
        """Detach volume from node

        Args:
            name (str): Name of volume
            timeout (int): Operation waiting time in sec
            interval (int): Operation retry time in sec
        """
        volume = self.get_volume_by_name(name)
        if not volume:
            return

        try:
            self._driver.detach_volume(volume)
        except Exception as e:
            log.error(e)
            raise CloudProviderError(e)

        self.wait_for_volume_state(name, STATE_AVAILABLE, timeout, interval)

        return True

    def destroy_volume(self, name, timeout=300, interval=10):
        """Destroy volume from node

        Args:
            name (str): Name of volume
            timeout (int): Operation waiting time in sec
            interval (int): Operation retry time in sec
        """
        volume = self.get_volume_by_name(name)
        if not volume:
            return

        try:
            self._driver.destroy_volume(volume)
        except Exception as e:
            log.error(e)
            raise CloudProviderError(e)

        self.wait_for_volume_state(name, STATE_DESTROY, timeout, interval)

        return True

    def wait_for_volume_state(self, name, state, timeout=300, interval=10):
        """Wait for volume to be in state

        Args:
            name (str): Name of volume
            state (str): Expected volume state
            timeout (int): Operation waiting time in sec
            interval (int): Operation retry time in sec
        """
        id = self.get_volume_id(name)

        log.info(f"Waiting for volume '{name}' to be in '{state}' state")
        for w in WaitUntil(timeout=timeout, interval=interval):
            try:
                _state = self.get_volume_by_id(id).state
            except Exception as e:
                if f"404 Not Found Volume {id} could not be found" in str(e):
                    _state = STATE_DESTROY
                else:
                    log.error(e)
                    raise CloudProviderError(e)

            if _state == state:
                log.info(f"Volume '{name}' is in expected '{state}' state")
                break

            log.info(
                f"Volume '{name}' is not in expected '{state}' state, retry after {interval} sec"
            )

        if w.expired:
            msg = f"Volume '{name}' is in unexpected state '{_state}'"
            log.error(msg)
            raise UnexpectedStateError(msg)

    def get_node_public_ips(self, name):
        """Detach public IP address from node

        Args:
            name (str): Name of name
        """
        node = self.get_node_by_name(name)
        if not node:
            return

        try:
            return node.public_ips
        except Exception as e:
            log.error(e)
            raise CloudProviderError(e)

    def wait_for_node_public_ips(self, name, timeout=300, interval=10):
        """Wait for node public IP address

        Args:
            name (str): Name of name
            timeout (int): Operation waiting time in sec
            interval (int): Operation retry time in sec
        """
        id = self.get_node_id(name)

        log.info(f"Waiting for node '{name}' public IP addresses")
        for w in WaitUntil(timeout, interval):
            try:
                public_ips = self.get_node_by_id(id).public_ips
            except Exception as e:
                log.error(e)
                raise CloudProviderError(e)

            if public_ips:
                log.info(
                    f"Public IP addresses {','.join(public_ips)} are assigned to node '{name}'"
                )
                break

            log.info(
                f"No IP address assigned to node '{name}', retry after {interval} sec"
            )

        if w.expired:
            msg = f"No IP address assigned to node '{name}'"
            log.error(msg)
            raise UnexpectedStateError(msg)

    def detach_node_public_ips(self, name, ips):
        """Detach public IP address from node

        Args:
            name (str): Name of name
            ips (list|tuple): List of IP adresses
        """
        node = self.get_node_by_name(name)
        if not node:
            return

        try:
            [self._driver.ex_detach_floating_ip_from_node(node, ip) for ip in ips]
            return True
        except Exception as e:
            log.error(e)
            raise CloudProviderError(e)

    def get_node_private_ips(self, name):
        """Detach public IP address from node

        Args:
            name (str): Name of name
        """
        node = self.get_node_by_name(name)
        if not node:
            return

        try:
            return node.private_ips
        except Exception as e:
            log.error(e)
            raise CloudProviderError(e)

    def detach_node_private_ips(self, name, ips):
        """Detach private IP address from node

        Args:
            name (str): Name of name
            ips (list|tuple): List of IP adresses
        """
        node = self.get_node_by_name(name)
        if not node:
            return

        try:
            [self._driver.ex_detach_floating_ip_from_node(node, ip) for ip in ips]
            return True
        except Exception as e:
            log.error(e)
            raise CloudProviderError(e)

    def wait_for_node_private_ips(self, name, timeout=300, interval=10):
        """Wait for node private IP address

        Args:
            name (str): Name of name
            timeout (int): Operation waiting time in sec
            interval (int): Operation retry time in sec
        """
        id = self.get_node_id(name)

        log.info(f"Waiting for node '{name}' private IP")
        for w in WaitUntil(timeout, interval):
            try:
                private_ips = self.get_node_by_id(id).private_ips
            except Exception as e:
                log.error(e)
                raise CloudProviderError(e)

            if private_ips:
                log.info(
                    f"Private IP addresses {','.join(private_ips)} are assigned to node '{name}'"
                )
                break

            log.info(
                f"No IP address assigned to node '{name}', retry after {interval} sec"
            )

        if w.expired:
            msg = f"No IP address assigned to node '{name}'"
            log.error(msg)
            raise UnexpectedStateError(msg)
