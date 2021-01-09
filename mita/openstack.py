"""Module to interface with the OpenStack API."""
import datetime
import logging
import socket
from time import sleep
from typing import Optional

from libcloud.compute.base import NodeImage, NodeSize
from libcloud.compute.drivers.openstack import OpenStackNetwork, StorageVolume
from libcloud.compute.providers import get_driver
from libcloud.compute.types import Provider

logger = logging.getLogger(__name__)

# libcloud does not have a timeout enabled for Openstack calls to
# ``create_node``, and it uses the default timeout value from socket which is
# ``None`` (meaning: it will wait forever). This setting will set the default
# to a magical number, which is 280 (4 minutes). This is 1 minute less than the
# timeouts for production settings that should allow enough time to handle the
# exception and return a response
socket.setdefaulttimeout(280)

# FIXME
# At the time this example was written, https://nova-api.trystack.org:5443
# was using a certificate issued by a Certificate Authority (CA) which is
# not included in the default Ubuntu certificates bundle (ca-certificates).
# Note: Code like this poses a security risk (MITM attack) and that's the
# reason why you should never use it for anything else besides testing. You
# have been warned.
# signed cert installed : https://projects.engineering.redhat.com/browse/CID-2407
# libcloud.security.VERIFY_SSL_CERT = False

OpenStack = get_driver(Provider.OPENSTACK)


class InvalidHostName(Exception):
    pass


class NodeErrorState(Exception):
    pass


class GetIPError(Exception):
    pass


class ResourceNotFound(Exception):
    pass


class VolumeError(Exception):
    pass


class CephVMNode(object):
    """OpenStack Instance object."""

    def __init__(self, **kw):
        self.image_name = kw["image-name"]
        self.node_name = kw["node-name"]
        self.vm_size = kw["vm-size"]
        self.vm_network = kw.get("vm-network")
        self.role = kw["role"]
        self.no_of_volumes = kw.get("no-of-volumes", 0)
        self.size_of_disk = kw.get("size-of-disks") if self.no_of_volumes else 0
        self.cloud_data = kw["cloud-data"]
        self.username = kw["username"]
        self.password = kw["password"]
        self.auth_url = kw["auth-url"]
        self.auth_version = kw["auth-version"]
        self.tenant_name = kw["tenant-name"]
        self.service_region = kw["service-region"]
        self.keypair = kw["keypair"]
        self.root_login = kw["root-login"]
        self.domain_name = kw["domain"]
        self.tenant_domain_id = kw["tenant-domain-id"]

        # Lazy initialization
        self.node = None
        self.hostname = self.node_name
        self.ip_address = None
        self.subnet = None
        self.floating_ip = None
        self.volumes = list()
        self.driver = self.get_driver(api_version="2.2")

        self.create_node()

    def get_driver(self, **kw):
        return OpenStack(
            self.username,
            self.password,
            api_version=kw.get("api_version", "1.1"),
            ex_force_auth_url=self.auth_url,
            ex_force_auth_version=self.auth_version,
            ex_tenant_name=self.tenant_name,
            ex_force_service_region=self.service_region,
            ex_domain_name=self.domain_name,
            ex_tenant_domain_id=self.tenant_domain_id,
        )

    def _get_image_by_name(self) -> NodeImage:
        """Return the glance image reference."""
        logger.info("Gathering %s image details.", self.image_name)

        url = f"/v2/images?name={self.image_name}"
        _object = self.driver.image_connection.request(url).object
        images = self.driver._to_images(_object, ex_only_active=False)

        if len(images) != 1:
            raise ResourceNotFound(f"Exact match failed for {self.image_name}.")

        return images[0]

    def _get_flavor_by_name(self) -> NodeSize:
        """Return the flavor reference."""
        logger.info("Gathering %s flavor details.", self.vm_size)

        for flavor in self.driver.list_sizes():
            if flavor.name == self.vm_size:
                return flavor

        raise ResourceNotFound(f"No matching {self.vm_size}flavor found.")

    def _get_network_by_name(self, name: str) -> OpenStackNetwork:
        """
        Return the OpenStackNetwork object.

        Args:
            name:   The name of the network to be retrieved.

        Returns:
            An instance of OpenStackNetwork object.

        Raises:
            ResourceNotFound    when the object cannot be found.
        """
        logger.info("Retrieving %s network details.", name)

        url = f"{self.driver._networks_url_prefix}?name={name}"
        _object = self.driver.network_connection.request(url).object
        networks = self.driver._to_networks(_object)

        if len(networks) != 1:
            raise ResourceNotFound(f"Exact match failed for {name} network.")

        return networks[0]

    def _has_free_ip_address(self, net: OpenStackNetwork) -> bool:
        """
        Return True if the given network has an IPv4 Address to lease.

        Arguments:
            net_id: String, The network UUID

        Returns:
            bool - True on success else False

        Note: This method returns True only if the given network has more than 3
        """
        try:
            logger.info("Checking for free ip address pool in %s", net.name)

            url = f"/v2.0/network-ip-availabilities/{net.id}"
            resp = self.driver.network_connection.request(url)
            subnets = resp.object.get("network_ip_availability", {}).get(
                "subnet_ip_availability", []
            )

            for subnet in subnets:
                _free_ips = subnet.get("total_ips") - subnet.get("used_ips")

                if _free_ips > 3:
                    self.subnet = subnet.get("cidr")
                    return True

            return False
        except BaseException as be:  # noqa
            logger.warning(be)

        return False

    def _get_network(self, network: Optional[str] = None) -> OpenStackNetwork:
        """
        Return the provider network.

        Arguments:
            network: Network to be attached to the VM

        Returns:
            An OpenStackNetwork libcloud object

        Raises:
            GetIPError when there are no suitable networks
        """
        _networks = (
            [
                "provider_net_cci_8",
                "provider_net_cci_7",
                "provider_net_cci_6",
                "provider_net_cci_5",
                "provider_net_cci_4",
            ]
            if network is None
            else [network]
        )

        for net in _networks:
            # If retrieval fails, consider it as a soft error.
            try:
                os_net = self._get_network_by_name(net)

                if not self._has_free_ip_address(os_net):
                    logger.debug("%s has no free ip addresses", os_net.name)
                    continue

                return os_net
            except BaseException as be:  # noqa
                logger.debug(be)
                continue

        raise ResourceNotFound(f"No suitable network resource found.")

    def _create_vm_node(self) -> None:
        """Create the instance using the provided data."""
        logger.info("Starting to create VM node with name %s.", self.node_name)

        self.node = self.driver.create_node(
            name=self.node_name,
            image=self._get_image_by_name(),
            size=self._get_flavor_by_name(),
            ex_userdata=self.cloud_data,
            networks=[self._get_network(self.vm_network)],
        )

        if self.node is None:
            raise NodeErrorState(f"Failed to create {self.node_name}.")

    def _wait_until_vm_state_running(self):
        """Wait till the VM moves to running state."""
        timeout = datetime.timedelta(seconds=600)
        start_time = datetime.datetime.now()
        while True:
            logger.info("Waiting for %s state to be running ", self.node_name)
            sleep(5)

            _node = self.driver.ex_get_node_details(self.node.id)

            if _node.state == "running":
                logger.info("%s is now in running state.", self.node.name)
                break

            if _node.state == "error":
                logger.error("Failed to create %s", _node.state)
                raise NodeErrorState(_node.extra.get("fault", {}).get("message"))

            if datetime.datetime.now() - start_time > timeout:
                logger.info("Failed to bring the node in running state in %s", timeout)
                raise NodeErrorState(
                    'node {name} is in "{state}" state'.format(
                        name=self.node_name, state=_node.state
                    )
                )

    def _wait_until_ip_is_known(self):
        """Retrieve the IP address of the instance."""
        timeout = datetime.timedelta(seconds=300)
        start_time = datetime.datetime.now()

        while True:
            logger.info("Gathering VM network address")
            sleep(5)
            self.ip_address = self.get_private_ip()

            if self.ip_address:
                break

            if datetime.datetime.now() - start_time > timeout:
                logger.info("Failed to get host ip_address in %s", timeout)
                raise GetIPError("Unable to get IP for {}".format(self.node_name))

    def _create_attach_volumes(self):
        """Create and attach the volumes."""
        for item in range(0, self.no_of_volumes):
            logger.info(
                "Creating %gb of storage for %s", self.size_of_disk, self.node_name
            )
            try:
                _vol = self.driver.create_volume(
                    self.size_of_disk, "{}-vol-{}".format(self.node_name, item)
                )
                self.volumes.append(_vol)
            except BaseException as be:  # noqa
                logger.debug("Failed to create volume.")
                logger.debug(be)

        for _vol in self.volumes:
            if not self._wait_until_volume_available(_vol):
                raise VolumeError(f"{_vol.name} state failed to be available.")

        for _vol in self.volumes:
            if not self.driver.attach_volume(self.node, _vol):
                raise VolumeError(f"Unable to attach volume {_vol.name}")

            logger.info("Successfully attached %s to %s", _vol.name, self.node_name)

    def _wait_until_volume_available(self, volume) -> bool:
        """Wait until a StorageVolume's state is "available"."""
        tries = 0
        while True:
            sleep(3)
            tries = tries + 1
            volume = self.driver.ex_get_volume(volume.id)
            logger.info("Volume: %s is in state: %s", volume.name, volume.state)

            if volume.state.lower() == "available":
                return True

            if "error" in volume.state:
                break

            if tries > 10:
                logger.info("Maximum amount of tries reached..")
                break

        return False

    def get_private_ip(self) -> str:
        """Retrieve the Private IP address"""
        _node = self.driver.ex_get_node_details(self.node)
        return _node.private_ips[0] if _node.private_ips else ""

    def get_volume(self, name) -> StorageVolume:
        """Retrieve a StorageVolume instance using the provided name."""
        url = f"/volumes?name={name}"
        _object = self.driver.volumev2_connection.request(url).object

        if len(_object.get("volumes", [])) != 1:
            raise ResourceNotFound(f"Exact match failed for volume with {name}.")

        volume_info = _object.get("volumes")[0]

        return self.driver.ex_get_volume(volume_info.get("id"))

    def create_node(self):
        """Create the instance with the provided data."""
        self._create_vm_node()
        self._wait_until_vm_state_running()
        self._wait_until_ip_is_known()

        if self.no_of_volumes:
            self._create_attach_volumes()

        logger.info("Successfully create VM node with name %s", self.node_name)

    def destroy_node(self):
        """
        Relies on the fact that names **should be** unique. Along the chain we
        prevent non-unique names to be used/added.
        TODO: raise an exception if more than one node is matched to the name, that
        can be propagated back to the client.
        """
        driver = self.driver
        driver.ex_detach_floating_ip_from_node(self.node, self.floating_ip)
        driver.destroy_node(self.node)
        sleep(15)
        for volume in self.volumes:
            driver.destroy_volume(volume)

    def destroy_volume(self, name):
        driver = self.driver
        volume = self.get_volume(name)
        # check to see if this is a valid volume
        if volume.state != "notfound":
            logger.info("Destroying volume %s", name)
            driver.destroy_volume(volume)

    def attach_floating_ip(self, timeout=120):
        pool = self.driver.ex_list_floating_ip_pools()[0]
        self.floating_ip = pool.create_floating_ip()
        self.ip_address = self.floating_ip.ip_address

        timeout = datetime.timedelta(seconds=timeout)
        start_time = datetime.datetime.now()
        logger.info("Gather hostname within %s seconds", timeout)
        while True:
            try:
                sleep(3)
                _name, _, _ = socket.gethostbyaddr(self.ip_address)

                if _name is not None:
                    self.hostname = _name
                    break
            except Exception:  # noqa
                if datetime.datetime.now() - start_time > timeout:
                    logger.info("Failed to get hostname in %s", timeout)
                    raise InvalidHostName("Invalid hostname for " + self.ip_address)

        logger.info("ip: %s and hostname: %s", self.ip_address, self.hostname)
        self.driver.ex_attach_floating_ip_to_node(self.node, self.floating_ip)
