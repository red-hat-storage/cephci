"""Module to interface with the OpenStack API."""
import datetime
import logging
import socket
from ssl import SSLError
from time import sleep
from typing import Optional

from libcloud.compute.base import NodeImage, NodeSize
from libcloud.compute.drivers.openstack import OpenStackNetwork
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


class OpenStackDriverError(Exception):
    pass


class VolumeAttachmentError(Exception):
    pass


class CephVMNode(object):
    """OpenStack Instance object."""

    def __init__(self, **kw):
        self.image_name = kw["image-name"]
        self.node_name = kw["node-name"]
        self.vm_size = kw["vm-size"]
        self.vm_network = kw.get("vm-network")
        self.role = kw["role"]
        self.no_of_volumes = None
        if kw.get("no-of-volumes"):
            self.no_of_volumes = kw["no-of-volumes"]
            self.size_of_disk = kw["size-of-disks"]
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
        self.driver = None
        self.driver_v2 = None

        self.driver = self.get_driver_v1()
        self.driver_v2 = self.get_driver_v2()

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

    def get_driver_v1(self):
        """
        Get Apache Libcloud OpenStack driver for nova version 1.1.

        https://libcloud.readthedocs.io/en/latest/compute/drivers/
        openstack.html#compute-1-0-api-version-older-installations

        Returns:
            OpenStack driver
        """
        return self.get_driver() if self.driver is None else self.driver

    def get_driver_v2(self):
        """
        Get Apache Libcloud OpenStack driver for nova version 2.0.

        https://libcloud.readthedocs.io/en/latest/compute/drivers/
        openstack.html#compute-2-0-api-version-current

        Returns:
            OpenStack driver
        """
        return self.driver_v2 if self.driver_v2 else self.get_driver(api_version="2.2")

    def _get_image(self) -> NodeImage:
        """Return the glance image reference."""
        images = self.driver_v2.list_images()
        for image in images:
            if image.name == self.image_name:
                return image
        raise ResourceNotFound("Image {} not found".format(self.image_name))

    def _get_flavor(self) -> NodeSize:
        """Return the flavor reference."""
        flavors = self.driver_v2.list_sizes()
        for flavor in flavors:
            if flavor.name == self.vm_size:
                return flavor
        raise ResourceNotFound("Flavor {} not found".format(self.vm_size))

    def _has_free_ip_address(self, network: str) -> bool:
        """
        Return True if the given network has an IPv4 Address to lease.

        Arguments:
            network: String, The network UUID

        Returns:
            bool - True on success else False

        Note: This method returns True only if the given network has more than 3
        """
        try:
            resp = self.driver_v2.network_connection.request(
                "/v2.0/network-ip-availabilities/{}".format(network),
            )
        except BaseException as be:
            logger.info(be)
            return False

        if resp.status != 200:
            logger.debug("Failed to gather the network details.")
            return False

        _total = resp.object.get("network_ip_availability", {}).get("total_ips")
        _used = resp.object.get("network_ip_availability", {}).get("used_ips")

        return (_total - _used) > 3

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

        _available_networks = self.driver_v2.ex_list_networks()

        for net in _networks:
            try:
                os_net = [n for n in _available_networks if n.name == net][0]

                if not self._has_free_ip_address(os_net.id):
                    logger.debug("%s does not meet the requirements", net)
                    continue

                self.subnet = [
                    s.cidr
                    for s in self.driver_v2.ex_list_subnets()
                    if s.id in os_net.extra.get("subnets")
                ][0]

                return os_net
            except TypeError:
                logger.warning("%s is not accessible.", net)
            except BaseException as be:  # noqa
                logger.warning("Something went wrong during network selection process.")
                logger.info(be)

        raise ResourceNotFound("No suitable network resource found.")

    def _create_vm_node(self) -> None:
        """Create the instance using the provided data."""
        try:
            logger.info("Instantiating VM with name %s", self.node_name)
            self.node = self.driver_v2.create_node(
                name=self.node_name,
                image=self._get_image(),
                size=self._get_flavor(),
                ex_userdata=self.cloud_data,
                networks=[self._get_network(self.vm_network)],
            )

            if self.node is None:
                raise NodeErrorState(
                    "Unable to create the instance {}".format(self.node_name)
                )

            logger.info("%s is created", self.node.name)
            return
        except SSLError:
            logger.error("Connection failed, probably a timeout was reached")
        except BaseException as be:  # noqa
            logger.error(be)

        raise NodeErrorState("Failed to create the instance {}".format(self.node_name))

    def _wait_until_vm_state_running(self):
        """Wait till the VM moves to running state."""
        timeout = datetime.timedelta(seconds=600)
        start_time = datetime.datetime.now()
        while True:
            logger.info("Waiting for %s state to be running ", self.node_name)
            sleep(15)

            _node = self.driver_v2.ex_get_node_details(self.node.id)

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
            sleep(10)
            self.ip_address = self.get_private_ip()

            if self.ip_address is not None:
                # Let's keep the ip_address as a string instead of bytes
                self.ip_address = self.ip_address.decode("ascii", "ignore")
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
                _vol = self.driver_v2.create_volume(
                    self.size_of_disk, "{}-vol-{}".format(self.node_name, item)
                )
                self.volumes.append(_vol)
            except BaseException as be:  # noqa
                logger.debug("Failed to create volume.")
                logger.debug(be)

        for _vol in self.volumes:
            self._wait_until_volume_available(_vol, maybe_in_use=True)

        for _vol in self.volumes:
            if not self.driver_v2.attach_volume(self.node, _vol):
                raise VolumeAttachmentError("Unable to attach volume %s", _vol.name)

            logger.info("Successfully attached %s to %s", _vol.name, self.node_name)

    def _wait_until_volume_available(self, volume, maybe_in_use=False):
        """
        Wait until a StorageVolume's state is "available".
        Set "maybe_in_use" to True in order to wait even when the volume is
        currently in_use. For example, set this option if you're recycling
        this volume from an old node that you've very recently
        destroyed.
        """
        ok_states = ["creating"]  # it's ok to wait if the volume is in this
        tries = 0
        if maybe_in_use:
            ok_states.append("in_use")
        logger.info("Volume: %s is in state: %s", volume.name, volume.state)

        while volume.state in ok_states:
            sleep(3)
            volume = self.get_volume(volume.name)
            tries = tries + 1
            if tries > 10:
                logger.info("Maximum amount of tries reached..")
                break
            if volume.state == "notfound":
                logger.error("no volume was found for: %s", volume.name)
                break
            logger.info("%s is ... %s", volume.name, volume.state)

        if volume.state != "available":
            # OVH uses a non-standard state of 3 to indicate an available
            # volume
            logger.info("Volume %s is %s (not available)", volume.name, volume.state)
            logger.info(
                "The volume %s is not available, but will continue anyway...",
                volume.name,
            )

        return True

    def get_private_ip(self):
        """
        Workaround. self.node.private_ips returns empty list.
        """
        node_detail = self.driver.ex_get_node_details(self.node)
        private_ip = node_detail.private_ips[0].encode("ascii", "ignore")
        return private_ip

    def get_volume(self, name):
        """ Return libcloud.compute.base.StorageVolume """
        driver = self.driver
        volumes = driver.list_volumes()
        try:
            return [v for v in volumes if v.name == name][0]
        except IndexError:
            raise RuntimeError("Unable to get volume")

    def create_node(self):
        """Create the instance with the provided data."""
        self._create_vm_node()
        self._wait_until_vm_state_running()
        self._wait_until_ip_is_known()

        if self.no_of_volumes:
            self._create_attach_volumes()

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
