"""Module to interface with the OpenStack API."""

import datetime
import socket
from time import sleep
from typing import Optional

from libcloud.compute.base import NodeImage, NodeSize
from libcloud.compute.drivers.openstack import OpenStackNetwork, StorageVolume
from libcloud.compute.providers import get_driver
from libcloud.compute.types import Provider

from utility.log import Log

logger = Log(__name__)

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


class VolumeOpError(Exception):
    pass


class CephVMNode(object):
    """OpenStack Instance object."""

    default_rhosd_network_names = [
        "provider_net_cci_12",
        "provider_net_cci_11",
        "provider_net_cci_9",
        "provider_net_cci_8",
        "provider_net_cci_7",
        "provider_net_cci_6",
        "provider_net_cci_5",
        "provider_net_cci_4",
    ]

    default_rhos01_network_names = [
        "shared_net_12",
        "shared_net_11",
        "shared_net_9",
        "shared_net_8",
        "shared_net_7",
        "shared_net_6",
        "shared_net_5",
        "shared_net_4",
    ]

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
        self.driver = self.get_driver()

        if "rhod-d" in self.auth_url:
            self.default_network_names = self.default_rhosd_network_names
        elif "rhos-01" in self.auth_url:
            self.default_network_names = self.default_rhos01_network_names
        else:
            self.default_network_names = None
            logger.info("No default config network set")

        self.create_node()

    def get_driver(self, **kw):
        return OpenStack(
            self.username,
            self.password,
            api_version=kw.get("api_version", "2.2"),
            ex_force_auth_url=self.auth_url,
            ex_force_auth_version=self.auth_version,
            ex_tenant_name=self.tenant_name,
            ex_force_service_region=self.service_region,
            ex_domain_name=self.domain_name,
            ex_tenant_domain_id=self.tenant_domain_id,
        )

    def _get_image(self, name: Optional[str] = None) -> NodeImage:
        """
        Return a NodeImage instance using the provided name or self.image_name.

        Args:
            name: (Optional), the name of the image to be retrieved.

        Return:
            NodeImage instance that is referenced by the image name.

        Raises:
            ResourceNotFound - when the named image resource does not exist in the given
                               OpenStack cloud.
        """
        name = self.image_name if name is None else name
        url = f"/v2/images?name={name}"
        object_ = self.driver.image_connection.request(url).object
        images = self.driver._to_images(object_, ex_only_active=False)

        if not images:
            raise ResourceNotFound(f"Failed to retrieve image with name: {name}")

        return images[0]

    def _get_vm_size(self, name: Optional[str] = None) -> NodeSize:
        """
        Return a NodeSize instance found using the provided name or self.vm_size.

        Args:
            name: (Optional), the name of the VM size to be retrieved.
                  Example:
                            m1.small, m1.medium or m1.large

        Return:
            NodeSize instance that is referenced by the vm size name.

        Raises:
            ResourceNotFound - when the named vm size resource does not exist in the
                               given OpenStack Cloud.
        """
        name = self.vm_size if name is None else name
        for flavor in self.driver.list_sizes():
            if flavor.name == name:
                return flavor

        raise ResourceNotFound(f"Failed to retrieve vm size with name: {name}")

    def _get_network_by_name(self, name: str) -> OpenStackNetwork:
        """
        Retrieve the OpenStackNetwork instance using the provided name.

        Args:
            name:   the name of the network.

        Returns:
            OpenStackNetwork instance referenced by the name.

        Raises:
            ResourceNotFound: when the named network resource does not exist in the
                              given OpenStack cloud
        """
        url = f"{self.driver._networks_url_prefix}?name={name}"
        object_ = self.driver.network_connection.request(url).object
        networks = self.driver._to_networks(object_)

        if not networks:
            raise ResourceNotFound(f"No network resource with name {name} found.")

        return networks[0]

    def _has_free_ip_addresses(self, net: OpenStackNetwork) -> bool:
        """
        Return True if the given network has more than 3 free ip addresses.

        This buffer of 3 free IPs is in place to avoid failures during node creation.
        As in OpenStack, the private IP request for allocation occurs towards the end
        of the workflow.

        When a subnet with free IPs is identified then it's CIDR information is
        assigned to self.subnet attribute on this object.

        Arguments:
            net:    The OpenStackNetwork instance to be checked for IP availability.

        Returns:
            True on success else False
        """
        url = f"/v2.0/network-ip-availabilities/{net.id}"
        resp = self.driver.network_connection.request(url)
        subnets = resp.object["network_ip_availability"]["subnet_ip_availability"]

        for subnet in subnets:
            free_ips = subnet["total_ips"] - subnet["used_ips"]

            if free_ips > 3:
                self.subnet = subnet["cidr"]
                return True

        return False

    def _get_network(self, name: Optional[str] = None) -> OpenStackNetwork:
        """
        Return the first available OpenStackNetwork with a free IP address to lease.

        This method will search a preconfigured list of network names and return the
        first one that has more than 3 IP addresses to lease. One can override the
        preconfigured list by specifying a single network name.

        Args:
            name: (Optional), the network name to be retrieved in place of the default
                              list of networks.

        Returns:
            OpenStackNetwork instance that has free IP addresses to lease.

        Raises:
            ResourceNotFound when there no suitable networks in the environment.
        """
        network_names = self.default_network_names if name is None else [name]

        for net in network_names:
            # Treating an exception as a soft error as it is possible to find another
            # suitable network from the list.
            try:
                os_net = self._get_network_by_name(name=net)

                if not self._has_free_ip_addresses(net=os_net):
                    continue

                return os_net
            except BaseException as be:  # noqa
                logger.warning(be)
                continue

        raise ResourceNotFound(f"No networks had free IP addresses: {network_names}.")

    def _create_vm_node(self) -> None:
        """Create the instance using the provided data."""
        logger.info("Starting to create VM with name %s", self.node_name)

        self.node = self.driver.create_node(
            name=self.node_name,
            image=self._get_image(),
            size=self._get_vm_size(),
            ex_userdata=self.cloud_data,
            networks=[self._get_network(self.vm_network)],
        )

        if self.node is None:
            raise NodeErrorState(f"Failed to create {self.node_name}")

    def _wait_until_vm_state_running(self):
        """Wait till the VM moves to running state."""
        timeout = datetime.timedelta(seconds=600)
        start_time = datetime.datetime.now()

        while True:
            sleep(5)
            node = self.driver.ex_get_node_details(self.node.id)

            if node.state == "running":
                end_time = datetime.datetime.now()
                duration = (end_time - start_time).total_seconds()
                logger.info(
                    "%s moved to running state in %d seconds.",
                    self.node_name,
                    int(duration),
                )
                break

            if node.state == "error":
                raise NodeErrorState(node.extra.get("fault", {}).get("message"))

            if datetime.datetime.now() - start_time > timeout:
                raise NodeErrorState(
                    'node {name} is in "{state}" state'.format(
                        name=self.node_name, state=node.state
                    )
                )

    def _wait_until_ip_is_known(self):
        """Retrieve the IP address of the instance."""
        timeout = datetime.timedelta(seconds=300)
        start_time = datetime.datetime.now()

        while True:
            sleep(5)
            self.ip_address = self.get_private_ip()

            if self.ip_address is not None:
                # Let's keep the ip_address as a string instead of bytes
                self.ip_address = self.ip_address.decode("ascii", "ignore")
                break

            if datetime.datetime.now() - start_time > timeout:
                raise GetIPError("Unable to get IP for {}".format(self.node_name))

    def _create_attach_volumes(self):
        """Create and attach the volumes."""
        logger.info(
            "Creating %d volumes with %sGiB storage for %s",
            self.no_of_volumes,
            self.size_of_disk,
            self.node_name,
        )

        for item in range(0, self.no_of_volumes):
            vol_name = f"{self.node_name}-vol-{item}"
            volume = self.driver.create_volume(self.size_of_disk, vol_name)

            if not volume:
                raise VolumeOpError(f"Failed to create volume with name {vol_name}")

            self.volumes.append(volume)

        for _vol in self.volumes:
            if not self._wait_until_volume_available(_vol):
                raise VolumeOpError(f"{_vol.name} failed to become available.")

        for _vol in self.volumes:
            if not self.driver.attach_volume(self.node, _vol):
                raise VolumeOpError("Unable to attach volume %s", _vol.name)

    def _wait_until_volume_available(self, volume: StorageVolume) -> bool:
        """Wait until the state of the StorageVolume is available."""
        tries = 0
        while True:
            sleep(3)
            tries += 1
            volume = self.driver.ex_get_volume(volume.id)

            if volume.state.lower() == "available":
                return True

            if "error" in volume.state.lower():
                logger.error("%s state is %s", volume.name, volume.state)
                break

            if tries > 10:
                logger.error("Max retries for %s reached.", volume.name)
                break

        return False

    def get_private_ip(self):
        """
        Workaround. self.node.private_ips returns empty list.
        """
        node_detail = self.driver.ex_get_node_details(self.node)
        private_ip = node_detail.private_ips[0].encode("ascii", "ignore")
        return private_ip

    def get_volume(self, name: str) -> StorageVolume:
        """Retrieve the StorageVolume instance using the given name."""
        url = f"/volumes?name={name}"
        object_ = self.driver.volumev2_connection.request(url).object
        volumes = object_.get("volumes")

        if not volumes:
            raise ResourceNotFound(f"Failed to retrieve volume with name: {name}")

        volume_info = volumes[0]
        return self.driver.ex_get_volume(volume_info.get("id"))

    def create_node(self):
        """Create the instance with the provided data."""
        try:
            self._create_vm_node()
            self._wait_until_vm_state_running()
            self._wait_until_ip_is_known()

            if self.no_of_volumes:
                self._create_attach_volumes()
        except (ResourceNotFound, NodeErrorState, GetIPError, VolumeOpError):
            raise
        except BaseException as be:  # noqa
            logger.error(be)
            raise NodeErrorState(
                f"Unknown exception occurred during creation of {self.node_name}"
            )

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
