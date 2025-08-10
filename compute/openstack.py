"""Support VM lifecycle operation in an OpenStack Cloud."""

import socket
from datetime import datetime, timedelta
from time import sleep
from typing import List, Optional, Union
from uuid import UUID

from libcloud.compute.base import Node, NodeDriver, NodeImage, NodeSize
from libcloud.compute.drivers.openstack import (
    OpenStack_2_NodeDriver,
    OpenStackNetwork,
    StorageVolume,
)
from libcloud.compute.providers import get_driver
from libcloud.compute.types import Provider

from utility.log import Log

from .exceptions import (
    ExactMatchFailed,
    NetworkOpFailure,
    NodeDeleteFailure,
    NodeError,
    ResourceNotFound,
    VolumeOpFailure,
)

LOG = Log(__name__)

# libcloud does not have a timeout enabled for Openstack calls to
# ``create_node``, and it uses the default timeout value from socket which is
# ``None`` (meaning: it will wait forever). This setting will set the default
# to a magical number, which is 280 (4 minutes). This is 1 minute less than the
# timeouts for production settings that should allow enough time to handle the
# exception and return a response
socket.setdefaulttimeout(280)


def get_openstack_driver(**creds) -> Union[NodeDriver, OpenStack_2_NodeDriver]:
    """
    Return the client that can interact with the OpenStack cloud.

    Args:
        **creds: Key-value pairs that are required to authenticate the user.
        Required keys are:
            - username:          The username to authenticate with.
            - password:          The password to authenticate with.
            - auth_url:          The URL to authenticate against.
            - auth_version:      The authentication version to use.
            - tenant_name:       The name of the tenant to use.
            - service_region:    The region to use for the OpenStack services.
            - domain_name:       The domain name to use for authentication.
            - tenant_domain_id:  The domain ID of the tenant.
    Returns:
        An instance of NodeDriver or OpenStack_2_NodeDriver that can be used to
        interact with the OpenStack cloud.
    """
    openstack = get_driver(Provider.OPENSTACK)
    return openstack(
        creds["username"],
        creds["password"],
        api_version=creds.get("api_version", "2.2"),
        ex_force_auth_url=creds["auth_url"],
        ex_force_auth_version=creds["auth_version"],
        ex_tenant_name=creds["tenant_name"],
        ex_force_service_region=creds["service_region"],
        ex_domain_name=creds["domain_name"],
        ex_tenant_domain_id=creds["tenant_domain_id"],
    )


class CephVMNodeV2:
    """Represent the VMNode required for cephci."""

    default_rhosd_network_names = [
        "provider_net_cci_16",
        "provider_net_cci_15",
        "provider_net_cci_14",
        "provider_net_cci_13",
        "provider_net_cci_12",
        "provider_net_cci_11",
        "provider_net_cci_9",
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

    def __init__(
        self,
        os_cred: dict,
        node_name: Optional[str] = None,
    ) -> None:
        """
        Initialize the instance using the provided information.

        The co

        Args:
            os_cred:    Key-value pairs that are required to authenticate the user.
            node_name:      The name of the node to be retrieved.
        """
        self._os_cred = os_cred
        self.driver = get_openstack_driver(**os_cred)
        self.node: Optional[Node] = None

        # CephVM attributes
        self._subnet: list = list()
        self._roles: list = list()

        # Fixme: determine if we can pick this information for OpenStack.
        self.root_login: str
        self.osd_scenario: int
        self.keypair: Optional[str] = None

        if node_name:
            self.node = self._get_node(name=node_name)

        if "rhos-d" in self._os_cred["auth_url"]:
            self.default_network_names = self.default_rhosd_network_names
        elif "rhos-01" in self._os_cred["auth_url"]:
            self.default_network_names = self.default_rhos01_network_names
        else:
            self.default_network_names = None

        LOG.info(f"OSP Config network set used are - {self.default_network_names}")

    def create(
        self,
        node_name: str,
        image_name: str,
        vm_size: str,
        cloud_data: str,
        vm_network: Optional[Union[List, str]] = None,
        size_of_disks: int = 0,
        no_of_volumes: int = 0,
    ) -> None:
        """
        Create the instance with the provided data.

        Args:
            node_name:     Name of the VM.
            image_name:    Name of the image to use for creating the VM.
            vm_size:       Flavor to be used to create the VM
            vm_network:    Name of the network/s
            cloud_data:    The cloud-init configuration information
            size_of_disks: The storage capacity of the volumes
            no_of_volumes: The number of volumes to be attached.
        """
        LOG.info("Starting to create VM with name %s", node_name)
        try:
            image = self._get_image(name=image_name)
            vm_size = self._get_vm_size(name=vm_size)
            vm_network = self.get_network(vm_network)

            LOG.info(f"{node_name} networks: {[i.name for i in vm_network]}")

            self.node = self.driver.create_node(
                name=node_name,
                image=image,
                size=vm_size,
                ex_userdata=cloud_data,
                ex_config_drive=True,
                networks=vm_network,
            )

            self._wait_until_vm_state(target_state="running")

            if no_of_volumes:
                self._create_attach_volumes(no_of_volumes, size_of_disks)

        except (ResourceNotFound, NetworkOpFailure, NodeError, VolumeOpFailure):
            raise
        except BaseException as be:  # noqa
            LOG.error(be, exc_info=True)
            raise NodeError(f"Unknown error. Failed to create VM with name {node_name}")

        # Ideally, we should be able to use HEAD to check if self.node is stale or not
        # instead of pulling the node details always. As a workaround, the self.node
        # is assigned the latest information after create is complete.
        self.node = self.driver.ex_get_node_details(node_id=self.node.id)

    def delete(self) -> None:
        """Remove the VM from the given OpenStack cloud."""
        # Deleting of the node when in building or pending state will fail. We are
        # checking for pending state as BUILD & PENDING map to the same value in
        # libcloud module.
        if self.node is None:
            return

        # Gather the current details of the node.
        self.node = self.driver.ex_get_node_details(node_id=self.node.id)
        if self.node.state == "pending":
            raise NodeDeleteFailure(f"{self.node.name} cannot be deleted.")

        LOG.info("Removing the instance with name %s", self.node.name)
        for ip in self.floating_ips:
            self.driver.ex_detach_floating_ip_from_node(self.node, ip)

        # At this point self.node is stale
        for vol in self.volumes:
            try:
                self.driver.detach_volume(volume=vol)
                self.driver.destroy_volume(volume=vol)
            except BaseException as e:
                print(
                    f"Volume detach/deletion failed, exception hit is {e}, Proceeding with destroying {self.node}"
                )

        self.driver.destroy_node(self.node)
        self.node = None

    def get_private_ip(self) -> str:
        """Return the private IP address of the VM."""
        return self.node.private_ips[0] if self.node else ""

    def shutdown(self, wait: bool = False) -> None:
        """
        Gracefully power off the VM associated with this instance.
        Args:
            wait:   True if the method should wait until the VM is powered off.
        """
        try:
            LOG.info("Initiating shutdown of node: {}".format(self.node.name))
            self.driver.ex_stop_node(self.node)
            if wait:
                try:
                    self._wait_until_vm_state(target_state="stopped")
                except NodeError as e:
                    LOG.error(
                        "Error while waiting for VM to stop with error: {}".format(e)
                    )
                    raise
        except (ResourceNotFound, NetworkOpFailure, NodeError, VolumeOpFailure):
            LOG.error(
                "Error while initiating shutdown on node {}".format(self.node.name)
            )
            raise

    def power_on(self) -> None:
        """
        Power on the VM associated with this instance.
        """
        try:
            LOG.info(f"Powering on node: {self.node.name}")
            self.driver.ex_start_node(self.node)
            self._wait_until_vm_state(target_state="running")
        except (ResourceNotFound, NetworkOpFailure, NodeError, VolumeOpFailure):
            LOG.error(
                "Error while initiating powering on node {}".format(self.node.name)
            )
            raise

    def __getstate__(self) -> dict:
        """
        Prepare the object state for pickling.
        Sanitizes unserializable fields like drivers.
        """
        state = dict(self.__dict__)
        state["driver"] = None
        if "node" in state and hasattr(state["node"], "driver"):
            state["node"].driver = None
        return state

    def __setstate__(self, state) -> None:
        self.__dict__.update(state)

        self.driver = get_openstack_driver(**self._os_cred)

    # Private methods to the object
    def _get_node(self, name: str) -> Node:
        """
        Retrieve the Node object using the provided name.

        The artifacts that are retrieved are
          - volumes
          - ip address
          - hostname
          - node_name
          - subnet

        Args:
            name:   The name of the node whose details need to be retrieved.

        Return:
            Instance of the Node retrieved using the provided name.
        """
        url = f"/servers?name={name}"
        object_ = self.driver.connection.request(url).object
        servers = object_["servers"]

        if len(servers) != 1:
            raise ExactMatchFailed(
                f"Found none or more than one resource with name: {name}"
            )

        return self.driver.ex_get_node_details(servers[0]["id"])

    def _get_image(self, name: str) -> NodeImage:
        """
        Return a NodeImage instance using the provided name.

        Args:
            name: The name of the image to be retrieved.

        Return:
            NodeImage instance that is referenced by the image name.

        Raises:
            ExactMatchFailed - when the named image resource does not exist in the given
                               OpenStack cloud.
        """
        try:
            if UUID(hex=name):
                return self.driver.get_image(name)
        except ValueError:
            pass

        url = f"/v2/images?name={name}"
        object_ = self.driver.image_connection.request(url).object
        images = self.driver._to_images(object_, ex_only_active=False)

        if len(images) != 1:
            raise ExactMatchFailed(
                f"Found none or more than one image resource with name: {name}"
            )

        return images[0]

    def _get_vm_size(self, name: str) -> NodeSize:
        """
        Return a NodeSize instance found using the provided name.

        Args:
            name: The name of the VM size to be retrieved.
                  Example:
                            m1.small, m1.medium or m1.large

        Return:
            NodeSize instance that is referenced by the vm size name.

        Raises:
            ResourceNotFound - when the named vm size resource does not exist in the
                               given OpenStack Cloud.
        """
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
                self._subnet.append(subnet["cidr"])
                return True

        return False

    def get_network(
        self,
        name: Optional[Union[List, str]] = None,
    ) -> List[OpenStackNetwork]:
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
        default_network_count = 1
        if name:
            network_names = name if isinstance(name, list) else [name]
            default_network_count = len(network_names)
        else:
            network_names = self.default_network_names

        rtn_nets = list()
        for net in network_names:
            # Treating an exception as a soft error as it is possible to find another
            # suitable network from the list.
            try:
                os_net = self._get_network_by_name(name=net)

                if not self._has_free_ip_addresses(net=os_net):
                    continue

                rtn_nets.append(os_net)
                if len(rtn_nets) == default_network_count:
                    return rtn_nets

            except BaseException as be:  # noqa
                LOG.warning(be)
                continue

        raise ResourceNotFound(f"No networks had free IP addresses: {network_names}.")

    def _wait_until_vm_state(self, target_state: str, timeout: int = 1200):
        """
        Wait until the VM reaches the specified state.

        Args:
            target_state (str): The desired final state (e.g., 'running', 'stopped').
            timeout (int): Maximum wait time in seconds.
        """
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=timeout)

        node = None
        while end_time > datetime.now():
            sleep(5)
            node = self.driver.ex_get_node_details(self.node.id)

            if node.state == target_state:
                duration = (datetime.now() - start_time).total_seconds()
                LOG.info(
                    "%s moved to %s state in %d seconds.",
                    self.node.name,
                    target_state,
                    int(duration),
                )
                return

            if node.state == "error":
                msg = (
                    "Unknown Error"
                    if not node.extra
                    else node.extra.get("fault", {}).get("message", "Unknown Error")
                )
                raise NodeError(msg)

        raise NodeError(
            "{} did not reach {}. Final state: {}".format(
                node.name, target_state, node.state
            )
        )

    def _create_attach_volumes(self, no_of_volumes: int, size_of_disk: int) -> None:
        """
        Create and attach the volumes.

        This method creates the requested number of volumes and then checks if each
        volume has moved to available state. Once the volume has moved to available,
        then it is attached to the node.

        Args:
            no_of_volumes:  The number of volumes to be created.
            size_of_disk:   The storage capacity of the volume in GiB.
        """
        LOG.info(
            "Creating %d volumes with %sGiB storage for %s",
            no_of_volumes,
            size_of_disk,
            self.node.name,
        )
        volumes = list()

        for item in range(0, no_of_volumes):
            vol_name = f"{self.node.name}-vol-{item}"
            volume = self.driver.create_volume(size_of_disk, vol_name)

            if not volume:
                raise VolumeOpFailure(f"Failed to create volume with name {vol_name}")

            volumes.append(volume)

        for _vol in volumes:
            if not self._wait_until_volume_available(_vol):
                raise VolumeOpFailure(f"{_vol.name} failed to become available.")

        for _vol in volumes:
            if not self.driver.attach_volume(self.node, _vol):
                raise VolumeOpFailure("Unable to attach volume %s", _vol.name)

    def _wait_until_ip_is_known(self):
        """Retrieve the IP address of the VM node."""
        end_time = datetime.now() + timedelta(seconds=120)

        while end_time > datetime.now():
            self.node = self.driver.ex_get_node_details(self.node.id)

            if self.ip_address is not None:
                break

            sleep(5)

        raise NetworkOpFailure("Unable to get IP for {}".format(self.node.name))

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
                LOG.error("%s state is %s", volume.name, volume.state)
                break

            if tries > 10:
                LOG.error("Max retries for %s reached.", volume.name)
                break

        return False

    def _get_subnet_cidr(self, id_: str) -> str:
        """Return the CIDR information of the given subnet id."""
        url = f"{self.driver._subnets_url_prefix}/{id_}"
        object_ = self.driver.network_connection.request(url).object
        subnet = self.driver._to_subnet(object_)

        if not subnet:
            raise ResourceNotFound("No matching subnet found.")

        return subnet.cidr

    # properties
    @property
    def ip_address(self) -> str:
        """Return the private IP address of the node."""
        if self.node is None:
            return ""

        if self.node.public_ips:
            return self.node.public_ips[0]

        return self.node.private_ips[0]

    @property
    def floating_ips(self) -> List[str]:
        """Return the list of floating IP's"""
        return self.node.public_ips if self.node else []

    @property
    def hostname(self) -> str:
        """Return the hostname of the VM."""
        end_time = datetime.now() + timedelta(seconds=30)
        while end_time > datetime.now():
            try:
                name, _, _ = socket.gethostbyaddr(self.ip_address)

                if name is not None:
                    return name

            except socket.herror:
                break
            except BaseException as be:  # noqa
                LOG.warning(be)

            sleep(5)

        return self.node.name

    @property
    def volumes(self) -> List[StorageVolume]:
        """Return the list of storage volumes attached to the node."""
        if self.node is None:
            return []

        return [
            self.driver.ex_get_volume(vol["id"])
            for vol in self.node.extra.get("volumes_attached", [])
        ]

    @property
    def subnet(self) -> str:
        """Return the subnet information."""
        if self.node is None:
            return ""

        if self._subnet:
            return self._subnet[0]

        networks = self.node.extra.get("addresses")
        for network in networks:
            net = self._get_network_by_name(name=network)
            subnet_id = net.extra.get("subnets")
            self._subnet.append(self._get_subnet_cidr(subnet_id))

        # Fixme: The CIDR returned needs to be part of the required network.
        return self._subnet[0]

    @property
    def shortname(self) -> str:
        """Return the shortform of the hostname."""
        return self.hostname.split(".")[0]

    @property
    def no_of_volumes(self) -> int:
        """Return the number of volumes attached to the VM."""
        return len(self.volumes)

    @property
    def role(self) -> List:
        """Return the Ceph roles of the instance."""
        return self._roles

    @role.setter
    def role(self, roles: list) -> None:
        """Set the roles for the VM."""
        from copy import deepcopy

        self._roles = deepcopy(roles)

    @property
    def node_type(self) -> str:
        """Return the provider type."""
        return "openstack"
