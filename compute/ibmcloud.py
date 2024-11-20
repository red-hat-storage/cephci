"""Support VM lifecycle operation in an OpenStack Cloud."""

import socket
from logging import getLogger
from typing import Optional

import SoftLayer

LOG = getLogger(__name__)

# libcloud does not have a timeout enabled for Openstack calls to
# ``create_node``, and it uses the default timeout value from socket which is
# ``None`` (meaning: it will wait forever). This setting will set the default
# to a magical number, which is 280 (4 minutes). This is 1 minute less than the
# timeouts for production settings that should allow enough time to handle the
# exception and return a response
socket.setdefaulttimeout(280)


def get_ibmcloud_client(
    username: str,
    password: str,
):
    """
    Return the client that can interact with the OpenStack cloud.

    Args:
        username:           The name of the user to be set for the session.
        password:           The password of the provided user.
        auth_url:           The endpoint that can authenticate the user.
        auth_version:       The API version to be used for authentication.
        tenant_name:        The name of the user's project.
        tenant_domain_id:   The ID of the user's project.
        service_region:     The realm to be used.
        domain_name:        The authentication domain to be used.
        api_version:        The API Version to be used for communication.
    """
    ibm_client = SoftLayer.create_client_from_env(username=username, api_key=password)
    return ibm_client


# Custom exception objects
class ResourceNotFound(Exception):
    pass


class ExactMatchFailed(Exception):
    pass


class VolumeOpFailure(Exception):
    pass


class NetworkOpFailure(Exception):
    pass


class NodeError(Exception):
    pass


class NodeDeleteFailure(Exception):
    pass


class CephVMNodeV2:
    """Represent the VMNode required for cephci."""

    def __init__(self, username: str, password: str, **kwargs) -> None:
        """
        Initialize the instance using the provided information.

        The co

        Args:
            username:   The name of the user to be set for the session.
            password:   The password of the provided user.
            domain_name:    The authentication domain to be used.
            node_name:      The name of the node to be retrieved.
        """
        self.driver = get_ibmcloud_client(username=username, password=password)
        self.node = None

        # CephVM attributes
        self._subnet: list = list()
        self._roles: list = list()

        # Fixme: determine if we can pick this information for OpenStack.
        self.root_login: str
        self.osd_scenario: int
        self.keypair: Optional[str] = None

        # if node_name:
        #     self.node = self._get_node(name=node_name)

    def create(
        self,
        node_name: str,
        image_name: str,
        size_of_disks: int = 0,
        no_of_volumes: int = 0,
        **kwargs,
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
            ibmcloud_client = SoftLayer.VSManager(self.driver)
            keys = SoftLayer.SshKeyManager(self.driver).list_keys()
            key_ids = [d["id"] for d in keys if d["label"] == "Amarnath"]
            new_vsi = {
                "domain": "cephci-QE.cloud",
                "hostname": node_name,
                "datacenter": "che01",
                "flavor": "B1_2X4X25",
                "dedicated": False,
                "private": True,
                "os_code": image_name,
                "hourly": True,
                "ssh_keys": key_ids,
                "local_disk": False,
                "tags": "test, pleaseCancel",
            }
            if no_of_volumes:
                size_of_disks = 25 if size_of_disks <= 25 else size_of_disks
                new_vsi["disks"] = [str(size_of_disks)] * no_of_volumes

            self.node = ibmcloud_client.create_instance(**new_vsi)
            ibmcloud_client.wait_for_ready(self.node["id"])
        except (ResourceNotFound, NetworkOpFailure, NodeError, VolumeOpFailure):
            raise
        except BaseException as be:  # noqa
            LOG.error(be, exc_info=True)
            raise NodeError(f"Unknown error. Failed to create VM with name {node_name}")
