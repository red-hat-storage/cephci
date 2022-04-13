"""IBM-Cloud VPC provider implementation for CephVMNode."""
import re
import socket
from copy import deepcopy
from datetime import datetime, timedelta
from time import sleep
from typing import Any, Dict, List, Optional

from ibm_cloud_networking_services import DnsSvcsV1
from ibm_cloud_networking_services.dns_svcs_v1 import (
    ResourceRecordInputRdataRdataARecord,
    ResourceRecordInputRdataRdataPtrRecord,
)
from ibm_cloud_sdk_core.api_exception import ApiException
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from ibm_vpc import VpcV1  # noqa
from requests.exceptions import ReadTimeout

from utility.log import Log
from utility.retry import retry

from .exceptions import NodeDeleteFailure, NodeError, ResourceNotFound

LOG = Log(__name__)


def get_ibm_service(access_key: str, service_url: str):
    """
    Return the authenticated connection from the given service_url.

    Args:
        access_key (str):   The access key(API key) of the user.
        service_url (str):  VPC endpoint to be used for provisioning.
    """
    authenticator = IAMAuthenticator(access_key)

    service = VpcV1(authenticator=authenticator)
    service.set_service_url(service_url=service_url)

    return service


def get_dns_service(
    access_key: str,
    service_url: Optional[str] = "https://api.dns-svcs.cloud.ibm.com/v1",
):
    """
    Return the authenticated connection from the given endpoint.
    Args:
        accessKey    The access key(API key) of the user.
    """
    authenticator = IAMAuthenticator(access_key)

    dnssvc = DnsSvcsV1(authenticator=authenticator)
    dnssvc.set_service_url(service_url=service_url)

    return dnssvc


def get_resource_id(resource_name: str, response: Dict) -> str:
    """
    Retrieve the ID of the given resource from the provided response.

    Args:
        resource_name (str):    Name of the resource.
        response (Dict):        DetailedResponse returned from the collections.

    Returns:
        Resource id (str)

    Raises:
        ResourceNotFound    when there is a failure to retrieve the ID.
    """
    return get_resource_details(resource_name, response)["id"]


def get_resource_details(resource_name: str, response: Dict) -> Dict:
    """
    Returns the details for the provided resource_name from the given collection.

    Args:
        resource_name (str):    Name of the resource.
        response (Dict):        DetailedResponse returned from the collections.

    Returns:
        Resource id (str)

    Raises:
        ResourceNotFound    when there is a failure to retrieve the ID.
    """
    resource_url = response["first"]["href"]
    resource_list_name = re.search(r"v1/(.*?)\?", resource_url).group(1)

    for i in response[resource_list_name]:
        if i["name"] == resource_name:
            return i

    raise ResourceNotFound(f"Failed to retrieve the ID of {resource_name}.")


def get_dns_zone_id(zone_name: str, response: Any) -> str:
    """
    Retrieve the DNS Zone ID for the provided zone name using the provided response.

    Args:
        zone_name (str):    DNS Zone whose ID needs to be retrieved.
        response (Dict):    Response returned from the collection.

    Returns:
        DNS Zone ID (str)

    Raises:
        ResourceNotFound    when there is a failure to retrieve the given zone ID.
    """
    for i in response["dnszones"]:
        if i["name"] == zone_name:
            return i["id"]

    raise ResourceNotFound(f"Failed to retrieve the ID of {zone_name}.")


class CephVMNodeIBM:
    """Represents a VMNode object created by softlayer driver."""

    def __init__(
        self,
        access_key: str,
        service_url: str,
        vsi_id: Optional[str] = None,
        node: Optional[Dict] = None,
    ) -> None:
        """
        Initializes the instance using the provided information.

        Args:
            access_key (str):   Service credential secret token
            service_url (str):  Endpoint of the service provider
            vsi_id (str):       The VSI node ID to be retrieved
            node (dict):
        """
        # CephVM attributes
        self._subnet: str = ""
        self._roles: list = list()
        self.node = None

        self.service = get_ibm_service(access_key=access_key, service_url=service_url)
        self.dns_service = get_dns_service(access_key=access_key)

        if vsi_id:
            self.node = self.service.get_instance(id=vsi_id).get_result()

        if node:
            self.node = node

    # properties

    @property
    def ip_address(self) -> str:
        """Return the private IP address of the node."""
        return self.node["primary_network_interface"]["primary_ipv4_address"]

    @property
    def floating_ips(self) -> List[str]:
        """Return the list of floating IP's"""
        if not self.node:
            return []

        resp = self.service.list_instance_network_interface_floating_ips(
            instance_id=self.node["id"],
            network_interface_id=self.node["primary_network_interface"]["id"],
        )

        return [
            x["address"] for x in resp.get("floating_ips") if x["status"] == "available"
        ]

    @property
    def public_ip_address(self) -> str:
        """Return the public IP address of the node."""
        return self.floating_ips[0]

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

        return self.node["name"]

    @property
    def volumes(self) -> List:
        """Return the list of storage volumes attached to the node."""
        if self.node is None:
            return []

        # Removing boot volume from the list
        volume_attachments = []
        for vol in self.node["volume_attachments"]:
            if self.node["name"] in vol["volume"]["name"]:
                volume_attachments.append(vol)
        return volume_attachments

    @property
    @retry(ReadTimeout, tries=5, delay=15)
    def subnet(self) -> str:
        """Return the subnet information."""
        if self._subnet:
            return self._subnet

        subnet_details = self.service.get_subnet(
            self.node["primary_network_interface"]["subnet"]["id"]
        )

        return subnet_details.get_result()["ipv4_cidr_block"]

    @property
    def shortname(self) -> str:
        """Return the short form of the hostname."""
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
        self._roles = deepcopy(roles)

    @property
    def node_type(self) -> str:
        """Return the provider type."""
        return "ibmc"

    def create(
        self,
        node_name: str,
        image_name: str,
        network_name: str,
        private_key: str,
        vpc_name: str,
        profile: str,
        group_access: str,
        zone_name: str,
        zone_id_model_name: str,
        size_of_disks: int = 0,
        no_of_volumes: int = 0,
        userdata: str = "",
    ) -> None:
        """
        Create the instance in IBM Cloud with the provided data.

        Args:
            node_name           Name of the VM.
            image_name          Name of the image to use for creating the VM.
            network_name        Name of the Network
            private_key         Private ssh key
            access_key          Users IBM cloud access key
            vpc_name            Name of VPC
            profile             Node profile. EX: "bx2-2x8"
            group_access        group security policy
            zone_name           Name of zone
            zone_id_model_name  Name of zone identity model
            size_of_disks       size of disk
            no_of_volumes       Number of volumes for each node
            userdata            user related data

        """
        LOG.info(f"Starting to create VM with name {node_name}")
        try:
            # Construct a dict representation of a VPCIdentityById model
            vpcs = self.service.list_vpcs()
            vpc_id = get_resource_id(vpc_name, vpcs.get_result())
            vpc_identity_model = dict({"id": vpc_id})

            subnets = self.service.list_subnets()
            subnet = get_resource_details(network_name, subnets.get_result())
            subnet_identity_model = dict({"id": subnet["id"]})
            self._subnet = subnet["ipv4_cidr_block"]

            security_group = self.service.list_security_groups()
            security_group_id = get_resource_id(
                group_access, security_group.get_result()
            )
            security_group_identity_model = dict({"id": security_group_id})

            # Construct a dict representation of a NetworkInterfacePrototype model
            network_interface_prototype_model = dict(
                {
                    "allow_ip_spoofing": False,
                    "subnet": subnet_identity_model,
                    "security_groups": [security_group_identity_model],
                }
            )

            # Construct a dict representation of a ImageIdentityById model
            images = self.service.list_images(name=image_name)
            image_id = get_resource_id(image_name, images.get_result())
            image_identity_model = dict({"id": image_id})

            # Construct a dict representation of a KeyIdentityById model
            keys = self.service.list_keys()
            key_id = get_resource_id(private_key, keys.get_result())

            key_identity_model = dict({"id": key_id})
            key_identity_shared = {
                "fingerprint": "SHA256:OkzMbGLDIzqUcZoH9H/j5o/v01trlqKqp5DaUpJ0tcQ"
            }

            # Construct a dict representation of a ResourceIdentityById model
            resource_group_identity_model = dict(
                {"id": "cb8d87c33ca04965a180fd7ab7383936"}
            )

            # Construct a dict representation of a InstanceProfileIdentityByName model
            instance_profile_identity_model = dict({"name": profile})

            # Construct a dict representation of a ZoneIdentityByName model
            zone_identity_model = dict({"name": zone_id_model_name})

            # Construct a dict representation of a VolumeProfileIdentityByName model
            volume_profile_identity_model = dict({"name": "general-purpose"})

            volume_attachment_list = []
            for i in range(0, no_of_volumes):
                volume_attachment_volume_prototype_instance_context_model1 = dict(
                    {
                        "name": f"{node_name.lower()}-{str(i)}",
                        "profile": volume_profile_identity_model,
                        "capacity": size_of_disks,
                    }
                )

                volume_attachment_prototype_instance_context_model1 = dict(
                    {
                        "delete_volume_on_instance_delete": True,
                        "volume": volume_attachment_volume_prototype_instance_context_model1,
                    }
                )

                volume_attachment_list.append(
                    volume_attachment_prototype_instance_context_model1
                )

            # Prepare the VSI payload
            instance_prototype_model = dict(
                {"keys": [key_identity_model, key_identity_shared]}
            )

            instance_prototype_model["name"] = node_name.lower()
            instance_prototype_model["profile"] = instance_profile_identity_model
            instance_prototype_model["resource_group"] = resource_group_identity_model
            instance_prototype_model["user_data"] = userdata
            instance_prototype_model["volume_attachments"] = volume_attachment_list
            instance_prototype_model["vpc"] = vpc_identity_model
            instance_prototype_model["image"] = image_identity_model
            instance_prototype_model[
                "primary_network_interface"
            ] = network_interface_prototype_model
            instance_prototype_model["zone"] = zone_identity_model

            # Set up parameter values
            instance_prototype = instance_prototype_model
            response = self.service.create_instance(instance_prototype)

            instance_id = response.get_result()["id"]
            self.wait_until_vm_state_running(instance_id)

            response = self.service.get_instance(instance_id)
            self.node = response.get_result()

            # DNS record creation phase
            LOG.debug(f"Adding DNS records for {node_name}")
            dns_zone = self.dns_service.list_dnszones(
                "a55534f5-678d-452d-8cc6-e780941d8e31"
            )
            dns_zone_id = get_dns_zone_id(zone_name, dns_zone.get_result())

            resource = self.dns_service.list_resource_records(
                instance_id="a55534f5-678d-452d-8cc6-e780941d8e31",
                dnszone_id=dns_zone_id,
            )
            records_a = [
                i for i in resource.get_result()["resource_records"] if i["type"] == "A"
            ]
            records_ip = [
                i
                for i in records_a
                if i["rdata"]["ip"]
                == self.node["primary_network_interface"]["primary_ipv4_address"]
            ]
            if records_ip:
                self.dns_service.update_resource_record(
                    instance_id="a55534f5-678d-452d-8cc6-e780941d8e31",
                    dnszone_id=dns_zone_id,
                    record_id=records_ip[0]["id"],
                    name=self.node["name"],
                    rdata=records_ip[0]["rdata"],
                )

            a_record = ResourceRecordInputRdataRdataARecord(
                self.node["primary_network_interface"]["primary_ipv4_address"]
            )
            self.dns_service.create_resource_record(
                instance_id="a55534f5-678d-452d-8cc6-e780941d8e31",
                dnszone_id=dns_zone_id,
                type="A",
                ttl=900,
                name=self.node["name"],
                rdata=a_record,
            )

            ptr_record = ResourceRecordInputRdataRdataPtrRecord(
                f"{self.node['name']}.{zone_name}"
            )
            self.dns_service.create_resource_record(
                instance_id="a55534f5-678d-452d-8cc6-e780941d8e31",
                dnszone_id=dns_zone_id,
                type="PTR",
                ttl=900,
                name=self.node["primary_network_interface"]["primary_ipv4_address"],
                rdata=ptr_record,
            )

        except NodeError:
            raise
        except BaseException as be:  # noqa
            LOG.error(be, exc_info=True)
            raise NodeError(f"Unknown error. Failed to create VM with name {node_name}")

    def delete(self, zone_name: Optional[str] = None) -> None:
        """
        Removes the VSI instance from the platform along with its DNS record.

        Args:
            zone_name (str):    DNS Zone name associated with the instance.
        """
        if not self.node:
            return

        node_id = self.node["id"]
        node_name = self.node["name"]

        try:
            self.remove_dns_records(zone_name)
        except BaseException:  # noqa
            LOG.warning(f"Encountered an error in removing DNS records of {node_name}")

        LOG.info(f"Preparing to remove {node_name}")
        resp = self.service.delete_instance(node_id)

        if resp.get_status_code() != 204:
            LOG.debug(f"{node_name} cannot be found.")
            return

        # Wait for the VM to be delete
        end_time = datetime.now() + timedelta(seconds=600)
        while end_time > datetime.now():
            sleep(5)
            try:
                resp = self.service.get_instance(node_id)
                if resp.get_status_code == 404:
                    LOG.info(f"Successfully removed {node_name}")
                    return
            except ApiException:
                LOG.info(f"Successfully removed {node_name}")
                self.remove_dns_records(zone_name)
                return

        LOG.debug(resp.get_result())
        raise NodeDeleteFailure(f"Failed to remove {node_name}")

    def wait_until_vm_state_running(self, instance_id: str) -> None:
        """
        Waits until the VSI moves to a running state within the specified time.

        Args:
            instance_id (str)   The ID of the VSI to be checked.

        Returns:
            None

        Raises:
            NodeError
        """
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1200)

        node_details = None
        while end_time > datetime.now():
            sleep(5)
            resp = self.service.get_instance(instance_id)
            if resp.get_status_code() != 200:
                LOG.debug("Encountered an error getting the instance.")
                sleep(5)
                continue

            node_details = resp.get_result()
            if node_details["status"] == "running":
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                LOG.info(
                    "%s moved to running state in %d seconds.",
                    node_details["name"],
                    int(duration),
                )
                return

            if node_details["status"] == "failed":
                raise NodeError(node_details["status_reasons"])

        raise NodeError(f"{node_details['name']} is in {node_details['status']} state.")

    def remove_dns_records(self, zone_name):
        """
        Remove the DNS records associated this VSI.

        Args:
            zone_name (str):    DNS zone name associated with this VSI
        """
        if not self.node:
            return

        zones = self.dns_service.list_dnszones("a55534f5-678d-452d-8cc6-e780941d8e31")
        zone_id = get_dns_zone_id(zone_name, zones.get_result())

        resp = self.dns_service.list_resource_records(
            instance_id="a55534f5-678d-452d-8cc6-e780941d8e31", dnszone_id=zone_id
        )
        records = resp.get_result()

        # ToDo: There is a maximum of 200 records that can be retrieved at a time.
        #       Support pagination is required.
        for record in records["resource_records"]:
            if record["type"] == "A" and self.node.get("name") in record["name"]:
                if record.get("linked_ptr_record"):
                    LOG.info(
                        f"Deleting PTR record {record['linked_ptr_record']['name']}"
                    )
                    self.dns_service.delete_resource_record(
                        instance_id="a55534f5-678d-452d-8cc6-e780941d8e31",
                        dnszone_id=zone_id,
                        record_id=record["linked_ptr_record"]["id"],
                    )

                LOG.info(f"Deleting Address record {record['name']}")
                self.dns_service.delete_resource_record(
                    instance_id="a55534f5-678d-452d-8cc6-e780941d8e31",
                    dnszone_id=zone_id,
                    record_id=record["id"],
                )

                return

        # This code path can happen if there are no matching/associated DNS records
        # Or we have a problem
        LOG.debug(f"No matching DNS records found for {self.node['name']}")
