"""IBM-Cloud VPC provider implementation for CephVMNode."""

import re
import socket
from copy import deepcopy
from datetime import datetime, timedelta
from time import sleep
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import parse_qs, urlparse

from ibm_cloud_networking_services import DnsSvcsV1
from ibm_cloud_networking_services.dns_svcs_v1 import (
    ResourceRecordInputRdataRdataARecord,
    ResourceRecordInputRdataRdataPtrRecord,
)
from ibm_cloud_sdk_core.api_exception import ApiException
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from ibm_vpc import VpcV1  # noqa
from requests.exceptions import ConnectionError, ReadTimeout, RequestException

from utility.log import Log
from utility.retry import retry

from .exceptions import (
    NetworkOpFailure,
    NodeDeleteFailure,
    NodeError,
    ResourceNotFound,
    VolumeOpFailure,
)

LOG = Log(__name__)

# Tuple of exceptions that should trigger retry for DNS operations
DNS_RETRY_EXCEPTIONS = (ApiException, ConnectionError, ReadTimeout, RequestException)


class ResourceRecordIterator:
    """Iterator for paginated DNS resource records."""

    def __init__(
        self,
        fetch_page_func,
        dns_svc_id: str,
        dns_zone_id: str,
        limit: int = 50,
    ):
        """
        Initialize the iterator.

        Args:
            fetch_page_func: Function to fetch a page of records (with retry logic).
            dns_svc_id: GUID of the DNS Service.
            dns_zone_id: DNS Zone ID.
            limit: Number of records per page.
        """
        self._fetch_page_func = fetch_page_func
        self.dns_svc_id = dns_svc_id
        self.dns_zone_id = dns_zone_id
        self.limit = limit
        self.offset = None
        self.records_yielded = 0
        self.current_page_records = []
        self.current_index = 0
        self._has_more = True

    def __iter__(self):
        """Return the iterator object itself."""
        return self

    def __next__(self) -> Dict:
        """Return the next resource record."""
        # If we have records in the current page, return the next one
        if self.current_index < len(self.current_page_records):
            record = self.current_page_records[self.current_index]
            self.current_index += 1
            self.records_yielded += 1
            return record

        # If no more pages, stop iteration
        if not self._has_more:
            raise StopIteration

        # Fetch next page
        self._fetch_next_page()

        # Return the first record from the new page
        if self.current_page_records:
            record = self.current_page_records[0]
            self.current_index = 1
            self.records_yielded += 1
            return record

        # No more records
        raise StopIteration

    def _extract_offset_from_href(self, href: str) -> Optional[int]:
        """
        Extract offset from next href using proper URL parsing.

        Args:
            href: URL string from 'next' field.

        Returns:
            Offset value or None if not found/invalid.
        """
        try:
            parsed = urlparse(href)
            query_params = parse_qs(parsed.query)

            # Try offset first, then start
            if "offset" in query_params:
                offset_str = query_params["offset"][0]
            elif "start" in query_params:
                offset_str = query_params["start"][0]
            else:
                return None

            offset = int(offset_str)
            # Validate offset is non-negative
            if offset < 0:
                LOG.warning(f"Invalid negative offset in href: {href}")
                return None
            return offset

        except (ValueError, TypeError, KeyError, IndexError) as e:
            LOG.warning(f"Could not parse offset from href {href}: {e}")
            return None

    def _fetch_next_page(self) -> None:
        """Fetch the next page of resource records."""
        try:
            result = self._fetch_page_func(
                dns_svc_id=self.dns_svc_id,
                dns_zone_id=self.dns_zone_id,
                limit=self.limit,
                offset=self.offset,
            )
            records = result.get("resource_records", [])

            # Clear previous page data
            self.current_page_records = []
            self.current_index = 0

            if not records:
                # Even if no records, check if there's a next link
                # (API might return empty page but indicate more pages exist)
                next_info = result.get("next")
                if next_info:
                    offset = self._extract_offset_from_href(next_info["href"])
                    if offset is not None:
                        self.offset = offset
                        self._has_more = True
                        return
                self._has_more = False
                return

            self.current_page_records = records

            # Check if pagination continues
            next_info = result.get("next")
            if not next_info:
                self._has_more = False
                return

            # Extract offset from next href using proper URL parsing
            offset = self._extract_offset_from_href(next_info["href"])
            if offset is not None:
                self.offset = offset
            else:
                # Fallback: if no offset/start found, use total count
                total_count = result.get("total_count", 0)
                if self.records_yielded >= total_count:
                    self._has_more = False
                else:
                    # Use records_yielded as fallback, but log warning
                    LOG.warning(
                        f"Could not extract offset from href, using records_yielded={self.records_yielded}"
                    )
                    self.offset = self.records_yielded

        except DNS_RETRY_EXCEPTIONS as e:
            LOG.error(f"Error listing resource records after retries: {e}")
            # Clear state on error to prevent returning stale data
            self._has_more = False
            self.current_page_records = []
            self.current_index = 0


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
    service.set_http_config({"timeout": 130})  # Increase to 130s

    return service


def get_dns_service(
    access_key: str,
    service_url: Optional[str] = "https://api.dns-svcs.cloud.ibm.com/v1",
):
    """
    Return the authenticated connection from the given endpoint.
    Args:
        access_key (str): The access key(API key) of the user.
        service_url (str): DNS Services endpoint URL.
    """
    authenticator = IAMAuthenticator(access_key)

    dnssvc = DnsSvcsV1(authenticator=authenticator)
    dnssvc.set_service_url(service_url=service_url)

    # Configure HTTP settings
    # Note: Cloudflare rate limiting happens at the edge based on IP and request frequency,
    # not headers. The retry decorator with exponential backoff is the primary solution
    # for handling rate limits. Only set timeout - let SDK handle headers to avoid conflicts.
    dnssvc.set_default_headers(
        {
            "User-Agent": "ibmcloud-dns-sdk/1.0 (safe-fetch-script)",
            "Accept": "application/json",
        }
    )

    http_config = {
        "timeout": 130,  # Increase timeout to 130s for long-running operations
    }
    dnssvc.set_http_config(http_config)

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


def get_dns_zone_instance_id(zone_name: str, response: Any) -> str:
    """
    Retrieve the DNS Zone Instance ID for the provided zone name using the provided response.

    Args:
        zone_name (str):    DNS Zone whose ID needs to be retrieved.
        response (Dict):    Response returned from the collection.

    Returns:
        DNS Zone Instance ID (str)

    Raises:
        ResourceNotFound    when there is a failure to retrieve the given zone ID.
    """
    for i in response["dnszones"]:
        if i["name"] == zone_name:
            return i["instance_id"]
    raise ResourceNotFound(f"Failed to retrieve the ID of {zone_name}.")


class CephVMNodeIBM:
    """Represents a VMNode object created by softlayer driver."""

    def __init__(
        self,
        os_cred_ibm: dict,
        vsi_id: Optional[str] = None,
        node: Optional[Dict] = None,
    ) -> None:
        """
        Initializes the instance using the provided information.

        Args:
            os_cred_ibm (dict): Dictionary containing 'accesskey' and 'service_url'.
            vsi_id (str):       The VSI node ID to be retrieved
            node (dict):
        """
        # CephVM attributes
        self._os_cred_ibm = os_cred_ibm
        self._subnet: str = ""
        self._roles: list = list()
        self.node = None

        self.service = get_ibm_service(
            access_key=os_cred_ibm["accesskey"], service_url=os_cred_ibm["service_url"]
        )
        self.dns_service = get_dns_service(access_key=os_cred_ibm["accesskey"])

        if vsi_id:
            self.node = self.service.get_instance(id=vsi_id).get_result()

        if node:
            self.node = node

    # properties

    @property
    def ip_address(self) -> str:
        """Return the private IP address of the node."""
        return self.node["primary_network_interface"]["primary_ip"]["address"]

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
        resource_group_id: str,
        dns_svc_id: str,
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
            resource_group_id   The UUID of the resource group.
            dns_svc_id          The UUID of the DNS Service hosted in IBM Cloud
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
            # SSH fingerprint of ceph-qe-sa.pub - ssh-keygen -lf ceph-qe-sa.pub
            key_identity_shared = {
                "fingerprint": "SHA256:SFan4TdEd1xcT4v9So8q4A+B/f2PcXOoPfS2vwPk9/M"
            }

            # Construct a dict representation of a ResourceIdentityById model
            # ToDo: Move this information to credentials... resource groups.
            resource_group_identity_model = dict({"id": resource_group_id})

            # Construct a dict representation of a InstanceProfileIdentityByName model
            instance_profile_identity_model = dict({"name": profile})

            # Construct a dict representation of a ZoneIdentityByName model
            zone_identity_model = dict({"name": zone_id_model_name})

            # Set the volume profile according to the VPC as SDS is not
            # available in ci-vpc-01.
            volume_profile_identity_model = dict({"name": "sdp"})
            if vpc_name == "ci-vpc-01":
                volume_profile_identity_model = dict({"name": "general-purpose"})

            # Set the boot volume profile
            boot_volume_prototype_model = dict(
                {
                    "name": f"{node_name.lower()}-boot",
                    "profile": volume_profile_identity_model,
                }
            )
            # Set the boot volume attachment profile
            boot_volume_attachment_prototype_model = dict(
                {
                    "volume": boot_volume_prototype_model,
                }
            )

            # Prepare the volume attachments
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
            instance_prototype_model["boot_volume_attachment"] = (
                boot_volume_attachment_prototype_model
            )
            instance_prototype_model["volume_attachments"] = volume_attachment_list
            instance_prototype_model["vpc"] = vpc_identity_model
            instance_prototype_model["image"] = image_identity_model
            instance_prototype_model["primary_network_interface"] = (
                network_interface_prototype_model
            )
            instance_prototype_model["zone"] = zone_identity_model

            # Set up parameter values
            instance_prototype = instance_prototype_model
            response = self.service.create_instance(instance_prototype)

            instance_id = response.get_result()["id"]
            self._wait_until_vm_state(instance_id, target_state="running")

            response = self.service.get_instance(instance_id)
            self.node = response.get_result()

            # DNS record creation phase
            self._add_dns_records(
                zone_name=zone_name,
                dns_svc_id=dns_svc_id,
                node_name=self.node["name"],
                node_ip=self.node["primary_network_interface"]["primary_ip"]["address"],
            )

        except NodeError:
            raise
        except BaseException as be:  # noqa
            LOG.error(be, exc_info=True)
            raise NodeError(f"Unknown error. Failed to create VM with name {node_name}")

    def delete(
        self,
        zone_name: Optional[str] = None,
        dns_svc_id: Optional[str] = None,
    ) -> None:
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
            self.remove_dns_records(zone_name, dns_svc_id)
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
                self.remove_dns_records(zone_name, dns_svc_id)
                return

        LOG.debug(resp.get_result())
        raise NodeDeleteFailure(f"Failed to remove {node_name}")

    def shutdown(self, wait: bool = False) -> None:
        """
        Gracefully power off the IBM Cloud VM.
        Args:
            wait (bool): Wait until the VM is fully powered off.
        """
        try:
            if not self.node:
                return
            node_id = self.node["id"]
            LOG.info(
                "Initiating shutdown of IBM node: {} (ID: {})".format(
                    self.node["name"], node_id
                )
            )
            self.service = get_ibm_service(
                access_key=self._os_cred_ibm["accesskey"],
                service_url=self._os_cred_ibm["service_url"],
            )
            self.service.create_instance_action(node_id, type="stop")
            if wait:
                try:
                    self._wait_until_vm_state(node_id, target_state="stopped")
                except NodeError as e:
                    LOG.error(
                        "Error while waiting for IBM Cloud VM to stop with error: {}".format(
                            e
                        )
                    )
                    raise
        except (ResourceNotFound, NetworkOpFailure, NodeError, VolumeOpFailure):
            LOG.error(
                "Error while initiating shutdown on node {}".format(self.node["id"])
            )
            raise

    def power_on(self) -> None:
        """
        Start the IBM Cloud VM.
        """
        try:
            if not self.node:
                return
            node_id = self.node["id"]
            LOG.info(
                "Powering on IBM node: {} (ID: {})".format(self.node["name"], node_id)
            )
            self.service = get_ibm_service(
                access_key=self._os_cred_ibm["accesskey"],
                service_url=self._os_cred_ibm["service_url"],
            )
            self.service.create_instance_action(node_id, type="start")
            self._wait_until_vm_state(node_id, target_state="running")
        except (ResourceNotFound, NetworkOpFailure, NodeError, VolumeOpFailure):
            LOG.error(
                "Error while initiating powering on node {}".format(self.node["id"])
            )
            raise

    def _wait_until_vm_state(
        self, instance_id: str, target_state: str, timeout: int = 1200
    ) -> None:
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
        end_time = start_time + timedelta(seconds=timeout)

        node_details = None
        while end_time > datetime.now():
            sleep(5)
            resp = self.service.get_instance(instance_id)
            if resp.get_status_code() != 200:
                LOG.debug("Encountered an error getting the instance.")
                sleep(5)
                continue

            node_details = resp.get_result()
            if node_details["status"] == target_state:
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                LOG.info(
                    "%s moved to %s state in %d seconds.",
                    node_details["name"],
                    target_state,
                    int(duration),
                )
                return

            if node_details["status"] == "failed":
                raise NodeError(node_details["status_reasons"])

        raise NodeError(f"{node_details['name']} is in {node_details['status']} state.")

    @retry(DNS_RETRY_EXCEPTIONS, tries=5, delay=60, backoff=3)
    def _list_resource_records_page(
        self,
        dns_svc_id: str,
        dns_zone_id: str,
        limit: int,
        offset: Optional[int] = None,
        name: Optional[str] = None,
        type: Optional[str] = None,
    ) -> Dict:
        """
        List a single page of resource records with retry support and rate limit handling.

        Args:
            dns_svc_id (str):         GUID of the DNS Service.
            dns_zone_id (str):        DNS Zone ID.
            limit (int):              Number of records per page.
            offset (int, optional):    Pagination offset from 'next' href.

        Returns:
            Dict: Response result containing resource records.

        Raises:
            ApiException: If the API call fails after retries.
            ConnectionError: If connection fails after retries.
            ReadTimeout: If request times out after retries.
            RequestException: If request fails after retries.
        """
        kwargs = {
            "instance_id": dns_svc_id,
            "dnszone_id": dns_zone_id,
            "limit": limit,
        }
        if offset is not None:
            kwargs["offset"] = offset
        if name is not None:
            kwargs["name"] = name
        if type is not None:
            kwargs["type"] = type

        resp = self.dns_service.list_resource_records(**kwargs)
        return resp.get_result()

    def _list_resource_records_with_pagination(
        self, dns_svc_id: str, dns_zone_id: str, limit: int = 50
    ) -> Iterator[Dict]:
        """
        Return an iterator for all resource records with pagination support.

        Args:
            dns_svc_id (str):   GUID of the DNS Service.
            dns_zone_id (str):  DNS Zone ID.
            limit (int):        Number of records per page (default: 50).

        Returns:
            Iterator[Dict]: Iterator that yields individual resource record dictionaries.
        """
        return ResourceRecordIterator(
            fetch_page_func=self._list_resource_records_page,
            dns_svc_id=dns_svc_id,
            dns_zone_id=dns_zone_id,
            limit=limit,
        )

    @retry(DNS_RETRY_EXCEPTIONS, tries=5, delay=60, backoff=3)
    def _create_resource_record(
        self,
        dns_svc_id: str,
        dns_zone_id: str,
        record_type: str,
        name: str,
        rdata: Any,
        ttl: int = 900,
        node_context: Optional[str] = None,
    ) -> None:
        """
        Create a DNS resource record with retry support and rate limit handling.

        Args:
            dns_svc_id (str):         GUID of the DNS Service.
            dns_zone_id (str):        DNS Zone ID.
            record_type (str):         Type of record (A, PTR, etc.).
            name (str):                Record name.
            rdata (Any):               Record data.
            ttl (int):                 Time to live (default: 900).
            node_context (str, optional): Node/VM context for error messages (e.g., "node_name/IP").

        Raises:
            ApiException: If the API call fails after retries.
            ConnectionError: If connection fails after retries.
            ReadTimeout: If request times out after retries.
            RequestException: If request fails after retries.
        """
        context_info = f" (node: {node_context})" if node_context else ""
        LOG.debug(
            f"Creating {record_type} record '{name}'{context_info} "
            f"(rdata: {rdata}, ttl: {ttl})"
        )
        try:
            self.dns_service.create_resource_record(
                instance_id=dns_svc_id,
                dnszone_id=dns_zone_id,
                type=record_type,
                ttl=ttl,
                name=name,
                rdata=rdata,
            )
        except ApiException as e:
            # Handle cases where record might have been created but API call failed
            # (e.g., bad gateway, timeout) - delete existing record before retry
            if e.code == 409 or (e.code >= 500 and e.code < 600):
                # Record might exist (409) or was created but we got server error (5xx)
                try:
                    result = self._list_resource_records_page(
                        dns_svc_id=dns_svc_id,
                        dns_zone_id=dns_zone_id,
                        limit=1,
                        name=name,
                        type=record_type,
                    )
                    existing_records = result.get("resource_records", [])
                    if existing_records:
                        existing_record = existing_records[0]
                        record_id = existing_record.get("id")
                        LOG.debug(
                            f"Found existing {record_type} record '{name}' (id: {record_id}) "
                            f"after error {e.code}, deleting before retry"
                        )
                        self._delete_resource_record(
                            dns_svc_id=dns_svc_id,
                            dns_zone_id=dns_zone_id,
                            record_id=record_id,
                            node_context=node_context,
                        )
                        LOG.debug(
                            f"Deleted existing {record_type} record '{name}', "
                            f"will retry creation"
                        )
                except Exception as delete_error:
                    # Log but don't fail - let retry mechanism handle it
                    LOG.debug(
                        f"Could not check/delete existing record after error {e.code}: "
                        f"{delete_error}. Will retry creation"
                    )

            # Enhance error message with context for better debugging in parallel operations
            context_msg = f" for node {node_context}" if node_context else ""
            error_msg = (
                f"Failed to create {record_type} record '{name}'{context_msg}: "
                f"{e.message} (Status: {e.code})"
            )
            LOG.error(error_msg)
            # Re-raise with enhanced context in the exception message
            # Preserve original exception details while adding context
            raise ApiException(
                code=e.code,
                message=error_msg,
                http_response=e.http_response,
            ) from e

    @retry(DNS_RETRY_EXCEPTIONS, tries=5, delay=60, backoff=3)
    def _update_resource_record(
        self,
        dns_svc_id: str,
        dns_zone_id: str,
        record_id: str,
        name: str,
        rdata: Any,
    ) -> None:
        """
        Update a DNS resource record with retry support and rate limit handling.

        Args:
            dns_svc_id (str):   GUID of the DNS Service.
            dns_zone_id (str):  DNS Zone ID.
            record_id (str):    ID of the record to update.
            name (str):         Record name (not used in update, kept for API compatibility).
            rdata (Any):        Record data.

        Raises:
            ApiException: If the API call fails after retries.
            ConnectionError: If connection fails after retries.
            ReadTimeout: If request times out after retries.
            RequestException: If request fails after retries.
        """
        # Note: name is not included in update - API rejects it as forbidden property.
        # Record is identified by record_id, and name cannot be changed during update.
        self.dns_service.update_resource_record(
            instance_id=dns_svc_id,
            dnszone_id=dns_zone_id,
            record_id=record_id,
            rdata=rdata,
        )

    @retry(DNS_RETRY_EXCEPTIONS, tries=5, delay=60, backoff=3)
    def _delete_resource_record(
        self,
        dns_svc_id: str,
        dns_zone_id: str,
        record_id: str,
        node_context: Optional[str] = None,
    ) -> None:
        """
        Delete a DNS resource record with retry support and rate limit handling.

        Args:
            dns_svc_id (str):         GUID of the DNS Service.
            dns_zone_id (str):        DNS Zone ID.
            record_id (str):          ID of the record to delete.
            node_context (str, optional): Node/VM context for error messages (e.g., "node_name/IP").

        Raises:
            ApiException: If the API call fails after retries.
            ConnectionError: If connection fails after retries.
            ReadTimeout: If request times out after retries.
            RequestException: If request fails after retries.
        """
        context_info = f" (node: {node_context})" if node_context else ""
        LOG.debug(f"Deleting DNS record '{record_id}'{context_info}")
        try:
            self.dns_service.delete_resource_record(
                instance_id=dns_svc_id,
                dnszone_id=dns_zone_id,
                record_id=record_id,
            )
        except ApiException as e:
            # Enhance error message with context for better debugging in parallel operations
            context_msg = f" for node {node_context}" if node_context else ""
            error_msg = (
                f"Failed to delete DNS record '{record_id}'{context_msg}: "
                f"{e.message} (Status: {e.code})"
            )
            LOG.error(error_msg)
            # Re-raise with enhanced context in the exception message
            # Preserve original exception details while adding context
            raise ApiException(
                code=e.code,
                message=error_msg,
                http_response=e.http_response,
            ) from e

    @retry(DNS_RETRY_EXCEPTIONS, tries=5, delay=60, backoff=3)
    def _add_dns_records(
        self,
        zone_name: str,
        dns_svc_id: str,
        node_name: str,
        node_ip: str,
    ) -> None:
        """
        Recreate DNS records (A and PTR) for the node.

        Finds existing A and PTR records, deletes them if found, then creates new records
        with the new IP and domain. The entire operation is retried if any step fails.

        Args:
            zone_name (str):    DNS Zone name.
            dns_svc_id (str):   GUID of the DNS Service.
            node_name (str):    Name of the node (FQDN without zone).
            node_ip (str):      IP address of the node.

        Raises:
            ApiException: If DNS operations fail after retries.
            ConnectionError: If connection fails after retries.
            ReadTimeout: If request times out after retries.
            RequestException: If request fails after retries.
        """
        LOG.debug(f"Creating DNS records for {node_name} with IP {node_ip}")
        dns_zone = self.dns_service.list_dnszones(dns_svc_id)
        dns_zone_id = get_dns_zone_id(zone_name, dns_zone.get_result())

        # Step 1: Fetch PTR record by IP address (one record at a time)
        ptr_result = self._list_resource_records_page(
            dns_svc_id=dns_svc_id,
            dns_zone_id=dns_zone_id,
            limit=1,
            name=node_ip,
            type="PTR",
        )
        existing_ptr_record = None
        if ptr_result.get("resource_records"):
            existing_ptr_record = ptr_result["resource_records"][0]
            LOG.debug(f"Found existing PTR record: {existing_ptr_record.get('id')}")

        # Step 2: Use PTR record data to find A record
        existing_a_record = None
        if existing_ptr_record:
            # Extract hostname from PTR record's ptrdname (FQDN)
            ptrdname = existing_ptr_record.get("rdata", {}).get("ptrdname")
            if ptrdname:
                # Remove zone suffix to get hostname (A record name is relative to zone)
                # e.g., "node1.zone.com" with zone "zone.com" -> "node1"
                if ptrdname.endswith(f".{zone_name}"):
                    a_record_name = ptrdname[: -(len(zone_name) + 1)]
                else:
                    # Fallback: extract first part if zone doesn't match
                    a_record_name = ptrdname.split(".")[0]

                # Fetch A record using the hostname from PTR record
                a_result = self._list_resource_records_page(
                    dns_svc_id=dns_svc_id,
                    dns_zone_id=dns_zone_id,
                    limit=1,
                    name=a_record_name,
                    type="A",
                )
                if a_result.get("resource_records"):
                    existing_a_record = a_result["resource_records"][0]
                    LOG.debug(
                        f"Found existing A record via PTR (name: {a_record_name}): "
                        f"{existing_a_record.get('id')}"
                    )

        # Step 3: If A record not found via PTR, try direct lookup by node_name
        if not existing_a_record:
            a_result = self._list_resource_records_page(
                dns_svc_id=dns_svc_id,
                dns_zone_id=dns_zone_id,
                limit=1,
                name=node_name,
                type="A",
            )
            if a_result.get("resource_records"):
                existing_a_record = a_result["resource_records"][0]
                LOG.debug(
                    f"Found existing A record by name: {existing_a_record.get('id')}"
                )

        # Step 4: Delete existing records if found
        node_context = f"{node_name}/{node_ip}"
        if existing_a_record:
            LOG.debug(f"Deleting existing A record {existing_a_record.get('id')}")
            self._delete_resource_record(
                dns_svc_id=dns_svc_id,
                dns_zone_id=dns_zone_id,
                record_id=existing_a_record["id"],
                node_context=node_context,
            )

        if existing_ptr_record:
            LOG.debug(f"Deleting existing PTR record {existing_ptr_record.get('id')}")
            self._delete_resource_record(
                dns_svc_id=dns_svc_id,
                dns_zone_id=dns_zone_id,
                record_id=existing_ptr_record["id"],
                node_context=node_context,
            )

        # Step 5: Create new A record with new IP
        LOG.info(f"Creating new A record {node_name} with IP {node_ip}")
        a_record_data = ResourceRecordInputRdataRdataARecord(node_ip)
        self._create_resource_record(
            dns_svc_id=dns_svc_id,
            dns_zone_id=dns_zone_id,
            record_type="A",
            name=node_name,
            rdata=a_record_data,
            node_context=node_context,
        )

        # Step 6: Create new PTR record with new domain
        ptr_domain = f"{node_name}.{zone_name}"
        LOG.info(f"Creating new PTR record {node_ip} with domain {ptr_domain}")
        ptr_record_data = ResourceRecordInputRdataRdataPtrRecord(ptr_domain)
        self._create_resource_record(
            dns_svc_id=dns_svc_id,
            dns_zone_id=dns_zone_id,
            record_type="PTR",
            name=node_ip,
            rdata=ptr_record_data,
            node_context=node_context,
        )

    @retry(ConnectionError, tries=3, delay=60, backoff=3)
    def remove_dns_records(self, zone_name, dns_svc_id):
        """
        Remove the DNS records associated this VSI.

        Args:
            zone_name (str):    DNS zone name associated with this VSI
            dns_svc_id (str):   GUID of the DNS Service.
        """
        if not self.node:
            return

        zones = self.dns_service.list_dnszones(dns_svc_id)
        zone_id = get_dns_zone_id(zone_name, zones.get_result())
        zone_instance_id = get_dns_zone_instance_id(zone_name, zones.get_result())

        # Use pagination to list all resource records (iterator pattern)
        for record in self._list_resource_records_with_pagination(
            dns_svc_id=zone_instance_id, dns_zone_id=zone_id, limit=50
        ):
            if record["type"] == "A" and self.node.get("name") in record["name"]:
                if record.get("linked_ptr_record"):
                    LOG.info(
                        f"Deleting PTR record {record['linked_ptr_record']['name']}"
                    )
                    self.dns_service.delete_resource_record(
                        instance_id=dns_svc_id,
                        dnszone_id=zone_id,
                        record_id=record["linked_ptr_record"]["id"],
                    )

                LOG.info(f"Deleting Address record {record['name']}")
                self.dns_service.delete_resource_record(
                    instance_id=dns_svc_id,
                    dnszone_id=zone_id,
                    record_id=record["id"],
                )

                return

        # This code path can happen if there are no matching/associated DNS records
        # Or we have a problem
        LOG.debug(f"No matching DNS records found for {self.node['name']}")

    def __getstate__(self) -> dict:
        """
        Prepare the object state for pickling.
        Removes unserializable fields like service clients.
        """
        state = dict(self.__dict__)
        state["service"] = None
        state["dns_service"] = None
        return state

    def __setstate__(self, state) -> None:
        """
        Rehydrate the object after unpickling.
        Rebuilds the IBM Cloud services using stored credentials.
        """
        self.__dict__.update(state)

        access_key = self._os_cred_ibm["accesskey"]
        service_url = self._os_cred_ibm["service_url"]
        self.service = get_ibm_service(access_key=access_key, service_url=service_url)
        self.dns_service = get_dns_service(access_key=access_key)
