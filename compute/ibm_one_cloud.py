import requests
from typing import Dict, Any, Optional, List
from utility.retry import retry
from datetime import datetime
from requests.exceptions import ConnectionError, ReadTimeout, RequestException


class IbmOneCloud:
    def __init__(self, token: str):
        self.token = token
        self.base_url = f"https://portal.onecloud.wdc.app.cirrus.ibm.com/api/v3/"
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        })

    @property
    def ip_address (self) -> str:
        """Return the private IP address of the node."""
        return self.node["primary_network_interface"]["primary_ip"]["address"]

    @property
    def floating_ips (self) -> List[str]:
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
    def public_ip_address (self) -> str:
        """Return the public IP address of the node."""
        return self.floating_ips[0]

    @property
    def hostname (self) -> str:
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
    def volumes (self) -> List:
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
    def subnet (self) -> str:
        """Return the subnet information."""
        if self._subnet:
            return self._subnet

        subnet_details = self.service.get_subnet(
            self.node["primary_network_interface"]["subnet"]["id"]
        )
        return subnet_details.get_result()["ipv4_cidr_block"]

    @property
    def shortname (self) -> str:
        """Return the short form of the hostname."""
        return self.hostname.split(".")[0]

    @property
    def no_of_volumes (self) -> int:
        """Return the number of volumes attached to the VM."""
        return len(self.volumes)

    @property
    def role (self) -> List:
        """Return the Ceph roles of the instance."""
        return self._roles

    @role.setter
    def role (self, roles: list) -> None:
        """Set the roles for the VM."""
        self._roles = deepcopy(roles)

    @property
    def node_type (self) -> str:
        """Return the provider type."""
        return "ibmc"

    # Core request methods
    def _request(self, method: str, path: str, *, params=None, json=None):
        url = f"{self.base_url}{path}"
        if params is None:
            params = {}

        response = self.session.request(
            method=method,
            url=url,
            params=params,
            json=json,
            timeout=60
        )
        if not response.ok:
            raise Exception(f"API Error {response.status_code}: {response.text}")
        try:
            return response.json()
        except ValueError:
            return None

    def get(self, path: str, params: Dict[str, Any] = None):
        return self._request("GET", path, params=params)

    def post(self, path: str, data: Dict[str, Any]):
        return self._request("POST", path, json=data)

    def delete(self, path: str):
        return self._request("DELETE", path)

    def put(self, path: str, data: Dict[str, Any]):
        return self._request("PUT", path, json=data)

    def list_images(self):
        return self.get("/vm/images")

    def list_vms(self):
        return self.get("/vm")

    def get_vms(self, instance_id: str):
        return self.get(f"/vm/{instance_id}")

    def delete_vms(self, instance_id: str):
        return self.delete(f"/vm/{instance_id}")

    def create(self,
                  name: str,
                  notes: str,
                  image_id: str,
                  resources: str,
                  project_id: str,
                  site: str,
                  groupid: str,
                  vlan: str,
                  user_data: Optional[str] = None
                  ):
        body = {
              "virtualMachines": [
                {
                  "vmname": name,
                  "vmnotes": notes
                }
              ],
              "imageID": image_id,
              "resources": resources,
              "projectID": project_id,
              "site": site,
              "groupID": groupid,
              "VLAN": vlan
            }

        if user_data:
            body["user_data"] = user_data
        return self.post("/vm", body)

