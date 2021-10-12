"""Collects the Baremetal information and creates the cephNode object."""
import logging
from typing import List, Optional

from libcloud.compute.base import Node

from ceph.ceph import SSHConnectionManager

LOG = logging.getLogger()


class NetworkOpFailure(Exception):
    pass


class NodeError(Exception):
    pass


class CephBaremetalNode:
    """Represent the VMNode required for cephci."""

    def __init__(
        self,
        **params,
    ) -> None:
        """
        Initialize the instance node using the provided information.
        This will assign below properties to Baremetal Node

        params :
            username
            password
            Ip
            Hostname
            root_login
            volumes

        """
        self.node: Optional[Node] = None
        # CephVM attributes
        self._subnet: list = list()
        self._roles: list = list()

        # Fixme: determine if we can pick this information for OpenStack.
        self.root_login: str
        self.osd_scenario: int
        self.keypair: Optional[str] = None
        self.params = params
        self.root_connection = SSHConnectionManager(
            self.params.get("ip"),
            "root",
            self.params.get("root_password"),
            look_for_keys=False,
        )
        self.rssh = self.root_connection.get_client
        self.rssh().exec_command(command="useradd cephuser")
        self.rssh().exec_command(command="chown -R cephuser:cephuser /home/cephuser/")
        self.rssh().exec_command(
            'echo "cephuser ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/cephuser'
        )
        self.rssh().exec_command(command="touch /ceph-qa-ready")

    @property
    def ip_address(self) -> str:
        """Return the private IP address of the node."""
        if self.params.get("ip"):
            return self.params.get("ip")

    @property
    def node_type(self) -> str:
        return "baremetal"

    @property
    def hostname(self) -> str:
        """Return the hostname of the VM."""
        if self.params.get("hostname"):
            return self.params.get("hostname")

    @property
    def root_password(self) -> str:
        """Return root password for the machine"""
        if self.params.get("root_password"):
            return self.params.get("root_password")
        else:
            return "passwd"

    @property
    def root_login(self) -> str:
        return self.params.get("root_login", "")

    @property
    def volumes(self):
        """Return the list of storage volumes attached to the node."""
        if self.params.get("volumes"):
            return self.params.get("volumes")
        return []

    @property
    def no_of_volumes(self) -> int:
        """Return the number of volumes attached to the VM."""
        return len(self.volumes)

    @property
    def osd_scenario(self) -> int:
        """Return the number of volumes attached to the VM."""
        return 0

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
    def role(self) -> List:
        """Return the Ceph roles of the instance."""
        return self._roles

    @role.setter
    def role(self, roles: list) -> None:
        """Set the roles for the VM."""
        from copy import deepcopy

        self._roles = deepcopy(roles)
