"""Collects the Baremetal information and creates the cephNode object."""
import logging
from copy import deepcopy
from typing import List, Optional

from ceph.ceph import SSHConnectionManager

LOG = logging.getLogger(__name__)


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
            subnet
        """
        # CephVM attributes
        self._roles: list = list()

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
        return self.params.get("ip")

    @property
    def node_type(self) -> str:
        return "baremetal"

    @property
    def hostname(self) -> str:
        """Return the hostname of the VM."""
        return self.params.get("hostname")

    @property
    def root_password(self) -> str:
        """Return root password for the machine"""
        return self.params.get("root_password", "passwd")

    @property
    def root_login(self) -> str:
        return self.params.get("root_login", "root")

    @property
    def volumes(self):
        """Return the list of storage volumes attached to the node."""
        return self.params.get("volumes", [])

    @property
    def no_of_volumes(self) -> int:
        """Return the number of volumes attached to the VM."""
        return len(self.volumes)

    @property
    def subnet(self) -> str:
        """Return the subnet information."""
        return self.params.get("subnet")

    @property
    def role(self) -> List:
        """Return the Ceph roles of the instance."""
        return self._roles

    @role.setter
    def role(self, roles: list) -> None:
        """Set the roles for the VM."""
        self._roles = deepcopy(roles)
