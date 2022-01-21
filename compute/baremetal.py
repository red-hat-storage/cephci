"""Collects the Baremetal information and creates the cephNode object."""
import logging
from copy import deepcopy
from typing import List, Optional

from ceph.ceph import SSHConnectionManager

LOG = logging.getLogger(__name__)


class CephBaremetalNode:
    """Represent the Baremetal Node."""

    def __init__(self, **params) -> None:
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

        self.osd_scenario: Optional[int] = None
        self.keypair: Optional[str] = None
        self.params = params
        self.private_key = params.get("root_private_key")
        if self.private_key:
            self.root_connection = SSHConnectionManager(
                self.params.get("ip"),
                "root",
                self.params.get("root_password"),
                look_for_keys=True,
                private_key_file_path=self.private_key,
            )
        else:
            self.root_connection = SSHConnectionManager(
                self.params.get("ip"),
                "root",
                self.params.get("root_password"),
                look_for_keys=False,
            )

        self.rssh = self.root_connection.get_client

        # Check if user exists
        try:
            self.rssh().exec_command(command="id -u cephuser")
        except BaseException:  # noqa
            LOG.info("Creating cephuser...")

            self.rssh().exec_command(
                command="useradd cephuser -p '$1$1fsNAJ7G$bx4Sz9VnpOnIygVKVaGCT.'"
            )
            self.rssh().exec_command(command="mkdir /home/cephuser/.ssh")
            self.rssh().exec_command(
                command="cp ~/.ssh/authorized_keys /home/cephuser/.ssh/ || true"
            )
            self.rssh().exec_command(
                command="chown -R cephuser:cephuser /home/cephuser/"
            )
            self.rssh().exec_command(
                command="chmod 600 /home/cephuser.ssh/authorized_keys || true"
            )

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
        return len(self.volumes) if self.volumes else 0

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

    @property
    def shortname(self) -> str:
        """Return the shortform of the hostname."""
        return self.hostname.split(".")[0]
