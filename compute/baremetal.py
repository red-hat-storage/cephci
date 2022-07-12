"""Collects the Baremetal information and creates the cephNode object."""
from copy import deepcopy
from os.path import expanduser
from typing import List, Optional

from ceph.ceph import SSHConnectionManager
from utility.log import Log

LOG = Log(__name__)


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
            Id
            location
        """
        # CephVM attributes
        self._roles: list = list()
        self.osd_scenario: Optional[int] = None
        self.keypair: Optional[str] = None

        self.params = params
        self.location = params.get("location")
        self.private_key = params.get("root_private_key")
        if self.private_key:
            self.private_key = expanduser(self.private_key)
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
            _, out, err = self.rssh().exec_command(
                command="id -u cephuser",
            )

            if err:
                self._create_user(name="cephuser")
            else:
                LOG.debug("Reusing existing user account of cephuser.")
        except BaseException:  # noqa
            self._create_user(name="cephuser")

        self.rssh().exec_command(
            'echo "cephuser ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/cephuser'
        )
        self.rssh().exec_command(command="touch /ceph-qa-ready")

    def _create_user(self, name: str = "cephuser") -> None:
        """
        Create a Linux user account using the provided name.

        Args:
            name (str):     Name of the user account

        Raises:
            CommandError
        """
        LOG.info(f"Creating user account with {name} ...")

        self.rssh().exec_command(
            command=f"useradd {name} -p '$1$1fsNAJ7G$bx4Sz9VnpOnIygVKVaGCT.'"
        )
        self.rssh().exec_command(command=f"install -d -m 700  /home/{name}/.ssh")
        self.rssh().exec_command(
            command=f"install -m 600 ~/.ssh/authorized_keys /home/{name}/.ssh/ || true"
        )
        self.rssh().exec_command(command=f"chown -R {name}:{name} /home/{name}/")

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

    @property
    def id(self) -> int:
        """Return the node id."""
        return self.params.get("id")
