import json

from cli.connectible.remote import Remote
from cli.exceptions import ConfigError, UnexpectedStateError
from cli.utilities.utils import load_config
from utility.log import Log

LOG = Log(__name__)


class Baremetal:
    """Insterface for Baremetal operations"""

    def __init__(
        self,
        server,
        env,
        owner,
        ssh_key=None,
        username=None,
        password=None,
        inventory=None,
        timeout=600,
    ):
        """Initialize instance using provided details

        Args:
            server (str): Server IP or Hostname
            env (str): Environment where teuthology is setup
            owner (str): Owner of nodes
            port (int): Server port number
            ssh_key (str): SSH Key path
            username (str): Name of user to be connected
            password (str): Password of user
            inventory (str): Octa lab hdd inventory
            timeout (int): Socket timeout
        """
        # Set teuthology hostname configs
        self._env = env.strip("/")
        self._owner = owner
        self._timeout = timeout

        # Set node inventory
        self._inventory = inventory

        # Connect to teuthology node
        self._client = Remote(server, ssh_key, username, password)

    def _teuthology_lock(
        self,
        list=False,
        locked=False,
        status=None,
        owner=None,
        lock=False,
        unlock=False,
        machine_type=None,
    ):
        """Execute teuthology lock operation

        Args:
            list (bool): Show lock info for machines owned
            locked (bool): Whether a machine is locked {true,false}
            status (str): Whether a machine is usable for testing {up,down}
            owner (str): Owner of the lock(s)
            lock (str): Lock particular machines
            unlock (str): Unlock particular machine
            machine_type (str): Type of machine to lock
        """
        # Set teuthology lock command
        cmd = "teuthology-lock"

        # Check for --list parameter
        cmd += " --list" if list else ""

        # Check for --locked parameter
        cmd += " --locked true" if locked else ""

        # Check for --status parameter
        cmd += f" --status {status}" if status else ""

        # Check for --owner parameter
        cmd += f" --owner {owner}" if owner else ""

        # Check for --lock parameter
        cmd += f" --lock {lock}" if lock else ""

        # Check for --unlock parameter
        cmd += f" --unlock {unlock}" if unlock else ""

        # Check for --machine-type parameter
        cmd += f" --machine-type {machine_type}" if machine_type else ""

        # Set teuthology environment
        cmd = f"{self._env}/{cmd}"

        # Execute command
        return self._client.run(cmd)

    def _teuthology_reimage(self, name, os_type, os_version, interval=30, timeout=1800):
        """Reimage node

        Args:
            name (str): Name of node
            os_type (str): OS type
            os_version (str): OS version
            inverval (int): Interval to retry
            timeout (int): Retry timeout
        """
        # Set teuthology lock command
        cmd = f"{self._env}/teuthology-reimage --os-type {os_type} --os-version {os_version} {name}"

        # Execute command
        return self._client.run_async(cmd=cmd, interval=interval, timeout=timeout)

    def unlock_node(self, name):
        """Unlock node from inventory

        Args:
            name (str): Name of node
        """
        # Set expected messages
        success = f"teuthology.lock.ops:unlocked: {name}"
        reason = "reason: Cannot unlock an already-unlocked node"

        # Unlock node
        _, stderr = self._teuthology_lock(unlock=name, owner=self._owner)

        # Check for expected messages
        if success in stderr or reason in stderr:
            LOG.info(f"Node '{name}' unlocked sucessfully")
            return True

        # DEBUG. Failed
        LOG.error(f"Failed to unlock node due to error -\n{stderr}")
        return False

    def lock_node(self, name):
        """lock node from inventory

        Args:
            name (str): Name of node
        """
        # Set expected messages
        # success = f"teuthology.lock.ops:locked: {name}"
        reason = "reason: Cannot lock an already-locked node"

        # lock node
        _, stderr = self._teuthology_lock(lock=name, owner=self._owner)

        # Check for expected messages
        if reason in stderr:
            LOG.info(f"Node '{name}' locked sucessfully")
            return True

        # DEBUG. Failed
        LOG.error(f"Failed to lock node due to error -\n{stderr}")
        return False

    def get_node_details_by_name(self, name):
        """Get node details

        Args:
            name (str): Name of node
        """
        stdout, _ = self._teuthology_lock(list=True)

        # Check for ouotput
        if not stdout:
            LOG.error("No hosts are available on octa lab")

        # Check for name in list of nodes
        for node in json.loads(stdout):
            if name in node.get("name"):
                return node

        return {}

    def get_node_id(self, name):
        """Get node id

        Args:
            name (str): Name of node
        """
        return self.node_details_by_name(name=name).get("name")

    def get_node_state_by_name(self, name):
        """Get node state

        Args:
            name (str): Name of node
        """
        return self.get_node_details_by_name(name=name).get("up")

    def get_node_public_ips(self, name):
        """Get node public IP address

        Args:
            name (str): Name of node
        """
        # TODO (vamahaja): Add steps to get public IPs
        pass

    def get_node_private_ips(self, name):
        """Get node private IP address

        Args:
            name (str): Name of node
        """
        # TODO (vamahaja): Add steps to get private IPs
        pass

    def get_node_volumes(self, name):
        """Get volumes attached to node

        Args:
            name (str): Name of node
        """
        # TODO (vamahaja): Add steps to get volumes attached
        pass

    def create_node(self, name, os_type, os_version, interval=30, timeout=1800):
        """Configure node with OS

        Args:
            name (str): Name of node
            os_type (str): OS type
            os_version (str): OS version
            inverval (int): Interval to retry
            timeout (int): Retry timeout
        """
        # Reimage node
        self._teuthology_reimage(
            name=name,
            os_type=os_type,
            os_version=os_version,
            interval=interval,
            timeout=timeout,
        )

        # Validate node details
        node = self.get_node_details_by_name(name=name)
        if not (
            node.get("os_type") == os_type
            and node.get("os_version") == str(os_version)
            and node.get("up")
        ):
            raise UnexpectedStateError(f"Failed to reimage node {name}")

        return True

    def get_inventory(self):
        """Read node configs"""
        # Load inventory file
        inventory = load_config(self._inventory)

        # Get versions id
        version_id = inventory.get("version_id")

        # Get id
        _id = inventory.get("id")

        # Check for image details
        nodes = inventory.get("instance", {}).get("nodes")
        if not nodes:
            raise ConfigError("Mandatory parameter 'nodes' not available in inventory")

        # Check for instance config
        cloud_data = inventory.get("setup")

        return {
            "version_id": version_id,
            "id": _id,
            "nodes": nodes,
            "cloud_data": cloud_data,
        }
