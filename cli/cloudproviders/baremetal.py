import json
import socket

from cli.connectible.remote import Remote
from cli.exceptions import CloudProviderError, ConfigError, UnexpectedStateError
from utility.log import Log

LOG = Log(__name__)


class Baremetal:
    """Insterface for Baremetal operations"""

    def __init__(self, timeout=600, **config):
        """Initialize instance using provided details

        Args:
            timeout (int): Socket timeout

        **Kwargs:
            server (str): Server IP or Hostname
            env (str): Environment where teuthology is setup
            owner (str): Owner of nodes
            port (int): Server port number
            ssh_key (str): SSH Key path
            username (str): Name of user to be connected
            password (str): Password of user
            inventory (str): Octa lab hdd inventory
        """
        try:
            LOG.info(
                f"Connecting to server {config['server']} for user {config['username']}"
            )
            # Set teuthology hostname configs
            self._env = config["env"].strip("/")
            self._owner = config["owner"]
            self._timeout = timeout

            # Set node inventory
            self._inventory = config.get("inventory")

            # Connect to teuthology node
            self._client = Remote(
                config["server"],
                config.get("ssh_key"),
                config["username"],
                config.get("password"),
            )
        except KeyError:
            msg = "Insufficient config related to Cloud provider"
            LOG.error(msg)
            raise ConfigError(msg)

        except Exception as e:
            LOG.error(e)
            raise CloudProviderError(e)

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
        return self.get_node_details_by_name(name=name).get("name")

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
        return socket.gethostbyname(self.get_node_id(name))

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

    def create_node(self, name, interval=30, timeout=1800, **config):
        """Configure node with OS

        Args:
            name (str): Name of node
            inverval (int): Interval to retry
            timeout (int): Retry timeout

        **Kwargs:
            os_type (str): OS type
            os_version (str): OS version
        """
        # Reimage node
        try:
            self._teuthology_reimage(
                name=name,
                os_type=config["os_type"],
                os_version=config["os_version"],
                interval=interval,
                timeout=timeout,
            )
        except Exception as e:
            LOG.error(e)
            raise CloudProviderError(e)

        # Validate node details
        node = self.get_node_details_by_name(name=name)
        if not (
            node.get("os_type") == config.get("os_type")
            and node.get("os_version") == str(config.get("os_version"))
            and node.get("up")
        ):
            raise UnexpectedStateError(f"Failed to reimage node {name}")

        return True
