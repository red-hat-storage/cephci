"""Collects the Baremetal information and creates the cephNode object."""
from copy import deepcopy
from datetime import datetime, timedelta
from os.path import expanduser
from re import search
from sys import argv
from time import sleep
from typing import List, Optional

from ceph.ceph import SSHConnectionManager
from utility.log import Log
from utility.utils import get_cephci_config

LOG = Log(__name__)


class CephBaremetalNode:
    """Represent the Baremetal Node."""

    # pylint: disable=too-many-instance-attributes

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
        self.teuthology_creds = get_cephci_config()["teuthology"]
        self.conn = None

        # Re-image the node to desired os version
        self.node = params.get("hostname")

        # Parse the args to get the os details
        self.os_details = argv[argv.index("--rhel-version") + 1]

        # Parse the teuthology framework related data from config
        teuthology_infra_node = self.teuthology_creds.get("infra_node")
        username = self.teuthology_creds.get("username")
        password = self.teuthology_creds.get("password")

        # Create ssh conn obj
        r_conn = SSHConnectionManager(teuthology_infra_node, username, password)
        try:
            self.conn = r_conn.get_client()
        except Exception as conn_excep:
            print(
                "Failed to establish ssh connection to {node} "
                "machine with {error}Aborting!".format(
                    node=self.node, error=str(conn_excep)
                )
            )
            exit(1)

        # Re-image the node
        self._re_image_node()

        # Create root password and enable remote ssh login via magna006
        self._create_root_credentials_and_remote_login()

        # Perform wipefs (for disk in lsblk, do wipefs)
        self._perform_wipefs()

        # Close connection
        self.conn.close()

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
            _, _, err = self.rssh().exec_command(
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

    def _re_image_node(self):
        """
        Re-images a node to the specified rhel version.
        The method uses the 'teuthology' framework to handle re-imaging of nodes
        :return:NIL
        """

        # Identify the linux flavour and version to which the node has to be re-imaged
        os_type = "rhel"
        os_version = self.os_details

        # Check if the node is locked/unlocked. Lock it under teuthology_infra_node user
        self._unlock_and_lock_node()

        re_image_cmd = (
            "teuthology-reimage --os-type {os_type} "
            "--os-version {os_version} {node}".format(
                os_type=os_type, os_version=os_version, node=self.node
            )
        )
        _, out, err = self._execute_command_in_venv(re_image_cmd)
        LOG.info(out.read().decode(encoding="UTF-8"))
        LOG.info(err.read())

        """
       TODO: The error check is now disabled as teuthology at times returns
             "teuthology.exceptions.MaxWhileTries:"
             even after successful reimage. Once the issue is fixed, the below
             check can be included
       if err:
           LOG.error(err.read().decode(encoding="UTF-8"))
           print("Failure happened while re-imaging {node}. Exiting!".format(node=node))
           LOG.debug(out.read().decode(encoding="UTF-8"))
           exit(1)
       """

        # Validate the os_version is updated
        if self._get_os_version() != os_version:
            LOG.error("{} not re-imaged to desired os version.".format(self.node))
            exit(1)

        # Re-lock the node back to user
        self._relock_node()
        LOG.info(
            "Re-image successfull. The cuurent os-version of {node} "
            "is {os}".format(node=self.node, os=os_version)
        )

    def _unlock_and_lock_node(self):
        """
        Fetch the current lock status of the nodes. Unlock if from the
        previous user, lock it under the teuthology creds.
        :return: True/False
        """
        t_user = "{}@{}".format(
            self.teuthology_creds.get("username"), self.teuthology_creds.get("hostname")
        )

        # Get the lock status of the node
        lock_status = self._get_node_lock_status()

        # Check 1: Verify the node is already locked, if yes, proceed to unlock, and finally lock
        if lock_status["is_locked"]:
            locked_user = lock_status["locked_user"]
            self.node_owner = locked_user
            # Now that the machine is locked, check if its already locked by the teuthology user
            if locked_user != t_user:
                # If not, then perform unlock
                LOG.debug("{} is currently locked by {}".format(self.node, locked_user))

                # Unlock the node using the current locked user data
                cmd = "teuthology-lock --unlock --owner {} {}".format(
                    locked_user, self.node
                )
                _, out, _ = self._execute_command_in_venv(cmd)
                print(out.read().decode(encoding="UTF-8"))
            else:
                LOG.debug(
                    "The node is already locked by teuthology user {}."
                    "Skipping the locking steps".format(t_user)
                )
                return

        # Now that the machine is unlocked, perform lock using t_user
        cmd = "teuthology-lock --lock --owner {} {}".format(t_user, self.node)
        _, out, _ = self._execute_command_in_venv(cmd)
        print(out.read().decode(encoding="UTF-8"))

        # Verify the lock command has been successful
        lock_status = self._get_node_lock_status()
        if not (lock_status["is_locked"] and lock_status["locked_user"] == t_user):
            LOG.error(
                "Lock failed. {} still under {}".format(
                    self.node, lock_status["locked_user"]
                )
            )
            exit(1)

        # The lock command executed successful
        LOG.debug("{} successfully locked by {}".format(self.node, t_user))

    def _relock_node(self):
        """
        Relock the node to the actual owner. This step is done to ensure that the node is assigned back to
        user and not reserved under the teuthology master node.
        :return: (bool) based on lock status
        """
        # Unlock the node from teuthology master node
        # Unlock the node using the current locked user data
        locked_user = "{}@{}".format(
            self.teuthology_creds.get("username"), self.teuthology_creds.get("hostname")
        )
        cmd = "teuthology-lock --unlock --owner {} {}".format(locked_user, self.node)
        _, out, _ = self._execute_command_in_venv(cmd)
        print(out.read().decode(encoding="UTF-8"))
        print("Node Unlocked\n")

        # Lock it back to the user
        cmd = "teuthology-lock --lock --owner {} {}".format(self.node_owner, self.node)
        _, out, _ = self._execute_command_in_venv(cmd)
        print(out.read().decode(encoding="UTF-8"))

        # Verify the lock command has been successful
        lock_status = self._get_node_lock_status()
        if not (
            lock_status["is_locked"] and lock_status["locked_user"] == self.node_owner
        ):
            LOG.error(
                "Lock failed. {} still under {}".format(
                    self.node, lock_status["locked_user"]
                )
            )
            exit(1)

        LOG.debug(
            "{} successfully locked back to {}".format(self.node, self.node_owner)
        )

    def _execute_command_in_venv(self, command):
        """
        Executes the command inside venv, deactivate the venv after execution, returns the result
        :param command: command to execute
        :return: res,out,err
        """
        v_env_path = self.teuthology_creds.get("v_env_path")
        v_env_cmd = "source {venv}/bin/activate;{command};deactivate".format(
            venv=v_env_path, command=command
        )
        res, out, err = self.conn.exec_command(
            command=v_env_cmd,
        )
        LOG.debug("Executed ", v_env_cmd)
        return res, out, err

    def _get_node_summary(self):
        """
        Returns teuthology-lock --list {node} output
        :param conn:
        :return:
        """
        lock_list_cmd = "teuthology-lock --list {node}".format(node=self.node)
        _, out, _ = self._execute_command_in_venv(lock_list_cmd)
        lock_summary = out.readlines()
        if not lock_summary:
            LOG.error(
                "Failed to fetch lock status of {node}. Exiting".format(node=self.node)
            )
            exit(1)
        return lock_summary

    def _get_node_lock_status(self):
        """
        Checks if the node is locked or not
        :return: (dict) lock_status
        """

        lock_status = {}
        lock_summary = self._get_node_summary()

        is_locked = [True for entry in lock_summary if '"locked": true' in entry]
        if is_locked:
            lock_status["is_locked"] = True
            lock_status["locked_user"] = [
                search('"(.*)"', entry.split(":")[-1]).group(1)
                for entry in lock_summary
                if "locked_by" in entry
            ][0]
        else:
            lock_status["is_locked"] = False

        return lock_status

    def _get_os_version(self):
        """
        Parses /etc/os-release and returns the os version
        :return: (str) OS Version (e.g: 8.5, 7.9 etc)
        """
        cmd = "cat /etc/os-release"
        _, out, err = self.conn.exec_command(
            command=cmd,
        )
        if err:
            LOG.error(err.read().decode(encoding="UTF-8"))

        # If the os_version is not same as the one specified, raise error and exit
        os_version = search('"(.*)"', out.readlines()[4]).group(1)
        LOG.info("OS details of {} : {}".format(self.node, os_version))
        return os_version

    def _create_root_credentials_and_remote_login(self):
        """
        Creates a root login in the node and enable root login via ssh
        Steps:
        1. ssh to the machine using the predefined oub key
        2. Set root passwd (default to "passswd")
        3. Update /etc/ssh/sshd_config with "PermitRootLogin Yes"
        4. Restart sshd service
        :return: NIL
        """
        cmd = (
            'ssh -i ~/.ssh/magna_002_rsa ubuntu@{node} \'echo "passwd" | '
            'sudo passwd --stdin root;grep -qxF "PermitRootLogin yes" '
            '/etc/ssh/sshd_config || echo "PermitRootLogin yes" | '
            "sudo tee -a /etc/ssh/sshd_config;"
            "sudo service sshd restart' ".format(node=self.node)
        )
        _, _, err = self.conn.exec_command(
            command=cmd,
        )
        if err:
            LOG.error(err.read().decode(encoding="UTF-8"))

        # Verify ssh over root is successful
        test_root_conn = SSHConnectionManager(self.node, "root", "passwd")
        conn = None
        try:
            conn = test_root_conn.get_client()
        except Exception as conn_excep:
            LOG.error(
                "Failed to establish ssh connection over root user to {node} "
                "machine with {error}Aborting!".format(
                    node=self.node, error=str(conn_excep)
                )
            )
            conn.close()
            exit(1)
        conn.close()
        LOG.info(
            "Creation of root user and ssh remote login successful for {}".format(
                self.node
            )
        )

    def _perform_wipefs(self):
        """
        Perform wipefs on nodes where cephfs entries are found
        root@argo026 ~]# lsblk
        NAME                             MAJ:MIN RM   SIZE RO TYPE MOUNTPOINT
        sda                              8:0    0 447.1G  0 disk
        └─sda1                           8:1    0 447.1G  0 part /
        sdb                              8:16   0   1.8T  0 disk
        └─ceph--41d33927--a3ff...        253:1    0   1.8T  0 lvm
        nvme0n1                          259:0    0 931.5G  0 disk
        └─ceph--7050dc1e--428d-...       253:0    0 931.5G  0 lvm
        :return:
        """

        get_all_disks = "lsblk -o NAME -d"
        get_root_disk = (
            r"lsblk -oMOUNTPOINT,PKNAME -rn | awk '$1 ~ /^\/$/ { print $2 }'"
        )
        root_conn = SSHConnectionManager(self.node, "root", "passwd")
        conn = None
        try:
            conn = root_conn.get_client()
        except Exception as conn_excep:
            LOG.error(
                "Failed to establish ssh connection over root user to {node} "
                "machine with {error}Aborting!".format(
                    node=self.node, error=str(conn_excep)
                )
            )
            conn.close()
            exit(1)

        # Get all disks
        _, out, _ = conn.exec_command(
            command=get_all_disks,
        )
        all_disks = out.readlines()[1:]

        # Get root disk
        _, out, _ = conn.exec_command(
            command=get_root_disk,
        )
        root_disk = out.readlines()

        # For all the non root disk, perform wipefs
        for non_root_disk in set(all_disks) - set(root_disk):
            disk = non_root_disk.strip()
            cmd = "wipefs -a --force /dev/{}".format(disk)
            _, out, _ = conn.exec_command(
                command=cmd,
            )
            LOG.info(out.readlines())

        LOG.info("wipefs completed on all disks")

        # Perform a reboot - keep 600 secs
        conn = self._reboot_node(conn)

        # Verify the wipefs is successful by grepping for cephs in lsblk
        cmd = "lsblk | grep ceph"
        _, out, _ = conn.exec_command(
            command=cmd,
        )

        res = out.readlines()
        if res:
            LOG.error(
                "Ceph entry found in disk even after wipefs on {}."
                "Aborting!!".format(self.node)
            )
            exit(1)
        conn.close()
        LOG.info("Wipefs successful. No cephfs entry found on disks")

    def _reboot_node(self, conn):
        """
        Reboots the node and waits till reboot is complete
        :return: NIL
        """
        timeout = 600
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=timeout)

        conn.exec_command("/sbin/reboot -f > /dev/null 2>&1 &")
        LOG.info("Rebooted {}. Waiting for 60 secs".format(self.node))
        sleep(60)
        is_connected = False
        while datetime.now() < end_time:
            try:
                root_conn = SSHConnectionManager(self.node, "root", "passwd")
                conn = root_conn.get_client()
                _, out, _ = conn.exec_command("hostname")
                LOG.info("Connected back to {}".format(out.readlines()))
                is_connected = True
                break
            except Exception:
                LOG.info("Reboot on {} not complete. Waiting...".format(self.node))
                sleep(20)
        if not is_connected:
            LOG.error(
                "Timeout of 600 seconds reached. Reboot failed on {}."
                "Exiting!!".format(self.node)
            )
            exit(1)
        else:
            LOG.info("Reboot complete on {}".format(self.node))
        return conn

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
