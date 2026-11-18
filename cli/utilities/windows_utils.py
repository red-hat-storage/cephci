"""Helpers for the Windows NFS/SMB clients.

NOTE: Windows is the one remaining platform in this framework that still
authenticates over SSH with a password. Ceph (Linux) nodes are key-only --
``ceph.ceph.SSHConnectionManager`` does not accept a password at all. The
password handling needed by the Windows boxes is kept here, isolated and
explicit, rather than in the shared connection machinery.

Migrating these clients to public-key auth (Windows OpenSSH supports it via
``administrators_authorized_keys``) is an infrastructure task; once it is
done, this module can drop to the common key-based path.
"""

import datetime
from socket import socket
from time import sleep

import paramiko
from paramiko.ssh_exception import SSHException

from ceph.ceph import CephNode, SocketTimeoutException
from utility.log import Log

log = Log(__name__)


class WindowsSSHConnectionManager(object):
    """Password-authenticated SSH connection, for Windows clients only.

    Do not use this for Ceph nodes -- those are key-only. See the module
    docstring for why this exists.
    """

    def __init__(self, ip_address, username, password, outage_timeout=600):
        self.ip_address = ip_address
        self.username = username
        self.password = password
        self.__client = paramiko.SSHClient()
        self.__client.set_missing_host_key_policy(paramiko.MissingHostKeyPolicy())
        self.__transport = None
        self.outage_timeout = datetime.timedelta(seconds=outage_timeout)

    @property
    def client(self):
        return self.get_client()

    def get_client(self):
        if not (self.__transport and self.__transport.is_active()):
            self.__connect()
            self.__transport = self.__client.get_transport()

        return self.__client

    def get_transport(self):
        self.__transport = self.client.get_transport()
        return self.__transport

    @property
    def transport(self):
        return self.get_transport()

    def close(self):
        try:
            if self.__client:
                self.__client.close()
        except Exception:
            pass
        self.__transport = None

    def __connect(self):
        end_time = datetime.datetime.now() + self.outage_timeout
        while end_time > datetime.datetime.now():
            try:
                self.__client.connect(
                    hostname=self.ip_address,
                    username=self.username,
                    password=self.password,
                    allow_agent=False,
                    look_for_keys=False,
                )
                log.info(
                    "Connected to windows client %s as %s",
                    self.ip_address,
                    self.username,
                )
                return
            except Exception as e:
                log.warning(
                    "Windows SSH connect failed to %s as %s: %s",
                    self.ip_address,
                    self.username,
                    e,
                )
                sleep(10)

        raise AssertionError(
            f"Unable to establish a connection with windows client {self.ip_address}"
        )


class WindowsCephNode(CephNode):
    """CephNode whose SSH connections authenticate with a password.

    Only for Windows clients. Ceph nodes must use the key-only CephNode.
    """

    def __init__(self, **kw):
        password = kw.pop("password")
        super().__init__(**kw)
        self._win_password = password
        self._attach_windows_connections()

    def _attach_windows_connections(self):
        manager = WindowsSSHConnectionManager(
            self.ip_address, self.username, self._win_password
        )
        # Windows has no privileged second account here; both handles point at
        # the same session, so `sudo=True` call sites keep working.
        self.root_connection = manager
        self.connection = manager
        self.rssh = manager.get_client
        self.rssh_transport = manager.get_transport
        self.ssh = manager.get_client
        self.ssh_transport = manager.get_transport

    def __setstate__(self, pickle_dict):
        self.__dict__.update(pickle_dict)
        self._attach_windows_connections()


def setup_windows_clients(windows_clients):
    """
    Setup windows clients
    Args:
        windows_clients(dict(dict)): Dict containing ip address and user creds of windows nodes
    """
    count = 0
    windows_clients_obj = []
    for windows_client in windows_clients:
        ceph_nodename = f"windows_client{count}"
        ceph = WindowsCephNode(
            username=windows_client["user"],
            password=windows_client["password"],
            private_key_path=None,
            role=["windows_client"],
            no_of_volumes=None,
            ip_address=windows_client["ip"],
            subnet=None,
            private_ip=None,
            hostname=windows_client["hostname"],
            ceph_vmnode=None,
            ceph_nodename=ceph_nodename,
            id=ceph_nodename,
        )
        windows_clients_obj.append(ceph)
        count = count + 1
    return windows_clients_obj


def establish_windows_client_conn(windows_clients):
    """
    Establishes connection to the given windows clients
    Args:
        windows_clients(dict(dict)): Dict containing ip address and user creds of windows nodes
                               e.g: {"10.70.1.1":{"user":"root",
                                                  "password": "passwd"},
                                     "10.70.1.2":{"user":"user1",
                                                  "password": "passwd"}}
    """
    win_clients = []
    for ip, user_creds in windows_clients.items():
        # If user is not specified, use root
        user = user_creds["user"] if "user" in user_creds.keys() else "root"
        connection = WindowsSSHConnectionManager(ip, user, user_creds["password"])
        win_clients.append(connection.get_client)

    return win_clients


def execute_command_on_windows_node(node, cmd):
    """
    Executes command on given windows node
    Args:
        node(SSHConnectionManager): Node details
        cmd(str): Command to execute
    """
    stdout = str()
    stderr = str()
    _stdout = None
    _stderr = None
    try:
        log.info(f"Running command {cmd} on windows node")
        _, _stdout, _stderr = node().exec_command(cmd, timeout=60)
        for line in _stdout:
            if line:
                stdout += line
        for line in _stderr:
            if line:
                stderr += line
        log.info(f"stdout: {stdout}")
        log.info(f"stderr: {stderr}")
    except socket.timeout as sock_err:
        log.error("socket.timeout happened while connecting to the node")
        node().close()
        raise SocketTimeoutException(sock_err)
    except SSHException as e:
        log.error("SSHException during cmd: %s", str(e))
