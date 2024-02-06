from socket import socket

from paramiko.ssh_exception import SSHException

from ceph.ceph import SocketTimeoutException, SSHConnectionManager
from utility.log import Log

log = Log(__name__)


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
        root_connection = SSHConnectionManager(ip, user, user_creds["password"])
        win_clients.append(root_connection.get_client)

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
