import socket

import paramiko

from cli.exceptions import RemoteConnectionError, UnexpectedStateError
from cli.utilities.waiter import WaitUntil
from utility.log import Log

LOG = Log(__name__)


class Remote:
    """Interface for remote operations.

    Key authentication by default. ``password`` is only honoured when
    ``allow_password=True``, which exists solely for third-party appliances
    that cannot be given an SSH key (see the exemptions noted in the callers).
    Ceph nodes must never use it.
    """

    def __init__(
        self, host, ssh_key=None, username=None, password=None, allow_password=False
    ):
        """Initialize instance using configs

        Args:
            host (str): host IP or Hostname
            ssh_key (str): SSH Key path
            username (str): Name of user to be connected
            password (str): Password of user -- requires allow_password
            allow_password (bool): Opt in to password auth (appliances only)
        """
        # Set host configs
        self._host = host

        # Connect to teuthology node
        self._client = self._connect(host, ssh_key, username, password, allow_password)

    def _connect(
        self, host, ssh_key=None, username=None, password=None, allow_password=False
    ):
        """Connect to host

        Args:
            host (str): host IP or Hostname
            ssh_key (str): SSH Key path
            username (str): Name of user to be connected
            password (str): Password of user -- requires allow_password
            allow_password (bool): Opt in to password auth (appliances only)
        """
        if password and not allow_password:
            raise RemoteConnectionError(
                f"Password authentication is not supported for '{host}'. "
                f"Provide ssh_key instead."
            )

        client = None
        try:
            # Set client object
            client = paramiko.SSHClient()

            # Set missing host key polict
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            # Connect to host
            LOG.debug(f"Connecting to host {host}")
            connect_kw = {"hostname": host, "username": username}
            if allow_password and password:
                connect_kw["password"] = password
            else:
                connect_kw["pkey"] = ssh_key
                connect_kw["look_for_keys"] = True
            client.connect(**connect_kw)
        except Exception as e:
            LOG.error(f"Failed to connect to host '{host}' with error -\n{str(e)}")
            raise RemoteConnectionError(str(e))

        LOG.info(f"Connected to host '{host}' successfully")
        return client

    def run(self, cmd, timeout=600):
        """Execute command on host

        Args:
            cmd (str): Command to be executed
            timeout (int): Socket timeout
        """
        stdout, stderr = "", ""
        try:
            # DEBUG.
            LOG.info(f"[{self._host}] Executing command - {cmd}")

            # Execuet command on node
            _, stdout, stderr = self._client.exec_command(command=cmd, timeout=timeout)

            # Format command output
            stdout = "\n".join(map(lambda x: x.strip(), stdout.readlines()))
            stderr = "\n".join(map(lambda x: x.strip(), stderr.readlines()))
        except Exception as e:
            LOG.error(f"Command '{cmd}' execution failed with error -\n{e}")
            raise RemoteConnectionError(e)

        return stdout, stderr

    def run_async(self, cmd, interval=10, timeout=300):
        """Execute command in background on host

        Args:
            cmd (str): Command to be executed
            inverval (int): Interval to retry
            timeout (int): Retry timeout
        """
        # Set return variables
        stdout, stderr = "", ""

        # DEBUG.
        LOG.info(f"[{self._host}] Executing command in background - {cmd}")

        try:
            # Get transport channel and set configs
            channel = self._client.get_transport().open_session()
            channel.setblocking(0)

            # Execute command on node
            channel.exec_command(command=cmd)
        except Exception as e:
            LOG.error(f"Command '{cmd}' execution failed with error -\n{e}")
            raise RemoteConnectionError(e)

        # Wait for command execution and read output
        for w in WaitUntil(timeout=timeout, interval=interval):
            # Check for command status
            if channel.exit_status_ready():
                LOG.info(f"Command completed successfully within {timeout} sec")
                break

            # Check command output
            while True:
                try:
                    # Get 1kb of output
                    out = channel.recv(1024)
                    if not out:
                        break

                    # Convert bytes to string
                    stdout += out.decode()
                except socket.timeout:
                    break

            LOG.info(f"Command still in progress, waiting for {interval} sec")

        # Raise exception in case command still in progress
        if w.expired:
            raise UnexpectedStateError(
                f"Failed to complete command within {timeout} sec"
            )

        # Read command error
        while True:
            try:
                # Get 1kb of error
                err = channel.recv_stderr(1024)
                if not err:
                    break

                # Convert bytes to string
                stderr += err.decode()
            except socket.timeout:
                break

        return stdout, stderr
