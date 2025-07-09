from cli import Cli
from cli.ceph.auth.auth import Auth
from cli.exceptions import OperationFailedError


class MountFailedError(Exception):
    pass


class Mount(Cli):
    """This module provides CLI support for mount operations"""

    def __init__(self, nodes):
        super(Mount, self).__init__(nodes)
        self.base_cmd = "mount"

    def nfs(self, mount, version, port, server, export):
        """
        Perform nfs mount
        Args:
            mount (str): nfs mount path
            version (str): Nfs version (4.0, 4.1, 4.2 etc)
            port (str): nfs port to bind
            server (str): Nfs server hostname
            export (str): nfs export path)
        """
        # Check if mount dir is present, else create
        out = self.execute(cmd=f"ls {mount}", sudo=True)
        if not out[0]:
            self.execute(cmd=f"mkdir {mount}", sudo=True)

        # Create the mount point
        cmd = f"{self.base_cmd} -t nfs -o vers={version},port={port} {server}:{export} {mount}"
        self.execute(sudo=True, long_running=True, cmd=cmd)

        out = self.execute(sudo=True, cmd="mount")
        if isinstance(out, tuple):
            out = out[0]

        if not mount.rstrip("/") in out:
            raise MountFailedError(f"Nfs mount failed: {out}")


class Unmount(Cli):
    def __init__(self, nodes):
        super(Unmount, self).__init__(nodes)
        self.base_cmd = "umount"

    def unmount(self, mount, lazy=True):
        """
        Perform unmount of volume
        Args:
            mount (str): path to mount point
            lazy (bool): Perform a lazy unmount or not
        Returns:

        """
        cmd = f"{self.base_cmd} {mount}"
        if lazy:
            cmd += " -l"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out


class FuseMount(Cli):
    """Supports CephFS mounts using ceph-fuse."""

    def __init__(self, nodes):
        super().__init__(nodes)
        self.base_cmd = "ceph-fuse"
        self.auth_tool = Auth(nodes, base_cmd="ceph")

    def mount(self, mount_point, client_hostname, extra_params=None):
        """
        Mount CephFS using FUSE.

        Args:
            mount_point (str): Local path to mount CephFS.
            client_hostname (str): Ceph client ID (host shortname).
            extra_params (str): Optional ceph-fuse arguments.

        Returns:
            str: Mount command's STDOUT on success.

        Raises:
            FuseMountError: On authentication or mount failure.
        """
        # Ensure keyring exists
        try:
            self.auth_tool.get_or_create_client_keyring(client_hostname)
        except Exception as e:
            raise OperationFailedError(
                f"Failed to prepare auth for client.{client_hostname}: {e}"
            )

        # Ensure mount directory exists
        out = self.execute(cmd=f"ls {mount_point}", sudo=True)
        if not out[0]:
            self.execute(cmd=f"mkdir -p {mount_point}", sudo=True)

        # Run ceph-fuse command
        cmd = f"{self.base_cmd} -n client.{client_hostname} {mount_point}"
        if extra_params:
            cmd += f" {extra_params}"

        out = self.execute(sudo=True, long_running=True, cmd=cmd)
        stdout = out[0].strip() if isinstance(out, tuple) else out
        return stdout
