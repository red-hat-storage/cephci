from cli import Cli


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

    def fuse(self, mount, client_hostname, **kwargs):
        """
        Perform fuse mount
        Args:
            mount (str): path to mount point
            client_fs (str): ceph client fs name
            extra_params (str): additional parameters for fuse mount
        """
        self.fuse_auth_list(client_hostname)
        # Check if mount dir is present, else create
        out = self.execute(cmd=f"ls {mount}", sudo=True)
        if not out[0]:
            self.execute(cmd=f"mkdir {mount}", sudo=True)

        cmd = f"ceph-fuse -n client.{client_hostname} {mount} "
        if kwargs.get("extra_params"):
            cmd += f"{kwargs.get('extra_params')} "

        out = self.execute(sudo=True, long_running=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def fuse_auth_list(self, client_hostname):
        """
        Get or create auth for fuse mount
        Args:
            client_hostname (str): Hostname of the client for which auth is to be created
        Returns:
            str: Output of the auth command
        """
        try:
            out = self.execute(
                sudo=True,
                cmd=f"ceph auth get-or-create client.{client_hostname} \
                                mon 'allow *' \
                                mds 'allow * path=/' \
                                osd 'allow *' \
                            -o /etc/ceph/ceph.client.{client_hostname}.keyring",
            )
        except Exception as e:
            raise MountFailedError(
                f"Failed to get or create auth for client {client_hostname}: {e}"
            )
        if isinstance(out, tuple):
            return out[0].strip()
        return out


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
