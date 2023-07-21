from cli import Cli


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
        # Create the mount point
        cmd = f"{self.base_cmd} -t nfs -o vers={version},port={port} {server}:{export} {mount}"
        out = self.execute(sudo=True, cmd=cmd)
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
