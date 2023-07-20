from cli import Cli


class Volume(Cli):
    """This module provides CLI interface for FS volume related operations"""

    def __init__(self, nodes, base_cmd):
        super(Volume, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} volume"

    def create(self, volume):
        """
        Creates ceph volume
        Args:
            volume (str): Name of vol to be created
        """
        cmd = f"{self.base_cmd} create {volume}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def rm(self, volume, yes_i_really_mean_it=False):
        """
        Removes ceph volume
        Args:
            volume (str): Name of vol to be removed
        """
        cmd = f"{self.base_cmd} rm {volume}"
        if yes_i_really_mean_it:
            cmd += " --yes-i-really-mean-it"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def ls(self):
        """
        List volumes
        """
        cmd = f"{self.base_cmd} ls"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
