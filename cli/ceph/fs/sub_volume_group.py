from cli import Cli
from cli.utilities.utils import build_cmd_from_args


class SubVolumeGroup(Cli):
    """This module provides CLI interface for FS subvolume group related operations"""

    def __init__(self, nodes, base_cmd):
        super(SubVolumeGroup, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} subvolumegroup"

    def create(self, volume, group, **kwargs):
        """
        Creates ceph subvolume group
        Args:
            volume (str): Name of vol where subvol group has to be created
            group (str): Name of the subvol group
            kw: Key/value pairs of configuration information to be used in the test.
        """
        cmd = f"{self.base_cmd} create {volume} {group} {build_cmd_from_args(**kwargs)}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def rm(self, volume, group, force=False):
        """
        Removes ceph subvol group
        Args:
            volume (str): Name of vol where subvolgroup has to be removed
            group (str): subvol group name
            force (bool): Force tag
        """
        cmd = f"{self.base_cmd} rm {volume} {group}"
        if force:
            cmd += " --force"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def ls(self, volume):
        """
        List subvol groups
        Args:
            volume (str): Name of vol where subvolgroup is present
        """
        cmd = f"{self.base_cmd} ls {volume}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def getpath(self, volume, group):
        """
        Get the absolute path of a subvolume group
        Args:
            volume (str): Name of vol where subvolgroup is present
            group (str): subvol group name
        """
        cmd = f"{self.base_cmd} getpath {volume} {group}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
