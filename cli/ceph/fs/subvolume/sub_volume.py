from cli import Cli
from cli.utilities.utils import build_cmd_from_args

from .earmark import Earmark


class SubVolume(Cli):
    """This module provides CLI interface for FS subvolume related operations"""

    def __init__(self, nodes, base_cmd):
        super(SubVolume, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} subvolume"
        self.earmark = Earmark(nodes, base_cmd)

    def create(self, volume, subvolume, **kwargs):
        """
        Creates ceph subvolume
        Args:
            volume (str): Name of vol where subvol has to be created
            subvolume (str): Name of the subvol
            kw: Key/value pairs of configuration information to be used in the test.
        """
        cmd = f"{self.base_cmd} create {volume} {subvolume} {build_cmd_from_args(**kwargs)}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def rm(self, volume, subvolume, group=None, force=False):
        """
        Removes ceph subvol
        Args:
            volume (str): Name of vol where subvol has to be removed
            subvolume (str): subvol group name
            group (str): subvol group name
            force (bool): Force tag
        """
        cmd = f"{self.base_cmd} rm {volume} {subvolume}"
        if group:
            cmd += f" {group}"
        if force:
            cmd += " --force"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def ls(self, volume, group=None):
        """
        List subvol groups
        Args:
            volume (str): Name of vol where subvol is present
            group (str): subvol group name
        """
        cmd = f"{self.base_cmd} ls {volume}"
        if group:
            cmd += f" {group}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def resize(self, volume, subvolume, size, **kwargs):
        """
        Resize a subvolume
        Args:
            volume (str): Name of vol where subvol is present
            subvolume (str): subvol name
            size (str): new size of the sub volume
        """
        cmd = f"{self.base_cmd} resize {volume} {subvolume} {size} {build_cmd_from_args(**kwargs)}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def getpath(self, volume, subvolume, **kwargs):
        """
        Get the absolute path of a subvolume
        Args:
            volume (str): Name of vol where subvol is present
            subvolume (str): subvol name
        """
        cmd = f"{self.base_cmd} getpath {volume} {subvolume} {build_cmd_from_args(**kwargs)}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
