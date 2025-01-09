from cli import Cli
from cli.utilities.utils import build_cmd_from_args


class Earmark(Cli):
    """This module provides CLI interface for FS subvolume related operations"""

    def __init__(self, nodes, base_cmd):
        super(Earmark, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} subvolume earmark"

    def set(self, volume, subvolume_name, earmark, **kwargs):
        """
        Sets an earmark to the subvolume
        Args:
            volume (str): Name of vol where subvol is present
            subvolume (str): subvol name
            earmark (str): earmark name
        """
        cmd = f"{self.base_cmd} set {volume} {subvolume_name} --earmark {earmark}{build_cmd_from_args(**kwargs)}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def get(self, volume, subvolume_name, **kwargs):
        """
        Gets earmark from subvolume, if earmark is already present
        Args:
            volume (str): Name of vol where subvol is present
            subvolume (str): subvol name
        """
        cmd = f"{self.base_cmd} get {volume} {subvolume_name}{build_cmd_from_args(**kwargs)}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def remove(self, volume, subvolume_name, **kwargs):
        """
        Remove the earmark from subvolume
        Args:
            volume (str): Name of vol where subvol is present
            subvolume (str): subvol name
        """
        cmd = f"{self.base_cmd} rm {volume} {subvolume_name}{build_cmd_from_args(**kwargs)}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
