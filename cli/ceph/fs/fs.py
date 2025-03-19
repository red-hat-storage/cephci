from cli import Cli

from .sub_volume_group import SubVolumeGroup
from .subvolume.sub_volume import SubVolume
from .volume import Volume


class Fs(Cli):
    """This module provides CLI interface for FS related operations"""

    def __init__(self, nodes, base_cmd):
        super(Fs, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} fs"
        self.volume = Volume(nodes, self.base_cmd)
        self.sub_volume_group = SubVolumeGroup(nodes, self.base_cmd)
        self.sub_volume = SubVolume(nodes, self.base_cmd)

    def get(self, conf, format=None):
        """
        Returns the details of given conf
        Args:
            conf (str): Given conf
            format (str): Required format of the output
        """
        cmd = f"{self.base_cmd} get {conf}"
        if format:
            cmd += f" --format={format}"
        out = self.execute(sudo=True, check_ec=False, long_running=False, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
