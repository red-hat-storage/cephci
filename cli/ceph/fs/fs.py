from cli import Cli

from .sub_volume import SubVolume
from .sub_volume_group import SubVolumeGroup
from .volume import Volume


class Fs(Cli):
    """This module provides CLI interface for FS related operations"""

    def __init__(self, nodes, base_cmd):
        super(Fs, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} fs"
        self.volume = Volume(nodes, self.base_cmd)
        self.sub_volume_group = SubVolumeGroup(nodes, self.base_cmd)
        self.sub_volume = SubVolume(nodes, self.base_cmd)
