from cli import Cli

from .lvm import Lvm


class CephVolume(Cli):
    """This module provides CLI interface to manage the ceph-volume plugin."""

    def __init__(self, nodes, base_cmd):
        super(CephVolume, self).__init__(nodes)

        self.base_cmd = f"{base_cmd} ceph-volume"
        self.lvm = Lvm(nodes, self.base_cmd)
