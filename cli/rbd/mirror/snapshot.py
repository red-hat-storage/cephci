from cli import Cli
from cli.rbd.mirror.schedule import Schedule


class Snapshot(Cli):
    """
    This module provides CLI interface to manage the mirror snapshots.
    """

    def __init__(self, nodes, base_cmd):
        super(Snapshot, self).__init__(nodes)
        self.base_cmd = base_cmd + " snapshot"
        self.schedule = Schedule(nodes, self.base_cmd)
