from .mirror.schedule import Schedule


class Snapshot:
    """
    This module provides CLI interface to manage the mirror snapshots.
    """

    def __init__(self, nodes, base_cmd):
        self.base_cmd = base_cmd + " snapshot"
        self.schedule = Schedule(nodes, self.base_cmd)
