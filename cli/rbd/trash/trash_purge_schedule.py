from copy import deepcopy

from cli import Cli
from cli.utilities.utils import build_cmd_from_args


class TrashPurgeSchedule(Cli):
    """
    This module provides CLI interface to manage snapshots from images in pool.
    """

    def __init__(self, nodes, base_cmd):
        super(TrashPurgeSchedule, self).__init__(nodes)
        self.base_cmd = base_cmd + " purge schedule"

    def add(self, **kw):
        """
        Add specified trash purge schedule to the given pool
        """
        kw_copy = deepcopy(kw)
        cmd = f"{self.base_cmd} add {build_cmd_from_args(**kw_copy)}"

        return self.execute(cmd=cmd)
