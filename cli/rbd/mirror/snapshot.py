from copy import deepcopy

from cli import Cli
from cli.rbd.mirror.schedule import Schedule
from cli.utilities.utils import build_cmd_from_args


class Snapshot(Cli):
    """
    This module provides CLI interface to manage the mirror snapshots.
    """

    def __init__(self, nodes, base_cmd):
        super(Snapshot, self).__init__(nodes)
        self.base_cmd = base_cmd + " snapshot"
        self.schedule = Schedule(nodes, self.base_cmd)

    def add(self, **kw):
        kw_copy = deepcopy(kw)
        group_spec = kw_copy.pop("group-spec", "")
        cmd = f"{self.base_cmd} {group_spec} {build_cmd_from_args(**kw_copy)}"
        return self.execute_as_sudo(cmd=cmd)
