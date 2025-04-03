from copy import deepcopy

from cli import Cli
from cli.rbd.mirror.snapshot import Snapshot
from cli.utilities.utils import build_cmd_from_args


class Group(Cli):
    """
    This module provides CLI interface to manage the mirror group
    """

    def __init__(self, nodes, base_cmd):
        super(Group, self).__init__(nodes)
        self.base_cmd = base_cmd + " group"
        self.snapshot = Snapshot(nodes, self.base_cmd)

    def enable(self, **kw):
        """Wrapper for rbd mirror group enable.
        Args
            kw: Key value pair of method arguments
        Example::
        Supported keys:
            group-spec: poolname/[namespace]/groupname
            mode: mirror group mode [default: snapshot]
            See rbd help mirror group enable for more supported keys
        """
        kw_copy = deepcopy(kw)
        group_spec = kw_copy.pop("group-spec", "")
        mirrormode = kw_copy.pop("mirrormode", "snapshot")
        cmd = f"{self.base_cmd} enable {group_spec} {mirrormode} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def disable(self, **kw):
        """Wrapper for rbd mirror group disable.
        Args
            kw: Key value pair of method arguments
        Example::
        Supported keys:
            group-spec: poolname/[namespace]/groupname
            See rbd help mirror group disable for more supported keys
        """
        kw_copy = deepcopy(kw)
        group_spec = kw_copy.pop("group-spec", "")
        cmd = f"{self.base_cmd} disable {group_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def promote(self, **kw):
        """Wrapper for rbd mirror group promote.
        Args
            kw: Key value pair of method arguments
        Example::
        Supported keys:
            group-spec: poolname/[namespace]/groupname
            See rbd help mirror group promote for more supported keys
        """
        kw_copy = deepcopy(kw)
        group_spec = kw_copy.pop("group-spec", "")
        cmd = f"{self.base_cmd} promote {group_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def demote(self, **kw):
        """Wrapper for rbd mirror group demote.
        Args
            kw: Key value pair of method arguments
        Example::
        Supported keys:
            group-spec: poolname/[namespace]/groupname
            See rbd help mirror group demote for more supported keys
        """
        kw_copy = deepcopy(kw)
        group_spec = kw_copy.pop("group-spec", "")
        cmd = f"{self.base_cmd} demote {group_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def resync(self, **kw):
        """Wrapper for rbd mirror group resync
        Args
            kw: Key value pair of method arguments
        Example::
        Supported keys:
            group-spec: poolname/[namespace]/groupname
            See rbd help mirror group resync for more supported keys
        """
        kw_copy = deepcopy(kw)
        group_spec = kw_copy.pop("group-spec", "")
        cmd = f"{self.base_cmd} resync {group_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def status(self, **kw):
        """Wrapper for rbd mirror group status.
        Args
            kw: Key value pair of method arguments
        Example::
        Supported keys:
            group-spec: poolname/[namespace]/groupname
            See rbd help mirror group status for more supported keys
        """
        kw_copy = deepcopy(kw)
        group_spec = kw_copy.pop("groupspec", "")
        cmd = f"{self.base_cmd} status {group_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)
