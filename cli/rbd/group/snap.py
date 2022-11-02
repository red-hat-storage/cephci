class Snap:
    """
    This module provides CLI interface to manage group snapshots.
    """

    def __init__(self, nodes, base_cmd):
        self.base_cmd = base_cmd + " snap"

    def create(self, **kw):
        """
        This method is used to make a snapshot of a group.
        Args:
          kw(dict): Key/value pairs that needs to be provided to the installer
          Example:
            Supported keys:
              group-snap-spec(str): group snap specification.

        Returns:
          Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        kw = kw.get("kw")
        group_snap_spec = kw.get("group-snap-spec")
        cmd = self.base_cmd + " create" + f" {group_snap_spec}"

        return self.execute(cmd=cmd)

    def list_(self, **kw):
        """
        This method is used to list snapshots of a group.
        Args:
          kw(dict): Key/value pairs that needs to be provided to the installer
          Example:
            Supported keys:
              group-spec(str): group specification.([pool-name/[namespace-name/]]group-name])

        Returns:
          Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        kw = kw.get("kw")
        group_spec = kw.get("group-spec")
        cmd = self.base_cmd + " list" + f" {group_spec}"

        return self.execute(cmd=cmd)

    def rm(self, **kw):
        """
        This method is used to remove a snapshot from a group.
        Args:
          kw(dict): Key/value pairs that needs to be provided to the installer
          Example:
            Supported keys:
              group-snap-spec(str): group snap specification.

        Returns:
          Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        kw = kw.get("kw")
        group_snap_spec = kw.get("group-snap-spec")
        cmd = self.base_cmd + " rm" + f" {group_snap_spec}"

        return self.execute(cmd=cmd)

    def rename(self, **kw):
        """
        This method is used to rename group’s snapshot.
        Args:
          kw(dict): Key/value pairs that needs to be provided to the installer
          Example:
            Supported keys:
              group-snap-spec(str): group snap specification.
              snap-name(str): snap name.

        Returns:
          Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        kw = kw.get("kw")
        group_snap_spec = kw.get("group-snap-spec")
        snap_name = kw.get("snap-name")
        cmd = self.base_cmd + " rename" + f" {group_snap_spec} {snap_name}"

        return self.execute(cmd=cmd)

    def rollback(self, **kw):
        """
        This method is used to rollback group to snapshot.
        Args:
          kw(dict): Key/value pairs that needs to be provided to the installer
          Example:
            Supported keys:
              group-snap-spec(str): group snap specification.

        Returns:
          Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        kw = kw.get("kw")
        group_snap_spec = kw.get("group-snap-spec")
        cmd = self.base_cmd + " rollback" + f" {group_snap_spec}"

        return self.execute(cmd=cmd)
