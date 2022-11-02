from .group.image import Image
from .group.snap import Snap


class Group:
    """
    This module provides CLI interface to modify group specs via rbd group command.
    """

    def __init__(self, nodes, base_cmd):
        self.base_cmd = base_cmd + " group"
        self.image = Image(nodes, self.base_cmd)
        self.snap = Snap(nodes, self.base_cmd)

    def create(self, **kw):
        """
        This method is used to create a group.
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
        cmd = self.base_cmd + " create" + f" {group_spec}"

        return self.execute(cmd=cmd)

    def ls(self, **kw):
        """
        This method is used to list rbd groups.
        Args:
          kw(dict): Key/value pairs that needs to be provided to the installer
          Example:
            Supported keys:
              pool-name(str): name of the pool.

        Returns:
          Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        kw = kw.get("kw")
        pool_name = kw.get("pool_name")
        cmd = self.base_cmd + " ls --pool-name" + f" {pool_name}"

        return self.execute(cmd=cmd)

    def rename(self, **kw):
        """
        This method is used to rename a group.
        Note: rename across pools is not supported.
        Args:
          kw(dict): Key/value pairs that needs to be provided to the installer
          Example:
            Supported keys:
              src-group-spec(str) : source group specification.
              dest-group-spec(str): destination group specification.

        Returns:
          Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        kw = kw.get("kw")
        src_group_spec = kw.get("src-group-spec")
        dest_group_spec = kw.get("dest-group-spec")
        cmd = self.base_cmd + " rename" + f" {src_group_spec} {dest_group_spec}"

        return self.execute(cmd=cmd)

    def rm(self, **kw):
        """
        This method is used to delete a group.
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
        cmd = self.base_cmd + " rm" + f" {group_spec}"

        return self.execute(cmd=cmd)
