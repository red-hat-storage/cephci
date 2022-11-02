from copy import deepcopy

from cli.utilities.utils import build_cmd_args


class Pool:
    """
    This module provides CLI interface to manage pools in rbd via rbd config pool command.
    """

    def __init__(self, nodes, base_cmd):
        self.base_cmd = base_cmd + " pool"

    def set_(self, **kw):
        """
        This method is used to set a pool level configuration override.
        Args:
          kw(dict): Key/value pairs that needs to be provided to the installer
          Example:
            Supported keys:
              pool_name(str): This is a global, client or client id.
              key(str): This is the config key.
              value(str): This is the config value.
        Returns:
          Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        kw = kw.get("kw")
        pool_name = kw.get("pool_name")
        key = kw.get("key")
        value = kw.get("value")
        cmd = self.base_cmd + f" set {pool_name} {key} {value}"

        return self.execute(cmd=cmd)

    def get_(self, **kw):
        """
        This method is used to get a pool level configuration override.
        Args:
          kw(dict): Key/value pairs that needs to be provided to the installer
          Example:
            Supported keys:
              pool_name(str): This is a global, client or client id.
              key(str): This is the config key.

        Returns:
          Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        kw = kw.get("kw")
        pool_name = kw.get("pool_name")
        key = kw.get("key")
        cmd = self.base_cmd + f" get {pool_name} {key}"

        return self.execute(cmd=cmd)

    def list_(self, **kw):
        """
        This method is used to list a pool level configuration override.
        Args:
          kw(dict): Key/value pairs that needs to be provided to the installer
          Example:
            Supported keys
              pool_name(str): This is a global, client or client id.
              key(str): This is the config key.
              format(str): plain | json | xml (Optional).
              pretty-format(str): set this to true to prettify the output.(Optional)

        Returns:
          Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run"""
        kw = kw.get("kw")
        kw_copy = deepcopy(kw)
        pool_name = kw_copy.pop("pool_name")
        cmd_args = build_cmd_args(kw=kw_copy)
        cmd = self.base_cmd + f" list {pool_name}" + cmd_args

        return self.execute(cmd=cmd)

    def remove_(self, **kw):
        """
        This method is used to remove a pool level configuration override.
        Args:
          kw(dict): Key/value pairs that needs to be provided to the installer
          Example:
            Supported keys:
              pool_name(str): This is a global, client or client id.
              key(str): This is the config key.

        Returns:
          Dict(str)
            A mapping of host strings to the given task’s return value for that host’s execution run
        """
        kw = kw.get("kw")
        pool_name = kw.get("pool_name")
        key = kw.get("key")
        cmd = self.base_cmd + f" remove {pool_name} {key}"

        return self.execute(cmd=cmd)
