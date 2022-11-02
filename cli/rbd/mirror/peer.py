from copy import deepcopy

from cli.utilities.utils import build_cmd_args

from .mirror.bootstrap import Bootstrap


class Peer:
    """This module provides CLI interface to manage rbd mirror pool commands."""

    def __init__(self, nodes, base_cmd):
        self.base_cmd = base_cmd + " peer"
        self.bootstrap = Bootstrap(nodes, self.base_cmd)

    def add_(self, **kw):
        """Wrapper for rbd mirror pool peer add.

        Command is used for adding a mirror pool peer
        Args:
            kw: Key value pair of method arguments
            Example::
            Supported keys:
                pool_name: Name of the pool.
                pool_name: Name of the pool. (--pool <name>)
                remote_cluster_spec: remote cluster specification - <client-name>@<cluster-name>
                remote_client_name: remote client name
                remote_cluster: remote cluster name
                remote_mon_host: remote mon host(s)
                remote_key_file: remote key file path
                direction: mirroring direction (rx-only, rx-tx)
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")

        kw_copy = deepcopy(kw)
        pool_name = kw_copy.pop("pool_name", "")
        remote_cluster_spec = kw_copy.pop("remote_cluster_spec", "")
        cmd_args = build_cmd_args(kw=kw_copy)

        cmd = f"{self.base_cmd} add {pool_name} {remote_cluster_spec}{cmd_args}"

        return self.execute(cmd=cmd)

    def remove_(self, **kw):
        """Wrapper for rbd mirror pool peer remove.

        Command is used to remove a mirroring peer from a pool.

        Args:
            kw: Key value pair of method arguments
            Example::
            Supported keys:
                pool_name: Name of the pool.
                pool_name: Name of the pool. (--pool <name>)
                uuid: peer uuid

        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")

        kw_copy = deepcopy(kw)
        pool_name = kw_copy.pop("pool_name", "")
        uuid = kw_copy.pop("uuid", "")
        cmd_args = build_cmd_args(kw=kw_copy)

        cmd = f"{self.base_cmd} remove {pool_name} {uuid}{cmd_args}"

        return self.execute(cmd=cmd)

    def set_(self, **kw):
        """Wrapper for rbd mirror pool peer set.

        This command is used to update mirroring peer settings.

        Args:
            kw: Key value pair of method arguments
            Example::
            Supported keys:
                pool_name: Name of the pool.
                pool_name: Name of the pool. (--pool <name>)
                uuid: peer uuid
                key: peer parameter
                (direction, site-name, client, mon-host, key-file)
                value: new value for specified key

        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")

        kw_copy = deepcopy(kw)
        pool_name = kw_copy.pop("pool_name", "")
        uuid = kw_copy.pop("uuid", "")
        key = kw_copy.pop("key", "")
        value = kw_copy.pop("value", "")
        cmd_args = build_cmd_args(kw=kw_copy)

        cmd = f"{self.base_cmd} set {pool_name} {uuid} {key} {value}{cmd_args}"

        return self.execute(cmd=cmd)
