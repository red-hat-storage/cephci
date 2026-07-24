from copy import deepcopy

from cli import Cli
from cli.rbd.mirror.bootstrap import Bootstrap
from cli.utilities.utils import build_cmd_from_args


class Peer(Cli):
    """This module provides CLI interface to manage rbd mirror pool commands."""

    def __init__(self, nodes, base_cmd):
        super(Peer, self).__init__(nodes)
        self.base_cmd = base_cmd + " peer"
        self.bootstrap = Bootstrap(nodes, self.base_cmd)

    def add_(self, **kw):
        """Wrapper for rbd mirror pool peer add.
        Command is used for adding a mirror pool peer
        Args:
            kw: Key value pair of method arguments
            Example::
            Supported keys:
                pool-name: Name of the pool.
                pool: Name of the pool. (--pool <name>)
                remote-cluster-spec: remote cluster specification - <client-name>@<cluster-name>
                See rbd help mirror pool peer add for more supported keys
        """
        kw_copy = deepcopy(kw)
        pool_name = kw_copy.pop("pool-name", "")
        remote_cluster_spec = kw_copy.pop("remote-cluster-spec", "")
        cmd = f"{self.base_cmd} add {pool_name} {remote_cluster_spec} \
            {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def remove_(self, **kw):
        """Wrapper for rbd mirror pool peer remove.
        Command is used to remove a mirroring peer from a pool.
        Args:
            kw: Key value pair of method arguments
            Example::
            Supported keys:
                pool-name: Name of the pool.
                pool: Name of the pool. (--pool <name>)
                uuid: peer uuid
                See rbd help mirror pool peer remove for more supported keys
        """
        kw_copy = deepcopy(kw)
        pool_name = kw_copy.pop("pool-name", "")
        uuid = kw_copy.pop("uuid", "")
        cmd = f"{self.base_cmd} remove {pool_name} {uuid} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def set_(self, **kw):
        """Wrapper for rbd mirror pool peer set.
        This command is used to update mirroring peer settings.
        Args:
            kw: Key value pair of method arguments
            Example::
            Supported keys:
                pool-name: Name of the pool.
                pool: Name of the pool. (--pool <name>)
                uuid: peer uuid
                key: peer parameter
                (direction, site-name, client, mon-host, key-file)
                value: new value for specified key
                See rbd help mirror pool peer set for more supported keys
        """
        kw_copy = deepcopy(kw)
        pool_name = kw_copy.pop("pool-name", "")
        uuid = kw_copy.pop("uuid", "")
        key = kw_copy.pop("key", "")
        value = kw_copy.pop("value", "")
        cmd = f"{self.base_cmd} set {pool_name} {uuid} {key} {value} \
            {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)
