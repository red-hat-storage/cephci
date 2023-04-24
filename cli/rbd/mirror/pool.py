from copy import deepcopy

from cli import Cli
from cli.rbd.mirror.peer import Peer
from cli.utilities.utils import build_cmd_from_args


class Pool(Cli):
    """This module provides CLI interface to manage rbd mirror pool commands."""

    def __init__(self, nodes, base_cmd):
        super(Pool, self).__init__(nodes)
        self.base_cmd = base_cmd + " pool"
        self.peer = Peer(nodes, self.base_cmd)

    def demote(self, **kw):
        """Wrapper for rbd mirror pool demote.
        Command is used to demote all primary images in the pool.
        Args:
            kw: Key value pair of method arguments
            Example::
            Supported keys:
                pool: Name of the pool.
                namespace: Name of the namespace.
                pool-spec: pool-name/namespace.
                See rbd help mirror pool demote for more supported keys
        """
        kw_copy = deepcopy(kw)
        pool_spec = kw_copy.pop("pool-spec", "")
        cmd = f"{self.base_cmd} demote {pool_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def disable(self, **kw):
        """Wrapper for rbd mirror pool disable.
        Command is used to disable RBD mirroring by default within a pool.
        Args:
            kw: Key value pair of method arguments
            Example::
            Supported keys:
                pool: Name of the pool.
                namespace: Name of the namespace.
                pool-spec: pool-name/namespace.
                See rbd help mirror pool spec for more supported keys
        """
        kw_copy = deepcopy(kw)
        pool_spec = kw_copy.pop("pool-spec", "")
        cmd = f"{self.base_cmd} disable {pool_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def enable(self, **kw):
        """Wrapper for rbd mirror pool enable.
        Command is used to enable RBD mirroring by default within a pool.
        Args:
            kw: Key value pair of method arguments
            Example::
            Supported keys:
                pool: Name of the pool.
                namespace: Name of the namespace.
                pool-spec: pool-name/namespace.
                mode: mirror mode [image or pool]
                site-name: Name of the site.
                See rbd help mirror pool enable for more supported keys
        """
        kw_copy = deepcopy(kw)
        pool_spec = kw_copy.pop("pool-spec", "")
        mode = kw_copy.pop("mode", "")
        cmd = f"{self.base_cmd} enable {pool_spec} {mode} \
            {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def info(self, **kw):
        """Wrapper for rbd mirror pool info.
        Command is used to show information about the pool mirroring configuration.
        Args:
            kw: Key value pair of method arguments
            Example::
            Supported keys:
                pool: Name of the pool.
                namespace: Name of the namespace.
                pool-spec: pool-name/namespace.
                format: output format (plain, json, or xml) [default: plain]
                pretty-format: True - pretty formatting (json and xml)
                all: True - All information
                See rbd help mirror pool info for more supported keys
        """
        kw_copy = deepcopy(kw)
        pool_spec = kw_copy.pop("pool-spec", "")
        cmd = f"{self.base_cmd} info {pool_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def promote(self, **kw):
        """Wrapper for rbd mirror pool promote.
        Command is used to promote a secondary image to primary.
        Args:
            kw: Key value pair of method arguments
            Example::
            Supported keys:
                pool: Name of the pool.
                namespace: Name of the namespace.
                pool-spec: pool-name/namespace.
                force: True - Bool value to force promote the image.
                See rbd help mirror pool promote for more supported keys
        """
        kw_copy = deepcopy(kw)
        pool_spec = kw_copy.pop("pool-spec", "")
        cmd = f"{self.base_cmd} promote {pool_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)

    def status(self, **kw):
        """Wrapper for rbd mirror pool status.
        Command is used to display the mirroring status of pool.
        Args:
            kw: Key value pair of method arguments
            Example::
            Supported keys:
                pool: Name of the pool.
                namespace: Name of the namespace.
                pool-spec: pool-name/namespace.
                format: output format (plain, json, or xml) [default: plain]
                pretty-format: pretty formatting (json and xml)
                verbose: Bool Value - display status of all images.
                See rbd help mirror pool status for more supported keys
        """
        kw_copy = deepcopy(kw)
        pool_spec = kw_copy.pop("pool-spec", "")
        cmd = f"{self.base_cmd} status {pool_spec} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)
