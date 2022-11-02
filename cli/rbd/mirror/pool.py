from copy import deepcopy

from cli.utilities.utils import build_cmd_args

from .mirror.peer import Peer


class Pool:
    """This module provides CLI interface to manage rbd mirror pool commands."""

    def __init__(self, nodes, base_cmd):
        self.base_cmd = base_cmd + " pool"
        self.peer = Peer(nodes, self.base_cmd)

    def demote(self, **kw):
        """Wrapper for rbd mirror pool demote.

        Command is used to demote all primary images in the pool.

        Args:
            kw: Key value pair of method arguments
            Example::
            Supported keys:
                pool_name: Name of the pool.
                namespace: Name of the namespace.
                pool_spec: pool-name/namespace.
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")
        kw_copy = deepcopy(kw)
        pool_spec = kw_copy.pop("pool_spec", "")
        cmd_args = build_cmd_args(kw=kw_copy)

        cmd = f"{self.base_cmd} demote {pool_spec}{cmd_args}"

        return self.execute(cmd=cmd)

    def disable(self, **kw):
        """Wrapper for rbd mirror pool disable.

        Command is used to disable RBD mirroring by default within a pool.

        Args:
            kw: Key value pair of method arguments
            Example::
            Supported keys:
                pool_name: Name of the pool.
                namespace: Name of the namespace.
                pool_spec: pool-name/namespace.

        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")
        kw_copy = deepcopy(kw)
        pool_spec = kw_copy.pop("pool_spec", "")
        cmd_args = build_cmd_args(kw=kw_copy)

        cmd = f"{self.base_cmd} disable {pool_spec}{cmd_args}"

        return self.execute(cmd=cmd)

    def enable(self, **kw):
        """Wrapper for rbd mirror pool enable.

        Command is used to enable RBD mirroring by default within a pool.

        Args:
            kw: Key value pair of method arguments
            Example::
            Supported keys:
                pool_name: Name of the pool.
                namespace: Name of the namespace.
                pool_spec: pool-name/namespace.
                mode: mirror mode [image or pool]
                site-name: Name of the site.

        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")

        kw_copy = deepcopy(kw)
        pool_name = kw_copy.pop("pool_name", "")
        mode = kw_copy.pop("mode", "")
        site_name = kw_copy.pop("site_name", "")

        cmd = (
            f"{self.base_cmd} enable --site-name {site_name} --pool {pool_name} {mode}"
        )

        return self.execute(cmd=cmd)

    def info(self, **kw):
        """Wrapper for rbd mirror pool info.

        Command is used to show information about the pool mirroring configuration.
        Args:
            kw: Key value pair of method arguments
            Example::
            Supported keys:
                pool_name: Name of the pool.
                namespace: Name of the namespace.
                pool_spec: pool-name/namespace.
                format: output format (plain, json, or xml) [default: plain]
                pretty-format: True - pretty formatting (json and xml)
                all: True - All information

        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")

        kw_copy = deepcopy(kw)
        pool_spec = kw_copy.pop("pool_spec", "")
        cmd_args = build_cmd_args(kw=kw_copy)

        cmd = f"{self.base_cmd} info {pool_spec}{cmd_args}"

        return self.execute(cmd=cmd)

    def promote(self, **kw):
        """Wrapper for rbd mirror pool promote.

        Command is used to promote a secondary image to primary.
        Args:
            kw: Key value pair of method arguments
            Example::
            Supported keys:
                pool_name: Name of the pool.
                namespace: Name of the namespace.
                pool_spec: pool-name/namespace.
                force: True - Bool value to force promote the image.

        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")

        kw_copy = deepcopy(kw)
        pool_spec = kw_copy.pop("pool_spec", "")
        cmd_args = build_cmd_args(kw=kw_copy)
        cmd = f"{self.base_cmd} promote {pool_spec}{cmd_args}"

        return self.execute(cmd=cmd)

    def status(self, **kw):
        """Wrapper for rbd mirror pool status.

        Command is used to display the mirroring status of pool.
        Args:
            kw: Key value pair of method arguments
            Example::
            Supported keys:
                pool_name: Name of the pool.
                namespace: Name of the namespace.
                pool_spec: pool-name/namespace.
                format: output format (plain, json, or xml) [default: plain]
                pretty-format: pretty formatting (json and xml)
                verbose: Bool Value - display status of all images.

        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")

        kw_copy = deepcopy(kw)
        pool_spec = kw_copy.pop("pool_spec", "")
        cmd_args = build_cmd_args(kw=kw_copy)

        cmd = f"{self.base_cmd} status {pool_spec}{cmd_args}"

        return self.execute(cmd=cmd)
