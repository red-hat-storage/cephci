from copy import deepcopy

from cli import Cli
from cli.utilities.utils import build_cmd_from_args


class Bootstrap(Cli):
    """This module provides CLI interface to manage rbd pool peer bootstrap commands."""

    def __init__(self, nodes, base_cmd):
        super(Bootstrap, self).__init__(nodes)
        self.base_cmd = base_cmd + " bootstrap"

    def create(self, **kw):
        """Wrapper for pool peer bootstrap create.
        Create a peer bootstrap token to import in a remote cluster.
        Args:
            kw: Key value pair of method arguments
        Example::
        Supported keys:
            pool-name: Name of the pool of which peer is to be bootstrapped.
            site-name: Name of the site for rbd-mirror
                       name of both sites should be the same
            token-path: path to store bootstrap token
            See rbd help mirror pool peer bootstrap create for more supported keys
        """
        kw_copy = deepcopy(kw)
        pool_name = kw_copy.pop("pool-name", "")
        token_path = kw_copy.pop("token-path", "")
        cmd = f"{self.base_cmd} create {pool_name} {build_cmd_from_args(**kw_copy)} > {token_path}"

        return self.execute_as_sudo(cmd=cmd)

    def import_(self, **kw):
        """Wrapper for pool peer bootstrap import.
        Import a peer bootstrap token created from a remote cluster
        Args:
            kw: Key value pair of method arguments
        Example::
        Supported keys:
            pool-name: Name of the pool of which peer is to be bootstrapped.
            site-name: Name of the site for rbd-mirror
                       name of both sites should be the same
            token-path: The file name in which the key is stored
            direction: mirroring direction (rx-only, rx-tx)
            See rbd help mirror pool peer bootstrap import for more supported keys
        """
        kw_copy = deepcopy(kw)
        pool_name = kw_copy.pop("pool-name", "")
        token_path = kw_copy.pop("token-path", "")
        cmd = f"{self.base_cmd} import {pool_name} {token_path} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)
