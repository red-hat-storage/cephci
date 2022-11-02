from copy import deepcopy


class Bootstrap:
    """This module provides CLI interface to manage rbd pool peer bootstrap commands."""

    def __init__(self, nodes, base_cmd):
        self.base_cmd = base_cmd + " bootstrap"

    def create(self, **kw):
        """Wrapper for pool peer bootstrap create.

        Create a peer bootstrap token to import in a remote cluster.

        Args:
            kw: Key value pair of method arguments
        Example::
        Supported keys:
            pool_name: Name of the pool of which peer is to be bootstrapped.
            site_name: Name of the site for rbd-mirror
                       name of both sites should be the same
            token_path: path to store bootstrap token
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")

        kw_copy = deepcopy(kw)
        pool_name = kw_copy.pop("pool_name", "")
        token_path = kw_copy.pop("token_path")
        site_name = kw_copy.pop("site_name")

        cmd = (
            f"{self.base_cmd} create {pool_name} --site-name {site_name} > {token_path}"
        )

        return self.execute(cmd=cmd)

    def import_(self, **kw):
        """Wrapper for pool peer bootstrap import.

        Import a peer bootstrap token created from a remote cluster

        Args:
            kw: Key value pair of method arguments
        Example::
        Supported keys:
            pool_name: Name of the pool of which peer is to be bootstrapped.
            site_name: Name of the site for rbd-mirror
                       name of both sites should be the same
            token_path: The file name in which the key is stored
            direction: mirroring direction (rx-only, rx-tx)
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")

        kw_copy = deepcopy(kw)
        pool_name = kw_copy.pop("pool_name", "")
        site_name = kw_copy.pop("site_name")
        direction = kw_copy.pop("direction")
        token_path = kw_copy.pop("token_path")

        cmd = f"{self.base_cmd} import {pool_name} --site-name {site_name} --direction {direction} {token_path}"

        return self.execute(cmd=cmd)
