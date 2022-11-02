from copy import deepcopy

from cli.utilities.utils import build_cmd_args


class Namespace:

    """
    This Class provides wrappers for rbd namespace commands.
    """

    def __init__(self, nodes, base_cmd):
        self.base_cmd = base_cmd + " namespace"

    def create(self, **kw):
        """Wrapper for rbd namespace create.

        Create an rbd image namespace inside specified pool.
        Args:
        kw: Key value pair of method arguments
            Example::
            Supported keys:
                pool_spec(str): pool specification <pool-name>[/<namespace>]
                namespace(str): name of the namespace
                pool(str): pool name
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")

        kw_copy = deepcopy(kw)
        pool_spec = kw_copy.pop("pool_spec", "")
        cmd_args = build_cmd_args(kw=kw_copy)

        cmd = f"{self.base_cmd} create {pool_spec}{cmd_args}"

        return self.execute(cmd=cmd)

    def list_(self, **kw):
        """Wrapper for rbd namespace list.

        List RBD image namespaces.
        Args:
        kw: Key value pair of method arguments
            Example::
            Supported keys:
                pool_spec(str): pool specification.
                pool(str): pool name
                format(str): output format (plain, json, or xml) [default: plain]
                pretty-format(bool): True
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")

        kw_copy = deepcopy(kw)
        pool_spec = kw_copy.pop("pool_spec", "")
        cmd_args = build_cmd_args(kw=kw_copy)

        cmd = f"{self.base_cmd} list {pool_spec}{cmd_args}"

        return self.execute(cmd=cmd)

    def remove_(self, **kw):
        """Wrapper for rbd namespace remove.

        Remove an RBD image namespace.
        Args:
        kw: Key value pair of method arguments
            Example::
            Supported keys:
                pool_spec(str): pool specification <pool-name>[/<namespace>]
                namespace(str): name of the namespace
                pool(str): pool name
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")

        kw_copy = deepcopy(kw)
        pool_spec = kw_copy.pop("pool_spec", "")
        cmd_args = build_cmd_args(kw=kw_copy)

        cmd = f"{self.base_cmd} remove {pool_spec}{cmd_args}"

        return self.execute(cmd=cmd)
