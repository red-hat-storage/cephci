from copy import deepcopy

from cli import Cli
from cli.utilities.utils import build_cmd_from_args


class Pool(Cli):
    """
    This module provides CLI interface to manage pools in rbd via rbd pool command.
    """

    def __init__(self, nodes, base_cmd):
        super(Pool, self).__init__(nodes)
        self.base_cmd = base_cmd + " pool"

    def init(self, **kw):
        """
        Initiates rbd application on specified pool.
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool-name(str) : name of the pool
                See rbd help pool for more supported keys
        """
        kw_copy = deepcopy(kw)
        pool_name = kw_copy.pop("pool-name", "")
        cmd = f"{self.base_cmd} init {pool_name} {build_cmd_from_args(**kw_copy)}"

        return self.execute_as_sudo(cmd=cmd)
