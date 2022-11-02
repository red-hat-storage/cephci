class Pool:
    """
    This module provides CLI interface to manage pools in rbd via rbd pool command.
    """

    def __init__(self, nodes, base_cmd):
        self.base_cmd = base_cmd + " pool"

    def init(self, **kw):
        """
        Initiates rbd application on specified pool.
        Args:
        kw(dict): Key/value pairs that needs to be provided to the installer
            Example::
            Supported keys:
                pool_name(str) : name of the pool
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")
        pool_name = kw.get("pool_name")
        cmd = self.base_cmd + f" init -p {pool_name}"

        return self.execute(cmd=cmd)
