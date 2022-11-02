from copy import deepcopy

from cli.utilities.utils import build_cmd_args


class Client:

    """
    This Class provides wrappers for rbd journal client commands.
    """

    def __init__(self, nodes, base_cmd):
        self.base_cmd = base_cmd + " client"

    def disconnect(self, **kw):
        """Wrapper for rbd journal client disconnect.

        Flag image journal client as disconnected.
        Args:
        kw: Key value pair of method arguments
            Example::
            Supported keys:
                journal_spec(str): journal specification.
                                    [<pool-name>/[<namespace>/]]<journal-name>
                namespace(str): name of the namespace
                pool(str): pool name
                image(str): image name
                journal(str): journal name
                client-id(str): client ID (leave unspecified to disconnect all)
        Returns:
            Dict(str):
            A mapping of host strings to the given task's return value for that host's execution run.
        """
        kw = kw.get("kw")

        kw_copy = deepcopy(kw)
        journal_spec = kw_copy.pop("journal_spec", "")
        cmd_args = build_cmd_args(kw=kw_copy)

        cmd = f"{self.base_cmd} disconnect {journal_spec}{cmd_args}"

        return self.execute(cmd=cmd)
