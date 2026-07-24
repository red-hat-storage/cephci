from cli import Cli
from cli.utilities.utils import build_cmd_from_args

from .module import Module


class Mgr(Cli):
    """This module provides CLI interface to manage the MGR service."""

    def __init__(self, nodes, base_cmd):
        super(Mgr, self).__init__(nodes)

        self.base_cmd = f"{base_cmd} mgr"
        self.module = Module(nodes, self.base_cmd)

    def fail(self, mgr):
        """
        Fail/down a given mgr
        Args:
            mgr (str): mgr to bring down
        """
        cmd = f"{self.base_cmd} fail {mgr}"
        return self.execute(sudo=True, check_ec=False, long_running=True, cmd=cmd)

    def services(self, **kw):
        """Get MGR service module"""
        cmd = f"{self.base_cmd} services{build_cmd_from_args(**kw)}"

        return self.execute(sudo=True, cmd=cmd)
