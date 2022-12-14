from cli import Cli

from .mgr import Mgr
from .orch.orch import Orch


class Ceph(Cli):
    """This module provides CLI interface for deployment and maintenance of ceph cluster."""

    def __init__(self, nodes, base_cmd=""):
        super(Ceph, self).__init__(nodes)

        self.base_cmd = f"{base_cmd} ceph" if base_cmd else "ceph"
        self.mgr = Mgr(nodes, self.base_cmd)
        self.orch = Orch(nodes, self.base_cmd)

    def version(self):
        """Get ceph version."""
        cmd = f"{self.base_cmd} version"
        out = self.execute(sudo=True, check_ec=False, long_running=False, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def status(self):
        """Get ceph status."""
        cmd = f"{self.base_cmd} status"
        out = self.execute(sudo=True, check_ec=False, long_running=False, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def fsid(self):
        """Get ceph cluster FSID."""
        cmd = f"{self.base_cmd} fsid"
        out = self.execute(sudo=True, check_ec=False, long_running=False, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
