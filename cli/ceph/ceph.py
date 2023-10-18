from cli import Cli

from .balancer import Balancer
from .config import Config
from .config_key import ConfigKey
from .crash import Crash
from .fs.fs import Fs
from .mgr.mgr import Mgr
from .nfs.nfs import Nfs
from .orch.orch import Orch
from .osd.osd import Osd
from .rgw.rgw import Rgw


class Ceph(Cli):
    """This module provides CLI interface for deployment and maintenance of ceph cluster."""

    def __init__(self, nodes, base_cmd=""):
        super(Ceph, self).__init__(nodes)

        self.base_cmd = f"{base_cmd} ceph" if base_cmd else "ceph"
        self.mgr = Mgr(nodes, self.base_cmd)
        self.orch = Orch(nodes, self.base_cmd)
        self.rgw = Rgw(nodes, self.base_cmd)
        self.balancer = Balancer(nodes, self.base_cmd)
        self.config_key = ConfigKey(nodes, self.base_cmd)
        self.config = Config(nodes, self.base_cmd)
        self.crash = Crash(nodes, self.base_cmd)
        self.nfs = Nfs(nodes, self.base_cmd)
        self.fs = Fs(nodes, self.base_cmd)
        self.osd = Osd(nodes, self.base_cmd)

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

    def insights(self, prune=False, hours=None):
        """
        Performs ceph insights related operations
        Args:
            prune (bool): To delete the existing insights reports
            hours (str): Delete logs from given hours, 0 to delete all
        """
        cmd = f"{self.base_cmd} insights"
        if prune:
            # Remove historical health data older than <hours>.
            # Passing 0 for <hours> will clear all health data.
            cmd += f"prune-health {hours}"
        out = self.execute(sudo=True, check_ec=False, long_running=False, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def health(self, detail=False):
        """Returns the Ceph cluster health"""
        cmd = f"{self.base_cmd} health"
        if detail:
            cmd += " detail"
        out = self.execute(sudo=True, check_ec=False, long_running=False, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
