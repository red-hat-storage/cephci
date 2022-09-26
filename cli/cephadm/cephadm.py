from cli import Cli
from cli.ceph.ceph import Ceph
from utility.log import Log

log = Log(__name__)


class CephAdm(Cli):
    """Module to execute cephadm operations"""

    def __init__(self, nodes, base_cmd="cephadm"):
        super(CephAdm, self).__init__(nodes)

        self.base_cmd = base_cmd
        self.base_shell_cmd = f"{self.base_cmd} shell"

        self.ceph = Ceph(nodes, self.base_shell_cmd)

    def shell(self, cmd):
        """
        Ceph orchestrator shell interface to run ceph commands.

        Args:
            cmd (str): Command to be executed
        """
        cmd = f"{self.base_shell_cmd} {cmd}"
        return self.execute(sudo=True, long_running=True, cmd=cmd)

    def rm_cluster(self, fsid, zap_osds=True, force=True):
        """Remove cephadm cluster from nodes

        Args:
            node: ceph node object
            fsid: Cluster FSID
            zap_osds: Remove OSDS
            force: Use `--force`
        """
        cmd = f"{self.base_cmd} rm-cluster --fsid {fsid}"
        if zap_osds:
            cmd += " --zap-osds"
        if force:
            cmd += " --force"

        return self.execute(sudo=True, long_running=True, cmd=cmd)
