from cli import Cli
from cli.ceph.ceph import Ceph


class CephAdm(Cli):
    """This module provides CLI interface to manage the CephAdm operations."""

    def __init__(self, nodes, mount=None, base_cmd="cephadm"):
        super(CephAdm, self).__init__(nodes)

        self.base_cmd = base_cmd
        self.base_shell_cmd = f"{self.base_cmd} shell"
        if mount:
            self.base_shell_cmd += f" --mount {mount} --"

        self.ceph = Ceph(nodes, self.base_shell_cmd)

    def shell(self, cmd):
        """Ceph orchestrator shell interface to run ceph commands.

        Args:
            cmd (str): command to be executed
        """
        cmd = f"{self.base_shell_cmd} {cmd}"
        return self.execute(sudo=True, long_running=True, cmd=cmd)

    def rm_cluster(self, fsid, zap_osds=True, force=True):
        """Remove cephadm cluster from nodes

        Args:
            node (str): ceph node object
            fsid (str): cluster FSID
            zap_osds (bool): remove OSDS
            force (bool): use `--force`
        """
        cmd = f"{self.base_cmd} rm-cluster --fsid {fsid}"
        if zap_osds:
            cmd += " --zap-osds"
        if force:
            cmd += " --force"

        return self.execute(sudo=True, long_running=True, cmd=cmd)
