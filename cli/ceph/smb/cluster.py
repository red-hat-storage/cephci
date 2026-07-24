from cli import Cli
from cli.utilities.utils import build_cmd_from_args


class Cluster(Cli):
    """This module provides CLI interface for smb cluster related operations"""

    def __init__(self, nodes, base_cmd):
        super(Cluster, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} cluster"

    def create(self, cluster_id, auth_mode, **kw):
        """Create an smb cluster

        Args:
            cluster_id (str): A short string uniquely identifying the cluster
            auth_mode (str): user or active-directory
            domain_realm (str): Active-directory domain realm
            domain_join_user_pass (str): A string in the form <username>%<password> to connect active-directoy
            custom_dns (str): Active-directory custom dns
            placement (str): Ceph orchestration placement specifier
        """
        cmd = f"{self.base_cmd} create {cluster_id} {auth_mode} {build_cmd_from_args(**kw)}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def ls(self, **kw):
        """List an smb cluster

        Args:
            format (str): Format type (YAML or JSON)
        """
        cmd = f"{self.base_cmd} ls {build_cmd_from_args(**kw)}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def rm(self, cluster_id):
        """Remove smb cluster

        Args:
            cluster_id (str): A short string uniquely identifying the cluster
        """
        cmd = f"{self.base_cmd} rm {cluster_id}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
