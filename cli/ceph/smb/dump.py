from cli import Cli
from cli.utilities.utils import build_cmd_from_args


class Dump(Cli):
    """This module provides CLI interface for smb dump related operations"""

    def __init__(self, nodes, base_cmd):
        super(Dump, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} dump"

    def cluster_config(self, cluster_id, **kw):
        """Dump smb cluster config

        Args:
            cluster_id (str): A short string uniquely identifying the cluster
        """
        cmd = f"{self.base_cmd} cluster-config {cluster_id} {build_cmd_from_args(**kw)}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def service_spec(self, cluster_id, **kw):
        """Dump smb cluster service spec

        Args:
            cluster_id (str): A short string uniquely identifying the cluster
        """
        cmd = f"{self.base_cmd} service-spec {cluster_id} {build_cmd_from_args(**kw)}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
