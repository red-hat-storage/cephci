from cli import Cli
from cli.utilities.utils import build_cmd_from_args


class Share(Cli):
    """This module provides CLI interface for smb share related operations"""

    def __init__(self, nodes, base_cmd):
        super(Share, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} share"

    def create(self, cluster_id, share_id, cephfs_volume, path, **kw):
        """Create an smb sahre

        Args:
            cluster_id (str): A short string uniquely identifying the cluster
            share_id (str): A short string uniquely identifying the share
            cephfs_volume (str): The name of the cephfs volume to be shared
            Path (str): A path relative to the root of the volume and/or subvolume
            share_name (str): Smb share name
            subvolume (str): Smb subvolume
        """
        cmd = f"{self.base_cmd} create {cluster_id} {share_id} {cephfs_volume} {path} {build_cmd_from_args(**kw)}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def rm(self, cluster_id, share_id):
        """Remove smb share

        Args:
            cluster_id (str): A short string uniquely identifying the cluster
            share_id (str): A short string uniquely identifying the share
        """
        cmd = f"{self.base_cmd} rm {cluster_id} {share_id}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
