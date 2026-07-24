from cli import Cli

from .cluster.cluster import Cluster
from .export.export import Export


class Nfs(Cli):
    """This module provides CLI interface for NFS related operations"""

    def __init__(self, nodes, base_cmd):
        super(Nfs, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} nfs"
        self.cluster = Cluster(nodes, self.base_cmd)
        self.export = Export(nodes, self.base_cmd)
