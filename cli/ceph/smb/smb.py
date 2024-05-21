from cli import Cli

from .apply import Apply
from .cluster import Cluster
from .share import Share


class Smb(Cli):
    """This module provides CLI interface for ceph smb operations"""

    def __init__(self, nodes, base_cmd=""):
        super(Smb, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} smb"
        self.cluster = Cluster(nodes, self.base_cmd)
        self.share = Share(nodes, self.base_cmd)
        self.apply = Apply(nodes, self.base_cmd)
