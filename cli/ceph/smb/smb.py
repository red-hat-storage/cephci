from cli import Cli
from cli.utilities.utils import build_cmd_from_args

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

    def show(self, resource_names, **kw):
        """
        Show smb resources
        Args:
            resource_names(str): smb resource name
            format (str): the type to be formatted(yaml or json)
        """
        if resource_names:
            cmd = f"{self.base_cmd} show {resource_names} {build_cmd_from_args(**kw)}"
        else:
            cmd = f"{self.base_cmd} show {build_cmd_from_args(**kw)}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
