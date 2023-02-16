from cli import Cli
from utility.log import Log

log = Log(__name__)


class Label(Cli):
    def __init__(self, nodes, base_cmd):
        super(Label, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} host label"

    def add(self, node, label):
        """
        Add label in the node
        Args:
            node : node name
            label : label name
        """
        cmd = f"{self.base_cmd} add {node} {label}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def rm(self, hostname, label):
        """
        Removes label.
        Args:
          hostname (str): host name
          label (str): label name
        """
        cmd = f"{self.base_cmd} rm {hostname} {label}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
