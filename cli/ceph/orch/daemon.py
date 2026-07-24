from cli import Cli
from cli.utilities.utils import build_cmd_from_args

from .daemons.add import Add


class Daemon(Cli):
    def __init__(self, nodes, base_cmd):
        super(Daemon, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} daemon"
        self.add = Add(nodes, self.base_cmd)

    def redeploy(self, daemon_name, **kw):
        """
        Redeploy daemon running in the node
        Args:
            daemon_name (str) : daemon name
            kw (dict): Key/value pairs that needs to be provided to the installer

            Supported keys:
                image (str) : image value
        """
        cmd = f"{self.base_cmd} redeploy {daemon_name} {build_cmd_from_args(**kw)}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def rotate_key(self, daemon_name):
        """
        Rotates the key for a given daemon
        Args:
            daemon_name (str) : daemon name
        """
        cmd = f"{self.base_cmd} rotate-key {daemon_name}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
