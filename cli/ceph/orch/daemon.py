from cli import Cli
from cli.utilities.utils import build_cmd_from_args
from utility.log import Log

log = Log(__name__)


class Daemon(Cli):
    def __init__(self, nodes, base_cmd):
        super(Daemon, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} daemon"

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
