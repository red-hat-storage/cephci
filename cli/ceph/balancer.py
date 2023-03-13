from cli import Cli


class Balancer(Cli):
    """This module provides CLI interface to manage the balancer module."""

    def __init__(self, nodes, base_cmd):
        super(Balancer, self).__init__(nodes)

        self.base_cmd = f"{base_cmd} balancer"

    def status(self):
        """Check status of balancer module."""
        cmd = f"{self.base_cmd} status"
        out = self.execute(sudo=True, check_ec=False, long_running=False, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
