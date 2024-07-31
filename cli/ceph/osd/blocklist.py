from cli import Cli


class Blocklist(Cli):
    """This module provides CLI interface to manage the Blocklist service."""

    def __init__(self, nodes, base_cmd):
        super(Blocklist, self).__init__(nodes)

        self.base_cmd = f"{base_cmd} blocklist"

    def ls(self):
        """List/show blocklisted clients"""
        cmd = f"{self.base_cmd} ls"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[1].splitlines()
        return out
