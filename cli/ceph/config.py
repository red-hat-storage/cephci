from cli import Cli


class Config(Cli):
    """This module provides CLI interface to manage the balancer module."""

    def __init__(self, nodes, base_cmd):
        super(Config, self).__init__(nodes)

        self.base_cmd = f"{base_cmd} config"

    def set(self, key, value):
        """set config key to value
        Args:
            key (str): config key
            value (str): config key value
        """
        cmd = f"{self.base_cmd} set {key} {value}"
        return self.execute(sudo=True, cmd=cmd)
