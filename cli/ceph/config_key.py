from cli import Cli


class ConfigKey(Cli):
    """This module provides CLI interface to manage the balancer module."""

    def __init__(self, nodes, base_cmd):
        super(ConfigKey, self).__init__(nodes)

        self.base_cmd = f"{base_cmd} config-key"

    def set(self, key, value):
        """set config key to value
        Args:
            key (str): config key
            value (str): config key value
        """
        cmd = f"{self.base_cmd} set {key} {value}"
        return self.execute(sudo=True, check_ec=False, long_running=True, cmd=cmd)

    def get(self, key):
        """Get config key value
        Args:
            key (str): config key
        """
        cmd = f"{self.base_cmd} get {key}"
        return self.execute(sudo=True, check_ec=False, long_running=True, cmd=cmd)
