from cli import Cli
from cli.utilities.utils import build_cmd_from_args


class Config(Cli):
    """This module provides CLI interface to manage the balancer module."""

    def __init__(self, nodes, base_cmd):
        super(Config, self).__init__(nodes)

        self.base_cmd = f"{base_cmd} config"

    def set(self, key, value, daemon=None):
        """set config key to value
        Args:
            key (str): config key
            value (str): config key value
            daemon (str): daemon with or without path
        """
        cmd = f"{self.base_cmd} set"
        if daemon:
            cmd += f" {daemon}"
        cmd += f" {key} {value}"
        return self.execute(sudo=True, cmd=cmd)

    def get(self, who, key=None):
        """Fetch the config value
        Args:
            who (str): config process
            key (str): config parameter
        """
        cmd = f"{self.base_cmd} get {who}"
        if key:
            cmd += f" {key}"
        return self.execute(sudo=True, cmd=cmd)

    def dump(self, **kw):
        """Executes ceph config specific command
        Agrs:
            kw (dict): additional command parameters
        """
        cmd = f"{self.base_cmd} dump{build_cmd_from_args(**kw)}"
        return self.execute(sudo=True, check_ec=False, long_running=False, cmd=cmd)
