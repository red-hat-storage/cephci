from cli import Cli


class Mgr(Cli):
    """This module provides CLI interface to manage the MGR service."""

    def __init__(self, nodes, base_cmd):
        super(Mgr, self).__init__(nodes)

        self.base_cmd = f"{base_cmd} mgr"

    def module(self, action, module=None, force=False):
        """Disable MGR module.

        Args:
            action (str): module action (disable|enable)
            module (str): ceph module to be disabled
            force (bool): use `--force`
        """
        cmd = f"{self.base_cmd} module {action}"
        if module:
            cmd += f" {module}"
        if force:
            cmd += " --force"

        return self.execute(sudo=True, long_running=True, cmd=cmd)

    def fail(self, mgr):
        """
        Fail/down a given mgr
        Args:
            mgr (str): mgr to bring down
        """
        cmd = f"{self.base_cmd} fail {mgr}"
        return self.execute(sudo=True, check_ec=False, long_running=True, cmd=cmd)
