from cli import Cli


class Module(Cli):
    """This module provides CLI interface for ceph mgr operations"""

    def __init__(self, nodes, base_cmd=""):
        super(Module, self).__init__(nodes)

        self.base_cmd = f"{base_cmd} module"

    def enable(self, module, force=False):
        """Enable ceph Mgr module

        Args:
            module (str): Ceph Mgr module name
            force (bool): Force option
        """
        cmd = f"{self.base_cmd} enable {module}"

        if force:
            cmd += " --force"

        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def disable(self, module, force=True):
        """Disable ceph Mgr module

        Args:
            module (str): Ceph Mgr module name
            force (bool): Force option
        """
        cmd = f"{self.base_cmd} disable {module}"

        if force:
            cmd += " --force"

        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def ls(self):
        """List ceph Mgr modules"""
        cmd = f"{self.base_cmd} ls"

        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
