from cli import Cli
from utility.log import Log

log = Log(__name__)


class Mgr(Cli):
    def __init__(self, nodes, base_cmd):
        super(Mgr, self).__init__(nodes)

        self.base_cmd = f"{base_cmd} mgr"

    def module_disable(self, module):
        """Disable MGR module.

        Args:
            module (str): Ceph module to be disabled
        """
        cmd = f"{self.base_cmd} module disable {module}"
        return self.execute(sudo=True, long_running=True, cmd=cmd)
