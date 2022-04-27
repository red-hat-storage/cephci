from utility.log import Log

log = Log(__name__)


class Pool:
    def __init__(self, parent_base_cmd, node):
        self.base_cmd = parent_base_cmd + " pool"
        self.node = node

    def create(self, args):
        """CLI wrapper for 'ceph osd pol create command

        Args:
            args: disctionary object with key pool_name and the
                  name of the pool to be created as its value.

        Returns:
            out: output after execution of command
            err: error after execution of command
        """
        cmd = self.base_cmd + f' create {args["pool_name"]}'
        return self.node.exec_command(cmd=cmd)
