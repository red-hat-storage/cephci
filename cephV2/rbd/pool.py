from utility.log import Log

log = Log(__name__)


class Pool:
    def __init__(self, parent_base_cmd, node):
        self.base_cmd = parent_base_cmd + " pool"
        self.node = node

    def init(self, args):
        """Wrapper for the command rbd pool init.

        Enables rbd application on provided pool.
        Args:
            pool_name: Poolname of the image
        """
        cmd = self.base_cmd + f" init {args['pool_name']}"
        return self.node.exec_command(cmd=cmd)

    def stats(self):
        pass
