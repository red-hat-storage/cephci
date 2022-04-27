from cephV2.rbd.mirror.bootstrap import Bootstrap
from utility.log import Log

log = Log(__name__)


class Peer:
    def __init__(self, parent_base_cmd, node):
        self.base_cmd = parent_base_cmd + " peer"
        self.node = node

        self.bootstrap = Bootstrap(self.base_cmd, node)
