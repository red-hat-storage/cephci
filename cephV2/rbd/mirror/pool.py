from cephV2.rbd.mirror.peer import Peer
from utility.log import Log

log = Log(__name__)


class Pool:
    def __init__(self, parent_base_cmd, node):
        self.base_cmd = parent_base_cmd + " pool"
        self.node = node

        self.peer = Peer(self.base_cmd, node)

    def demote(self):
        pass

    def disable(self):
        pass

    def enable(self):
        pass

    def info(self):
        pass

    def promote(self):
        pass

    def status(self):
        pass
