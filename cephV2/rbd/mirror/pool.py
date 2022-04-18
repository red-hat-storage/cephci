from cephV2.rbd.mirror.mirror import Mirror
from utility.log import Log

log = Log(__name__)


class Pool(Mirror):
    def __init__(self, nodes):
        self.nodes = nodes
        super(Pool, self).__init__(nodes=nodes)

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
