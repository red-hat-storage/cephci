from cephV2.rbd.rbd import Rbd
from utility.log import Log

log = Log(__name__)


class Pool(Rbd):
    def __init__(self, nodes):
        self.nodes = nodes
        super(Pool, self).__init__(nodes=nodes)

    def init(self):
        pass

    def stats(self):
        pass