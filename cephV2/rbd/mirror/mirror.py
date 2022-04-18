from cephV2.rbd.rbd import Rbd
from utility.log import Log

log = Log(__name__)


class Mirror(Rbd):
    def __init__(self, nodes):
        self.nodes = nodes
        super(Mirror, self).__init__(nodes=nodes)
