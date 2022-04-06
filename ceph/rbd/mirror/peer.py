from ceph.rbd.mirror.pool import Pool
from utility.log import Log

log = Log(__name__)


class Peer(Pool):
    def __init__(self, nodes):
        self.nodes = nodes
        super(Peer, self).__init__(nodes=nodes)
