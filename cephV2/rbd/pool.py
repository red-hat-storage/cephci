from cephV2.rbd.rbd import Rbd
from utility.log import Log

log = Log(__name__)


class Pool:
    def __init__(self, parent_base_cmd):
        self.base_cmd = parent_base_cmd + " pool"

    def init(self):
        pass

    def stats(self):
        pass
