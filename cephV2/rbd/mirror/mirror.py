from cephV2.rbd.mirror.image import Image
from cephV2.rbd.mirror.pool import Pool
from utility.log import Log

log = Log(__name__)


class Mirror:
    def __init__(self, parent_base_cmd, node):
        self.base_cmd = parent_base_cmd + " mirror"
        self.node = node

        self.image = Image(self.base_cmd, node)
        self.pool = Pool(self.base_cmd, node)
