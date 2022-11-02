from .mirror.image import Image
from .mirror.pool import Pool
from .mirror.snapshot import Snapshot


class Mirror:
    """Base class for all rbd mirror commands.

    This module provides CLI interface to manage rbd mirror and
    objects with wrapper for sub-commands.

    """

    def __init__(self, nodes, base_cmd):
        self.base_cmd = base_cmd + " mirror"
        self.image = Image(nodes, self.base_cmd)
        self.pool = Pool(nodes, self.base_cmd)
        self.snapshot = Snapshot(nodes, self.base_cmd)
