from .config.config_global import Global
from .config.image import Image
from .config.pool import Pool


class Config:
    """
    This module provides CLI interface to override the configuration settings set in the Ceph configuration file.
    """

    def __init__(self, nodes, base_cmd):
        self.base_cmd = base_cmd + " config"
        self.image = Image(nodes, self.base_cmd)
        self.config_global = Global(nodes, self.base_cmd)
        self.pool = Pool(nodes, self.base_cmd)
