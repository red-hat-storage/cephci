from cli import Cli

from .image import Image


class Config(Cli):
    """Base class for all rbd config commands.
    This module provides CLI interface to manage rbd config and
    objects with wrapper for sub-commands.
    """

    def __init__(self, nodes, base_cmd):
        super(Config, self).__init__(nodes)
        self.base_cmd = base_cmd + " config"
        self.image = Image(nodes, self.base_cmd)
