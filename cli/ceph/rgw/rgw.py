from cli import Cli

from .realm import Realm


class Rgw(Cli):
    """This module provides CLI interface for RGW related operations"""

    def __init__(self, nodes, base_cmd):
        super(Rgw, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} rgw"
        self.realm = Realm(nodes, self.base_cmd)
