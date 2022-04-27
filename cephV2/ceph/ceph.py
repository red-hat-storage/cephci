"""Module with CLI wrapper for ceph commands

This module is to contain CLI wrapper for all ceph commands. Modules for
sub-commands of ceph would inherit Ceph class from this module.

"""

from cephV2.ceph.osd import Osd
from utility.log import Log

log = Log(__name__)


class Ceph:
    """CLI wrapper class for commands of ceph

    This class contains CLI wrappers for some of the commands of ceph.
    The subcommands of ceph which would have further subcommands would
    inherit this class.
    """

    def __init__(self, node):
        self.base_cmd = "ceph"
        self.node = node
        self.osd = Osd(self.base_cmd, node)

    def df(self):
        pass
