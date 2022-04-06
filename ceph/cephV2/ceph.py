"""Module with CLI wrapper for ceph commands

This module is to contain CLI wrapper for all ceph commands. Modules for
sub-commands of ceph would inherit Ceph class from this module.

"""

from ceph.cli import CephCLI
from utility.log import Log

log = Log(__name__)


class Ceph(CephCLI):
    """CLI wrapper class for commands of ceph

    This class contains CLI wrappers for some of the commands of ceph.
    The subcommands of ceph which would have further subcommands would
    inherit this class.
    """

    def df(self):
        pass
