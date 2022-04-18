"""Module with CLI wrapper for `ceph osd ` commands

This module is to contain CLI wrapper for all `ceph osd ` commands
and modules for sub-commands of `ceph osd ` would use this module.
"""

from cephV2.ceph import Ceph
from utility.log import Log

log = Log(__name__)


class Osd(Ceph):
    def __init__(self, nodes):
        self.nodes = nodes
        super(Osd, self).__init__(nodes=nodes)

    def df(self):
        pass
