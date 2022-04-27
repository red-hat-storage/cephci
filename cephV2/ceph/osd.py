"""Module with CLI wrapper for `ceph osd ` commands

This module is to contain CLI wrapper for all `ceph osd ` commands
and modules for sub-commands of `ceph osd ` would use this module.
"""
from cephV2.ceph.pool import Pool
from utility.log import Log

log = Log(__name__)


class Osd:
    def __init__(self, parent_base_cmd, node):
        self.base_cmd = parent_base_cmd + " osd"
        self.node = node

        self.pool = Pool(self.base_cmd, node)

    def df(self):
        pass
