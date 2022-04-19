"""Module with CLI wrapper for `ceph osd ` commands

This module is to contain CLI wrapper for all `ceph osd ` commands
and modules for sub-commands of `ceph osd ` would use this module.
"""
from cephV2.ceph.pool import Pool
from utility.log import Log

log = Log(__name__)


class Osd:
    def __init__(self, parent_base_cmd):
        
        self.base_cmd = parent_base_cmd + " osd"
        self.pool = Pool(self.base_cmd)

    def df(self):
        pass
