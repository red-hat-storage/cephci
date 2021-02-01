"""
Module to deploy MDS as service and daemon(s).

this module deploy MDS service and daemon(s) along with
handling other prerequisites needed for deployment.

"""

from ceph.ceph_admin.daemon_mixin import DaemonMixin
from ceph.ceph_admin.apply import Apply


class MetaDataServer(Apply, DaemonMixin):
    def apply_mds(self, **args):
        super().apply("mds", **args)
