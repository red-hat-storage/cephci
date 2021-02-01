"""
Module to deploy NFS as service and daemon(s).

this module deploy NFS service and daemon(s) along with
handling other prerequisites needed for deployment.

"""

from ceph.ceph_admin.daemon_mixin import DaemonMixin
from ceph.ceph_admin.apply import Apply


class NFS(Apply, DaemonMixin):
    def apply_nfs(self, **args):
        super().apply("nfs", **args)
