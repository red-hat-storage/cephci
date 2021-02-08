"""
Module to deploy NFS service and daemon(s).

this module deploy NFS service and daemon(s) along with
handling other prerequisites needed for deployment.

"""

from ceph.ceph_admin.apply import Apply
from ceph.ceph_admin.daemon_mixin import DaemonMixin


class NFSRole(Apply, DaemonMixin):
    __ROLE = "nfs"

    def apply_nfs(self, **config):
        raise NotImplementedError

    def daemon_add_nfs(self):
        raise NotImplementedError
