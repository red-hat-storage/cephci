"""
ISCSI module to deploy as iscsi service and daemons.

this module deploy iscsi service and daemon(s) along with
handling other prerequisites needed for deployment.

"""

from ceph.ceph_admin.daemon_mixin import DaemonMixin
from ceph.ceph_admin.apply import Apply


class ISCSI(Apply, DaemonMixin):
    def apply_iscsi(self, **args):
        super().apply("iscsi", **args)
