"""
Module to deploy osd as service and daemon(s).

this module deploy OSD service and daemon(s) along with
handling other prerequisites needed for deployment.

"""

from ceph.ceph_admin.daemon_mixin import DaemonMixin
from ceph.ceph_admin.apply import Apply


class OSD(Apply, DaemonMixin):
    def apply_osd(self, **args):
        super().apply("osd", **args)
