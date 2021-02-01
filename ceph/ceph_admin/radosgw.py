"""
Module to deploy as RGW service and individual daemon(s).

this module deploy RGW service and daemon(s) along with
handling other prerequisites needed for deployment.

"""

from ceph.ceph_admin.daemon_mixin import DaemonMixin
from ceph.ceph_admin.apply import Apply


class RadosGW(Apply, DaemonMixin):
    def apply_rgw(self, **args):
        """
        Deploy RGW service using "orch apply" option
        Args:
            args: test arguments
        """
        super().apply("rgw", **args)

    def rgw_daemon_add(self, **args):
        super().add("rgw", **args)

    def rgw_daemon_remove(self, **args):
        super().remove("rgw", **args)
