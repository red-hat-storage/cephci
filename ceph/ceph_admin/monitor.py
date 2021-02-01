"""
Module to deploy as MON service and individual daemon(s).

this module deploy MON service and daemon(s) along with
handling other prerequisites needed for deployment.

"""


from ceph.ceph_admin.daemon_mixin import DaemonMixin
from ceph.ceph_admin.apply import Apply


class Monitor(Apply, DaemonMixin):
    def apply_mon(self, **args):
        """
        Deploy MON service using "orch apply" option
        Args:
            args: test arguments
        """
        super().apply("mon", **args)

    def daemon_add_mon(self, **args):
        """
        Deploy MON service using "orch apply" option
        Args:
            args: test arguments
        """
        super().add("mon", **args)
