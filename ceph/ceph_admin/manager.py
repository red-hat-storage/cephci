"""
Module to deploy as MGR service and individual daemon(s).

this module deploy MGR service and daemon(s) along with
handling other prerequisites needed for deployment.

"""


from ceph.ceph_admin.daemon_mixin import DaemonMixin
from ceph.ceph_admin.apply import Apply


class Manager(Apply, DaemonMixin):
    def apply_mgr(self, **args):
        """
        Deploy MGR service using "orch apply" option
        Args:
            args: test arguments
        """
        super().apply("mgr", **args)

    def daemon_add_mgr(self, **args):
        """
        Deploy MGR service using "orch apply" option
        Args:
            args: test arguments
        """
        super().add("mgr", **args)
