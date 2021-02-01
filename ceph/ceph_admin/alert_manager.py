"""
Module to deploy as Alert-Manager service and individual daemon(s).

this module deploy Alert-Manager service and daemon(s) along with
handling other prerequisites needed for deployment.

"""


from ceph.ceph_admin.daemon_mixin import DaemonMixin
from ceph.ceph_admin.apply import Apply


class AlertManager(Apply, DaemonMixin):
    def apply_alert_manager(self, **args):
        """
        Deploy Alert-Manager service using "orch apply" option
        Args:
            args: test arguments
        """
        super().apply("alert-manager", **args)

    def daemon_add_alert_manager(self, **args):
        """
        Deploy Alert-Manager service using "orch apply" option
        Args:
            args: test arguments
        """
        super().add("alert-manager", **args)
