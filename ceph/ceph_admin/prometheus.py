"""
Module to deploy as Prometheus service and individual daemon(s).

this module deploy Prometheus service and daemon(s) along with
handling other prerequisites needed for deployment.

"""

from ceph.ceph_admin.daemon_mixin import DaemonMixin
from ceph.ceph_admin.apply import Apply


class Prometheus(Apply, DaemonMixin):
    def apply_prometheus(self, **args):
        """
        Deploy prometheus service using "orch apply" option
        Args:
            args: test arguments
        """
        super().apply("prometheus", **args)

    def daemon_add_prometheus(self, **args):
        """
        Deploy prometheus service using "orch daemon" option
        Args:
            args: test arguments
        """
        super().add("prometheus", **args)
