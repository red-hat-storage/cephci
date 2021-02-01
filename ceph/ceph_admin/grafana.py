"""
Module to deploy as Grafana service and individual daemon(s).

this module deploy Grafana service and daemon(s) along with
handling other prerequisites needed for deployment.

"""

from ceph.ceph_admin.daemon_mixin import DaemonMixin
from ceph.ceph_admin.apply import Apply


class Grafana(Apply, DaemonMixin):
    def apply_grafana(self, **args):
        """
        Deploy grafana service using "orch apply" option
        Args:
            args: test arguments
        """
        super().apply("grafana", **args)

    def daemon_add_grafana(self, **args):
        """
        Deploy grafana service using "orch apply" option
        Args:
            args: test arguments
        """
        super().add("grafana", **args)
