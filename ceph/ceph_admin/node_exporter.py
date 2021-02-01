"""
Module to deploy as Node-Exporter service and individual daemon(s).

this module deploy Node-Exporter service and daemon(s) along with
handling other prerequisites needed for deployment.

"""

from ceph.ceph_admin.daemon_mixin import DaemonMixin
from ceph.ceph_admin.apply import Apply


class NodeExporter(Apply, DaemonMixin):
    def apply_node_exporter(self, **args):
        """
        Deploy Node-Exporter service using "orch apply" option
        Args:
            args: test arguments
        """
        super().apply("grafana", **args)

    def daemon_add_node_exporter(self, **args):
        """
        Deploy Node-Exporter service using "orch daemon" option
        Args:
            args: test arguments
        """
        super().add("grafana", **args)
