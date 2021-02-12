"""
Module to deploy Node-Exporter service and individual daemon(s).

this module deploy Node-Exporter service and daemon(s) along with
handling other prerequisites needed for deployment.

"""
from ceph.ceph_admin.apply import ApplyMixin

from .orch import Orch


class NodeExporter(ApplyMixin, Orch):
    SERVICE_NAME = "node-exporter"
