"""Module to deploy Node-Exporter service and individual daemon(s)."""
from .apply import ApplyMixin
from .orch import Orch


class NodeExporter(ApplyMixin, Orch):
    """Interface to manage node-exporter service."""

    SERVICE_NAME = "node-exporter"
