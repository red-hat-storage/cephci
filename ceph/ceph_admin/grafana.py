"""Manage the Ceph Grafana service via cephadm CLI."""
from .apply import ApplyMixin
from .orch import Orch


class Grafana(ApplyMixin, Orch):
    """Interface to Ceph service Grafana via CLI."""

    SERVICE_NAME = "grafana"
