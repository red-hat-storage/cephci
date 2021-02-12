"""Manage the Monitor service via cephadm CLI."""
from .apply import ApplyMixin
from .orch import Orch


class Mon(ApplyMixin, Orch):
    """Interface to ceph orch <action> mon."""

    SERVICE_NAME = "mon"
