"""Manage the Manager service via Ceph's cephadm CLI."""
from .apply import ApplyMixin
from .orch import Orch


class Mgr(ApplyMixin, Orch):
    """Manager interface for ceph orch <action> mgr"""

    SERVICE_NAME = "mgr"
