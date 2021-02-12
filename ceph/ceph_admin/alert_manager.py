"""Deploy the alert manager service in the cluster via cephadm CLI."""
from .apply import ApplyMixin
from .orch import Orch


class AlertManager(ApplyMixin, Orch):
    """Manage the alert-manager service."""

    SERVICE_NAME = "alertmanager"
