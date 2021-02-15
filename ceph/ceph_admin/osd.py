"""Manage OSD service via cephadm CLI."""
from typing import Dict, Optional
from .apply import ApplyMixin
from .orch import Orch


class OSD(ApplyMixin, Orch):
    """Interface to ceph orch osd."""

    SERVICE_NAME = "osd"

    def apply(
        self, prefix_args: Optional[Dict] = None, args: Optional[Dict] = None
    ) -> None:
        """
        Deploy the OSD service on all available storage devices.

        Args:
            prefix_args:    Key/value pairs to be passed to the base command.
            args:           Key/value pairs to be passed to the command.

        Example:

            config:
                command: osd
                prefix_args:    # Not supported
                args:

        """

        if not config.get("args"):
            config["args"] = "--all-available-devices"

        super().apply(**config)
