"""
Module to deploy osd service and daemon(s).

this module deploy OSD service and daemon(s) along with
handling other prerequisites needed for deployment.
"""
from ceph.ceph_admin.apply import ApplyMixin

from .orch import Orch


class OSD(ApplyMixin, Orch):
    SERVICE_NAME = "osd"

    def apply(self, **config):
        """Deploy the OSD service on all available storage devices."""
        if not config.get("args"):
            config["args"] = "--all-available-devices"

        super().apply(**config)
