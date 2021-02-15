"""Manage OSD service via cephadm CLI."""
from typing import Dict

from .apply import ApplyMixin
from .orch import Orch


class OSD(ApplyMixin, Orch):
    """Interface to ceph orch osd."""

    SERVICE_NAME = "osd"

    def apply(self, config: Dict) -> None:
        """
        Deploy the ODS service using the provided configuration.

        Args:
            config: Key/value pairs provided by the test case to create the service.

        Example
            config:
                command: apply
                service: ods
                base_cmd_args:          # arguments to ceph orch
                    concise: true
                    verbose: true
                    input_file: <name of spec>
                args:
                    all-available-devices: true
                    placement:
                        label: osd    # either label or node.
                        nodes:
                            - node1
                        limit: 3    # no of daemons
                        sep: " "    # separator to be used for placements
                    dry-run: true
                    unmanaged: true
        """
        if not config.get("args", {}).get("all-available-devices"):
            config["args"]["all-available-devices"] = True

        super().apply(config)
