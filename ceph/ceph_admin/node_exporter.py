"""Module to deploy Node-Exporter service and individual daemon(s)."""
from typing import Dict

from .apply import ApplyMixin
from .orch import Orch


class NodeExporter(ApplyMixin, Orch):
    """Interface to manage node-exporter service."""

    SERVICE_NAME = "node-exporter"

    def apply(self, config: Dict) -> None:
        """
        Deploy the node-exporter service using the provided configuration.

        Args:
            config: Key/value pairs provided by the test case to create the service.

        Example
            config:
                command: apply
                service: node-exporter
                base_cmd_args:          # arguments to ceph orch
                    concise: true
                    verbose: true
                    input_file: <name of spec>
                args:
                    placement:
                        label: node-exporter    # either label or node.
                        nodes:
                            - node1
                        limit: 3    # no of daemons
                        sep: " "    # separator to be used for placements
                    dry-run: true
                    unmanaged: true
        """
        super().apply(config=config)
