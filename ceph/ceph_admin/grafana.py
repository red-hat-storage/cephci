"""Manage the Ceph Grafana service via cephadm CLI."""
from typing import Dict

from .apply import ApplyMixin
from .orch import Orch


class Grafana(ApplyMixin, Orch):
    """Interface to Ceph service Grafana via CLI."""

    SERVICE_NAME = "grafana"

    def apply(self, config: Dict) -> None:
        """
        Deploy the grafana service using the provided configuration.

        Args:
            config: Key/value pairs provided by the test case to create the service.

        Example
            config:
                command: apply
                service: grafana
                base_cmd_args:          # arguments to ceph orch
                    concise: true
                    verbose: true
                    input_file: <name of spec>
                args:
                    placement:
                        label: grafana    # either label or node.
                        nodes:
                            - node1
                        limit: 3    # no of daemons
                        sep: " "    # separator to be used for placements
                    dry-run: true
                    unmanaged: true
        """
        super().apply(config=config)
