"""Module to deploy Prometheus service and individual daemon(s)."""
from typing import Dict

from .apply import ApplyMixin
from .orch import Orch


class Prometheus(ApplyMixin, Orch):
    SERVICE_NAME = "prometheus"

    def apply(self, config: Dict) -> None:
        """
        Deploy the Prometheus service using the provided configuration.

        Args:
            config: Key/value pairs provided by the test case to create the service.

        Example
            config:
                command: apply
                service: prometheus
                base_cmd_args:          # arguments to ceph orch
                    concise: true
                    verbose: true
                    input_file: <name of spec>
                args:
                    placement:
                        label: prometheus    # either label or node.
                        nodes:
                            - node1
                        limit: 3    # no of daemons
                        sep: " "    # separator to be used for placements
                    dry-run: true
                    unmanaged: true
        """
        super().apply(config=config)
