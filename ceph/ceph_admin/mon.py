"""Manage the Monitor service via cephadm CLI."""
from typing import Dict

from .apply import ApplyMixin
from .orch import Orch


class Mon(ApplyMixin, Orch):
    """Interface to ceph orch <action> mon."""

    SERVICE_NAME = "mon"

    def apply(self, config: Dict) -> None:
        """
        Deploy the Monitor service using the provided configuration.

        Args:
            config: Key/value pairs provided by the test case to create the service.

        Example
            config:
                command: apply
                service: mon
                base_cmd_args:          # arguments to ceph orch
                    concise: true
                    verbose: true
                    input_file: <name of spec>
                args:
                    placement:
                        label: mon    # either label or node.
                        nodes:
                            - node1
                        limit: 3    # no of daemons
                        sep: " "    # separator to be used for placements
                    dry-run: true
                    unmanaged: true
        """
        super().apply(config=config)
