"""Manage the Crash service via cephadm CLI."""

from typing import Dict

from .apply import ApplyMixin
from .orch import Orch


class Crash(ApplyMixin, Orch):
    """Interface to ceph orch <action> crash."""

    SERVICE_NAME = "crash"

    def apply(self, config: Dict) -> None:
        """
        Deploy the Crash service using the provided configuration.

        Args:
            config (Dict): Key/value pairs provided by the test case to create the service.

        Example::

            config:
                command: apply
                service: crash
                base_cmd_args:          # arguments to ceph orch
                    concise: true
                    verbose: true
                    input_file: <name of spec>
                args:
                    placement:
                        label: crash    # either label or node.
                        nodes:
                            - node1
                        limit: 3    # no of daemons
                        sep: " "    # separator to be used for placements
                    dry-run: true
                    unmanaged: true
        """
        super().apply(config=config)
