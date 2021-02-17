"""Manage the Manager service via Ceph's cephadm CLI."""
from typing import Dict

from .apply import ApplyMixin
from .orch import Orch


class Mgr(ApplyMixin, Orch):
    """Manager interface for ceph orch <action> mgr"""

    SERVICE_NAME = "mgr"

    def apply(self, config: Dict) -> None:
        """
        Deploy the Manager service using the provided configuration.

        Args:
            config: Key/value pairs provided by the test case to create the service.

        Example
            config:
                command: apply
                service: mgr
                base_cmd_args:          # arguments to ceph orch
                    concise: true
                    verbose: true
                    input_file: <name of spec>
                pos_args:
                    - india             # name of the filesystem
                args:
                    placement:
                        label: mgr    # either label or node.
                        nodes:
                            - node1
                        limit: 3    # no of daemons
                        sep: " "    # separator to be used for placements
                    dry-run: true
                    unmanaged: true
        """
        super().apply(config=config)
