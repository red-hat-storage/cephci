"""Manage the RBD Mirror service via the cephadm CLI."""
from typing import Dict

from .apply import ApplyMixin
from .orch import Orch


class RbdMirror(ApplyMixin, Orch):
    """Interface to ceph orch <action> rbd-mirror."""

    SERVICE_NAME = "rbd-mirror"

    def apply(self, config: Dict) -> None:
        """
        Deploy the rbd-mirror service using the provided configuration.

        Args:
            config: Key/value pairs provided by the test case to create the service.

        Example
            config:
                command: apply
                service: rbd-mirror
                base_cmd_args:          # arguments to ceph orch
                    concise: true
                    verbose: true
                    input_file: <name of spec>
                args:
                    placement:
                        label: rbd-mirror    # either label or node.
                        nodes:
                            - node1
                        sep: " "    # separator to be used for placements

        """
        super().apply(config=config)
