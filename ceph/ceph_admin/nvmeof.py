"""Manage the NVMeoF service via ceph-adm CLI."""

from typing import Dict

from .apply import ApplyMixin
from .orch import Orch


class NVMeoF(ApplyMixin, Orch):
    """Interface to ceph orch <action> nvmeof."""

    SERVICE_NAME = "nvmeof"

    def apply(self, config: Dict) -> None:
        """
        Deploy the NVMEoF service using the provided configuration.

        Args:
            config (Dict): Key/value pairs provided by the test case to create the service.

        Example::

            config:
                command: apply
                service: mon
                base_cmd_args:          # arguments to ceph orch
                    concise: true
                    verbose: true
                    input_file: <name of spec>
                pos_args:               # positional arguments
                    - rbd
                args:
                    placement:
                        label: nvmeof    # either label or node.
                        nodes:
                            - node1
                        limit: 3    # no of daemons
                        sep: " "    # separator to be used for placements
                    dry-run: true
                    unmanaged: true
        """
        super().apply(config=config)
