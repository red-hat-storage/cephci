"""Manage MDS service via Ceph's cephadm CLI."""
from typing import Dict

from .apply import ApplyMixin
from .orch import Orch


class MDS(ApplyMixin, Orch):
    """Interface to the MetaDataService."""

    SERVICE_NAME = "mds"

    def apply(self, config: Dict) -> None:
        """
        Deploy the MDS service using the provided configuration.

        Args:
            config: Key/value pairs provided by the test case to create the service.

        Example
            config:
                command: apply
                service: mds
                base_cmd_args:          # arguments to ceph orch
                    concise: true
                    verbose: true
                    input_file: <name of spec>
                pos_args:
                    - india             # name of the filesystem
                args:
                    placement:
                        label: mds    # either label or node.
                        nodes:
                            - node1
                        limit: 3    # no of daemons
                        sep: " "    # separator to be used for placements
                    dry-run: true
                    unmanaged: true
        """
        super().apply(config=config)
