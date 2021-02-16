"""Manage the NFS service via the cephadm CLI."""
from typing import Dict

from .apply import ApplyMixin
from .orch import Orch


class NFS(ApplyMixin, Orch):
    """Interface to ceph orch <action> nfs."""

    SERVICE_NAME = "nfs"

    def apply(self, config: Dict) -> None:
        """
        Deploy the NFS service using the provided configuration.

        Args:
            config: Key/value pairs provided by the test case to create the service.

        Example
            config:
                command: apply
                service: nfs
                base_cmd_args:          # arguments to ceph orch
                    concise: true
                    verbose: true
                    input_file: <name of spec>
                pos_args:
                    - india             # service identity
                    - southpool         # name of the pool
                args:
                    namespace: <name>       # namespace
                    placement:
                        label: nfs    # either label or node.
                        nodes:
                            - node1
                        limit: 3    # no of daemons
                        sep: " "    # separator to be used for placements
                    dry-run: true
                    unmanaged: true
        """
        super().apply(config=config)
