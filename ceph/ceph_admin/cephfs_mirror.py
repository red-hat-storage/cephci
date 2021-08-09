"""Manage the CephFS Mirror service via the cephadm CLI."""
from typing import Dict

from .apply import ApplyMixin
from .orch import Orch


class CephfsMirror(ApplyMixin, Orch):
    """Interface to ceph orch <action> rbd-mirror."""

    SERVICE_NAME = "cephfs-mirror"

    def apply(self, config: Dict) -> None:
        """
        Execute the command
        ceph orch apply cephfs-mirror

        Args:
            config (Dict): command and service are passed from the test case.
            op (Str): operation parameters

        Example ::

            config:
                command: apply
                service: cephfs-mirror
                base_cmd_args:          # arguments to ceph orch
                    concise: true
                    verbose: true
                    input_file: <name of spec>
                args:
                    placement:
                        label: cephfs-mirror    # either label or node.
                        nodes:
                            - node1
                        sep: " "    # separator to be used for placements

        """
        super().apply(config=config)
