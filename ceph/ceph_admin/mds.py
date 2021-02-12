"""Manage MDS service via Ceph's cephadm CLI."""
from typing import Optional, Dict

from .apply import ApplyMixin
from .orch import Orch


class MDS(ApplyMixin, Orch):
    """Interface to the MetaDataService."""

    SERVICE_NAME = "mds"

    def apply(
            self,
            prefix_args: Optional[Dict] = None,
            args: Optional[Dict] = None
    ) -> None:
        """
        Deploy the MDS service using the provided configuration.

        Args:
            prefix_args:    The key/value pairs to be passed to the base command.
            args:           The key/value pairs to be passed to the command.

        config:
            command: apply
            service: mds
            prefix_args:
                fs_name: india
            args:
                label: mds    # either label or node.
                nodes:
                    - node1
                limit: 3    # no of daemons
                sep: " "    # separator to be used for placements
        """
        fs_name = prefix_args.get("fs_name", "mds_fs")

        prefix_list = [fs_name]

        super().apply(prefix_args=prefix_list, args=args)
