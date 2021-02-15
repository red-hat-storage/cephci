"""Manage the NFS service via the cephadm CLI."""
from typing import Dict, Optional
from .apply import ApplyMixin
from .orch import Orch


class NFS(ApplyMixin, Orch):
    """Interface to ceph orch <action> nfs."""

    SERVICE_NAME = "nfs"

    def apply(
        self,
        prefix_args: Optional[Dict] = None,
        args: Optional[Dict] = None,
    ) -> None:
        """
        Deploy the NFS service.

        Args:
            prefix_args:    The key/value pairs to be passed to the base command.
            args:           The key/value pairs to be passed to the command.

        config:
            command: apply
            service: nfs
            prefix_args:
                name: india
                pool: south
            args:
                label: nfs    # either label or node.
                nodes:
                    - node1
                limit: 3    # no of daemons
                sep: " "    # separator to be used for placements
        """
        name = prefix_args.get("name", "nfs1")
        pool = prefix_args.get("pool", "nfs_pool")
        prefix_list = [name, pool]

        super().apply(prefix_args=prefix_list, args=args)
