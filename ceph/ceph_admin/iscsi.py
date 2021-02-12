"""Module to deploy and manage Ceph's iSCSI service."""
from typing import Dict, Optional

from .apply import ApplyMixin
from .orch import Orch


class ISCSI(ApplyMixin, Orch):
    """Interface to Ceph's iSCSI service via cephadm CLI."""

    SERVICE_NAME = "iscsi"

    def apply(
            self,
            prefix_args: Optional[Dict] = None,
            args: Optional[Dict] = None
    ) -> None:
        """
        Deploy ISCSI client daemon using the provided arguments.

        Fixme: This method does not consider the prefix args to the base command.

        Args:
            prefix_args:    Key/value pairs to be passed to the command.
            args:           Key/value pairs to be passed as arguments to the command.

        config:
            command: apply
            service: iscsi
            prefix_args:
                pool_name: india
                pool_pg_num: 3
                pool_pgp_num: 3
                api_user: user
                api_password: password
                trusted_ip_list: list of ips
            args:
                label: iscsi    # either label or node.
                nodes:
                    - node1
                limit: 3    # no of daemons
                sep: " "    # separator to be used for placements
        """
        pool = prefix_args.get("pool_name", "iscsi")
        pg_num = prefix_args.get("pool_pg_num", 3)
        pgp_num = prefix_args.get("pool_pgp_num", 3)
        user = prefix_args.get("api_user", "user")
        password = prefix_args.get("api_password", "password")
        trusted_ip_list = prefix_args.get("trusted_ip_list", [])

        prefix_list = list(
            [pool, user, password, repr(" ".join(trusted_ip_list))]
        )

        # Execute pre-requisites
        self.shell(
            args=[
                "ceph",
                "osd",
                "pool",
                "create",
                pool,
                str(pg_num),
                str(pgp_num),
                "replicated",
            ],
        )

        # Associate pool to RBD application
        self.shell(args=["ceph", "osd", "pool", "application", "enable", pool, "rbd"])

        super().apply(prefix_args=prefix_list, args=args)
