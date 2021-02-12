"""
ISCSI module to deploy iscsi service and daemons.

this module deploy iscsi service and daemon(s) along with
handling other prerequisites needed for deployment.

"""
from ceph.ceph_admin.apply import ApplyMixin

from .orch import Orch


class ISCSI(ApplyMixin, Orch):

    SERVICE_NAME = "iscsi"

    def apply(self, **config):
        """
        Deploy ISCSI client daemon using the provided config.

        Args:
            config: test arguments

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
        prefix_args = config.pop("prefix_args")
        pool = prefix_args.get("pool_name", "iscsi")
        pg_num = prefix_args.get("pool_pg_num", 3)
        pgp_num = prefix_args.get("pool_pgp_num", 3)
        user = prefix_args.get("api_user", "user")
        password = prefix_args.get("api_password", "password")
        trusted_ip_list = prefix_args.get("trusted_ip_list", [])

        config["prefix_args"] = list(
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

        super().apply(**config)
