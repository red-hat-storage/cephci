"""
ISCSI module to deploy iscsi service and daemons.

this module deploy iscsi service and daemon(s) along with
handling other prerequisites needed for deployment.

"""
from ceph.ceph import ResourcesNotFoundError
from ceph.ceph_admin.apply import Apply
from ceph.ceph_admin.daemon_mixin import DaemonMixin
from ceph.utils import get_nodes_by_id


class ISCSIRole(Apply, DaemonMixin):
    __ROLE = "iscsi"

    def apply_iscsi(self, **config):
        """
        Deploy ISCSI target sing "orch apply" option
        Args:
            config: apply config data

        config:
            nodes: list of nodes where iscsi to be deployed
            pool: pool_name
            api_user: username
            api_password: user_password
            trusted_ip_list: [ip_addr1, ip_addr2]
        """
        pool = config.get("pool_name", "iscsi")
        pg_num = config.get("pool_pg_num", 3)
        pgp_num = config.get("pool_pgp_num", 3)
        user = config.get("api_user", "user")
        password = config.get("api_password", "password")
        trusted_ip_list = config.get("trusted_ip_list", [])

        cmd = Apply.apply_cmd + [self.__ROLE]
        nodes = get_nodes_by_id(self.cluster, config.get("nodes"))

        cmd.extend([pool, user, password, repr(" ".join(trusted_ip_list))])

        if not nodes:
            raise ResourcesNotFoundError("Nodes not found: %s", config.get("nodes"))

        host_placement = [node.shortname for node in nodes]
        cmd.append("--placement '{}'".format(";".join(host_placement)))

        # Create pool
        self.shell(
            remote=self.installer,
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
        self.shell(
            remote=self.installer,
            args=["ceph", "osd", "pool", "application", "enable", pool, "rbd"],
        )

        Apply.apply(
            self,
            role=self.__ROLE,
            command=cmd,
            placements=host_placement,
        )

    def daemon_add_iscsi(self):
        raise NotImplementedError
