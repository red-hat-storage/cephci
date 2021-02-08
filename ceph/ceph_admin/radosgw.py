"""
Module to deploy RGW service and individual daemon(s).

this module deploy RGW service and daemon(s) along with
handling other prerequisites needed for deployment.

"""
from ceph.ceph import ResourcesNotFoundError
from ceph.ceph_admin.apply import Apply
from ceph.ceph_admin.daemon_mixin import DaemonMixin
from ceph.utils import get_nodes_by_id


class RadosGWRole(Apply, DaemonMixin):
    __ROLE = "rgw"

    def apply_rgw(self, **config):
        """
        Deploy RGW service using "orch apply" option
        Args:
            config: test arguments

        config:
            nodes: ['node1', 'node2']
            realm: realm_name
            zone: zone_name
        """
        realm = config.get("realm", "realm")
        zone = config.get("zone", "zone")
        cmd = Apply.apply_cmd + [self.__ROLE]
        nodes = get_nodes_by_id(self.cluster, config.get("nodes"))
        cmd.extend([realm, zone])

        if not nodes:
            raise ResourcesNotFoundError("Nodes not found: %s", config.get("nodes"))

        host_placement = [node.shortname for node in nodes]
        cmd.append("--placement '{}'".format(";".join(host_placement)))

        Apply.apply(
            self,
            role=self.__ROLE,
            command=cmd,
            placements=host_placement,
        )

    def daemon_add_rgw(self, **config):
        raise NotImplementedError

    def daemon_remove_rgw(self, **config):
        raise NotImplementedError
