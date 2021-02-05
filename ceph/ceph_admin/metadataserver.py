"""
Module to deploy MDS service and daemon(s).

this module deploy MDS service and daemon(s) along with
handling other prerequisites needed for deployment.

"""
import logging

from ceph.ceph import ResourcesNotFoundError
from ceph.ceph_admin.apply import Apply
from ceph.ceph_admin.daemon_mixin import DaemonMixin
from ceph.utils import CommandFailed, get_nodes_by_id

logger = logging.getLogger(__name__)


class MetaDataServerRole(Apply, DaemonMixin):
    __ROLE = "mds"

    def apply_mds(self, **config):
        """
        Deploy MDS service using orch apply

        Args:
            config: apply config arguments

        config:
            nodes: ['node1', 'node2']
            file_system: 'cephfs' # default: cephfs
        """
        cmd = Apply.apply_cmd + [self.__ROLE]
        nodes = get_nodes_by_id(self.cluster, config.get("nodes"))

        if not nodes:
            raise ResourcesNotFoundError("Nodes not found: %s", config.get("nodes"))

        host_placement = [node.shortname for node in nodes]

        # workaround, ceph-orch should create fs volume
        try:
            file_system = config.pop("file_system", "cephfs")
            cmd.extend(
                [file_system, "--placement '{}'".format(";".join(host_placement))]
            )
            self.shell(
                remote=self.installer,
                args=[
                    "ceph",
                    "fs",
                    "volume",
                    "create",
                    file_system,
                ],
            )
        except CommandFailed as warn:
            logger.warning(warn)

        Apply.apply(
            self,
            role=self.__ROLE,
            command=cmd,
            placements=host_placement,
        )

    def daemon_add_mds(self):
        raise NotImplementedError
