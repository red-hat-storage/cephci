"""
Module to deploy MDS service and daemon(s).

this module deploy MDS service and daemon(s) along with
handling other prerequisites needed for deployment.

"""
from ceph.ceph_admin.apply import ApplyMixin

from .orch import Orch


class MDS(ApplyMixin, Orch):

    SERVICE_NAME = "mds"

    def apply(self, **config):
        """
        Deploy the MDS service using the provided configuration.

        Args:
            config: test arguments

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
        prefix_args = config.pop("prefix_args")
        fs_name = prefix_args.get("fs_name", "mds_fs")

        config["prefix_args"] = [fs_name]

        super().apply(**config)
