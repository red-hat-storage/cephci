"""Module to deploy and manage Ceph's iSCSI service."""

from typing import Dict

from ceph.utils import get_nodes_by_ids

from .apply import ApplyMixin
from .orch import Orch


class ISCSI(ApplyMixin, Orch):
    """Interface to Ceph's iSCSI service via cephadm CLI."""

    SERVICE_NAME = "iscsi"

    def apply(self, config: Dict) -> None:
        """
        Deploy ISCSI client daemon using the provided arguments.

        Args:
            config (Dict) : Key/value pairs provided from the test scenario

        Example::

            config:
                command: apply
                service: iscsi
                base_cmd_args:          # arguments to ceph orch
                    concise: true
                    verbose: true
                    input_file: <name of spec>
                pos_args:
                    - india             # name of the pool
                    - api_user          # name of the API user
                    - api_pass          # password of the api_user.

                args:
                    trusted_ip_list:       #it can be used both as positional/keyword arg in 5.x
                        - node1
                        - node2               # space separate list of IPs
                    placement:
                        label: iscsi    # either label or node.
                        nodes:
                            - node1
                        limit: 3    # no of daemons
                        sep: " "    # separator to be used for placements
                    dry-run: true
                    unmanaged: true

        """
        args = config.get("args")
        trusted_ip_list = args.get("trusted_ip_list")
        if trusted_ip_list:
            node_ips = get_nodes_by_ids(self.cluster, trusted_ip_list)
            args["trusted_ip_list"] = repr(
                " ".join([node.ip_address for node in node_ips])
            )
        super().apply(config=config)
