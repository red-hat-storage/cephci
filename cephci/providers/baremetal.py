"""User provided infrastructure, also known as baremetal."""

from typing import Dict, List, Optional

from ceph.utils import create_baremetal_ceph_nodes
from cephci.providers.base import CloudProvider, NodeLogin
from compute.onecloud import expand_private_key_path


class BaremetalProvider(CloudProvider):
    """Uses an existing environment described by the cluster configuration."""

    name = "baremetal"

    # The nodes already exist, there is nothing to remove or wait for.
    cleanup_before_create = False
    supports_cluster_networks = True

    def create(
        self,
        cluster: Dict,
        inventory: Dict,
        osp_cred: Dict,
        run_id: str,
        prefix: Optional[str],
        custom_config: Optional[List[str]] = None,
        platform: Optional[str] = None,
    ) -> Dict:
        """Collect the nodes described by the cluster configuration."""
        return create_baremetal_ceph_nodes(cluster)

    def login(
        self,
        node,
        osp_cred: Dict,
        custom_config: Optional[List[str]] = None,
    ) -> NodeLogin:
        """Describe how to reach the given node."""
        private_key_path = node.private_key if node.private_key else ""

        return NodeLogin(
            private_ip=node.ip_address,
            ceph_nodename=node.hostname,
            root_password=node.root_password or "passwd",
            look_for_key=bool(node.private_key),
            private_key_path=(
                expand_private_key_path(private_key_path) if private_key_path else ""
            ),
        )
