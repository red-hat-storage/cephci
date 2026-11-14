"""IBM Cloud VPC based infrastructure."""

from typing import Dict, List, Optional

from libcloud.common.types import LibcloudError

from ceph.utils import cleanup_ibmc_ceph_nodes, create_ibmc_ceph_nodes
from cephci.providers.base import CloudProvider, NodeLogin
from compute.onecloud import expand_private_key_path


class IBMCProvider(CloudProvider):
    """Provisions the system under test on IBM Cloud."""

    name = "ibmc"
    retry_exceptions = (LibcloudError,)

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
        """Create the virtual machines of a single cluster."""
        return create_ibmc_ceph_nodes(
            cluster, inventory, osp_cred, run_id, prefix, custom_config
        )

    def cleanup(
        self,
        osp_cred: Dict,
        prefix: Optional[str],
        custom_config: Optional[List[str]] = None,
    ) -> None:
        """Remove the instances, volumes and DNS records matching the prefix."""
        cleanup_ibmc_ceph_nodes(osp_cred, prefix, custom_config=custom_config)

    def login(
        self,
        node,
        osp_cred: Dict,
        custom_config: Optional[List[str]] = None,
    ) -> NodeLogin:
        """Describe how to reach the given node."""
        ibmc = osp_cred.get("globals").get("ibm-credentials")
        private_key_path = ibmc.get("private_key_path")

        return NodeLogin(
            private_ip=node.ip_address,
            ceph_nodename=node.hostname,
            look_for_key=True,
            private_key_path=(
                expand_private_key_path(private_key_path) if private_key_path else ""
            ),
        )
