"""Red Hat OpenStack based infrastructure."""

from typing import Dict, List, Optional

from libcloud.common.types import LibcloudError

from ceph.utils import cleanup_ceph_nodes, create_ceph_nodes
from cephci.providers.base import CloudProvider, NodeLogin


class OpenStackProvider(CloudProvider):
    """Provisions the system under test on Red Hat OpenStack."""

    name = "openstack"
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
        enable_eus: bool = False,
    ) -> Dict:
        """Create the virtual machines of a single cluster."""
        return create_ceph_nodes(
            cluster,
            inventory,
            osp_cred,
            run_id,
            prefix,
            enable_eus=enable_eus,
            custom_config=custom_config,
        )

    def cleanup(
        self,
        osp_cred: Dict,
        prefix: Optional[str],
        custom_config: Optional[List[str]] = None,
    ) -> None:
        """Remove the instances and volumes matching the given prefix."""
        cleanup_ceph_nodes(osp_cred, prefix)

    def login(
        self,
        node,
        osp_cred: Dict,
        custom_config: Optional[List[str]] = None,
    ) -> NodeLogin:
        """Describe how to reach the given node."""
        return NodeLogin(
            private_ip=node.get_private_ip(),
            ceph_nodename=node.node.name,
        )
