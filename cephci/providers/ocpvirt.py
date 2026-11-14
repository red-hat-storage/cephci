"""OpenShift Virtualization based infrastructure."""

from typing import Dict, List, Optional

from libcloud.common.types import LibcloudError

from ceph.utils import create_ocpvirt_ceph_nodes
from cephci.providers.base import CloudProvider, NodeLogin
from compute.onecloud import expand_private_key_path
from compute.openshift import cleanup_ocpvirt_ceph_nodes, resolve_ocpvirt_credentials


class OCPVirtProvider(CloudProvider):
    """Provisions the system under test on OpenShift Virtualization."""

    name = "ocpvirt"
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
        return create_ocpvirt_ceph_nodes(
            cluster, inventory, osp_cred, run_id, prefix, custom_config
        )

    def cleanup(
        self,
        osp_cred: Dict,
        prefix: Optional[str],
        custom_config: Optional[List[str]] = None,
    ) -> None:
        """Remove the virtual machines matching the given prefix."""
        cleanup_ocpvirt_ceph_nodes(osp_cred, prefix, custom_config=custom_config)

    def login(
        self,
        node,
        osp_cred: Dict,
        custom_config: Optional[List[str]] = None,
    ) -> NodeLogin:
        """Describe how to reach the given node."""
        ocp_cfg = resolve_ocpvirt_credentials(osp_cred, custom_config)
        private_key_path = ocp_cfg.get("private_key_path", "")

        return NodeLogin(
            private_ip=node.ip_address,
            ceph_nodename=node.hostname,
            look_for_key=bool(private_key_path),
            private_key_path=(
                expand_private_key_path(private_key_path) if private_key_path else ""
            ),
        )
