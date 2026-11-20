"""OneCloud based infrastructure."""

from typing import Dict, List, Optional

from libcloud.common.types import LibcloudError

from ceph.utils import create_onecloud_ceph_nodes
from cephci.providers.base import CloudProvider, NodeLogin
from compute.onecloud import cleanup_onecloud_ceph_nodes, expand_private_key_path
from utility.log import Log

log = Log(__name__)


class OneCloudProvider(CloudProvider):
    """Provisions the system under test on OneCloud."""

    name = "onecloud"
    retry_exceptions = (LibcloudError,)

    # OneCloud instances need longer than the other clouds before they accept logins.
    post_create_wait = 75

    def __init__(self) -> None:
        """Initialize the provider with an unresolved credential set."""
        self._credentials: Dict = {}

    def prepare(
        self, osp_cred: Dict, custom_config: Optional[List[str]] = None
    ) -> None:
        """Resolve the OneCloud credentials once for the cluster being created.

        The details are read from ``globals.onecloud-credentials`` of the credentials
        file, falling back to ``credentials.cloud.onecloud`` of ``~/.cephci.yaml``.

        Args:
            osp_cred (dict): Credentials for the cloud.
            custom_config (list): Items of the form ``<key>=<value>``.

        Returns:
            None
        """
        globals_ = osp_cred.get("globals") or {}
        self._credentials = (
            (globals_.get("onecloud-credentials") if isinstance(globals_, dict) else {})
            or ((osp_cred.get("credentials") or {}).get("cloud") or {}).get("onecloud")
            or {}
        )
        log.info(
            "OneCloud auth: private_key_path=%s",
            self._credentials.get("private_key_path", "(none)"),
        )

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
        return create_onecloud_ceph_nodes(
            cluster,
            inventory,
            osp_cred,
            run_id,
            prefix,
            custom_config,
            platform=platform,
        )

    def cleanup(
        self,
        osp_cred: Dict,
        prefix: Optional[str],
        custom_config: Optional[List[str]] = None,
    ) -> None:
        """Remove the instances matching the given prefix."""
        cleanup_onecloud_ceph_nodes(osp_cred, prefix, custom_config=custom_config)

    def login(
        self,
        node,
        osp_cred: Dict,
        custom_config: Optional[List[str]] = None,
    ) -> NodeLogin:
        """Describe how to reach the given node."""
        if not self._credentials:
            self.prepare(osp_cred, custom_config)

        credentials = self._credentials
        ssh_username = credentials.get("ssh_user", "onecloud-user")
        private_key_path = credentials.get("private_key_path", "")

        return NodeLogin(
            private_ip=node.ip_address,
            ceph_nodename=node.hostname,
            username=ssh_username,
            # The login is key based, the password only matters for a root login.
            password="passwd" if ssh_username == "root" else "cephuser",
            root_username=ssh_username,
            # Privilege escalation relies on the key, never on a password.
            root_password="",
            look_for_key=True,
            private_key_path=(
                expand_private_key_path(private_key_path) if private_key_path else ""
            ),
            private_key_password=credentials.get("private_key_password"),
            bootstrap_key_path=expand_private_key_path(
                credentials.get("bootstrap_key_path", "")
            ),
            bootstrap_key_password=credentials.get("bootstrap_key_password", ""),
        )
