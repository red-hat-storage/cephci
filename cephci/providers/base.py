"""Contract shared by every infrastructure backend supported by CephCI.

A provider knows three things about its cloud: how to create the virtual machines for a
cluster, how to remove them, and how CephCI should log in to them. Everything else about
provisioning, such as building the ``Ceph`` objects or waiting for the nodes to settle,
is common and lives in :mod:`cephci.orchestration.provisioning`.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from utility.log import Log

log = Log(__name__)


@dataclass
class NodeLogin:
    """Access details CephCI uses to reach a provisioned node.

    The values are final, that is they are handed to ``CephNode`` unchanged. Each
    provider is responsible for resolving its own defaults so that the common
    provisioning code does not need to know which cloud it is talking to.
    """

    private_ip: str
    ceph_nodename: str
    username: str = "cephuser"
    password: str = "cephuser"
    root_username: Optional[str] = None
    root_password: str = "passwd"
    look_for_key: bool = False
    private_key_path: str = ""
    private_key_password: Optional[str] = None
    bootstrap_key_path: str = ""
    bootstrap_key_password: str = ""


class CloudProvider:
    """Base class for the supported infrastructure backends."""

    #: Value of --cloud that selects this provider.
    name: str = ""

    #: Exceptions worth retrying when the cloud misbehaves during creation.
    retry_exceptions: Tuple[type, ...] = tuple()

    #: Seconds to wait after creation before the nodes are expected to be reachable.
    post_create_wait: int = 15

    #: Whether an environment of the same name is removed before creating a new one.
    cleanup_before_create: bool = True

    #: Whether the cluster level network definitions are carried into the Ceph object.
    supports_cluster_networks: bool = False

    def prepare(
        self, osp_cred: Dict, custom_config: Optional[List[str]] = None
    ) -> None:
        """Resolve whatever the provider needs once per cluster creation.

        Args:
            osp_cred (dict): Credentials for the cloud.
            custom_config (list): Items of the form ``<key>=<value>``.

        Returns:
            None
        """

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
        """Create the virtual machines of a single cluster.

        Args:
            cluster (dict): A ``globals`` entry of the cluster configuration.
            inventory (dict): The system inventory to provision from.
            osp_cred (dict): Credentials for the cloud.
            run_id (str): Execution identifier.
            prefix (str): Name prefix shared by the created instances.
            custom_config (list): Items of the form ``<key>=<value>``.
            platform (str): Base operating system, for example ``rhel-9``.

        Returns:
            dict: Name to cloud node object.

        Raises:
            NotImplementedError: When the provider does not implement creation.
        """
        raise NotImplementedError

    def cleanup(
        self,
        osp_cred: Dict,
        prefix: Optional[str],
        custom_config: Optional[List[str]] = None,
    ) -> None:
        """Remove the instances and volumes matching the given prefix.

        Args:
            osp_cred (dict): Credentials for the cloud.
            prefix (str): Name prefix shared by the instances to be removed.
            custom_config (list): Items of the form ``<key>=<value>``.

        Returns:
            None
        """
        log.warning("Unknown cloud type.")

    def login(
        self,
        node,
        osp_cred: Dict,
        custom_config: Optional[List[str]] = None,
    ) -> NodeLogin:
        """Describe how to reach the given node.

        Args:
            node: The cloud node object returned by :meth:`create`.
            osp_cred (dict): Credentials for the cloud.
            custom_config (list): Items of the form ``<key>=<value>``.

        Returns:
            NodeLogin: Access details for the node.

        Raises:
            NotImplementedError: When the provider does not implement login details.
        """
        raise NotImplementedError
