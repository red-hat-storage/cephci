"""Creation and teardown of the system under test.

The cloud specific parts live in :mod:`cephci.providers`; this module owns everything
that is the same whichever infrastructure is in use, namely validating the cluster
configuration, turning cloud nodes into :class:`ceph.ceph.CephNode` objects and waiting
for them to become reachable.
"""

import time
from copy import deepcopy
from typing import Dict, List, Optional, Tuple

from ceph.ceph import Ceph, CephNode
from ceph.clients import WinNode
from cephci.providers import CloudProvider
from utility.log import Log
from utility.retry import retry
from utility.utils import resolve_use_ipv6, validate_conf, validate_image

log = Log(__name__)

# Number of attempts and initial delay when the cloud fails during creation.
CREATE_RETRIES = 5
CREATE_RETRY_DELAY = 15

# The cleanup performed before creating an environment forwards --custom-config only
# for these clouds. IBM Cloud and AWS derive their VPC, zone and region from the
# credentials file instead. Teardown requested by a suite behaves the same way.
CUSTOM_CONFIG_AWARE_CLEANUP = ("onecloud", "ocpvirt")


def cleanup_environment(
    provider: CloudProvider,
    osp_cred: Dict,
    prefix: Optional[str],
    custom_config: Optional[List[str]] = None,
) -> None:
    """Remove an environment created by a previous or the current execution.

    Args:
        provider (CloudProvider): The infrastructure holding the environment.
        osp_cred (dict): Credentials for the cloud.
        prefix (str): Name prefix shared by the instances to be removed.
        custom_config (list): Items of the form ``<key>=<value>``. Forwarded only to
            the clouds that resolve their platform from it.

    Returns:
        None
    """
    if provider.name not in CUSTOM_CONFIG_AWARE_CLEANUP:
        custom_config = None

    provider.cleanup(osp_cred, prefix, custom_config=custom_config)


def create_nodes(
    conf: Dict,
    inventory: Optional[Dict],
    osp_cred: Dict,
    run_id: str,
    provider: CloudProvider,
    cloud_type: str = "openstack",
    instances_name: Optional[str] = None,
    enable_eus: bool = False,
    custom_config: Optional[List[str]] = None,
    platform: Optional[str] = None,
) -> Tuple[Dict, List]:
    """Create the system under test environment.

    Args:
        conf (dict): Cluster layout.
        inventory (dict): Test environment.
        osp_cred (dict): Platform access information.
        run_id (str): Execution identifier.
        provider (CloudProvider): The infrastructure to provision on.
        cloud_type (str): The value of --cloud, used by the configuration validators.
        instances_name (str): Name prefix shared by the created instances.
        enable_eus (bool): Extended OS support.
        custom_config (list): Items of the form ``<key>=<value>``.
        platform (str): Base operating system, for example ``rhel-9``.

    Returns:
        tuple: Mapping of cluster name to :class:`ceph.ceph.Ceph`, and the list of
            Windows clients found in the environment.

    Notes:
        use custom_config to specify the environments or configuration to
        be used when using generic inventory files. Ensure to prefix the
        platform. For exmaple

            --custom-config ibmc_vpc=ci-vpc-01
            --custom-config ibmc_profile=bx2-2x8
            --custom-config ocpvirt_namespace=rdu3_ceph_jenkins
            --custom-config ocpvirt_profile=cx1.2xlarge  # optional; default o1.large
            # or custom sizing (mutually exclusive with ocpvirt_profile):
            # --custom-config ocpvirt_cpu=4 --custom-config ocpvirt_memory=16Gi
            --custom-config openstack_vm_profile=c1.standard.xl
            --custom-config openstack_networks=provider_net_cci_1
            --custom-config use_ipv6=true

        If these values are not provided then the defaults would be used.
        openstack_networks (single or comma-separated) overrides cluster conf for all
        OpenStack VMs. The defaults are the ones used in the example.
    """
    worker = _create_nodes
    if provider.retry_exceptions:
        worker = retry(
            provider.retry_exceptions, tries=CREATE_RETRIES, delay=CREATE_RETRY_DELAY
        )(worker)

    return worker(
        conf,
        inventory,
        osp_cred,
        run_id,
        provider,
        cloud_type,
        instances_name,
        enable_eus,
        custom_config,
        platform,
    )


def _create_nodes(
    conf: Dict,
    inventory: Optional[Dict],
    osp_cred: Dict,
    run_id: str,
    provider: CloudProvider,
    cloud_type: str,
    instances_name: Optional[str],
    enable_eus: bool,
    custom_config: Optional[List[str]],
    platform: Optional[str],
) -> Tuple[Dict, List]:
    """Create the environment, see :func:`create_nodes` for the arguments."""
    validate_conf(conf)
    validate_image(conf, cloud_type)

    if provider.cleanup_before_create:
        cleanup_environment(provider, osp_cred, instances_name, custom_config)

    ceph_cluster_dict = {}
    clients = []

    for cluster in conf.get("globals"):
        kwargs = {"enable_eus": enable_eus} if provider.name == "openstack" else {}
        ceph_vmnodes = provider.create(
            cluster,
            inventory,
            osp_cred,
            run_id,
            instances_name,
            custom_config=custom_config,
            platform=platform,
            **kwargs,
        )

        # Resolve use_ipv6 before building nodes so CephNode can use it for SSH
        # when requested
        use_ipv6 = resolve_use_ipv6(custom_config, cloud_type, osp_cred)

        provider.prepare(osp_cred, custom_config)

        ceph_nodes = []
        for node in ceph_vmnodes.values():
            login = provider.login(node, osp_cred, custom_config)

            if node.role == "win-iscsi-clients":
                clients.append(
                    WinNode(ip_address=node.ip_address, private_ip=login.private_ip)
                )
                continue

            # IPv6 attrs only when available (OpenStack dual-stack); other envs have
            # no ipv6_* on node
            ceph_nodes.append(
                CephNode(
                    username=login.username,
                    password=login.password,
                    root_password=login.root_password,
                    root_username=login.root_username,
                    look_for_key=login.look_for_key,
                    private_key_path=login.private_key_path,
                    private_key_password=login.private_key_password,
                    bootstrap_key_path=login.bootstrap_key_path,
                    bootstrap_key_password=login.bootstrap_key_password,
                    root_login=node.root_login,
                    role=node.role,
                    no_of_volumes=node.no_of_volumes,
                    ipv4_address=node.ip_address,
                    ipv4_subnet=node.subnet,
                    private_ip=login.private_ip,
                    hostname=node.hostname,
                    ceph_vmnode=node,
                    ceph_nodename=login.ceph_nodename,
                    id=node.id,
                    ipv6_address=getattr(node, "ipv6_address", None),
                    ipv6_subnet=getattr(node, "ipv6_subnet", None),
                    use_ipv6=use_ipv6,
                )
            )

        cluster_name = cluster.get("ceph-cluster").get("name", "ceph")
        ceph_cluster_dict[cluster_name] = Ceph(cluster_name, ceph_nodes)

        # Drive IPv6 when requested via --custom-config use_ipv6=true (any infra)
        ceph_cluster_dict[cluster_name].use_ipv6 = use_ipv6

        # Set the network attributes of the cluster
        # ToDo: Support other providers like openstack and IBM-C
        if provider.supports_cluster_networks:
            ceph_cluster_dict[cluster_name].networks = deepcopy(
                cluster.get("ceph-cluster", {}).get("networks", {})
            )

    # TODO: refactor cluster dict to cluster list
    log.info("Done creating osp instances")
    log.info("Waiting for Floating IPs to be available")
    log.info("Sleeping %d Seconds", provider.post_create_wait)
    time.sleep(provider.post_create_wait)

    for cluster_name, cluster in ceph_cluster_dict.items():
        for instance in cluster:
            instance.connect()

    return ceph_cluster_dict, clients
