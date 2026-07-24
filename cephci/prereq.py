import pickle
import re

from docopt import docopt

from cephci.utils.configs import (
    get_configs,
    get_packages,
    get_registry_credentials,
    get_repos,
    get_subscription_credentials,
)
from cephci.utils.configure import (
    add_cert_to_trusted_list,
    add_images_to_private_registry,
    copy_cert_to_secondary_node,
    create_link_to_domain_cert,
    create_registry_directories,
    create_self_signed_certificate,
    exec_cephadm_preflight,
    get_private_registry_image,
    set_registry_credentials,
    setup_ssh_keys,
    start_local_private_registry,
)
from cephci.utils.utility import set_logging_env
from cli.exceptions import NodeConfigError
from cli.utilities.containers import Registry
from cli.utilities.packages import Package, Rpm
from cli.utilities.packages import SubscriptionManager as sm
from cli.utilities.packages import SubscriptionManagerError
from cli.utilities.utils import os_major_version
from cli.utilities.waiter import WaitUntil
from utility.log import Log

LOG = Log(__name__)

CEPHADM_ANSIBLE = "cephadm-ansible"

doc = """
Utility to configure prerequisites for deployed cluster
    Usage:
        cephci/prereq.py --cluster <FILE>
            (--build <BUILD>)
            (--subscription <SUBSCRIPTION>)
            (--registry <REGISTRY>)
            [--setup-ssh-keys <BOOL>]
            [--cephadm-ansible <BOOL>]
            [--cephadm-preflight <BOOL>]
            [--private-registry <BOOL>]
            [--build-type <BUILD>]
            [--ceph-repo <REPO>]
            [--config <FILE>]
            [--log-level <LOG>]
            [--log-dir <PATH>]

        cephci/prereq.py --help

    Options:
        -h --help                    Help
        --cluster <FILE>             Cluster config file
        --build <BUILD>              Build version
        --subscription <CRED>        Subscription manager server
        --registry <STR>             Container registry server
        --setup-ssh-keys <BOOL>      Setup SSH keys on cluster
        --private-registry <BOOL>    Use private registry
        --cephadm-ansible <BOOL>     Setup cephadm ansible
        --cephadm-preflight <BOOL>   Run cephadm preflight
        --build-type <BUILD>         Build type [rh|ibm]
        --ceph-repo <REPO>           Ceph repo
        --config <FILE>              Ceph CI configuration file
        --log-level <LOG>            Log level for log utility Default: DEBUG
        --log-dir <PATH>             Log directory for logs
"""


def _load_cluster_config(config):
    """Load cluster configration from Ceph CI object"""
    cluster = None
    with open(config, "rb") as f:
        cluster = pickle.load(f)

    [n.reconnect() for _, c in cluster.items() for n in c]

    return cluster


def setup_subscription_manager(node, server):
    """Setup subscription manager on node"""
    # Get configuration details from cephci configs
    configs = get_subscription_credentials(server)
    configs["force"] = True

    # Get timeout and interval
    timeout = configs.get("timeout")
    retry = configs.get("retry")
    interval = int(timeout / retry)

    # Remove timeout and try configs
    configs.pop("timeout")
    configs.pop("retry")

    # Subscribe to server
    for w in WaitUntil(timeout=timeout, interval=interval):
        try:
            sm(node).register(**configs)
            LOG.info(f"Subscribed to '{server}' server successfully")
            return True
        except SubscriptionManagerError:
            LOG.error(f"Failed to subscribe to '{server}' server. Retrying")

    # Check if node subscribe to subscription manager
    if w.expired:
        LOG.error(f"Failed to subscribe to '{server}' server.")

    LOG.info(f"Logined to subscription manager '{server}' successfully")
    return False


def subscription_manager_status(node):
    """Get subscription manager status"""
    # Get subscription manager status
    status = sm(node).status()

    # Check for overall status
    expr = ".*Overall Status:(.*).*"
    match = re.search(expr, status)
    if not match:
        msg = "Unexpected subscription manager status"
        LOG.error(msg)
        raise SubscriptionManagerError(msg)

    return match.group(0)


def setup_local_repos(node, distro):
    """Setup local repositories on nodes"""
    # Get repos from cephci config
    repos = get_repos("local", distro)

    # Add local repositories
    for repo in repos:
        Package(node).add_repo(repo=repo)

    LOG.info("Added local RHEL repos successfully")
    return True


def registry_login(node, server, build):
    """Login to container registry"""
    # Get registry config from cephci config
    config = get_registry_credentials(server, build)

    # Login to container registry
    Registry(node).login(**config)

    LOG.info(f"Logined to container registry '{server}' successfully")
    return True


def enable_rhel_repos(node, server, distro):
    """Enable rhel repositories"""
    # Get RHEL repos from cephci config
    repos = get_repos(server, distro)

    # Enable RHEL repos
    sm(node).repos.enable(repos)

    LOG.info(f"Enabled repos '{server}' for '{distro}'")
    return True


def setup_private_container_registry(
    installer,
    nodes,
    registry,
    reg_username,
    reg_password,
    build_type,
    private_reg_username,
    private_reg_password,
    docker_reg_image,
    images=None,
):
    """
    Performs the pre-reqs required for the disconnected install

    Args:
        installer (ceph.ceph.Ceph): Installer node
        nodes (ceph.ceph.Ceph): List of nodes
        registry (str): registery name
        reg_username (str): registry username
        reg_password (str): registry password
        build_type (str): build type
        private_reg_username (str): private registry username
        private_reg_password (str): private registry password
        docker_reg_image (str): docker-registry image name
        images (list): list of images
    """

    # Step 1: Create folders for the private registry
    if not create_registry_directories(installer):
        return False

    # Step 2: Create credentials for accessing the private registry
    if not set_registry_credentials(
        installer, private_reg_username, private_reg_password
    ):
        return False

    # Step 3: Create a self-signed certificate
    if not create_self_signed_certificate(installer):
        return False

    # Step 4: Create a symbolic link to domain.cert to allow skopeo to locate the
    # certificate with the file extension .cert
    if not create_link_to_domain_cert(installer):
        return False

    # Step 5: Add the certificate to the trusted list on the private registry node
    if not add_cert_to_trusted_list(installer):
        return False

    # Step 6: Copy the certificate to any nodes that will access the private registry for installation and update the
    # trusted list
    if not copy_cert_to_secondary_node(installer, nodes):
        return False

    # Step 7: Login to the registry
    Registry(installer).login(
        registry=registry, username=reg_username, password=reg_password
    )

    # Step 8: Start the local secure private registry
    if not start_local_private_registry(installer, docker_reg_image):
        return False

    # Step 9: Add images to the private registry
    if not add_images_to_private_registry(
        installer,
        reg_username,
        reg_password,
        private_reg_username,
        private_reg_password,
        registry,
        build_type,
        images,
    ):
        return False

    # Step 10: List down private registry images
    if not get_private_registry_image(
        installer, private_reg_username, private_reg_password
    ):
        return False

    LOG.info("Private registry setup successfully for disconnected install")
    return True


def prereq(
    cluster,
    build,
    subscription,
    registry,
    build_type,
    cephadm_ansible=False,
    cephdam_preflight=False,
    ceph_repo=None,
    ssh=False,
    create_private_registry=False,
):
    """Configure cluster to install steps"""
    nodes = cluster.get_nodes()
    installer = cluster.get_ceph_object("installer")
    packages = " ".join(get_packages())

    for node in nodes:
        distro = f"rhel-{os_major_version(node)}"
        if subscription == "skip":
            enable_rhel_repos(node, "local", distro)

        elif subscription in ["cdn", "stage"]:
            setup_subscription_manager(node, subscription)

            status = subscription_manager_status(node)
            if status == "Unknown":
                msg = f"Subscription manager is in '{status}' status"
                LOG.error(msg)
                raise NodeConfigError(msg)

            enable_rhel_repos(node, subscription, distro)

        Package(node).install(packages)

        if registry != "skip":
            registry_login(node, registry, build)

    if ssh or cephdam_preflight:
        # cephadm preflight is required to have ssh keys setup
        setup_ssh_keys(installer, nodes)

    if (cephadm_ansible or cephdam_preflight) and not Rpm(installer).query(
        CEPHADM_ANSIBLE
    ):
        # cephadm preflight is required to have cephadm-ansible installed
        Package(installer).install(CEPHADM_ANSIBLE, nogpgcheck=True)

    if cephdam_preflight:
        exec_cephadm_preflight(installer, build, ceph_repo)

    if create_private_registry:
        setup_private_container_registry(installer, nodes, registry, build_type)
        registry_login(installer, registry, build)


if __name__ == "__main__":
    # Set user parameters
    args = docopt(doc)

    # Get user parameters
    cluster = args.get("--cluster")
    build = args.get("--build")
    subscription = args.get("--subscription")
    registry = args.get("--registry")
    setup_ssh = args.get("--setup-ssh-keys")
    cephadm_ansible = args.get("--cephadm-ansible")
    cephadm_preflight = args.get("--cephadm-preflight")
    create_private_registry = args.get("--private-registry")
    build_type = args.get("--build-type")
    ceph_repo = args.get("--ceph-repo")
    config = args.get("--config")
    log_level = args.get("--log-level")
    log_dir = args.get("--log-dir")

    # Set log level
    LOG = set_logging_env(level=log_level, path=log_dir)

    # Read configuration for reporting service
    get_configs(config)

    # Configure cluster
    cluster_dict = _load_cluster_config(cluster)
    for cluster_name in cluster_dict:
        prereq(
            cluster_dict.get(cluster_name),
            build,
            subscription,
            registry,
            build_type,
            cephadm_ansible,
            cephadm_preflight,
            ceph_repo,
            setup_ssh,
            create_private_registry,
        )
