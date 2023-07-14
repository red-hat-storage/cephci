from cephci.prereq import enable_rhel_repos, setup_subscription_manager
from cephci.utils.configs import get_repos
from cli.utilities.packages import Package, SubscriptionManager
from cli.utilities.utils import os_major_version
from utility.log import Log

log = Log(__name__)


class PackageValidationError(Exception):
    pass


def disable_rhel_repo(node, server, distro):
    """Disable rhel repos
    Args:
        node (str): Node where repos needs to be disabled
        server (str): Server name
        distro (distro): Distro name
    """
    # Get RHEL repos from cephci config
    repos = get_repos(server, distro)

    # Disable RHEL repos
    SubscriptionManager(node).repos.disable(repos)

    log.info(f"Disable repos '{server}' for '{distro}'")
    return True


def run(ceph_cluster, **kw):
    """Verify re-deploying of monitoring stack with custom images
    Args:
        ceph_cluster(ceph.ceph.Ceph): CephNode or list of CephNode object
    """

    # Get installer node
    node = ceph_cluster.get_nodes(role="installer")[0]

    # Setup subscription manager
    setup_subscription_manager(node, "cdn")

    # Enable repos
    distro = f"rhel-{os_major_version(node)}"
    enable_rhel_repos(node, "local", distro)

    # Install cephadm-ansible package
    out = Package(node).install("cephadm-ansible")
    if "DEPRECATION WARNING" in out:
        raise PackageValidationError(
            "cephadm-ansible throwing expected deprecation warning"
        )

    # Remove cephadm-ansible package from node
    Package(node).remove("cephadm-ansible")

    # Disable rhel repos
    disable_rhel_repo(node, "local", distro)

    return 0
