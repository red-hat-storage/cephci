import pickle
import re

from docopt import docopt
from utils.configs import (
    get_configs,
    get_packages,
    get_registry_credentials,
    get_repos,
    get_subscription_credentials,
)
from utils.configure import exec_cephadm_preflight, setup_ssh_keys

from cli.exceptions import ConfigError, NodeConfigError
from cli.utilities.containers import Registry
from cli.utilities.packages import Package, Rpm
from cli.utilities.packages import SubscriptionManager as sm
from cli.utilities.packages import SubscriptionManagerError
from cli.utilities.utils import os_major_version
from cli.utilities.waiter import WaitUntil
from utility.log import Log

log = Log(__name__)

CEPHADM_ANSIBLE = "cephadm-ansible"

doc = """
Utility to configure prerequisites for deployed cluster
    Usage:
        cephci/prereq.py --cluster <FILE>
            (--build <BUILD>)
            (--subscription <SUBSCRIPTION>)
            (--registry <REGISTRY>)
            [--setup-ssh-keys <BOOL>]
            [--config <FILE>]
            [--log-level <LOG>]
            [--cephadm-ansible <BOOL>]
            [--cephadm-preflight <BOOL>]
            [--ceph-repo <REPO>]

        cephci/prereq.py --help

    Options:
        -h --help                   Help
        -c --cluster <FILE>         Cluster config file
        -b --build <BUILD>          Build type [rh|ibm]
        -s --subscription <CRED>    Subscription manager server
        -r --registry <STR>         Container registry server
        -k --setup-ssh-keys <BOOL>  Setup SSH keys on cluster
        -f --config <FILE>          CephCI configuration file
        -l --log-level <LOG>        Log level for log utility
        -a --cephadm-ansible <BOOL> Setup cephadm ansible
        -p --cephadm-preflight <BOOL> Setup cephadm preflight
        -o --ceph-repo <REPO>       Ceph repository
"""


def _set_log(level):
    log.logger.setLevel(level.upper())


def _load_cluster_config(config):
    cluster_conf = None
    with open(config, "rb") as f:
        cluster_conf = pickle.load(f)

    for _, cluster in cluster_conf.items():
        [node.reconnect() for node in cluster]

    return cluster_conf


def setup_subscription_manager(node, server):
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
            log.info(f"Subscribed to '{server}' server successfully")
            return True
        except SubscriptionManagerError:
            log.error(f"Failed to subscribe to '{server}' server. Retrying")

    # Check if node subscribe to subscription manager
    if w.expired:
        log.error(f"Failed to subscribe to '{server}' server.")

    log.info(f"Logined to subscription manager '{server}' successfully")
    return False


def subscription_manager_status(node):
    # Get subscription manager status
    status = sm(node).status()

    # Check for overall status
    expr = ".*Overall Status:(.*).*"
    match = re.search(expr, status)
    if not match:
        msg = "Unexpected subscription manager status"
        log.error(msg)
        raise SubscriptionManagerError(msg)

    return match.group(0)


def setup_local_repos(node, distro):
    # Get repos from cephci config
    repos = get_repos("local", distro)

    # Add local repositories
    for repo in repos:
        Package(node).add_repo(repo=repo)

    log.info("Added local RHEL repos successfully")
    return True


def registry_login(node, server, build):
    # Get registry config from cephci config
    config = get_registry_credentials(server, build)

    # Login to container registry
    Registry(node).login(**config)

    log.info(f"Logined to container registry '{server}' successfully")
    return True


def enable_rhel_repos(node, server, distro):
    # Get RHEL repos from cephci config
    repos = get_repos(server, distro)

    # Enable RHEL repos
    sm(node).repos.enable(repos)

    log.info(f"Enabled repos '{server}' for '{distro}'")
    return True


def prereq(
    cluster,
    build,
    subscription,
    registry,
    ssh=False,
    cephadm_ansible=False,
    cephdam_preflight=False,
    ceph_repo=None,
):
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
                log.error(msg)
                raise NodeConfigError(msg)

            enable_rhel_repos(node, subscription, distro)

        Package(node).install(packages)

        if registry != "skip":
            registry_login(node, registry, build)

    if ssh:
        setup_ssh_keys(installer, nodes)

    if cephadm_ansible and not Rpm(installer).query(CEPHADM_ANSIBLE):
        Package(installer).install(CEPHADM_ANSIBLE, nogpgcheck=True)
    if cephdam_preflight:
        if not (ssh or cephadm_ansible):
            raise ConfigError(
                "SSH keys and cephadm-ansible params are required for executing preflight playbook"
            )
        exec_cephadm_preflight(installer, build, ceph_repo)


if __name__ == "__main__":
    args = docopt(doc)

    cluster = args.get("--cluster")
    build = args.get("--build")
    subscription = args.get("--subscription")
    registry = args.get("--registry")
    setup_ssh = args.get("--setup-ssh-keys")
    config = args.get("--config")
    log_level = args.get("--log-level")
    cephadm_ansible = args.get("--cephadm-ansible")
    cephadm_preflight = args.get("--cephadm-preflight")
    ceph_repo = args.get("--ceph-repo")

    _set_log(log_level)
    get_configs(config)

    cluster_dict = _load_cluster_config(cluster)
    for cluster_name in cluster_dict:
        prereq(
            cluster_dict.get(cluster_name),
            build,
            subscription,
            registry,
            setup_ssh,
            cephadm_ansible,
            cephadm_preflight,
            ceph_repo,
        )
