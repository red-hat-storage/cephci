import re

from ceph.waiter import WaitUntil
from cli.utilities.packages import (
    Package,
    SubscriptionManager,
    SubscriptionManagerError,
)
from cli.utilities.utils import os_major_version
from utility.log import Log
from utility.utils import get_cephci_config

log = Log(__name__)


class ConfigNotFoundError(Exception):
    pass


def setup_subscription_manager(ceph, server, timeout=300, interval=60):
    configs = get_cephci_config()
    creds = configs.get(f"{server}_credentials")
    if not creds:
        raise ConfigNotFoundError(f"{server} credentials are not provided")

    sm = SubscriptionManager(ceph)

    # Clean all local subscription data: entitlements, certs, identity
    sm.clean()

    for w in WaitUntil(timeout=timeout, interval=interval):
        try:
            sm.register(
                username=creds.get("username"),
                password=creds.get("password"),
                serverurl=creds.get("serverurl"),
                baseurl=creds.get("baseurl"),
                force=True,
            )
            log.info(f"Subscribed to {server} server successfully")
            return True
        except SubscriptionManagerError as e:
            log.info(
                f"Failed to subscribe to {server} server. Retrying, Reason: {str(e)}"
            )

    if w.expired:
        log.info(f"Failed to subscribe to {server} server.")

    return False


def subscription_manager_status(ceph):
    expr = ".*Overall Status:(.*).*"
    status = SubscriptionManager(ceph).status()
    match = re.search(expr, status)
    if not match:
        raise SubscriptionManagerError("Unexpected subscription manager status")
    return match.group(0)


def setup_local_repos(ceph):
    configs = get_cephci_config()
    os_version = os_major_version(ceph)
    repos = configs.get("repo")
    if not repos:
        raise ConfigNotFoundError("Repos are not provided")

    local_repos = repos.get("local", {}).get(f"rhel-{os_version}")
    if not local_repos:
        raise ConfigNotFoundError("local repositories are not provided")

    for repo in local_repos:
        Package(ceph).add_repo(repo=repo)

    log.info("Added local RHEL repos successfully")
    return True


def enable_rhel_rpms(ceph, distro_ver):
    repos = {
        "7": ["rhel-7-server-rpms", "rhel-7-server-extras-rpms"],
        "8": ["rhel-8-for-x86_64-appstream-rpms", "rhel-8-for-x86_64-baseos-rpms"],
        "9": ["rhel-9-for-x86_64-appstream-rpms", "rhel-9-for-x86_64-baseos-rpms"],
    }

    ceph.exec_command(sudo=True, cmd=f"subscription-manager release --set {distro_ver}")
    for repo in repos.get(distro_ver[0]):
        ceph.exec_command(
            sudo=True,
            cmd=f"subscription-manager repos --enable={repo}",
            long_running=True,
        )


def enable_rhel_eus_rpms(ceph, distro_ver):
    eus_repos = {"7": ["rhel-7-server-eus-rpms", "rhel-7-server-extras-rpms"]}
    for repo in eus_repos.get(distro_ver[0]):
        ceph.exec_command(
            sudo=True,
            cmd=f"subscription-manager repos --enable={repo}",
            long_running=True,
        )

    if distro_ver[0] == "7":
        release = "7.7"
    else:
        raise NotImplementedError(f"Cannot set EUS repos for {distro_ver[0]}")

    ceph.exec_command(
        sudo=True,
        cmd=f"subscription-manager release --set={release}",
        long_running=True,
    )
    ceph.exec_command(sudo=True, cmd="yum clean all", long_running=True)
