from cli.exceptions import ResourceNotFoundError
from cli.utilities.packages import Package, YumConfigManager
from cli.utilities.utils import os_major_version
from utility.log import Log
from utility.utils import get_cephci_config

log = Log(__name__)


def setup_local_repos(node):
    """Setup local repositories for the given Ceph cluster.

    Args:
        ceph (CephNode): The Ceph cluster object containing configuration details.
    """
    # Get configuration details from `~/.cephci.yaml`
    configs = get_cephci_config()

    # Get distro version
    os_version = os_major_version(node)

    # Get local repositories
    repos = configs.get("repo")
    if not repos:
        raise ResourceNotFoundError("Repos are not provided")

    # Get local repositories
    local_repos = repos.get("local", {}).get(f"rhel-{os_version}")
    if not local_repos:
        raise ResourceNotFoundError("Local repositories are not provided")

    # Add local repositories
    yum_add_repo(node, local_repos, gpgcheck=False)

    log.info("Added local RHEL repos successfully")
    return True


def yum_add_repo(node, repo, gpgcheck=True):
    """Add local repositories to the node using yum.

    Args:
        node (CephNode): The node to add the repositories to.
        repo (str | list): The list of repositories to add.
        gpgcheck (bool): Whether to enable GPG check for the repositories.
    """
    # Check if the repo is a list
    repo = repo if isinstance(repo, list) else [repo]

    # Add local repositories
    for r in repo:
        YumConfigManager(node).add_repo(repo=r)

    # Set GPG check for the repositories
    if not gpgcheck:
        log.info("Setting GPG check to False for the repositories")
        set_gpgcheck(node, repo, gpgcheck=gpgcheck)

    log.info("Added local RHEL repos successfully")
    return True


def set_gpgcheck(node, repo, gpgcheck=False):
    """Set GPG check for the given repositories.

    Args:
        node (CephNode): The node to set the GPG check on.
        repo (str | list): The list of repositories to set the GPG check for.
        gpgcheck (bool): Whether to enable GPG check for the repositories.
    """
    # Check if the repo is a list
    repo = repo if isinstance(repo, list) else [repo]

    # Get repo ids
    repo_ids = get_repo_id(node, repo)

    # Set GPG check for the repository
    for repo_id in repo_ids:
        YumConfigManager(node).setopt(repo_id, f"gpgcheck={1 if gpgcheck else 0}")


def get_repo_id(node, repo):
    """Get the repository ID for the given repository.

    Args:
        node (CephNode): The node to get the repository ID from.
        repo (str | list): The repository to get the ID for.
    """
    # Check if the repo is a list
    repo = repo if isinstance(repo, list) else [repo]

    # Get the repository ID
    repo_details = Package(node).repolist(enabled=True)

    # Get repository IDs
    repo_ids = []
    for line in repo_details.splitlines():
        for r in repo:
            if r in line:
                repo_ids.append(line.split()[0])

    return repo_ids
