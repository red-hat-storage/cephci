from cli.exceptions import NotSupportedError, OperationFailedError
from cli.utilities.packages import Package, SubscriptionManager
from cli.utilities.subscription_manager import re_register_subscription_manager
from utility.log import Log

log = Log(__name__)


def enable_rhel_rpms(node, distro_ver, server="rhsm"):
    """Enable required RHEL repositories based on the distro version.

    Args:
        node: The node or context to run commands on.
        distro_ver (str): The major RHEL version (e.g., '7', '8', '9')
    """
    # Define the repositories for each RHEL version
    repos = {
        "7": ["rhel-7-server-rpms", "rhel-7-server-extras-rpms"],
        "8": ["rhel-8-for-x86_64-appstream-rpms", "rhel-8-for-x86_64-baseos-rpms"],
        "9": ["rhel-9-for-x86_64-appstream-rpms", "rhel-9-for-x86_64-baseos-rpms"],
    }

    # Set the subscription-manager release version
    try:
        SubscriptionManager(node).set(distro_ver)
    except OperationFailedError as e:
        # Reregister the subscription manager
        log.info(
            f"Failed to set subscription-manager release for {distro_ver}: "
            f"{e},\nre-registering ..."
        )
        re_register_subscription_manager(node, server)

        # Reattempt to set the subscription-manager release version
        log.info(f"Retrying to set subscription-manager release for {distro_ver}")
        SubscriptionManager(node).set(distro_ver)

    # Enable the repositories based on the distro version
    for repo in repos.get(distro_ver[0]):
        SubscriptionManager(node).repos.enable(repos=repo)


def enable_rhel_eus_rpms(node, distro_ver):
    """Enable Extended Update Support (EUS) repositories for RHEL 7.

    Args:
        node: The node or object to run commands on.
        distro_ver (str): The major RHEL version (e.g., '7').

    Raises:
        NotSupportedError: If the version is not supported for EUS.
    """
    # Define the EUS repositories for RHEL 7
    repos = {"7": ["rhel-7-server-eus-rpms", "rhel-7-server-extras-rpms"]}

    # Enable the EUS repositories based on the distro version
    for repo in repos.get(distro_ver[0]):
        SubscriptionManager(node).repos.enable(repos=repo)

    # Check if the distro version is supported for EUS
    if distro_ver[0] != "7":
        raise NotSupportedError(f"Cannot set EUS repos for {distro_ver[0]}")

    # Set the release version for EUS repositories
    release = "7.7"

    # Set the subscription-manager release version for EUS
    SubscriptionManager(node).set(release)

    # Clean up the yum cache to ensure the new repositories are recognized
    Package(node).clean()
