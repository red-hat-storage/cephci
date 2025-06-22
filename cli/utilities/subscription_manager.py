import re

from cli.exceptions import ConfigError
from cli.utilities.packages import SubscriptionManager, SubscriptionManagerError
from utility.log import Log
from utility.utils import get_cephci_config

log = Log(__name__)


def setup_subscription_manager(ceph, server):
    """
    Set up the subscription manager on a Ceph node with provided credentials.

    This includes:
    - Unregistering the system if already registered
    - Cleaning all local subscription data using `subscription-manager clean`
    - Removing any residual certificate or identity files
    - Registering to the specified subscription server

    Args:
        ceph: Ceph node object where subscription-manager is to be configured.
        server: Name of the subscription server, used to fetch credentials from config.

    Returns:
        bool: True if subscription is successful, False otherwise.

    Raises:
        ConfigError: if credentials for the given server are not found.
    """
    configs = get_cephci_config()
    creds = configs.get(f"{server}_credentials")
    if not creds:
        raise ConfigError(f"{server} credentials are not provided")

    sm = SubscriptionManager(ceph)

    # Clean all local subscription data: entitlements, certs, identity
    try:
        sm.unregister()
    except SubscriptionManagerError as e:
        log.warning(f"Unregister failed (possibly already unregistered): {e}")

    try:
        sm.clean()
    except SubscriptionManagerError as e:
        log.warning(f"Subscription clean failed; stale entitlements may remain: {e}")

    try:
        cleanup_subscription_files(ceph)
    except SubscriptionManagerError as e:
        log.warning(f"Residual subscription file cleanup failed (certs/identity): {e}")

    # Registration
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
        log.error(f"Failed to subscribe to {server} server: {e}")
        return False


def subscription_manager_status(ceph):
    """
    Returns the current status of subscription-manager on the Ceph node.

    Args:
        ceph: Ceph node object

    Returns:
        str: Status string (e.g., "Subscribed", "Unknown", etc.)

    Raises:
        SubscriptionManagerError: if parsing the status fails.
    """
    expr = ".*Overall Status:(.*).*"
    status = SubscriptionManager(ceph).status()
    match = re.search(expr, status)
    if not match:
        raise SubscriptionManagerError("Unexpected subscription manager status")
    return match.group(0)


def cleanup_subscription_files(ceph):
    """
    Remove residual subscription-related certificate and identity files.

    Args:
        ceph: Ceph node object.

    Raises:
        SubscriptionManagerError: if the file cleanup command fails.
    """
    cmd = "rm -rf /etc/pki/consumer /etc/pki/product"
    ceph.exec_command(sudo=True, long_running=True, cmd=cmd)
