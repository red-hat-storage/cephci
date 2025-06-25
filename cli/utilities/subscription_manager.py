import re

from cli.exceptions import OperationFailedError, ResourceNotFoundError
from cli.utilities.packages import SubscriptionManager
from cli.utilities.waiter import WaitUntil
from utility.log import Log
from utility.utils import get_cephci_config

log = Log(__name__)


def setup_subscription_manager(node, server, timeout=300, interval=60):
    """Setup the subscription manager on a Ceph node.

    Args:
        ceph: Ceph node object.
        server: Name of the subscription server (e.g., "rhsm", "satellite").
        timeout: Maximum time to wait for subscription (default: 300 seconds).
        interval: Time interval between retries (default: 60 seconds).

    Returns:
        bool: True if subscription is successful, False otherwise.
    """
    # Get configuration details from `~/.cephci.yaml`
    configs = get_cephci_config()

    # Get credentials and validate
    creds = configs.get(f"{server}_credentials")
    if not creds:
        raise ResourceNotFoundError(f"{server} credentials are not provided.")

    # Subscribe to server
    for w in WaitUntil(timeout=timeout, interval=interval):
        try:
            SubscriptionManager(node).register(
                username=creds.get("username"),
                password=creds.get("password"),
                serverurl=creds.get("serverurl"),
                baseurl=creds.get("baseurl"),
                force=True,
            )
            log.info(f"Subscribed to {server} server successfully.")
            return True
        except OperationFailedError:
            log.info(f"Failed to subscribe to {server} server. Retrying ...")

    if w.expired:
        log.info(f"Failed to subscribe to {server} server.")

    return False


def re_register_subscription_manager(node, server):
    """Setup the subscription manager on a Ceph node.

    Args:
        node: Ceph node object.
        server: Name of the subscription server.

    Returns:
        bool: True if subscription is successful, False otherwise.

    Raises:
        ConfigError: if credentials for the given server are not found.
    """
    # Create subscription manager instance
    sm = SubscriptionManager(node)

    # Unregister the subscription manager
    try:
        sm.unregister()
    except OperationFailedError as e:
        log.info(f"Failed to unregister subscription manager:\n{str(e)}")

    # Clean all subscription-related configs
    try:
        sm.clean()
    except OperationFailedError as e:
        log.info(f"Failed to clean subscription manager:\n{str(e)}")

    # Clean up any residual subscription files
    try:
        cleanup_subscription_files(node)
    except OperationFailedError as e:
        log.info(f"Failed to clean up residual subscription files:\n{str(e)}")

    # Re-register the subscription manager
    if not setup_subscription_manager(node, "cdn"):
        log.info("Trying to subscribe to stage server")
        return setup_subscription_manager(node, "stage")

    return True


def subscription_manager_status(node):
    """Returns the current status of subscription-manager on the Ceph node.

    Args:
        node: Ceph node object

    Returns:
        str: Status string (e.g., "Subscribed", "Unknown", etc.)

    Raises:
        OperationFailedError: if parsing the status fails.
    """
    # Regular expression to match the overall status in subscription-manager output
    expr = ".*Overall Status:(.*).*"

    # Get the status of subscription manager
    status = SubscriptionManager(node).status()

    # Check if the status is empty
    match = re.search(expr, status)
    if not match:
        raise OperationFailedError("Unexpected subscription manager status")

    # Return the matched status
    return match.group(0)


def cleanup_subscription_files(node):
    """Remove residual subscription-related certificate and identity files.

    Args:
        node: Ceph node object.

    Raises:
        OperationFailedError: if the file cleanup command fails.
    """
    # List of subscription manager certificate directories to clean up
    log.info("Cleaning up subscription manager files...")
    subscription_manager_certs = [
        "/etc/pki/consumer",
        "/etc/pki/product",
        "/etc/pki/entitlement",
    ]

    # Remove subscription manager certificate files
    for cert in subscription_manager_certs:
        log.info(f"Removing subscription manager certificate: {cert}")
        node.remove_file(cert)
