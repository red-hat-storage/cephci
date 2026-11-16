"""Ceph-NVMeoF Common/Generic utility module."""

from functools import wraps

from ceph.ceph import CommandFailed
from ceph.waiter import WaitUntil
from utility.log import Log

LOG = Log(__name__)

# Transient gRPC/gateway failures seen while NVMeoF daemons settle or redeploy.
# "is going down" is returned by a daemon that accepted the RPC and is exiting.
CONNECT_ALL_ADDRESSES_ERR = "failed to connect to all addresses"
GATEWAY_GOING_DOWN_ERR = "is going down"
TRANSIENT_GATEWAY_ERRORS = (CONNECT_ALL_ADDRESSES_ERR, GATEWAY_GOING_DOWN_ERR)
CONNECT_RETRY_TIMEOUT = 120  # seconds
CONNECT_RETRY_INTERVAL = 10  # seconds


def transient_gateway_error(exc):
    """Return the transient-gateway marker contained in *exc*, or None."""
    text = str(exc)
    for marker in TRANSIENT_GATEWAY_ERRORS:
        if marker in text:
            return marker
    return None


def run_with_connect_retry(
    cmd_fn,
    description="NVMeoF command",
    timeout=CONNECT_RETRY_TIMEOUT,
    interval=CONNECT_RETRY_INTERVAL,
):
    """Run an NVMeoF CLI call, retrying while the gateway is unavailable.

    Retries for up to *timeout* seconds at *interval* second intervals when
    the failure is a transient gateway error (connect refused to every
    address, or the gateway reports it is going down). Returns as soon as
    the command succeeds. Other failures are raised immediately.

    Args:
        cmd_fn: Zero-arg callable that performs the CLI execution.
        description: Short label used in retry log messages.
        timeout: Retry budget in seconds.
        interval: Sleep between attempts in seconds.

    Returns:
        Whatever ``cmd_fn`` returns on success.
    """
    last_exc = None
    attempt = 0
    for _ in WaitUntil(timeout=timeout, interval=interval):
        attempt += 1
        try:
            return cmd_fn()
        except CommandFailed as exc:
            marker = transient_gateway_error(exc)
            if marker is None:
                raise
            last_exc = exc
            LOG.warning(
                "%s hit '%s'; retrying in %ds (attempt %d, timeout %ds)",
                description,
                marker,
                interval,
                attempt,
                timeout,
            )
    if last_exc is not None:
        raise last_exc
    raise CommandFailed(
        f"{description} failed after {timeout}s with a transient gateway error"
    )


def substitute_keys(key_map: dict):
    """Decorator factory that renames keys in kwargs based on mapping.

    This key map is to support the existing test case(s) which works with
     existing Container CLI args.

    Args:
        key_map (dict): Key dict mapping
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if "args" in kwargs and isinstance(kwargs["args"], dict):
                kwargs["args"] = {
                    (key_map.get(k, k)): v for k, v in kwargs["args"].items()
                }
            return func(*args, **kwargs)

        return wrapper

    return decorator
