"""Ceph-NVMeoF Common/Generic utility module."""

from functools import wraps

from ceph.ceph import CommandFailed
from ceph.waiter import WaitUntil
from utility.log import Log

LOG = Log(__name__)

# Transient gRPC/gateway connect failure seen while NVMeoF daemons settle.
CONNECT_ALL_ADDRESSES_ERR = "failed to connect to all addresses"
CONNECT_RETRY_TIMEOUT = 120  # seconds
CONNECT_RETRY_INTERVAL = 10  # seconds


def run_with_connect_retry(cmd_fn, description="NVMeoF command"):
    """Run an NVMeoF CLI call, retrying on gateway connect failures.

    Retries for up to ``CONNECT_RETRY_TIMEOUT`` seconds at
    ``CONNECT_RETRY_INTERVAL`` second intervals when the failure message
    contains ``CONNECT_ALL_ADDRESSES_ERR``. Returns as soon as the command
    succeeds. Non-matching failures are raised immediately.

    Args:
        cmd_fn: Zero-arg callable that performs the CLI execution.
        description: Short label used in retry log messages.

    Returns:
        Whatever ``cmd_fn`` returns on success.
    """
    last_exc = None
    attempt = 0
    for _ in WaitUntil(timeout=CONNECT_RETRY_TIMEOUT, interval=CONNECT_RETRY_INTERVAL):
        attempt += 1
        try:
            return cmd_fn()
        except CommandFailed as exc:
            if CONNECT_ALL_ADDRESSES_ERR not in str(exc):
                raise
            last_exc = exc
            LOG.warning(
                "%s hit '%s'; retrying in %ds " "(attempt %d, timeout %ds)",
                description,
                CONNECT_ALL_ADDRESSES_ERR,
                CONNECT_RETRY_INTERVAL,
                attempt,
                CONNECT_RETRY_TIMEOUT,
            )
    if last_exc is not None:
        raise last_exc
    raise CommandFailed(
        f"{description} failed after {CONNECT_RETRY_TIMEOUT}s with "
        f"'{CONNECT_ALL_ADDRESSES_ERR}'"
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
