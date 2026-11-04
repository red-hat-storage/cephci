import re
from json import loads

from ceph.ceph import CommandFailed
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from cli.utilities.waiter import WaitUntil
from utility.log import Log

log = Log(__name__)

KEY_PATTERN = re.compile(r"^\s*key\s*=\s*(\S+)\s*$", re.MULTILINE)


def _get_key(cephadm, entity):
    """Return only the key value from ceph auth output."""
    auth_data = cephadm.ceph.auth.get(entity=entity)
    match = KEY_PATTERN.search(auth_data)
    if not match:
        raise OperationFailedError(f"Unable to read the key for {entity}")
    return match.group(1)


def _execute(cli, args):
    """Execute a CLI command with exit-code checking enabled."""
    result = cli.execute(sudo=True, cmd=f"{cli.base_cmd} {args}", check_ec=True)
    return result[0].strip() if isinstance(result, tuple) else result


def _rotate_key(cephadm, daemon):
    """Rotate a daemon key, using the Tentacle compatibility workflow."""
    daemon_name = daemon["daemon_name"]
    daemon_cli = cephadm.ceph.orch.daemon
    try:
        result = _execute(daemon_cli, f"rotate-key {daemon_name}")
        if "Scheduled" not in result and "Rotated" not in result:
            raise OperationFailedError(
                f"Unexpected rotate-key response for {daemon_name}: {result!r}"
            )
        return False
    except CommandFailed as error:
        if "Invalid command: rotate-key" not in str(error):
            raise

    if daemon.get("is_active"):
        raise OperationFailedError(
            "The compatibility key-rotation workflow requires a standby mgr"
        )

    log.warning(
        "ceph orch daemon rotate-key is unavailable; rotating %s with "
        "ceph auth rotate followed by daemon redeploy",
        daemon_name,
    )
    _execute(cephadm.ceph.auth, f"rotate {daemon_name}")
    result = _execute(daemon_cli, f"redeploy {daemon_name}")
    if "Scheduled" not in result:
        raise OperationFailedError(
            f"Failed to schedule redeploy for {daemon_name}: {result!r}"
        )
    return True


def run(ceph_cluster, **kw):
    """Verify key rotate feature
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    node = ceph_cluster.get_nodes(role="installer")[0]
    cephadm = CephAdm(node)

    # Get the key value from ceph auth
    args = {"daemon_type": "mgr", "format": "json"}
    mgrs = loads(cephadm.ceph.orch.ps(**args))
    if not mgrs:
        raise OperationFailedError("No mgr daemon found for key rotation")
    mgr = next((daemon for daemon in mgrs if not daemon.get("is_active")), mgrs[0])
    mgr_name = mgr["daemon_name"]
    old_key = _get_key(cephadm, mgr_name)
    old_container_id = mgr.get("container_id")

    # Perform key rotate
    redeployed = _rotate_key(cephadm, mgr)

    # Verify that the key changed and the daemon is running with its new
    # container when the compatibility workflow required a redeploy.
    daemon = {}
    for w in WaitUntil(300, 10):
        cephadm.ceph.orch.ps(refresh=True)
        mgrs = loads(cephadm.ceph.orch.ps(**args))
        daemon = next(
            (item for item in mgrs if item.get("daemon_name") == mgr_name), {}
        )
        key_changed = _get_key(cephadm, mgr_name) != old_key
        container_ready = bool(daemon.get("container_id")) and (
            not redeployed or daemon.get("container_id") != old_container_id
        )
        if key_changed and daemon.get("status_desc") == "running" and container_ready:
            log.info("Key rotate successful. The key for the given daemon has changed")
            return 0
    if w.expired:
        raise OperationFailedError(
            f"Key rotation did not complete for {mgr_name}; daemon state: {daemon}"
        )
