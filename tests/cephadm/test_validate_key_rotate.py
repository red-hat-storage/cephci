import re
from json import loads

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from cli.utilities.waiter import WaitUntil
from utility.log import Log

log = Log(__name__)

UNSUPPORTED_RELEASE_MAJORS = {"8", "9", "19", "20"}
KEY_PATTERN = re.compile(r"^\s*key\s*=\s*(\S+)\s*$", re.MULTILINE)


def _release_major(ceph_cluster, config):
    """Return the RHCS or Ceph major version supplied by the runner."""
    release = config.get("rhbuild") or getattr(ceph_cluster, "rhcs_version", "")
    version = getattr(release, "version", release)
    if isinstance(version, (list, tuple)) and version:
        return str(version[0])

    match = re.match(r"\d+", str(version))
    return match.group(0) if match else ""


def _get_key(cephadm, entity):
    """Read only the key value so unrelated auth metadata cannot pass the test."""
    auth_data = cephadm.ceph.auth.get(entity=entity)
    match = KEY_PATTERN.search(auth_data)
    if not match:
        raise OperationFailedError(f"Unable to read the CephX key for {entity}")
    return match.group(1)


def run(ceph_cluster, **kw):
    """Verify key rotate feature
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config") or {}
    release_major = _release_major(ceph_cluster, config)
    if release_major in UNSUPPORTED_RELEASE_MAJORS:
        log.info(
            "Skipping daemon key rotation: it is disabled in RHCS 8/9 "
            "(Ceph Squid/Tentacle)"
        )
        return -1

    node = ceph_cluster.get_nodes(role="installer")[0]
    cephadm = CephAdm(node)

    # Get the key value from ceph auth
    args = {"daemon_type": "mgr", "format": "json"}
    mgr = loads(cephadm.ceph.orch.ps(**args))[0]["daemon_name"]
    old_key = _get_key(cephadm, mgr)

    # Perform key rotate
    cephadm.ceph.orch.daemon.rotate_key(mgr)

    # Now verify if the key has been changed
    for w in WaitUntil(300, 15):
        if _get_key(cephadm, mgr) != old_key:
            log.info("Key rotate successful. The key for the given daemon has changed")
            break
    if w.expired:
        raise OperationFailedError("The key rotate failed. #Bz: 1783271")

    return 0
