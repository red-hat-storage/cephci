from json import loads

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from cli.utilities.waiter import WaitUntil
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify key rotate feature
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    node = ceph_cluster.get_nodes(role="installer")[0]

    # Get the key value from ceph auth
    args = {"daemon_type": "mgr", "format": "json"}
    mgr = loads(CephAdm(node).ceph.orch.ps(**args))[0]["daemon_name"]
    auth_data = CephAdm(node).ceph.auth.get(entity=mgr)

    # Perform key rotate
    CephAdm(node).ceph.orch.daemon.rotate_key(mgr)

    # Now verify if the key has been changed
    for w in WaitUntil(300, 15):
        if CephAdm(node).ceph.auth.get(entity=mgr) != auth_data:
            log.info("Key rotate successful. The key for the given daemon has changed")
            break
    if w.expired:
        raise OperationFailedError("The key rotate failed. #Bz: 1783271")

    return 0
