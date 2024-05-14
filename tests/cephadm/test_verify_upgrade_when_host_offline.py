import json

from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from cli.utilities.utils import bring_node_offline
from utility.log import Log

log = Log(__name__)


def check_upgrade_status(node):
    """Check upgrade status
    Args:
        node: installer node
    """
    timeout, interval = 600, 5
    for w in WaitUntil(timeout=timeout, interval=interval):
        out = CephAdm(node).ceph.orch.upgrade.status()
        if not json.loads(out)["in_progress"]:
            log.info("Upgrade completed")
            return True
        log.info(f"Upgrade still In Progress. Retry for status after '{interval}' sec")
    if w.expired:
        log.error("Cluster upgrade not completed")
        raise False


def run(ceph_cluster, **kw):
    """Verify rebooting a node doesn't affect the ceph health
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    mon_node = ceph_cluster.get_nodes("mon")[1]
    target_image = config.get("container_image")

    if CephAdm(mon_node).ceph.orch.upgrade.check(image=target_image):
        raise OperationFailedError("Upgrade image check failed")

    # Bring the mon_node down
    out = bring_node_offline(mon_node, timeout=120)
    if not out:
        raise OperationFailedError(f"Failed to bring {mon_node.hostname} offline")

    # Start the upgrade
    CephAdm(mon_node).ceph.orch.upgrade.start(image=target_image)

    # Monitor upgrade status, till completion
    if check_upgrade_status(mon_node):
        log.error("Upgrade succeeded even with the host offiline:  #BZ:2253078")
        return 1
    else:
        log.info("Expected: The upgrade failed when node is offline")

    return 0
