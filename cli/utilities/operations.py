import json
from logging import getLogger

from ceph.waiter import WaitUntil
from cli.ceph.ceph import Ceph
from cli.exceptions import UnexpectedStateError

log = getLogger(__name__)


def wait_for_cluster_health(node, status, timeout=300, interval=20):
    """Checks if cluster health is in expected status

    Args:
        timeout (int): Timeout duration in seconds
        interval (int): Interval duration in seconds
        node (ceph): Node to execute the cmd
        status (str): Expected status, e.g. HEALTH_OK/HEALTH_WARN

    Returns (bool): Based on expected v/s actual status
    """
    for w in WaitUntil(timeout=timeout, interval=interval):
        _status = Ceph(node).health()
        if status in _status:
            log.info(f"Cluster status is in expected state {status}")
            return True
    if w.expired:
        log.error(f"Cluster is not in {status} state even after {timeout} secs")
        return False


def wait_for_osd_daemon_state(client, id, state):
    """Wait for OSD daemon state

    Args:
        client (CephNode): Ceph node object
        id (str): OSD daemon id
        state (str): Expected daemon state
    """
    timeout, interval = 300, 20
    for w in WaitUntil(timeout=timeout, interval=interval):
        # Get osd tree and check whether the deleted osd is present or not
        tree = Ceph(client).osd.tree(states=state, format="json")
        for daemon in json.loads(tree).get("nodes"):
            _id = daemon.get("id")
            _state = daemon.get("status")
            if str(_id) == str(id) and _state == state:
                log.info(f"OSD 'osd.{_id}' is in '{_state}' state")
                return True

        log.info(f"OSD 'osd.{id}' is not in '{state}' state. Retrying")

    if w.expired:
        raise UnexpectedStateError(
            f"Failed to get OSD 'osd.{id}' in '{state}' state within {timeout} sec"
        )
