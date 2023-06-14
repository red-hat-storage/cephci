from ceph.waiter import WaitUntil
from cli.ceph.ceph import Ceph
from utility.log import Log

log = Log(__name__)


def wait_for_cluster_health(node, status, timeout=300, interval=20):
    """
    Checks if cluster health is in expected status
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
