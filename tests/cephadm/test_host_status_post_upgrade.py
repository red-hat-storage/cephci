from json import loads

from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError, UnexpectedStateError
from cli.utilities.utils import bring_node_offline
from utility.log import Log

log = Log(__name__)


def check_host_status(node, hostname, status=None):
    """
    Checks the status of host(offline or online) using
    ceph orch host ls and return boolean
    Args:
        node (ceph): Node in which the cmd is executed
        hostname: hostname of host to be checked
        status: custom status check for the host
    """
    conf = {"host_pattern": hostname, "format": "json-pretty"}
    out = loads((CephAdm(node).ceph.orch.host.ls(**conf))[node[0].hostname][0])
    host_status = out[0]["status"].lower().strip()
    if status is None:
        if host_status is None:
            log.info("Status of the host is online")
            return True
    else:
        if status.lower() == host_status:
            log.info(f"Status of the host is {host_status}")
            return True
    return False


def run(ceph_cluster, **kw):
    """Verify upgrade, host should not go offline
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw["config"]
    nodes = [node for node in ceph_cluster.get_nodes() if node.role not in ["client"]]
    installer = ceph_cluster.get_nodes("installer")
    mon_node = ceph_cluster.get_nodes("mon")[1]
    ssh_keepalive_interval = config.get("ssh_keepalive_interval")
    ssh_keepalive_count_max = config.get("ssh_keepalive_count_max")

    # Check the status of all hosts in cluster
    for _node in nodes:
        out = check_host_status(installer, _node.hostname)
        if out:
            raise UnexpectedStateError("All hosts of cluster are not online")

    # Set the value of ssh_keepalive_interval as 30
    CephAdm(installer).ceph.config.set(
        key="mgr/cephadm/ssh_keepalive_interval",
        value=ssh_keepalive_interval,
        daemon="mgr",
    )
    log.info("cephadm ssh_keepalive_interval is set to 30")

    # Set the value of ssh_keepalive_count_max as 7
    CephAdm(installer).ceph.config.set(
        key="mgr/cephadm/ssh_keepalive_count_max",
        value=ssh_keepalive_count_max,
        daemon="mgr",
    )
    log.info("cephadm ssh_keepalive_count_max is set to 7")

    # Bring the mon_node down
    out = bring_node_offline(mon_node, timeout=120)
    if not out:
        raise OperationFailedError(f"Failed to bring {mon_node.hostname} offline")

    # Check the status of the host that is down is shown offline
    timeout, interval = 120, 5
    for w in WaitUntil(timeout=timeout, interval=interval):
        out = check_host_status(installer, mon_node.hostname, "offline")
        if out:
            log.info("Host status is offline as expected")
            break
    if w.expired:
        raise OperationFailedError("Host status is not offline as expected")

    return 0
