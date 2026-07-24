from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from cli.utilities.utils import bring_node_offline, is_node_online


class ClusterHealthError(Exception):
    pass


class UnexpectedNodeStatusError(Exception):
    pass


def run(ceph_cluster, **kw):
    """Verify cluster health detail result when a node is down
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    installer_node = ceph_cluster.get_nodes(role="installer")[0]
    mon_node = ceph_cluster.get_nodes(role="mon")[1]

    # Get Ceph Health Detail
    ceph_health = CephAdm(installer_node).ceph.health()
    if not ceph_health == "HEALTH_OK":
        raise ClusterHealthError("Ceph cluster not healthy")

    # Bring the mon_node down
    out = bring_node_offline(mon_node, timeout=120)
    if not out:
        raise UnexpectedNodeStatusError(f"Failed to bring {mon_node.hostname} offline")

    # Once the mon node is offline, check for the cluster health detail
    # Validate whether the ceph health detail returns unnecessary info
    # as mentioned in https://tracker.ceph.com/issues/54132
    unexpected_entries = [
        "Received MSG_CHANNEL_OPEN_CONFIRMATION",
        "Initial send window",
        "Sent MSG_IGNORE",
        "Sent MSG_CHANNEL_CLOSE",
        "Received channel close",
        "Received MSG_CHANNEL_REQUEST",
        "Received MSG_CHANNEL_EOF",
    ]

    # Check whether the health detail doesn't dump unexpected data
    # Repeat the checks multiple times and validate data
    timeout, interval = 120, 10
    for _ in WaitUntil(timeout=timeout, interval=interval):
        ceph_health_detail = CephAdm(installer_node).ceph.health(detail=True)
        for entry in unexpected_entries:
            if entry in ceph_health_detail:
                raise ClusterHealthError(
                    f"{entry} present in result: {ceph_health_detail}"
                )
    # Wait for the thread to complete and bring the n/w interface up
    out.join()

    for w in WaitUntil(timeout=timeout, interval=interval):
        # Check whether the node is up
        if is_node_online(mon_node):
            break
    if w.expired:
        raise UnexpectedNodeStatusError(
            f"{mon_node.hostname} is not up after the n/w interface is up"
        )
    return 0
