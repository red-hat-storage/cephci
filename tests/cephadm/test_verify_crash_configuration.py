from json import loads

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError, ResourceNotFoundError
from cli.utilities.utils import get_running_containers, start_container, stop_container


def run(ceph_cluster, **kw):
    """Verify redeploy for a specific service
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    # Check for mgr nodes
    mgr_nodes = ceph_cluster.get_nodes(role="mgr")
    if not mgr_nodes:
        raise ResourceNotFoundError("No mgr node available")
    node = mgr_nodes[0]

    # Get initial crash stats
    init_crash_stat = CephAdm(node).ceph.crash.stat()

    # Disable balancer module
    CephAdm(node).ceph.mgr.module(action="disable", module="balancer")

    # Get running mgr containers
    running_ctrs, _ = get_running_containers(
        node, format="json", expr="name=mgr", sudo=True
    )
    if not running_ctrs:
        raise ResourceNotFoundError("No running mgr containers")
    ctr_id = [item.get("Names")[0] for item in loads(running_ctrs)][0]

    # Stop mgr containers
    stop_container(node, ctr_id)

    # Get crash stats
    crash_stat = CephAdm(node).ceph.crash.stat()

    # Check for crash records
    if crash_stat == "0 crashes recorded" or init_crash_stat == crash_stat:
        raise OperationFailedError(
            "No new crash stats were recorded even after a crash happened"
        )

    return 0
