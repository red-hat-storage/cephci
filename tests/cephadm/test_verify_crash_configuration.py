from json import loads

from cli.cephadm.cephadm import CephAdm
from cli.utilities.utils import get_running_containers, start_container, stop_container


class SystemctlFailed(Exception):
    pass


class CrashConfigFailure(Exception):
    pass


def run(ceph_cluster, **kw):
    """Verify redeploy for a specific service
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    mgr_nodes = ceph_cluster.get_nodes(role="mgr")
    node = mgr_nodes[0]
    initial_crash_stat = None
    CephAdm(node).ceph.mgr.module(action="disable", module="balancer")

    running_containers, _ = get_running_containers(
        node, format="json", expr="name=mgr", sudo=True
    )
    container_ids = [item.get("Names")[0] for item in loads(running_containers)]
    stop_container(node, container_ids[0])

    crash_stat = CephAdm(mgr_nodes[1]).ceph.crash.stat()

    start_container(node, container_ids[0])
    if crash_stat == "0 crashes recorded" or initial_crash_stat == crash_stat:
        raise CrashConfigFailure(
            "No new crash stats were recorded even after a crash happened"
        )
    return 0
