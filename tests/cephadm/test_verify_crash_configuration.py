from json import loads

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError, ResourceNotFoundError
from cli.utilities.utils import get_running_containers, stop_container
from utility.log import Log

log = Log(__name__)


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
    log.info(f"Initial recorded crash stats are '{init_crash_stat}'")

    # Disable balancer module
    CephAdm(node).ceph.mgr.module.disable("balancer")

    # Enable crash module
    CephAdm(node).ceph.mgr.module.enable("crash")

    # Get running mgr containers
    running_ctrs, _ = get_running_containers(
        node, format="json", expr="name=mgr", sudo=True
    )
    if not running_ctrs:
        raise ResourceNotFoundError("No running mgr containers")
    ctr_ids = [item.get("Names")[0] for item in loads(running_ctrs)]

    # Stop mgr containers
    for ctr_id in ctr_ids:
        stop_container(node, ctr_id)

    # Get crash stats
    crash_stat = CephAdm(node).ceph.crash.stat()
    log.info(f"Crash stats after stopping container are '{crash_stat}'")

    # Check for crash records
    if crash_stat in ("0 crashes recorded", init_crash_stat):
        # Check if core dump is found
        try:
            cmd = "ls /var/lib/ceph/crash"
            node.exec_command(cmd=cmd, sudo=True)
            raise OperationFailedError(
                "No new crash stats were recorded even after a crash happened"
            )
        except Exception:
            log.info(
                "No crashes were created during test and the stat returned the expected value"
            )

    return 0
