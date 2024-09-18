from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from cli.utilities.utils import get_service_id


def _is_cluster_healthy(node):
    # Verify if the cluster is in healthy state
    timeout, interval = 300, 10
    for w in WaitUntil(timeout=timeout, interval=interval):
        if CephAdm(node).ceph.health() == "HEALTH_OK":
            return True
    if w.expired:
        return False


def run(ceph_cluster, **kw):
    """Verify Ceph Mgr crash issue post mgr fail
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    installer = ceph_cluster.get_nodes(role="installer")[0]
    node = ceph_cluster.get_nodes(role="mgr")[0]
    osd_node = ceph_cluster.get_nodes(role="osd")[1]
    mgr_service = get_service_id(node, "mgr")[0]

    # Drain a host
    CephAdm(installer).ceph.orch.host.drain(
        osd_node.hostname, force=True, zap_osd_devices=True
    )

    # Perform mgr fail operation
    if CephAdm(installer).ceph.mgr.fail(mgr=mgr_service):
        raise OperationFailedError("Failed to execute `ceph mgr fail` command")

    # Perform the reproducer. The osd rm status should not return "Error ENOENT: Module not found"
    out = CephAdm(installer).ceph.orch.osd.rm(status=True)
    if "Error ENOENT: Module not found" in out:
        raise OperationFailedError(
            "Unexpected! Error ENOENT: Module not found error seen while checking rm status post mgr fail. #2305677"
        )

    return 0
