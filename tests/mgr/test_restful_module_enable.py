from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError


def run(ceph_cluster, **kw):
    """
    ceph-mgr restful call for viewing/modifying cluster information
    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster object
        kw: test config arguments
    Returns:
        int: non-zero on failure, zero on pass
    """
    config = kw.get("config")
    node = ceph_cluster.get_nodes(role="mgr")[0]
    username = config.get("username")

    # Enable restful module
    CephAdm(node).ceph.mgr.module.enable("restful")

    # Generate self-signed certificate
    out = CephAdm(node).ceph.restful.create_self_signed_cert()
    if not out:
        raise OperationFailedError("Failed to generate self signed certificate")

    # Create an API user
    key = CephAdm(node).ceph.restful.create_key(username)
    if not key:
        raise OperationFailedError("Failed to create API user")
    return 0
