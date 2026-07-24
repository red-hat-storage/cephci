from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError


def run(ceph_cluster, **kw):
    """Verify changing the cluster network using cephadm
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    public_ip = config.get("public-network")
    node = ceph_cluster.get_nodes("installer")[0]

    # Set the public network
    CephAdm(node).ceph.config.set(daemon="mon", key="public_network", value=public_ip)

    public_network = CephAdm(node).ceph.config.get("mon", "public_network")[0]
    if public_ip not in public_network:
        raise OperationFailedError("Failed to set the public_network")

    # Set the cluster network
    CephAdm(node).ceph.config.set("cluster_network", public_ip, "global")
    cluster_network = CephAdm(node).ceph.config.get("mon", "cluster_network")[0]
    if public_ip not in cluster_network:
        raise OperationFailedError("Failed to set the cluster_network")

    return 0
