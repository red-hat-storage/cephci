from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError

PORT = "5000"


def run(ceph_cluster, **kw):
    """Verify re-deploying of monitoring stack with custom images
    Args:
        ceph_cluster(ceph.ceph.Ceph): CephNode or list of CephNode object
    """

    # Get config file and node
    config = kw.get("config")
    installer = ceph_cluster.get_nodes(role="installer")[0]
    bootstrap_image = config.get("bootstrap_image")

    # Perform upgrade
    image = f"{installer.hostname}:{PORT}/{bootstrap_image}"
    kw = {"image": f"{image}"}
    out = CephAdm(installer).ceph.orch.upgrade.check(**kw)
    if "`cephadm pull` failed" in out:
        raise OperationFailedError(f"Failed to perform ceph upgrade : {out}")

    # Perform Upgrade
    CephAdm(installer).ceph.orch.upgrade.start(**kw)

    return 0
