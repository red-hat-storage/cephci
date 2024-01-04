from cli.cephadm.cephadm import CephAdm
from cli.exceptions import CephadmOpsExecutionError


def run(ceph_cluster, **kwargs):
    """Change public network and check cluster health"""

    # Get config
    config = kwargs.get("config")

    # Get public network
    public_network = config.get("public_network")

    # Get installer node
    installer = ceph_cluster.get_nodes(role="installer")[0]

    # Set public network
    CephAdm(installer).ceph.config.set(
        daemon="mon", key="public_network", value=public_network
    )

    # TO DO (mobisht): Need to collect more info on getting public network
    # Get cluster health
    out = CephAdm(installer).ceph.health(detail=True)
    if out != "HEALTH_OK":
        raise CephadmOpsExecutionError("Cluster is not HEALTHY")
        return 1
    return 0
