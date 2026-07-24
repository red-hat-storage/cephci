from cli.cephadm.cephadm import CephAdm
from cli.exceptions import CephadmOpsExecutionError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kwargs):
    """Verify upgrade not creates additional rgw pool"""
    # Get config
    config = kwargs.get("config")

    # Get pool
    pool = config["pool"]

    # Get installer node
    installer = ceph_cluster.get_nodes(role="installer")[0]

    # Get pool list and check rgw pool
    out = CephAdm(installer).ceph.osd.pool.ls()
    if pool in out:
        raise CephadmOpsExecutionError("Upgrade creates additional rgw pool")
    return 0
