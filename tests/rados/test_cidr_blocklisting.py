import yaml

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.test_data_migration_bw_pools import create_given_pool
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test to verify CIDR blocklisting of ceph clients
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    # client_nodes = rados_obj.ceph_cluster.get_nodes(role="client")
    pool_details = config["pool_configs"]
    pool_configs_path = config["pool_configs_path"]
    log.debug("Verifying CIDR Blocklisting of ceph clients")

    with open(pool_configs_path, "r") as fd:
        pool_conf = yaml.safe_load(fd)

    for i in pool_details.values():
        pool = pool_conf[i["type"]][i["conf"]]
        pool.update({"app_name": "rbd"})
        create_given_pool(rados_obj, pool)

    return 0
