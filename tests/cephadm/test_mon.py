from ceph.ceph_admin.common import fetch_method
from ceph.ceph_admin.helper import get_cluster_state
from ceph.ceph_admin.mon import Mon
from utility.log import Log

log = Log(__name__)

CLUSTER_STATE = [
    "ceph status",
    "ceph orch ls mon -f json-pretty",
    "ceph orch ps '' mon -f json-pretty",
    "ceph health detail -f yaml",
]


def run(ceph_cluster, **kw):
    """
    Ceph-admin module to manage monitor service

    Args:
        ceph_cluster (ceph.ceph.Ceph): Ceph cluster object
        kw: test data

    check ceph.ceph_admin.mon for test config
    """
    log.info("Running Ceph-admin Monitor test")
    config = kw.get("config")

    build = config.get("build", config.get("rhbuild"))
    ceph_cluster.rhcs_version = build

    # Manage Ceph using ceph-admin orchestration
    command = config.pop("command")
    log.info("Executing MON %s service" % command)
    monitor = Mon(cluster=ceph_cluster, **config)
    try:
        method = fetch_method(monitor, command)
        method(config)
    finally:
        # Get cluster state
        get_cluster_state(monitor, CLUSTER_STATE)
    return 0
