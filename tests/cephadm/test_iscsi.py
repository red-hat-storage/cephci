from ceph.ceph_admin.common import fetch_method
from ceph.ceph_admin.helper import get_cluster_state
from ceph.ceph_admin.iscsi import ISCSI
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Ceph-admin module to manage ceph-iscsi service

    Args:
        ceph_cluster (ceph.ceph.Ceph): Ceph cluster object
        kw: test data

    check ceph.ceph_admin.iscsi for test config
    """

    log.info("Running Ceph-admin ISCSI test")
    config = kw.get("config")

    build = config.get("build", config.get("rhbuild"))
    ceph_cluster.rhcs_version = build

    # Manage Ceph using ceph-admin orchestration
    command = config.pop("command")
    log.info("Executing ISCSI %s service" % command)
    iscsi = ISCSI(cluster=ceph_cluster, **config)

    try:
        method = fetch_method(iscsi, command)
        method(config)
    finally:
        # Get cluster state
        get_cluster_state(iscsi)
    return 0
