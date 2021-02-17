import logging

from ceph.ceph_admin.common import fetch_method
from ceph.ceph_admin.iscsi import ISCSI

log = logging.getLogger(__name__)


def run(ceph_cluster, **kw):
    """
    Ceph-admin module to manage ceph-iscsi service

    Args:
        ceph_cluster (ceph.ceph.Ceph): Ceph cluster object
        kw: test data

    check ceph.ceph_admin.iscsi for test config
    """
    log.info("Running Ceph-admin MDS test")
    config = kw.get("config")

    build = config.get("build", config.get("rhbuild"))
    ceph_cluster.rhcs_version = build

    # Manage Ceph using ceph-admin orchestration
    command = config.pop("command")
    log.info("Executing ISCSI %s service" % command)
    iscsi = ISCSI(cluster=ceph_cluster, **config)
    method = fetch_method(iscsi, command)
    method(config)
    return 0
