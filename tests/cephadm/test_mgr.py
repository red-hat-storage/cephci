import logging

from ceph.ceph_admin.common import fetch_method
from ceph.ceph_admin.mgr import Mgr

log = logging.getLogger(__name__)


def run(ceph_cluster, **kw):
    """
    Ceph-admin module to manage ceph-manager service

    Args:
        ceph_cluster (ceph.ceph.Ceph): Ceph cluster object
        kw: test data

    check ceph.ceph_admin.mon for test config
    """
    log.info("Running Ceph-Admin Manager test")
    config = kw.get("config")

    build = config.get("build", config.get("rhbuild"))
    ceph_cluster.rhcs_version = build

    # Manage Ceph using ceph-admin orchestration
    command = config.pop("command")
    log.info("Executing MGR %s service" % command)
    manager = Mgr(cluster=ceph_cluster, **config)
    method = fetch_method(manager, command)
    method(config)
    return 0
