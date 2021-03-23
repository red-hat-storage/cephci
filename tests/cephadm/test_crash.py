import logging

from ceph.ceph_admin.common import fetch_method
from ceph.ceph_admin.crash import Crash
from ceph.ceph_admin.helper import get_cluster_state

log = logging.getLogger(__name__)


def run(ceph_cluster, **kw):
    """
    Ceph-admin module to manage Crash service

    Args:
        ceph_cluster (ceph.ceph.Ceph): Ceph cluster object
        kw: test data

    check ceph.ceph_admin.crash for test config
    """
    log.info("Running Ceph-admin Crash service test")
    config = kw.get("config")

    build = config.get("build", config.get("rhbuild"))
    ceph_cluster.rhcs_version = build

    # Manage Ceph using ceph-admin orchestration
    command = config.pop("command")
    log.info("Executing Crash %s service" % command)
    crash = Crash(cluster=ceph_cluster, **config)
    try:
        method = fetch_method(crash, command)
        method(config)
    finally:
        # Get cluster state
        get_cluster_state(crash)
    return 0
