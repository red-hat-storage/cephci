"""Manage the ceph dashboard service via cephadm CLI."""

from ceph.ceph_admin import CephAdmin, dashboard
from ceph.ceph_admin.common import fetch_method
from ceph.ceph_admin.helper import get_cluster_state
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Ceph-admin module to manage ceph-dashboard service.

    check ceph.ceph_admin.dashboard for test config.

    Args:
        ceph_cluster (ceph.ceph.Ceph): Ceph cluster object.
        kw: keyword arguments from test data.

    Returns:
        value 0 on success.

    """
    log.info("Running Ceph-admin Dashboard test")
    config = kw.get("config")

    build = config.get("build", config.get("rhbuild"))
    ceph_cluster.rhcs_version = build

    # Manage Ceph using ceph-admin orchestration
    command = config.pop("command")
    log.info("Executing dashboard %s operation" % command)
    instance = CephAdmin(cluster=ceph_cluster, **config)

    try:
        method = fetch_method(dashboard, command)
        method(instance, config.get("args"))
    finally:
        # Get cluster state
        get_cluster_state(instance)
    return 0
