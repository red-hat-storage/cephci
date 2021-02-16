import logging

from ceph.ceph_admin.common import fetch_method
from ceph.ceph_admin.osd import OSD

log = logging.getLogger(__name__)


def run(ceph_cluster, **kw):
    """
    Ceph-admin module to manage ceph-osd service

    Args:
        ceph_cluster (ceph.ceph.Ceph): Ceph cluster object
        kw: test data

    check ceph.ceph_admin.osd for test config
    """
    log.info("Running Ceph-admin Monitor test")
    config = kw.get("config")

    build = config.get("build", config.get("rhbuild"))
    ceph_cluster.rhcs_version = build

    # Manage Ceph using ceph-admin orchestration
    command = config.pop("command")
    log.info("Executing OSD %s service" % command)
    osd = OSD(cluster=ceph_cluster, **config)
    method = fetch_method(osd, command)
    method(config)
    return 0
