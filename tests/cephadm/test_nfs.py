import logging

from ceph.ceph_admin.common import fetch_method
from ceph.ceph_admin.nfs import NFS

log = logging.getLogger(__name__)


def run(ceph_cluster, **kw):
    """
    Ceph-admin module to manage NFS service

    Args:
        ceph_cluster (ceph.ceph.Ceph): Ceph cluster object
        kw: test data

    check ceph.ceph_admin.nfs for test config
    """
    log.info("Running Ceph-admin NFS-Ganesha test")
    config = kw.get("config")

    build = config.get("build", config.get("rhbuild"))
    ceph_cluster.rhcs_version = build

    # Manage Ceph using ceph-admin orchestration
    command = config.pop("command")

    log.info("Executing NFS-Ganesha %s service" % command)
    nfs = NFS(cluster=ceph_cluster, **config)
    method = fetch_method(nfs, command)
    method(config)
    if "get_cluster_details" in config:
        nfs.get_cluster_state(config["get_cluster_details"])
    return 0
