from ceph.ceph_admin.common import fetch_method
from ceph.ceph_admin.helper import get_cluster_state
from ceph.ceph_admin.nvmeof import NVMeoF
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Ceph-admin module to manage ceph NVMeoF service

    Args:
        ceph_cluster (ceph.ceph.Ceph): Ceph cluster object
        kw: test data

    check ceph.ceph_admin.nvmeof for test config
    """
    log.info("Running Ceph-admin NVMeoF test")
    config = kw.get("config")

    build = config.get("build", config.get("rhbuild"))
    ceph_cluster.rhcs_version = build

    # Manage Ceph using ceph-admin orchestration
    command = config.pop("command")
    log.info("Executing Ceph NVMeoF %s service" % command)
    nvmeof = NVMeoF(cluster=ceph_cluster, **config)
    try:
        method = fetch_method(nvmeof, command)
        method(config)
    finally:
        # Get cluster state
        get_cluster_state(nvmeof)
    return 0
