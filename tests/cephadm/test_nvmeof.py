from ceph.ceph_admin.common import fetch_method
from ceph.ceph_admin.helper import get_cluster_state
from ceph.ceph_admin.nvmeof import NVMeoF
from distutils.version import LooseVersion
from utility.log import Log
from functools import wraps

log = Log(__name__)


def apply_nvme_version_specific_cfg(func):
    """Decorator to add version-specific configuration."""
    @wraps(func)
    def wrapper(ceph_cluster, **kwargs):
        version = kwargs.get("version")

        if version == LooseVersion("8.0"):
            pos_args = kwargs.get("config").get("pos_args", [])
            pos_args.append("gw1")
            # Update the cfg with the modified pos_args
            kwargs["config"]["pos_args"] = pos_args

        return func(ceph_cluster, **kwargs)
    return wrapper



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

    # Manage Ceph using ceph-admin orchestration
    command = config.pop("command")
    log.info("Executing Ceph NVMeoF %s service" % command)
    nvmeof = NVMeoF(cluster=ceph_cluster, **config)
    try:
        method = fetch_method(nvmeof, command)
        method(config)
    finally:
        # Get cluster state
        if kw.get("no_cluster_state", True):
            get_cluster_state(nvmeof)
    return 0
