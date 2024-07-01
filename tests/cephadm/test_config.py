from ceph.ceph_admin import CephAdmin
from utility.log import Log

log = Log(__name__)


def verify_multiple_public_network(cls, configured_network):
    """
    To validate multiple public network set from ceph conf.
    Args:
        cls: cephadm instance object
        configured_network: subnet address of config

    Returns:
        False -> Test Failed to set multiple public network to ceph
        True -> Test passed to validate multiple public network to ceph
    """
    # To validate the set address using get operation
    public_network, _ = cls.shell(
        args=["ceph", "config", "get", "mon", "public_network"]
    )
    if sorted(configured_network.split(",")) == sorted(
        public_network.rstrip().split(",")
    ):
        return True
    return False


def run(ceph_cluster, **kw):
    """
    Test to validate to configure any specific ceph commands using ceph config option.

    Args:
        ceph_cluster (ceph.ceph.Ceph): Ceph cluster object
        kw: test data
    """
    config = kw["config"]
    instance = CephAdmin(cluster=ceph_cluster, **config)
    configured_network = config["public_network"]
    if verify_multiple_public_network(instance, configured_network):
        log.info("Verified multiple public network to ceph")
        return 0
    log.error("Test failed to set multiple public network to ceph")
    return 1
