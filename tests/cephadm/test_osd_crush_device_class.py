from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError, OperationFailedError, OsdOperationError


def run(ceph_cluster, **kwargs):
    """Verify osd spec class crush_device_class"""
    # Get config
    config = kwargs.get("config")

    # Check mandatory parameter crush_device_class
    if not config.get("crush_device_class"):
        raise ConfigError("Mandatory config 'crush_device_class' not provided")

    # get cluster config
    installer = ceph_cluster.get_nodes(role="installer")[0]

    # Get crush_device_class
    crush_device_class = config.get("crush_device_class")

    # Wait for OSDs deployment
    timeout, interval = 300, 6
    for w in WaitUntil(timeout=timeout, interval=interval):
        if CephAdm(installer).ceph.osd.ls():
            break
    if w.expired:
        raise OperationFailedError("Failed to wait for OSD to generate")

    # Verify crush_device_class
    out = CephAdm(installer).ceph.osd.tree()
    if crush_device_class not in out:
        raise OsdOperationError("crush_device_class not found")

    return 0
