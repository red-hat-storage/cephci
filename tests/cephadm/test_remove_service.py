from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from utility.log import Log

log = Log(__name__)


class RemoveService(Exception):
    pass


def _verify_service(node, service_name):
    """
    Verifies service is removed
    Args:
        node (str): monitor node object
        service_name (str): service name
    return: (bool)
    """
    # Get service status
    out = CephAdm(node).ceph.orch.ls(service_type=service_name, format="json-pretty")
    # If service was removed, wait for a timeout to check whether its removed
    timeout, interval = 20, 2
    for w in WaitUntil(timeout=timeout, interval=interval):
        out = CephAdm(node).ceph.orch.ls(
            service_type=service_name, format="json-pretty"
        )
        if "No services reported" in out:
            return True
    if w.expired:
        raise RemoveService(
            f"Service {service_name} is not removed after rm operation."
        )


def run(ceph_cluster, **kw):
    """Verify redeploy for a specific service
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    node = ceph_cluster.get_nodes(role="mon")[0]
    service_name = config.get("service_name")
    if not service_name:
        raise RemoveService("service_name value not present in config")

    # Remove service service
    log.info(f"Removing {service_name} service")
    CephAdm(node).ceph.orch.rm(service_name)

    # Verify if service is removed
    _verify_service(node, service_name)
    return 0
