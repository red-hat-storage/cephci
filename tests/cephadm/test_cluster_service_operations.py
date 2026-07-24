from cli.cephadm.cephadm import CephAdm
from utility.log import Log

log = Log(__name__)

MSG = {"stop": "Stopping entire {} service is prohibited."}


def run(ceph_cluster, **kw):
    """
    Verify service operation for ceph services
    """
    config = kw.get("config")
    operation = config.pop("operation")
    services = config.pop("service")

    # Stop the service
    node = ceph_cluster.get_nodes(role="installer")[0]
    for service in services:
        log.info(f"Performing operation on {service} service")
        out = getattr(CephAdm(node).ceph.orch, operation)(service)
        if MSG.get(operation).format(service) not in out:
            log.info(
                f"Failed to perform operation on service {service}, unexpected message '{out}'"
            )
            return 1

    log.info("Successfully performed operation on services with expected message")
    return 0
