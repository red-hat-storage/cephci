from cli.exceptions import (
    OperationFailedError,
    ResourceNotFoundError,
    UnexpectedStateError,
)
from cli.utilities.operations import wait_for_cluster_health
from cli.utilities.utils import get_service_id, get_service_state, set_service_state
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    # Check if client node is present
    client = ceph_cluster.get_nodes(role="client")
    if not client:
        raise ResourceNotFoundError("Client node is missing, add a client node")
    client, osd_id = client[0], list()
    # Fetch the nodes with osd service daemons
    osd_node = ceph_cluster.get_nodes(role="osd")
    for node in osd_node:
        # Fetch id of osd service
        osd_id = get_service_id(node, "osd")
        for id in osd_id:
            # Stop the service using systemctl command
            service_state = set_service_state(node, id, "stop")
            if not service_state:
                raise OperationFailedError(
                    f"Failed to stop OSD service on {node.hostname}"
                )
            # Check if service has stopped using systemctl
            status = get_service_state(node, id)
            if "inactive" in status:
                log.info("Service has been stopped")
            # Check the cluster health after service is stopped
            health = wait_for_cluster_health(client, "HEALTH_WARN", 300, 10)
            if not health:
                raise UnexpectedStateError("Cluster is not in expected state")
            # Start the service using systemctl command
            service_state = set_service_state(node, id, "start")
            if not service_state:
                raise OperationFailedError(
                    f"Failed to start OSD service on {node.hostname}"
                )
            # Check if service has started using systemctl
            status = get_service_state(node, id)
            if "active" in status:
                log.info("Service has been started")
            # Check the cluster health after service is started
            health = wait_for_cluster_health(client, "HEALTH_OK", 300, 10)
            if not health:
                raise UnexpectedStateError("Cluster is not in expected state")
    return 0
