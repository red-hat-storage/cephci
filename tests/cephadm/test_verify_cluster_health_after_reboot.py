from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from cli.utilities.utils import get_service_id, reboot_node, set_service_state


class RebootFailed(Exception):
    pass


class ClusterNotHealthy(Exception):
    pass


class SystemctlFailed(Exception):
    pass


def _is_cluster_healthy(node):
    # Verify if the cluster is in healthy state
    timeout, interval = 100, 10
    for w in WaitUntil(timeout=timeout, interval=interval):
        if CephAdm(node).ceph.health() == "HEALTH_OK":
            return True
    if w.expired:
        return False


def run(ceph_cluster, **kw):
    """Verify rebooting a node doesn't affect the ceph health
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    action = config.get("action")
    mon_node = ceph_cluster.get_nodes("mon")[0]

    if action == "node-reboot":
        for node in ceph_cluster.get_nodes():
            # Perform node reboot
            status = reboot_node(node)
            if not status:
                raise RebootFailed(f"Node {node.hostname} failed to reboot.")

            # Verify the reboot doesn't affect the cluster health
            if not _is_cluster_healthy(mon_node):
                raise ClusterNotHealthy(
                    f"Ceph cluster not healthy after rebooting {node.hostname}"
                )

    elif action == "service-state":
        # Get the SERVICE_ID of the mon service
        mon_service = get_service_id(mon_node, "mon")

        for state in ["start", "stop", "restart"]:
            # Set the service to each state
            status = set_service_state(mon_node, mon_service, state)
            if not status:
                raise SystemctlFailed(
                    f"Failed to {state} mon service on {mon_node.hostname}"
                )

            # Verify the change of state doesn't affect the cluster health
            if not _is_cluster_healthy(mon_node):
                raise ClusterNotHealthy(
                    f"Ceph cluster not healthy after setting mon service to {state}"
                )
    return 0
