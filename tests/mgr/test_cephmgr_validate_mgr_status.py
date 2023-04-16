from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from cli.utilities.utils import get_service_id, reboot_node, set_service_state


class CephMgrServiceFailError(Exception):
    pass


class CephClusterHealthError(Exception):
    pass


class NodeRebootFailureError(Exception):
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
    """Verify Ceph Mgr status
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    node = ceph_cluster.get_nodes(role="mgr")[0]
    step_to_validate = kw.get("config").get("operation")

    mgr_service = get_service_id(node, "mgr")
    if step_to_validate in ["systemctl-ops", "kill-mgr"]:
        for state in ["start", "stop", "restart"]:
            # Set the service to each state
            status = set_service_state(node, mgr_service, state)

            # Verify stopping an active mgr brings up the standby mgr
            # and cluster health doesn't raise health warn
            if state == "stop" and step_to_validate == "kill-mgr":
                if not _is_cluster_healthy(node):
                    raise CephClusterHealthError(
                        "Ceph cluster not healthy after killing active mgr. Standby mgr not up"
                    )
            if not status:
                raise CephMgrServiceFailError(
                    f"Failed to {state} mgr service on {node.hostname}"
                )

    elif step_to_validate == "reboot":
        status = reboot_node(node)
        if not status:
            raise NodeRebootFailureError(f"Node {node.hostname} failed to reboot.")

        # Verify the reboot doesn't affect the cluster health
        if not _is_cluster_healthy(node):
            raise CephClusterHealthError(
                f"Ceph cluster not healthy after rebooting mgr node {node.hostname}"
            )

    elif step_to_validate == "fail-mgr":
        if CephAdm(node).ceph.mgr.fail(mgr=mgr_service):
            raise CephMgrServiceFailError("Failed to execute `ceph mgr fail` command")

        # The standby mgr should be up and cluster should not throw health warning
        if not _is_cluster_healthy(node):
            raise CephClusterHealthError(
                f"Standby mgr is not up after failing the active mgr. "
                f"Cluster not healthy{node.hostname}"
            )

    return 0
