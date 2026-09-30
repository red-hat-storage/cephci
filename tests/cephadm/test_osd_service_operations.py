import json
import re

from ceph.waiter import WaitUntil
from cli.ceph.ceph import Ceph
from cli.exceptions import (
    OperationFailedError,
    ResourceNotFoundError,
    UnexpectedStateError,
)
from cli.utilities.operations import (
    wait_for_cluster_health,
    wait_for_osd_daemon_state,
)
from cli.utilities.utils import get_service_id, set_service_state
from utility.log import Log

log = Log(__name__)


def _log_health_details(client):
    """Collect health and daemon events without hiding the health failure."""
    ceph = Ceph(client)
    detail, daemons = "Health details unavailable", []
    try:
        detail = ceph.health(detail=True)
        log.error(detail)
    except Exception as error:
        log.warning(f"Unable to collect health details: {error}")
    try:
        output = ceph.orch.ps(format="json", refresh=True)
        daemons = json.loads(output)
        failed_daemons = [daemon for daemon in daemons if daemon.get("status") != 1]
        log.error(f"Non-running daemon status and events: {json.dumps(failed_daemons)}")
    except Exception as error:
        log.warning(f"Unable to collect daemon status: {error}")
    return detail, daemons


def _has_only_node_exporter_warning(detail, daemons):
    """Return True when node-exporter is the cluster's only health problem."""
    warning_codes = set(
        re.findall(r"^\[(?:WRN|ERR)\] ([A-Z0-9_]+):", detail, re.MULTILINE)
    )
    failed_daemons = [daemon for daemon in daemons if daemon.get("status") != 1]
    return (
        warning_codes == {"CEPHADM_FAILED_DAEMON"}
        and failed_daemons
        and all(
            daemon.get("daemon_name", "").startswith("node-exporter.")
            for daemon in failed_daemons
        )
    )


def _wait_for_healthy_cluster(client, context, **_):
    """Wait for HEALTH_OK, allowing an unrelated node-exporter-only warning."""
    for waiter in WaitUntil(timeout=300, interval=10):
        health = Ceph(client).health()
        log.info(f"Cluster status is {health}")
        if "HEALTH_OK" in health:
            return

        detail, daemons = _log_health_details(client)
        if _has_only_node_exporter_warning(detail, daemons):
            log.warning(
                "Continuing OSD service validation because node-exporter is the "
                "only unhealthy daemon"
            )
            return

    if waiter.expired:
        raise UnexpectedStateError(f"Cluster is not HEALTH_OK {context}: {detail}")


def _check_service_state(node, service_id, expected):
    """Check exact systemd state; inactive normally has a nonzero exit status."""
    status, _ = node.exec_command(
        cmd=f"systemctl is-active {service_id}", sudo=True, check_ec=False
    )
    if status.strip() != expected:
        raise UnexpectedStateError(
            f"OSD service {service_id} on {node.hostname} is "
            f"{status.strip()!r}, expected {expected!r}"
        )
    log.info(f"OSD service {service_id} is {expected}")


def run(ceph_cluster, **kw):
    """Test stopping and starting each OSD through systemd."""
    # Check if client node is present
    client = ceph_cluster.get_nodes(role="client")
    if not client:
        raise ResourceNotFoundError("Client node is missing, add a client node")
    client = client[0]
    # Fetch the nodes with osd service daemons
    osd_nodes = ceph_cluster.get_nodes(role="osd")
    if not osd_nodes:
        raise ResourceNotFoundError("OSD nodes are missing")

    _wait_for_healthy_cluster(client, "before stopping any OSD")
    for node in osd_nodes:
        # Fetch id of osd service
        service_ids = get_service_id(node, "osd")
        if not service_ids or not all(service_ids):
            raise ResourceNotFoundError(f"OSD services are missing on {node.hostname}")
        for service_id in service_ids:
            osd_id = service_id.rsplit("@osd.", 1)[1].removesuffix(".service")
            try:
                if not set_service_state(node, service_id, "stop"):
                    raise OperationFailedError(
                        f"Failed to stop OSD service {service_id} on {node.hostname}"
                    )
                _check_service_state(node, service_id, "inactive")
                wait_for_osd_daemon_state(client, osd_id, "down")
                if not wait_for_cluster_health(client, "HEALTH_WARN", 300, 10):
                    detail, _ = _log_health_details(client)
                    raise UnexpectedStateError(
                        f"Cluster is not HEALTH_WARN after stopping {service_id}: {detail}"
                    )
            finally:
                # Restore the OSD even when a stopped-state assertion fails.
                if not set_service_state(node, service_id, "start"):
                    raise OperationFailedError(
                        f"Failed to start OSD service {service_id} on {node.hostname}"
                    )
                _check_service_state(node, service_id, "active")

            wait_for_osd_daemon_state(client, osd_id, "up")
            _wait_for_healthy_cluster(client, f"after starting {service_id}")
    return 0
