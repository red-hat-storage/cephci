from json import loads

from ceph.utils import get_node_by_id
from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import CephadmOpsExecutionError, OperationFailedError
from cli.utilities.containers import Container
from cli.utilities.utils import create_yaml_config
from utility.log import Log

log = Log(__name__)


def _wait_for_service_daemons(host, service_type):
    """Wait until every service daemon is running with a container ID."""
    orch = CephAdm(host).ceph.orch
    service_info = []
    for waiter in WaitUntil(timeout=300, interval=10):
        orch.ps(refresh=True)
        conf = {"daemon_type": service_type, "format": "json-pretty"}
        service_info = loads(orch.ps(**conf))
        pending = [
            daemon
            for daemon in service_info
            if daemon.get("status_desc") != "running" or not daemon.get("container_id")
        ]
        if service_info and not pending:
            log.info(f"All {service_type} services are running with container IDs")
            return service_info

        for daemon in pending:
            log.info(
                f"Waiting for {daemon.get('daemon_name', service_type)} on "
                f"{daemon.get('hostname')}: status={daemon.get('status_desc')}, "
                f"container_id={daemon.get('container_id') or 'none'}"
            )

    if waiter.expired:
        raise OperationFailedError(
            f"{service_type} daemons did not become running with container IDs: "
            f"{service_info}"
        )


def _wait_for_custom_config(
    ceph_cluster,
    host,
    service_type,
    service_info,
    mount_path,
    content,
):
    """Wait until the custom file is present in every current daemon container."""
    orch = CephAdm(host).ceph.orch
    daemon_names = {daemon["daemon_name"] for daemon in service_info}
    pending = {}

    for waiter in WaitUntil(timeout=300, interval=10):
        orch.ps(refresh=True)
        conf = {"daemon_type": service_type, "format": "json-pretty"}
        current_daemons = {
            daemon["daemon_name"]: daemon for daemon in loads(orch.ps(**conf))
        }
        pending = {}

        for daemon_name in sorted(daemon_names):
            daemon = current_daemons.get(daemon_name)
            if not daemon:
                pending[daemon_name] = "daemon is absent from ceph orch ps"
                continue

            status = daemon.get("status_desc")
            container_id = daemon.get("container_id")
            if status != "running" or not container_id:
                pending[daemon_name] = (
                    f"status={status}, container_id={container_id or 'none'}"
                )
                continue

            hostname = daemon["hostname"]
            node = get_node_by_id(ceph_cluster, hostname)
            if not node:
                raise CephadmOpsExecutionError(
                    f"Unable to find cluster node for {daemon_name} on {hostname}"
                )

            result, error = Container(node).exec(
                container=container_id,
                cmds=f"cat {mount_path}",
            )
            if result != content:
                reason = f"expected content is absent from container {container_id}"
                if error:
                    reason = f"{reason}: {error.strip()}"
                pending[daemon_name] = reason

        if not pending:
            log.info(
                f"Custom config {mount_path} is present in all {service_type} daemons"
            )
            return

        for daemon_name, reason in pending.items():
            log.info(f"Waiting for custom config in {daemon_name}: {reason}")

    if waiter.expired:
        raise CephadmOpsExecutionError(
            f"Custom config {mount_path} was not updated in all {service_type} "
            f"daemons: {pending}"
        )


def run(ceph_cluster, **kw):
    """Verify cephadm custom config file support"""

    # Get configs
    config = kw.get("config")

    # Get the installer node
    installer = ceph_cluster.get_nodes(role="installer")[0]

    # Get the config spec
    spec = config.get("spec", {})
    service_type = spec.get("service_type", {})
    custom_configs = spec.get("custom_configs", {})[0]
    mount_path = custom_configs.get("mount_path", {})
    content = custom_configs.get("content", {})

    # Generate a custom config yaml file out of spec
    file = create_yaml_config(installer, spec)

    # Mount and apply custom config file
    c = {"pos_args": [], "input": file}
    out = CephAdm(nodes=installer, mount=file).ceph.orch.apply(**c)
    if "Scheduled" not in out:
        raise OperationFailedError(f"Fail to apply {file} file")

    # Redeploy daemon
    out = CephAdm(nodes=installer).ceph.orch.redeploy(service_type)
    if "Scheduled" not in out:
        raise OperationFailedError(f"Fail to redeploy {service_type} daemon")

    service_info = _wait_for_service_daemons(installer, service_type)
    _wait_for_custom_config(
        ceph_cluster,
        installer,
        service_type,
        service_info,
        mount_path,
        content,
    )

    return 0
