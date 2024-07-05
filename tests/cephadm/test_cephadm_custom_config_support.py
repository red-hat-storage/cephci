from json import loads

from ceph.utils import get_node_by_id
from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import CephadmOpsExecutionError, OperationFailedError
from cli.utilities.containers import Container
from cli.utilities.utils import create_yaml_config
from utility.log import Log

log = Log(__name__)


def _verify_service_status(host, service_type):
    """Verify service status is running"""
    has_status = False
    for w in WaitUntil():
        CephAdm(host).ceph.orch.ps(refresh=True)
        conf = {"daemon_type": service_type, "format": "json-pretty"}
        service_info = loads(CephAdm(host).ceph.orch.ps(**conf))
        status_desc = [c.get("status_desc") for c in service_info][0]
        if status_desc == "starting":
            has_status = True
            log.info(f"{service_type} service is in starting")
        if has_status and status_desc == "running":
            log.info(f"All {service_type} services are up and running")
            return True
    if w.expired:
        raise OperationFailedError(
            f"{service_type} service failed; expected status: running, found: {status_desc}"
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

    # Verify daemon are running
    _verify_service_status(installer, service_type)

    # Get container ID
    CephAdm(installer).ceph.orch.ps(refresh=True)
    conf = {"daemon_type": service_type, "format": "json-pretty"}
    service_info = loads(CephAdm(nodes=installer).ceph.orch.ps(**conf))
    container_id = [c.get("container_id") for c in service_info][0]
    hostname = [c.get("hostname") for c in service_info][0]

    # Convert hostname to ceph object
    node = get_node_by_id(ceph_cluster, hostname)

    # Validate custom file mounted in containers
    timeout, interval = 60, 6
    for w in WaitUntil(timeout=timeout, interval=interval):
        result = Container(node).exec(
            container=str(container_id),
            interactive=True,
            tty=True,
            cmds=f"cat {mount_path}",
        )[0]
        if result == content:
            break
    if w.expired:
        raise CephadmOpsExecutionError(
            f"{service_type} container file content not updated"
        )

    return 0
