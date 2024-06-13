from json import loads

from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from cli.utilities.utils import OperationFailedError, create_yaml_config
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Re-deploy node-exporter service with extra entrypoint args"""

    # Get configs
    config = kw.get("config")

    # Get the installer and OSD nodes
    installer = ceph_cluster.get_nodes(role="installer")[0]

    # Get spec file for node-exporter
    specs = config.get("specs", {})

    # Create a spec file for node-exporter re-deployment
    file = create_yaml_config(installer, specs)

    # Re-deploy node-exporter with extraentrypoint
    c = {"pos_args": [], "input": file}
    CephAdm(nodes=installer, mount=file).ceph.orch.apply(**c)

    # Wait for node-exporter services to be ready and running state
    timeout, interval = 30, 5
    for w in WaitUntil(timeout=timeout, interval=interval):
        configs = loads(
            CephAdm(installer).ceph.orch.ls(
                service_type="node-exporter", format="json-pretty"
            )
        )
        running = configs[-1].get("status", {}).get("running")
        size = configs[-1].get("status", {}).get("size")
        if running == size:
            log.info("All node-exporter services are up and running")
            break
    if w.expired:
        raise OperationFailedError(
            f"Node-exporter service failed: expected: {size}, running: {running}"
        )

    return 0
