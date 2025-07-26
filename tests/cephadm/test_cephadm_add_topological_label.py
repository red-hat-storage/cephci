from ceph.utils import get_node_by_id
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Add/Remove topological labels for host in cluster
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
        e.g
        test:
            config:
            node: Node to which label needs to be added/removed
            topological_labels (str): Label to added
    """
    config = kw.get("config")
    admin_node = ceph_cluster.get_nodes(role="installer")[0]
    nodes = config.get("node")
    node_id = get_node_by_id(ceph_cluster, nodes)
    node = node_id.hostname
    labels = config.get("topological_labels")
    # Add label for host
    # Note: If no label is provided, existing label will be removed
    out = CephAdm(admin_node).ceph.orch.host.set_topological_labels(node, labels)
    if "Updated host" not in out:
        raise OperationFailedError("Failed to set topologiocal label to the host")
    log.info(f"Topological-label has been set successfully for host {node}")
    return 0
