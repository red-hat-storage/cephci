from utility import utils
from utility.log import Log

LOG = Log(__name__)


def _resolve_target_nodes(cluster, config):
    if config.get("target-nodes"):
        shortnames = set(config["target-nodes"])
        return [n for n in cluster.get_nodes() if n.shortname in shortnames]

    target_nodes = []
    seen = set()
    for role in config.get("target-roles", []):
        for node in cluster.get_nodes(role=role):
            if node.shortname not in seen:
                seen.add(node.shortname)
                target_nodes.append(node)
    if not target_nodes:
        raise ValueError("target-nodes or target-roles must be provided")
    return target_nodes


def run(ceph_cluster, **kwargs):
    """
    Distribute cephadm root CA certificate to cluster nodes using key-based SSH.

    Config keys:
        role: node role to fetch the certificate from (default: rgw)
        idx: index of the source node within that role (default: 0)
        target-nodes: list of node shortnames to receive the certificate
        target-roles: list of roles whose nodes should receive the certificate
        exclude-source: skip the source node when distributing (default: True)
        update-ca-trust: run update-ca-trust on each target (default: True)
    """
    config = kwargs["config"]
    role = config.get("role", "rgw")
    source_node = ceph_cluster.get_nodes(role=role)[config.get("idx", 0)]
    target_nodes = _resolve_target_nodes(ceph_cluster, config)

    if config.get("exclude-source", True):
        target_nodes = [
            node for node in target_nodes if node.ip_address != source_node.ip_address
        ]

    LOG.info(
        "Distributing cephadm root CA from %s to %s",
        source_node.shortname,
        [node.shortname for node in target_nodes],
    )
    utils.distribute_cephadm_root_ca_cert(
        source_node,
        target_nodes,
        update_ca_trust=config.get("update-ca-trust", True),
    )
    return 0
