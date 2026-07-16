"""Label placement geometry and host resolution for NFS Multi-Active tests."""

from cli.exceptions import ConfigError
from tests.nfs.lib.multi_active.constants import MIN_NFS_NODES, NFS_DAEMON_COUNT

# Default spare slice when placement omits "spare".
SPARE_SLICE = (3, 4)


def _placement(min_nodes, nfs, ingress, spare, nfs_label, ingress_label):
    return {
        "min_nodes": min_nodes,
        "nfs": nfs,
        "ingress": ingress,
        "spare": spare,
        "labels": {"nfs": nfs_label, "ingress": ingress_label},
    }


# Rotate nfs/ingress/spare across the 4-node grim lab so each suite uses all nodes.
FAILOVER_PLACEMENT = _placement(
    4, (0, 2), (1, 3), (3, 4), "nfs-ma-nfs", "nfs-ma-ingress"
)
INTEGRATION_PLACEMENT = _placement(
    4, (1, 3), (2, 4), (0, 1), "nfs-ma-int-nfs", "nfs-ma-int-ingress"
)
COLOCATION_PLACEMENT = _placement(
    4, (2, 4), (3, 1), (1, 2), "nfs-ma-coloc-nfs", "nfs-ma-coloc-ingress"
)
NEGATIVE_PLACEMENT = _placement(
    4, (3, 1), (0, 2), (2, 3), "nfs-ma-neg-nfs", "nfs-ma-neg-ingress"
)


def hostnames(nodes):
    return [node.hostname for node in nodes]


def unique_nodes(*node_groups):
    seen = set()
    ordered = []
    for group in node_groups:
        for node in group:
            key = node.hostname
            if key not in seen:
                seen.add(key)
                ordered.append(node)
    return ordered


def node_slice(nodes, start, end):
    """Return nodes[start:end], wrapping when start >= end (e.g. (3, 1) -> [3, 0])."""
    if start < end:
        return nodes[start:end]
    if start > end:
        return nodes[start:] + nodes[:end]
    return []


def _node_slice(nodes, start, end):
    return node_slice(nodes, start, end)


def resolve_placement_hosts(
    nfs_nodes,
    placement,
    config=None,
    *,
    nfs_count=None,
    spare_slice=SPARE_SLICE,
    min_nodes=None,
    include_spare=True,
):
    """Return nfs_hosts, ingress_hosts, deploy_nodes, nfs_count, labels, spare_host."""
    config = config or {}
    min_nodes = min_nodes or placement.get("min_nodes", MIN_NFS_NODES)
    nfs_count = int(
        nfs_count
        if nfs_count is not None
        else config.get("nfs_count", NFS_DAEMON_COUNT)
    )
    nfs_start, nfs_end = placement["nfs"]
    ingress_start, ingress_end = placement["ingress"]

    if len(nfs_nodes) < min_nodes:
        raise ConfigError(f"Need {min_nodes} NFS-capable nodes, found {len(nfs_nodes)}")

    nfs_sel = _node_slice(nfs_nodes, nfs_start, nfs_end)
    ingress_sel = _node_slice(nfs_nodes, ingress_start, ingress_end)
    deploy_groups = [nfs_sel, ingress_sel]
    spare_host = None
    if include_spare:
        spare_start, spare_end = placement.get("spare", spare_slice)
        spare_sel = _node_slice(nfs_nodes, spare_start, spare_end)
        if not spare_sel:
            raise ConfigError(f"No spare node in slice {(spare_start, spare_end)!r}")
        deploy_groups.append(spare_sel)
        spare_host = spare_sel[0].hostname

    labels = placement.get("labels") or {}
    return (
        hostnames(nfs_sel),
        hostnames(ingress_sel),
        unique_nodes(*deploy_groups),
        nfs_count,
        labels,
        spare_host,
    )


def resolve_integration_hosts(nfs_nodes, placement=INTEGRATION_PLACEMENT):
    """Return nfs_hosts, ingress_hosts, deploy_nodes, spare_host."""
    nfs_hosts, ingress_hosts, deploy_nodes, _nfs_count, _labels, spare_host = (
        resolve_placement_hosts(nfs_nodes, placement)
    )
    return nfs_hosts, ingress_hosts, deploy_nodes, spare_host
