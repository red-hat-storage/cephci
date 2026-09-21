"""
Stretch-cluster HA workflow for NVMe-oF gateways.

Builds on HighAvailability with stretch_mode: gateway locations,
same-site failover preference, and disaster set/clear for controlled failback.
"""

from tests.nvmeof.workflows.ha import HighAvailability
from utility.log import Log

LOG = Log(__name__)


def build_gw_location_map(gateways, node_to_location):
    """
    Build gw_id -> location map for set_gateway_locations from a node->location map.

    Args:
        gateways: List of NVMeGateway instances (e.g. nvme_service.gateways).
        node_to_location: dict mapping node id (e.g. "node1") or hostname to location string.

    Returns:
        dict: gw_id (daemon_name) -> location, one entry per gateway. Also sets gw.location on each gateway.
    """
    gw_location_map = {}
    for gw in gateways:
        loc = (
            node_to_location.get(gw.node.id)
            or node_to_location.get(gw.hostname)
            or node_to_location.get(gw.daemon_name)
        )
        if loc is not None:
            gw_location_map[gw.gw_id] = loc
            gw.location = loc
    return gw_location_map


def run_stretch_ha(
    ceph_cluster,
    config,
    nvme_service,
    gw_location_map=None,
    configure_entities_fn=None,
    **configure_entities_kw,
):
    """
    Run HA failover/failback with stretch-cluster options (locations + same-site preference).

    Ensures config has stretch_mode=True, builds HA, sets gateway locations when
    provided, then runs ha.run().

    Args:
        ceph_cluster: Ceph cluster object.
        config: Test config (must include gw_nodes, rbd_pool, fault-injection-methods, initiators, etc.).
        nvme_service: NVMeService instance with gateways initialized.
        gw_location_map: Optional dict gw_id -> location. If None, uses config.get("gw_locations").
        configure_entities_fn: Optional callable(nvme_service, **kw) to configure subsystems/namespaces before HA.
        **configure_entities_kw: Passed to configure_entities_fn if provided.

    Returns:
        int: 0 on success. Raises on failure.

    Example config:
        config["stretch_mode"] = True
        config["gw_locations"] = {"node1": "DC1", "node2": "DC1", "node6": "DC2", "node7": "DC2"}
    """
    config["stretch_mode"] = True
    config.setdefault("gw_locations", gw_location_map or {})

    if configure_entities_fn:
        configure_entities_fn(nvme_service, **configure_entities_kw)

    config["nvme_service"] = nvme_service
    ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)
    ha.gateways = nvme_service.gateways

    location_map = config.get("gw_locations") or gw_location_map
    if location_map and ha.nvme_gw:
        if not ha.set_gateway_locations(location_map):
            raise RuntimeError("set_gateway_locations failed")
        LOG.info("Gateway locations set for stretch HA")

    ha.run()
    return 0
