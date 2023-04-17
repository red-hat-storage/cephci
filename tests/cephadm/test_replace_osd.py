import json

from ceph.utils import get_nodes_by_ids
from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from utility.log import Log

log = Log(__name__)


class OsdOperationError(Exception):
    pass


def verify_osd_state(cephadm, osd_id, state):
    kw = {"format": "json-pretty"}
    timeout, interval = 60, 2
    for w in WaitUntil(timeout=timeout, interval=interval):
        out = cephadm.ceph.osd("tree", **kw)
        data = json.loads(out[0])
        for node in data.get("nodes"):
            _id = node.get("id")
            _type = node.get("type")
            _state = node.get("status")
            if _type == "osd" and str(_id) == osd_id and _state == state:
                return True
        log.info(f"OSD '{osd_id}' is not in '{state}' state. Retrying")
    if w.expired:
        raise OsdOperationError(
            f"Failed to get OSD osd '{osd_id}' in '{state}' state within '{interval}' sec"
        )


def run(ceph_cluster, **kw):
    config = kw.get("config")
    replace_config = config.get("replace", {})
    add_config = config.get("add", {})
    installer = ceph_cluster.get_nodes(role="installer")[0]

    # Replace OSD
    osd_id_list = replace_config.get("pos_args", [])
    kw = {"--replace": True}
    node_name_list = replace_config.get("nodes", [])
    if not node_name_list or not osd_id_list or not osd_id_list[0]:
        raise OsdOperationError("Missing node or OSD ID for replace OSD test")
    osd_id = str(osd_id_list[0])
    cephadm = CephAdm(installer)
    log.info(f"Replacing OSD {osd_id}")
    cephadm.ceph.orch.osd.rm(osd_id=osd_id, **kw)
    verify_osd_state(cephadm, osd_id, "destroyed")
    log.info(f"OSD {osd_id} has been replaced successfully")

    # Add OSD
    node_name_list = add_config.get("pos_args", [])
    if not node_name_list or len(node_name_list) < 2:
        raise OsdOperationError("Missing node or device for add OSD test")
    node = [node_name_list[0]]
    nodes = get_nodes_by_ids(ceph_cluster, node)
    hostname = nodes[0].shortname
    device = node_name_list[1]
    log.info(f"Adding OSD on {hostname}")
    cephadm.ceph.orch.daemon.add.osd(hostname, device)
    log.info(f"OSD {osd_id} has been added successfully")

    # Check if the replaced OSD has same ID and is up
    verify_osd_state(cephadm, osd_id, "up")
    return 0
