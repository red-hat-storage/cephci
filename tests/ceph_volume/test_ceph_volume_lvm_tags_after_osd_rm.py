from json import loads

from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from cli.utilities.utils import get_lvm_on_osd_container, get_running_containers


class RemoveOsdError(Exception):
    pass


def run(ceph_cluster, **kw):
    """Check LVM tags post removing OSD (--zap)
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    osd_node = ceph_cluster.get_nodes(role="osd")[0]

    # Get the lvm list before osd remove
    running_containers, _ = get_running_containers(
        osd_node, format="json", expr="name=osd", sudo=True
    )
    container_ids = [item.get("Names")[0] for item in loads(running_containers)]
    lvm_list = get_lvm_on_osd_container(container_ids[0], osd_node)

    # Identify an OSD ID to perform
    osd_id = list(lvm_list.keys())[0]

    # Change the replication to 2 / default is 3
    out = CephAdm(osd_node).ceph.osd(command="pool ls detail", format="json")
    if not out:
        raise RemoveOsdError("Failed to fetch the pools")

    pools = loads(out[0])
    pool_size = 2  # Update the pool size to 2
    for pool in pools:
        pool_name = pool.get("pool_name")
        cmd = f"pool set {pool_name} size {pool_size}"
        _, out = CephAdm(osd_node).ceph.osd(command=cmd)
        if not out:
            raise RemoveOsdError("Failed to update the pool size")

    # Perform osd remove
    conf = {"--zap": True}
    osd_rm = CephAdm(osd_node).ceph.orch.osd.rm(osd_id=osd_id, **conf)
    if not osd_rm:
        raise RemoveOsdError("Failed to remove osd")

    # Wait until the rm operation is complete
    timeout, interval = 60, 2
    for w in WaitUntil(timeout=timeout, interval=interval):
        conf = {"format": "json"}
        out = CephAdm(osd_node).ceph.orch.osd.rm(status=True, **conf)
        if "No OSD remove/replace operations reported" in out:
            break
    if w.expired:
        raise RemoveOsdError("Failed to perform osd rm operation. Timed out!")

    # Get osd tree and check whether the deleted osd is present or not
    out = CephAdm(osd_node).ceph.osd(command="tree", format="json")
    osd_tree = loads(out[0])
    for osd_ in osd_tree.get("nodes"):
        if osd_.get("id") == osd_id and osd_.get("status") == "up":
            raise RemoveOsdError("The osd is up after rm operation")

    # Get the lvm list after osd remove
    lvm_list = get_lvm_on_osd_container(container_ids[0], osd_node)
    if osd_id in (list(lvm_list.keys())):
        raise RemoveOsdError("OSD id still in lvm even after rm operation")

    return 0
