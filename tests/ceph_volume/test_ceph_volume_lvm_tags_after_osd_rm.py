from json import loads

from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from cli.utilities.utils import (
    exec_command_on_container,
    get_lvm_on_osd_container,
    get_running_containers,
)
from utility.log import Log

log = Log(__name__)


class RemoveOsdError(Exception):
    pass


class RemoveLvmError(Exception):
    pass


def run(ceph_cluster, **kw):
    """Check LVM tags post removing OSD (--zap)
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")

    osd_node = ceph_cluster.get_nodes(role="osd")[0]

    # Get the lvm list before osd remove
    running_containers, _ = get_running_containers(
        osd_node, format="json", expr="name=osd", sudo=True
    )
    container_ids = [item.get("Names")[0] for item in loads(running_containers)]
    lvm_list = get_lvm_on_osd_container(container_ids[0], osd_node)

    # Identify an OSD ID to perform
    osd_id = list(lvm_list.keys())[0]
    osd_lvm = lvm_list.get(osd_id)[0].get("lv_path")

    # Change the replication to 2 / default is 3
    out = CephAdm(osd_node).ceph.osd.pool.ls(format="json")
    if not out:
        raise RemoveOsdError("Failed to fetch the pools")

    pools = loads(out[0])
    pool_size = 2  # Update the pool size to 2
    for pool in pools:
        pool_name = pool.get("pool_name")
        out = CephAdm(osd_node).ceph.osd.pool.set(pool_name, "size", pool_size)
        if not out:
            raise RemoveOsdError("Failed to update the pool size")

    # Perform osd remove
    conf = {"zap": True} if config.get("operation") else {}
    osd_rm = CephAdm(osd_node).ceph.orch.osd.rm(osd_id=osd_id, **conf)
    if not osd_rm:
        raise RemoveOsdError("Failed to remove osd")

    # Wait until the rm operation is complete
    timeout, interval = 300, 6
    for w in WaitUntil(timeout=timeout, interval=interval):
        conf = {"format": "json"}
        out = CephAdm(osd_node).ceph.orch.osd.rm(status=True, **conf)
        if "No OSD remove/replace operations reported" in out:
            break
    if w.expired:
        raise RemoveOsdError("Failed to perform osd rm operation. Timed out!")

    # Step to validate lvm zap
    if config.get("operation"):
        timeout, interval = 60, 5
        for w in WaitUntil(timeout=timeout, interval=interval):
            try:
                cmd = f" ceph-volume lvm zap --destroy {osd_lvm}"
                _ = exec_command_on_container(
                    node=osd_node, ctr=container_ids[0], cmd=cmd, sudo=True
                )
                break
            except Exception:
                pass
        if w.expired:
            raise RemoveOsdError("Failed to perform lvm zap operation. Timed out!")

        # Verify the lvm list has been updated
        new_lvm_list = get_lvm_on_osd_container(container_ids[0], osd_node)
        if lvm_list == new_lvm_list:
            raise RemoveLvmError("LVM not removed after the osd remove")
    else:
        # Get osd tree and check whether the deleted osd is present or not
        out = CephAdm(osd_node).ceph.osd.tree(format="json")
        osd_tree = loads(out)
        for osd_ in osd_tree.get("nodes"):
            if osd_.get("id") == osd_id and osd_.get("status") == "up":
                raise RemoveOsdError("The osd is up after rm operation")

        # Validate the osd rm process
        try:
            _ = get_lvm_on_osd_container(container_ids[0], osd_node)
        except Exception as e:
            if "Error: no container" not in str(e):
                raise RemoveOsdError("Osd rm operation was not successful")
            log.info("Osd rm process was successful")
            return 0

        # Raise error is the container is still up
        raise RemoveOsdError("OSD id still in lvm even after rm operation")

    return 0
