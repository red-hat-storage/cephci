import time
import json

from ceph.parallel import parallel
from tests.rbd_mirror.rbd_mirror_utils import rbd_mirror_config
from ceph.utils import find_vm_node_by_hostname
from tests.cephfs.cephfs_system.osd_node_failure_ops import object_compare
from utility.log import Log

log = Log(__name__)


def test_9470(ceph_cluster, rbd_mirror, pool_type, **kw):
    try:
        mirror1 = rbd_mirror.get("mirror1")
        mirror2 = rbd_mirror.get("mirror2")
        config = kw.get("config")
        pool = config[pool_type]["pool"]
        image = config[pool_type]["image"]
        imagespec = pool + "/" + image
        state_after_demote = "up+stopped" if mirror1.ceph_version < 3 else "up+unknown"
        log.info("Rebooting all the nodes in the primary cluster:")
        with parallel() as p:
            for node in mirror1.ceph_nodes:
                p.spawn(
                    mirror1.exec_cmd,
                    ceph_args=False,
                    cmd="reboot",
                    node=node,
                    check_ec=False,
                )
        # Shut down primary cluster to simulate permanent failure
        clients = ceph_cluster.get_ceph_objects("client")
        mds_nodes = ceph_cluster.get_ceph_objects("mds")
        mon_nodes = ceph_cluster.get_ceph_objects("mon")
        osd_nodes_list = ceph_cluster.get_ceph_objects("osd")
        unique_objects = []
        for obj in osd_nodes_list:
            if not any(object_compare(obj, u_obj) for u_obj in unique_objects):
                unique_objects.append(obj)
        osd_nodes = unique_objects

        log.info("Shutting down the cluster")
        for client in clients:
            target_node = find_vm_node_by_hostname(ceph_cluster, client.node.hostname)
            target_node.shutdown(wait=True)
        log.info("Client Nodes Powered OFF Successfully")

        for mds in mds_nodes:
            target_node = find_vm_node_by_hostname(ceph_cluster, mds.node.hostname)
            target_node.shutdown(wait=True)
        log.info("MDS Nodes Powered OFF Successfully")

        for mon in mon_nodes:
            target_node = find_vm_node_by_hostname(ceph_cluster, mon.node.hostname)
            target_node.shutdown(wait=True)
        log.info("Mon Nodes Powered OFF Successfully")

        for osd in osd_nodes:
            target_node = find_vm_node_by_hostname(ceph_cluster, osd.node.hostname)
            target_node.shutdown(wait=True)
        log.info("OSD Nodes Powered OFF Successfully")

        mirror2.promote(imagespec=imagespec, force=True)
        mirror2.wait_for_status(imagespec=imagespec, state_pattern="up+stopped")
        mirror2.benchwrite(imagespec=imagespec, io=config[pool_type].get("io_total"))
        time.sleep(60)

        # Bring up the primary cluster back
        log.info("Bringing up the primary cluster back")
        for mon in mon_nodes:
            target_node = find_vm_node_by_hostname(ceph_cluster, mon.node.hostname)
            target_node.power_on()
        log.info("Mon Nodes Powered ON Successfully")
        for osd in osd_nodes:
            target_node = find_vm_node_by_hostname(ceph_cluster, osd.node.hostname)
            target_node.power_on()
        log.info("OSD Nodes Powered OFF Successfully")
        for mds in mds_nodes:
            target_node = find_vm_node_by_hostname(ceph_cluster, mds.node.hostname)
            target_node.power_on()
        log.info("MDS Nodes Powered ON Successfully")
        for client in clients:
            target_node = find_vm_node_by_hostname(ceph_cluster, client.node.hostname)
            target_node.power_on()
        log.info("Clients Nodes Powered ON Successfully")
        out, rc = clients[0].exec_command(sudo=True, cmd="ceph -s -f json")
        cluster_info = json.loads(out)
        log.info(f"Cluster info after powering on nodes: {cluster_info}")

        mirror1.demote(imagespec=imagespec)
        mirror1.wait_for_status(imagespec=imagespec, state_pattern="up+error")
        mirror1.resync(imagespec=imagespec)
        time.sleep(100)
        mirror1.wait_for_status(imagespec=imagespec, state_pattern="up+replaying")
        mirror1.wait_for_replay_complete(imagespec=imagespec)
        mirror2.demote(imagespec=imagespec)
        mirror2.wait_for_status(imagespec=imagespec, state_pattern=state_after_demote)
        mirror1.wait_for_status(imagespec=imagespec, state_pattern=state_after_demote)
        mirror1.promote(imagespec=imagespec)
        mirror2.wait_for_status(imagespec=imagespec, state_pattern="up+replaying")
        mirror1.wait_for_status(imagespec=imagespec, state_pattern="up+stopped")
        mirror1.benchwrite(imagespec=imagespec, io=config[pool_type].get("io_total"))
        mirror1.check_data(peercluster=mirror2, imagespec=imagespec)
        return 0

    except Exception as e:
        log.exception(e)
        return 1

    finally:
        mirror1.clean_up(peercluster=mirror2, pools=[pool])


def run(ceph_cluster, **kw):
    """
    DR Use case verification - Local/Primary cluster failure - Abrupt failure - Recovery of cluster
    Args:
        **kw: test data

    Returns:
        int: The return value. 0 for success, 1 otherwise

    Test case covered -
    CEPH-9470 - DR Use case verification - Local/Primary cluster failure - Abrupt failure - Recovery of cluster
    Pre-requisites -
    Two ceph clusters with rbd mirror configured along with:
        1. 3 monitors
        2. Atleast 9 osds
        3. Atleast 1 Client

    Test Case Flow:
    1. Follow the latest official Block device Doc to configure RBD Mirroring - For Both Pool and Image Based Mirroring.
        VMs should be running on the images that get mirrored.
        With IOs running on these images, carry on the mirroring for an hour+ (with heavy IOs).
    2. Shutdown the primary cluster.
        Follow the latest official Block device Doc for failover After a Non-Orderly Shutdown.
    3. Restart the IOs on secondary images. Carry on the IOs for an hour+.
    4. While IO is going on remote/secondary cluster, bring up the local/primary cluster.
    5. Halt the IOs. Follow the latest official Block device Doc for Failback After a Non-Orderly Shutdown.
    6. After resync, demote the remote/secondary images and then promote local/primary images.
    7. Run IOs run from it and make sure mirroring is successfully being done in remote/secondary cluster.
    """
    log.info("Starting CEPH-9470")

    mirror_obj = rbd_mirror_config(**kw)

    if mirror_obj:
        log.info("Executing test on replicated pool")
        if test_9470(mirror_obj.get("rep_rbdmirror"), "rep_pool_config", **kw):
            return 1

        log.info("Executing test on ec pool")
        if test_9470(ceph_cluster, mirror_obj.get("ec_rbdmirror"), "ec_pool_config", **kw):
            return 1

    return 0
