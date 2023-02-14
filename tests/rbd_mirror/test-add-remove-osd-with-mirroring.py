from ceph.ceph_admin import CephAdmin
from ceph.ceph_admin.orch import Orch
from ceph.ceph_admin.osd import OSD
from ceph.parallel import parallel
from tests.rbd_mirror.rbd_mirror_utils import execute_dynamic, rbd_mirror_config
from utility.log import Log

log = Log(__name__)


def test_add_remove_osd_with_mirroring(rbd_mirror, pool_type, **kw):
    """
    Test to add and remove osd nodes with mirroring
    Args:
        rbd_mirror: rbd mirror object
        pool_type: ec pool or rep pool
        **kw:

    Returns:
        0 if test passes, else 1
    """
    try:
        mirror1 = rbd_mirror.get("mirror1")
        mirror2 = rbd_mirror.get("mirror2")
        config = kw.get("config")
        pool = config[pool_type]["pool"]
        image = config[pool_type]["image"]
        imagespec = pool + "/" + image

        results = {}

        orch_obj = Orch(cluster=mirror1.ceph_nodes, **config)
        osd_obj = OSD(cluster=mirror1.ceph_nodes, **config)
        ceph_admin = CephAdmin(cluster=mirror1.ceph_nodes, **config)

        log.info("Add OSDs to primary cluster with bench IOs running on mirrored image")
        with parallel() as p:
            p.spawn(
                execute_dynamic,
                mirror1,
                "benchwrite",
                results,
                imagespec=imagespec,
                io="100M",
            )
            p.spawn(
                execute_dynamic,
                "ceph.ceph_admin.helper",
                "add_remove_osd",
                results,
                command="add",
                osd_nodes=config["osds_to_add"],
                ceph_nodes=mirror1.ceph_nodes,
                orch_obj=orch_obj,
                osd_obj=osd_obj,
                ceph_admin=ceph_admin,
            )

        if results["benchwrite"] or results["add_remove_osd"]:
            log.error(f"Results of benchwrite : {results['benchwrite']}")
            log.error(f"Results of add_remove_osd : {results['add_remove_osd']}")
            log.error("Adding OSDs failed for primary cluster")
            return 1

        log.info("Check if mirror data is consistent")
        try:
            mirror1.check_data(mirror2, imagespec)
        except BaseException:
            log.error(
                "Data consistency check failed after adding OSDs to primary cluster"
            )
            return 1

        log.info(
            "Remove OSDs from primary cluster with bench IOs running on mirrored image"
        )
        with parallel() as p:
            p.spawn(
                execute_dynamic,
                mirror1,
                "benchwrite",
                results,
                imagespec=imagespec,
                io="100M",
            )
            p.spawn(
                execute_dynamic,
                "ceph.ceph_admin.helper",
                "add_remove_osd",
                results,
                command="rm",
                osd_nodes=config["osds_to_remove"],
                ceph_nodes=mirror1.ceph_nodes,
                orch_obj=orch_obj,
                osd_obj=osd_obj,
                ceph_admin=ceph_admin,
            )

        if results["benchwrite"] or results["add_remove_osd"]:
            log.error(f"Results of benchwrite : {results['benchwrite']}")
            log.error(f"Results of add_remove_osd : {results['add_remove_osd']}")
            log.error("Removing OSDs failed for primary cluster")
            return 1

        log.info("Check if mirror data is consistent")
        try:
            mirror1.check_data(mirror2, imagespec)
        except BaseException:
            log.error(
                "Data consistency check failed after removing OSDs from primary cluster"
            )
            return 1

        orch_obj = Orch(cluster=mirror2.ceph_nodes, **config)
        osd_obj = OSD(cluster=mirror2.ceph_nodes, **config)
        ceph_admin = CephAdmin(cluster=mirror2.ceph_nodes, **config)

        log.info(
            "Add OSDs to secondary cluster with bench IOs running on mirrored image"
        )
        with parallel() as p:
            p.spawn(
                execute_dynamic,
                mirror1,
                "benchwrite",
                results,
                imagespec=imagespec,
                io="100M",
            )
            p.spawn(
                execute_dynamic,
                "ceph.ceph_admin.helper",
                "add_remove_osd",
                results,
                command="add",
                osd_nodes=config["osds_to_add"],
                ceph_nodes=mirror2.ceph_nodes,
                orch_obj=orch_obj,
                osd_obj=osd_obj,
                ceph_admin=ceph_admin,
            )

        if results["benchwrite"] or results["add_remove_osd"]:
            log.error(f"Results of benchwrite : {results['benchwrite']}")
            log.error(f"Results of add_remove_osd : {results['add_remove_osd']}")
            log.error("Adding OSDs failed for secondary cluster")
            return 1

        log.info("Check if mirror data is consistent")
        try:
            mirror1.check_data(mirror2, imagespec)
        except BaseException:
            log.error(
                "Data consistency check failed after adding OSDs to secondary cluster"
            )
            return 1

        log.info(
            "Remove OSDs from secondary cluster with bench IOs running on mirrored image"
        )
        with parallel() as p:
            p.spawn(
                execute_dynamic,
                mirror1,
                "benchwrite",
                results,
                imagespec=imagespec,
                io="100M",
            )
            p.spawn(
                execute_dynamic,
                "ceph.ceph_admin.helper",
                "add_remove_osd",
                results,
                command="rm",
                osd_nodes=config["osds_to_remove"],
                ceph_nodes=mirror2.ceph_nodes,
                orch_obj=orch_obj,
                osd_obj=osd_obj,
                ceph_admin=ceph_admin,
            )

        if results["benchwrite"] or results["add_remove_osd"]:
            log.error(f"Results of benchwrite : {results['benchwrite']}")
            log.error(f"Results of add_remove_osd : {results['add_remove_osd']}")
            log.error("Removing OSDs failed for secondary cluster")
            return 1

        log.info("Check if mirror data is consistent")
        try:
            mirror1.check_data(mirror2, imagespec)
        except BaseException:
            log.error(
                "Data consistency check failed after removing OSDs from secondary cluster"
            )
            return 1
        return 0

    except Exception as e:
        log.exception(e)
        return 1

    finally:
        mirror1.clean_up(peercluster=mirror2, pools=[pool])


def run(**kw):
    """
    Add remove osd with mirroring - local/primary cluster and remote/secondary cluster
    Args:
        **kw: test data

    Returns:
        int: The return value. 0 for success, 1 otherwise

    Test case covered -
    CEPH-9489 - Add remove osd with mirroring - local/primary cluster and remote/secondary cluster
    Pre-requisites -
    Two ceph clusters with rbd mirror configured along with:
        1. 3 monitors
        2. Atleast 9 osds
        3. Atleast 1 Client
        4. 2 nodes dedicated to be added as OSDs.

    Test Case Flow:
    1. Follow the latest official Block device Doc to configure RBD Mirroring - For Image Based Mirroring.
        VMs should be running on the images that get mirrored.
    2. Add 2 new osd nodes to the primary cluster, while running IOs on RBD mirrored images
    3. Remove 2 existing osd nodes from the primary cluster, while running IOs on RBD mirrored images
    4. Add 2 new osd nodes to the secondary cluster, while running IOs on RBD mirrored images
    5. Remove 2 existing osd nodes from the secondary cluster, while running IOs on RBD mirrored images
    """
    log.info("Starting CEPH-9489")

    mirror_obj = rbd_mirror_config(**kw)

    if mirror_obj:
        log.info("Executing test on replicated pool")
        if test_add_remove_osd_with_mirroring(
            mirror_obj.get("rep_rbdmirror"), "rep_pool_config", **kw
        ):
            return 1

        log.info("Executing test on ec pool")
        if test_add_remove_osd_with_mirroring(
            mirror_obj.get("ec_rbdmirror"), "ec_pool_config", **kw
        ):
            return 1

    return 0
