import pdb
import time

from ceph.parallel import parallel
from ceph.utils import hard_reboot
from tests.rbd.rbd_utils import execute_dynamic
from tests.rbd_mirror.rbd_mirror_utils import rbd_mirror_config
from utility.log import Log

log = Log(__name__)


def test_trash_primary_image(rbd_mirror, pool_type, **kw):
    try:
        mirror1 = rbd_mirror.get("mirror1")
        mirror2 = rbd_mirror.get("mirror2")
        rbd1 = rbd_mirror.get("rbd1")
        rbd2 = rbd_mirror.get("rbd2")
        config = kw.get("config")
        pool = config[pool_type]["pool"]
        image = config[pool_type]["image"]
        imagespec = pool + "/" + image

        image_id = rbd1.get_image_id(pool, image)
        if rbd1.move_image_trash(pool, image):
            log.error(f"Moving {image} to trash failed")
            return 1
        if not rbd1.trash_exist(pool, image):
            log.error(f"Trashed Image {image} not found in the Trash")
            return 1

        mirror2.promote(imagespec=imagespec, force=True)
        mirror2.wait_for_status(imagespec=imagespec, state_pattern="up+stopped")

        results = {}

        log.info(
            "Create multiple snapshots when IO on image is in progress. (use rbd bench)"
        )
        with parallel() as p:
            p.spawn(
                execute_dynamic,
                rbd2,
                "rbd_bench",
                results,
                imagespec=imagespec,
            )
            p.spawn(
                execute_dynamic,
                rbd2,
                "create_multiple_snapshots",
                results,
                pool_name=pool,
                image_name=image,
                snap_names=f"{pool_type}_snap",
                snap_count=12,
                wait=10,
            )
        if results["create_multiple_snapshots"] or results["rbd_bench"]:
            log.error(f"Creating snapshots while writing IOs failed for {pool}/{image}")
            return 1

        log.info("Restore trashed image")
        if rbd1.trash_restore(pool, image_id):
            log.error(f"Trash restore failed for {pool}/{image}")
            return 1
        if rbd1.trash_exist(pool, image):
            log.error("Restored image found in the Trash")
            return 1

        log.info("Demote primary, resync and check if data integrity is maintained")
        mirror1.demote(imagespec=imagespec)
        mirror1.wait_for_status(imagespec=imagespec, state_pattern="up+error")
        mirror1.resync(imagespec=imagespec)
        time.sleep(100)
        mirror1.wait_for_status(imagespec=imagespec, state_pattern="up+replaying")
        mirror1.wait_for_replay_complete(imagespec=imagespec)
        mirror2.check_data(peercluster=mirror1, imagespec=imagespec)

        log.info("Get cluster mirroring back to its initial state")
        mirror2.demote(imagespec=imagespec)
        mirror2.wait_for_status(imagespec=imagespec, state_pattern="up+unknown")
        mirror1.promote(imagespec=imagespec)
        mirror1.wait_for_status(imagespec=imagespec, state_pattern="up+stopped")
        mirror2.wait_for_status(imagespec=imagespec, state_pattern="up+replaying")

        mirror2.clean_up(peercluster=mirror1, pools=[pool])
        return 0

    except Exception as e:
        log.exception(e)
        return 1


def run(**kw):
    """
    Image mirroring - Try marking the primary image for deletion. Promote secondary create snaps on secondary,
    restore primary and verify the relationship and IO is not affected.
    Args:
        **kw: test data

    Returns:
        int: The return value. 0 for success, 1 otherwise

    Test case covered -
    CEPH-9470 - Image mirroring - Try marking the primary image for deletion. Promote secondary
    create snaps on secondary, restore primary and verify the relationship and IO is not affected.
    Pre-requisites -
    Two ceph clusters with rbd mirror configured along with:
        1. 3 monitors
        2. Atleast 9 osds
        3. Atleast 1 Client

    Test Case Flow:
    1. configure image mirroring
    2. Move primary to trash. Promote --force secondary.
    3. Run IOs and Create snapshot on secondary snapshots at various points.
    4. Restore primary, demote and resync.
    """
    pdb.set_trace()
    log.info("Starting CEPH-11418")
    config = {
        "create_rbd_object": True,
        "rep_pool_config": {
            "mode": "image",
        },
        "ec_pool_config": {
            "mode": "image",
        },
    }
    if kw.get("config"):
        kw["config"].update(config)
    else:
        kw["config"] = config

    mirror_obj = rbd_mirror_config(**kw)

    if mirror_obj:
        log.info("Executing test on replicated pool")
        if test_trash_primary_image(
            mirror_obj.get("rep_rbdmirror"), "rep_pool_config", **kw
        ):
            return 1

        log.info("Executing test on ec pool")
        if test_trash_primary_image(
            mirror_obj.get("ec_rbdmirror"), "ec_pool_config", **kw
        ):
            return 1

    return 0
