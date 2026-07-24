from tests.rbd.exceptions import RbdBaseException
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import run_fio

log = Log(__name__)


def trash_restore_creat_clone_snap(rbd, pool_type, **kw):
    """
    Run move image to trash and restore back the image from trash
    perform clone and snap operation on images
    Args:
        rbd: rbd object
        pool_type: pool type (ec_pool_config or rep_pool_config)
        **kw: Test data
    """
    pool = kw["config"][pool_type]["pool"]
    image = kw["config"][pool_type]["image"]
    snap = kw["config"][pool_type].get("snap", f"{image}_snap")
    clone = kw["config"][pool_type].get("clone", f"{image}_clone")
    try:
        client = kw["ceph_cluster"].get_nodes(role="client")[0]
        run_fio(image_name=image, pool_name=pool, client_node=client)
        rbd.move_image_trash(pool, image)
        image_id = rbd.get_image_id(pool, image)
        log.info(f"image id is {image_id}")
        rbd.trash_restore(pool, image_id)
        rbd.snap_create(pool, image, snap)
        snap_name = f"{pool}/{image}@{snap}"
        rbd.protect_snapshot(snap_name)
        rbd.create_clone(snap_name, pool, clone)
        return 0

    except RbdBaseException as error:
        log.error(error.message)
        return 1

    finally:
        rbd.clean_up(pools=[kw["config"][pool_type]["pool"]])


def run(**kw):
    """Verify the trash restore and create snap and clone functionality.

    This module verifies trash operations

    Args:
        kw: test data

    Returns:
        int: The return value. 0 for success, 1 otherwise

    Pre-requisites :
    We need atleast one client node with ceph-common package,
    conf and keyring files

    Test cases covered -
    1) CEPH-11413 - Delayed Deletion - In a time based deletion,
    restore an image which is marked for deletion.
    Verify if this image can be used as a normal image after.
    Create snaps on it, and clones from it.
    Test Case Flow
    1. Create a Rep pool and an Image
    2. generate IO in images
    3. Move images to trash
    4. Restore the moved image
    5. Create snaps and clones on this image.
    5. Repeat the above steps for EC-pool.
    """
    log.info("Running Trash image restore and create snap and clone on restored image ")
    rbd_obj = initial_rbd_config(**kw)
    if rbd_obj:
        log.info("Executing test on Replication pool")
        if trash_restore_creat_clone_snap(
            rbd_obj.get("rbd_reppool"), "rep_pool_config", **kw
        ):
            return 1
        log.info("Executing test on EC pool")
        if trash_restore_creat_clone_snap(
            rbd_obj.get("rbd_ecpool"), "ec_pool_config", **kw
        ):
            return 1
    return 0
