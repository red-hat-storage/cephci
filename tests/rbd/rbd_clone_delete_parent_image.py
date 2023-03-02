from ceph.parallel import parallel
from tests.rbd.exceptions import RbdBaseException
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

log = Log(__name__)


def rbd_clone_delete_parent_image(rbd, pool_type, **kw):
    """
    verify for parent image deletion after flattening the clone and removng snap
    Args:
        rbd: rbd object
        pool_type: pool type (ec_pool_config or rep_pool_config)
        **kw: Key/value pairs of configuration information to be used in the test
    """
    pool = kw["config"][pool_type]["pool"]
    image = kw["config"][pool_type]["image"]
    snap_1 = kw["config"][pool_type].get("snap", f"{image}_snap")
    clone_1 = kw["config"][pool_type].get("clone", f"{image}_clone")
    config = kw.get("config")
    try:
        # Create multilevel snap and clone
        snap_created = []
        clone_created = []
        rbd.snap_create(pool, image, snap_1)
        snap_name_1 = f"{pool}/{image}@{snap_1}"
        snap_created.append(snap_1)
        rbd.protect_snapshot(snap_name_1)
        rbd.create_clone(snap_name_1, pool, clone_1)
        clone_created.append(clone_1)

        # create clone from cloned image
        snap_2 = clone_1 + "_snap2"
        clone_2 = clone_1 + "_clone2"
        rbd.snap_create(pool, image, snap_2)
        snap_name_2 = f"{pool}/{image}@{snap_2}"
        snap_created.append(snap_2)
        rbd.protect_snapshot(snap_name_2)
        rbd.create_clone(snap_name_2, pool, clone_2)
        clone_created.append(clone_2)

        # move parent image to trash and flatten the clone and verify the clone flattening
        rbd.move_image_trash(pool, image)
        image_id = rbd.get_image_id(pool, image)
        log.info(f"image id is {image_id}")
        for clone in clone_created:
            rbd.flatten_clone(pool, clone)

        # unprotect and delete the snap and verify snap deletion
        for snap in snap_created:
            rbd.unprotect_snapshot(snap, pool=pool, image_id=image_id)
            rbd.snap_remove(pool, image, snap, image_id=image_id)

        # Restore image from trash
        rbd.trash_restore(pool, image_id)
        # while Running IOs on the image try to delete the image
        with parallel() as p:
            p.spawn(
                rbd.exec_cmd,
                cmd=f"rbd bench --io-type write --io-total {config.get('io-total')} {pool}/{image}",
            )
            p.spawn(rbd.remove_image, pool, image)

        # move image to trash and delete permanantly and verify trash
        rbd.move_image_trash(pool, image)
        image_id = rbd.get_image_id(pool, image)
        log.info(f"image id is {image_id}")
        rbd.remove_image_trash(pool, image_id)
        if rbd.trash_exist(pool, image):
            log.error(" Image is found in the Trash")
            return 1
        else:
            log.info("Image is removed from the trash")
            return 0

    except RbdBaseException as error:
        log.error(error.message)
        return 1

    finally:
        rbd.clean_up(pools=[kw["config"][pool_type]["pool"]])


def run(**kw):
    """verify for parent image deletion after flattening the clone and removing snap.

    Args:
        kw: test data

    Returns:
        int: The return value. 0 for success, 1 otherwise

    Pre-requisites :
    We need atleast one client node with ceph-common package,
    conf and keyring files

    Test cases covered -
    1) CEPH-11409 Delayed Deletion - Create a clone of a RBD image,
    and then try to delete the parent image while the relationship is active.
    Create snaps on it, and clones from it.
    Test Case Flow
    1. Create a Rep pool and an Image
    2. Create multi level snaps on it, and clones from it.
    3. Move parent images to trash
    4. Flatten the clone when parent image is in trash
    5. Unprotect and delete the snap while the image is in trash.(use --image-id)
    6. Restore the image from the trash. Start IOs on the image using rbd bench.
    7. While IOs is going on delete the image.
    8. Stop IOs and then move the image to trash and delete the image permanently.
    """
    rbd_obj = initial_rbd_config(**kw)
    if rbd_obj:
        log.info("Executing test on Replication pool")
        if rbd_clone_delete_parent_image(
            rbd_obj.get("rbd_reppool"), "rep_pool_config", **kw
        ):
            return 1
        log.info("Executing test on EC pool")
        if rbd_clone_delete_parent_image(
            rbd_obj.get("rbd_ecpool"), "ec_pool_config", **kw
        ):
            return 1
    return 0
