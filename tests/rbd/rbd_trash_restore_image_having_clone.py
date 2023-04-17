from time import sleep

from ceph.parallel import parallel
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import run_fio

log = Log(__name__)


def move_to_trash_and_restore(rbd, pool, image, sleep_time):
    """
    Move given image to trash, sleep for given time and restore image back from trash

    Args:
        rbd: RBD object
        pool: pool in which image is present
        image: image to be trashed
        sleep_time: seconds to sleep before restoring
    """
    if rbd.move_image_trash(pool, image):
        log.error(f"Moving image {image} to trash failed")
        raise Exception(f"Moving image {image} to trash failed")

    image_id = rbd.get_image_id(pool, image)

    if not rbd.trash_exist(pool, image):
        log.error("Image is not found in the Trash")
        raise Exception("Image is not found in the Trash")
    else:
        log.info(f"Image {image} moved to trash")

    sleep(sleep_time)

    if rbd.trash_restore(pool, image_id):
        log.error(f"Trash restore failed for image {image}")
        raise Exception(f"Trash restore failed for image {image}")
    return 0


def rbd_trash_restore_image_having_clone(rbd, pool_type, **kw):
    """
    Verify parent image moved to trash and restored back when IO running on clone
    and parent clone relationship still active.
    Args:
        rbd: rbd object
        pool_type: pool type (ec_pool_config or rep_pool_config)
        **kw: Key/value pairs of configuration information to be used in the test
    """
    pool = kw["config"][pool_type]["pool"]
    image = kw["config"][pool_type]["image"]
    snap = kw["config"][pool_type].get("snap", f"{image}_snap")
    clone = kw["config"][pool_type].get("clone", f"{image}_clone")
    try:
        # Create snap and clone
        rbd.snap_create(pool, image, snap)
        snap_name_1 = f"{pool}/{image}@{snap}"
        rbd.protect_snapshot(snap_name_1)
        rbd.create_clone(snap_name_1, pool, clone)

        log.info(
            f"Checking parent-clone relationship for parent: {image}, clone: {clone}"
        )
        rbd_info = rbd.image_info(pool, clone)
        if (
            rbd_info.get("parent").get("image") == image
            and rbd_info.get("parent").get("snapshot") == snap
            and not rbd_info.get("parent").get("trash")
        ):
            log.info(
                f"Parent-clone {image}-{clone} relationship is intact and parent is not in trash"
            )
        else:
            log.error(f"Parent-clone {image}-{clone} relationship broken")
            return 1

        rbd_du = rbd.get_disk_usage_for_pool(pool, image=clone)
        size_before_io = rbd_du["images"][0]["used_size"]
        log.info(f"Clone size before IO : {size_before_io}")

        client = kw["ceph_cluster"].get_nodes(role="client")[0]
        log.info("Move parent image to trash when IO on Clone is in progress.")
        with parallel() as p:
            p.spawn(
                run_fio,
                image_name=clone,
                pool_name=pool,
                client_node=client,
                size="1G",
            )
            p.spawn(
                move_to_trash_and_restore,
                rbd=rbd,
                pool=pool,
                image=image,
                sleep_time=60,
            )

        rbd_du = rbd.get_disk_usage_for_pool(pool, image=clone)
        size_after_io = rbd_du["images"][0]["used_size"]
        log.info(f"Clone size after IO : {size_after_io}")

        written = int(
            (size_after_io - size_before_io) / 1073741824
        )  # 1GB = 1073741824 bytes
        if written != 1:
            log.error(
                f"1GB of IOs were intended to be written on clone but only {written}GB"
                "were written, so there might have been some interruption in IO"
            )
            return 1

        rbd_info = rbd.image_info(pool, clone)
        if (
            rbd_info.get("parent").get("image") == image
            and rbd_info.get("parent").get("snapshot") == snap
            and not rbd_info.get("parent").get("trash")
        ):
            log.info(
                f"Parent-clone {image}-{clone} relationship is intact and parent is restored from trash"
            )
        else:
            log.error(f"Parent-clone {image}-{clone} relationship broken")
            return 1

        return 0
    except Exception as error:
        log.error(str(error))
        return 1

    finally:
        rbd.clean_up(pools=[kw["config"][pool_type]["pool"]])


def run(**kw):
    """Verify parent image moved to trash and restored back when IO running on clone
    and parent clone relationship still active.

    Args:
        kw: test data

    Returns:
        int: The return value. 0 for success, 1 otherwise

    Pre-requisites :
    We need atleast one client node with ceph-common package,
    conf and keyring files

    Test cases covered -
    1) CEPH-11414 - In a parent clone relationship, move parent image to trash.
    Dont sever the child image from parent, but try to restore the parent, and
    verify the clone relationship is intact. (Keep IO in progress on clone image)
    Test Case Flow
    1. Create pool, image and clone the image. Start IOs on clone.
    2. Move the parent image to trash.
    3. Restore the parent  image back
    4. Check parent-clone relationship.(Do rbd info on clone)
    """
    log.info(
        "Executing test CEPH-11414 - Trash restore parent image while IOs on clone are in progress..."
    )
    rbd_obj = initial_rbd_config(**kw)
    if rbd_obj:
        log.info("Executing test on Replication pool")
        if rbd_trash_restore_image_having_clone(
            rbd_obj.get("rbd_reppool"), "rep_pool_config", **kw
        ):
            return 1
        log.info("Executing test on EC pool")
        if rbd_trash_restore_image_having_clone(
            rbd_obj.get("rbd_ecpool"), "ec_pool_config", **kw
        ):
            return 1
    return 0
