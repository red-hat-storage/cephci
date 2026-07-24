from ceph.parallel import parallel
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import run_fio

log = Log(__name__)


def create_snap_and_clone(rbd, pool, image_name, snap_name, clone_name):
    """
    Create snap, protect and create clone for the given image
    Args:
        rbd: rbd object
        pool: pool name
        image_name: image name
        snap_name: snap name
        clone_name: clone name
    """
    if rbd.snap_create(pool, image_name, snap_name):
        log.error(f"Snap creation failed for {image_name}")
        return 1
    snap_spec = f"{pool}/{image_name}@{snap_name}"
    if rbd.protect_snapshot(snap_spec):
        log.error(f"Snap protect failed for {snap_spec}")
        return 1
    if rbd.create_clone(snap_spec, pool, clone_name):
        log.error(f"Clone creation failed for {clone_name}")
        return 1
    return 0


def rbd_trash_purge(rbd, pool_type, **kw):
    """
    Run trash and force remove operation on images
    Args:
        rbd: rbd object
        pool_type: pool type (ec_pool_config or rep_pool_config)
        **kw: Test data
    """
    pool = kw["config"][pool_type]["pool"]
    num_images = kw["config"].get("no_of_images", 10)
    num_clones = kw["config"].get("no_of_clones", 4)
    client = kw["ceph_cluster"].get_nodes(role="client")[0]
    images_with_clone = list()
    try:
        log.info(f"Creating {num_images} number of images in pool {pool}")

        for i in range(0, num_images):
            image_name = f"image_{i}"
            if rbd.create_image(pool, image_name, "10G"):
                log.error(f"Image creation failed for {image_name}")
                return 1
            run_fio(
                image_name=image_name, pool_name=pool, client_node=client, runtime=10
            )

        log.info(
            f"Creating snapshot and clone for {num_clones} out of {num_images} number of images"
        )

        with parallel() as p:
            for i in range(0, num_clones):
                image_name = f"image_{i}"
                images_with_clone.append(image_name)
                snap_name = f"snap_{i}"
                clone_name = f"clone_{i}"
                p.spawn(
                    create_snap_and_clone, rbd, pool, image_name, snap_name, clone_name
                )

        log.info("Moving all images created above to trash")
        for i in range(0, num_images):
            image_name = f"image_{i}"
            if rbd.move_image_trash(pool, image_name):
                log.error(f"Image {image_name} not moved to trash")
                return 1

        log.info(f"Performing trash purge on pool {pool}")
        out = rbd.trash_purge(pool)

        if "some expired images could not be removed" in str(out[1]):
            log.info("rbd trash purge was unable to purge all images in the pool")
        else:
            log.error("rbd trash purge did not work as expected")
            return 1

        for i in range(0, num_images):
            image = f"image_{i}"
            if image in images_with_clone and not rbd.trash_exist(pool, image):
                log.error(f"Trash purge removed image {image} which has a clone")
                return 1
            elif image not in images_with_clone and rbd.trash_exist(pool, image):
                log.error(
                    f"Image {image} is still present in trash, it should have been purged"
                )
                return 1

        return 0

    except Exception as error:
        log.error(error.message)
        return 1

    finally:
        rbd.clean_up(pools=[kw["config"][pool_type]["pool"]])


def run(**kw):
    """Verify the trash purge functionality.

    This module verifies trash purge operation

    Args:
        kw: test data

    Returns:
        int: The return value. 0 for success, 1 otherwise

    Pre-requisites :
    We need atleast one client node with ceph-common package,
    conf and keyring files

    Test cases covered -
    1) CEPH-83574482 - 'rbd trash purge' should purge all images without snapshots
    or clones in the pool specified
    Test Case Flow
    1. Create a pool and multiple Images
    2. generate IO in images
    3. Create clones for few of the images
    4. Move images to trash
    5. Perform rbd trash purge on the pool. Purge should work without getting stuck.
    Images without clones should get purged.
    6. Repeat the above steps for EC-pool.
    """
    log.info(
        "CEPH-83574482 - Running Trash purge test on images with and without clones"
    )
    rbd_obj = initial_rbd_config(**kw)
    if rbd_obj:
        if "rbd_reppool" in rbd_obj:
            log.info("Executing test on Replication pool")
            if rbd_trash_purge(rbd_obj.get("rbd_reppool"), "rep_pool_config", **kw):
                return 1
        if "rbd_ecpool" in rbd_obj:
            log.info("Executing test on EC pool")
            if rbd_trash_purge(rbd_obj.get("rbd_ecpool"), "ec_pool_config", **kw):
                return 1
    return 0
