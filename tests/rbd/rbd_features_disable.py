from tests.rbd.exceptions import ImageFoundError, RbdBaseException
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import run_fio

log = Log(__name__)


def disable_image_feature(rbd, pool_type, **kw):
    """
    Disable the image features on image when image is moved to trash.
    Args:
        # feature_name: name of the feature to enable
        rbd: rbd object
        pool_type: pool type (ec_pool_config or rep_pool_config)
        **kw: Test data
    """
    pool = kw["config"][pool_type]["pool"]
    image = kw["config"][pool_type]["image"]
    feature_name = "fast-diff"
    try:
        client = kw["ceph_cluster"].get_nodes(role="client")[0]
        run_fio(image_name=image, pool_name=pool, client_node=client)

        # Move the image to trash
        if rbd.move_image_trash(pool, image):
            log.error("Moving Image to Trash is Failed")
            return 1

        out, err = rbd.disable_rbd_feature(
            pool, image, feature_name, all=True, check_ec=False
        )
        if "rbd: error opening image" not in err:
            log.error(f"{out}")
            raise ImageFoundError(
                f" The Image is available for Image feature disable: {out}"
            )
        log.info(f"{err}")

        # Restore Image from trash and try to disable feature - should succeed
        image_id = rbd.get_image_id(pool, image)
        rbd.trash_restore(pool, image_id)
        log.info("Image restored successfully from trash.")
        if rbd.disable_rbd_feature(pool, image, feature_name):
            log.error("RBD feature was not disabled successfully")
            return 1
        else:
            log.info("RBD feature was disabled successfully")

        # Verify the feature is disabled and not present in the image info
        image_load = rbd.image_info(pool, image)
        if feature_name in image_load.get("features", {}):
            log.error(
                f"Feature {feature_name} is present in the rbd info of {pool}/{image}."
            )
            return 1

        return 0

    except RbdBaseException as error:
        log.error(error.message)
        return 1

    finally:
        rbd.clean_up(pools=[kw["config"][pool_type]["pool"]])


def run(**kw):
    """Enable the image features on image, and mark for deletion. Now try to  disable features.

    Args:
        kw: test data

    Returns:
        int: The return value. 0 for success, 1 otherwise

    Pre-requisites :
    We need atleast one client node with ceph-common package,
    conf and keyring files

    Test cases covered -
    1) CEPH-11415 - Enable the image features on image, and mark for deletion. Now try to  disable features.
    Test Case Flow
    1. Create a pool and an Image - RBD Image features are enabled by default
    2. generate IO in images
    3. Move images to trash and perform disable image feature
    4. Restore image from trash and verify that image feature is able to disable successfully
    5. Repeat the above steps for EC-pool.
    """
    log.info("Running Disable RBD feature when mark for deletion ")
    rbd_obj = initial_rbd_config(**kw)
    if rbd_obj:
        if "rbd_reppool" in rbd_obj:
            log.info("Executing test on Replication pool")
            if disable_image_feature(
                rbd_obj.get("rbd_reppool"), "rep_pool_config", **kw
            ):
                return 1
        if "rbd_ecpool" in rbd_obj:
            log.info("Executing test on EC pool")
            if disable_image_feature(rbd_obj.get("rbd_ecpool"), "ec_pool_config", **kw):
                return 1
    return 0
