from tests.rbd.exceptions import ImageFoundError, ImageNotFoundError, RbdBaseException
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import run_fio

log = Log(__name__)


def rbd_trash(rbd, pool_type, **kw):
    """
    restore trash and verify
    Args:
        rbd: rbd object
        pool_type: pool type (ec_pool_config or rep_pool_config)
        **kw: Test data
    """
    pool = kw["config"][pool_type]["pool"]
    image = kw["config"][pool_type]["image"]

    try:
        client = kw["ceph_cluster"].get_nodes(role="client")[0]
        run_fio(image_name=image, pool_name=pool, client_node=client)
        cmd = "ceph config set client rbd_move_to_trash_on_remove true"
        rbd.exec_cmd(cmd=cmd)
        rbd.remove_image(pool, image)
        if not rbd.trash_exist(pool, image):
            raise ImageNotFoundError(" Deleted Image not found in the Trash")
        image_id = rbd.get_image_id(pool, image)
        log.info(f"image id is {image_id}")
        rbd.trash_restore(pool, image_id)
        if rbd.trash_exist(pool, image):
            raise ImageFoundError("Restored image found in the Trash")

        return 0

    except RbdBaseException as error:
        print(error.message)
        return 1

    finally:
        rbd.clean_up(pools=[kw["config"][pool_type]["pool"]])


def run(**kw):
    """Verify the trash restore functionality.

    This module verifies trash restore operations

    Pre-requisites :
    We need at least one client node with ceph-common package,
    conf and keyring files

    Test case covered -
    CEPH-83573298 - Move images to trash, restore them and verify
    Test Case Flow
    1. Create a pool and an Image
    2. generate IO in images
    3. Move the images to trash and check whether images are in trash or not
    4. Undo images from trash and check the image info.

    """
    log.info("Running Trash restore function")
    rbd_obj = initial_rbd_config(**kw)
    rc = 1
    if rbd_obj:
        log.info("Executing test on Replication pool")
        rc = rbd_trash(rbd_obj.get("rbd_reppool"), "rep_pool_config", **kw)
        if rc:
            return rc
        log.info("Executing test on EC pool")
        rc = rbd_trash(rbd_obj.get("rbd_ecpool"), "ec_pool_config", **kw)
    return rc
