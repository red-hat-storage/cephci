from tests.rbd.exceptions import ImageFoundError, ImageNotFoundError, RbdBaseException
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import run_fio

log = Log(__name__)


def rbd_trash(rbd, pool_type, **kw):
    """
    Enable/Disable trash and verify
    Args:
        rbd: rbd object
        pool_type: pool type (ec_pool_config or rep_pool_config)
        **kw: Test data
    """
    pool = kw["config"][pool_type]["pool"]
    image = kw["config"][pool_type]["image"]
    config = kw["config"]
    try:
        client = kw["ceph_cluster"].get_nodes(role="client")[0]
        run_fio(image_name=image, pool_name=pool, client_node=client)
        if config["enable"]:
            cmd = "ceph config set client rbd_move_to_trash_on_remove true"
            rbd.exec_cmd(cmd=cmd)
            rbd.remove_image(pool, image)
            if not rbd.trash_exist(pool, image):
                raise ImageNotFoundError(" Deleted Image not found in the Trash")
        else:
            cmd = "ceph config set client rbd_move_to_trash_on_remove false"
            rbd.exec_cmd(cmd=cmd)
            rbd.remove_image(pool, image)
            if rbd.trash_exist(pool, image):
                raise ImageFoundError(" Image is found in the Trash")
        return 0

    except RbdBaseException as error:
        print(error.message)
        return 1

    finally:
        rbd.clean_up(pools=[kw["config"][pool_type]["pool"]])


def run(**kw):
    """Verify the trash functionality.

    This module verifies trash operations

    Pre-requisites :
    We need atleast one client node with ceph-common package,
    conf and keyring files

    Test cases covered -
    1) CEPH-83573297 - Enable "rbd_move_to_trash_on_remove" config setting, delete images
    and check if they are moved to trash automatically.
    Test Case Flow
    1. Create a pool and an Image
    2. generate IO in images
    3. Enable "rbd_move_to_trash_on_remove" in client config
    4. Delete images and verify the deleted images are moved to trash automatically
    5.Repeat the above test steps on EC pool


    2) CEPH-83573296 - Disable "rbd_move_to_trash_on_remove" config setting, delete images
    and check if they are not moved to trash automatically.
    Test case Flow
    1. Create a pool and an Image
    2. generate IO in images
    3. Disable "rbd_move_to_trash_on_remove" in client config
    4. Delete Images and verify images are not moved to trash automatically
    5. Repeat the above steps on EC pool

    """
    log.info("Running Trash function")
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
