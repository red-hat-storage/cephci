from tests.rbd.exceptions import ImageFoundError, RbdBaseException
from tests.rbd.rbd_utils import Rbd
from utility.log import Log
from utility.utils import run_fio

log = Log(__name__)


def run(**kw):
    """Verify the trash functionality.

    This module verifies trash operations

    Args:
        kw: test data

    Returns:
        int: The return value. 0 for success, 1 otherwise

    Pre-requisites :
    We need atleast one client node with ceph-common package,
    conf and keyring files

    Test cases covered -
    1) CEPH-83573289 - Move images to trash, apply force remove on some of the images
    and check them, if they are removed from trash
    Test Case Flow
    1. Create a pool and an Image
    2. generate IO in images
    3. Move images to trash
    4. Remove/Delete image from trash and verify that images is deleted from trash
    """
    log.info("Running Trash image remove test")
    rbd = Rbd(**kw)
    pool = rbd.random_string()
    image = rbd.random_string()
    size = "10G"
    client = kw["ceph_cluster"].get_nodes(role="client")[0]
    try:
        # create rep pool and execute the test work flow
        rbd.initial_rbd_config(rbd, pool, image, size=size)
        client = kw["ceph_cluster"].get_nodes(role="client")[0]
        run_fio(image_name=image, pool_name=pool, client_node=client)
        rbd.move_image_trash(pool, image)
        image_id = rbd.get_image_id(pool, image)
        log.info(f"image id is {image_id}")
        rbd.remove_image_trash(pool, image_id)
        if rbd.trash_exist(pool, image):
            raise ImageFoundError(" Image is found in the Trash")
        else:
            return 0

    except RbdBaseException as error:
        print(error.message)
        return 1

    finally:
        rbd.clean_up(pools=[pool])
