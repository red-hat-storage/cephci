"""Verify there is no name collision with a trashed image.
This module verifies trash operations

Pre-requisites :

We need atleast one client node with ceph-common package,
conf and keyring files

Test cases covered -

CEPH-11393 - Delayed Deletion - Verify there is no name collision with a trashed image

Test case flow -
1. Create a Rep pool and an Image
2. generate IO in images
3. Move images to trash
4. Create a same(trashed image) name image again.
4. Restore the moved image in trash
5. verify restore failes with message as
'an image with the same name already exists,
try again with with a different name'
6. Repeat the above steps for EC-pool.
"""

from tests.rbd.exceptions import RbdBaseException
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import run_fio

log = Log(__name__)


def trash_restore_same_name_image(rbd, pool_type, **kw):
    """
    Run move image to trash and restore back the image from trash with same name
        Args:
        rbd: rbd object
        pool_type: pool type (ec_pool_config or rep_pool_config)
        **kw: Test data
    """
    pool = kw["config"][pool_type]["pool"]
    image = kw["config"][pool_type]["image"]

    try:
        # run ios on image and move image to trash
        client = kw["ceph_cluster"].get_nodes(role="client")[0]
        run_fio(image_name=image, pool_name=pool, client_node=client)
        rbd.move_image_trash(pool, image)

        # Create a image with same name as moved to trash
        rbd.create_image(pool, image, "10G")

        # Restore the image from trash
        image_id = rbd.get_image_id(pool, image)
        out, err = rbd.trash_restore(pool, image_id, all=True, check_ec=False)
        if "an image with the same name already exists" in err:
            log.info("As expected Restoration of image with same name is restricted")
            return 0
        log.error(err)
        return 1

    except RbdBaseException as error:
        log.error(error.message)
        return 1

    finally:
        rbd.clean_up(pools=[kw["config"][pool_type]["pool"]])


def run(**kw):
    """verifies the image restoration is block from trash,
    when image with same name already present.

    Args:
        kw: test data

    Returns:
        int: The return value. 0 for success, 1 otherwise

    """
    log.info("CEPH-11393 - Running Trash image restore with same name")
    rbd_obj = initial_rbd_config(**kw)
    if rbd_obj:
        log.info("Executing test on Replication pool")
        if trash_restore_same_name_image(
            rbd_obj.get("rbd_reppool"), "rep_pool_config", **kw
        ):
            return 1
        log.info("Executing test on EC pool")
        if trash_restore_same_name_image(
            rbd_obj.get("rbd_ecpool"), "ec_pool_config", **kw
        ):
            return 1
    return 0
