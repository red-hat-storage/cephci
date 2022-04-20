from tests.rbd.exceptions import RbdBaseException
from tests.rbd.rbd_utils import Rbd
from utility.log import Log

log = Log(__name__)


def run(**kw):
    """Verify that clones can be created without protecting snapshot(v2clone).

    This module verifies snapshot and cloning operations with using snapshot protect having v2 clone enabled

    Args:
        kw: test data

    Returns:
        int: The return value. 0 for success, 1 otherwise

    Test case covered -
    CEPH-83573308 - Verify that clones can be created without protecting snapshot
    Pre-requisites :
    1. Cluster must be up and running with capacity to create pool
       (At least with 64 pgs)
    2. We need atleast one client node with ceph-common package,
       conf and keyring files

    Test Case Flow:
    1. Create a pool and an Image
    2. Create snapshot for Image
    3. make v2clone default  - ceph osd set-require-min-compat-client mimic
    4. Create clone for an image which is having snapshots
    """
    log.info("Running snap and clone operations with v2clone function")
    rbd = Rbd(**kw)
    pool = rbd.random_string()
    image = rbd.random_string()
    snap = rbd.random_string()
    clone = rbd.random_string()
    size = "1G"

    try:
        if not rbd.create_pool(poolname=pool):
            # create pool does not catch exceptions, it returns true/false.
            # so we are returning 1 instead of raising exception
            return 1
        rbd.create_image(pool_name=pool, image_name=image, size=size)
        rbd.snap_create(pool, image, snap)
        snap_name = f"{pool}/{image}@{snap}"
        cmd = "ceph osd set-require-min-compat-client mimic"
        rbd.exec_cmd(cmd=cmd)
        rbd.create_clone(snap_name, pool, clone)
        cmd = "ceph config set client rbd_move_to_trash_on_remove true"
        rbd.exec_cmd(cmd=cmd)
        rbd.remove_image(pool, image)
        rbd.trash(pool, image)

    except RbdBaseException as error:
        print(error.message)

    finally:
        rbd.clean_up(pools=[pool])
        return rbd.flag
