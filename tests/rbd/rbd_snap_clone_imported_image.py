from tests.rbd.exceptions import RbdBaseException
from tests.rbd.rbd_utils import Rbd
from utility.log import Log

log = Log(__name__)


def run(**kw):
    """Verification of snap and clone on an imported image.

    This module verifies snapshot and cloning operations on an imported image

    Args:
        kw: test data

    Returns:
        int: The return value. 0 for success, 1 otherwise

    Test case covered -
    CEPH-9230 - verification snap and clone on an imported image
    Pre-requisites :
    1. Cluster must be up and running with capacity to create pool
       (At least with 64 pgs)
    2. We need atleast one client node with ceph-common package,
       conf and keyring files

    Test Case Flow:
    1. Create a pool
    2. Create a dummy file, import it as an image
    3. Create snapshot of it and protect the snapshot
    4. Create a clone
    """
    log.info("Running snap and clone operations on imported image")
    rbd = Rbd(**kw)
    pool = rbd.random_string()
    image = rbd.random_string()
    snap = rbd.random_string()
    dir_name = rbd.random_string()
    clone = rbd.random_string()

    rbd.exec_cmd(cmd="mkdir {}".format(dir_name))
    try:
        if not rbd.create_pool(poolname=pool):
            # create pool does not catch exceptions, it returns true/false.
            # so we are returning 1 instead of raising exception
            return 1

        rbd.create_file_to_import(filename="dummy_file")
        rbd.import_file("dummy_file", pool, image)
        rbd.snap_create(pool, image, snap)
        snap_name = f"{pool}/{image}@{snap}"
        rbd.protect_snapshot(snap_name)
        rbd.create_clone(snap_name, pool, clone)
    except RbdBaseException as error:
        print(error.message)

    finally:
        rbd.clean_up(pools=[pool])
        return rbd.flag
