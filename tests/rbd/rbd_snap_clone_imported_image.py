from tests.rbd.exceptions import RbdBaseException
from tests.rbd.rbd_utils import Rbd, initial_rbd_config
from utility.log import Log

log = Log(__name__)


def test_snap_clone(rbd, pool_type, **kw):
    """
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
    5. perform same test on EC pool
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
            log.error(f"Pool creation failed for pool {pool}")
            return 1
        rbd.create_file_to_import(filename="dummy_file")
        rbd.import_file("dummy_file", pool, image)
        rbd.snap_create(pool, image, snap)
        snap_name = f"{pool}/{image}@{snap}"
        rbd.protect_snapshot(snap_name)
        rbd.create_clone(snap_name, pool, clone)
        return 0

    except RbdBaseException as error:
        log.error(error)
        return 1

    finally:
        if not kw.get("config").get("do_not_cleanup_pool"):
            rbd.clean_up(pools=[pool])


def run(**kw):
    """
    This module verifies snapshot and cloning operations on an imported image
    on EC pool and Replicated pool.

    Args:
        kw: test data

    Returns:
        int: The return value. 0 for success, 1 otherwise
    """
    rbd_obj = initial_rbd_config(**kw)
    if rbd_obj:
        # To run test on EC pool config
        log.info(
            "Running snapshot and cloning operations on an imported image in EC pool"
        )
        if test_snap_clone(rbd_obj.get("rbd_ecpool"), "ec_pool_config", **kw):
            return 1

        # To run test on replicated pool
        log.info(
            "Running snapshot and cloning operations on an imported image in replicated pool"
        )
        if test_snap_clone(rbd_obj.get("rbd_reppool"), "rep_pool_config", **kw):
            return 1
        return 0
