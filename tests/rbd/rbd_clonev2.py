from tests.rbd.exceptions import RbdBaseException
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

log = Log(__name__)


def rbd_clonev2(rbd, pool_type, **kw):
    """
    Run snap and clone operations with clonev2 function
    Args:
        rbd: rbd object
        pool_type: pool type (ec_pool_config or rep_pool_config)
        **kw: test data
    """
    pool = kw["config"][pool_type]["pool"]
    image = kw["config"][pool_type]["image"]
    snap = kw["config"][pool_type].get("snap", f"{image}_snap")
    clone = kw["config"][pool_type].get("clone", f"{image}_clone")

    try:
        if rbd.snap_create(pool, image, snap):
            log.error(f"Creation of snapshot {snap} failed")
            return 1

        snap_name = f"{pool}/{image}@{snap}"
        cmd = "ceph osd set-require-min-compat-client mimic"
        if rbd.exec_cmd(cmd=cmd):
            log.error(f"Execution of command {cmd} failed")
            return 1

        if rbd.create_clone(snap_name, pool, clone):
            log.error(f"Creation of clone {clone} failed")
            return 1

        cmd = "ceph config set client rbd_move_parent_to_trash_on_remove true"
        if rbd.exec_cmd(cmd=cmd):
            log.error(f"Execution of command {cmd} failed")
            return 1

        if rbd.snap_remove(pool, image, snap):
            log.error(f"Removal of snapshot {snap} failed")
            return 1

        if rbd.remove_image(pool, image):
            log.error(f"Removal of image {image} failed")
            return 1

        if not rbd.trash_exist(pool, image):
            log.error("Deleted Image not found in the Trash")
            return 1

        if rbd.flatten_clone(pool, clone):
            log.error(f"Flatten of clone {clone} failed")
            return 1

        if rbd.trash_exist(pool, image):
            log.error("Images are found in Trash")
            return 1
        return 0

    except RbdBaseException as error:
        print(error.message)
        return 1

    finally:
        if not kw.get("config").get("do_not_cleanup_pool"):
            rbd.clean_up(pools=[pool])


def run(**kw):
    """Verify that clones can be created without protecting snapshot(v2clone).

    This module verifies snapshot and cloning operations with using snapshot protect having v2 clone enabled

    Args:
        kw: test data

    Returns:
        int: The return value. 0 for success, 1 otherwise

    Test case covered -
    CEPH-83573308 - Verify that clones can be created without protecting snapshot
    CEPH-83573307 - Delete parent snapshot and verify whether it is moved to trash or not.
    CEPH-83573309 - Verify deletion of parent Image when clone is flattened.
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
    5. Remove parent image snapshot and check the status
    6. Flattened the clone and check the trash views
    7. Repeat the above steps for ecpool
    """
    log.info("Running snap and clone operations with v2clone function")
    log.info("executing *****")
    rbd_obj = initial_rbd_config(**kw)
    rc = 1
    if rbd_obj:
        log.info("Executing test on replicated pool")
        rc = rbd_clonev2(rbd_obj.get("rbd_reppool"), "rep_pool_config", **kw)

        if rc:
            return rc

        log.info("Executing test on ec pool")
        rc = rbd_clonev2(rbd_obj.get("rbd_ecpool"), "ec_pool_config", **kw)

    return rc
