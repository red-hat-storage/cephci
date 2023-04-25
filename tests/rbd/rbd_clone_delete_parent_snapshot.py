"""Module to verify for parent snapshot deletion after flattening the clone.

Pre-requisites :
We need atleast one client node with ceph-common package,
conf and keyring files

Test cases covered -
1) CEPH-83573650 - Create image with Deep Flattening enabled,take snap,protect,clone,snap,
flatten the clone, unprotect the parent snap, delete the parent snap.

Test Case Flow -
1. Create a Rep pool and an Image
2. Create Snapshot and protect, and clones from it.
3. Create a snapshot of clone.
4. Flatten the clone.
5. Unprotect and delete the parent snapshot
6. Repeat above steps on EC pool
"""

from tests.rbd.exceptions import RbdBaseException
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

log = Log(__name__)


def rbd_clone_delete_parent_snap(rbd, pool_type, **kw):
    """
    Args:
        rbd: rbd object
        pool_type: pool type (ec_pool_config or rep_pool_config)
        **kw: Key/value pairs of configuration information to be used in the test
    """
    pool = kw["config"][pool_type]["pool"]
    image = kw["config"][pool_type]["image"]
    snap_1 = kw["config"][pool_type].get("snap", f"{image}_snap")
    clone_1 = kw["config"][pool_type].get("clone", f"{image}_clone")

    try:
        if rbd.snap_create(pool, image, snap_1):
            log.error(f"Snapshot with name {snap_1} creation failed for {image}")
            return 1
        snap_name_1 = f"{pool}/{image}@{snap_1}"

        if rbd.protect_snapshot(snap_name_1):
            log.error(f"Snapshot protect failed for {pool}/{image}")
            return 1
        if rbd.create_clone(snap_name_1, pool, clone_1):
            log.error(f"Clone creation failed for {pool}/{clone_1}")
            return 1

        snap_2 = clone_1 + "_snap2"
        if rbd.snap_create(pool, clone_1, snap_2):
            log.error(f"Snapshot with name {snap_2} creation failed for {clone_1}")
            return 1
        if rbd.flatten_clone(pool, clone_1):
            log.error(f"Flatten clone failed for {clone_1}")
            return 1
        # verify parent snap remove
        if rbd.unprotect_snapshot(snap_name_1):
            log.error(f"Unprotect snapshot is failed for {snap_1}")
            return 1
        if rbd.snap_remove(pool, image, snap_1):
            log.error(f"Snapshot remove is failed for {image}")
            return 1
        return 0

    except RbdBaseException as error:
        log.error(error.message)
        return 1

    finally:
        rbd.clean_up(pools=[kw["config"][pool_type]["pool"]])


def run(**kw):
    """verify for parent snap deletion after flattening the clone
    Args:
        kw: test data

    Returns:
        int: The return value. 0 for success, 1 otherwise
    """
    log.info(
        "Executing CEPH-83573650 - verify for parent snap deletion after clone flattening"
    )
    rbd_obj = initial_rbd_config(**kw)
    if rbd_obj:
        log.info("Executing test on Replication pool")
        if rbd_clone_delete_parent_snap(
            rbd_obj.get("rbd_reppool"), "rep_pool_config", **kw
        ):
            return 1
        log.info("Executing test on EC pool")
        if rbd_clone_delete_parent_snap(
            rbd_obj.get("rbd_ecpool"), "ec_pool_config", **kw
        ):
            return 1
    return 0
