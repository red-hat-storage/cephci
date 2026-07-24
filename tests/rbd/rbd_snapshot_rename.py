from random import randint

from tests.rbd.exceptions import RbdBaseException
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

log = Log(__name__)


def create_rename_multiple_snaps(rbd, pool, image, snap, snap_rename, num_of_snaps):
    """
    Creates and renames multiple snapshots of a given image based on num_of_snaps specified
    Args:
        rbd: RBD object
        pool: pool name
        image: image name
        snap: snap prefix
        snap_rename: snap rename prefix
        num_of_snaps: number of snaps to be created
    """
    for i in range(1, num_of_snaps):
        snap_name_i = f"{snap}_{i}"
        if rbd.snap_create(pool, image, snap_name_i):
            log.error(f"Creation of snapshot {snap_name_i} failed")
            return 1

    for i in range(1, num_of_snaps):
        snap_name_i = f"{snap}_{i}"
        snap_rename_i = f"{snap_rename}_{i}"
        if rbd.snap_rename(pool, image, snap_name_i, snap_rename_i):
            log.error(f"Rename of snapshot {snap_name_i} to {snap_rename_i} failed")
            return 1

    return 0


def test_snapshot_rename(rbd, pool_type, **kw):
    """
    This module tests snapshot rename functionality
    Args:
        rbd: RBD object
        pool_type: pool type (ec_pool_config or rep_pool_config)
        **kw: test data
    """
    try:
        pool = kw["config"][pool_type]["pool"]
        image = kw["config"][pool_type]["image"]
        snap = kw["config"][pool_type].get("snap", f"{image}_snap")
        clone = kw["config"][pool_type].get("clone", f"{image}_clone")
        image_spec = f"{pool}/{image}"
        snap_rename = kw["config"][pool_type].get("snap_rename", f"{image}_new_snap")

        num_of_snaps = randint(5, 20)
        log.info(
            f"Testing snapshot rename on {num_of_snaps} number of snaps created for image {image_spec}"
        )

        # Create snaps on original image and rename
        if create_rename_multiple_snaps(
            rbd, pool, image, snap, snap_rename, num_of_snaps
        ):
            return 1

        # Create a clone of the original image using one of the snaps
        snap_name = f"{pool}/{image}@{snap_rename}_1"
        if rbd.create_clone(snap_name, pool, clone):
            log.error(f"Creation of clone {clone} failed")
            return 1

        if rbd.flatten_clone(pool, clone):
            log.error(f"Clone flatten failed for {clone}")
            return 1

        # Create snapshots on clone and rename the same
        if create_rename_multiple_snaps(
            rbd, pool, clone, f"{snap}_clone", f"{snap_rename}_clone", num_of_snaps
        ):
            return 1

        return 0
    except RbdBaseException as error:
        log.error(error.message)
        return 1

    finally:
        if not kw.get("config").get("do_not_cleanup_pool"):
            rbd.clean_up(pools=[pool])


def run(**kw):
    """Verification of snap and clone on an imported image.

    This module verifies snapshot rename operations on an image and its clone

    Args:
        kw: test data

    Returns:
        int: The return value. 0 for success, 1 otherwise

    Test case covered -
    CEPH-9833 - verification rename snap on an image and its clone
    Pre-requisites :
    1. Cluster must be up and running with capacity to create pool
       (At least with 64 pgs)
    2. We need atleast one client node with ceph-common package,
       conf and keyring files

    Test Case Flow:
    1. Create a pool
    2. Create an image
    3. Create multiple snapshots of this image
    4. Rename each of the snapshots
    5. Create a clone of this image
    6. Create multiple snapshots of the cloned image
    7. Rename each of the above snapshots
    8. Repeat steps 1 to 7 for ecpool
    """
    log.info("Running rename snapshots on image and its clone")
    rbd_obj = initial_rbd_config(**kw)
    rc = 1
    if rbd_obj:
        rc = test_snapshot_rename(rbd_obj.get("rbd_reppool"), "rep_pool_config", **kw)

        if rc:
            return rc

        rc = test_snapshot_rename(rbd_obj.get("rbd_ecpool"), "ec_pool_config", **kw)

    return rc
