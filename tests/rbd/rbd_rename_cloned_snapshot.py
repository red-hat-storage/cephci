from random import randint

from tests.rbd.exceptions import RbdBaseException
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

log = Log(__name__)


def test_snapshot_rename_clone(rbd, pool_type, **kw):
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

        # Create snaps on original image
        snaps_created = []
        for i in range(1, num_of_snaps):
            snap_name_i = f"{snap}_{i}"
            snaps_created.append(snap_name_i)
            if rbd.snap_create(pool, image, snap_name_i):
                log.error(f"Creation of snapshot {snap_name_i} failed")
                return 1

        # List all snapshots created
        listed_snaps = rbd.snap_ls(pool, image)
        if not len(listed_snaps) == num_of_snaps - 1:
            log.error(
                f"Number of snapshots created {num_of_snaps} is not equal to number listed {len(listed_snaps)}"
            )
            return 1
        snaps_listed = [snap["name"] for snap in listed_snaps]
        snap_not_present = [snap for snap in snaps_created if snap not in snaps_listed]
        if snap_not_present:
            log.error(
                f"Snaps created {snaps_created} mismatches with snaps listed {snaps_listed}"
            )
            return 1

        for i in range(1, num_of_snaps):
            snap_name = f"{pool}/{image}@{snap}_{i}"
            clone_name = f"{clone}_{i}"
            if rbd.create_clone(snap_name, pool, clone_name):
                log.error(f"Creation of clone {clone_name} failed")
                return 1

            if rbd.snap_rename(pool, image, f"{snap}_{i}", f"{snap_rename}_{i}"):
                log.error(
                    f"Rename failed for the snapshot {snap}_{i} holding the clone {clone_name}"
                )
                return 1
            else:
                log.info(
                    f"Snapshot {snap}_{i} holding the clone {clone_name} was renamed"
                )
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
    CEPH-9835 - verification rename snap of a cloned image
    Pre-requisites :
    1. Cluster must be up and running with capacity to create pool
       (At least with 64 pgs)
    2. We need atleast one client node with ceph-common package,
       conf and keyring files

    Test Case Flow:
    1. Create a pool
    2. Create an image
    3. Create multiple snapshots of this image
    4. Create a clone of this image using on of the snapshots
    5. Rename the snapshot which was used to clone
    """
    log.info("Running rename snapshot on snapshot used to create clone")
    rbd_obj = initial_rbd_config(**kw)
    rc = 1
    if rbd_obj:
        rc = test_snapshot_rename_clone(
            rbd_obj.get("rbd_reppool"), "rep_pool_config", **kw
        )

        if rc:
            return rc

        rc = test_snapshot_rename_clone(
            rbd_obj.get("rbd_ecpool"), "ec_pool_config", **kw
        )

    return rc
