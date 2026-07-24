import re

from ceph.parallel import parallel
from tests.rbd.exceptions import RbdBaseException
from tests.rbd.rbd_utils import execute_dynamic, initial_rbd_config
from utility.log import Log

log = Log(__name__)


def test_snapshot_rename_advanced(rbd, pool_type, **kw):
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
        snap = kw["config"][pool_type].get("snap", f"{pool}_{image}_snap")
        clone = kw["config"][pool_type].get("clone", f"{pool}_{image}_clone")
        size = kw["config"][pool_type]["size"]
        size_int = int(re.findall(r"\d+", size)[0])
        snap_rename = kw["config"][pool_type].get(
            "snap_rename", f"{pool}_{image}_new_snap"
        )
        snap_name = f"{pool}/{image}@{snap}"

        if rbd.snap_create(pool, image, snap):
            log.error(f"Creation of snapshot {snap} failed")
            return 1

        if rbd.protect_snapshot(snap_name):
            log.error(f"Snapshot protect failed for {snap_name}")
            return 1

        log.info("Create a clone of the original image using one of the snaps")
        snap_name = f"{pool}/{image}@{snap}"
        if rbd.create_clone(snap_name, pool, clone):
            log.error(f"Creation of clone {clone} failed")
            return 1

        resize_int = size_int + 2
        resize = f"{resize_int}{size.replace(str(size_int), '')}"

        results = {}

        log.info("Rename the snapshot when IO on Clone is in progress. (use rbd bench)")
        with parallel() as p:
            image_spec = f"{pool}/{clone}"
            p.spawn(
                execute_dynamic,
                rbd,
                "rbd_bench",
                results,
                imagespec=image_spec,
            )
            p.spawn(
                execute_dynamic,
                rbd,
                "snap_rename",
                results,
                pool_name=pool,
                image_name=image,
                current_snap_name=snap,
                new_snap_name=snap_rename,
            )

        if results["snap_rename"] or results["rbd_bench"]:
            log.error(f"Renaming snapshot while writing IOs failed for {snap_name}")
            return 1

        log.info(
            "Rename the snapshot, when resize operation is happening on Clone image."
        )
        with parallel() as p:
            p.spawn(
                execute_dynamic,
                rbd,
                "image_resize",
                results,
                pool_name=pool,
                image_name=clone,
                size=resize,
            )
            p.spawn(
                execute_dynamic,
                rbd,
                "snap_rename",
                results,
                pool_name=pool,
                image_name=image,
                current_snap_name=snap_rename,
                new_snap_name=f"{snap_rename}_1",
            )

        if results["snap_rename"] or results["image_resize"]:
            log.error(
                f"Renaming snapshot failed for {snap_name} while performing resize on clone {clone}"
            )
            return 1

        log.info("Rename a snapshot, and then create a clone on top of it.")
        clone_2 = f"{clone}_2"
        snap_name = f"{pool}/{image}@{snap_rename}_1"
        if rbd.create_clone(snap_name, pool, clone_2):
            log.error(f"Creation of clone {clone_2} failed")
            return 1

        log.info(
            "Rename the Parent snapshot, when Flatten operation is happening on Clone image."
        )
        with parallel() as p:
            p.spawn(
                execute_dynamic,
                rbd,
                "flatten_clone",
                results,
                pool_name=pool,
                image_name=clone_2,
            )
            p.spawn(
                execute_dynamic,
                rbd,
                "snap_rename",
                results,
                pool_name=pool,
                image_name=image,
                current_snap_name=f"{snap_rename}_1",
                new_snap_name=f"{snap_rename}_2",
            )

        if results["snap_rename"] or results["flatten_clone"]:
            log.error(
                f"Renaming snapshot failed for {snap_name} while performing flatten on clone {clone_2}"
            )
            return 1
        return 0
    except RbdBaseException as error:
        log.error(error.message)
        return 1

    finally:
        if not kw.get("config").get("do_not_cleanup_pool"):
            rbd.clean_up(pools=[pool])


def run(**kw):
    """Rename the snapshot when different operations on the clone/parent image is in progress.
    This module verifies snapshot rename when different operations on the clone/parent image is in progress.
    Args:
        kw: test data
    Returns:
        int: The return value. 0 for success, 1 otherwise
    Test case covered -
    CEPH-9836 - Rename the snapshot when different operations on the clone/parent image is in progress.
    Pre-requisites :
    1. Cluster must be up and running with capacity to create pool
       (At least with 64 pgs)
    2. We need atleast one client node with ceph-common package,
       conf and keyring files
    Test Case Flow:
    1. Create a pool
    2. Create an image
    3. Create a snapshot of this image
    4. Rename the snapshot when IO on Clone is in progress. (use rbd bench)
    5. Rename the snapshot, when resize operation is happening on Clone image.
    6. Rename a snapshot, and then create a clone on top of it.
    7. Rename the Parent snapshot, when Flatten operation is happening on Clone image.
    8. Repeat steps 1 to 7 for ecpool
    """
    log.info(
        "Running rename snapshots when operations on clone/parent image is in progress"
    )
    rbd_obj = initial_rbd_config(**kw)
    rc = 1
    if rbd_obj:
        rc = test_snapshot_rename_advanced(
            rbd_obj.get("rbd_reppool"), "rep_pool_config", **kw
        )

        if rc:
            return rc

        rc = test_snapshot_rename_advanced(
            rbd_obj.get("rbd_ecpool"), "ec_pool_config", **kw
        )

    return rc
