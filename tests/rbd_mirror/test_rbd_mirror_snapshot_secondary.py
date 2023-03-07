import pdb

from tests.rbd_mirror.rbd_mirror_utils import rbd_mirror_config
from utility.log import Log

log = Log(__name__)


def parse_return_value(rc, negative, test):
    """

    Args:
        rc:
        negative:
        test:

    Returns:

    """
    # If return value is 1 and its not a negative scenario, then operation failed when it should have passed,
    # If return value if 0 and its a negative scenario, then operation passed when it should have failed,
    # In both these cases we need to log error and return 1
    pdb.set_trace()
    status = "failed" if rc == 1 else "passed"
    if (rc == 1 and not negative) or (rc != 1 and negative):
        log.error(f"{test} {status}")
        return 1
    else:
        log.info(f"{test} {status}")
        return 0


def perform_snap_operations(mirror, pool_type, negative, **kw):
    """

    Args:
        mirror:
        pool_type:
        negative:
        **kw:

    Returns:

    """
    pdb.set_trace()
    rc = 0
    config = kw.get("config")
    poolname = config[pool_type]["pool"]
    imagename = config[pool_type]["image"]
    imagespec = poolname + "/" + imagename

    # check if snapshots are created for image created above
    log.info(f"Add snapshot schedule for {imagespec}")
    snapshot_schedule_level = config[pool_type].get("snapshot_schedule_level")
    if not snapshot_schedule_level or snapshot_schedule_level == "cluster":
        log.info(f"Adding snapshot schedule at cluster level for {imagespec}")
        ret = mirror.mirror_snapshot_schedule_add()
        rc = parse_return_value(
            ret,
            negative,
            f"Snapshot schedule addition at cluster level for {imagespec}",
        )

        ret = mirror.mirror_snapshot_schedule_add(interval="1h")
        rc = parse_return_value(
            ret,
            negative,
            f"Snapshot schedule addition for 1h interval at cluster level for {imagespec}",
        )

    elif snapshot_schedule_level == "pool":
        log.info(f"Adding snapshot schedule at pool level for {imagespec}")
        ret = mirror.mirror_snapshot_schedule_add(poolname=poolname)
        rc = parse_return_value(
            ret, negative, f"Snapshot schedule addition at pool level for {imagespec}"
        )

        ret = mirror.mirror_snapshot_schedule_add(poolname=poolname, interval="1h")
        rc = parse_return_value(
            ret,
            negative,
            f"Snapshot schedule addition for 1h interval at pool level for {imagespec}",
        )

    else:
        log.info(f"Adding snapshot schedule at image level for {imagespec}")
        ret = mirror.mirror_snapshot_schedule_add(
            poolname=poolname, imagename=imagename
        )
        rc = parse_return_value(
            ret, negative, f"Snapshot schedule addition at image level for {imagespec}"
        )

        ret = mirror.mirror_snapshot_schedule_add(
            poolname=poolname, imagename=imagename, interval="1h"
        )
        rc = parse_return_value(
            ret,
            negative,
            f"Snapshot schedule addition for 1h interval at image level for {imagespec}",
        )

    # this is the verification of interval 1h
    log.info(f"Verify snapshot schedule addition for {imagespec}")
    ret = mirror.verify_snapshot_schedule(imagespec, interval=4)#=40)
    rc = parse_return_value(
        ret, negative, f"Snapshot schedule verification for {imagespec}"
    )

    log.info(f"List snapshot schedule for {imagespec}")
    ret = mirror.mirror_snapshot_schedule_list(poolname=poolname, imagename=imagename)
    rc = parse_return_value(ret, negative, f"Snapshot schedule list for {imagespec}")

    log.info(f"Fetch snapshot schedule status for {imagespec}")
    ret = mirror.mirror_snapshot_schedule_status(poolname=poolname, imagename=imagename)
    rc = parse_return_value(
        ret, negative, f"Snapshot schedule fetch status for {imagespec}"
    )

    # create one more image in the pool and check if snapshots are created
    log.info(
        "Create another image in pool and verify snapshot schedule on the new image"
    )
    imagename_2 = pool_type + mirror.random_string() + "_image2"
    imagespec_2 = poolname + "/" + imagename_2

    ret = mirror.create_image(imagespec=imagespec_2, size=config[pool_type].get("size"))
    rc = parse_return_value(ret, negative, f"Image {imagespec_2} creation")

    ret = mirror.enable_mirror_image(poolname, imagename_2, "snapshot")
    rc = parse_return_value(ret, negative, f"Enable mirroring for image {imagename_2}")

    if snapshot_schedule_level == "image":
        log.info(f"Adding snapshot schedule at image level for {imagespec_2}")
        ret = mirror.mirror_snapshot_schedule_add(
            poolname=poolname, imagename=imagename_2
        )
        rc = parse_return_value(
            ret,
            negative,
            f"Snapshot schedule addition for image {imagename_2} at image level",
        )

    log.info(f"Verify snapshot schedule for {imagespec_2}")
    ret = mirror.verify_snapshot_schedule(imagespec_2)
    rc = parse_return_value(
        ret, negative, f"Snapshot schedule verification for {imagespec_2}"
    )

    log.info(f"List snapshot schedule for {imagespec_2}")
    ret = mirror.mirror_snapshot_schedule_list(poolname=poolname, imagename=imagename_2)
    rc = parse_return_value(
        ret, negative, f"Snapshot schedule list failed for {imagespec_2}"
    )

    log.info(f"Fetch snapshot schedule status for {imagespec_2}")
    ret = mirror.mirror_snapshot_schedule_status(
        poolname=poolname, imagename=imagename_2
    )
    rc = parse_return_value(
        ret, negative, f"Snapshot schedule status failed for {imagespec_2}"
    )

    # snapshot schedule should be removed at the level (cluster, pool, image) at which it was added
    log.info("Removing snapshot schedule")
    if not snapshot_schedule_level or snapshot_schedule_level == "cluster":
        log.info("Removing snapshot schedule at cluster level")
        ret = mirror.mirror_snapshot_schedule_remove(interval="1h")
        rc = parse_return_value(
            ret, negative, "Snapshot schedule remove for interval 1h at cluster level"
        )

        ret = mirror.mirror_snapshot_schedule_remove()
        rc = parse_return_value(
            ret, negative, "Snapshot schedule remove at cluster level"
        )

        log.info("Verify snapshot schedule remove at cluster level")
        ret = mirror.verify_snapshot_schedule_remove()
        rc = parse_return_value(
            ret, negative, "Snapshot schedule remove verification at cluster level"
        )
    elif snapshot_schedule_level == "pool":
        log.info("Removing snapshot schedule at pool level")
        ret = mirror.mirror_snapshot_schedule_remove(poolname=poolname, interval="1h")
        rc = parse_return_value(
            ret, negative, "Snapshot schedule remove for interval 1h at pool level"
        )

        ret = mirror.mirror_snapshot_schedule_remove(poolname=poolname)
        rc = parse_return_value(ret, negative, "Snapshot schedule remove at pool level")

        log.info("Verify snapshot schedule remove at pool level")
        ret = mirror.verify_snapshot_schedule_remove(poolname=poolname)
        rc = parse_return_value(
            ret, negative, "Snapshot schedule remove verification at pool level"
        )
    else:
        log.info("Removing snapshot schedule at image level")
        ret = mirror.mirror_snapshot_schedule_remove(
            poolname=poolname, imagename=imagename, interval="1h"
        )
        rc = parse_return_value(
            ret,
            negative,
            f"Snapshot schedule remove for interval 1h at image level for {imagename_2}",
        )

        ret = mirror.mirror_snapshot_schedule_remove(
            poolname=poolname, imagename=imagename
        )
        rc = parse_return_value(
            ret, negative, f"Snapshot schedule remove at image level for {imagename_2}"
        )

        log.info("Verify snapshot schedule remove at image level")
        ret = mirror.verify_snapshot_schedule_remove(
            poolname=poolname, imagename=imagename
        )
        rc = parse_return_value(
            ret,
            negative,
            f"Snapshot schedule remove verification at image level for {imagename_2}",
        )

        ret = mirror.mirror_snapshot_schedule_remove(
            poolname=poolname, imagename=imagename_2
        )
        rc = parse_return_value(
            ret, negative, f"Snapshot schedule remove at image level for {imagename_2}"
        )

        log.info("Verify snapshot schedule remove at image level")
        ret = mirror.verify_snapshot_schedule_remove(
            poolname=poolname, imagename=imagename_2
        )
        rc = parse_return_value(
            ret,
            negative,
            f"Snapshot schedule remove verification at image level for {imagename_2}",
        )

    return rc


def test_snap_schedule_on_primary_and_secondary(rbd_mirror, pool_type, **kw):
    """

    Args:
        rbd_mirror:
        pool_type:
        **kw:

    Returns:

    """
    pdb.set_trace()
    mirror1 = rbd_mirror.get("mirror1")
    mirror2 = rbd_mirror.get("mirror2")

    log.info("Perform snap schedule operations on primary cluster")
    if perform_snap_operations(mirror1, pool_type, False, **kw):
        log.error("Snap schedule operations failed on primary cluster")
        return 1
    log.info("Snap schedule operations on primary cluster successful")

    if not perform_snap_operations(mirror2, pool_type, True, **kw):
        log.error("Snap schedule operations did not fail on secondary cluster")
        return 1
    log.info("Snap schedule operations on secondary cluster failed as expected")
    return 0


def run(**kw):
    """
    --> Creates Pool Image and enables snapshot based Mirroring on the pool
    --> Runs IO using rbd bench
    --> Creates images on the pool and verifies snapshots are created for each image
    --> Cleanup
    Args:
        **kw:
    Returns:
        0 - if test case pass
        1 - it test case fails
    """
    pdb.set_trace()
    log.info("Starting CEPH-83574842")

    mirror_obj = rbd_mirror_config(**kw)

    if mirror_obj:
        log.info("Executing test on replicated pool")
        if test_snap_schedule_on_primary_and_secondary(
            mirror_obj.get("rep_rbdmirror"), "rep_pool_config", **kw
        ):
            return 1

        log.info("Executing test on ec pool")
        if test_snap_schedule_on_primary_and_secondary(
            mirror_obj.get("ec_rbdmirror"), "ec_pool_config", **kw
        ):
            return 1

    return 0
