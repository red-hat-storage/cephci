"""Test case covered -
CEPH-83575375 and CEPH-83575376

Test Case Flow:
1. Configure snapshot based mirroring between two clusters
2. Add mirror snapshot schedule at cluster and pool level
3. create some images and wait for them to mirror to secondary
4. Check that mirror snapshots are created for each images
5. Create a snapshot mirror schedule and list the snapshots
6. View the status of schedule snapshot
7. remove the snapshot schedule from cluster and pool level, verify the same
8. Perform test steps for both Replicated and EC pool
"""

from tests.rbd_mirror.rbd_mirror_utils import rbd_mirror_config
from utility.log import Log

log = Log(__name__)


def test_snapshot_schedule(rbd_mirror, pool_type, **kw):
    """Method to validate snapshot based mirroring schedule
    at cluster and pool level.
    1) Create schedulers at multiple level and verify the same
    2) Remove schedulers at multiple level and verify the same

    Args:
        rbd_mirror: Object
        pool_type: Replicated or EC pool
        **kw: test data
            Example::
            config:
                ec_pool_config:
                  mirrormode: snapshot
                  mode: image
                rep_pool_config:
                  mirrormode: snapshot
                  mode: image
                snapshot_schedule_level: "cluster"
                imagesize: 2G
    Returns:
        int: The return value. 0 for success, 1 otherwise
    """
    try:
        log.info("Starting snapshot RBD mirroring test case")
        config = kw.get("config")
        mirror1 = rbd_mirror.get("mirror1")
        mirror2 = rbd_mirror.get("mirror2")
        poolname = config[pool_type]["pool"]
        imagename = config[pool_type]["image"]
        imagespec = poolname + "/" + imagename

        # check if snapshots are created for image created above
        snapshot_schedule_level = config.get("snapshot_schedule_level")
        if not snapshot_schedule_level or snapshot_schedule_level == "cluster":
            mirror1.mirror_snapshot_schedule_add()
            mirror1.mirror_snapshot_schedule_add(interval="1h")
        elif snapshot_schedule_level == "pool":
            mirror1.mirror_snapshot_schedule_add(poolname=poolname)
            mirror1.mirror_snapshot_schedule_add(poolname=poolname, interval="1h")
        else:
            mirror1.mirror_snapshot_schedule_add(poolname=poolname, imagename=imagename)
            mirror1.mirror_snapshot_schedule_add(
                poolname=poolname, imagename=imagename, interval="1h"
            )
        # this is the verification of interval 1h
        mirror1.verify_snapshot_schedule(imagespec, interval=40)
        mirror1.mirror_snapshot_schedule_list(poolname=poolname, imagename=imagename)
        mirror1.mirror_snapshot_schedule_status(poolname=poolname, imagename=imagename)

        # create one more image in the pool and check if snapshots are created
        imagename_2 = mirror1.random_string() + "_tier_1_rbd_mirror_image"
        imagespec_2 = poolname + "/" + imagename_2
        mirror1.create_image(imagespec=imagespec_2, size=config.get("imagesize"))
        mirror1.enable_mirror_image(poolname, imagename_2, "snapshot")
        if snapshot_schedule_level == "image":
            mirror1.mirror_snapshot_schedule_add(
                poolname=poolname, imagename=imagename_2
            )
        mirror1.verify_snapshot_schedule(imagespec_2)
        mirror1.mirror_snapshot_schedule_list(poolname=poolname, imagename=imagename_2)
        mirror1.mirror_snapshot_schedule_status(
            poolname=poolname, imagename=imagename_2
        )

        # snapshot schedule should be removed at the level (cluster, pool, image) at which it was added
        if not snapshot_schedule_level or snapshot_schedule_level == "cluster":
            mirror1.mirror_snapshot_schedule_remove(interval="1h")
            mirror1.mirror_snapshot_schedule_remove()
            mirror1.verify_snapshot_schedule_remove()
        elif snapshot_schedule_level == "pool":
            mirror1.mirror_snapshot_schedule_remove(poolname=poolname, interval="1h")
            mirror1.mirror_snapshot_schedule_remove(poolname=poolname)
            mirror1.verify_snapshot_schedule_remove(poolname=poolname)
        else:
            mirror1.mirror_snapshot_schedule_remove(
                poolname=poolname, imagename=imagename, interval="1h"
            )
            mirror1.mirror_snapshot_schedule_remove(
                poolname=poolname, imagename=imagename
            )
            mirror1.verify_snapshot_schedule_remove(
                poolname=poolname, imagename=imagename
            )
            mirror1.mirror_snapshot_schedule_remove(
                poolname=poolname, imagename=imagename_2
            )
            mirror1.verify_snapshot_schedule_remove(
                poolname=poolname, imagename=imagename_2
            )

        return 0

    except ValueError as ve:
        log.error(
            f"{kw.get('ceph_cluster_dict').values} has less or more clusters Than Expected(2 clusters expected)"
        )
        log.exception(ve)
        return 1
    except Exception as e:
        log.exception(e)
        return 1

    finally:
        # Cleans up the configuration
        mirror1.delete_image(imagespec)
        mirror1.delete_image(imagespec_2)
        mirror1.clean_up(peercluster=mirror2, pools=[poolname])


def run(**kw):
    """
    Snapshot based rbd mirroring schedulers at cluster and pool level.

    Args:
        **kw: test data
            Example::
            config:
                ec_pool_config:
                  mirrormode: snapshot
                  mode: image
                rep_pool_config:
                  mirrormode: snapshot
                  mode: image
                snapshot_schedule_level: "cluster"
                imagesize: 2G

    Returns:
        int: The return value. 0 for success, 1 otherwise
    """
    log.info(
        "Starting CEPH-83575375 and CEPH-83575376"
        "Create snapshot based RBD mirror at cluster and pool level and verify the same"
    )

    mirror_obj = rbd_mirror_config(**kw)

    if mirror_obj:
        log.info("Executing test on replicated pool")
        if test_snapshot_schedule(
            mirror_obj.get("rep_rbdmirror"), "rep_pool_config", **kw
        ):
            return 1

        log.info("Executing test on ec pool")
        if test_snapshot_schedule(
            mirror_obj.get("ec_rbdmirror"), "ec_pool_config", **kw
        ):
            return 1

    return 0
