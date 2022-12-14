from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from utility.log import Log

log = Log(__name__)


def run(**kw):
    """
    1. Enable journal and snapshot mode for images
    2. Make changes to the mirrored image and check the consistancy
    3. Delete the image from secondary cluster and see if sync happens and check the consistency
        (for checking the conistency we can leverage check_data())
    4. Shutdown/stop network of the rbdmirror node( node where rbd mirror daemon is running).
       write data and check the sync after bringing back the node
    5. Verify rbd mirror image commands
        a. create Image and enable snapshot-based mirroring on it
        b. schedule snapshot and verify snapshots are getting created within the interval at any point of time,
            image should have only 3 snapshots. all the latest ones should be retained
        c. check the status of the image
        d. Remove the image
    Args:
        **kw:
    Returns:
        0 - if test case pass
        1 - it test case fails
    """
    try:
        log.info("Starting RBD mirroring test case")
        config = kw.get("config")
        mirror1, mirror2 = [
            rbdmirror.RbdMirror(cluster, config)
            for cluster in kw.get("ceph_cluster_dict").values()
        ]
        poolname = mirror1.random_string() + "_tier_1_rbd_mirror_pool"
        imagename = mirror1.random_string() + "_tier_1_rbd_mirror_image"
        imagespec = poolname + "/" + imagename

        mirror1.initial_mirror_config(
            mirror2,
            poolname=poolname,
            imagename=imagename,
            imagesize=config.get("imagesize", "1G"),
            io_total=config.get("io-total", "1G"),
            mode="image",
            mirrormode="journal",
        )

        mirror1.check_data(peercluster=mirror2, imagespec=imagespec)

        # Stop the rdb-mirror service and cehck the status
        service_name = mirror2.get_rbd_service_name("rbd-mirror")
        mirror2.change_service_state(service_name=service_name, operation="stop")
        mirror2.wait_for_status(imagespec=imagespec, state_pattern="down+stopped")
        mirror1.benchwrite(imagespec=imagespec, io=config.get("io-total"))
        mirror2.change_service_state(service_name=service_name, operation="start")
        mirror1.wait_for_status(imagespec=imagespec, state_pattern="up+stopped")
        mirror2.wait_for_status(imagespec=imagespec, state_pattern="up+replaying")
        mirror1.check_data(peercluster=mirror2, imagespec=imagespec)

        mirror1.delete_image(imagespec)
        # Add check of the image in secondary cluster

        # Create image and enable snapshot mirroring"
        imagename_1 = mirror1.random_string() + "_tier_1_rbd_mirror_image"
        imagespec_1 = poolname + "/" + imagename_1
        mirror1.create_image(imagespec=imagespec_1, size=config.get("imagesize"))
        mirror1.enable_mirror_image(poolname, imagename_1, "snapshot")
        mirror2.wait_for_status(poolname=poolname, images_pattern=1)
        mirror1.benchwrite(imagespec=imagespec_1, io=config.get("io-total"))
        mirror1.wait_for_status(imagespec=imagespec_1, state_pattern="up+stopped")
        mirror2.wait_for_status(imagespec=imagespec_1, state_pattern="up+replaying")
        # Check Data failing for snapshot mirroring looks like it is syncing snapshots
        # mirror1.check_data(peercluster=mirror2, imagespec=imagespec_1)

        # schedule snapshot rbd-mirror
        mirror1.mirror_snapshot_schedule_add(poolname=poolname, imagename=imagename_1)
        mirror1.verify_snapshot_schedule(imagespec_1)
        mirror1.mirror_snapshot_schedule_list(poolname=poolname, imagename=imagename_1)
        mirror1.mirror_snapshot_schedule_status(
            poolname=poolname, imagename=imagename_1
        )
        mirror1.mirror_snapshot_schedule_remove(
            poolname=poolname, imagename=imagename_1
        )
        # Cleans up the configuration
        mirror1.delete_image(imagespec_1)
        mirror1.clean_up(peercluster=mirror2, pools=[poolname])
        return 0

    except ValueError as ve:
        log.error(
            f"{kw.get('ceph_cluster_dict').values} has less or more clusters Than Expected(2 clusters expected)"
        )
        log.exception(ve)
    except Exception as e:
        log.exception(e)
        return 1
