from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from utility.log import Log

log = Log(__name__)


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
    try:
        log.info("Starting snapshot RBD mirroring test case")
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
            mirrormode="snapshot",
        )

        # check if snapshots are created for image created above
        snapshot_schedule_level = config.get("snapshot_schedule_level")
        if not snapshot_schedule_level or snapshot_schedule_level == "cluster":
            mirror1.mirror_snapshot_schedule_add()
        elif snapshot_schedule_level == "pool":
            mirror1.mirror_snapshot_schedule_add(poolname=poolname)
        else:
            mirror1.mirror_snapshot_schedule_add(poolname=poolname, imagename=imagename)
        mirror1.verify_snapshot_schedule(imagespec)
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
            mirror1.mirror_snapshot_schedule_remove()
            mirror1.verify_snapshot_schedule_remove()
        elif snapshot_schedule_level == "pool":
            mirror1.mirror_snapshot_schedule_remove(poolname=poolname)
            mirror1.verify_snapshot_schedule_remove(poolname=poolname)
        else:
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

        # Cleans up the configuration
        mirror1.delete_image(imagespec)
        mirror1.delete_image(imagespec_2)
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
