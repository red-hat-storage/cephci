import time

from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from utility.log import Log

log = Log(__name__)


def run(**kw):
    """
    1. Enable snapshot mode for images
    2. Run IO on mirrored images
    3. Rename the image from primary cluster and see if image reflected to secondary
       and check the data consistency
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
        mirror1.create_pool(poolname=poolname)
        mirror2.create_pool(poolname=poolname)

        # Create image and enable snapshot mirroring"
        imagename_1 = mirror1.random_string() + "_tier_1_rbd_mirror_image"
        imagespec_1 = poolname + "/" + imagename_1
        mirror1.create_image(imagespec=imagespec_1, size=config.get("imagesize"))
        mirror1.config_mirror(mirror2, poolname=poolname, mode="image")
        mirror1.enable_mirror_image(poolname, imagename_1, "snapshot")
        mirror2.wait_for_status(poolname=poolname, images_pattern=1)
        mirror1.benchwrite(imagespec=imagespec_1, io=config.get("io-total"))
        mirror1.wait_for_status(imagespec=imagespec_1, state_pattern="up+stopped")
        mirror2.wait_for_status(imagespec=imagespec_1, state_pattern="up+replaying")

        # Rename primary image and check on secondary
        mirror1.rename_primary_image(
            source_imagespec=imagespec_1,
            dest_imagespec="rename_image",
            peercluster=mirror2,
            poolname=poolname,
        )
        mirror1.create_mirror_snapshot(f"{poolname}/rename_image")
        time.sleep(30)
        out2 = mirror2.exec_cmd(cmd=f"rbd info {poolname}/rename_image")
        log.info(out2)

        # Cleans up the configuration
        mirror1.delete_image(f"{poolname}/rename_image")
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
