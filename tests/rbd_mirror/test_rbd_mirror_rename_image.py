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

        # Create image and enable snapshot mirroring"
        imagename_1 = mirror1.random_string() + "_tier_1_rbd_mirror_image"
        imagespec_1 = poolname + "/" + imagename_1

        mirror1.initial_mirror_config(
            mirror2,
            poolname=poolname,
            imagename=imagename_1,
            imagesize=config.get("imagesize", "1G"),
            io_total=config.get("io-total", "1G"),
            mode="image",
            mirrormode="snapshot",
        )

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
