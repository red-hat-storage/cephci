from ceph.parallel import parallel
from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from utility.log import Log

log = Log(__name__)


def run(**kw):
    """verification Snap mirroring on journaling based image after disable journaling on image.

    Args:
        **kw:
    Returns:
        0 - if test case pass
        1 - it test case fails

    Test case flow:
    1. Create image on cluster-1.
    2. Enable journal mirroring for created image on cluster-1 and run IOs.
    3. Ensure mirror image created on cluster-2.
    4. Disable journal mirroring on image.
    5. enable snapshot mirroring on same image.
    6. Verify for snapshot mirroring and run IOs
    7. cleanup
    """
    try:
        log.info("Starting RBD mirroring test case")
        config = kw.get("config")
        mirror1, mirror2 = [
            rbdmirror.RbdMirror(cluster, config)
            for cluster in kw.get("ceph_cluster_dict").values()
        ]
        poolname = mirror1.random_string() + "_tier_2_rbd_mirror_pool"
        imagename = mirror1.random_string() + "_tier_2_rbd_mirror_image"
        imagespec = poolname + "/" + imagename

        # initial mirror configuration
        mirror1.initial_mirror_config(
            mirror2,
            poolname=poolname,
            imagename=imagename,
            imagesize=config.get("imagesize", "1G"),
            io_total=config.get("io-total", "1G"),
            mode="image",
            mirrormode="journal",
            **kw,
        )

        # Disabling journal mirror on image
        mirror1.disable_mirroring("image", imagespec)

        # enable snapshot mirroring on image
        mirror1.enable_mirror_image(poolname, imagename, "snapshot")
        mirror2.wait_for_status(poolname=poolname, images_pattern=1)

        # Verification of mirrored image on cluster
        mirror2.image_exists(imagespec)
        if mirror1.get_mirror_mode(imagespec) == "snapshot":
            log.info("snapshot mirroring is enabled")
        mirror1.benchwrite(imagespec=imagespec, io=config.get("io-total", "1G"))
        with parallel() as p:
            p.spawn(
                mirror1.wait_for_status, imagespec=imagespec, state_pattern="up+stopped"
            )
            p.spawn(
                mirror2.wait_for_status,
                imagespec=imagespec,
                state_pattern="up+replaying",
            )

        # Cleans up the configuration
        mirror1.clean_up(peercluster=mirror2, pools=[poolname])
        return 0

    except ValueError:
        log.error(
            f"{kw.get('ceph_cluster_dict').values} has less or more clusters Than Expected(2 clusters expected)"
        )

    except Exception as e:
        log.error(e)
        return 1
