from tests.rbd.rbd_utils import Rbd
from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from utility.log import Log

log = Log(__name__)


def run(**kw):
    """Verification of trash restore on an image with snap and clone in primary cluster
    to check that mirror relationship is intact post restore.

    Args:
        **kw:

    Returns:
        0 - if test case pass
        1 - it test case fails

    Test case covered -
    CEPH-11417 - Pool Mirror - verify mirroring is intact after restore primary
    image having snap and clone configured from trash

    Pre-requisites :
    1. At least two clusters must be up and running with enough number of OSDs to create pools
    2. We need atleast one client node with ceph-common package,
       conf and keyring files

    Test Case Flow:
    1. Create an image in primary and perform IOs.
    2. enable pool mirroring on the image.
    3. verify for the pool mirror mode enabled on secondary.
    4. Create anapshot and clone from the snapshot on primary.
    5. Move the primary image to the trash.
    6. Restore the primary image from the trash.
    7. Make sure that image is restored and the mirror relationship is intact.
    """
    try:
        log.info("Starting RBD mirroring test case - CEPH-11417")
        config = kw.get("config")

        mirror1, mirror2 = [
            rbdmirror.RbdMirror(cluster, config)
            for cluster in kw.get("ceph_cluster_dict").values()
        ]

        rbd1, rbd2 = [
            Rbd(**kw, req_cname=cluster_name)
            for cluster_name in kw.get("ceph_cluster_dict").keys()
        ]

        poolname = mirror1.random_string()
        imagename = mirror1.random_string()
        imagespec = poolname + "/" + imagename
        mirror1.initial_mirror_config(
            mirror2,
            poolname=poolname,
            imagename=imagename,
            imagesize=config.get("imagesize", "1G"),
            mode="pool",
            **kw,
        )

        mirror1.benchwrite(imagespec=imagespec, io=config.get("io-total", "1G"))

        # create snapshot and clone for the primary image
        snap = rbd1.random_string()
        clone = rbd1.random_string()
        rbd1.snap_create(poolname, imagename, snap)
        snap_name = f"{poolname}/{imagename}@{snap}"
        rbd1.create_clone(
            snap_name=snap_name,
            pool_name=poolname,
            image_name=clone,
            clone_version="v2",
        )

        # Move primary image to trash
        rbd1.move_image_trash(poolname, imagename)
        image_id = rbd1.get_image_id(poolname, imagename)
        log.info(f"image id is {image_id}")

        # restore primary image
        rbd1.trash_restore(poolname, image_id)
        mirror1.image_exists(imagespec)
        mirror1.wait_for_status(imagespec=imagespec, state_pattern="up+stopped")
        mirror2.wait_for_status(imagespec=imagespec, state_pattern="up+replaying")
        if mirror1.get_mirror_mode(imagespec) == "journal":
            log.info("mirroring is intact and enabled even post trash restore")

        # write some bench IO after restore
        mirror1.benchwrite(imagespec=imagespec, io=config.get("io-total", "1G"))
        return 0

    except ValueError as ve:
        log.error(
            f"{kw.get('ceph_cluster_dict').values} has less or more clusters Than Expected(2 clusters expected)"
        )
        log.exception(ve)
    except Exception as e:
        log.exception(e)
        return 1

    # Cleans up the pool configuration
    finally:
        mirror1.clean_up(peercluster=mirror2, pools=[poolname])
