from tests.rbd.rbd_utils import Rbd
from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from utility.log import Log

log = Log(__name__)


def run(**kw):
    """verify that moving of secondary image on the relationship to trash is failed.

    Args:
        **kw:

    Returns:
        0 - if test case pass
        1 - it test case fails

    Test case covered -
    CEPH-11416 - Pool Mirror - Try marking the secondary on the relationship for deletion, and observe the behaviour	

    Pre-requisites :
    1. At least two clusters must be up and running with enough number of OSDs to create pools
    2. We need atleast one client node with ceph-common package,
       conf and keyring files

    Test Case Flow:
    1. Create an image in primary and perform IOs.
    2. enable pool mirroring on the image.
    3. verify for the pool mirror mode enabled on secondary.
    4. Move the secondary image to the trash.
    6. verify that unable to mark the secondary image for deletion, as image is locked.
    """
    try:
        log.info("Starting RBD mirroring test case - CEPH-11416")
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

        # Move secondary image to trash
        if rbd2.move_image_trash(poolname, imagename):


        image_id = rbd1.get_image_id(poolname, imagename)
        log.info(f"image id is {image_id}")

        if rbd2.trash_exist(pool, image):
            raise ImageFoundError(" Image is found in the Trash")
        else:
            return 0

    except ValueError as ve:
        log.error(
            f"{kw.get('ceph_cluster_dict').values} has less or more clusters Than Expected(2 clusters expected)"
        )
        log.exception(ve)
    except Exception as e:
        log.exception(e)
        return 1