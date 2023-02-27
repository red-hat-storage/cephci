from tests.rbd.rbd_utils import Rbd
from tests.rbd_mirror.rbd_mirror_utils import rbd_mirror_config
from utility.log import Log

log = Log(__name__)


def test_image_move_secondary_trash(rbd_mirror, pool_type, **kw):
    """
    Test to check secondary image move to trash is failed as image will be locked and busy.
    Args:
        rbd_mirror: rbd mirror object
        pool_type: ec pool or rep pool
        **kw:
    Returns:
        0 if test pass, else 1
    """
    try:
        mirror1 = rbd_mirror.get("mirror1")
        config = kw.get("config")
        pool = config[pool_type]["pool"]
        image = config[pool_type]["image"]
        imagespec = pool + "/" + image

        rbd1, rbd2 = [
            Rbd(**kw, req_cname=cluster_name)
            for cluster_name in kw.get("ceph_cluster_dict").keys()
        ]

        mirror1.benchwrite(imagespec=imagespec, io=config.get("io-total", "1G"))

        # Move secondary image to trash
        if rbd2.move_image_trash(pool, image):
            log.info("Image not moved to trash since it is in secondary cluster")
            return 0
        else:
            log.error("Image moved to trash inspite of it being in secondary cluster")
            return 1

    except Exception as e:
        log.exception(e)
        return 1


def run(**kw):
    log.info("Starting RBD mirroring test case - CEPH-11416")
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
    mirror_obj = rbd_mirror_config(**kw)

    if mirror_obj:
        if "rep_rbdmirror" in mirror_obj:
            log.info("Executing test on replicated pool")
            if test_image_move_secondary_trash(
                mirror_obj.get("rep_rbdmirror"), "rep_pool_config", **kw
            ):
                return 1
        if "ec_rbdmirror" in mirror_obj:
            log.info("Executing test on ec pool")
            if test_image_move_secondary_trash(
                mirror_obj.get("ec_rbdmirror"), "ec_pool_config", **kw
            ):
                return 1
    return 0
