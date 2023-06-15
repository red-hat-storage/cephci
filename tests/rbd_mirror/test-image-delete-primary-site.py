from tests.rbd_mirror.rbd_mirror_utils import rbd_mirror_config
from utility.log import Log

log = Log(__name__)


def test_image_delete_from_primary(rbd_mirror, pool_type, **kw):
    try:
        mirror1 = rbd_mirror.get("mirror1")
        mirror2 = rbd_mirror.get("mirror2")
        config = kw.get("config")
        pool = config[pool_type]["pool"]
        image = config[pool_type]["image"]
        imagespec = pool + "/" + image

        mirror1.benchwrite(imagespec=imagespec, io=config[pool_type].get("io_total"))

        mirror1.delete_image(imagespec)
        if mirror2.image_exists(imagespec):
            return 0

    except Exception as e:
        log.exception(e)

    # Cleans up the pool configuration
    finally:
        mirror1.clean_up(peercluster=mirror2, pools=[pool])

    return 1


def run(**kw):
    """Verification of primary image deletion mirroring.

    This module verifies that the deletion of primary image deletes the image in secondary also

    Args:
        **kw:

    Returns:
        0 - if test case pass
        1 - it test case fails

    Test case covered -
    CEPH-9501 - Delete image on local/primary cluster that is getting mirrored

    Pre-requisites :
    1. At least two clusters must be up and running with enough number of OSDs to create pools
    2. We need atleast one client node with ceph-common package,
       conf and keyring files

    Test Case Flow:
    1. Create an image in primary and perform IOs.
    2. Delete the image when contents are getting mirrored.
    3. Make sure that image is deleted in secondary site.
    """
    log.info("Starting RBD mirroring test case - 9501")
    mirror_obj = rbd_mirror_config(**kw)

    if mirror_obj:
        log.info("Executing test on replicated pool")
        if test_image_delete_from_primary(
            mirror_obj.get("rep_rbdmirror"), "rep_pool_config", **kw
        ):
            return 1

        log.info("Executing test on ec pool")
        if test_image_delete_from_primary(
            mirror_obj.get("ec_rbdmirror"), "ec_pool_config", **kw
        ):
            return 1

    return 0
