from ceph.parallel import parallel
from tests.rbd.exceptions import IOonSecondaryError
from tests.rbd_mirror.rbd_mirror_utils import rbd_mirror_config
from utility.log import Log

log = Log(__name__)


def test_expand_or_shrink_img_at_secondary(rbd_mirror, pool_type, **kw):
    """
    Expand and shrink the given image on secondary site

    Args:
        rbd_mirror: Dict containing mirror1 and mirror2 objects for primary and secondary clusters
        pool_type: ec_pool_config or rep_pool_config
        **kw: test data containing pool name, image name and other details
    """
    mirror1 = rbd_mirror.get("mirror1")
    mirror2 = rbd_mirror.get("mirror2")
    config = kw.get("config")
    pool = config[pool_type]["pool"]
    image = config[pool_type]["image"]
    imagespec = pool + "/" + image
    flag = 1

    try:
        log.info("Trying to shrink secondary image")
        # Shrink size is taken as imagesize/2 rounded upto the next integer
        size = (
            str(round(int(config[pool_type].get("imagesize", "1G")[0:-1]) / 2))
            + config.get("imagesize", "1G")[-1]
        )
        with parallel() as p:
            p.spawn(
                mirror1.benchwrite,
                imagespec=imagespec,
                io=config[pool_type].get("io-total", "1G"),
            )
            p.spawn(mirror2.resize_image, imagespec=imagespec, size="10M")

    except IOonSecondaryError:
        log.info("Shrinking secondary image has failed as expected")
        flag = 0

    if flag:
        log.error("Shrinking secondary image did not fail as expected")
        return flag

    try:
        log.info("Trying to expand secondary image")
        # Expand size is taken as imagesize + 1
        size = (
            str(int(config[pool_type].get("imagesize", "1G")[0:-1]) + 1)
            + config.get("imagesize", "1G")[-1]
        )
        with parallel() as p:
            p.spawn(
                mirror1.benchwrite,
                imagespec=imagespec,
                io=config[pool_type].get("io-total", "1G"),
            )
            p.spawn(mirror2.resize_image, imagespec=imagespec, size=size)

        if flag:
            log.error("Expanding secondary image did not fail as expected")
    except IOonSecondaryError:
        log.info("Expanding secondary image has failed as expected")
        flag = 0

    finally:
        mirror1.delete_image(imagespec)
        mirror1.clean_up(peercluster=mirror2, pools=[pool])

    return flag


def run(**kw):
    """Verification of secondary image expand/shrink fails.

    This module verifies that the attempt to resize image which is secondary to its peer fails.
    Args:
        **kw: Test data
    Returns:
        0 - if test case pass
        1 - it test case fails
    Test case covered -
    CEPH-9500 - Attempt Expanding or Shrinking images on remote/secondary cluster while mirroring is happening.
    Pre-requisites :
    1. At least two clusters must be up and running with enough number of OSDs to create pools
    2. We need atleast one client node with ceph-common package,
       conf and keyring files
    Test Case Flow:
    1. Create an image in primary and perform IOs.
    2. Try to shrink the secondary image and verify that it fails
    3. Try to expand the secondary image and verify that it fails
    4. Repeat the same for EC pool
    """
    try:
        log.info("Starting RBD mirroring test case - 9500")
        mirror_obj = rbd_mirror_config(**kw)

        if mirror_obj:
            log.info("Executing test on replicated pool")
            if test_expand_or_shrink_img_at_secondary(
                mirror_obj.get("rep_rbdmirror"), "rep_pool_config", **kw
            ):
                return 1

            log.info("Executing test on ec pool")
            if test_expand_or_shrink_img_at_secondary(
                mirror_obj.get("ec_rbdmirror"), "ec_pool_config", **kw
            ):
                return 1

        return 0

    except Exception as e:
        log.exception(e)
        return 1
