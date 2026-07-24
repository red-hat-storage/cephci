import time

from tests.rbd_mirror.rbd_mirror_utils import rbd_mirror_config
from utility.log import Log

log = Log(__name__)


def test_image_removal_from_secondary(rbd_mirror, pool_type, **kw):
    try:
        mirror1 = rbd_mirror.get("mirror1")
        mirror2 = rbd_mirror.get("mirror2")
        config = kw.get("config")
        pool = config[pool_type]["pool"]
        image = config[pool_type]["image"]
        imagespec = pool + "/" + image
        # verify for journal mirroring enable
        if mirror1.get_mirror_mode(imagespec) == "journal":
            log.info("journal mirroring is enabled")

        count = 0
        # Disable and enable journaling on primary image while writing some IOs
        while count < config.get("repeat_count"):
            mirror1.image_feature_disable(
                imagespec=imagespec, image_feature="journaling"
            )
            mirror1.benchwrite(imagespec=imagespec, io=config.get("io-total"))
            mirror1.image_feature_enable(
                imagespec=imagespec, image_feature="journaling"
            )
            time.sleep(60)

            count += 1

        # again disable the journaling feature on primary image
        mirror1.image_feature_disable(imagespec=imagespec, image_feature="journaling")
        time.sleep(60)

        # verify removal from secondary
        if mirror2.image_exists(imagespec):
            log.info("Image does not exist after disabling journaling feature")
            return 0
        else:
            log.error(
                "Image still exist in secondary cluster even after removing the journaling feature"
            )
            return 1
    except Exception as e:
        log.exception(e)
        return 1

    # Cleans up the pool configuration
    finally:
        mirror1.clean_up(peercluster=mirror2, pools=[pool])


def run(**kw):
    log.info("Starting RBD mirroring test case - CEPH-10470")
    """Verification of image removal from secondary after disabling the journaling
    feature on primary image
    Args:
        **kw:
    Returns:
        0 - if test case pass
        1 - it test case fails
    Test case flow:
    1. Create image on cluster-1.
    2. Enable mirroring for created image on cluster-1 and run IOs.
    3. Ensure mirror image created on cluster-2.
    4. execute:
        for i in {1..10}; do rbd feature disable RBD/test1 journaling --cluster master
        rbd bench-write RBD/test1 --io-size 10240 --cluster master
        rbd feature enable RBD/test1 journaling --cluster master
    5. Again disable journaling feature on primary/master.
    5. Verify for image removal on secondary.
    """
    mirror_obj = rbd_mirror_config(**kw)
    if mirror_obj:
        if "rep_rbdmirror" in mirror_obj:
            log.info("Executing test on replicated pool")
            if test_image_removal_from_secondary(
                mirror_obj.get("rep_rbdmirror"), "rep_pool_config", **kw
            ):
                return 1
        if "ec_rbdmirror" in mirror_obj:
            log.info("Executing test on ec pool")
            if test_image_removal_from_secondary(
                mirror_obj.get("ec_rbdmirror"), "ec_pool_config", **kw
            ):
                return 1
    return 0
