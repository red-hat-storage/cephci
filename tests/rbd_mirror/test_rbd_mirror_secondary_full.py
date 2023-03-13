from tests.rbd.rbd_utils import Rbd
from tests.rbd_mirror.rbd_mirror_utils import rbd_mirror_config
from utility.log import Log

log = Log(__name__)


def test_image_mirror_secondary_full(rbd_mirror, pool_type, **kw):
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
        mirror2 = rbd_mirror.get("mirror2")
        config = kw.get("config")
        pool = config[pool_type]["pool"]

        imagespec = pool + "/" + rbd_mirror.random_string()

        mirror2.exec_cmd(cmd=f'ceph osd set-full-ratio 0.1', output=True)
        mirror1.create_image(imagespec=imagespec, size="10G")
        mirror1.wait_for_status(imagespec=imagespec, state_pattern="up+stopped")
        mirror2.wait_for_status(imagespec=imagespec, state_pattern="up+replaying")

    except Exception as e:
        log.exception(e)
        return 1


def run(**kw):
    log.info("Starting RBD mirroring test case - CEPH-9507")
    """verify the mirroring of image copy when the secondary cluster doesn't have enough space left to mirror the image copy	
    Args:
        **kw:
    Returns:
        0 - if test case pass
        1 - it test case fails
    Test case covered -
    CEPH-9507 - Create a mirror image, when secondary cluster doesn't have enough space left to mirror the image copy	
    Pre-requisites :
    1. At least two clusters must be up and running with enough number of OSDs to create pools
    2. We need atleast one client node with ceph-common package,
       conf and keyring files
    Test Case Flow:
    1. create two cluster as primary and secondary
    2. enable pool mirroring on the image.
    3. verify for the pool mirror mode enabled on secondary.
    4. fill up the secondary cluster or use "ceph config set osd full_ratio" to nake cluster full and stops clients from writing data
    6. verify that proper error message is shown when cluster is full while doing image copy on secondary.
    """
    mirror_obj = rbd_mirror_config(**kw)

    if mirror_obj:
        if "rep_rbdmirror" in mirror_obj:
            log.info("Executing test on replicated pool")
            if test_image_mirror_secondary_full(
                mirror_obj.get("rep_rbdmirror"), "rep_pool_config", **kw
            ):
                return 1
        if "ec_rbdmirror" in mirror_obj:
            log.info("Executing test on ec pool")
            if test_image_mirror_secondary_full(
                mirror_obj.get("ec_rbdmirror"), "ec_pool_config", **kw
            ):
                return 1
    return 0
