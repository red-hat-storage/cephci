from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from utility.log import Log
import time

log = Log(__name__)


def run(**kw):
    """ verification of removal of failed back images should reflect to secondary.
    this verifies that the deletion of primary images deletes the images on secondary also when we perform multiple
    promote and demote operation.

    Test case flow:
    1. Create image on cluster-1.
    2. Enable mirroring for created image on cluster-1 and run IOs.
    3. Ensure mirror image created on cluster-2.
    4. Force promote image on cluster-2 and run IOs.
    5. demote image on cluster 1.
    6. resync the image to cluster 2.
    7. Demote image on cluster-2 and promote image on cluster-1.
    8. Delete image on cluster-1.

    Args:
        **kw:
    Returns:
        0 - if test case pass
        1 - it test case fails
    """
    try:
        log.info("Starting RBD mirroring test case - CEPH-83574741")
        mirror1, mirror2 = [
            rbdmirror.RbdMirror(cluster, config)
            for cluster in kw.get("ceph_cluster_dict").values()
        ]
        config = kw.get("config")
        poolname = mirror1.random_string() + "83574741pool"
        imagename = mirror1.random_string() + "83574741image"
        imagespec = poolname + "/" + imagename
        mirror1.create_pool(poolname=poolname)
        mirror2.create_pool(poolname=poolname)
        mirror1.create_image(imagespec=imagespec, size=config.get("imagesize"))
        mirror1.config_mirror(mirror2, poolname=poolname, mode="image")
        mirror1.enable_mirror_image(poolname, imagename, "snapshot")
        mirror2.wait_for_status(poolname=poolname, images_pattern=1)
        mirror1.benchwrite(imagespec=imagespec, io=config.get("io-total"))
        time.sleep(60)
        mirror1.wait_for_status(imagespec=imagespec, state_pattern="up+stopped")
        mirror2.wait_for_status(imagespec=imagespec, state_pattern="up+replaying")
        mirror2.promote(imagespec=imagespec, force=True)
        mirror2.wait_for_status(imagespec=imagespec, state_pattern="up+stopped")
        mirror2.benchwrite(imagespec=imagespec, io=config.get("io-total"))
        time.sleep(60)
        mirror1.demote(imagespec=imagespec)
        mirror1.wait_for_status(imagespec=imagespec, state_pattern="up+unknown")
        mirror1.resync(imagespec=imagespec)
        time.sleep(100)
        mirror2.demote(imagespec=imagespec)
        mirror2.wait_for_status(imagespec=imagespec, state_pattern="up+unknown")
        mirror1.promote(imagespec=imagespec)
        mirror1.wait_for_status(imagespec=imagespec, state_pattern="up+stopped")
        mirror1.delete_image(imagespec)

        if mirror2.image_exists(imagespec):
            return 0

    except Exception as e:
        log.exception(e)
        return 1
