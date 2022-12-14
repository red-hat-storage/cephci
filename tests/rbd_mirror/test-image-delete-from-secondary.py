import datetime
import time

from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from utility.log import Log

log = Log(__name__)


def run(**kw):
    """verification of removal of failed back images should reflect to secondary.

    this verifies that the deletion of primary images deletes the images on secondary also when we perform multiple
    promote and demote operation.

    Args:
        **kw:
        repeat_count - number of times promote and demote happens before delete image
    Returns:
        0 - if test case pass
        1 - it test case fails

    Test case flow:
    1. Create image on cluster-1.
    2. Enable mirroring for created image on cluster-1 and run IOs.
    3. Ensure mirror image created on cluster-2.
    4. Force promote image on cluster-2 and run IOs.
    5. demote image on cluster 1.
    6. resync the image to cluster 2.
    7. Demote image on cluster-2 and promote image on cluster-1.
    8. Delete image on cluster-1.
    """
    try:
        log.info("Starting RBD mirroring test case - CEPH-83574741")
        config = kw.get("config")
        mirror1, mirror2 = [
            rbdmirror.RbdMirror(cluster, config)
            for cluster in kw.get("ceph_cluster_dict").values()
        ]

        # create images and enable snapshot mirroring
        poolname = mirror1.random_string() + "83574741pool"
        imagename = mirror1.random_string() + "83574741image"
        imagespec = poolname + "/" + imagename

        mirror1.initial_mirror_config(
            mirror2,
            poolname=poolname,
            imagename=imagename,
            imagesize=config.get("imagesize", "1G"),
            io_total=config.get("io-total", "1G"),
            mode="image",
            mirrormode="snapshot",
        )

        # promote and demote images
        # resyncing of images after demote
        count = 0
        tout = datetime.timedelta(seconds=120)
        while count < config.get("repeat_count"):
            mirror2.promote(imagespec=imagespec, force=True)
            mirror2.wait_for_status(
                tout=tout, imagespec=imagespec, state_pattern="up+stopped"
            )
            mirror2.benchwrite(imagespec=imagespec, io=config.get("io-total"))
            time.sleep(60)
            mirror1.demote(imagespec=imagespec)
            mirror1.wait_for_status(
                tout=tout, imagespec=imagespec, state_pattern="up+error"
            )
            mirror1.resync(imagespec=imagespec)
            time.sleep(60)
            mirror1.wait_for_status(
                tout=tout, imagespec=imagespec, state_pattern="up+replaying"
            )
            mirror2.demote(imagespec=imagespec)
            mirror2.wait_for_status(
                tout=tout, imagespec=imagespec, state_pattern="up+unknown"
            )
            mirror1.promote(imagespec=imagespec)
            mirror1.wait_for_status(
                tout=tout, imagespec=imagespec, state_pattern="up+stopped"
            )
            mirror2.wait_for_status(
                tout=tout, imagespec=imagespec, state_pattern="up+replaying"
            )
            count += 1

        # Remove from primary
        mirror1.delete_image(imagespec)
        time.sleep(60)

        # verify removal from secondary
        if mirror2.image_exists(imagespec):
            return 0

    except Exception as e:
        log.exception(e)
        return 1
