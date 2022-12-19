from tests.rbd.exceptions import IOonSecondaryError
from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from utility.log import Log

log = Log(__name__)


def run(**kw):
    """Verification of secondary image shrink fails.

    This module verifies that the attempt to resize image which is secondary to its peer fails.
    Args:
        **kw:
    Returns:
        0 - if test case pass
        1 - it test case fails
    Test case covered -
    CEPH-9500 - Attempt Shrinking images on remote/secondary cluster while mirroring is happening.
    Pre-requisites :
    1. At least two clusters must be up and running with enough number of OSDs to create pools
    2. We need atleast one client node with ceph-common package,
       conf and keyring files
    Test Case Flow:
    1. Create an image in primary and perform IOs.
    2. Try to resize the secondary image
    """
    try:
        log.info("Starting RBD mirroring test case - 9500")
        config = kw.get("config")
        mirror1, mirror2 = [
            rbdmirror.RbdMirror(cluster, config)
            for cluster in kw.get("ceph_cluster_dict").values()
        ]
        poolname = mirror1.random_string() + "_ceph_9500"
        imagename = mirror1.random_string() + "_ceph_9500"
        imagespec = poolname + "/" + imagename

        mirror1.initial_mirror_config(
            mirror2,
            poolname=poolname,
            imagename=imagename,
            imagesize=config.get("imagesize", "1G"),
            mode="pool",
        )

        mirror1.benchwrite(imagespec=imagespec, io=config.get("io-total", "1G"))

        mirror2.resize_image(imagespec, "10M")

    except IOonSecondaryError:
        log.info("Resizing secondary image has failed as expected")
        return 0

    except Exception as e:
        log.exception(e)

    return 1
