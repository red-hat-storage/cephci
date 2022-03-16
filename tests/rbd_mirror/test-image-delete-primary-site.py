from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from utility.log import Log

log = Log(__name__)


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
    try:
        log.info("Starting RBD mirroring test case - 9501")
        config = kw.get("config")
        mirror1, mirror2 = [
            rbdmirror.RbdMirror(cluster, config)
            for cluster in kw.get("ceph_cluster_dict").values()
        ]
        poolname = mirror1.random_string() + "_ceph_9501"
        imagename = mirror1.random_string() + "_ceph_9501"
        imagespec = poolname + "/" + imagename

        initial_config(
            mirror1,
            mirror2,
            poolname,
            imagespec,
            imagesize=config.get("imagesize", "1G"),
        )
        mirror1.benchwrite(imagespec=imagespec, io=config.get("io-total", "1G"))

        mirror1.delete_image(imagespec)
        if mirror2.image_exists(imagespec):
            return 0

    except Exception as e:
        log.exception(e)

    return 1


def initial_config(mirror1, mirror2, poolname, imagespec, imagesize):
    """
    Calls create_pool function on both the clusters,
    creates an image on primary cluster and
    waits for image to be present in secondary cluster with replying status
    """
    mirror1.create_pool(poolname=poolname)
    mirror2.create_pool(poolname=poolname)
    mirror1.create_image(imagespec=imagespec, size=imagesize)
    mirror1.config_mirror(mirror2, poolname=poolname, mode="pool")
    mirror2.wait_for_status(poolname=poolname, images_pattern=1)
    mirror1.wait_for_status(imagespec=imagespec, state_pattern="up+stopped")
    mirror2.wait_for_status(imagespec=imagespec, state_pattern="up+replaying")
