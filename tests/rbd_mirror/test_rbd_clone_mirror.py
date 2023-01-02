from tests.rbd.exceptions import RbdBaseException
from tests.rbd.rbd_utils import Rbd
from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from utility.log import Log

log = Log(__name__)


def run(**kw):
    """Verification mirroring of clone images.

    This module create pool, image and cloned images and
    verifies if the clone of the image within the pool also get mirrored.

    Args:
        **kw:
    Returns:
        0 - if test case pass
        1 - it test case fails

    Test case covered -
    CEPH-9521 - When a Pool based mirror is established,
    verify if the clone of a image within the Pool also gets mirrored and
    image cloned after the mirroring is also get mirrored


    Pre-requisites :
    1. Two Clusters must be up and running to create pool
    2. We need atleast one client node with ceph-common package,
       conf and keyring files on each node.

    Test Case Flow:
    1. Create a pool on both clusters.
    2. Create an Image on cluster 1 and create a clone of the image in same pool.
    3. Configure mirroring (peer bootstrap) between two clusters.
    4. Enable pool mood journal based mirroring on the pool respectively.
    5. Verify for the clone image is also get mirrored.
    6. Create one more clone from the image and verify it is mirrored.
    """

    try:
        log.info("Running RBD mirroring test case for clone images")
        config = kw.get("config")
        rbd = Rbd(**kw)

        poolname = rbd.random_string() + "9521pool"
        imagename = rbd.random_string() + "9521image"
        snap = rbd.random_string()
        clone1 = rbd.random_string()
        size = "10G"
        image_feature = "exclusive-lock,layering,journaling"
        # create pool and image on cluster1
        size = kw.get("size", "10G")
        if not rbd.create_pool(poolname=poolname):
            log.error(f"Pool creation failed for pool {poolname}")
            return 1
        rbd.create_image(
            pool_name=poolname,
            image_name=imagename,
            size=size,
            image_feature=image_feature,
        )

        # create clone of the image
        rbd.snap_create(poolname, imagename, snap)
        snap_name = f"{poolname}/{imagename}@{snap}"
        rbd.create_clone(
            snap_name=snap_name,
            pool_name=poolname,
            image_name=clone1,
            clone_version="v2",
        )
        # enable journal based mirroring for pool mode
        mirror1, mirror2 = [
            rbdmirror.RbdMirror(cluster, config)
            for cluster in kw.get("ceph_cluster_dict").values()
        ]
        imagespec = poolname + "/" + imagename
        mirror2.create_pool(poolname=poolname)
        mirror1.config_mirror(mirror2, poolname=poolname, mode="pool")
        mirror2.wait_for_status(poolname=poolname, images_pattern=2)
        mirror1.wait_for_status(imagespec=imagespec, state_pattern="up+stopped")
        mirror2.wait_for_status(imagespec=imagespec, state_pattern="up+replaying")

        # verify for cloned image mirrored
        imagespec1 = poolname + "/" + clone1
        mirror2.wait_for_status(poolname=poolname, images_pattern=2)
        mirror1.wait_for_status(imagespec=imagespec1, state_pattern="up+stopped")
        mirror2.wait_for_status(imagespec=imagespec1, state_pattern="up+replaying")

        # Create clone of the image and verify for the image mirrored status
        clone2 = rbd.random_string()
        imagespec2 = poolname + "/" + clone2
        rbd.create_clone(snap_name, poolname, clone2)
        mirror2.wait_for_status(poolname=poolname, images_pattern=3)
        mirror2.image_exists(imagespec2)
        mirror1.wait_for_status(imagespec=imagespec2, state_pattern="up+stopped")
        mirror2.wait_for_status(imagespec=imagespec2, state_pattern="up+replaying")

        # clean up the configuration
        rbd.snap_remove(poolname, imagename, snap)
        mirror1.delete_image(imagespec1)
        mirror1.delete_image(imagespec2)
        mirror1.delete_image(imagespec)
        mirror1.clean_up(peercluster=mirror2, pools=[poolname])

        return 0

    except RbdBaseException as error:
        print(error.message)
        return 1
