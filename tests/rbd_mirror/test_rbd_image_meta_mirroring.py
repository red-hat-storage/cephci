import time

from tests.rbd.rbd_utils import Rbd
from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from utility.log import Log

log = Log(__name__)


def run(**kw):
    """Verification mirroring of image meta operations.

    This module sets, modifies, removes rbd image meta (key, value) and
    verifies its mirroring in both in journal based and snapshot based mirroring.

    Args:
        **kw:
    Returns:
        0 - if test case pass
        1 - it test case fails

    Test case covered -
    CEPH-9524 - [Rbd Mirror ] Change the metadata of the primary image using "image-meta set"
                    and verify the mirrored image on secondary reflects this change

    Pre-requisites :
    1. Two Clusters must be up and running with capacity to create pool
       (At least with 64 pgs).
    2. We need atleast one client node with ceph-common package,
       conf and keyring files on each node.

    Test Case Flow:
    1. Create a pool on both clusters and enable mirroring in image mode.
    2. Configure mirroring (peer bootstrap) between two clusters.
    2. Create two images, enable journal and snapshot based mirroring one of each images respectively.
    3. For each image, add, modify, remove image meta and check peer cluster has got the updates by
            checking meta in the peer cluster.
    """
    try:
        log.info("Starting RBD mirroring test case")
        config = kw.get("config")
        mirror1, mirror2 = [
            rbdmirror.RbdMirror(cluster, config)
            for cluster in kw.get("ceph_cluster_dict").values()
        ]
        rbd1, rbd2 = [
            Rbd(**kw, req_cname=cluster_name)
            for cluster_name in kw.get("ceph_cluster_dict").keys()
        ]

        poolname = "rbd_mirror_pool"
        imagename = "journal_mirrored"
        imagespec = f"{poolname}/{imagename}"
        imagename_1 = "snap_mirrored"
        imagespec_1 = f"{poolname}/{imagename_1}"

        mirror1.create_pool(poolname=poolname)
        mirror2.create_pool(poolname=poolname)
        mirror1.create_image(imagespec=imagespec, size=config.get("imagesize"))
        mirror1.config_mirror(mirror2, poolname=poolname, mode="image")
        mirror1.enable_mirror_image(poolname, imagename, "journal")
        mirror2.wait_for_status(poolname=poolname, images_pattern=1)
        mirror1.benchwrite(imagespec=imagespec, io=config.get("io-total"))
        mirror1.wait_for_status(imagespec=imagespec, state_pattern="up+stopped")
        mirror2.wait_for_status(imagespec=imagespec, state_pattern="up+replaying")

        # Create image and enable snapshot mirroring"
        mirror1.create_image(imagespec=imagespec_1, size=config.get("imagesize"))
        mirror1.enable_mirror_image(poolname, imagename_1, "snapshot")
        mirror2.wait_for_status(poolname=poolname, images_pattern=2)
        mirror1.benchwrite(imagespec=imagespec_1, io=config.get("io-total"))
        mirror1.wait_for_status(imagespec=imagespec_1, state_pattern="up+stopped")
        mirror2.wait_for_status(imagespec=imagespec_1, state_pattern="up+replaying")

        key = config.get("key")
        value = config.get("value")
        for i in imagespec, imagespec_1:
            # Add meta value
            rbd1.image_meta(action="set", image_spec=i, key=key, value=value)
            if i == imagespec_1:
                mirror1.create_mirror_snapshot(i)
            time.sleep(30)

            # Verify value at secondary
            if value != rbd2.image_meta(action="get", image_spec=i, key=key)[:-1]:
                log.error(f"Meta value did not get mirrored on {i}")
                return 1

            # change metadata
            rbd1.image_meta(action="set", image_spec=i, key=key, value="changed")
            if i == imagespec_1:
                mirror1.create_mirror_snapshot(i)
            time.sleep(30)

            # Verify value at secondary
            if "changed" != rbd2.image_meta(action="get", image_spec=i, key=key)[:-1]:
                log.error(f"Change in meta value did not get mirrored on {i}")
                return 1

            # Remove from primary
            rbd1.image_meta(action="remove", image_spec=i, key=key)
            if i == imagespec_1:
                mirror1.create_mirror_snapshot(i)
            time.sleep(30)

            # Verify removal at secondary
            if type(rbd2.image_meta(action="get", image_spec=i, key=key)) == str:
                log.error(f"Meta key removal did not get mirrored on {i}")

        return 0

    except ValueError:
        log.error(
            f"{kw.get('ceph_cluster_dict').values} has less or more clusters Than Expected(2 clusters expected)"
        )

    except Exception as e:
        log.error(e)
        return 1
