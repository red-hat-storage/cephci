"""Test case covered -
    CEPH-9476- Primary cluster permanent failure,
    Recreate the primary cluster and Re-establish mirror with newly created cluster as Primary.
    Pre-requisites :
    1. Cluster must be up and running with capacity to create pool
    (At least with 64 pgs)
    2. We need atleast one client node with ceph-common package,
    conf and keyring files

    Test case flows:
    1) Create a pool with same name as secondary pool has
    1) Perform the mirroring bootstrap for cluster peers
    2) copy and import bootstrap token to peer cluster
    3) verify peer cluster got added successfully after failback
    4) verify all the images from secondary mirrored to primary
"""
import time

from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from utility.log import Log

log = Log(__name__)


def run(**kw):
    """Verification of rbd mirroring failback scenarios on primary site,
    This module configures the mirroring back to failback cluster.

    Args:
        kw: test data
        Example::
            config:
                poolname: pool_failback
    Returns:
        int: The return value - 0 for success, 1 otherwise
    """
    try:
        log.info("Re-establishing mirror with newly created cluster as Primary")
        config = kw.get("config")
        mirror1 = rbdmirror.RbdMirror(
            kw.get("ceph_cluster_dict").get("ceph-rbd3"), config
        )
        mirror2 = rbdmirror.RbdMirror(
            kw.get("ceph_cluster_dict").get("ceph-rbd2"), config
        )
        poolname = kw.get("poolname")
        mirror1.create_pool(poolname=poolname)
        mirror1.config_mirror(mirror2, poolname=poolname, mode="image")
        mirror1.wait_for_status(poolname=poolname, health_pattern="OK")
        mirror1.wait_for_replay_complete()
        state_after_demote = "up+stopped" if mirror1.ceph_version < 3 else "up+unknown"
        mirror1_image_list = mirror1.list_images(poolname)
        mirror2_image_list = mirror2.list_images(poolname)
        if mirror1_image_list.sort() == mirror2_image_list.sort():
            log.info(
                "After failback, secondary cluster data successfuly migrated to primary cluster"
            )
        else:
            log.error(
                "After failback, secondary cluster data are not successfuly migrated to primary cluster"
            )
            return 1
        # To run I/O on only first image
        is_first_image = True
        for imagename in mirror1_image_list:
            imagespec = poolname + "/" + imagename
            if is_first_image:
                mirror2.benchwrite(imagespec=imagespec, io=config.get("io-total"))
                is_first_image = False
                time.sleep(60)
                mirror2.check_data(peercluster=mirror1, imagespec=imagespec)
            mirror2.demote(imagespec=imagespec)
            mirror2.wait_for_status(
                imagespec=imagespec, state_pattern=state_after_demote
            )
            mirror1.wait_for_status(
                imagespec=imagespec, state_pattern=state_after_demote
            )
            mirror1.promote(imagespec=imagespec)
        return 0

    except Exception as e:
        log.exception(e)
        return 1

    finally:
        if not kw.get("config").get("do_not_cleanup_pool"):
            mirror1.clean_up(peercluster=mirror2, pools=[poolname])
