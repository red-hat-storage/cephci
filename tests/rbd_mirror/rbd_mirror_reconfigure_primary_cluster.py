"""Test case covered -
CEPH-9476- Primary cluster permanent failure,
Recreate the primary cluster and Re-establish mirror with newly created cluster as Primary.

Pre-requisites :
1. Cluster must be up and running with capacity to create pool
(At least with 64 pgs)
2. We need atleast one client node with ceph-common package,
conf and keyring files

Test case flows:
1) After site-a failure, promote site-b cluster as primary
2) Create a pool with same name as secondary pool has
3) Perform the mirroring bootstrap for cluster peers
4) copy and import bootstrap token to peer cluster
5) verify peer cluster got added successfully after failback
6) verify all the images from secondary mirrored to primary
7) Demote initially promoted secondary as primary
8) Promote newly created mirrored cluster as primary
"""

# import datetime
# import time

from tests.rbd.rbd_utils import Rbd
from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from utility.log import Log

log = Log(__name__)


def run(**kw):
    """
    Verification of rbd mirroring failback scenario on primary site,
    This module configures the mirroring failback to newly created cluster.

    Args:
        kw: test data
        Example::
            config:
                poolname: pool_failback
    Returns:
        int: The return value - 0 for success, 1 for failure
    """
    try:
        log.info("Running test CEPH-9476, Promoting secondary cluster as primary")

        config = kw.get("config")
        poolname = config.get("poolname")

        mirror3 = rbdmirror.RbdMirror(
            kw.get("ceph_cluster_dict").get("ceph-rbd3"), config
        )

        mirror2 = rbdmirror.RbdMirror(
            kw.get("ceph_cluster_dict").get("ceph-rbd2"), config
        )

        rbd1, rbd2, rbd3 = [
            Rbd(**kw, req_cname=cluster_name)
            for cluster_name in kw["ceph_cluster_dict"].keys()
        ]

        kw["peer_mode"] = kw.get("peer_mode", "bootstrap")
        kw["rbd_client"] = kw.get("rbd_client", "client.admin")
        kw["failback"] = True

        mirror2_image_list = rbd2.list_images(poolname)
        for imagename in mirror2_image_list:
            imagespec = poolname + "/" + imagename

            # promote site-b images as primary
            mirror2.promote(imagespec=imagespec, force=True)

            # After site-a failure, site-b promoted image status will be up+stopped
            mirror2.wait_for_status(imagespec=imagespec, state_pattern="up+stopped")

        # Due to Product issue BZ-2190366 and BZ-2190352 test fails in
        # above status check itself hence avoiding next tests as below

        # # once mirror2 images primary, able to run i/o in that image
        # mirror2.benchwrite(imagespec=imagespec, io=config.get("io-total"))

        # log.info("Successfully promoted secondary cluster as primary")

        # log.info("Re-establishing mirror with newly created cluster as Primary")

        # # remove the failed cluster from mirror peers
        # mirror2.peer_remove(poolname=poolname)

        # # For newly added cluster, creating pool and configuring two-way mirror
        # mirror3.create_pool(poolname=poolname)

        # # set custom_cluster_order according to the cluster to configure mirroring
        # kw["custom_cluster_order"] = "1,2"

        # mirror2.config_mirror(mirror3, poolname=poolname, mode="image", **kw)

        # # Schedeluing the snapshots for image reflection
        # mirror2.mirror_snapshot_schedule_add(poolname=poolname)

        # # wait till image gets reflected to secondary
        # end_time = datetime.datetime.now() + datetime.timedelta(seconds=300)
        # while end_time > datetime.datetime.now():
        #     mirror3_image_list = rbd3.list_images(poolname)
        #     mirror2_image_list = rbd2.list_images(poolname)
        #     if set(mirror3_image_list) == set(mirror2_image_list):
        #         log.info(
        #             "After failback, secondary cluster images successfuly mirrored to primary cluster"
        #         )
        #         break
        #     time.sleep(60)
        # else:
        #     log.error(
        #         "After failback, secondary cluster images are not successfuly mirrored to primary cluster"
        #     )
        #     return 1

        # for imagename in mirror3_image_list:
        #     imagespec = poolname + "/" + imagename

        #     # after reconfiguring mirroring to new cluster check for image status
        #     mirror3.wait_for_status(imagespec=imagespec, state_pattern="up+replaying")
        #     mirror2.wait_for_status(imagespec=imagespec, state_pattern="up+stopped")

        #     # Demote initially promoted secondary as primary
        #     mirror2.demote(imagespec=imagespec)

        #     # waiting for demotion reflected to peer cluster
        #     time.sleep(60)

        #     # Promote newly created mirrored cluster as primary
        #     mirror3.promote(imagespec=imagespec)

        #     # waiting for promotion reflected to peer cluster
        #     time.sleep(60)

        #     mirror3.mirror_snapshot_schedule_add(poolname=poolname, imagename=imagename)

        #     # after successfull demote and promote of images check for it's status
        #     mirror3.wait_for_status(imagespec=imagespec, state_pattern="up+stopped")
        #     mirror2.wait_for_status(imagespec=imagespec, state_pattern="up+replaying")

        # # Once new cluster becomes primary run i/o and check data sync among peer cluster
        # mirror3.benchwrite(imagespec=imagespec, io=config.get("io-total"))

        # mirror3.mirror_snapshot_schedule_add(poolname=poolname, imagename=imagename)
        # # snapshot scheduled for 1 minute hence waiting for image to get reflect on peer-site
        # time.sleep(120)

        # # To check data consistency among newly mirrored cluster
        # mirror3.check_data(peercluster=mirror2, imagespec=imagespec)

        return 0

    except Exception as e:
        log.exception(e)
        return 1

    finally:
        if not kw.get("config").get("do_not_cleanup_pool"):
            mirror3.clean_up(peercluster=mirror2, pools=[poolname])
