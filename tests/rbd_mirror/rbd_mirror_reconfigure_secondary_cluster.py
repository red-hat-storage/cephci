"""Test case covered -
CEPH-9477- Secondary cluster permanent failure,
Recreate the secondary cluster and Re-establish mirror with newly created cluster as secondary.

Pre-requisites :
1. Cluster must be up and running with capacity to create pool
(At least with 64 pgs)
2. We need atleast one client node with ceph-common package,
conf and keyring files

Test case flows:
1) After site-b failure, bring up new cluster for secondary
2) Create a pool with same name as secondary pool
3) Perform the mirroring bootstrap for cluster peers
4) copy and import bootstrap token to peer cluster
5) verify peer cluster got added successfully after failover
6) verify all the images from primary mirrored to secondary
"""

import ast
import datetime
import time

from tests.rbd.rbd_utils import Rbd
from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from utility.log import Log

log = Log(__name__)


def run(**kw):
    """
    Verification of rbd mirroring failover scenario on secondary site,
    This module configures the mirroring to newly created cluster.

    Args:
        kw: test data
        Example::
            config:
                poolname: pool_failback
    Returns:
        int: The return value - 0 for success, 1 for failure
    """
    try:
        log.info(
            "Running test recreate secondary cluster in case of permanent failover"
        )

        config = kw.get("config")
        poolname = config.get("poolname")

        mirror3 = rbdmirror.RbdMirror(
            kw.get("ceph_cluster_dict").get("ceph-rbd3"), config
        )

        mirror1 = rbdmirror.RbdMirror(
            kw.get("ceph_cluster_dict").get("ceph-rbd1"), config
        )

        rbd1, _, rbd3 = [
            Rbd(**kw, req_cname=cluster_name)
            for cluster_name in kw["ceph_cluster_dict"].keys()
        ]

        kw["peer_mode"] = kw.get("peer_mode", "bootstrap")
        kw["rbd_client"] = kw.get("rbd_client", "client.admin")
        kw["failback"] = True

        log.info("Re-establishing mirror with newly created cluster as secondary")

        # remove the failed cluster from mirror peers
        mirror1.peer_remove(poolname=poolname)

        if not mirror1.mirror_info(poolname, "peers"):
            log.error("Peers not removed successfully")
            return 1

        # For newly added cluster, creating pool and configuring two-way mirror
        mirror3.create_pool(poolname=poolname)

        # set custom_cluster_order according to the cluster to configure mirroring
        # 0 indicates mirror1, 1 indicates mirror2 and 2 indicates mirror3
        kw["custom_cluster_order"] = "0,2"

        mirror1.config_mirror(
            mirror3,
            poolname=poolname,
            mode="image",
            start_rbd_mirror_primary=True,
            **kw,
        )

        mirror1_peers = ast.literal_eval(mirror1.mirror_info(poolname, "peers"))
        if mirror1_peers and "ceph-rbd3" in mirror1_peers[0]["site_name"]:
            log.info("New secondary cluster added as peer")
        else:
            log.error("Error while configuring peers to new secondary cluster")
            return 1

        mirror3_peers = ast.literal_eval(mirror3.mirror_info(poolname, "peers"))
        if mirror3_peers and "ceph-rbd1" in mirror3_peers[0]["site_name"]:
            log.info("New secondary cluster has primary as peer")
        else:
            log.error("Error while configuring peers to new secondary cluster")
            return 1

        # Schedeluing the snapshots for image reflection
        mirror1.mirror_snapshot_schedule_add(poolname=poolname)

        # wait till image gets reflected to secondary
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=300)
        while end_time > datetime.datetime.now():
            mirror3_image_list = rbd3.list_images(poolname)
            mirror1_image_list = rbd1.list_images(poolname)
            if set(mirror3_image_list) == set(mirror1_image_list):
                log.info(
                    "After failback, primary cluster images successfuly mirrored to new secondary cluster"
                )
                break
            time.sleep(60)
            for imagename in mirror1_image_list:
                mirror1.verify_snapshot_schedule(imagespec=f"{poolname}/{imagename}")
        else:
            log.error(
                "After failback, primary cluster images are not successfuly mirrored to new secondary cluster"
            )
            return 1

        for image in mirror1_image_list:
            image_info = rbd3.image_info(pool_name=poolname, image_name=image)
            if image_info.get("mirroring"):
                if (
                    not image_info["mirroring"]["primary"]
                    and image_info["mirroring"]["state"] == "enabled"
                    and image_info["mirroring"]["mode"] == "snapshot"
                ):
                    log.info(
                        f"Mirroring configured successfully for image\
                            {image} in the new secondary cluster"
                    )
                else:
                    log.error(
                        f"Mirroring configuration failed for image\
                            {image} in the new secondary cluster"
                    )
                    return 1
        return 0

    except Exception as e:
        log.exception(e)
        return 1

    finally:
        if not kw.get("config").get("do_not_cleanup_pool"):
            mirror3.clean_up(peercluster=mirror1, pools=[poolname])
