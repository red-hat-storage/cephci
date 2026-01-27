"""
Module to verify :
  - Verify group mirroring for images with 0 size & Verify group mirroring with 0 images in group

Test case covered:
CEPH-83613272 - Verify group mirroring for images with 0 size & Verify group mirroring with 0 images in group

Pre-requisites :
1. Cluster must be up in 8.1 and above and running with capacity to create pool
2. We need atleast one client node with ceph-common package,
   conf and keyring files

TC#1: Test Case Flow:
Step1: Deploy Two ceph cluster on version 8.1 or later 	Deployment should be successful
Step 2: Create RBD pool 'pool_1' on both sites
Step 3: Enable Image mode mirroring on pool_1 on both sites
Step 4: Bootstrap the storage cluster peers (Two-way)
Step 5: Create 1 RBD image with size 0 in pool_1
Step 6: Calculate MD5sum of all files
Step 7: Create 2 Consistency group
Step 8: Add Images in the consistency group
Step 9: Enable Mirroring for the group
Step 10: Wait for mirroring to complete
Step 11: Check all image is mirrored on site-b
Step 12: Validate size of each image should be same on site-a and site-b
Step 13: Check group is mirrored on site-b
Step 14: Check whether images are part of correct group on site-b
Step 15: Check pool mirror status, image mirror status and group mirror status on both sites
does not show 'unknown' or 'error' on both clusters
Step 16: Confirm that the global ids match for the groups and images on both clusters.
Step 17: Validate the integrity of the data on secondary site-b
Step 18: Repeat above on EC pool
Step 19: Repeat above on EC pool with or without namespace
Step 20: Cleanup the images, file and pools

"""

import json
import random
from copy import deepcopy

from ceph.rbd.initial_config import initial_mirror_config
from ceph.rbd.mirror_utils import (
    check_mirror_consistency,
    compare_image_size_primary_secondary,
)
from ceph.rbd.utils import getdict
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.group_mirror import (
    disable_group_mirroring_and_verify_state,
    enable_group_mirroring_and_verify_state,
    group_mirror_status_verify,
    remove_group_image_and_verify,
    wait_for_idle,
)
from ceph.rbd.workflows.namespace import enable_namespace_mirroring
from utility.log import Log

log = Log(__name__)


def test_group_mirroring_with_zero_size(
    rbd_primary,
    rbd_secondary,
    client_primary,
    client_secondary,
    primary_cluster,
    secondary_cluster,
    pool_types,
    **kw,
):
    """
    Verify group mirroring for images with 0 size & Verify group mirroring with 0 images in group
    Args:
        rbd_primary: RBD object of primary cluster
        rbd_secondary: RBD objevct of secondary cluster
        client_primary: client node object of primary cluster
        client_secondary: client node object of secondary cluster
        primary_cluster: Primary cluster object
        secondary_cluster: Secondary cluster object
        pool_types: Replication pool or EC pool
        **kw: any other arguments
    """

    for pool_type in pool_types:
        rbd_config = kw.get("config", {}).get(pool_type, {})
        multi_pool_config = deepcopy(getdict(rbd_config))
        log.info("Running test CEPH-83613272 for %s", pool_type)

        for pool, pool_config in multi_pool_config.items():
            group_config = {}
            if "data_pool" in pool_config.keys():
                _ = pool_config.pop("data_pool")

            group_spec = pool_config.get("group-spec")
            group_config.update({"group-spec": group_spec})
            image_spec = []
            for image, image_config in pool_config.items():
                if "image" in image:
                    if "namespace" in pool_config:
                        image_spec.append(
                            pool + "/" + pool_config.get("namespace") + "/" + image
                        )
                        enable_namespace_mirroring(
                            rbd_primary, rbd_secondary, pool, **pool_config
                        )
                    else:
                        image_spec.append(pool + "/" + image)

            # Get Group Mirroring Status
            group_mirror_status, err = rbd_primary.mirror.group.status(**group_config)
            if err:
                if "mirroring not enabled on the group" in err:
                    mirror_state = "Disabled"
                else:
                    raise Exception("Getting group mirror status failed : " + str(err))
            else:
                mirror_state = "Enabled"
            log.info(
                "Group "
                + group_config["group-spec"]
                + " mirroring state is "
                + mirror_state
            )

            # Enable Group Mirroring and Verify
            if mirror_state == "Disabled":
                enable_group_mirroring_and_verify_state(rbd_primary, **group_config)
            log.info("Successfully Enabled group mirroring with image of zero size")

            # Wait for group mirroring to complete
            wait_for_idle(rbd_primary, **group_config)
            log.info(
                "Successfully completed sync for group mirroring to secondary site"
            )

            # Validate size of each image should be same on site-a and site-b
            group_image_list, err = rbd_primary.group.image.list(
                **group_config, format="json"
            )
            if err:
                raise Exception("Getting group image list failed : " + str(err))

            compare_image_size_primary_secondary(
                rbd_primary, rbd_secondary, group_image_list
            )
            log.info(
                "Successfully verified size of rbd images is zero across both clusters"
            )

            # Check group is replicated on site-b using group info
            group_info_status, err = rbd_secondary.group.info(
                **group_config, format="json"
            )
            if err:
                raise Exception("Getting group info failed : " + str(err))
            if (
                json.loads(group_info_status)["group_name"] != pool_config.get("group")
                or json.loads(group_info_status)["mirroring"]["state"] != "enabled"
                or json.loads(group_info_status)["mirroring"]["mode"] != "snapshot"
                or json.loads(group_info_status)["mirroring"]["primary"]
            ):
                raise Exception("group info is not as expected on secondary cluster")
            log.info("Successfully verified group is present on secondary cluster")

            # Check whether images are part of correct group on site-b using group image-list
            group_image_list_primary, err = rbd_primary.group.image.list(
                **group_config, format="json"
            )
            group_image_list_secondary, err = rbd_secondary.group.image.list(
                **group_config, format="json"
            )
            if err:
                raise Exception("Getting group image list failed : " + str(err))
            if json.loads(group_image_list_primary) != json.loads(
                group_image_list_secondary
            ):
                raise Exception(
                    "Group image list does not match for primary and secondary cluster"
                )
            log.info(
                "Successfully verified image list for the group matches across both cluster"
            )

            # Verify group mirroring status on both clusters & Match global id of both cluster
            group_mirror_status_verify(
                primary_cluster,
                secondary_cluster,
                rbd_primary,
                rbd_secondary,
                primary_state="up+stopped",
                secondary_state="up+replaying",
                **group_config,
                global_id=False,
            )
            log.info(
                "Successfully verified group status."
                "Primary Group Status: up+stopped, Secondary Group status: up+replaying"
            )

            # Validate the integrity of the data on secondary site-b
            check_mirror_consistency(
                rbd_primary,
                rbd_secondary,
                client_primary,
                client_secondary,
                group_image_list,
            )
            log.info(
                "Successfully verified md5sum of zero size image matches across both clusters"
            )

            # Verify Group with Zero images can be mirrored
            log.info("Verify Group with Zero images can be mirrored")
            log.info("Remove image from group for an empty group")
            disable_group_mirroring_and_verify_state(rbd_primary, **group_config)
            group_image_kw = {
                "group-spec": group_config["group-spec"],
                "image-spec": image_spec[0],
            }
            remove_group_image_and_verify(rbd_primary, **group_image_kw)
            log.info("Successfully verified image is removed from group")
            enable_group_mirroring_and_verify_state(rbd_primary, **group_config)

            # Check whether images are part of correct group on site-b using group image-list
            group_image_list_primary, err = rbd_primary.group.image.list(
                **group_config, format="json"
            )
            group_image_list_secondary, err = rbd_secondary.group.image.list(
                **group_config, format="json"
            )
            if err:
                raise Exception("Getting group image list failed : " + str(err))
            if json.loads(group_image_list_primary) != json.loads(
                group_image_list_secondary
            ):
                raise Exception(
                    "Group image list does not match for primary and secondary cluster"
                )
            log.info(
                "Successfully verified image list for the group matches across both cluster"
            )


def run(**kw):
    """
    Verify group mirroring for images with 0 size & Verify group mirroring with 0 images in group
    Args:
        kw: test data
    Returns:
        int: The return value. 0 for success, 1 otherwise

    """
    try:
        pool_types = ["rep_pool_config", "ec_pool_config"]
        log.info("Running Consistency Group Mirroring with zero size image")
        grouptypes = ["single_pool_without_namespace", "single_pool_with_namespace"]
        if not kw.get("config").get("grouptype"):
            for pooltype in pool_types:
                group_type = grouptypes.pop(random.randrange(len(grouptypes)))
                kw.get("config").get(pooltype).update({"grouptype": group_type})
                log.info("Choosing Group type on %s - %s", pooltype, group_type)

        mirror_obj = initial_mirror_config(**kw)
        mirror_obj.pop("output", [])
        for val in mirror_obj.values():
            if not val.get("is_secondary", False):
                rbd_primary = val.get("rbd")
                client_primary = val.get("client")
                primary_cluster = val.get("cluster")
            else:
                rbd_secondary = val.get("rbd")
                client_secondary = val.get("client")
                secondary_cluster = val.get("cluster")

        pool_types = list(mirror_obj.values())[0].get("pool_types")

        test_group_mirroring_with_zero_size(
            rbd_primary,
            rbd_secondary,
            client_primary,
            client_secondary,
            primary_cluster,
            secondary_cluster,
            pool_types,
            **kw,
        )
        log.info(
            "Test Consistency Group Mirroring with zero size image passed successfully"
        )

    except Exception as e:
        log.error(
            "Test Consistency Group Mirroring with zero size image failed: " + str(e)
        )
        return 1

    finally:
        cleanup(pool_types=pool_types, multi_cluster_obj=mirror_obj, **kw)

    return 0
