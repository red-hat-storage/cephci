"""
Module to verify :
  -  Verify Addition/removal of Images to Consistency group (without namespace)

Test case covered:
CEPH-83611277 - Verify Addition/removal of Images to Consistency group

Pre-requisites :
1. Cluster must be up in 8.1 and above and running with capacity to create pool
2. We need atleast one client node with ceph-common package,
   conf and keyring files

Test Case Flow:
Step1: Deploy Two ceph cluster on version 8.1 or later
Step 2: Create RBD pool ‘pool_1’ on both sites
Step 3: Enable Image mode mirroring on pool_1 on both sites
Step 4: Bootstrap the storage cluster peers (Two-way)
Step 5: Create 2 RBD images in pool_1
Step 6: Add data to the images
Step 7: Calculate MD5sum of all files
Step 8: Create Consistency group
Step 9: Enable Mirroring for the group
Step 10: Add Images in the consistency group should throw error
Step 11: Disable Mirroring for the group
Step 12: Add Images in the consistency group should succeed
Step 13: Enable Mirroring for the group
Step 14: Remove image1 from pool_1 should throw error "cannot remove image from mirror enabled group"
Step 15: Disable Mirroring for the group
Step 16: Remove image1 from pool_1 should succeed
Step 17: Enable Mirroring for the group
Step 18: Check only image2 is replicated on site-b for pool_1
Step 19: Check group is replicated on site-b
Step 20: Check only image2 should be part of image list on site-b, image1 should not be present
Step 21: Check pool mirror status, image mirror status and group mirror status on both sites does not show
'unknown' or 'error' on both clusters
Step 22: Confirm that the global ids match for the groups and images on both clusters.
Step 23: Validate the integrity of the data on secondary site-b for image2
Step 24: Repeat above on EC pool with or without namespace
Step 25: Cleanup the images, file and pools

"""

import json
import random
import time
from copy import deepcopy

from ceph.rbd.initial_config import initial_mirror_config
from ceph.rbd.mirror_utils import (
    check_mirror_consistency,
    compare_image_size_primary_secondary,
)
from ceph.rbd.utils import getdict, random_string
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.group_mirror import (
    add_group_image_and_verify,
    disable_group_mirroring_and_verify_state,
    enable_group_mirroring_and_verify_state,
    group_mirror_status_verify,
    remove_group_image_and_verify,
    wait_for_idle,
)
from ceph.rbd.workflows.krbd_io_handler import krbd_io_handler
from ceph.rbd.workflows.namespace import enable_namespace_mirroring
from utility.log import Log

log = Log(__name__)


def test_add_remove_group_mirroring(
    rbd_primary,
    rbd_secondary,
    client_primary,
    client_secondary,
    primary_cluster,
    secondary_cluster,
    pool_types,
    **kw
):
    """
    Test user can successfully add/remove images only in a disabled mirror rbd group
    Args:
        rbd_primary: RBD object of primary cluster
        rbd_secondary: RBD object of secondary cluster
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
        log.info("Running test CEPH-83611277 for %s", pool_type)

        # FIO Params Required for ODF workload exclusively in group mirroring
        fio = kw.get("config", {}).get("fio", {})
        io_config = {
            "size": fio["size"],
            "do_not_create_image": True,
            "runtime": fio["runtime"],
            "num_jobs": fio["ODF_CONFIG"]["num_jobs"],
            "iodepth": fio["ODF_CONFIG"]["iodepth"],
            "rwmixread": fio["ODF_CONFIG"]["rwmixread"],
            "direct": fio["ODF_CONFIG"]["direct"],
            "invalidate": fio["ODF_CONFIG"]["invalidate"],
            "config": {
                "file_size": fio["size"],
                "file_path": [
                    "/mnt/mnt_" + random_string(len=5) + "/file",
                    "/mnt/mnt_" + random_string(len=5) + "/file",
                ],
                "get_time_taken": True,
                "operations": {
                    "fs": "ext4",
                    "io": True,
                    "mount": True,
                    "map": True,
                },
                "cmd_timeout": 2400,
                "io_type": fio["ODF_CONFIG"]["io_type"],
            },
        }

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
                    else:
                        image_spec.append(pool + "/" + image)
            if "namespace" in pool_config:
                enable_namespace_mirroring(
                    rbd_primary, rbd_secondary, pool, **pool_config
                )
                pool_spec = pool + "/" + pool_config.get("namespace")
            else:
                pool_spec = pool
            image_spec_copy = deepcopy(image_spec)
            io_config["rbd_obj"] = rbd_primary
            io_config["client"] = client_primary
            io_config["config"]["image_spec"] = image_spec_copy
            io, err = krbd_io_handler(**io_config)
            if err:
                raise Exception("Map, mount and run IOs failed for " + str(image_spec))
            else:
                log.info("Map, mount and IOs successful for " + str(image_spec))

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
                + pool_config.get("group-spec")
                + " mirroring state is "
                + mirror_state
            )

            # Create Image_standalone in same pool
            image_size = kw.get("config", {}).get(pool_type, {}).get("size")
            image_name = "image_standalone"
            standalone_image_spec = pool_spec + "/" + image_name
            image_create_status, err = rbd_primary.create(
                **{"image-spec": standalone_image_spec, "size": image_size}
            )
            if err:
                raise Exception(
                    "Failed in enabling mirroring on standlaone image: "
                    + standalone_image_spec
                    + ", err: "
                    + err
                )
            log.info(
                "Successfully created standalone image for mirroring: "
                + image_create_status
            )

            # Run IO on image
            io_config["rbd_obj"] = rbd_primary
            io_config["client"] = client_primary

            io, err = krbd_io_handler(**io_config)
            if err:
                raise Exception(
                    "Map, mount and run IOs failed for " + standalone_image_spec
                )
            else:
                log.info("Map, mount and IOs successful for " + standalone_image_spec)

            # Enable mirroring on Image
            image_mirror_status, err = rbd_primary.mirror.image.enable(
                **{"image-spec": standalone_image_spec, "mode": "snapshot"}
            )
            if err:
                raise Exception(
                    "Failed in enabling mirroring on standlaone image: "
                    + standalone_image_spec
                    + " , err: "
                    + err
                )
            log.info("Successfully enabled mirroring on standalone image")

            # Verify if Image mirroring state is achieved
            retry = 0
            while retry < 10:
                image_mirror_status, err = rbd_primary.mirror.image.status(
                    **{"image-spec": standalone_image_spec, "format": "json"}
                )
                if err:
                    raise Exception(
                        "Failed to get image status on standlaone image: "
                        + standalone_image_spec
                        + " , err: "
                        + err
                    )
                image_state = json.loads(image_mirror_status)["state"]
                if image_state == "up+stopped":
                    break
                else:
                    time.sleep(5)
                    retry = retry + 1

            if retry == 10:
                raise Exception(
                    "Expected state 'up+stopped' not achieved , err: " + err
                )
            log.info(
                "Successfully verified state of image_standalone mirroring is up+stopped"
            )

            # Try adding mirrored enabled image to group (Should FAIL)
            try:
                add_group_image_and_verify(
                    rbd_primary,
                    **{
                        "group-spec": group_config.get("group-spec"),
                        "image-spec": standalone_image_spec,
                    },
                )
                raise Exception(
                    "Addition of image to mirror enabled group should not have succeeded"
                )
            except Exception as e:
                if "cannot add mirror enabled image to group" in str(e):
                    log.info(
                        "Successfully verified mirror enabled image can not be added to group"
                    )
                else:
                    raise Exception("Add group image failed with " + e)

            # Verfy size for image_standalone should match across both clusters
            if "namespace" in pool_config:
                image_spec = [
                    {
                        "image": image_name,
                        "pool": pool,
                        "namespace": pool_config.get("namespace"),
                    }
                ]
            else:
                image_spec = [{"image": image_name, "pool": pool}]
            image_spec = json.dumps(image_spec)
            compare_image_size_primary_secondary(rbd_primary, rbd_secondary, image_spec)
            log.info(
                "Successfully verified image size matches across both clusters for standalone mirrored image"
            )

            # Verify MD5sum should match for image_standalone across both clusters
            check_mirror_consistency(
                rbd_primary,
                rbd_secondary,
                client_primary,
                client_secondary,
                image_spec,
            )
            log.info(
                "Successfully verified md5sum of standalone mirrored image across both clusters"
            )

            # Enable Group Mirroring and Verify
            if mirror_state == "Disabled":
                enable_group_mirroring_and_verify_state(rbd_primary, **group_config)
                log.info("Successfully Enabled group mirroring")
                mirror_state = "Enabled"

            # Wait for group mirroring to complete
            wait_for_idle(rbd_primary, **group_config)
            log.info("Successfully completed group mirroring syncing to secondary")

            # Remove RBD image 'image1' from group # Should FAIL
            group_image_kw = {
                "group-spec": group_config["group-spec"],
                "image-spec": io_config["config"]["image_spec"][0],
            }
            try:
                remove_group_image_and_verify(rbd_primary, **group_image_kw)
                raise Exception(
                    "Image should not have removed successfully when group mirroring is enabled"
                )
            except Exception as e:
                if "cannot remove image from mirror enabled group" in str(e):
                    log.info(
                        "Successfully verified image is not removed from group when group mirroring is enabled"
                    )
                else:
                    raise Exception("Remove group image failed with " + e)

            # Disable Mirroring
            if mirror_state == "Enabled":
                disable_group_mirroring_and_verify_state(rbd_primary, **group_config)
                mirror_state = "Disabled"

            # Remove RBD image 'image1' from group # Should Succeed
            remove_group_image_and_verify(rbd_primary, **group_image_kw)
            log.info(
                "Successfully verified image is removed from group when group mirroring is disabled"
            )

            # Enable Mirroring
            if mirror_state == "Disabled":
                enable_group_mirroring_and_verify_state(rbd_primary, **group_config)
                mirror_state = "Enabled"

            # Add rbd image 'image1' to group # Should FAIL
            try:
                add_group_image_and_verify(rbd_primary, **group_image_kw)
                raise Exception(
                    "Image should not have been added successfully when group mirroring is enabled"
                )
            except Exception as e:
                if "cannot add image to mirror enabled group" in str(e):
                    log.info(
                        "Successfully verified image is not added to the group when group mirroring is enabled"
                    )
                else:
                    raise Exception("Add group image failed with " + e)

            # Disable Mirroring
            if mirror_state == "Enabled":
                disable_group_mirroring_and_verify_state(rbd_primary, **group_config)
                mirror_state = "Disabled"

            # Add rbd image 'image1' to group # Should Succeed
            add_group_image_and_verify(rbd_primary, **group_image_kw)
            log.info(
                "Successfully verified image is added to the group when group mirroring is disabled"
            )

            # Enable mirroring
            if mirror_state == "Disabled":
                enable_group_mirroring_and_verify_state(rbd_primary, **group_config)
                mirror_state = "Enabled"
            log.info("Successfully Enabled group mirroring")

            # Wait for group mirroring to complete
            wait_for_idle(rbd_primary, **group_config)
            log.info("Successfully completed group mirroring sync to secondary")

            # Validate size of each image should be same on site-a and site-b
            group_image_list, err = rbd_primary.group.image.list(
                **group_config, format="json"
            )
            if err:
                raise Exception("Getting group image list failed : " + str(err))
            compare_image_size_primary_secondary(
                rbd_primary, rbd_secondary, group_image_list
            )
            log.info("Successfully verified image size matches across both clusters")

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
            log.info("Successfully verified group is present on secondary site")

            # Check whether images are part of correct group on site-b using group image-list
            group_image_list_secondary, err = rbd_secondary.group.image.list(
                **group_config, format="json"
            )
            if err:
                raise Exception(
                    "Getting group image list failed for"
                    + group_config["group-spec"]
                    + " : "
                    + str(err)
                )

            if json.loads(group_image_list) != json.loads(group_image_list_secondary):
                raise Exception(
                    "Group image list does not match for primary and secondary cluster"
                )
            log.info(
                "Successfully verified image list matches for the group on both clusters"
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
            )
            log.info(
                "Successfully verified group status. Verified States Primary: up+stopped, Secondary: up+replaying"
            )

            # Validate the integrity of the data on secondary site-b
            check_mirror_consistency(
                rbd_primary,
                rbd_secondary,
                client_primary,
                client_secondary,
                group_image_list,
            )
            log.info("Successfully verified md5sum of all images across both clusters")


def run(**kw):
    """
    This test verify that a user can successfully add/remove images from disabled mirror rbd group
    Args:
        kw: test data
    Returns:
        int: The return value. 0 for success, 1 otherwise

    """
    try:
        log.info("Running Consistency Group Mirroring across two clusters")
        pool_types = ["rep_pool_config", "ec_pool_config"]
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

        test_add_remove_group_mirroring(
            rbd_primary,
            rbd_secondary,
            client_primary,
            client_secondary,
            primary_cluster,
            secondary_cluster,
            pool_types,
            **kw,
        )

        log.info("Test add/remove images while group mirroring passed")

    except Exception as e:
        log.error(
            "Test: Add/Remove in RBD group mirroring (snapshot mode) across two clusters failed: "
            + str(e)
        )
        return 1

    finally:
        cleanup(pool_types=pool_types, multi_cluster_obj=mirror_obj, **kw)

    return 0
