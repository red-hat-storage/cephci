"""
Module to verify :
TC#1 - Verify that a user can successfully mirror rbd group from primary to secondary sites
            Option: pool1/ns1/image1 & pool1/ns1/image2 (Images with namespaces)
TC#2 - Verify Addition and removal of namespace level Images to Consistency group
TC#3 - Verify Group Renaming while mirroring in progress (with namespace)

Test case covered:
CEPH-83613293 - Verify that a user can successfully mirror rbd group from primary to secondary sites (with namespace)
CEPH-83613294 - Verify Addition/removal of Images to Consistency group (with namespace level images)
CEPH-83613295 - Verify Group Renaming while mirroring in progress (with namespace)

Pre-requisites :
1. Cluster must be up in 8.1 and above and running with capacity to create pool
2. We need atleast one client node with ceph-common package,
   conf and keyring files

TC#1: Test Case Flow:
Step1: Deploy Two ceph cluster on version 8.1 and later ceph version
Step 2: Create RBD pool ‘pool_1’ on both sites
Step 3: Enable Image mode mirroring on pool_1 on both sites
Step 4: Bootstrap the storage cluster peers (Two-way)
Step 5: Create namespace ‘ns1’ on both sites
Step 6: Enable mirroring at namespace level on both sites
Step 7: Create 2 RBD images in pool_1/ns1
Step 8: Add data to the images using rbd bench or FIO
Step 9: Calculate MD5sum of all files
Step 10: Create Consistency group
Step 11: Add Images in the consistency group
Step 12: Enable Mirroring for the group
Step 13: Wait for mirroring to complete
Step 14: Check all image is mirrored on site-b
Step 15: Validate size of each image should be same on site-a and site-b
Step 16: Check group is mirrored on site-b
Step 17: Check whether images are part of correct group on site-b
Step 18: Check pool mirror status, image mirror status and group mirror status on both sites does
not show 'unknown' or 'error' on both clusters
Step 19: Confirm that the global ids match for the groups on both clusters.
Step 20: Validate the integrity of the data on secondary site-b
Step 21: Repeat above on EC pool
Step 22: Cleanup the images, file and pools

TC#2: Test Case Flow:
Step1: Deploy Two ceph cluster on version 8.1 or later 	Deployment should be successful
Step 2: Create RBD pool ‘pool_1’ on both sites
Step 3: Enable Image mode mirroring on pool_1 on both sites
Step 4: Bootstrap the storage cluster peers (Two-way)
Step 5: Create namespace ‘ns1’ on both sites
Step 6: Enable mirroring at namespace level on both sites
Step 7: Create 2 RBD images in pool_1
Step 8: Add data to the images
Step 9: Create standalone image in pool
Step 10: Run IO on image_standalone
Step 11: Enable mirroring on image
Step 12: Verify if Image mirroring state is achieved
Step 13: Try adding mirrored enabled image to group (Should FAIL)
Step 14: Verify size for image_standalone should match across both the clusters
Step 15: Verify MD5sum should match for image_standalone across both clusters
Step 16: Calculate MD5sum of all files
Step 17: Create Consistency group
Step 18: Enable Mirroring for the group
Step 19: Add Images in the consistency group should throw error
Step 20: Disable Mirroring for the group
Step 21: Add Images in the consistency group should succeed
Step 22: Enable Mirroring for the group
Step 23: Remove image1 from pool_1 should throw error "cannot remove image from mirror enabled group"
Step 24: Disable Mirroring for the group
Step 25: Remove image1 from pool_1 should succeed
Step 26: Enable Mirroring for the group
Step 27: Check only image2 is replicated on site-b for pool_1
Step 28: Check group is replicated on site-b
Step 29: Check only image2 should be part of image list on site-b, image1 should not be present
Step 30: Check pool mirror status, image mirror status and group mirror status on both sites does
not show 'unknown' or 'error' on both clusters
Step 31: Confirm that the global ids match for the groups and images on both clusters.
Step 32: Validate the integrity of the data on secondary site-b for image2
Step 33: Repeat above on EC pool
Step 34: Cleanup the images, file and pools

TC#3: Test Case Flow:
Step1: Deploy Two ceph cluster on version 8.1 or later
Step 2: Create RBD pool ‘pool_1’ on both sites
Step 3: Enable Image mode mirroring on pool_1 on both sites
Step 4: Bootstrap the storage cluster peers (Two-way)
Step 5: Create namespace ‘ns1’ on both sites
Step 6: Enable mirroring at namespace level on both sites
Step 7: Create 2 large RBD images in pool_1
Step 8: Add data to the images
Step 9: Calculate MD5sum of all files
Step 10: Create Consistency group
Step 11: Add Images in the consistency group
Step 12: Enable Mirroring for the group
Step 13: Rename group
Step 14: Wait for replication to complete, status should show idle for all images
Step 15: Check all images are replicated on site-b for pool_1
Step 16: Check new group is replicated on site-b
Step 17: Check all images should be part of image list on site-b
Step 18: Check pool mirror status, image mirror status and group mirror status on both sites does
not show 'unknown' or 'error' on both clusters
Step 19: Confirm that the global ids match for the groups and images on both clusters.
Step 20: Validate the integrity of the data on secondary site-b for image2
Step 21: Kill mirror daemon on site-a
Step 22: Disable mirroring on the group should succeed
Step 23: restart mirror daemon
Step 24: Repeat above on EC pool
Step 25: Cleanup the images, file and pools

"""

import json
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
from ceph.rbd.workflows.rbd_mirror import wait_for_status
from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from utility.log import Log

log = Log(__name__)


def test_namespace_group_mirroring(
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
    Test user can successfully mirror rbd group containing rbd images between primary
    and secondary sites (with namespace level images)
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
        log.info("Running test CEPH-83613293 for %s", pool_type)

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
                    "device_map": True,
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
                    image_spec.append(
                        pool + "/" + pool_config.get("namespace") + "/" + image
                    )
            image_spec_copy = deepcopy(image_spec)
            io_config["rbd_obj"] = rbd_primary
            io_config["client"] = client_primary
            io_config["config"]["image_spec"] = image_spec_copy
            (io, err) = krbd_io_handler(**io_config)
            if err:
                raise Exception("Map, mount and run IOs failed for " + str(image_spec))
            else:
                log.info("Map, mount and IOs successful for " + str(image_spec))

            # Enable image mode mirroring on namespace
            if enable_namespace_mirroring(
                rbd_primary, rbd_secondary, pool, **pool_config
            ):
                raise Exception("Failed to enable namespace level mirroring")

            # Get Group Mirroring Status
            (group_mirror_status, err) = rbd_primary.mirror.group.status(**group_config)
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
                + " and group mirror status is "
                + group_mirror_status
            )

            # Enable Group Mirroring and Verify
            if mirror_state == "Disabled":
                enable_group_mirroring_and_verify_state(rbd_primary, **group_config)
            log.info("Successfully Enabled group mirroring")

            # Wait for group mirroring to complete
            wait_for_idle(rbd_primary, **group_config)
            log.info(
                "Successfully completed sync for group mirroring to secondary site"
            )

            # Validate size of each image should be same on site-a and site-b
            (group_image_list, err) = rbd_primary.group.image.list(
                **group_config, format="json"
            )
            if err:
                raise Exception("Getting group image list failed : " + str(err))
            compare_image_size_primary_secondary(
                rbd_primary, rbd_secondary, group_image_list
            )
            log.info(
                "Successfully verified size of rbd images matches across both clusters"
            )

            # Check group is replicated on site-b using group info
            (group_info_status, err) = rbd_secondary.group.info(
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
                raise Exception(
                    "group info is not as expected on secondary cluster "
                    + group_info_status
                )
            log.info("Successfully verified group is present on secondary cluster")

            # Check whether images are part of correct group on site-b using group image-list
            (group_image_list_secondary, err) = rbd_secondary.group.image.list(
                **group_config, format="json"
            )
            if err:
                raise Exception("Getting group image list failed : " + str(err))
            if json.loads(group_image_list) != json.loads(group_image_list_secondary):
                raise Exception(
                    "Group image list does not match for primary and secondary cluster "
                    + group_image_list
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
                global_id=True,
            )
            log.info(
                "Successfully verified group status. Verified State primary: up+stopped, Secondary: up+replaying"
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
                "Successfully verified md5sum of all images matches across both clusters"
            )


def test_add_remove_group_mirroring(
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
    Test user can successfully add/remove namespace images only in a disabled mirror rbd group
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
        log.info("Running test CEPH-83613294  for %s", pool_type)

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
                    image_spec.append(
                        pool + "/" + pool_config.get("namespace") + "/" + image
                    )
            image_spec_copy = deepcopy(image_spec)
            io_config["rbd_obj"] = rbd_primary
            io_config["client"] = client_primary
            io_config["config"]["image_spec"] = image_spec_copy
            (io, err) = krbd_io_handler(**io_config)
            if err:
                raise Exception("Map, mount and run IOs failed for " + str(image_spec))
            else:
                log.info("Map, mount and IOs successful for " + str(image_spec))

            # Enable image mode mirroring on namespace
            if enable_namespace_mirroring(
                rbd_primary, rbd_secondary, pool, **pool_config
            ):
                raise Exception("Failed to enable namespace level mirroring")

            # Get Group Mirroring Status
            (group_mirror_status, err) = rbd_primary.mirror.group.status(**group_config)
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
                + " and group mirror status is "
                + group_mirror_status
            )

            # Create Image_standalone in same pool
            image_size = kw.get("config", {}).get(pool_type, {}).get("size")
            image_name = "image_standalone"
            (image_create_status, err) = rbd_primary.create(
                pool=pool,
                image=image_name,
                namespace=pool_config.get("namespace"),
                size=image_size,
            )
            if err:
                raise Exception(
                    "Failed in enabling mirroring on standalone image: "
                    + pool
                    + "/"
                    + pool_config.get("namespace")
                    + "/"
                    + image_name
                    + ", err: "
                    + err
                )
            log.info("Successfully created standalone image for mirroring")

            # Run IO on image
            image_spec = []
            io_config["rbd_obj"] = rbd_primary
            io_config["client"] = client_primary
            image_spec.append(
                pool + "/" + pool_config.get("namespace") + "/" + image_name
            )
            io_config["config"]["image_spec"] = image_spec
            (io, err) = krbd_io_handler(**io_config)
            if err:
                raise Exception(
                    "Map, mount and run IOs failed for "
                    + str(io_config["config"]["image_spec"])
                )
            else:
                log.info(
                    "Map, mount and IOs successful for "
                    + str(io_config["config"]["image_spec"])
                )

            # Enable mirroring on Image
            (image_mirror_status, err) = rbd_primary.mirror.image.enable(
                pool=pool,
                image=image_name,
                namespace=pool_config.get("namespace"),
                mode="snapshot",
            )
            if err:
                raise Exception(
                    "Failed in enabling mirroring on standalone image: "
                    + io_config["config"]["image_spec"]
                    + " , err: "
                    + err
                )
            log.info("Successfully enabled mirroring on standalone image")

            # Verify if Image mirroring state is achieved
            wait_for_status(
                rbd=rbd_primary,
                cluster_name=primary_cluster.name,
                imagespec=io_config["config"]["image_spec"][0],
                state_pattern="up+stopped",
            )

            log.info(
                "Successfully verified state of image_standalone mirroring is up+stopped"
            )

            # Try adding mirrored enabled image to group (Should FAIL)
            try:
                add_group_image_and_verify(
                    rbd_primary,
                    **{
                        "group-spec": group_config["group-spec"],
                        "image-spec": io_config["config"]["image_spec"][0],
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
                    raise Exception(
                        "Add image to mirror enabled group failed with failed with " + e
                    )

            # Verfy size for image_standalone should match across both clusters
            image_spec = [
                {
                    "image": image_name,
                    "pool": pool,
                    "namespace": pool_config.get("namespace"),
                }
            ]
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
            (group_image_list, err) = rbd_primary.group.image.list(
                **group_config, format="json"
            )

            for spec in list(json.loads(group_image_list)):
                image_spec = (
                    spec["pool"]
                    + "/"
                    + pool_config.get("namespace")
                    + "/"
                    + spec["image"]
                )
                break

            group_image_kw = {
                "group-spec": group_config["group-spec"],
                "image-spec": image_spec,
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
                    raise Exception(
                        "Add image to mirror enabled group failed with failed with " + e
                    )

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
            compare_image_size_primary_secondary(
                rbd_primary, rbd_secondary, group_image_list
            )
            log.info("Successfully verified image size matches across both clusters")

            # Check group is replicated on site-b using group info
            (group_info_status, err) = rbd_secondary.group.info(
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
                raise Exception(
                    "group info is not as expected on secondary cluster "
                    + group_info_status
                )
            log.info("Successfully verified group is present on secondary site")

            # Check whether images are part of correct group on site-b using group image-list
            (group_image_list_secondary, err) = rbd_secondary.group.image.list(
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
                "Successfully verified group status. Verified state Primary: up+stopped, Secondary: up+replaying"
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


def test_group_renaming_with_mirroring(
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
    Test user can successfully rename group when group mirroring is in progress
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
        log.info("Running test CEPH-83613295  for %s", pool_type)

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
                    "device_map": True,
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
                    image_spec.append(
                        pool + "/" + pool_config.get("namespace") + "/" + image
                    )
            image_spec_copy = deepcopy(image_spec)
            io_config["rbd_obj"] = rbd_primary
            io_config["client"] = client_primary
            io_config["config"]["image_spec"] = image_spec_copy
            (io, err) = krbd_io_handler(**io_config)
            if err:
                raise Exception("Map, mount and run IOs failed for " + str(image_spec))
            else:
                log.info("Map, mount and IOs successful for " + str(image_spec))

            # Enable image mode mirroring on namespace
            if enable_namespace_mirroring(
                rbd_primary, rbd_secondary, pool, **pool_config
            ):
                raise Exception("Failed to enable namespace level mirroring")

            # Get Group Mirroring Status
            (group_mirror_status, err) = rbd_primary.mirror.group.status(**group_config)
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
                + " and group mirror status is "
                + group_mirror_status
            )

            # Enable Group Mirroring and Verify
            if mirror_state == "Disabled":
                enable_group_mirroring_and_verify_state(rbd_primary, **group_config)
            log.info("Successfully Enabled group mirroring")

            # Rename group
            group_new = "group_new" + random_string(len=4)
            out, err = rbd_primary.group.rename(
                **{
                    "source-group-spec": group_config["group-spec"],
                    "dest-group-spec": f'{pool}/{pool_config.get("namespace")}/{group_new}',
                }
            )
            if err:
                raise Exception(
                    "Rename operation for group "
                    + group_config["group-spec"]
                    + "failed with error "
                    + err
                )
            else:
                log.info(
                    "Successfully renamed group to "
                    + pool
                    + "/"
                    + pool_config.get("namespace")
                    + "/"
                    + group_new
                )

            # Update New group name in group_config
            group_config.update(
                {
                    "group-spec": pool
                    + "/"
                    + pool_config.get("namespace")
                    + "/"
                    + group_new
                }
            )

            # Wait for group mirroring to complete
            wait_for_idle(rbd_primary, **group_config)
            log.info(
                "Successfully completed sync for group mirroring to secondary site"
            )

            # Validate size of each image should be same on site-a and site-b
            (group_image_list, err) = rbd_primary.group.image.list(
                **group_config, format="json"
            )
            if err:
                raise Exception("Getting group image list failed : " + str(err))
            compare_image_size_primary_secondary(
                rbd_primary, rbd_secondary, group_image_list
            )
            log.info(
                "Successfully verified size of rbd images matches across both clusters"
            )

            # Check group is replicated on site-b using group info
            (group_info_status, err) = rbd_secondary.group.info(
                **group_config, format="json"
            )
            if err:
                raise Exception("Getting group info failed : " + str(err))
            if (
                json.loads(group_info_status)["group_name"] != group_new
                or json.loads(group_info_status)["mirroring"]["state"] != "enabled"
                or json.loads(group_info_status)["mirroring"]["mode"] != "snapshot"
                or json.loads(group_info_status)["mirroring"]["primary"]
            ):
                raise Exception(
                    "group info is not as expected on secondary cluster "
                    + group_info_status
                )
            log.info("Successfully verified group is present on secondary cluster")

            # Check whether images are part of correct group on site-b using group image-list
            (group_image_list_secondary, err) = rbd_secondary.group.image.list(
                **group_config, format="json"
            )
            if err:
                raise Exception("Getting group image list failed : " + str(err))
            if json.loads(group_image_list) != json.loads(group_image_list_secondary):
                raise Exception(
                    "Group image list does not match for primary and secondary cluster "
                    + group_image_list
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
            )
            log.info(
                "Successfully verified group status. Verified state Primary: up+stopped, Secondary: up+replaying"
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
                "Successfully verified md5sum of all images matches across both clusters"
            )

            # Kill/Stop mirror-daemon on site-a
            mirror2 = rbdmirror.RbdMirror(
                kw.get("ceph_cluster_dict").get("ceph-rbd2"), kw["config"]
            )
            service_name = mirror2.get_rbd_service_name("rbd-mirror")
            mirror2.change_service_state(service_name=service_name, operation="stop")
            time.sleep(5)
            group_mirror_status_verify(
                primary_cluster,
                secondary_cluster,
                rbd_primary,
                rbd_secondary,
                primary_state="up+stopped",
                secondary_state="down+stopped",
                **group_config,
            )
            log.info(
                "Successfully verified group status. Verified state Primary: up+stopped, Secondary: down+stopped"
            )

            # Disable group mirroring
            if mirror_state == "Enabled":
                disable_group_mirroring_and_verify_state(rbd_primary, **group_config)
            log.info("Successfully Disabled group mirroring")

            # Re-deploy/Restart mirror-daemon
            mirror2.change_service_state(service_name=service_name, operation="start")
            time.sleep(5)
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
                "Successfully verified group status. Verified state Primary: up+stopped, Secondary: up+replaying"
            )


def run(**kw):
    """
    TC#1: This test verify that a user can successfully mirror rbd group containing rbd images between primary and
    secondary sites
            Option 1: pool1/ns1/image1 & pool1/ns1/image2 (Images with namespaces) in a
            group is consistent between both clusters
    TC#2: Verify Addition/removal of Images to Consistency group (with namespace level images)
    TC#3: Verify Group Renaming while mirroring in progress (with namespace)
    Args:
        kw: test data
    Returns:
        int: The return value. 0 for success, 1 otherwise

    """
    try:
        operation_mapping = {
            "CEPH-83613293": test_namespace_group_mirroring,
            "CEPH-83613294": test_add_remove_group_mirroring,
            "CEPH-83613295": test_group_renaming_with_mirroring,
        }
        operation = kw.get("config").get("operation")
        if operation in operation_mapping:
            pool_types = ["rep_pool_config", "ec_pool_config"]
            log.info("Running Group Mirroring tests for namespace level images")
            kw.get("config").update({"grouptype": kw.get("config").get("grouptype")})
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

            operation_mapping[operation](
                rbd_primary,
                rbd_secondary,
                client_primary,
                client_secondary,
                primary_cluster,
                secondary_cluster,
                pool_types,
                **kw,
            )
            log.info("Test " + str(operation_mapping[operation]) + " passed")

    except Exception as e:
        log.error(
            "Test: RBD group mirroring (snapshot mode) across two clusters for namespace level rbd images failed: "
            + str(e)
        )
        return 1

    finally:
        cleanup(pool_types=pool_types, multi_cluster_obj=mirror_obj, **kw)

    return 0
