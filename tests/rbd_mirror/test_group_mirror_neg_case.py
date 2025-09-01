"""
Module to verify :
  - Exercise group mirroring with unsupported configurations (e.g Journal based mirroring)
  - Exercise Adding removing listing mirror group snapshot schedule when group mirroring is disabled

Test case covered:
CEPH-83614238 - Exercise group mirroring with unsupported configurations (e.g Journal based mirroring)
CEPH-83620497 - Adding/removing/listing mirror group snapshot schedule when group mirroring is disabled

Pre-requisites :
1. Cluster must be up in 8.1 and above and running with capacity to create pool
2. We need atleast one client node with ceph-common package,
   conf and keyring files

TC#1: Test Case Flow:
Step1: Deploy Two ceph cluster on version 8.1 and later ceph version
Step 2: Create RBD pool 'pool_1' on both sites
Step 3: Enable Image mode mirroring on pool_1 on both sites
Step 4: Bootstrap the storage cluster peers (Two-way)
Step 5: Create 2 RBD images in pool_1
Step 6: Calculate MD5sum of all files
Step 7: Create Consistency group
Step 8: Enable journal based mirroring on images
Step 9: Add Images in the consistency group
Step 10: Disable mirroring on images
Step 11: Add images in the consistency group
Step 12: Enable journal mirror mode on image1, Should fail
Step 13: Add image from different pool in the same group, Should FAIL
Step 14: Repeat above on EC pool
Step 15: Cleanup the images, file and pools

TC#2: Test Case Flow:
Step1: Deploy Two ceph cluster on version 8.1 or later
Step 2: Create RBD pool 'pool_1' on both sites
Step 3: Enable Image mode mirroring on pool_1 on both sites
Step 4: Bootstrap the storage cluster peers (Two-way)
Step 5: Create 2 RBD images in pool_1
Step 6: Create Consistency group
Step 7: Add Images in the consistency group
Step 8: Add the group mirror snapshot schedule should fail when group mirroing is disabled
Step 9: Removing the group mirror snapshot schedule should fail when group mirroing is disabled
Step 10: Listing the group mirror snapshot schedule should fail when group mirroing is disabled
Step 11: Repeat above on EC pool (with namespace randomization/optimization)
Step 12: Cleanup all objects (pool, images, groups)
"""

import json
import random
from copy import deepcopy

from ceph.rbd.initial_config import initial_mirror_config
from ceph.rbd.utils import getdict, random_string
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.group_mirror import (
    add_group_image_and_verify,
    disable_group_mirroring_and_verify_state,
    enable_group_mirroring_and_verify_state,
    remove_group_image_and_verify,
    verify_group_snapshot_ls,
)
from ceph.rbd.workflows.namespace import (
    create_namespace_and_verify,
    enable_namespace_mirroring,
)
from ceph.rbd.workflows.rbd import create_single_pool_and_images
from ceph.rbd.workflows.snap_scheduling import add_snapshot_scheduling
from utility.log import Log

log = Log(__name__)


def test_group_mirroring_neg_case(
    rbd_primary,
    rbd_secondary,
    client_primary,
    pool_types,
    **kw,
):
    """
    Exercise group mirroring with unsupported configurations (e.g Journal based mirroring)
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
        log.info("Running test CEPH-83614238 for %s", pool_type)

        config = kw.get("config", {})

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
            pool_spec = None
            if "namespace" in pool_config:
                enable_namespace_mirroring(
                    rbd_primary, rbd_secondary, pool, **pool_config
                )
                pool_spec = pool + "/" + pool_config.get("namespace")
            else:
                pool_spec = pool

            image_spec_copy = deepcopy(image_spec)

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
            )

            log.info(
                "Check journal mode mirorring can not be enabled on images part of the group"
            )
            # Enable group mirroring
            if mirror_state == "Disabled":
                enable_group_mirroring_and_verify_state(rbd_primary, **group_config)
                mirror_state = "Enabled"
            log.info("Successfully Enabled group mirroring")

            # Enable Journal Mode mirroring on images, should FAIL
            (image_mirror_status, err) = rbd_primary.mirror.image.enable(
                **{
                    "image-spec": image_spec_copy[0],
                    "mode": "journal",
                }
            )
            if not err:
                raise Exception(
                    "Journal based mirroring should not be enabled on images which are part of group"
                    + image_mirror_status
                    + " , err: "
                    + err
                )
            else:
                if (
                    "cannot enable mirroring on an image that is member of a group"
                    in err
                ):
                    log.info(
                        "Successfully verified journal mirroring cannot be enabled on image in the group"
                    )
                else:
                    raise Exception(
                        "Error string while enabling journal based mirroring is not as expected"
                        + image_mirror_status
                        + " , err: "
                        + err
                    )

            # Disable group mirroring
            if mirror_state == "Enabled":
                disable_group_mirroring_and_verify_state(rbd_primary, **group_config)
                mirror_state = "Disabled"
            log.info("Successfully Disabled group mirroring")

            # Enable Journal Mode mirroring on images, should FAIL
            (image_mirror_status, err) = rbd_primary.mirror.image.enable(
                **{
                    "image-spec": image_spec_copy[0],
                    "mode": "journal",
                }
            )
            if not err:
                raise Exception(
                    "Journal based mirroring should not be enabled on images which are part of group"
                    + image_mirror_status
                    + " , err: "
                    + err
                )
            else:
                if (
                    "cannot enable mirroring on an image that is member of a group"
                    in err
                ):
                    log.info(
                        "Successfully verified journal mirroring cannot be enabled on image in the group"
                    )
                else:
                    raise Exception(
                        "Error string while enabling journal based mirroring is not as expected"
                        + image_mirror_status
                        + " , err: "
                        + err
                    )

            log.info("Check Journal mode enabled images cannot be added to the group")
            # Remove Images from the group, Should Succeed
            group_image_kw = {
                "group-spec": group_config["group-spec"],
                "image-spec": image_spec_copy[0],
            }
            remove_group_image_and_verify(rbd_primary, **group_image_kw)
            log.info(
                "Successfully verified image is removed from group when group mirroring is disabled"
            )

            # Enable Journal Mode mirroring on images, Should Succeed
            (image_mirror_status, err) = rbd_primary.mirror.image.enable(
                **{
                    "image-spec": image_spec_copy[0],
                    "mode": "journal",
                }
            )
            log.info(
                "Successfully enabled journal based mirroring on image after removing from group"
            )

            # Add images to the group, Should FAIL
            try:
                add_group_image_and_verify(rbd_primary, **group_image_kw)
                raise Exception(
                    "Image should not have been added successfully when journal based "
                    "mirroring is enabled on the images"
                )
            except Exception as e:
                if "cannot add mirror enabled image to group" in str(e):
                    log.info(
                        "Successfully verified journal based mirror image can not be added to the "
                        "group in disabled state"
                    )
                else:
                    raise Exception("Add group image failed with " + str(e))

            # Enable group mirroring
            if mirror_state == "Disabled":
                enable_group_mirroring_and_verify_state(rbd_primary, **group_config)
                mirror_state = "Enabled"
            log.info("Successfully Enabled group mirroring")

            # Add images to the group, Should FAIL
            try:
                add_group_image_and_verify(rbd_primary, **group_image_kw)
                raise Exception(
                    "Image should not have been added successfully when journal based "
                    "mirroring is enabled on the images"
                )
            except Exception as e:
                if "cannot add image to mirror enabled group" in str(e):
                    log.info(
                        "Successfully verified journal based mirror image can not be added "
                        "to the group in enabled state"
                    )
                else:
                    raise Exception("Add group image failed with " + str(e))

            log.info("Add images from different pool to the group should FAIL")
            # Create second pool & image in the second pool
            second_pool = "second_pool_" + random_string(len=5)
            pool_config["data_pool"] = second_pool
            rc = create_single_pool_and_images(
                config=config,
                pool=second_pool,
                pool_config=pool_config,
                client=client_primary,
                cluster="ceph",
                rbd=rbd_primary,
                ceph_version=int(config.get("rhbuild")[0]),
                is_ec_pool=False,
                is_secondary=False,
                do_not_create_image=True,
            )
            if rc:
                log.error("Creation of Another pool " + second_pool + " failed")
                return rc
            log.info("Creation of Another pool " + second_pool + " is successful")

            # Create Image in the pool
            if len(pool_spec.split("/")) == 2:
                # Create Namespace
                rc = create_namespace_and_verify(
                    **{
                        "pool-name": second_pool,
                        "namespace": pool_config.get("namespace"),
                        "client": client_primary,
                    }
                )
                if rc != 0:
                    raise Exception(
                        "Error creating namespace in second pool "
                        + pool_config.get("namespace")
                    )
                pool_spec = second_pool + "/" + pool_config.get("namespace") + "/"
            else:
                pool_spec = second_pool + "/"

            image_size = kw.get("config", {}).get(pool_type, {}).get("size")
            image_name_second_pool = "second_pool_image_" + random_string(len=5)
            (image_create_status, err) = rbd_primary.create(
                **{
                    "image-spec": pool_spec + image_name_second_pool,
                    "size": image_size,
                }
            )
            if err:
                raise Exception(
                    "Failed to create image: "
                    + image_name_second_pool
                    + ", err: "
                    + err
                )
            log.info(
                "Successfully created image "
                + image_name_second_pool
                + " in pool "
                + pool_spec
                + image_create_status
            )

            # Disable group mirorring
            if mirror_state == "Enabled":
                disable_group_mirroring_and_verify_state(rbd_primary, **group_config)
                mirror_state = "Disabled"
            log.info("Successfully Disabled group mirroring")

            # Add image from second pool to the group, should not FAIL
            group_image_kw = {
                "group-spec": group_config["group-spec"],
                "image-spec": pool_spec + image_name_second_pool,
            }
            add_group_image_and_verify(rbd_primary, **group_image_kw)
            log.info("Successfully added image from different pool to the group")

            # Enable mirroring on group, Should FAIL
            try:
                if mirror_state == "Disabled":
                    enable_group_mirroring_and_verify_state(rbd_primary, **group_config)
                    mirror_state = "Enabled"
            except Exception as e:
                if "image is in a different pool" in str(e):
                    log.info(
                        "Successfully verified that group mirroring cannot be enabled "
                        "on group containing images from multiple pools"
                    )
                else:
                    raise Exception("Add group image failed with " + str(e))

    log.info("Test Consistency group negative test passed")


def test_group_mirror_snapshot_schedule_neg_case(
    rbd_primary,
    rbd_secondary,
    client_primary,
    pool_types,
    **kw,
):
    """
    Exercise Adding removing listing mirror group snapshot schedule when group mirroring is disabled
    Args:
        rbd_primary: RBD object of primary cluster
        rbd_secondary: RBD objevct of secondary cluster
        client_primary: client node object of primary cluster
        pool_types: Replication pool or EC pool
        **kw: any other arguments
    """
    for pool_type in pool_types:
        rbd_config = kw.get("config", {}).get(pool_type, {})
        multi_pool_config = deepcopy(getdict(rbd_config))
        log.info("Running test CEPH-83620497 for %s", pool_type)

        for pool, pool_config in multi_pool_config.items():
            group_config = {}
            if "data_pool" in pool_config.keys():
                _ = pool_config.pop("data_pool")

            group_spec = pool_config.get("group-spec")
            group_config.update({"group-spec": group_spec})
            for image, image_config in pool_config.items():
                if "image" in image:
                    if "namespace" in pool_config:
                        image_spec = (
                            pool + "/" + pool_config.get("namespace") + "/" + image
                        )
                    else:
                        image_spec = pool + "/" + image

            if "namespace" in pool_config:
                enable_namespace_mirroring(
                    rbd_primary, rbd_secondary, pool, **pool_config
                )

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
            )

            # Add the group mirror snapshot schedule should fail
            (group_info_status, err) = rbd_primary.group.info(
                **group_config, format="json"
            )
            group_id = json.loads(group_info_status)["group_id"]

            snap_schedule_config = {
                "pool": pool,
                "image": image_spec.split("/")[-1],
                "level": "group",
                "group": pool_config.get("group"),
                "interval": "1m",
            }
            if "namespace" in pool_config:
                snap_schedule_config.update({"namespace": pool_config.get("namespace")})
            out, err = add_snapshot_scheduling(rbd_primary, **snap_schedule_config)
            if err and (f"group {group_id} is not in snapshot mirror mode" in err):
                log.info(
                    "Successfully Verified snapshot schedule cannot be added when group mirroring is disabled"
                )
            else:
                log.info(err)
                raise Exception(
                    "Group snapshot schedule of 1m should not be added when group mirorring is disabled"
                )

            # Removing the group mirror snapshot schedule should fail
            snap_schedule_config.pop("image")
            snap_schedule_config.pop("level")
            out, err = rbd_primary.mirror.group.snapshot.schedule.remove_(
                **snap_schedule_config
            )
            if err and f"group {group_id} is not in snapshot mirror mode" in err:
                log.info(
                    "Successfully Verified snapshot schedule cannot be removed when group mirroring is disabled"
                )
            else:
                raise Exception(
                    "Group snapshot schedule of 1m should not be removed when group mirorring is disabled"
                )

            # Listing the group mirror snapshot schedule should FAIL
            status_spec = {
                "pool": pool,
                "group": pool_config.get("group"),
                "format": "json",
            }
            if "namespace" in pool_config:
                status_spec.update({"namespace": pool_config.get("namespace")})
            else:
                group_spec = pool + "/" + pool_config.get("group")

            try:
                verify_group_snapshot_ls(rbd_primary, group_spec, "1m", **status_spec)
            except Exception as e:
                if f"group {group_id} is not in snapshot mirror mode" in str(e):
                    log.info(
                        "Successfully Verified snapshot schedule cannot be listed "
                        "when group mirroring is disabled"
                    )
                else:
                    raise Exception(
                        "Group snapshot schedule of 1m should not be listed when group mirorring is disabled"
                    )

    log.info("Test Consistency group snapshot schedule negative test passed")


def run(**kw):
    """
    This test verifies 8.1 group mirroring rbd negative test cases
    1- Journal based mirroring images
    2- images from different pool
    Args:
        kw: test data
    Returns:
        int: The return value. 0 for success, 1 otherwise

    """
    try:
        operation_mapping = {
            "CEPH-83614238": test_group_mirroring_neg_case,
            "CEPH-83620497": test_group_mirror_snapshot_schedule_neg_case,
        }
        operation = kw.get("config").get("operation")
        if operation in operation_mapping:
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
                else:
                    rbd_secondary = val.get("rbd")

            pool_types = list(mirror_obj.values())[0].get("pool_types")

            operation_mapping[operation](
                rbd_primary,
                rbd_secondary,
                client_primary,
                pool_types,
                **kw,
            )

    except Exception as e:
        log.error(
            "Test: RBD group mirroring (snapshot mode) across two clusters failed: "
            + str(e)
        )
        return 1

    finally:
        cleanup(pool_types=pool_types, multi_cluster_obj=mirror_obj, **kw)

    return 0
