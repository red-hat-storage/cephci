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
        Site-a: ceph osd pool create pool_1
                    ceph osd pool application enable pool_1 rbd
                    rbd pool init -p pool_1
        Site-b: ceph osd pool create pool_1
                    ceph osd pool application enable pool_1 rbd
                    rbd pool init -p pool_1
Step 3: Enable Image mode mirroring on pool_1 on both sites
        Site-a: rbd mirror pool enable pool_1 image
        Site-b: rbd mirror pool enable pool_1 image
 Step 4: Bootstrap the storage cluster peers (Two-way)
        site-a: rbd mirror pool peer bootstrap create --site-name site-a pool_1 > /root/bootstrap_token_site-a
        site-b: rbd mirror pool peer bootstrap import --site-name site-b --direction rx-tx pool_1
        /root/bootstrap_token_site-a
Step 5: Create 2 RBD images in pool_1
       Site-a:  rbd create image1 --size 1G --pool pool_1
                rbd create image2 --size 1G --pool pool_1
Step 6: Add data to the images
rbd bench --io-type write --io-threads 16 --io-total 500M pool_1/image1
rbd bench --io-type write --io-threads 16 --io-total 500M pool_1/image2
Step 7: Calculate MD5sum of all files
        rbd export pool_1/image1 file1.txt
        rbd export pool_1/image2 file2.txt
        md5sum file1.txt
        md5sum file2.txt
Step 8: Create Consistency group
        rbd group create --pool pool_1 --group group_1
Step 9: Enable Mirroring for the group
        rbd mirror group enable --group group_1 --pool pool_1
“Mirroring is enabled” message should be displayed
Step 10: Add Images in the consistency group should throw error
        rbd group image add --image image1 --group group_1 --group-pool pool_1 --image-pool pool_1
        rbd group image add --image image2 --group group_1 --group-pool pool_1 --image-pool pool_1
Adding images to the group should throw error : “cannot add image to mirror enabled group”
Step 11: Disable Mirroring for the group
        rbd mirror group disable --group group_1 --pool pool_1
“Mirroring is disabled” message should be displayed
Step 12: Add Images in the consistency group should succeed
        rbd group image add --image image1 --group group_1 --group-pool pool_1 --image-pool pool_1
        rbd group image add --image image2 --group group_1 --group-pool pool_1 --image-pool pool_1
Images should get added successfully
Step 13: Enable Mirroring for the group
        rbd mirror group enable --group group_1 --pool pool_1
“Mirroring is enabled” message should be displayed
Step 14: Remove image1 from pool_1 should throw error "cannot remove image from mirror enabled group"
        rbd group image remove --image image1 --group group_1 --group-pool pool_1 --image-pool pool_1
Removing images from the group should throw error : “cannot remove image to mirror enabled group”,
verify image is still part of the pool using rbd group image list
Step 15: Disable Mirroring for the group
        rbd mirror group disable --group group_1 --pool pool_1
“Mirroring is disabled” message should be displayed
Step 16: Remove image1 from pool_1 should succeed
        rbd group image remove --image image1 --group group_1 --group-pool pool_1 --image-pool pool_1
Images should be removed successfully, verify using rbd group image list
Step 17: Enable Mirroring for the group
        rbd mirror group enable --group group_1 --pool pool_1
“Mirroring is enabled” message should be displayed
Step 18: Check only image2 is replicated on site-b for pool_1
       Site-b:  rbd ls pool_1
Only ‘image2’ should be part of rbd list on site-b
Step 19: Check group is replicated on site-b
       Site-b: rbd group info --group group_1 --pool pool_1
Step 20: Check only image2 should be part of image list on site-b, image1 should not be present
   Site-b: rbd group image list --pool pool_1 --group group_1
Only ‘image2’ should be part of group image list
Step 21: Check pool mirror status, image mirror status and group mirror status on both sites does not show
'unknown' or 'error' on both clusters
  Site-a: Pool Level: rbd mirror pool status pool_1
        Image level: rbd mirror image status pool_1/image2
        Group Level: rbd mirror group status pool_1/group_1

    Site-b: Pool Level: rbd mirror pool status pool_1
        Image level: rbd mirror image status pool_1/image2
        Group Level: rbd mirror group status pool_1/group_1
Both sites should not show 'unknown' or 'error' on any clusters
Step 22: Confirm that the global ids match for the groups and images on both clusters.
     Site-a:  rbd mirror group status pool_1/group_1
     Site-b:  rbd mirror group status pool_1/group_1
Global ids should match for both sites
Step 23: Validate the integrity of the data on secondary site-b for image2
       Site-b:  rbd export pool_1/image2 file2_b.txt
                md5sum file2_b.txt
Note: (Md5sum should match with site-a images)
Step 24: Repeat above on EC pool
Step 25: Cleanup the images, file and pools

"""

import json
from copy import deepcopy

from ceph.rbd.initial_config import initial_mirror_config
from ceph.rbd.mirror_utils import (
    check_mirror_consistency,
    compare_image_size_primary_secondary,
    run_IO,
)
from ceph.rbd.utils import getdict
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.group_mirror import (
    add_group_image_and_verify,
    disable_group_mirroring_and_verify_state,
    enable_group_mirroring_and_verify_state,
    group_mirror_status_verify,
    remove_group_image_and_verify,
    wait_for_idle,
)
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

        for pool, pool_config in multi_pool_config.items():
            group_config = {}
            if "data_pool" in pool_config.keys():
                _ = pool_config.pop("data_pool")
            group_config.update({"pool": pool})

            for image, image_config in pool_config.items():
                if "image" in image:
                    # Add data to the images
                    err = run_IO(rbd_primary, client_primary, pool, image, **kw)
                    if err:
                        return 1

            # Get Groups in a Pool
            (group, out_err) = rbd_primary.group.list(**group_config)
            log.info("Groups present in pool " + pool + " are: " + group)
            if out_err:
                log.error("Listing Group in a pool Failed")
                return 1
            group_config.update({"group": group.strip()})

            # Get Group Mirroring Status
            (group_mirror_status, out_err) = rbd_primary.mirror.group.status(
                **group_config
            )
            if out_err:
                if "mirroring not enabled on the group" in out_err:
                    mirror_state = "Disabled"
                else:
                    log.error("Getting group mirror status failed : " + str(out_err))
                    return 1
            else:
                mirror_state = "Enabled"
            log.info("Group " + group + " mirroring state is " + mirror_state)

            # Enable Group Mirroring and Verify
            if mirror_state == "Disabled":
                out = enable_group_mirroring_and_verify_state(
                    rbd_primary, **group_config
                )
                if out:
                    log.error("Enabling group mirroring failed")
                    return 1
                mirror_state = "Enabled"

            # Wait for group mirroring to complete
            out = wait_for_idle(rbd_primary, **group_config)
            if out:
                log.error("Group Mirorring is not idle after 300 seconds")
                return 1

            # Remove RBD image 'image1' from group # Should FAIL
            (group_image_list, out_err) = rbd_primary.group.image.list(
                **group_config, format="json"
            )

            for spec in list(json.loads(group_image_list)):
                image_spec = spec["pool"] + "/" + spec["image"]
                break

            kw = {
                "group-spec": group_config["pool"] + "/" + group_config["group"],
                "image-spec": image_spec,
            }
            out = remove_group_image_and_verify(rbd_primary, **kw)
            if not out:
                log.error(
                    "Image should not have removed successfully when group mirroring is enabled"
                )
                return 1

            (group_image_list, out_err) = rbd_primary.group.image.list(
                **group_config, format="json"
            )

            # Disable Mirroring
            if mirror_state == "Enabled":
                out = disable_group_mirroring_and_verify_state(
                    rbd_primary, **group_config
                )
                if out:
                    log.error("Enabling group mirroring failed")
                    return 1
                mirror_state = "Disabled"

            # Remove RBD image 'image1' from group # Should Succeed
            out = remove_group_image_and_verify(rbd_primary, **kw)
            if not out:
                log.error("Image is not removed from the group")
                return 1

            (group_image_list, out_err) = rbd_primary.group.image.list(
                **group_config, format="json"
            )

            # Enable Mirroring
            if mirror_state == "Disabled":
                out = enable_group_mirroring_and_verify_state(
                    rbd_primary, **group_config
                )
                if out:
                    log.error("Enabling group mirroring failed")
                    return 1
                mirror_state = "Enabled"

            # Add rbd image 'image1' to group # Should FAIL
            out = add_group_image_and_verify(rbd_primary, **kw)
            if not out:
                log.error(
                    "Image should not have been added successfully when group mirroring is enabled"
                )
                return 1

            (group_image_list, out_err) = rbd_primary.group.image.list(
                **group_config, format="json"
            )

            # Disable Mirroring
            if mirror_state == "Enabled":
                out = disable_group_mirroring_and_verify_state(
                    rbd_primary, **group_config
                )
                if out:
                    log.error("Enabling group mirroring failed")
                    return 1
                mirror_state = "Disabled"

            # Add rbd image 'image1' to group # Should Succeed
            out = add_group_image_and_verify(rbd_primary, **kw)
            if not out:
                log.error("Image is not added to the group")
                return 1

            (group_image_list, out_err) = rbd_primary.group.image.list(
                **group_config, format="json"
            )
            log.info("Group Image list after add " + str(group_image_list))

            # Enable mirroring
            if mirror_state == "Disabled":
                out = enable_group_mirroring_and_verify_state(
                    rbd_primary, **group_config
                )
                if out:
                    log.error("Enabling group mirroring failed")
                    return 1
                mirror_state = "Enabled"

            # Wait for group mirroring to complete
            out = wait_for_idle(rbd_primary, **group_config)
            if out:
                log.error("Group Mirorring is not idle after 300 seconds")
                return 1

            # Validate size of each image should be same on site-a and site-b
            (group_image_list, out_err) = rbd_primary.group.image.list(
                **group_config, format="json"
            )
            res = compare_image_size_primary_secondary(
                rbd_primary, rbd_secondary, group_image_list
            )
            if res:  # res will be 0 when sizes match and 1 when not matched
                log.error(
                    "size of rbd images do not match on primary and secondary site"
                )
                return 1

            # Check group is replicated on site-b using group info
            (group_info_status, out_err) = rbd_secondary.group.info(
                **group_config, format="json"
            )
            if out_err:
                log.error("Getting group info failed : " + str(out_err))
                return 1
            else:
                if (
                    json.loads(group_info_status)["group_name"] != group_config["group"]
                    or json.loads(group_info_status)["mirroring"]["state"] != "enabled"
                    or json.loads(group_info_status)["mirroring"]["mode"] != "snapshot"
                    or json.loads(group_info_status)["mirroring"]["primary"]
                ):
                    log.error("group info is not as expected on secondary cluster")
                    return 1

            # Check whether images are part of correct group on site-b using group image-list
            (group_image_list_primary, out_err) = rbd_primary.group.image.list(
                **group_config, format="json"
            )
            (group_image_list_secondary, out_err) = rbd_secondary.group.image.list(
                **group_config, format="json"
            )
            if out_err:
                log.error(
                    "Getting group image list failed for"
                    + group_config["group"]
                    + " : "
                    + str(out_err)
                )
                return 1
            else:
                if json.loads(group_image_list_primary) != json.loads(
                    group_image_list_secondary
                ):
                    log.error(
                        "Group image list does not match for primary and secondary cluster"
                    )
                    return 1

            # Verify group mirroring status on both clusters & Match global id of both cluster
            out = group_mirror_status_verify(
                primary_cluster,
                secondary_cluster,
                rbd_primary,
                rbd_secondary,
                primary_state="up+stopped",
                secondary_state="up+replaying",
                **group_config
            )
            if out:
                log.error("Group mirroring status is not healthy")
                return 1

            # Validate the integrity of the data on secondary site-b
            err = check_mirror_consistency(
                rbd_primary,
                rbd_secondary,
                client_primary,
                client_secondary,
                group_image_list,
                **group_config
            )
            if err:
                return 1

    return 0


def run(**kw):
    """
    This test verify that a user can successfully add/remove images from disabled mirror rbd group
    Args:
        kw: test data
    Returns:
        int: The return value. 0 for success, 1 otherwise

    """
    try:
        pool_types = ["rep_pool_config", "ec_pool_config"]
        log.info("Running Add/Remove Consistency Group Mirroring across two clusters")
        for grouptype in kw.get("config").get("grouptypes"):
            kw.get("config").update({"grouptype": grouptype})
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
            rc = test_add_remove_group_mirroring(
                rbd_primary,
                rbd_secondary,
                client_primary,
                client_secondary,
                primary_cluster,
                secondary_cluster,
                pool_types,
                **kw
            )
            if rc:
                log.error(
                    "Test: Add/Remove in RBD group mirroring (snapshot mode) across two clusters failed"
                )
                return 1

    except Exception as e:
        log.error(
            "Test: Add/Remove in RBD group mirroring (snapshot mode) across two clusters failed: "
            + str(e)
        )
        return 1

    finally:
        cleanup(pool_types=pool_types, multi_cluster_obj=mirror_obj, **kw)
        log.info("Cleanup")

    return 0
