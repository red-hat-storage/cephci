"""
Module to verify :
  - Verify that a user can successfully mirror rbd group containing rbd images between primary and secondary sites
            Option 1: pool1/image1 & pool1/image2 (Images without namespaces)
  - Verify that a user can successfully mirror rbd group containing rbd images when group is renamed
    during syncing operation
  - Toggle group mirroring enable/disable when there is an active sync operation in progress

Test case covered:
CEPH-83610860 - Verify that a user can successfully replicate grouped RBD images between primary and secondary sites
CEPH-83611278 - Verify Group Renaming while mirroring in progress
CEPH-83613271 - Toggle group mirroring enable/disable when there is an active sync operation in progress

Pre-requisites :
1. Cluster must be up in 8.1 and above and running with capacity to create pool
2. We need atleast one client node with ceph-common package,
   conf and keyring files

TC#1: Test Case Flow:
Step 1: Deploy Two ceph cluster on version 8.1 or above
Step 2: Create RBD pool ‘pool_1’ on both sites
Step 3: Enable Image mode mirroring on pool_1 on both sites
Step 4: Bootstrap the storage cluster peers (Two-way)
Step 5: Create 2 RBD images in pool_1
Step 6: Create Consistency group
Step 7: Add Images in the consistency group
Step 8: Add data to the images
Step 9: Enable Mirroring for the group
Step 10: Wait for mirroring to complete
Step 11: Check all image is replicated on site-b
Step 12: Validate size of each image should be same on site-a and site-b
Step 13: Check group is replicated on site-b
Step 14: Check whether images are part of correct group on site-b
Step 15: Check pool mirror status, image mirror status and group mirror status on both sites does
         not show 'unknown' or 'error' on both clusters
Step 16: Confirm that the global ids match for the groups and images on both clusters.
Step 17: Validate the integrity of the data on secondary site-b
Step 18: Repeat above on EC pool

TC#2: Test Case Flow:
Step1: Deploy Two ceph cluster on version 8.1 or later
Step 2: Create RBD pool ‘pool_1’ on both sites
Step 3: Enable Image mode mirroring on pool_1 on both sites
Step 4: Bootstrap the storage cluster peers (Two-way)
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
Step 21: Kill mirror daemon on site-b
Step 22: Disable mirroring on the group should succeed
Step 23: restart mirror daemon
Step 24: Repeat above on EC pool
Step 25: Cleanup the images, file and pools

TC#3: Test Case Flow:
Step1: Deploy Two ceph cluster on version 8.1 or later
Step 2: Create RBD pool ‘pool_1’ on both sites
Step 3: Enable Image mode mirroring on pool_1 on both sites
Step 4: Bootstrap the storage cluster peers (Two-way)
Step 5: Create 2 large RBD images in pool_1
Step 6: Add data to the images
Step 7: Calculate MD5sum of all files
Step 8: Create Consistency group
Step 9: Add Images in the consistency group
Step 10: Toggle Enable/disable Mirroring for the group in a loop of 20 times
Step 11: Wait for mirroring to complete, status should show idle for all images
Step 12: Check all images are mirrored on site-b for pool_1
Step 13: Check group is mirrored on site-b
Step 14: Check all images should be part of image list on site-b
Step 15: Check pool mirror status, image mirror status and group mirror status on both
sites does not show 'unknown' or 'error' on both clusters
Step 16: Confirm that the global ids match for the groups and images on both clusters
Step 17: Validate the integrity of the data on secondary site-b for image2
Step 18: Repeat above on EC pool
Step 19: Cleanup the images, file and pools

"""

import json
import random
import time
from copy import deepcopy

from ceph.parallel import parallel
from ceph.rbd.initial_config import initial_mirror_config
from ceph.rbd.mirror_utils import (
    check_mirror_consistency,
    compare_image_size_primary_secondary,
)
from ceph.rbd.utils import getdict, random_string
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.group_mirror import (
    disable_group_mirroring_and_verify_state,
    enable_group_mirroring_and_verify_state,
    group_mirror_status_verify,
    wait_for_idle,
)
from ceph.rbd.workflows.krbd_io_handler import krbd_io_handler
from ceph.rbd.workflows.namespace import enable_namespace_mirroring
from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from utility.log import Log

log = Log(__name__)


def test_group_mirroring(
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
    Test user can successfully mirror rbd group containing rbd images between primary and secondary sites
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
        log.info("Running test CEPH-83610860 for %s", pool_type)

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
            pool_spec = None
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
                + group_config["group-spec"]
                + " mirroring state is "
                + mirror_state
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
            group_image_list, err = rbd_primary.group.image.list(
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
                global_id=True,
            )
            log.info(
                "Successfully verified group status and global ids match for both clusters"
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

            disable_group_mirroring_and_verify_state(rbd_primary, **group_config)

            out, err = rbd_secondary.group.list(**{"pool-spec": pool_spec})
            if out:
                raise Exception(
                    "group is listed on the secondary cluster after disabling group mirroring on primary: %s",
                    out,
                )
            else:
                log.info(
                    "Group deleted on secondary when group mirroring is disabled on primary"
                )

            enable_group_mirroring_and_verify_state(rbd_primary, **group_config)
            rbd_primary.group.remove(**group_config)
            out, err = rbd_primary.group.list(**{"pool-spec": pool_spec})
            if out:
                raise Exception("group listed on primary after deleting: %s", out)
            else:
                log.info("Group removed on primary")
            out, err = rbd_secondary.group.list(**{"pool-spec": pool_spec})
            if out:
                raise Exception(
                    "Group is listed on the secondary cluster after removing on primary: %s",
                    out,
                )
            else:
                log.info("Group removed on secondary when group is removed on primary")


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
        log.info("Running test CEPH-83611278 for %s", pool_type)

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
                    "dest-group-spec": f"{pool_spec}/{group_new}",
                }
            )
            if err:
                raise Exception(
                    "Rename of group "
                    + group_config["group-spec"]
                    + "failed with error "
                    + err
                )
            else:
                log.info("Successfully renamed group to " + pool_spec + "/" + group_new)

            # Update New group name in group_config
            group_config.update({"group-spec": pool_spec + "/" + group_new})
            time.sleep(60)
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
                "Successfully verified size of rbd images matches across both clusters"
            )

            # Check group is replicated on site-b using group info
            group_info_status, err = rbd_secondary.group.info(
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
            group_image_list_secondary, err = rbd_secondary.group.image.list(
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
                "Successfully verified group status. Verified state Primary: up+stopped, Secondary:up+replaying"
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
            log.info("Successfully verified Renaming of group with mirroring")

            log.info(
                "Verifying group mirroring can be disabled when rbd-mirror daemon on secondary is killed"
            )
            # Kill/Stop mirror-daemon on site-b
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
                "Successfully verified group status. Verified state primary: up+stopped, Secondary: up+replaying"
            )


def test_toggle_enable_disable(
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
    Test Toggle enable and disable group mirroring
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
        log.info("Running test CEPH-83613271 for %s", pool_type)

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

            image_spec_copy = deepcopy(image_spec)
            io_config["rbd_obj"] = rbd_primary
            io_config["client"] = client_primary
            io_config["config"]["image_spec"] = image_spec_copy
            io, err = krbd_io_handler(**io_config)
            if err:
                raise Exception("Map, mount and run IOs failed for " + str(image_spec))
            else:
                log.info("Map, mount and IOs successful for " + str(image_spec))

            with parallel() as p:
                p.spawn(
                    toggle_group_mirror_enable_disable,
                    rbd_primary=rbd_primary,
                    **group_config,
                )

            # Wait for mirroring to complete, status should show idle for all images
            wait_for_idle(rbd_primary, **group_config)
            log.info(
                "Successfully completed sync for group mirroring to secondary site"
            )

            # Validate the integrity of the data on secondary site-b
            group_image_list, err = rbd_primary.group.image.list(
                **group_config, format="json"
            )
            if err:
                raise Exception("Getting group image list failed : " + str(err))
            check_mirror_consistency(
                rbd_primary,
                rbd_secondary,
                client_primary,
                client_secondary,
                group_image_list,
            )
            log.info(
                "Successfully verified md5sum of all images matches across both clusters after "
                "toggling mirror group enable and disable"
            )


def toggle_group_mirror_enable_disable(rbd_primary, **group_config):
    log.info("Toggling group enable and disable 20 times")
    for i in range(0, 20):
        enable_group_mirroring_and_verify_state(rbd_primary, **group_config)
        log.info("Successfully Enabled group mirroring")
        disable_group_mirroring_and_verify_state(rbd_primary, **group_config)
        log.info("Successfully Disabled group mirroring")
    enable_group_mirroring_and_verify_state(rbd_primary, **group_config)
    log.info("Successfully Enabled group mirroring again on disabled mirror group")


def run(**kw):
    """
    This test verifies 8.1 group mirroring rbd test cases
    Args:
        kw: test data
    Returns:
        int: The return value. 0 for success, 1 otherwise

    """
    try:
        operation_mapping = {
            "CEPH-83610860": test_group_mirroring,
            "CEPH-83611278": test_group_renaming_with_mirroring,
            "CEPH-83613271": test_toggle_enable_disable,
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
            "Test: RBD group mirroring (snapshot mode) across two clusters failed: "
            + str(e)
        )
        return 1

    finally:
        cleanup(pool_types=pool_types, multi_cluster_obj=mirror_obj, **kw)

    return 0
