"""
Module to verify :
  - Verify Group Mirroring with scale

Test case covered:
CEPH-83611376 - Scale test for consistency group mirroring (snapshot mode)
- 1 group with 50 images,
- 100 groups with 1 images each
- 20 groups with 5 images each
- 5 groups with 20 images each

Pre-requisites :
1. Cluster must be up in 8.1 and above and running with capacity to create pool
2. We need atleast one client node with ceph-common package,
   conf and keyring files

TC#1: Test Case Flow:
Step 1: Deploy Two ceph cluster on version 8.1 or above
Step 2: Create RBD pool ‘pool_1’ on both sites
Step 3: Enable Image mode mirroring on pool_1 on both sites
Step 4: Bootstrap the storage cluster peers (Two-way)
Step 5: Create group and images based on 4 options given for scale above
Step 6: Add Images in the consistency group
Step 7: Add data to the images
Step 8: Enable Mirroring for the group
Step 9: Wait for mirroring to complete
Step 10: Check all image is replicated on site-b
Step 11: Validate size of each image should be same on site-a and site-b
Step 12: Check group is replicated on site-b
Step 13: Check whether images are part of correct group on site-b
Step 14: Check pool mirror status, image mirror status and group mirror status on both sites does
         not show 'unknown' or 'error' on both clusters
Step 15: Validate the integrity of the data on secondary site-b
Step 16: Repeat above on EC pool with or without namespace
"""

import json
import random
from copy import deepcopy

from ceph.rbd.initial_config import initial_mirror_config
from ceph.rbd.mirror_utils import (
    check_mirror_consistency,
    compare_image_size_primary_secondary,
)
from ceph.rbd.utils import getdict, random_string
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.group_mirror import (
    create_group_add_images,
    enable_group_mirroring_and_verify_state,
    group_mirror_status_verify,
    wait_for_idle,
)
from ceph.rbd.workflows.krbd_io_handler import krbd_io_handler
from ceph.rbd.workflows.namespace import enable_namespace_mirroring
from utility.log import Log

log = Log(__name__)


def test_scale_options(
    rbd_primary,
    rbd_secondary,
    client_primary,
    client_secondary,
    primary_cluster,
    secondary_cluster,
    pool_types,
    scale_option,
    **kw,
):
    """
    Test group mirroring with scale for 1 group having 50 images
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
        log.info("Running test CEPH-83611376 for %s", pool_type)

        # FIO Params Required for ODF workload exclusively in group mirroring
        fio = kw.get("config", {}).get("fio", {})
        io_config = {
            "size": fio["size"],
            "do_not_create_image": True,
            "num_jobs": fio["ODF_CONFIG"]["num_jobs"],
            "iodepth": fio["ODF_CONFIG"]["iodepth"],
            "rwmixread": fio["ODF_CONFIG"]["rwmixread"],
            "direct": fio["ODF_CONFIG"]["direct"],
            "invalidate": fio["ODF_CONFIG"]["invalidate"],
            "config": {
                "file_size": fio["size"],
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
            if "data_pool" in pool_config.keys():
                _ = pool_config.pop("data_pool")

            pool_spec = None
            if "namespace" in pool_config:
                enable_namespace_mirroring(
                    rbd_primary, rbd_secondary, pool, **pool_config
                )
                pool_spec = pool + "/" + pool_config.get("namespace")
            else:
                pool_spec = pool

            # Create group
            scale_map = {
                "test_1_group_50_images": (1, 50),
                "test_100_groups_100_images": (100, 1),
                "test_20_groups_5_images": (20, 5),
                "test_5_groups_20_images": (5, 20),
            }

            no_of_group, no_of_images_in_each_group = scale_map.get(
                scale_option, (1, 1)
            )

            group_image_kw = {
                "no_of_group": no_of_group,
                "no_of_images_in_each_group": no_of_images_in_each_group,
                "size_of_image": "30M",
                "pool_spec": pool_spec,
            }
            out = create_group_add_images(rbd_primary, **group_image_kw)
            log.info("Successfully created groups, images and added images to group")

            # Run IO on images
            group_config = {}
            image_spec_copy = []
            file_path_list = []
            for group_spec, images_spec in out.items():
                group_config.update({"group-spec": group_spec})
                for image_spec in images_spec:
                    io_config["rbd_obj"] = rbd_primary
                    io_config["client"] = client_primary
                    image_spec_copy.append(image_spec)
                    file_path_list.append("/mnt/mnt_" + random_string(len=5) + "/file")
                io_config["config"]["image_spec"] = image_spec_copy
                io_config["config"]["file_path"] = file_path_list
                io, err = krbd_io_handler(**io_config)
                if err:
                    raise Exception("Map, mount and run IOs failed for " + image_spec)
                else:
                    log.info("Map, mount and IOs successful for " + image_spec)

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

            group_info = json.loads(group_info_status)
            group_name_expected = group_config["group-spec"].split("/")[-1]

            if (
                group_info["group_name"] != group_name_expected
                or group_info["mirroring"]["state"] != "enabled"
                or group_info["mirroring"]["mode"] != "snapshot"
                or group_info["mirroring"]["primary"]
            ):
                raise Exception("Group info is not as expected on secondary cluster")

            log.info("Successfully verified group is present on secondary cluster")

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
            log.info("Successfully verified group status")

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


def run(**kw):
    """
    This test verifies 8.1 group mirroring rbd scale test case
    Args:
        kw: test data
    Returns:
        int: The return value. 0 for success, 1 otherwise

    """
    try:
        scale_options = [
            "test_1_group_50_images",
            "test_100_groups_100_images",
            "test_20_groups_5_images",
            "test_5_groups_20_images",
        ]
        scale_option = scale_options[random.randrange(len(scale_options))]
        log.info(
            "Running consistency group mirroring scale with scale option selected as: "
            + scale_option
        )
        pool_types = ["rep_pool_config", "ec_pool_config"]
        grouptypes = ["single_pool_without_namespace", "single_pool_with_namespace"]
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

        test_scale_options(
            rbd_primary,
            rbd_secondary,
            client_primary,
            client_secondary,
            primary_cluster,
            secondary_cluster,
            pool_types,
            scale_option,
            **kw,
        )
        log.info(
            "Test group mirroring with scale passed with scale option as "
            + scale_option
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
