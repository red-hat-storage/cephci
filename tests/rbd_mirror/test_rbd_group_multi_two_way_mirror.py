"""
Module to verify :
  -  Verify multiple mirrored groups with multiple images, some primary on cluster1 and some on cluster2

Test case covered:
CEPH-83613276 - Verify multiple mirrored groups with multiple images, some primary on cluster1 and some on cluster2

Pre-requisites :
1. Cluster must be up in 8.1 and above and running with capacity to create pool
2. We need atleast one client node with ceph-common package,
   conf and keyring files

Test Case Flow:
Step 1: Deploy Two ceph cluster on version 8.1 or above
Step 2: Create RBD pool ‘pool_1’ on both sites with/without namespace
Step 3: Enable Image mode mirroring on pool_1 on both sites
Step 4: Bootstrap the storage cluster peers (Two-way)
Step 6: Create Consistency groups, two groups on site-A and two on site-B
Step 7: Create and Add Images in the consistency groups.
Step 9: Add Data to the images.
Step 8: Enable Mirroring for the groups on site-A and site-B
Step 11: Wait for mirroring to complete on site-B and site-A
Step 12: Check all images are mirrored on site-A and site-B respectively.
Step 13: Validate size of each image should be same on site-a and site-b.
Step 14: Check groups are mirrored on site-B and site-A
Step 15: Check whether images are part of correct group on site-B and site-A
Step 16: Check group mirror status on both sites.
Step 17: Confirm that the global ids match for the groups on both clusters.
Step 18: Validate the integrity of the data on site-B and site-A for the respective groups
Step 21: Repeat above on EC pool with or without namespace
Step 22: Cleanup all rbd test objects like pools, images, groups etc
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
from ceph.rbd.workflows.group import (
    add_image_to_group_and_verify,
    create_group_and_verify,
)
from ceph.rbd.workflows.group_mirror import (
    enable_group_mirroring_and_verify_state,
    group_mirror_status_verify,
)
from ceph.rbd.workflows.krbd_io_handler import krbd_io_handler
from ceph.rbd.workflows.namespace import enable_namespace_mirroring
from utility.log import Log

log = Log(__name__)


def test_multi_group_two_way_mirror(
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
    Test Verify data is consistent for both manual and scheduled snapshots
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
        log.info("Running test CEPH-83613276  for %s", pool_type)

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
            # group_config = {}
            if "data_pool" in pool_config.keys():
                _ = pool_config.pop("data_pool")
            image_spec = []

            for image, image_config in pool_config.items():
                if "image" in image:
                    if "namespace" in pool_config:
                        pool_spec = pool + "/" + pool_config.get("namespace") + "/"
                    else:
                        pool_spec = pool + "/"
                    image_spec.append(pool_spec + image)
            group1_spec = pool_config.get("group-spec")
            if "namespace" in pool_config:
                enable_namespace_mirroring(
                    rbd_primary, rbd_secondary, pool, **pool_config
                )

            # Enable Group Mirroring and Verify
            enable_group_mirroring_and_verify_state(
                rbd_primary, **{"group-spec": group1_spec}
            )

            # Create second group on site-A
            group2 = "group_" + random_string(len=3)
            group2_spec = pool_spec + group2
            create_group_and_enable_mirroring(
                rbd_primary, client_primary, pool, group2, pool_config, io_config, **kw
            )
            group_mirror_status_verify(
                primary_cluster,
                secondary_cluster,
                rbd_primary,
                rbd_secondary,
                primary_state="up+stopped",
                secondary_state="up+replaying",
                **{"group-spec": group2_spec},
                global_id=True,
            )
            log.info(
                "Group states reached 'up+stopped' on site-A and 'up+replaying' on site-B"
            )

            # Create first group on site-B
            group3 = "group_" + random_string(len=3)
            group3_spec = pool_spec + group3
            create_group_and_enable_mirroring(
                rbd_secondary,
                client_secondary,
                pool,
                group3,
                pool_config,
                io_config,
                **kw,
            )
            group_mirror_status_verify(
                secondary_cluster,
                primary_cluster,
                rbd_secondary,
                rbd_primary,
                primary_state="up+stopped",
                secondary_state="up+replaying",
                **{"group-spec": group3_spec},
                global_id=True,
            )
            log.info(
                "Group states reached 'up+stopped' on site-B and 'up+replaying' on site-A"
            )
            # Create second group on site-B
            group4 = "group_" + random_string(len=3)
            group4_spec = pool_spec + group4
            create_group_and_enable_mirroring(
                rbd_secondary,
                client_secondary,
                pool,
                group4,
                pool_config,
                io_config,
                **kw,
            )
            group_mirror_status_verify(
                secondary_cluster,
                primary_cluster,
                rbd_secondary,
                rbd_primary,
                primary_state="up+stopped",
                secondary_state="up+replaying",
                **{"group-spec": group4_spec},
                global_id=True,
            )
            log.info(
                "Group states reached 'up+stopped' on site-B and 'up+replaying' on site-A"
            )

            # Check group is mirrored on site-B using group info
            for group_spec in [group1_spec, group2_spec]:
                group_info_status, err = rbd_secondary.group.info(
                    **{"group-spec": group_spec, "format": "json"}
                )
                if err:
                    raise Exception("Getting group info failed : " + str(err))
                if (
                    json.loads(group_info_status)["group_name"]
                    != group_spec.split("/")[-1]
                    or json.loads(group_info_status)["mirroring"]["state"] != "enabled"
                    or json.loads(group_info_status)["mirroring"]["mode"] != "snapshot"
                    or json.loads(group_info_status)["mirroring"]["primary"]
                ):
                    raise Exception("group info is not as expected on Site-B")
                log.info("Successfully verified group is present on Site-B")

            # Check group is mirrored on site-A using group info
            for group_spec in [group3_spec, group4_spec]:
                group_info_status, err = rbd_primary.group.info(
                    **{"group-spec": group_spec, "format": "json"}
                )
                if err:
                    raise Exception("Getting group info failed : " + str(err))
                if (
                    json.loads(group_info_status)["group_name"]
                    != group_spec.split("/")[-1]
                    or json.loads(group_info_status)["mirroring"]["state"] != "enabled"
                    or json.loads(group_info_status)["mirroring"]["mode"] != "snapshot"
                    or json.loads(group_info_status)["mirroring"]["primary"]
                ):
                    raise Exception("group info is not as expected on Site-A")
                log.info("Successfully verified group is present on Site-A")

            if rbd_primary.ls(
                **{"pool-spec": pool_spec.rstrip("/"), "format": "json"}
            ) != rbd_secondary.ls(
                **{"pool-spec": pool_spec.rstrip("/"), "format": "json"}
            ):
                raise Exception(
                    f"Images of {pool} on site-A and site-B are not same after two-way mirroring"
                )
            log.info(
                "Images of pool %s on site-A and site-B are same after two-way mirroring",
                pool,
            )

            group_images_dict = {}

            # Compare images and their sizes for groups mirrored from site-A to site-B
            group_image_list = compare_image_list_size(
                rbd_primary, rbd_secondary, group1_spec
            )
            group_images_dict.update({"group1": group_image_list})

            group_image_list = compare_image_list_size(
                rbd_primary, rbd_secondary, group2_spec
            )
            group_images_dict.update({"group2": group_image_list})

            # Compare images and their sizes for groups mirrored from site-B to site-A
            group_image_list = compare_image_list_size(
                rbd_primary, rbd_secondary, group3_spec
            )
            group_images_dict.update({"group3": group_image_list})

            group_image_list = compare_image_list_size(
                rbd_primary, rbd_secondary, group4_spec
            )
            group_images_dict.update({"group4": group_image_list})

            for group in group_images_dict.keys():
                # Validate the integrity of the data on site-A and site-B
                check_mirror_consistency(
                    rbd_primary,
                    rbd_secondary,
                    client_primary,
                    client_secondary,
                    group_images_dict[group],
                )
                log.info(
                    "Successfully verified md5sum of all images matches across both clusters for images %s",
                    group_images_dict[group],
                )


def compare_image_list_size(rbd_primary, rbd_secondary, group_spec):
    """
    - Lists and compares images in the group on site-A and site-B
    - Compared the sizes of the images on site-A and site-B using 'rbd du'

    Args:
        rbd primary (module): The rbd site-A object
        rbd Secondary (module): The rbd site-B object
        group spec: Group spec (pool/[namespace]/group)

    Returns:
        image list in the group if comparisons match, otherwise raises Exception
    """
    group_image_list, err = rbd_primary.group.image.list(
        **{"group-spec": group_spec}, format="json"
    )
    if err:
        raise Exception("Getting group image list failed : " + str(err))

    if (
        group_image_list
        != rbd_secondary.group.image.list(
            **{"group-spec": group_spec, "format": "json"}
        )[0]
    ):
        raise Exception(
            f"Images are not same on site A and site-B for group {group_spec}"
        )

    compare_image_size_primary_secondary(rbd_primary, rbd_secondary, group_image_list)
    log.info(
        "Successfully verified size of rbd images across both clusters for group %s",
        group_spec,
    )
    return group_image_list


def create_group_and_enable_mirroring(
    rbd, client, pool, group, pool_config, io_config, **kw
):
    """
    - Creates a group
    - Create 2 images
    - Add both images to group
    - Writes IO on images
    - Enables group mirroring

    Args:
        rbd (module): The rbd object
        client (object): client object.
        pool: poolname
        group: group name
        pool config (object) : pool configuration object
        io config: IO configuration parameters
        kw (dict): A dictionary of keyword arguments.

    Returns:
        int: 0 if all operations success, otherwise raises Exception
    """
    group_config = {"client": client, "pool": pool, "group": group}

    images = ["image_" + random_string(len=5), "image_" + random_string(len=5)]
    image_specs = []
    for image in images:
        if "namespace" in pool_config:
            pool_spec = pool + "/" + pool_config.get("namespace") + "/"
            group_spec = f"{pool}/{pool_config.get('namespace')}/{group}"
            group_config.update({"namespace": pool_config.get("namespace")})
        else:
            pool_spec = pool + "/"
            group_spec = f"{pool}/{group}"

        image_spec = pool_spec + image
        image_specs.append(image_spec)
        rbd.create(**{"image-spec": image_spec, "size": "2G"})
    create_group_and_verify(**group_config)
    for image_spec in image_specs:
        rc = add_image_to_group_and_verify(
            **{
                "group-spec": group_spec,
                "image-spec": image_spec,
                "client": client,
            }
        )
        if rc != 0:
            return rc
    image_spec_copy = deepcopy(image_specs)
    io_config["rbd_obj"] = rbd
    io_config["client"] = client
    io_config["config"]["image_spec"] = image_spec_copy
    io, err = krbd_io_handler(**io_config)
    if err:
        raise Exception("Map, mount and run IOs failed for " + str(image_specs))
    else:
        log.info("Map, mount and IOs successful for " + str(image_specs))

    # Enable Group Mirroring and Verify
    enable_group_mirroring_and_verify_state(rbd, **{"group-spec": group_spec})

    return 0


def run(**kw):
    """
    This test verifies:
    -   Verify multiple mirrored groups with multiple images, some primary on
        cluster1 and some on cluster2
    Args:
        kw: test data
    Returns:
        int: The return value. 0 for success, 1 otherwise

    """
    test_name = kw["run_config"]["test_name"][:-2:].replace("_", " ")
    try:
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

        test_multi_group_two_way_mirror(
            rbd_primary,
            rbd_secondary,
            client_primary,
            client_secondary,
            primary_cluster,
            secondary_cluster,
            pool_types,
            **kw,
        )

        log.info("Test %s passed", test_name)
    except Exception as e:
        log.error("Test %s failed with error %s", test_name, e)
        return 1

    finally:
        cleanup(pool_types=pool_types, multi_cluster_obj=mirror_obj, **kw)

    return 0
