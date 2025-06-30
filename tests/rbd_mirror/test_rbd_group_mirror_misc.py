"""
Module to verify :
  - Add or remove group mirror snapshot schedule when client is blocklisted
  - Image level & pool level promote/demote when group mirroring is enabled/disabled

Test case covered:
CEPH-83613275 - Add or remove group mirror snapshot schedule when client is blocklisted
CEPH-83614239 - Image level & pool level promote/demote when group mirroring is enabled/disabled

Pre-requisites :
1. Cluster must be up in 8.1 and above and running with capacity to create pool
2. We need atleast one client node with ceph-common package,
   conf and keyring files

CEPH-83613275:
Test Case Flow:
Step 1: Deploy Two ceph cluster on version 8.1 or above
Step 2: Create RBD pool ‘pool_1’ on both sites with/without namespace
Step 3: Enable Image mode mirroring on pool_1 on both sites
Step 4: Bootstrap the storage cluster peers (Two-way)
Step 5: Create 2 RBD images in pool_1
Step 6: Add data to the images
Step 7: Create Consistency group
Step 8: Add Images in the consistency group
Step 9: Enable Mirroring for the group
Step 10: Add mirror group snapshot schedule
Step 11: Blocklist the client
Step 12: Add another group mirror snapshot schedule
step 13: Removing the group mirror snapshot schedule when client is blocklisted
step 14: Remove the client from blocklisting
step 15: verify the snapshot schedules
step 16: Repeat above on EC pool with or without namespace.
Step 17: Cleanup rbd test objects like pool, images, groups etc

CEPH-83614239:
Test Case Flow:
Step 1: Deploy Two ceph cluster on version 8.1 or above
Step 2: Create RBD pool ‘pool_1’ on both sites with/without namespace
Step 3: Enable Image mode mirroring on pool_1 on both sites
Step 4: Bootstrap the storage cluster peers (Two-way)
Step 5: Create 2 RBD images in pool_1
Step 6: Add data to the images
Step 7: Create Consistency group
Step 8: Add Images in the consistency group
Step 9: Enable Mirroring for the group
Step 10: Wait for replication to complete
Step 11: Demote site-a image1, should fail
Step 12: Promote image1 at site-b, should fail
Step 13: Demote pool_1 at site-a, should fail
Step 14: Promote pool_1 at site-b, should fail
Step 15: Disable group mirroring
Step 16: Remove image from group
Step 17: Enable mirroring on images
Step 18: Demote site-a image1, should succeed
Step 19:  Promote image1 at site-b, should succeed
Step 20: Demote pool_1 at site-b, should demote all images in pool_1
Step 21: Promote pool_1 at site-a, should promote all images in pool_1
Step 22: Repeat above on EC pool with or without namespace
Step 23: Cleanup the images, file and pools
"""

import random
import time
from copy import deepcopy

from ceph.rbd.initial_config import initial_mirror_config
from ceph.rbd.utils import exec_cmd, getdict, random_string
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.group_mirror import (
    disable_group_mirroring_and_verify_state,
    enable_group_mirroring_and_verify_state,
    remove_group_image_and_verify,
    verify_group_snapshot_ls,
    verify_group_snapshot_schedule,
    wait_for_idle,
)
from ceph.rbd.workflows.krbd_io_handler import krbd_io_handler
from ceph.rbd.workflows.namespace import enable_namespace_mirroring
from ceph.rbd.workflows.snap_scheduling import add_snapshot_scheduling
from utility.log import Log

log = Log(__name__)


def test_group_consistency(
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
    Test Add or remove group mirror snapshot schedule when client is blocklisted
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
        log.info("Running test CEPH-83613275  for %s", pool_type)
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
            group_config = {}
            if "data_pool" in pool_config.keys():
                _ = pool_config.pop("data_pool")
            group_spec = pool_config.get("group-spec")
            group_config.update({"group-spec": group_spec})

            image_spec = []
            for image, image_config in pool_config.items():
                if "image" in image:
                    if "namespace" in pool_config:
                        pool_spec = pool + "/" + pool_config.get("namespace") + "/"
                    else:
                        pool_spec = pool + "/"
                    image_spec.append(pool_spec + image)
            if "namespace" in pool_config:
                enable_namespace_mirroring(
                    rbd_primary, rbd_secondary, pool, **pool_config
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

            # Enable Group Mirroring and Verify
            enable_group_mirroring_and_verify_state(
                rbd_primary, **{"group-spec": group_spec}
            )

            # Wait for group mirroring to complete
            wait_for_idle(rbd_primary, **{"group-spec": group_spec})
            log.info("Data replay state is idle for all images in the group")

            snap_schedule_config = {
                "pool": pool,
                "image": image,
                "level": "group",
                "group": pool_config.get("group"),
                "interval": "1m",
            }
            if "namespace" in pool_config:
                snap_schedule_config.update({"namespace": pool_config.get("namespace")})

            out, err = add_snapshot_scheduling(rbd_primary, **snap_schedule_config)
            if err:
                raise Exception(
                    "Failed to add group snapshot schedule of 1m before blocklist"
                )
            log.info("Added group snapshot schedule of 1m before client blocklist")
            snap_schedule_config.update({"interval": "3m"})
            out, err = add_snapshot_scheduling(rbd_primary, **snap_schedule_config)
            if err:
                raise Exception("Failed to add group snapshot schedule of 3m")
            log.info("Added group snapshot schedule of 3m before client blocklist")
            if exec_cmd(
                node=client_primary,
                cmd=f"ceph osd blocklist add {client_primary.ip_address}",
            ):
                raise Exception(
                    "Failed to blocklist the client %s", client_primary.ip_address
                )
            log.info("Client successfully blocklisted")

            snap_schedule_config.update({"interval": "2m"})
            out, err = add_snapshot_scheduling(rbd_primary, **snap_schedule_config)
            if err:
                raise Exception(
                    "Failed to add group snapshot schedule of 2m after cliet blocklist"
                )
            log.info("Added group snapshot schedule of 2m after client blocklist")

            snap_schedule_rm_config = deepcopy(snap_schedule_config)
            snap_schedule_rm_config.update({"interval": "3m"})
            snap_schedule_rm_config.pop("image")
            snap_schedule_rm_config.pop("level")
            out, err = rbd_primary.mirror.group.snapshot.schedule.remove_(
                **snap_schedule_rm_config
            )
            if err:
                raise Exception(
                    "Failed to remove group snapshot schedule of 3m after client blocklist"
                )
            log.info("Removed group snapshot schedule of 3m after client blocklist")

            status_spec = {
                "pool": pool,
                "group": pool_config.get("group"),
                "format": "json",
            }
            if "namespace" in pool_config:
                status_spec.update({"namespace": pool_config.get("namespace")})
            else:
                group_spec = pool + "/" + pool_config.get("group")

            if verify_group_snapshot_ls(rbd_primary, group_spec, "1m", **status_spec):
                raise Exception("Failed to verify group snapshot schedule of 1m")
            log.info(
                "Verified Snapshot schedule of 1m set before client blocklisting is preserved"
            )

            if verify_group_snapshot_ls(rbd_primary, group_spec, "2m", **status_spec):
                raise Exception("Failed to verify group snapshot schedule of 2m")
            log.info("Verified Snapshot schedule of 2m set after client blocklisting")

            if exec_cmd(
                node=client_primary,
                cmd=f"ceph osd blocklist rm {client_primary.ip_address}",
            ):
                raise Exception(
                    "Failed to remove the client %s from blocklisting",
                    client_primary.ip_address,
                )
            log.info(
                "Removed the client %s from blocklisting", client_primary.ip_address
            )
            time.sleep(10)
            if verify_group_snapshot_schedule(
                rbd_primary,
                pool,
                pool_config.get("group"),
                "1m",
                namespace=pool_config.get("namespace"),
            ):
                raise Exception("Failed to verify Snapshot creation as per 1m schedule")
            if verify_group_snapshot_schedule(
                rbd_primary,
                pool,
                pool_config.get("group"),
                "2m",
                namespace=pool_config.get("namespace"),
            ):
                raise Exception("Failed to verify Snapshot creation as per 2m schedule")

            snap_schedule_rm_config.update({"interval": "1m"})
            out, err = rbd_primary.mirror.group.snapshot.schedule.remove_(
                **snap_schedule_rm_config
            )
            if err:
                raise Exception("Failed to remove group snapshot schedule of 1m")
            log.info("Removed group snapshot schedule of 1m ")

            snap_schedule_rm_config.update({"interval": "2m"})
            out, err = rbd_primary.mirror.group.snapshot.schedule.remove_(
                **snap_schedule_rm_config
            )
            if err:
                raise Exception("Failed to remove group snapshot schedule of 2m")
            log.info("Removed group snapshot schedule of 2m")


def test_rbd_group_mirror_unsupported_ops(
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
    Image level & pool level promote/demote when group mirroring is enabled/disabled
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
        log.info("Running test CEPH-83614239  for %s", pool_type)

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
                        pool_spec = pool + "/" + pool_config.get("namespace")
                    else:
                        pool_spec = pool
                    image_spec.append(pool_spec + "/" + image)
            if "namespace" in pool_config:
                enable_namespace_mirroring(
                    rbd_primary, rbd_secondary, pool, **pool_config
                )

            # Enable Group Mirroring and Verify
            enable_group_mirroring_and_verify_state(
                rbd_primary, **{"group-spec": group_spec}
            )

            # Wait for group mirroring to complete
            wait_for_idle(rbd_primary, **{"group-spec": group_spec})
            log.info("Data replay state is idle for all images in the group")

            out, err = rbd_primary.mirror.image.demote(**{"image-spec": image_spec[0]})
            if "cannot demote an image that is member of a group" in err:
                log.info("Failed to demote image on site-A as image is member of group")
            else:
                raise Exception(
                    "Demote image on site-A is successful when image is member of group"
                )

            out, err = rbd_secondary.mirror.image.promote(
                **{"image-spec": image_spec[0]}
            )
            if "cannot promote an image that is member of a group" in err:
                log.info("Failed to promote image as image is part of group")
            else:
                raise Exception(
                    "Promote image is successful when image is member of group"
                )

            out, err = rbd_primary.mirror.pool.demote(**{"pool": pool})
            if "Demoted 0 mirrored images" in out:
                log.info(
                    "Failed to Demote pool on site-A as images are members of group"
                )
            else:
                raise Exception(
                    "Demote pool is successful when images are members of group"
                )

            out, err = rbd_secondary.mirror.pool.promote(**{"pool": pool})
            if "Promoted 0 mirrored images" in out:
                log.info(
                    "Failed to promote pool on site-B as images are members of group"
                )
            else:
                raise Exception(
                    "Promote pool on site-B is successful when images are members of group"
                )

            disable_group_mirroring_and_verify_state(
                rbd_primary, **{"group-spec": group_spec}
            )

            remove_group_image_and_verify(
                rbd_primary, **{"group-spec": group_spec, "image-spec": image_spec[0]}
            )

            out, err = rbd_primary.mirror.image.enable(
                **{"image-spec": image_spec[0], "mode": "snapshot"}
            )
            if err:
                raise Exception(
                    "Enable image on site-A failed when image is not a member of group"
                )

            if "Mirroring enabled" in out:
                log.info(
                    "Enable image on site-A is successful when image is not a member of group"
                )

            time.sleep(10)
            out, err = rbd_primary.mirror.image.demote(**{"image-spec": image_spec[0]})
            if err:
                raise Exception(
                    "Demote image failed when image is not a member of group"
                )
            if "Image demoted to non-primary" in out:
                log.info(
                    "Demote image is successful when image is not a member of group"
                )

            time.sleep(10)
            out, err = rbd_secondary.mirror.image.promote(
                **{"image-spec": image_spec[0]}
            )
            if err:
                raise Exception(
                    "Promote image on site-B failed when image is not a member of group"
                )
            if "Image promoted to primary" in out:
                log.info(
                    "Promote image on site-B is successful when image is not a member of group"
                )

            out, err = rbd_secondary.mirror.pool.demote(**{"pool-spec": pool_spec})
            if err:
                raise Exception(
                    "Demote pool on site-B failed when image is not a member of group"
                )
            if "Demoted 1 mirrored images" in out:
                log.info(
                    "Demote pool is successful when image is not a member of group"
                )

            out, err = rbd_primary.mirror.pool.promote(**{"pool-spec": pool_spec})
            if err:
                raise Exception(
                    "Promote pool on site-A failed when image is not a member of group"
                )
            if "Promoted 1 mirrored images" in out:
                log.info(
                    "Promote pool successful on site-A when image is not a member of group"
                )


def run(**kw):
    """
    This test verifies:
    - add or remove group mirror snapshot schedule when client is blocklisted
    - Image level & pool level promote/demote when group mirroring is enabled/disabled
    Args:
        kw: test data
    Returns:
        int: The return value. 0 for success, 1 otherwise

    """
    test_name = kw["run_config"]["test_name"][:-2:].replace("_", " ")
    try:
        pool_types = ["rep_pool_config", "ec_pool_config"]
        test_map = {
            "CEPH-83613275": test_group_consistency,
            "CEPH-83614239": test_rbd_group_mirror_unsupported_ops,
        }

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
        test_func = kw["config"]["operation"]
        if test_func in test_map:
            test_map[test_func](
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
        if "blocklist" in test_name:
            exec_cmd(
                node=client_primary,
                cmd=f"ceph osd blocklist rm {client_primary.ip_address}",
            )
            log.info(
                "Removed the client %s from blocklisting", client_primary.ip_address
            )
        cleanup(pool_types=pool_types, multi_cluster_obj=mirror_obj, **kw)

    return 0
