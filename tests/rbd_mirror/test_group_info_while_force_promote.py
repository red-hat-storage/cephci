"""
Module to verify :
  - Verify Group info should verify snapshot completion before marking the group as primary

Test case covered:
CEPH-83620649 - Group info should verify snapshot completion before marking the group as primary

Pre-requisites :
1. Cluster must be up in 8.1 and above and running with capacity to create pool
2. We need atleast one client node with ceph-common package,
   conf and keyring files

TC#1: Test Case Flow:
Step1: Deploy Two ceph cluster on version 8.1 or later
Step 2: Create RBD pool 'pool_1' on both sites
Step 3: Enable Image mode mirroring on pool_1 on both sites
Step 4: Bootstrap the storage cluster peers (Two-way)
Step 5: Create 2 RBD images in pool_1
Step 6: Add data to the images
Step 7: Create Consistency group
Step 8: Add Images in the consistency group
Step 9: Enable Mirroring for the group
Step 10: wait for 50% sync to complete
Step 11: Perform force promote on site-b
step 12: While force promote is in progress, Check group snap list on site-b,
the snapshot should not be marked complete till force promote is in progress
Step 13: Check group info in loop, till force promote is in progress 'primary'
field should not be marked true
Step 14: Once force promotes complete, check group snapshot list should mark the
snapshot complete and group info should set 'primary' field to 'true'
Step 15: Repeat above on EC pool (with namespace optimisation)
Step 16: Cleanup all objects (pool, images, groups)
"""

import json
import random
import time
from copy import deepcopy

from ceph.parallel import parallel
from ceph.rbd.initial_config import initial_mirror_config
from ceph.rbd.utils import getdict, random_string
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.group_mirror import (
    enable_group_mirroring_and_verify_state,
    get_mirror_group_snap_copied_status,
    get_mirror_group_snap_id,
    get_snap_state_by_snap_id,
    wait_for_idle,
    wait_till_image_sync_percent,
)
from ceph.rbd.workflows.krbd_io_handler import krbd_io_handler
from ceph.rbd.workflows.namespace import enable_namespace_mirroring
from utility.log import Log

log = Log(__name__)


def test_group_info_during_force_promote(
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
    Test Group info should verify snapshot completion before marking the group as primary
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
        log.info("Running test CEPH-83620649 for %s", pool_type)

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

            if "namespace" in pool_config:
                enable_namespace_mirroring(
                    rbd_primary, rbd_secondary, pool, **pool_config
                )
                log.info(
                    "Successfully Enabled mirroring on Namespace "
                    + pool_config["namespace"]
                )

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

            image_spec = []
            for image, image_config in pool_config.items():
                if "image" in image:
                    if "namespace" in pool_config:
                        image_spec.append(
                            pool + "/" + pool_config.get("namespace") + "/" + image
                        )
                    else:
                        image_spec.append(pool + "/" + image)

            image_spec_copy = deepcopy(image_spec)
            io_config["rbd_obj"] = rbd_primary
            io_config["client"] = client_primary
            io_config["config"]["image_spec"] = image_spec_copy
            io, err = krbd_io_handler(**io_config)
            if err:
                raise Exception("Map, mount and run IOs failed for " + str(image_spec))
            else:
                log.info("Map, mount and IOs successful for " + str(image_spec))

            # Create manual group mirror snapshot
            out, err = rbd_primary.mirror.group.snapshot.add(
                **{"group-spec": group_spec}
            )
            if err:
                raise Exception("Failed to add manual mirror group snapshot %s", out)
            snapshot_id = out.split(" ")[-1].rstrip()

            # Wait for 50% sync to complete
            status_spec = {
                "pool": pool,
                "group": pool_config.get("group"),
                "format": "json",
            }
            if "namespace" in pool_config:
                status_spec.update({"namespace": pool_config.get("namespace")})
            else:
                group_spec = pool + "/" + pool_config.get("group")

            wait_till_image_sync_percent(rbd_primary, 50, **group_config)
            log.info("Successfully waited for 50 percent sync completion")

            """ Perform Force promote, group info and group snapshot list parallelly to verify
            group info primary should be false and snapshot list should show incomplete snapshot """
            with parallel() as p:
                p.spawn(
                    force_promote_secondary,
                    group_spec,
                    rbd_secondary=rbd_secondary,
                )
                p.spawn(
                    check_group_snap_list,
                    snapshot_id,
                    verify_state="not copied",
                    rbd_secondary=rbd_secondary,
                    **status_spec,
                )
                p.spawn(
                    check_group_info,
                    expected_primary_field=False,
                    rbd_secondary=rbd_secondary,
                    **group_config,
                )

            # Wait for force promote to complete
            time.sleep(10)

            # Check group snap list on site-b, the snapshot should be marked complete
            snapshot_id = get_mirror_group_snap_id(rbd_secondary, **status_spec)
            check_group_snap_list(
                snapshot_id, "copied", rbd_secondary=rbd_secondary, **status_spec
            )
            snap_state = get_snap_state_by_snap_id(
                rbd_secondary, snapshot_id, **status_spec
            )
            if snap_state != "created":
                raise Exception(
                    "Snapshot state did not turn created even after snapshot data sync complete/copied"
                )

            # Check group info, 'primary' field should be marked true
            check_group_info(
                expected_primary_field=True, rbd_secondary=rbd_secondary, **group_config
            )


def force_promote_secondary(group_spec, rbd_secondary):
    out, err = rbd_secondary.mirror.group.promote(
        **{"group-spec": group_spec, "force": True}
    )
    if err:
        raise Exception("Failed to force promote group on site-B: " + str(err))
    else:
        log.info("Successfully completed force promote on secondary")


def check_group_snap_list(snapshot_id, verify_state, rbd_secondary, **status_spec):
    if verify_state == "copied":
        verify_state = True
    elif verify_state == "not copied":
        verify_state = False
    snap_copied_status = get_mirror_group_snap_copied_status(
        rbd_secondary,
        snapshot_id,
        **status_spec,
    )
    if snap_copied_status != verify_state:
        raise Exception(
            "Snap state should not turn "
            + verify_state
            + " before force promote completion"
        )
    log.info(
        "Successfully snapshot created and data sync completed status to be "
        + str(verify_state)
    )


def check_group_info(expected_primary_field, rbd_secondary, **group_config):
    group_info_status, err = rbd_secondary.group.info(**group_config, format="json")
    if err:
        raise Exception("Getting group info failed : " + str(err))
    primary_value = json.loads(group_info_status)["mirroring"]["primary"]
    if primary_value != expected_primary_field:
        raise Exception(
            "group info primary field changes to " + str(primary_value) + " prematurely"
        )

    log.info(
        "Successfully verified group info primary field is "
        + str(expected_primary_field)
    )


def run(**kw):
    """
    This test verifies Group info should verify snapshot completion before marking the group as primary
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

        test_group_info_during_force_promote(
            rbd_primary,
            rbd_secondary,
            client_primary,
            client_secondary,
            primary_cluster,
            secondary_cluster,
            pool_types,
            **kw,
        )
        log.info("Test group info verification during force promote passed")

    except Exception as e:
        log.error(
            "Test: RBD group mirroring (snapshot mode) across two clusters failed: "
            + str(e)
        )
        return 1

    finally:
        cleanup(pool_types=pool_types, multi_cluster_obj=mirror_obj, **kw)
    return 0
