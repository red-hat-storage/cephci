"""
Module to verify dynamic addition of images to a mirror-enabled RBD consistency group.

Test cases covered:
    CEPH-83632349 - Dynamic group mirroring — add images to an already mirror-enabled
                group without disabling mirroring, during I/O and snapshot scheduling
    Test Steps:
    1. Deploy two Ceph clusters running version 9.1 or later.
    2. Configure cluster peering for RBD mirroring between primary and secondary clusters.
    3. Create a replicated RBD pool on the primary cluster and enable required RBD pool features,
        such as object-map and fast-diff.
    4. Enable RBD pool mirroring in snapshot mode.
    5. Create an RBD group in the pool.
    6. Create two RBD images, image1 and image2, and add them to the group.
    7. Enable group mirroring for the created group.
    8. Perform write I/O on image1 and image2 and wait for synchronization to complete.
    9. Verify on the secondary cluster that both images exist and are in replaying/synced state.
    10. Create a new RBD image, image3, in the same pool.
    11. Add image3 to the already mirror-enabled group using rbd group image add.
    12. Monitor mirror status for all group images.
    13. Wait for image3 synchronization to complete.
    14. Verify on the secondary cluster that image3 now exists.
    15. Perform additional write I/O on all three images and verify replication.
    16. Calculate MD5 checksum for image1, image2, and image3 on primary and secondary clusters.
    17. Start continuous write I/O workload, such as fio, on image1 and image2.
    18. While I/O is ongoing, create another image, image4.
    19. Add image4 to the mirror-enabled group while active I/O continues.
    20. Monitor mirror status for all images.
    21. Allow image4 synchronization to complete while I/O is running.
    22. Stop the active I/O workload.
    23. Configure snapshot scheduling for the group, for example every 1 minute,
        and add one more schedule with the start time option.
    24. Perform write I/O on images and allow 2 to 3 scheduled snapshots to be created.
    25. Verify scheduled snapshots exist on both clusters.
    26. Create another new image, image5, while snapshot scheduling is active.
    27. Add image5 to the mirror-enabled group using rbd group image add.
    28. Monitor snapshot and mirror status during the operation.
    29. Allow the snapshot scheduler to trigger additional cycles during image5 synchronization.
    30. Wait for image5 to reach up+synced state.
    31. Verify image5 now participates in subsequent scheduled snapshots.
    32. Calculate MD5 checksum for all images, image1 to image5.
    33. Verify mirror state counters/logs for existing images.
    34. Verify overall group mirror and scheduler health status.
    35. Remove images from the group.
    36. Delete images and pool.

Pre-requisites:
1. Two Ceph clusters version 9.1 or later with mon, mgr, osd
2. rbd-mirror daemon on both clusters
3. Client node with ceph-common, fio
"""

import datetime
import json
import random
import time
from copy import deepcopy

from ceph.parallel import parallel
from ceph.rbd.initial_config import initial_mirror_config
from ceph.rbd.mirror_utils import check_mirror_consistency
from ceph.rbd.utils import exec_cmd, getdict, random_string
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.group_mirror import (
    add_group_image_and_verify,
    disable_group_mirroring_and_verify_state,
    enable_group_mirroring_and_verify_state,
    get_peer_image_global_ids,
    get_snap_state_by_snap_id,
    group_mirror_status_verify,
    mirror_group_snapshot_add_and_wait_sync,
    remove_group_image_and_verify,
    verify_group_snapshot_ls,
    verify_group_snapshot_schedule,
    verify_peer_image_global_ids_unchanged,
    wait_for_idle,
)
from ceph.rbd.workflows.krbd_io_handler import krbd_io_handler
from ceph.rbd.workflows.namespace import enable_namespace_mirroring
from ceph.rbd.workflows.snap_scheduling import (
    add_snapshot_scheduling,
    remove_snapshot_scheduling,
)
from utility.log import Log
from utility.utils import run_fio

log = Log(__name__)


def test_group_mirror_dynamic(
    rbd_primary,
    rbd_secondary,
    client_primary,
    client_secondary,
    primary_cluster,
    secondary_cluster,
    pool_type,
    **kw,
):
    """
    Verify images can be added to a mirror-enabled consistency group dynamically.
    Args:
        rbd_primary: RBD object of primary cluster
        rbd_secondary: RBD object of secondary cluster
        client_primary: client node object of primary cluster
        client_secondary: client node object of secondary cluster
        primary_cluster: Primary cluster object
        secondary_cluster: Secondary cluster object
        pool_type: Replication pool or EC pool
        **kw: any other arguments
    """
    log.info("Running test CEPH-83632349 for %s", pool_type)
    fio = kw.get("config", {}).get("fio", {})
    io_fio_args = {
        "size": fio["size"],
        "num_jobs": fio["ODF_CONFIG"]["num_jobs"],
        "iodepth": fio["ODF_CONFIG"]["iodepth"],
        "rwmixread": fio["ODF_CONFIG"]["rwmixread"],
        "direct": fio["ODF_CONFIG"]["direct"],
        "invalidate": fio["ODF_CONFIG"]["invalidate"],
        "io_type": fio["ODF_CONFIG"]["io_type"],
        "run_time": fio["runtime"],
        "cmd_timeout": 2400,
    }
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

    rbd_config = kw.get("config", {}).get(pool_type, {})
    multi_pool_config = deepcopy(getdict(rbd_config))

    for pool, pool_config in multi_pool_config.items():
        group_config = {}
        pool_config.pop("data_pool", None)

        # Track schedules added during this pool's test so we can always
        # remove them in the finally block — even on failure.  Leaving
        # schedules behind when a pool is deleted triggers a Ceph bug
        # where the scheduler stops generating new snapshots on the next run.
        added_schedules = []  # list of dicts suitable for remove_snapshot_scheduling

        try:
            group_spec = pool_config.get("group-spec")
            group_config.update({"group-spec": group_spec})
            image_spec = []
            for image in pool_config:
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

            image_size = rbd_config.get("size", "4G")
            image_file_paths = {}
            io_file_paths = [
                "/mnt/mnt_" + random_string(len=5) + "/file" for _ in image_spec
            ]
            for spec, fp in zip(image_spec, io_file_paths):
                image_file_paths[spec] = fp

            _, err = rbd_primary.mirror.group.status(**group_config)
            if err:
                known_messages = [
                    "mirroring disabled",
                    "mirroring not enabled on the group",
                ]
                if any(msg in err for msg in known_messages):
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

            if mirror_state == "Disabled":
                enable_group_mirroring_and_verify_state(rbd_primary, **group_config)
            log.info("Successfully Enabled group mirroring")

            wait_for_idle(rbd_primary, rbd_secondary=rbd_secondary, **group_config)

            io_config["rbd_obj"] = rbd_primary
            io_config["client"] = client_primary
            io_config["config"]["image_spec"] = deepcopy(image_spec)
            io_config["config"]["file_path"] = io_file_paths
            _, err = krbd_io_handler(**io_config)
            if err:
                raise Exception("Map, mount and run IOs failed for " + str(image_spec))
            log.info("Map, mount and IOs successful for " + str(image_spec))

            wait_for_idle(rbd_primary, rbd_secondary=rbd_secondary, **group_config)
            log.info(
                "Successfully completed sync for group mirroring to secondary site"
            )

            # add mirror snapshot and wait for sync
            mirror_group_snapshot_add_and_wait_sync(
                rbd_primary,
                rbd_secondary,
                **group_config,
            )
            wait_for_idle(rbd_primary, rbd_secondary=rbd_secondary, **group_config)

            group_image_list, err = rbd_primary.group.image.list(
                **group_config, format="json"
            )
            if err:
                raise Exception("Getting group image list failed : " + str(err))
            log.info(
                "Successfully completed initial sync and mirror snapshot to secondary site"
            )

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

            group_image_list_primary, err = rbd_primary.group.image.list(
                **group_config, format="json"
            )
            if err:
                raise Exception("Getting primary group image list failed : " + str(err))

            group_image_list_secondary, err = rbd_secondary.group.image.list(
                **group_config, format="json"
            )
            if err:
                raise Exception(
                    "Getting secondary group image list failed : " + str(err)
                )
            if json.loads(group_image_list_primary) != json.loads(
                group_image_list_secondary
            ):
                raise Exception(
                    "Group image list does not match for primary and secondary cluster"
                )
            log.info(
                "Successfully verified image list for the group matches across both cluster"
            )

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

            snap_schedule_config = {
                "pool": pool,
                "level": "group",
                "group": pool_config.get("group"),
                "interval": "1m",
            }
            if "namespace" in pool_config:
                snap_schedule_config["namespace"] = pool_config.get("namespace")

            _, err = add_snapshot_scheduling(rbd_primary, **snap_schedule_config)
            if err:
                raise Exception("Failed to add group snapshot schedule of 1m")
            added_schedules.append(dict(snap_schedule_config))

            schedule_status_spec = {
                "pool": pool,
                "group": pool_config.get("group"),
                "format": "json",
            }
            if "namespace" in pool_config:
                schedule_status_spec["namespace"] = pool_config.get("namespace")
            status_spec = dict(schedule_status_spec)

            if verify_group_snapshot_ls(
                rbd_primary, group_spec, "1m", **schedule_status_spec
            ):
                raise Exception("Failed to verify group snapshot schedule of 1m")
            log.info("Added and verified group snapshot schedule of 1m")

            stable_names = [s.split("/")[-1] for s in image_spec]
            baseline_ids = get_peer_image_global_ids(
                rbd_primary, stable_names, **group_config
            )

            # Steps 10–16: add image_3 to mirror-enabled group
            image_3_spec = pool_spec + "/image_3"
            _, err = rbd_primary.create(
                **{"image-spec": image_3_spec, "size": image_size}
            )
            if err:
                raise Exception(
                    "Failed in creating image: " + image_3_spec + ", err: " + err
                )
            log.info(
                "Successfully created image "
                + image_3_spec
                + " for dynamic mirror group"
            )

            add_group_image_and_verify(
                rbd_primary,
                **{"group-spec": group_spec, "image-spec": image_3_spec},
            )
            log.info(
                "Successfully added the image "
                + image_3_spec
                + " to the mirror group dynamically"
            )
            image_spec.append(image_3_spec)

            wait_for_idle(rbd_primary, rbd_secondary=rbd_secondary, **group_config)
            verify_peer_image_global_ids_unchanged(
                rbd_primary,
                baseline_ids,
                after_event="adding image_3",
                **group_config,
            )

            group_image_list, err = rbd_primary.group.image.list(
                **group_config, format="json"
            )
            if err:
                raise Exception("Getting group image list failed : " + str(err))
            mirror_group_snapshot_add_and_wait_sync(
                rbd_primary,
                rbd_secondary,
                **group_config,
            )
            # Run I/O on image_3 (map + mount + io, auto-unmaps after)
            image_3_file = "/mnt/mnt_" + random_string(len=5) + "/file"
            image_file_paths[image_3_spec] = image_3_file
            image_3_io = deepcopy(io_config)
            image_3_io["config"]["image_spec"] = [image_3_spec]
            image_3_io["config"]["file_path"] = [image_3_file]
            _, err = krbd_io_handler(**image_3_io)
            if err:
                raise Exception("Map, mount and run IOs failed for " + image_3_spec)
            log.info("IO successful on dynamically added image %s", image_3_spec)

            # Run incremental I/O on existing images via run_fio
            for spec in image_spec[:-1]:
                run_fio(
                    client_node=client_primary,
                    filename=image_file_paths[spec],
                    get_time_taken=True,
                    **io_fio_args,
                )
            log.info("Incremental IO successful for %s", image_spec)

            group_image_list, err = rbd_primary.group.image.list(
                **group_config, format="json"
            )
            if err:
                raise Exception("Getting group image list failed : " + str(err))
            mirror_group_snapshot_add_and_wait_sync(
                rbd_primary,
                rbd_secondary,
                **group_config,
            )
            wait_for_idle(rbd_primary, rbd_secondary=rbd_secondary, **group_config)

            # Validate data integrity after image_3 sync
            check_mirror_consistency(
                rbd_primary,
                rbd_secondary,
                client_primary,
                client_secondary,
                group_image_list,
            )
            log.info("Incremental I/O and md5sum verified after image_3 sync")

            stable_names.append("image_3")
            baseline_ids = get_peer_image_global_ids(
                rbd_primary, stable_names, **group_config
            )

            # Steps 17–22: continuous I/O on image_1/image_2 while adding image_4
            # Run fio jobs in the background while dynamically adding image_4 to the
            # mirror-enabled group. The operations must happen concurrently, so
            # image_4 creation and add are done inside the parallel block after a
            # short delay to let fio start up.
            with parallel() as p:
                for spec in image_spec[:2]:
                    p.spawn(
                        run_fio,
                        client_node=client_primary,
                        filename=image_file_paths[spec],
                        long_running=True,
                        run_time=fio.get("background_runtime", 600),
                        size=io_fio_args["size"],
                        num_jobs=io_fio_args["num_jobs"],
                        iodepth=io_fio_args["iodepth"],
                        rwmixread=io_fio_args["rwmixread"],
                        direct=io_fio_args["direct"],
                        invalidate=io_fio_args["invalidate"],
                        io_type=io_fio_args["io_type"],
                    )
                # Give fio a moment to ramp up before adding image_4
                time.sleep(10)
                log.info("Started continuous background fio on %s", image_spec[:2])

                image_4_spec = pool_spec + "/image_4"
                _, err = rbd_primary.create(
                    **{"image-spec": image_4_spec, "size": image_size}
                )
                if err:
                    raise Exception(
                        "Failed to create image " + image_4_spec + ", err: " + str(err)
                    )
                add_group_image_and_verify(
                    rbd_primary,
                    **{"group-spec": group_spec, "image-spec": image_4_spec},
                )
                image_spec.append(image_4_spec)
                log.info(
                    "Successfully added %s to mirror-enabled group during background I/O",
                    image_4_spec,
                )

            # parallel block exits here — all fio jobs have completed
            log.info("Stopping background fio on %s", image_spec[:2])

            wait_for_idle(rbd_primary, rbd_secondary=rbd_secondary, **group_config)
            verify_peer_image_global_ids_unchanged(
                rbd_primary,
                baseline_ids,
                after_event="adding image_4",
                **group_config,
            )

            exec_cmd(
                node=client_primary,
                cmd="pkill -9 fio || true",
                sudo=True,
                check_ec=False,
            )
            log.info("Stopped background fio on %s", image_spec[:2])

            stable_names.append("image_4")
            baseline_ids = get_peer_image_global_ids(
                rbd_primary, stable_names, **group_config
            )

            # Steps 23–25: add 2m group snapshot schedule (1m schedule already active)
            start_time = (
                datetime.datetime.utcnow() + datetime.timedelta(minutes=3)
            ).strftime("%Y-%m-%dT%H:%M:%S")
            group_sched_kw = {
                "pool": pool,
                "group": pool_config.get("group"),
                "interval": "2m",
                "start-time": start_time,
            }
            if "namespace" in pool_config:
                group_sched_kw["namespace"] = pool_config.get("namespace")
            _, err = rbd_primary.mirror.group.snapshot.schedule.add_(**group_sched_kw)
            if err:
                raise Exception(
                    "Failed to add group snapshot schedule with start-time: " + str(err)
                )
            log.info("Added group snapshot schedule of 2m with start-time")
            added_schedules.append(
                {
                    "pool": pool,
                    "level": "group",
                    "group": pool_config.get("group"),
                    "interval": "2m",
                    **(
                        {"namespace": pool_config.get("namespace")}
                        if "namespace" in pool_config
                        else {}
                    ),
                }
            )

            if verify_group_snapshot_ls(
                rbd_primary,
                group_spec,
                "2m",
                **schedule_status_spec,
            ):
                raise Exception("Failed to verify group snapshot schedule of 2m")

            for spec in image_spec:
                if spec in image_file_paths:
                    run_fio(
                        client_node=client_primary,
                        filename=image_file_paths[spec],
                        get_time_taken=True,
                        **io_fio_args,
                    )
                else:
                    file_path = "/mnt/mnt_" + random_string(len=5) + "/file"
                    image_file_paths[spec] = file_path
                    new_image_io = deepcopy(io_config)
                    new_image_io["config"]["image_spec"] = [spec]
                    new_image_io["config"]["file_path"] = [file_path]
                    _, err = krbd_io_handler(**new_image_io)
                    if err:
                        raise Exception(
                            "Map, mount and run IOs failed during snapshot scheduling for "
                            + spec
                        )
            wait_for_idle(rbd_primary, rbd_secondary=rbd_secondary, **group_config)

            if verify_group_snapshot_schedule(
                rbd_primary,
                pool,
                pool_config.get("group"),
                "1m",
                namespace=pool_config.get("namespace"),
            ):
                raise Exception(
                    "Failed to verify scheduled group snapshots mirrored to secondary"
                )

            # wait for group mirror sync
            wait_for_idle(rbd_primary, rbd_secondary=rbd_secondary, **group_config)

            # Steps 26–31: add image_5 while snapshot scheduling is active
            image_5_spec = pool_spec + "/image_5"
            _, err = rbd_primary.create(
                **{"image-spec": image_5_spec, "size": image_size}
            )
            if err:
                raise Exception(
                    "Failed to create image " + image_5_spec + ", err: " + str(err)
                )
            add_group_image_and_verify(
                rbd_primary,
                **{"group-spec": group_spec, "image-spec": image_5_spec},
            )
            image_spec.append(image_5_spec)
            log.info(
                "Added image_5 to mirror-enabled group while snapshot scheduling is active"
            )

            snap_list_out, _ = rbd_primary.group.snap.list(
                **group_config, format="json"
            )
            if snap_list_out:
                mirror_snaps = [
                    s
                    for s in json.loads(snap_list_out)
                    if s.get("namespace", {}).get("type") == "mirror"
                ]
                if mirror_snaps:
                    snap_id = mirror_snaps[-1].get("id")
                    snap_state = get_snap_state_by_snap_id(
                        rbd_primary, snap_id, **status_spec
                    )
                    log.info(
                        "Mirror group snapshot id=%s state=%s during image_5 add",
                        snap_id,
                        snap_state,
                    )

            # wait for image to sync
            wait_for_idle(rbd_primary, rbd_secondary=rbd_secondary, **group_config)
            verify_peer_image_global_ids_unchanged(
                rbd_primary,
                baseline_ids,
                after_event="adding image_5",
                **group_config,
            )

            log.info(
                "Waiting 150s for scheduled snapshots to cycle during image_5 sync"
            )
            time.sleep(150)
            mirror_group_snapshot_add_and_wait_sync(
                rbd_primary,
                rbd_secondary,
                **group_config,
            )
            wait_for_idle(rbd_primary, rbd_secondary=rbd_secondary, **group_config)
            log.info(
                "image_5 successfully mirrored to secondary — "
                "mirror snapshot created and copied, replay state idle"
            )

            group_image_list, err = rbd_primary.group.image.list(
                **group_config, format="json"
            )
            if err:
                raise Exception("Getting group image list failed : " + str(err))
            mirror_group_snapshot_add_and_wait_sync(
                rbd_primary,
                rbd_secondary,
                **group_config,
            )
            wait_for_idle(rbd_primary, rbd_secondary=rbd_secondary, **group_config)

            # Validate data integrity for all images
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

            group_mirror_status_verify(
                primary_cluster,
                secondary_cluster,
                rbd_primary,
                rbd_secondary,
                primary_state="up+stopped",
                secondary_state="up+replaying",
                **group_config,
            )
            log.info("Successfully verified group mirror status on both clusters")

            # Happy-path schedule removal — schedules are also removed in the
            # finally block, so this is belt-and-suspenders for the success path.
            for sched in list(reversed(added_schedules)):
                _, err = remove_snapshot_scheduling(rbd_primary, **sched)
                if err:
                    raise Exception(
                        "Failed to remove group snapshot schedule "
                        + sched.get("interval", "")
                    )
                log.info("Removed group snapshot schedule of %s", sched["interval"])
            added_schedules.clear()

            # Disable group mirroring so images can be removed from the group
            disable_group_mirroring_and_verify_state(rbd_primary, **group_config)
            log.info("Group mirroring disabled successfully before image cleanup")

            # Remove images from the group
            for spec in image_spec:
                try:
                    remove_group_image_and_verify(
                        rbd_primary,
                        **{"group-spec": group_spec, "image-spec": spec},
                    )
                    log.info("Removed %s from group %s", spec, group_spec)
                except Exception as exc:
                    if "cannot remove image from mirror enabled group" in str(exc):
                        log.info(
                            "Remove %s skipped (mirror-enabled group): %s",
                            spec,
                            exc,
                        )
                    else:
                        raise Exception(
                            "Failed to remove image from group: " + str(exc)
                        )
            # Remove group from the pool
            _, err = rbd_primary.group.remove(**group_config)
            if err:
                raise Exception(
                    "Failed to remove group " + group_spec + ": " + str(err)
                )
            out, err = rbd_primary.group.list(**{"pool-spec": pool_spec})
            if out and pool_config.get("group") in out:
                raise Exception(
                    "Group still listed on primary after removal: " + str(out)
                )
            log.info(
                "Group %s removed successfully from pool %s", group_spec, pool_spec
            )

        finally:
            # Always remove snapshot schedules before pool deletion — leaving them
            # behind triggers a Ceph bug where the scheduler stops generating snapshots.
            for sched in reversed(added_schedules):
                try:
                    _, rm_err = remove_snapshot_scheduling(rbd_primary, **sched)
                    if rm_err:
                        log.warning(
                            "Could not remove snapshot schedule %s in finally: %s",
                            sched,
                            rm_err,
                        )
                    else:
                        log.info(
                            "Finally: removed snapshot schedule %s",
                            sched.get("interval"),
                        )
                except Exception as rm_exc:
                    log.warning(
                        "Exception removing snapshot schedule %s in finally: %s",
                        sched,
                        rm_exc,
                    )


def run(**kw):
    """
    Dispatch dynamic group mirroring tests by operation id from the suite YAML.
    Args:
        kw: test data
    Returns:
        int: 0 on success, 1 otherwise
    """
    operation_mapping = {
        "CEPH-83632349": test_group_mirror_dynamic,
    }

    pool_types = ["rep_pool_config", "ec_pool_config"]
    mirror_obj = None

    try:
        operation = kw.get("config").get("operation")
        if operation not in operation_mapping:
            raise Exception(
                f"Unknown operation {operation!r}; supported: "
                f"{list(operation_mapping.keys())}"
            )

        log.info("Running Consistency Group dynamic mirroring across two clusters")
        # Run on either replicated or EC pool
        pool_type = random.choice(pool_types)
        log.info("Running test on %s", pool_type)

        config = kw.get("config")
        if pool_type == "rep_pool_config":
            config["rep-pool-only"] = True
        else:
            config["ec-pool-only"] = True

        grouptypes = ["single_pool_without_namespace", "single_pool_with_namespace"]
        if not config.get("grouptype"):
            group_type = random.choice(grouptypes)
            config.get(pool_type).update({"grouptype": group_type})
            log.info("Choosing Group type on %s - %s", pool_type, group_type)

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
            pool_type,
            **kw,
        )
        log.info("Test passed for %s", pool_type)

    except Exception as e:
        log.error(
            "Test: Dynamic RBD group mirroring (snapshot mode) across two clusters failed: "
            + str(e)
        )
        return 1

    finally:
        if mirror_obj:
            cleanup(pool_types=pool_types, multi_cluster_obj=mirror_obj, **kw)

    return 0
