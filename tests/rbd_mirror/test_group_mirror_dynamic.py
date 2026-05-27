"""
Module to verify dynamic addition of images to a mirror-enabled RBD consistency group.

Test cases covered:
    CEPH-83632349 - Dynamic group mirroring — add images to an already mirror-enabled
                group without disabling mirroring, during I/O and snapshot scheduling


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
from ceph.rbd.mirror_utils import (
    check_mirror_consistency,
    compare_image_size_primary_secondary,
)
from ceph.rbd.utils import exec_cmd, getdict, random_string
from ceph.rbd.workflows.cleanup import cleanup, device_cleanup
from ceph.rbd.workflows.group_mirror import (
    add_group_image_and_verify,
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
        rbd_secondary: RBD objevct of secondary cluster
        client_primary: client node object of primary cluster
        client_secondary: client node object of secondary cluster
        primary_cluster: Primary cluster object
        secondary_cluster: Secondary cluster object
        pool_type: Replication pool or EC pool
        **kw: any other arguments
    """
    log.info("Running test CEPH-83632349 for %s", pool_type)
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

    rbd_config = kw.get("config", {}).get(pool_type, {})
    multi_pool_config = deepcopy(getdict(rbd_config))

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
            enable_namespace_mirroring(rbd_primary, rbd_secondary, pool, **pool_config)
            pool_spec = pool + "/" + pool_config.get("namespace")
        else:
            pool_spec = pool

        image_size = rbd_config.get("size", "4G")
        io_config["rbd_obj"] = rbd_primary
        io_config["client"] = client_primary
        for spec in image_spec:
            single_io = deepcopy(io_config)
            single_io["config"]["image_spec"] = [spec]
            single_io["config"]["file_path"] = [
                "/mnt/mnt_" + random_string(len=5) + "/file"
            ]
            _, err = krbd_io_handler(**single_io)
            if err:
                raise Exception("Map, mount and run IOs failed for " + spec)
        log.info("Map, mount and IOs successful for " + str(image_spec))

        group_mirror_status, err = rbd_primary.mirror.group.status(**group_config)
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

        wait_for_idle(rbd_primary, **group_config)
        log.info("Successfully completed sync for group mirroring to secondary site")

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

        group_info_status, err = rbd_secondary.group.info(**group_config, format="json")
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

        out, err = add_snapshot_scheduling(rbd_primary, **snap_schedule_config)
        if err:
            raise Exception("Failed to add group snapshot schedule of 1m")

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
        image_create_status, err = rbd_primary.create(
            **{"image-spec": image_3_spec, "size": image_size}
        )
        if err:
            raise Exception(
                "Failed to create image " + image_3_spec + ", err: " + str(err)
            )
        log.info(
            "Successfully created image for dynamic group add: "
            + str(image_create_status)
        )
        add_group_image_and_verify(
            rbd_primary,
            **{"group-spec": group_spec, "image-spec": image_3_spec},
        )
        image_spec.append(image_3_spec)

        wait_for_idle(rbd_primary, **group_config)
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
        check_mirror_consistency(
            rbd_primary,
            rbd_secondary,
            client_primary,
            client_secondary,
            group_image_list,
        )

        for spec in image_spec:
            single_io = deepcopy(io_config)
            single_io["config"]["image_spec"] = [spec]
            single_io["config"]["file_path"] = [
                "/mnt/mnt_" + random_string(len=5) + "/file"
            ]
            _, err = krbd_io_handler(**single_io)
            if err:
                raise Exception("Map, mount and run IOs failed for " + spec)
        mirror_group_snapshot_add_and_wait_sync(
            rbd_primary,
            rbd_secondary,
            **group_config,
        )
        group_image_list, err = rbd_primary.group.image.list(
            **group_config, format="json"
        )
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
            rbd_primary, stable_names[:-1], **group_config
        )

        # Steps 17–22: continuous I/O on image_1/image_2 while adding image_4
        bg_io_config = deepcopy(io_config)
        bg_io_config["config"]["image_spec"] = image_spec[:2]
        bg_io_config["config"]["operations"]["io"] = False
        bg_io_config["config"]["operations"]["nounmap"] = True
        bg_io_config["config"]["file_path"] = [
            "/mnt/mnt_" + random_string(len=5) + "/file",
            "/mnt/mnt_" + random_string(len=5) + "/file",
        ]
        _, err = krbd_io_handler(**bg_io_config)
        if err:
            raise Exception(
                "Map, mount failed for background I/O on " + str(image_spec[:2])
            )

        mount_paths = bg_io_config["config"]["file_path"]
        device_names = bg_io_config["config"].get("device_names", [])
        with parallel() as p:
            for path in mount_paths:
                p.spawn(
                    run_fio,
                    client_node=client_primary,
                    filename=path,
                    run_time=fio.get("background_runtime", 600),
                    size=fio["size"],
                    io_type=fio["ODF_CONFIG"]["io_type"],
                    num_jobs=fio["ODF_CONFIG"]["num_jobs"],
                    iodepth=fio["ODF_CONFIG"]["iodepth"],
                    rwmixread=fio["ODF_CONFIG"]["rwmixread"],
                    direct=fio["ODF_CONFIG"]["direct"],
                    invalidate=fio["ODF_CONFIG"]["invalidate"],
                    long_running=True,
                )
        log.info("Started continuous background fio on %s", image_spec[:2])

        image_4_spec = pool_spec + "/image_4"
        image_create_status, err = rbd_primary.create(
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

        wait_for_idle(rbd_primary, **group_config)
        verify_peer_image_global_ids_unchanged(
            rbd_primary,
            baseline_ids,
            after_event="adding image_4",
            **group_config,
        )

        exec_cmd(node=client_primary, cmd="pkill -9 fio || true", check_ec=False)
        exec_cmd(
            node=client_primary,
            cmd="pkill -9 fio || true",
            sudo=True,
            check_ec=False,
        )
        for mount_point, device_name in zip(mount_paths, device_names):
            dev = (
                device_name[0].strip()
                if isinstance(device_name, tuple)
                else device_name.strip()
            )
            mp = mount_point.rsplit("/", 1)[0]
            device_cleanup(
                rbd=rbd_primary,
                client=client_primary,
                file_name=mp,
                device_name=dev,
            )
        log.info("Stopped background fio and cleaned up mapped devices")

        stable_names.append("image_4")
        baseline_ids = get_peer_image_global_ids(
            rbd_primary, stable_names[:-1], **group_config
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
        out, err = rbd_primary.mirror.group.snapshot.schedule.add_(**group_sched_kw)
        if err:
            raise Exception(
                "Failed to add group snapshot schedule with start-time: " + str(err)
            )
        log.info("Added group snapshot schedule of 2m with start-time")

        if verify_group_snapshot_ls(
            rbd_primary,
            group_spec,
            "2m",
            **schedule_status_spec,
        ):
            raise Exception("Failed to verify group snapshot schedule of 2m")

        for spec in image_spec:
            single_io = deepcopy(io_config)
            single_io["config"]["image_spec"] = [spec]
            single_io["config"]["file_path"] = [
                "/mnt/mnt_" + random_string(len=5) + "/file"
            ]
            _, err = krbd_io_handler(**single_io)
            if err:
                raise Exception(
                    "Map, mount and run IOs failed during snapshot scheduling for "
                    + spec
                )

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

        out_p, err = rbd_primary.mirror.group.status(**status_spec)
        out_s, err_s = rbd_secondary.mirror.group.status(**status_spec)
        if err or err_s:
            raise Exception("Getting group mirror status for snapshot compare failed")
        snaps_p = {s["name"] for s in json.loads(out_p).get("snapshots", [])}
        snaps_s = {s["name"] for s in json.loads(out_s).get("snapshots", [])}
        if snaps_p - snaps_s:
            raise Exception(
                "Group snapshot lists do not match on primary and secondary"
            )
        log.info("Verified scheduled group snapshots on both clusters")

        snaps_before = len(json.loads(out_p).get("snapshots", []))

        # Steps 26–31: add image_5 while snapshot scheduling is active
        image_5_spec = pool_spec + "/image_5"
        image_create_status, err = rbd_primary.create(
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

        snap_list_out, _ = rbd_primary.group.snap.list(**group_config, format="json")
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

        wait_for_idle(rbd_primary, **group_config)
        verify_peer_image_global_ids_unchanged(
            rbd_primary,
            baseline_ids,
            after_event="adding image_5",
            **group_config,
        )

        time.sleep(150)
        mirror_group_snapshot_add_and_wait_sync(
            rbd_primary,
            rbd_secondary,
            **group_config,
        )

        out_after, _ = rbd_primary.mirror.group.status(**status_spec)
        snaps_after = len(json.loads(out_after).get("snapshots", []))
        if snaps_after > snaps_before:
            log.info(
                "image_5 participates in scheduled snapshots (%s -> %s)",
                snaps_before,
                snaps_after,
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

        for interval in ("2m", "1m"):
            snap_schedule_rm_config = deepcopy(snap_schedule_config)
            snap_schedule_rm_config["interval"] = interval
            out, err = remove_snapshot_scheduling(
                rbd_primary, **snap_schedule_rm_config
            )
            if err:
                raise Exception(
                    "Failed to remove group snapshot schedule of " + interval
                )
            log.info("Removed group snapshot schedule of %s", interval)

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
                    raise Exception("Failed to remove image from group: " + str(exc))


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
        # Run on either replicated or EC pool (create only the chosen pool type)
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
