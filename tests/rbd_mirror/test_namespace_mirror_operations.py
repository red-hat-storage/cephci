"""
Module to verify mirror image operation like demote, promote, resync, rename, resize,
remove for namespace mirrored images

Test case covered -
CEPH-83601539 - Perform mirror image operation like demote, promote, resync, rename,
resize, remove for namespace mirrored images

Pre-requisites :
1. Two ceph clusters version 8.0 or later with mon,mgr,osd
2. Deploy rbd-mirror daemon service on both clusters

Test Case Flow:
Test case covered -

CEPH-83601539:
1. Create two Ceph clusters (version 8.0 or later) with mon, mgr, and osd services.
2. Deploy rbd-mirror daemon service on both clusters.
3. Create a pool named pool1 on both clusters and initialize it
4. Enable mirroring for pool1 on both clusters
5. Create namespaces ns1_p on cluster1 and ns1_s on cluster2
6. Enable namespace-level mirroring between clusters
7. Create a mirrored image in ns1_p (primary) and enable snapshot-based mirroring:
8. rbd mirror image enable pool1/ns1_p/image1 snapshot
9. Demote the image in ns1_s (secondary)
10. Promote the image in ns1_s (secondary)
11. Resync the image in ns1_p to restart mirroring
12. Rename the mirrored image in ns1_p (primary)
13. Resize the mirrored image in ns1_p (primary)
14. Remove the mirrored image in ns1_p (primary)
15. Verify that the image in ns1_s (secondary) is also removed as part of namespace mirroring
"""

import json
import random
import time

from ceph.rbd.initial_config import initial_mirror_config
from ceph.rbd.utils import getdict, random_string
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.krbd_io_handler import krbd_io_handler
from ceph.rbd.workflows.rbd_mirror import enable_image_mirroring, wait_for_status
from ceph.rbd.workflows.snap_scheduling import (
    add_snapshot_scheduling,
    verify_snapshot_schedule,
)
from utility.log import Log

log = Log(__name__)


def test_namespace_mirror_operations(pri_config, sec_config, pool_types, **kw):
    log.info(
        "Starting CEPH-83601539 - Perform mirror image operation like demote, "
        + "promote, resync, rename, resize, remove for namespace mirrored images"
    )
    rbd_primary = pri_config.get("rbd")
    rbd_secondary = sec_config.get("rbd")
    client_primary = pri_config.get("client")

    for pool_type in pool_types:
        rbd_config = kw.get("config", {}).get(pool_type, {})
        multi_pool_config = getdict(rbd_config)

        for pool, pool_config in multi_pool_config.items():
            multi_image_config = getdict(pool_config)
            image_config = {k: v for k, v in multi_image_config.items()}
            namespace = pool_config.get("namespace")
            remote_namespace = pool_config.get("remote_namespace")
            log.info(namespace)
            log.info(remote_namespace)
            for image, image_config_val in image_config.items():
                pri_image_spec = f"{pool}/{namespace}/{image}"
                sec_image_spec = f"{pool}/{remote_namespace}/{image}"
                # Write data on the primary image
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
                        "file_path": ["/mnt/mnt_" + random_string(len=5) + "/file"],
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
                io_config["rbd_obj"] = rbd_primary
                io_config["client"] = client_primary
                image_spec = []
                image_spec.append(pri_image_spec)
                io_config["config"]["image_spec"] = image_spec
                (io, err) = krbd_io_handler(**io_config)
                if err:
                    raise Exception(
                        f"Map, mount and run IOs failed for {io_config['config']['image_spec']}"
                    )
                else:
                    log.info(
                        f"Map, mount and IOs successful for {io_config['config']['image_spec']}"
                    )

                image_enable_config = {
                    "pool": pool,
                    "image": image,
                    "mirrormode": "snapshot",
                    "namespace": namespace,
                    "remote_namespace": remote_namespace,
                }
                # Enable snapshot mode mirroring on images of the namespace
                enable_image_mirroring(pri_config, sec_config, **image_enable_config)
                # Verify image mirroring status on primary cluster
                wait_for_status(
                    rbd=rbd_primary,
                    cluster_name=pri_config.get("cluster").name,
                    imagespec=pri_image_spec,
                    state_pattern="up+stopped",
                )
                # Verify image mirroring status on secondary cluster
                wait_for_status(
                    rbd=rbd_secondary,
                    cluster_name=sec_config.get("cluster").name,
                    imagespec=sec_image_spec,
                    state_pattern="up+replaying",
                )

                snap_levels = image_config_val.get("snap_schedule_levels")
                snap_intervals = image_config_val.get("snap_schedule_intervals")

                if snap_levels and snap_intervals:
                    level = snap_levels[0]
                    interval = snap_intervals[0]
                    log.info(level)

                    # Handle case when level is 'namespace' but no namespace is defined (i.e., default ns)
                    effective_level = (
                        "image" if level == "namespace" and not namespace else level
                    )

                    out, err = add_snapshot_scheduling(
                        rbd_primary,
                        pool=pool,
                        image=image,
                        level=effective_level,
                        interval=interval,
                        namespace=namespace,
                    )
                    if err:
                        raise Exception(
                            "Adding snapshot schedule failed with error %s" % err
                        )

                    # Retain original level for remaining tests
                    level = effective_level

                    # Verify snapshot schedules are effective on the namespaces
                    verify_snapshot_schedule(
                        rbd_primary,
                        pool,
                        image=image,
                        interval=interval,
                        namespace=namespace,
                    )

                # Demote Primary Image on cluster-1
                log.info(f"Demoting primary image {pri_image_spec} on Cluster-1")
                out, err = rbd_primary.mirror.image.demote(
                    **{"image-spec": pri_image_spec}
                )
                if err:
                    raise Exception(
                        f"Demote image {pri_image_spec} failed with error {err}"
                    )
                log.info(f"Demoted image {pri_image_spec} successfully")

                time.sleep(
                    int(image_config_val["snap_schedule_intervals"][-1][:-1]) * 120
                )

                # Promoting Secondary Image on cluster-2
                log.info(f"Promoting secondary image {sec_image_spec} on Cluster-2")
                out, err = rbd_secondary.mirror.image.promote(
                    **{"image-spec": sec_image_spec}
                )
                if err:
                    raise Exception(
                        f"Promote image {sec_image_spec} failed with error {err}"
                    )
                log.info(f"Promoted image {sec_image_spec} successfully")

                # Resync on cluster-1
                log.info(f"Resyncing image {pri_image_spec} on Cluster-1")
                out, err = rbd_primary.mirror.image.resync(
                    **{"image-spec": pri_image_spec}
                )
                if err:
                    raise Exception(
                        f"Resync image {pri_image_spec} failed with error {err}"
                    )
                log.info(f"Resync initiated for image {pri_image_spec}")

                time.sleep(120)

                wait_for_status(
                    rbd=rbd_primary,
                    cluster_name=pri_config.get("cluster").name,
                    imagespec=pri_image_spec,
                    state_pattern="up+replaying",
                )
                # Verify image mirroring status on secondary cluster
                wait_for_status(
                    rbd=rbd_secondary,
                    cluster_name=sec_config.get("cluster").name,
                    imagespec=sec_image_spec,
                    state_pattern="up+stopped",
                )

                # Demote at cluster-2 and promote at cluster-1 to re-gain initial direction
                log.info(f"Demoting Secondary image {sec_image_spec} on Cluster-2")
                out, err = rbd_secondary.mirror.image.demote(
                    **{"image-spec": sec_image_spec}
                )
                if err:
                    raise Exception(
                        f"Demote image {sec_image_spec} failed with error {err}"
                    )
                log.info(f"Demoted image {sec_image_spec} successfully")

                time.sleep(
                    int(image_config_val["snap_schedule_intervals"][-1][:-1]) * 120
                )

                log.info(f"Promoting primary image {pri_image_spec} on Cluster-1")
                out, err = rbd_primary.mirror.image.promote(
                    **{"image-spec": pri_image_spec}
                )
                if err:
                    raise Exception(
                        f"Promote image {pri_image_spec} failed with error {err}"
                    )
                log.info(f"Promoted image {pri_image_spec} successfully")

                wait_for_status(
                    rbd=rbd_primary,
                    cluster_name=pri_config.get("cluster").name,
                    imagespec=pri_image_spec,
                    state_pattern="up+stopped",
                )

                wait_for_status(
                    rbd=rbd_secondary,
                    cluster_name=sec_config.get("cluster").name,
                    imagespec=sec_image_spec,
                    state_pattern="up+replaying",
                )

                # Rename the image on Primary
                new_image_spec_pri = f"{pool}/{namespace}/{image}_renamed"
                log.info(f"Renaming image {image} to {new_image_spec_pri} on Cluster-1")
                out, err = rbd_primary.rename(
                    **{
                        "source-image-spec": pri_image_spec,
                        "dest-image-spec": new_image_spec_pri,
                    }
                )
                if err:
                    raise Exception(
                        f"Rename image {pri_image_spec} failed with error {err}"
                    )
                log.info(f"Renamed image {pri_image_spec} to {new_image_spec_pri}")

                time.sleep(
                    int(image_config_val["snap_schedule_intervals"][-1][:-1]) * 120
                )

                new_image_spec_sec = f"{pool}/{remote_namespace}/{image}_renamed"
                out, err = rbd_secondary.info(
                    **{"image-or-snap-spec": new_image_spec_sec}
                )
                if err:
                    raise Exception(
                        f"Renamed image {new_image_spec_sec} not found on Cluster-2: {err}"
                    )
                log.info(f"Renamed image {new_image_spec_sec} found on Cluster-2")

                # Resize Image
                resize_size = 2 * 1024  # 2GB
                log.info(
                    f"Resizing image {new_image_spec_pri} to {resize_size} on Cluster-1"
                )
                out, err = rbd_primary.resize(
                    **{
                        "image-spec": new_image_spec_pri,
                        "size": resize_size,
                        "allow-shrink": True,
                    }
                )
                if out or err and "100% complete" not in out + err:
                    raise Exception(
                        f"Resize image {new_image_spec_pri} failed with error {err}"
                    )
                log.info(f"Resized image {new_image_spec_pri} to {resize_size}")

                time.sleep(
                    int(image_config_val["snap_schedule_intervals"][-1][:-1]) * 120
                )

                out, err = rbd_secondary.info(
                    **{"image-or-snap-spec": new_image_spec_sec, "format": "json"}
                )
                if err:
                    raise Exception(
                        f"Failed to get info for image {new_image_spec_sec} on Cluster-2: {err}"
                    )

                try:
                    image_info = json.loads(out)
                except Exception as e:
                    raise Exception(
                        f"Failed to parse image info JSON: {e}\nOutput: {out}"
                    )

                actual_size = int(image_info.get("size", 0)) // (1024 * 1024)
                if actual_size != resize_size:
                    raise Exception(
                        f"Image size mismatch after resize: expected {resize_size}, got {actual_size} on Cluster-2"
                    )
                log.info(f"Resize verified for image {new_image_spec_sec} on Cluster-2")

                # Remove image on Cluster-1
                log.info(f"Removing image {new_image_spec_pri} on Cluster-1")
                out, err = rbd_primary.rm(**{"image-spec": new_image_spec_pri})
                if out or err and "100% complete" not in out + err:
                    raise Exception(
                        f"Remove image {new_image_spec_pri} failed with error {err}"
                    )
                log.info(f"Removed image {new_image_spec_pri} on Cluster-1")

                time.sleep(
                    int(image_config_val["snap_schedule_intervals"][-1][:-1]) * 120
                )

                out, err = rbd_secondary.info(
                    **{"image-or-snap-spec": new_image_spec_sec}
                )
                if not err:
                    raise Exception(
                        f"Image {new_image_spec_sec} still exists on Cluster-2 after removal"
                    )
                log.info(
                    f"Verified image {new_image_spec_sec} is removed from Cluster-2"
                )
    return 0


def run(**kw):
    """
    Test to perform mirror image operation like demote, promote, resync,
    rename, resize, remove for namespace mirrored images
    Args:
        kw: Key/value pairs of configuration information to be used in the test
            Example::
          config:
            rep_pool_config:
              num_pools: 1
              num_images: 1
              do_not_create_image: True
              size: 1G
              mode: image
              mirror_level: namespace
              namespace_mirror_type: non-default_to_non-default
              mirrormode: snapshot
              snap_schedule_levels:
                - namespace
    """
    try:
        snap_schedule_level_options = ["pool", "namespace", "image"]
        used_schedule_levels = set()
        for pool_type in ["rep_pool_config", "ec_pool_config"]:
            if pool_type in kw.get("config", {}):
                pool_config = kw["config"][pool_type]

                if "snap_schedule_levels" not in pool_config:
                    available_levels = [
                        lvl
                        for lvl in snap_schedule_level_options
                        if lvl not in used_schedule_levels
                    ]
                    if not available_levels:
                        available_levels = snap_schedule_level_options.copy()
                        used_schedule_levels.clear()

                    selected_snap_level = random.choice(available_levels)
                    pool_config["snap_schedule_levels"] = [selected_snap_level]
                    used_schedule_levels.add(selected_snap_level)
                else:
                    selected_snap_level = pool_config["snap_schedule_levels"]
                log.info(
                    "Selected snap schedule level: %s for %s",
                    selected_snap_level,
                    pool_type,
                )

        mirror_obj = initial_mirror_config(**kw)
        mirror_obj.pop("output", [])
        for val in mirror_obj.values():
            if not val.get("is_secondary", False):
                pri_config = val
            else:
                sec_config = val
        log.info("Initial configuration complete")
        pool_types = list(mirror_obj.values())[0].get("pool_types")
        test_namespace_mirror_operations(pri_config, sec_config, pool_types, **kw)

    except Exception as e:
        log.error(f"Test namespace mirror all operations failed with error {str(e)}")
        return 1
    finally:
        cleanup(pool_types=pool_types, multi_cluster_obj=mirror_obj, **kw)
    return 0
