"""
Module to verify Snapshot based Namespace level mirroring from Non default to default namespace.

Test case covered -
CEPH-83612860 -  Snapshot based Namespace level mirroring from Non default to default namespace
CEPH-83612872 - Snapshot based namespace level mirroring from default to Non default namespace

Pre-requisites :
1. Two ceph clusters version 8.1 or later with mon,mgr,osd
2. Deploy rbd-mirror daemon service on both clusters

Test Case Flow:
Test case covered -

CEPH-83612860:
1. Create a pool  on both clusters:
2. Create namespaces ns1_p in pool1 on cluster1:
3. Enable non-default namespace mirroring on with "init-only" mirror mode on cluster-1
   # rbd mirror pool enable --pool pool1 init-only
   # rbd mirror pool enable pool1/ns1_p image --remote-namespace ' '
   Enable default namespace image mode mirroring on cluster-2
   # rbd mirror pool enable --pool pool1 image --remote-namespace ns1_p
4. Set up peering between the two clusters in two-way mode
5. Verify mirroring is configured successfully using below command on both clusters
6. Create an image in the namespace ns1_p on cluster1 and enable snapshot-based mirroring:
7. Verify image mirroring status for the namespace image
8. Add a snapshot schedule for the mirrored image in the namespace level:
9. Initiate I/O operations on the image using rbd bench or fio or file mount:
10. Wait till snapshot schedule interval set like above 1m
11. Verify that data is mirrored from the primary to the secondary cluster
12. Verify data consistency using md5sum checksum from primary and secondary
13. Similarly, create image in the default namespace in cluster2 and verify that the image
    got mirrored to cluster1 remote namespace
14. Add a snapshot schedule for the mirrored image in the namespace
15. Initiate I/O operations on the image using rbd bench, fio, or file mount
16. Wait till snapshot schedule interval set like above 1m.
17. Verify that data is mirrored from cluster-2 to cluster-1
18. Verify data consistency using md5sum checksum from primary and secondary
19.  Repeat the above test on EC pool
20. Cleanup the images, namespace, pools along with disk cleanup.

CEPH-83612872:
1. Create a pool  on both clusters:
2. Create namespaces ns1_p in pool1 on cluster2:
3. Enable non-default namespace mirroring on with "init-only" mirror mode on cluster-2
   # rbd mirror pool enable --pool pool1 init-only
   # rbd mirror pool enable pool1/ns1_p image --remote-namespace ' '
   Enable default namespace image mode mirroring on cluster-1
   # rbd mirror pool enable --pool pool1 image --remote-namespace ns1_p
4. Set up peering between the two clusters in two-way mode
5. Verify mirroring is configured successfully using below command on both clusters
6. Create an image in the empty(default) namespace on cluster1 and enable snapshot-based mirroring:
7. Verify image mirroring status for the default namespace image
8. Add a snapshot schedule for the mirrored image :
9. Initiate I/O operations on the image using rbd bench or fio or file mount:
10. Wait till snapshot schedule interval set like above 1m
11. Verify that data is mirrored from the primary to the secondary cluster
12. Verify data consistency using md5sum checksum from primary and secondary
13. Similarly, create image in the non-default namespace in cluster2 and verify that the image
    got mirrored to cluster1 default namespace
14. Add a snapshot schedule for the mirrored image in the namespace
15. Initiate I/O operations on the image using rbd bench, fio, or file mount
16. Wait till snapshot schedule interval set like above 1m.
17. Verify that data is mirrored from cluster-2 to cluster-1
18. Verify data consistency using md5sum checksum from primary and secondary
19.  Repeat the above test on EC pool
20. Cleanup the images, namespace, pools along with disk cleanup.

CEPH-83613949:
1. Create a pool  on both clusters:
2. Create namespaces ns1_p in pool1 on cluster2:
3. Enable non-default namespace mirroring on with "init-only" mirror mode on cluster-2
   # rbd mirror pool enable --pool pool1 init-only
   # rbd mirror pool enable pool1/ns1_p image --remote-namespace ' '
   Enable default namespace image mode mirroring on cluster-1
   # rbd mirror pool enable --pool pool1 image --remote-namespace ns1_p
4. Set up peering between the two clusters in two-way mode
5. Verify mirroring is configured successfully using below command on both clusters
6. Create an image in the empty(default) namespace on cluster1 and enable snapshot-based mirroring:
7. Verify image mirroring status for the default namespace image
8. Add a snapshot schedule for the mirrored image in pool level for replicated pool
9. Initiate I/O operations on the image using rbd bench or fio or file mount
10. Wait till snapshot schedule interval set like above 1m
11. Verify that data is mirrored from the primary to the secondary cluster
12. Verify data consistency using md5sum checksum from primary and secondary
13. Repeat the above test steps on Erasure Coded (EC) pool with non-default to
    default configuration with snapshot schedule in namespace level
14. Cleanup the images, namespace, pools along with disk cleanup.

CEPH-83613951:
1. Create a pool  on both clusters:
2. Create namespaces ns1_p in pool1 on cluster2:
3. Enable non-default namespace mirroring on with "init-only" mirror mode on cluster-2
   # rbd mirror pool enable --pool pool1 init-only
   # rbd mirror pool enable pool1/ns1_p image --remote-namespace ' '
   Enable default namespace image mode mirroring on cluster-1
   # rbd mirror pool enable --pool pool1 image --remote-namespace ns1_p
4. Set up peering between the two clusters in two-way mode
5. Verify mirroring is configured successfully using below command on both clusters
6. Create an image in the empty(default) namespace on cluster1 and enable snapshot-based mirroring:
7. Verify image mirroring status for the default namespace image
8. Add a snapshot schedule for the mirrored image in pool level for replicated pool
9. Initiate I/O operations on the image using rbd bench or fio or file mount
10. Wait till snapshot schedule interval set like above 1m
11. Demote the primary image on Cluster-1
12. Promote the secondary image on Cluster-2
13. Resync the image on Cluster-1
14. Rename the image on Cluster-2 and Verify the renamed image on cluster-1
15. Resize the image on Cluster-2 and Verify image resize is reflected on Cluster-1
16. Remove the image from Cluster-2 and Verify image removal on Cluster-1
17. Repeat the above test steps on Erasure Coded (EC) pool with non-default to
    default configuration with snapshot schedule in namespace level
18. Cleanup the images, namespace, pools along with disk cleanup.

CEPH-83613952:
1. Create a pool  on both clusters
2. Create namespaces ns1_p in pool1 on cluster2
3. Enable non-default namespace mirroring on with "init-only" mirror mode on cluster-2
   # rbd mirror pool enable --pool pool1 init-only
   # rbd mirror pool enable pool1/ns1_p image --remote-namespace ' '
   Enable default namespace image mode mirroring on cluster-1
   # rbd mirror pool enable --pool pool1 image --remote-namespace ns1_p
4. Set up peering between the two clusters in two-way mode
5. Verify mirroring is configured successfully using below command on both clusters
6. Create an image in the empty(default) namespace on cluster1 and enable snapshot-based mirroring
7. Verify image mirroring status for the default namespace image
8. Add a snapshot schedule for the mirrored image
9. Initiate I/O operations on the image using rbd bench or fio or file mount
10. Wait till snapshot schedule interval set like above 1m
11. Verify that data is mirrored from the primary to the secondary cluster
12. Verify data consistency using md5sum checksum from primary and secondary
13. Disable mirroring on the image in cluster-1
14. remove pool peer on both clusters and verify the status
15. Disable mirroring on the pool on both clusters
16. Re-enable the mirroring on the pool on both clusters and verify the status
17. Initiate I/O operations on the image using rbd bench or fio or file mount
18. Verify that data is mirrored from the primary to the secondary cluster
19. Repeat the above test on EC pool
20. Cleanup the images, namespace, pools along with disk cleanup.
"""

import ast
import json
import time

from ceph.parallel import parallel
from ceph.rbd.initial_config import initial_mirror_config
from ceph.rbd.utils import check_data_integrity, getdict, random_string
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.krbd_io_handler import krbd_io_handler
from ceph.rbd.workflows.rbd_mirror import enable_image_mirroring, wait_for_status
from ceph.rbd.workflows.snap_scheduling import (
    add_snapshot_scheduling,
    remove_snapshot_scheduling,
    verify_namespace_snapshot_schedule,
    verify_snapshot_schedule,
)
from utility.log import Log

log = Log(__name__)


def test_non_default_to_default_namespace_mirroring(
    pri_config, sec_config, pool_types, **kw
):
    log.info(
        "Starting CEPH-83612860 - Snapshot based Namespace "
        + "level mirroring from Non default to default namespace"
    )
    rbd_primary = pri_config.get("rbd")
    rbd_secondary = sec_config.get("rbd")
    client_primary = pri_config.get("client")
    client_secondary = sec_config.get("client")

    for pool_type in pool_types:
        rbd_config = kw.get("config", {}).get(pool_type, {})
        multi_pool_config = getdict(rbd_config)

        for pool, pool_config in multi_pool_config.items():
            multi_image_config = getdict(pool_config)
            image_config = {k: v for k, v in multi_image_config.items()}
            namespace = pool_config.get("namespace")
            remote_namespace = pool_config.get("remote_namespace")

            for image, image_config_val in image_config.items():
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
                    imagespec=f"{pool}/{namespace}/{image}",
                    state_pattern="up+stopped",
                )
                # Verify image mirroring status on secondary cluster
                wait_for_status(
                    rbd=rbd_secondary,
                    cluster_name=sec_config.get("cluster").name,
                    imagespec=f"{pool}/{image}",
                    state_pattern="up+replaying",
                )
                if image_config_val.get(
                    "snap_schedule_levels"
                ) and image_config_val.get("snap_schedule_intervals"):
                    for level, interval in zip(
                        image_config_val["snap_schedule_levels"],
                        image_config_val["snap_schedule_intervals"],
                    ):
                        snap_schedule_config = {
                            "pool": pool,
                            "image": image,
                            "level": level,
                            "interval": interval,
                            "namespace": namespace,
                        }
                        # Adding snapshot schedules to the images in namespace
                        out, err = add_snapshot_scheduling(
                            rbd_primary, **snap_schedule_config
                        )
                        if err:
                            raise Exception(
                                "Adding snapshot schedule failed with error " + err
                            )
                    # Verify snapshot schedules are effective on the namespaces
                    verify_namespace_snapshot_schedule(
                        rbd_primary, pool, namespace, interval=interval, image=image
                    )

                pri_image_spec = f"{pool}/{namespace}/{image}"
                sec_image_spec = f"{pool}/{image}"
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
                time.sleep(int(interval[:-1]) * 120)
                # Verify data is mirrored from the primary to the secondary cluster using du command
                prim_usage = rbd_primary.image_usage(
                    **{"image-spec": pri_image_spec, "format": "json"}
                )
                sec_usage = rbd_secondary.image_usage(
                    **{"image-spec": sec_image_spec, "format": "json"}
                )
                if (
                    ast.literal_eval(prim_usage[0])["images"][0]["used_size"]
                    != ast.literal_eval(sec_usage[0])["images"][0]["used_size"]
                ):
                    raise Exception(
                        "Mirrored image usage sizes are not same on primary and secondary"
                    )
                # Verify the data on mirrored images is consistent
                data_integrity_spec = {
                    "first": {
                        "image_spec": pri_image_spec,
                        "rbd": rbd_primary,
                        "client": client_primary,
                        "file_path": f"/tmp/{random_string(len=3)}",
                    },
                    "second": {
                        "image_spec": sec_image_spec,
                        "rbd": rbd_secondary,
                        "client": client_secondary,
                        "file_path": f"/tmp/{random_string(len=3)}",
                    },
                }
                if check_data_integrity(**data_integrity_spec):
                    raise Exception("Data integrity check failed for " + pri_image_spec)
                log.info(
                    "Data is consistent with the mirrored image for " + pri_image_spec
                )
            # create image in the default namespace on secondary
            log.info(
                "Creating image in the default namespace of secondary and "
                + "mirroring to non-default namespace on primary.."
            )
            image = "image_" + random_string(len=4)
            out, err = rbd_secondary.create(
                **{"image-spec": f"{pool}/{image}", "size": 1024}
            )
            if err:
                raise Exception(f"Create image {pool}/{image} failed with error {err}")
            else:
                log.info(
                    "Created image "
                    + image
                    + " in default namespace of "
                    + pool
                    + "on cluster 2"
                )

            image_enable_config = {
                "pool": pool,
                "image": image,
                "mirrormode": "snapshot",
                "namespace": pool_config.get("remote_namespace"),
                "remote_namespace": pool_config.get("namespace"),
            }
            # Enable snapshot mode mirroring on images
            enable_image_mirroring(sec_config, pri_config, **image_enable_config)
            log.info(
                "Enabled image mirroring on " + pool + "/" + image + " in Cluster2"
            )

            snap_schedule_config = {
                "pool": pool,
                "image": image,
                "level": level,
                "interval": interval,
            }
            # Add snapshot schedules on the images on secondary side
            out, err = add_snapshot_scheduling(rbd_secondary, **snap_schedule_config)
            if verify_snapshot_schedule(rbd_secondary, pool, image, interval=interval):
                raise Exception(
                    "Snapshot schedule verification " + pool + "/" + image + " failed"
                )
            pri_image_spec = f"{pool}/{image}"
            sec_image_spec = f"{pool}/{namespace}/{image}"
            io_config["rbd_obj"] = rbd_secondary
            io_config["client"] = client_secondary
            image_spec = []
            image_spec.append(pri_image_spec)
            io_config["config"]["image_spec"] = image_spec
            # Write data on images created on secondary side
            (io, err) = krbd_io_handler(**io_config)
            if err:
                raise Exception(
                    f"Map, mount and run IOs failed for {io_config['config']['image_spec']}"
                )
            else:
                log.info(
                    f"Map, mount and IOs successful for {io_config['config']['image_spec']}"
                )
            time.sleep(int(interval[:-1]) * 120)
            # Verify the data on mirrored images is consistent
            data_integrity_spec = {
                "first": {
                    "image_spec": pri_image_spec,
                    "rbd": rbd_secondary,
                    "client": client_secondary,
                    "file_path": f"/tmp/{random_string(len=3)}",
                },
                "second": {
                    "image_spec": sec_image_spec,
                    "rbd": rbd_primary,
                    "client": client_primary,
                    "file_path": f"/tmp/{random_string(len=3)}",
                },
            }
            if check_data_integrity(**data_integrity_spec):
                raise Exception("Data integrity check failed for " + pri_image_spec)
    return 0


def test_default_to_non_default_namespace_mirroring(
    pri_config, sec_config, pool_types, **kw
):
    log.info(
        "Starting CEPH-83612872 - Snapshot based Namespace "
        + "level mirroring from Default to non-default namespace"
    )
    rbd_primary = pri_config.get("rbd")
    rbd_secondary = sec_config.get("rbd")
    client_primary = pri_config.get("client")
    client_secondary = sec_config.get("client")

    for pool_type in pool_types:
        rbd_config = kw.get("config", {}).get(pool_type, {})
        multi_pool_config = getdict(rbd_config)

        for pool, pool_config in multi_pool_config.items():
            multi_image_config = getdict(pool_config)
            image_config = {k: v for k, v in multi_image_config.items()}
            namespace = pool_config.get("namespace")
            remote_namespace = pool_config.get("remote_namespace")

            # for image, image_config in multi_image_config.items():
            for image, image_config_val in image_config.items():
                image_enable_config = {
                    "pool": pool,
                    "image": image,
                    "mirrormode": "snapshot",
                    "namespace": namespace,
                    "remote_namespace": remote_namespace,
                }
                enable_image_mirroring(pri_config, sec_config, **image_enable_config)
                log.info(
                    "Enabled image mirroring on " + pool + "/" + image + " in Cluster1"
                )
                if image_config_val.get(
                    "snap_schedule_levels"
                ) and image_config_val.get("snap_schedule_intervals"):
                    for level, interval in zip(
                        image_config_val["snap_schedule_levels"],
                        image_config_val["snap_schedule_intervals"],
                    ):
                        snap_schedule_config = {
                            "pool": pool,
                            "image": image,
                            "interval": interval,
                        }

                        out, err = add_snapshot_scheduling(
                            rbd_primary, **snap_schedule_config
                        )

                        if verify_snapshot_schedule(
                            rbd_primary, pool, image, interval=interval
                        ):
                            raise Exception(
                                "Snapshot schedule verification "
                                + pool
                                + "/"
                                + image
                                + " failed"
                            )

                pri_image_spec = f"{pool}/{image}"
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
                time.sleep(int(interval[:-1]) * 120)

                # Verify data is mirrored from the primary to the secondary cluster using du command
                prim_usage = rbd_primary.image_usage(
                    **{"image-spec": pri_image_spec, "format": "json"}
                )
                sec_usage = rbd_secondary.image_usage(
                    **{"image-spec": sec_image_spec, "format": "json"}
                )
                if (
                    ast.literal_eval(prim_usage[0])["images"][0]["used_size"]
                    != ast.literal_eval(sec_usage[0])["images"][0]["used_size"]
                ):
                    raise Exception(
                        "Mirrored image usage sizes are not same on primary and secondary"
                    )
                data_integrity_spec = {
                    "first": {
                        "image_spec": pri_image_spec,
                        "rbd": rbd_primary,
                        "client": client_primary,
                        "file_path": f"/tmp/{random_string(len=3)}",
                    },
                    "second": {
                        "image_spec": sec_image_spec,
                        "rbd": rbd_secondary,
                        "client": client_secondary,
                        "file_path": f"/tmp/{random_string(len=3)}",
                    },
                }
                if check_data_integrity(**data_integrity_spec):
                    raise Exception("Data integrity check failed for " + pri_image_spec)
                    log.info(
                        "Data is consistent with the mirrored image for "
                        + pri_image_spec
                    )

                # Remove schedule from primary
                out, err = remove_snapshot_scheduling(
                    rbd_primary,
                    pool=pool,
                    image=image,
                    level=level,
                    interval=interval,
                    namespace=namespace,
                )

                if err:
                    raise Exception("Failed to remove mirror snapshot schedule")

                disable_args = {"pool": pool, "image": image}
                if namespace:
                    disable_args["namespace"] = namespace

                out, err = rbd_primary.mirror.image.disable(**disable_args)
                if "Mirroring disabled" in out:
                    log.info(f"Successfully disabled mirroring for {image} on site-A")

            # create image in the non-default namespace on secondary
            log.info(
                "Creating image in the non-default namespace of secondary and "
                + "mirroring to default namespace on primary.."
            )
            image = "image_" + random_string(len=4)
            out, err = rbd_secondary.create(
                **{"image-spec": f"{pool}/{remote_namespace}/{image}", "size": 1024}
            )
            if err:
                raise Exception(
                    f"Create image {pool}/{remote_namespace}/{image} failed with error {err}"
                )
            else:
                log.info(
                    "Created image "
                    + image
                    + " in  namespace "
                    + remote_namespace
                    + "of "
                    + pool
                    + "on cluster 2"
                )

            pri_image_spec = f"{pool}/{remote_namespace}/{image}"
            sec_image_spec = f"{pool}/{image}"

            image_enable_config = {
                "pool": pool,
                "image": image,
                "mirrormode": "snapshot",
                "namespace": pool_config.get("remote_namespace"),
                "remote_namespace": pool_config.get("namespace"),
            }
            # Enable snapshot mode mirroring on images
            enable_image_mirroring(sec_config, pri_config, **image_enable_config)
            log.info(
                "Enabled image mirroring on " + pool + "/" + image + " in Cluster2"
            )

            snap_schedule_config = {
                "pool": pool,
                "image": image,
                "level": level,
                "interval": interval,
                "namespace": remote_namespace,
            }
            # Adding snapshot schedules to the images in namespace
            out, err = add_snapshot_scheduling(rbd_secondary, **snap_schedule_config)
            if err:
                raise Exception("Adding snapshot schedule failed with error " + err)
            # Verify snapshot schedules are effective on the namespaces
            verify_namespace_snapshot_schedule(
                rbd_secondary, pool, remote_namespace, interval=interval, image=image
            )

            io_config["rbd_obj"] = rbd_secondary
            io_config["client"] = client_secondary
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
            time.sleep(int(interval[:-1]) * 120)
            # Verify the data on mirrored images is consistent
            data_integrity_spec = {
                "first": {
                    "image_spec": pri_image_spec,
                    "rbd": rbd_secondary,
                    "client": client_secondary,
                    "file_path": f"/tmp/{random_string(len=3)}",
                },
                "second": {
                    "image_spec": sec_image_spec,
                    "rbd": rbd_primary,
                    "client": client_primary,
                    "file_path": f"/tmp/{random_string(len=3)}",
                },
            }
            if check_data_integrity(**data_integrity_spec):
                raise Exception("Data integrity check failed for " + pri_image_spec)

            # Remove schedule from secondary
            out, err = remove_snapshot_scheduling(
                rbd_secondary,
                pool=pool,
                image=image,
                level=level,
                interval=interval,
                namespace=remote_namespace,
            )

            if err:
                raise Exception("Failed to remove mirror snapshot schedule")

            disable_args = {"pool": pool, "image": image}
            if remote_namespace:
                disable_args["namespace"] = remote_namespace

            out, err = rbd_secondary.mirror.image.disable(**disable_args)
            if "Mirroring disabled" in out:
                log.info(f"Successfully disabled mirroring for {image} on site-B")

            # Get peer info on primary cluster
            out, err = rbd_primary.mirror.pool.info(**{"pool": pool, "format": "json"})
            if err:
                raise Exception(f"Failed to get mirror pool info: {err}")
            try:
                info = json.loads(out)
                peer_uuids = [peer["uuid"] for peer in info.get("peers", [])]
                if not peer_uuids:
                    raise Exception("No peer UUID found in mirror pool info")
                peer_uuid = peer_uuids[0]
            except Exception as e:
                raise Exception(f"Failed to parse mirror pool info: {e}")

            # Get peer info on secondary cluster
            out, err = rbd_secondary.mirror.pool.info(
                **{"pool": pool, "format": "json"}
            )
            if err:
                raise Exception(f"Failed to get mirror pool info: {err}")
            try:
                info = json.loads(out)
                peer_uuids = [peer["uuid"] for peer in info.get("peers", [])]
                if not peer_uuids:
                    raise Exception("No peer UUID found in mirror pool info")
                peer_uuid = peer_uuids[0]
            except Exception as e:
                raise Exception(f"Failed to parse mirror pool info: {e}")

            # wait for the mirror to reflect in secondary
            time.sleep(int(interval[:-1]) * 120)

            # Remove peer from both clusters
            out, err = rbd_primary.mirror.pool.peer.remove_(
                **{"pool": pool, "uuid": peer_uuid}
            )
            if err:
                raise Exception(f"Failed to remove pool peer on primary: {err}")

            out, err = rbd_secondary.mirror.pool.peer.remove_(
                **{"pool": pool, "uuid": peer_uuid}
            )
            if err:
                raise Exception(f"Failed to remove pool peer on secondary: {err}")

            # Disable mirroring on both clusters
            out, err = rbd_primary.mirror.pool.disable(**{"pool": pool})
            if err:
                raise Exception(f"Failed to disable mirroring on primary: {err}")

            out, err = rbd_secondary.mirror.pool.disable(**{"pool": pool})
            if err:
                raise Exception(f"Failed to disable mirroring on secondary: {err}")

            # Enable image mode mirroring on both clusters
            out, err = rbd_primary.mirror.pool.enable(**{"pool": pool, "mode": "image"})
            if err:
                raise Exception(
                    f"Failed to enable image mode mirroring on primary: {err}"
                )

            out, err = rbd_secondary.mirror.pool.enable(
                **{"pool": pool, "mode": "image"}
            )
            if err:
                raise Exception(
                    f"Failed to enable image mode mirroring on secondary: {err}"
                )

            log.info("Changed mirroring mode to image level on both clusters")
    return 0


def test_multi_snap_scheduling_namespace_mirroring(
    pri_config, sec_config, pool_types, **kw
):
    log.info(
        "Starting CEPH-83613949 - Namespace-based Mirroring with"
        + " Multi-Level Snapshot Scheduling"
    )

    rbd_primary = pri_config.get("rbd")
    rbd_secondary = sec_config.get("rbd")
    client_primary = pri_config.get("client")
    client_secondary = sec_config.get("client")

    for pool_type in pool_types:
        rbd_config = kw.get("config", {}).get(pool_type, {})
        multi_pool_config = getdict(rbd_config)

        for pool, pool_config in multi_pool_config.items():
            multi_image_config = getdict(pool_config)
            namespace = pool_config.get("namespace")
            remote_namespace = pool_config.get("remote_namespace")
            image_status_map = {}

            # Enable mirroring on all images first
            for image, image_config_val in multi_image_config.items():
                pri_image_spec = (
                    "{}/{}/{}".format(pool, namespace, image)
                    if namespace
                    else "{}/{}".format(pool, image)
                )
                sec_image_spec = (
                    "{}/{}/{}".format(pool, remote_namespace, image)
                    if remote_namespace
                    else "{}/{}".format(pool, image)
                )

                # Enable mirroring on image
                image_enable_config = {
                    "pool": pool,
                    "image": image,
                    "mirrormode": "snapshot",
                    "namespace": namespace,
                    "remote_namespace": remote_namespace,
                }
                enable_image_mirroring(pri_config, sec_config, **image_enable_config)

                # Wait for mirroring to be established
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

                image_status_map[image] = {
                    "pri_image_spec": pri_image_spec,
                    "sec_image_spec": sec_image_spec,
                    "image_config_val": image_config_val,
                }

            # Add snapshot schedule only once per config (not per image)
            if image_status_map:
                first_image = next(iter(image_status_map))
                image_config_val = image_status_map[first_image]["image_config_val"]
                if image_config_val.get(
                    "snap_schedule_levels"
                ) and image_config_val.get("snap_schedule_intervals"):
                    for level, interval in zip(
                        image_config_val["snap_schedule_levels"],
                        image_config_val["snap_schedule_intervals"],
                    ):
                        snap_schedule_config = {
                            "pool": pool,
                            "level": level,
                            "interval": interval,
                        }
                        # Add namespace if required
                        if level == "namespace":
                            snap_schedule_config["namespace"] = namespace
                        # Add image if required
                        if level == "image":
                            snap_schedule_config["namespace"] = namespace
                            snap_schedule_config["image"] = first_image

                        out, err = add_snapshot_scheduling(
                            rbd_primary, **snap_schedule_config
                        )
                        if err:
                            raise Exception(
                                "Adding snapshot schedule failed for {}: {}".format(
                                    pri_image_spec, err
                                )
                            )

            # Snapshot schedule verification - run only once per pool
            if image_status_map:
                # Use the first image's config as reference
                first_image = next(iter(image_status_map))
                image_config_val = image_status_map[first_image]["image_config_val"]

                if image_config_val.get(
                    "snap_schedule_levels"
                ) and image_config_val.get("snap_schedule_intervals"):
                    for level, interval in zip(
                        image_config_val["snap_schedule_levels"],
                        image_config_val["snap_schedule_intervals"],
                    ):
                        verify_args = {
                            "rbd": rbd_primary,
                            "pool": pool,
                            "interval": interval,
                        }
                        if level == "namespace":
                            verify_args["namespace"] = namespace
                        elif level == "image":
                            verify_args["namespace"] = namespace
                            verify_args["image"] = first_image

                        if verify_snapshot_schedule(**verify_args):
                            raise Exception(
                                "Snapshot schedule verification failed at {} level for {} "
                                "with interval: {}".format(level, first_image, interval)
                            )

            # Run I/O on all primary images in parallel

            fio = kw.get("config", {}).get("fio", {})
            io_configs = []
            for img, stat in image_status_map.items():
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
                        "image_spec": [stat["pri_image_spec"]],
                    },
                    "rbd_obj": rbd_primary,
                    "client": client_primary,
                }
                io_configs.append(io_config)

            try:
                # Run I/O in parallel execution
                with parallel() as p:
                    for io_config in io_configs:
                        p.spawn(krbd_io_handler, **io_config)
            except Exception as e:
                raise Exception(f"I/O spawning or execution failed: {e}")

            log.info("All I/O completed successfully")

            # Wait for snapshot interval to elapse
            interval = image_config_val["snap_schedule_intervals"][-1]
            time.sleep(int(interval[:-1]) * 120)

            # Validate data consistency in parallel for all images
            data_integrity_specs = []
            for img, stat in image_status_map.items():
                data_integrity_specs.append(
                    {
                        "first": {
                            "image_spec": stat["pri_image_spec"],
                            "rbd": rbd_primary,
                            "client": client_primary,
                            "file_path": "/tmp/{}".format(random_string(len=3)),
                        },
                        "second": {
                            "image_spec": stat["sec_image_spec"],
                            "rbd": rbd_secondary,
                            "client": client_secondary,
                            "file_path": "/tmp/{}".format(random_string(len=3)),
                        },
                    }
                )

            try:
                with parallel() as p:
                    for spec in data_integrity_specs:
                        p.spawn(check_data_integrity, **spec)
            except Exception as e:
                raise Exception(f"Data integrity check spawn failed: {e}")

            log.info("Data is consistent from primary and secondary")

        # Log test case success for this pool
        namespace_mirror_type = rbd_config.get("namespace_mirror_type")

        # Extract unique snapshot levels from all images
        snap_schedule_level = list(
            set(
                level
                for status in image_status_map.values()
                for level in status["image_config_val"].get("snap_schedule_levels", [])
            )
        )

        log.info(
            "Test passed for pool_type: {}, namespace_mirror_type: {}, snap_schedule_level: {}".format(
                pool_type, namespace_mirror_type, snap_schedule_level
            )
        )

    return 0


def test_failover_and_image_operations(pri_config, sec_config, pool_types, **kw):
    log.info(
        "Starting CEPH-83613951 - Failover and Image Operations with Namespace Mirroring"
    )

    rbd_primary = pri_config.get("rbd")
    rbd_secondary = sec_config.get("rbd")
    client_primary = pri_config.get("client")

    def construct_imagespec(pool, namespace, image):
        return f"{pool}/{namespace}/{image}" if namespace else f"{pool}/{image}"

    for pool_type in pool_types:
        rbd_config = kw.get("config", {}).get(pool_type, {})
        multi_pool_config = getdict(rbd_config)

        for pool, pool_config in multi_pool_config.items():
            multi_image_config = getdict(pool_config)
            namespace = pool_config.get("namespace")
            remote_namespace = pool_config.get("remote_namespace")

            for image, image_config_val in multi_image_config.items():
                pri_image_spec = construct_imagespec(pool, namespace, image)
                sec_image_spec = construct_imagespec(pool, remote_namespace, image)

                enable_image_mirroring(
                    pri_config,
                    sec_config,
                    pool=pool,
                    image=image,
                    mirrormode="snapshot",
                    namespace=namespace,
                    remote_namespace=remote_namespace,
                )

                wait_for_status(
                    rbd=rbd_primary,
                    cluster_name=pri_config["cluster"].name,
                    imagespec=pri_image_spec,
                    state_pattern="up+stopped",
                )
                wait_for_status(
                    rbd=rbd_secondary,
                    cluster_name=sec_config["cluster"].name,
                    imagespec=sec_image_spec,
                    state_pattern="up+replaying",
                )

                if image_config_val.get(
                    "snap_schedule_levels"
                ) and image_config_val.get("snap_schedule_intervals"):
                    for level, interval in zip(
                        image_config_val["snap_schedule_levels"],
                        image_config_val["snap_schedule_intervals"],
                    ):
                        out, err = add_snapshot_scheduling(
                            rbd_primary,
                            pool=pool,
                            image=image,
                            level=level,
                            interval=interval,
                            namespace=namespace,
                        )
                        if err:
                            raise Exception(
                                f"Adding snapshot schedule failed with error {err}"
                            )

                fio_config = kw.get("config", {}).get("fio", {})
                io_config = {
                    "size": fio_config["size"],
                    "do_not_create_image": True,
                    "num_jobs": fio_config["ODF_CONFIG"]["num_jobs"],
                    "iodepth": fio_config["ODF_CONFIG"]["iodepth"],
                    "rwmixread": fio_config["ODF_CONFIG"]["rwmixread"],
                    "direct": fio_config["ODF_CONFIG"]["direct"],
                    "invalidate": fio_config["ODF_CONFIG"]["invalidate"],
                    "rbd_obj": rbd_primary,
                    "client": client_primary,
                    "config": {
                        "file_size": fio_config["size"],
                        "file_path": [f"/mnt/mnt_{random_string(len=5)}/file"],
                        "get_time_taken": True,
                        "operations": {
                            "fs": "ext4",
                            "io": True,
                            "mount": True,
                            "map": True,
                        },
                        "cmd_timeout": 2400,
                        "io_type": fio_config["ODF_CONFIG"]["io_type"],
                        "image_spec": [pri_image_spec],
                    },
                }
                io, err = krbd_io_handler(**io_config)
                if err:
                    raise Exception(
                        f"Map, mount and run IOs failed for {pri_image_spec}"
                    )
                log.info(f"Map, mount and IOs successful for {pri_image_spec}")

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

                log.info(f"Promoting secondary image {sec_image_spec} on Cluster-2")
                out, err = rbd_secondary.mirror.image.promote(
                    **{"image-spec": sec_image_spec}
                )
                if err:
                    raise Exception(
                        f"Promote image {sec_image_spec} failed with error {err}"
                    )
                log.info(f"Promoted image {sec_image_spec} successfully")

                log.info(f"Resyncing image {pri_image_spec} on Cluster-1")
                out, err = rbd_primary.mirror.image.resync(
                    **{"image-spec": pri_image_spec}
                )
                if err:
                    raise Exception(
                        f"Resync image {pri_image_spec} failed with error {err}"
                    )
                log.info(f"Resync initiated for image {pri_image_spec}")

                if image_config_val.get(
                    "snap_schedule_levels"
                ) and image_config_val.get("snap_schedule_intervals"):
                    for level, interval in zip(
                        image_config_val["snap_schedule_levels"],
                        image_config_val["snap_schedule_intervals"],
                    ):
                        out, err = add_snapshot_scheduling(
                            rbd_secondary,
                            pool=pool,
                            image=image,
                            level=level,
                            interval=interval,
                            namespace=remote_namespace,
                        )
                        if err:
                            raise Exception(
                                f"Adding snapshot schedule failed with error {err}"
                            )

                new_image_name = f"{image}_renamed"
                new_sec_image_spec = construct_imagespec(
                    pool, remote_namespace, new_image_name
                )
                new_pri_image_spec = construct_imagespec(
                    pool, namespace, new_image_name
                )
                log.info(
                    f"Renaming image {sec_image_spec} to {new_sec_image_spec} on Cluster-2"
                )
                out, err = rbd_secondary.rename(
                    **{
                        "source-image-spec": sec_image_spec,
                        "dest-image-spec": new_sec_image_spec,
                    }
                )
                if err:
                    raise Exception(
                        f"Rename image {sec_image_spec} failed with error {err}"
                    )
                log.info(f"Renamed image {sec_image_spec} to {new_sec_image_spec}")

                time.sleep(
                    int(image_config_val["snap_schedule_intervals"][-1][:-1]) * 120
                )

                out, err = rbd_primary.info(
                    **{"image-or-snap-spec": new_pri_image_spec}
                )
                if err:
                    raise Exception(
                        f"Renamed image {new_pri_image_spec} not found on Cluster-1: {err}"
                    )
                log.info(f"Renamed image {new_pri_image_spec} found on Cluster-1")

                resize_size = 2 * 1024  # 2GB
                log.info(
                    f"Resizing image {new_sec_image_spec} to {resize_size} on Cluster-2"
                )
                out, err = rbd_secondary.resize(
                    **{
                        "image-spec": new_sec_image_spec,
                        "size": resize_size,
                        "allow-shrink": True,
                    }
                )
                if out or err and "100% complete" not in out + err:
                    raise Exception(
                        f"Resize image {new_sec_image_spec} failed with error {err}"
                    )
                log.info(f"Resized image {new_sec_image_spec} to {resize_size}")

                time.sleep(
                    int(image_config_val["snap_schedule_intervals"][-1][:-1]) * 120
                )

                out, err = rbd_primary.info(
                    **{"image-or-snap-spec": new_pri_image_spec, "format": "json"}
                )
                if err:
                    raise Exception(
                        f"Failed to get info for image {new_pri_image_spec} on Cluster-1: {err}"
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
                        f"Image size mismatch after resize: expected {resize_size}, got {actual_size} on Cluster-1"
                    )

                log.info(f"Resize verified for image {new_sec_image_spec} on Cluster-1")

                log.info(f"Removing image {new_sec_image_spec} on Cluster-2")
                out, err = rbd_secondary.rm(**{"image-spec": new_sec_image_spec})
                if out or err and "100% complete" not in out + err:
                    raise Exception(
                        f"Remove image {new_sec_image_spec} failed with error {err}"
                    )
                log.info(f"Removed image {new_sec_image_spec} on Cluster-2")

                time.sleep(
                    int(image_config_val["snap_schedule_intervals"][-1][:-1]) * 120
                )

                out, err = rbd_primary.info(
                    **{"image-or-snap-spec": new_pri_image_spec}
                )
                if not err:
                    raise Exception(
                        f"Image {new_pri_image_spec} still exists on Cluster-1 after removal"
                    )
                log.info(
                    f"Verified image {new_pri_image_spec} is removed from Cluster-1"
                )

        log.info(
            f"Test passed for pool_type: {pool_type}, namespace_mirror_type: {rbd_config.get('namespace_mirror_type')}"
        )

    return 0


def test_disable_enable_namespace_mirroring(pri_config, sec_config, pool_types, **kw):
    log.info("Starting CEPH-83613952 - Disable and Enable Namespace-based Mirroring")
    rbd_primary = pri_config.get("rbd")
    rbd_secondary = sec_config.get("rbd")
    client_primary = pri_config.get("client")
    client_secondary = sec_config.get("client")

    def construct_imagespec(pool, namespace, image):
        return f"{pool}/{namespace}/{image}" if namespace else f"{pool}/{image}"

    for pool_type in pool_types:
        rbd_config = kw.get("config", {}).get(pool_type, {})
        multi_pool_config = getdict(rbd_config)

        for pool, pool_config in multi_pool_config.items():
            multi_image_config = getdict(pool_config)
            namespace = pool_config.get("namespace")
            remote_namespace = pool_config.get("remote_namespace")

            for image, image_config_val in multi_image_config.items():
                pri_image_spec = construct_imagespec(pool, namespace, image)
                sec_image_spec = construct_imagespec(pool, remote_namespace, image)

                enable_image_mirroring(
                    pri_config,
                    sec_config,
                    pool=pool,
                    image=image,
                    mirrormode="snapshot",
                    namespace=namespace,
                    remote_namespace=remote_namespace,
                )

                wait_for_status(
                    rbd=rbd_primary,
                    cluster_name=pri_config["cluster"].name,
                    imagespec=pri_image_spec,
                    state_pattern="up+stopped",
                )
                wait_for_status(
                    rbd=rbd_secondary,
                    cluster_name=sec_config["cluster"].name,
                    imagespec=sec_image_spec,
                    state_pattern="up+replaying",
                )

                if image_config_val.get(
                    "snap_schedule_levels"
                ) and image_config_val.get("snap_schedule_intervals"):
                    for level, interval in zip(
                        image_config_val["snap_schedule_levels"],
                        image_config_val["snap_schedule_intervals"],
                    ):
                        out, err = add_snapshot_scheduling(
                            rbd_primary,
                            pool=pool,
                            image=image,
                            level=level,
                            interval=interval,
                            namespace=namespace,
                        )
                        if err:
                            raise Exception(
                                f"Adding snapshot schedule failed with error {err}"
                            )

                fio_config = kw.get("config", {}).get("fio", {})
                io_config = {
                    "size": fio_config["size"],
                    "do_not_create_image": True,
                    "num_jobs": fio_config["ODF_CONFIG"]["num_jobs"],
                    "iodepth": fio_config["ODF_CONFIG"]["iodepth"],
                    "rwmixread": fio_config["ODF_CONFIG"]["rwmixread"],
                    "direct": fio_config["ODF_CONFIG"]["direct"],
                    "invalidate": fio_config["ODF_CONFIG"]["invalidate"],
                    "rbd_obj": rbd_primary,
                    "client": client_primary,
                    "config": {
                        "file_size": fio_config["size"],
                        "file_path": [f"/mnt/mnt_{random_string(len=5)}/file"],
                        "get_time_taken": True,
                        "operations": {
                            "fs": "ext4",
                            "io": True,
                            "mount": True,
                            "map": True,
                        },
                        "cmd_timeout": 2400,
                        "io_type": fio_config["ODF_CONFIG"]["io_type"],
                        "image_spec": [pri_image_spec],
                    },
                }
                io, err = krbd_io_handler(**io_config)
                if err:
                    raise Exception(
                        f"Map, mount and run IOs failed for {pri_image_spec}"
                    )
                log.info(f"Map, mount and IOs successful for {pri_image_spec}")

                log.info("wait for two minutes data to mirror")
                time.sleep(int(interval[:-1]) * 120)

                # Verify the data on mirrored images is consistent
                data_integrity_spec = {
                    "first": {
                        "image_spec": pri_image_spec,
                        "rbd": rbd_primary,
                        "client": client_primary,
                        "file_path": f"/tmp/{random_string(len=3)}",
                    },
                    "second": {
                        "image_spec": sec_image_spec,
                        "rbd": rbd_secondary,
                        "client": client_secondary,
                        "file_path": f"/tmp/{random_string(len=3)}",
                    },
                }
                if check_data_integrity(**data_integrity_spec):
                    raise Exception("Data integrity check failed for " + pri_image_spec)
                log.info(
                    "Data is consistent with the mirrored image for " + pri_image_spec
                )

                # Remove pool peer on both clusters
                out, err = rbd_primary.mirror.pool.info(
                    **{"pool": pool, "format": "json"}
                )
                if err:
                    raise Exception(f"Failed to get mirror pool info: {err}")
                try:
                    info = json.loads(out)
                    peer_uuids = [peer["uuid"] for peer in info.get("peers", [])]
                    if not peer_uuids:
                        raise Exception("No peer UUID found in mirror pool info")
                    peer_uuid = peer_uuids[0]
                except Exception as e:
                    raise Exception(f"Failed to parse mirror pool info: {e}")

                out, err = rbd_primary.mirror.pool.peer.remove_(
                    **{"pool": pool, "uuid": peer_uuid}
                )
                if err:
                    raise Exception(f"Failed to remove pool peer on primary: {err}")

                out, err = rbd_secondary.mirror.pool.info(
                    **{"pool": pool, "format": "json"}
                )
                if err:
                    raise Exception(f"Failed to get mirror pool info: {err}")
                try:
                    info = json.loads(out)
                    peer_uuids = [peer["uuid"] for peer in info.get("peers", [])]
                    if not peer_uuids:
                        raise Exception("No peer UUID found in mirror pool info")
                    peer_uuid = peer_uuids[0]
                except Exception as e:
                    raise Exception(f"Failed to parse mirror pool info: {e}")

                out, err = rbd_secondary.mirror.pool.peer.remove_(
                    **{"pool": pool, "uuid": peer_uuid}
                )
                if err:
                    raise Exception(f"Failed to remove pool peer on secondary: {err}")

                # Disable mirroring on the pool on both clusters
                out, err = rbd_primary.mirror.pool.disable(**{"pool": pool})
                if err:
                    raise Exception(f"Failed to disable mirroring on primary: {err}")

                out, err = rbd_secondary.mirror.pool.disable(**{"pool": pool})
                if err:
                    raise Exception(f"Failed to disable mirroring on secondary: {err}")

                # Re-enable the mirroring on the pool on both clusters
                out, err = rbd_primary.mirror.pool.enable(
                    **{"pool": pool, "mode": "image"}
                )
                if err:
                    raise Exception(
                        f"Failed to enable image mode mirroring on primary: {err}"
                    )

                out, err = rbd_secondary.mirror.pool.enable(
                    **{"pool": pool, "mode": "image"}
                )
                if err:
                    raise Exception(
                        f"Failed to enable image mode mirroring on secondary: {err}"
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
                    cluster_name=pri_config["cluster"].name,
                    imagespec=pri_image_spec,
                    state_pattern="up+stopped",
                )
                wait_for_status(
                    rbd=rbd_secondary,
                    cluster_name=sec_config["cluster"].name,
                    imagespec=sec_image_spec,
                    state_pattern="up+replaying",
                )

                if image_config_val.get(
                    "snap_schedule_levels"
                ) and image_config_val.get("snap_schedule_intervals"):
                    for level, interval in zip(
                        image_config_val["snap_schedule_levels"],
                        image_config_val["snap_schedule_intervals"],
                    ):
                        out, err = add_snapshot_scheduling(
                            rbd_primary,
                            pool=pool,
                            image=image,
                            level=level,
                            interval=interval,
                            namespace=namespace,
                        )
                        if err:
                            raise Exception(
                                f"Adding snapshot schedule failed with error {err}"
                            )

                bench_kw = {
                    "image-spec": pri_image_spec,
                    "io-type": "write",
                    "io-total": "200M",
                    "io-threads": 16,
                }

                out, err = rbd_primary.bench(**bench_kw)
                if err:
                    raise Exception(
                        "Failed to write IO to the image %s: %s" % (pri_image_spec, err)
                    )
                else:
                    log.info("Successfully ran IO on image %s" % pri_image_spec)

                log.info("waiting for 2 minutes data to mirror")
                time.sleep(int(interval[:-1]) * 120)

                # Verify the data on mirrored images is consistent
                data_integrity_spec = {
                    "first": {
                        "image_spec": pri_image_spec,
                        "rbd": rbd_primary,
                        "client": client_primary,
                        "file_path": f"/tmp/{random_string(len=3)}",
                    },
                    "second": {
                        "image_spec": sec_image_spec,
                        "rbd": rbd_secondary,
                        "client": client_secondary,
                        "file_path": f"/tmp/{random_string(len=3)}",
                    },
                }
                if check_data_integrity(**data_integrity_spec):
                    raise Exception("Data integrity check failed for " + pri_image_spec)
                log.info(
                    "Data is consistent with the mirrored image for " + pri_image_spec
                )

                log.info(
                    "Test passed successfully for disabling and enabling namespace mirroring"
                )

    return 0


def run(**kw):
    """
    Test to verify default and non-default namespace mirroring test scenarios
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
              namespace_mirror_type: non-default_to_default
              mirrormode: snapshot
              snap_schedule_levels:
                - namespace
              snap_schedule_intervals:
                - 1m
    """
    try:
        mirror_obj = initial_mirror_config(**kw)
        mirror_obj.pop("output", [])
        for val in mirror_obj.values():
            if not val.get("is_secondary", False):
                pri_config = val
            else:
                sec_config = val
        log.info("Initial configuration complete")
        pool_types = list(mirror_obj.values())[0].get("pool_types")
        test_map = {
            "CEPH-83612860": test_non_default_to_default_namespace_mirroring,
            "CEPH-83612872": test_default_to_non_default_namespace_mirroring,
            "CEPH-83613949": test_multi_snap_scheduling_namespace_mirroring,
            "CEPH-83613951": test_failover_and_image_operations,
            "CEPH-83613952": test_disable_enable_namespace_mirroring,
        }

        test_func = kw["config"]["test_function"]
        if test_func in test_map:
            test_map[test_func](pri_config, sec_config, pool_types, **kw)

    except Exception as e:
        log.error(f"Test {test_func} failed with error {str(e)}")
        return 1
    finally:
        cleanup(pool_types=pool_types, multi_cluster_obj=mirror_obj, **kw)
    return 0
