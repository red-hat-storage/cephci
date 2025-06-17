"""
Module to cover test cases related to Snapshot based Namespace
level mirroring from Non default to default and default to Non default.

Pre-requisites :
1. Two ceph clusters version 8.1 or later with mon,mgr,osd
2. Deploy rbd-mirror daemon service on both clusters

Test case covered:

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
8. Add a snapshot schedule for the mirrored image in the namespace level
9. Initiate I/O operations on the image using rbd bench or fio or file mount
10. Wait till snapshot schedule interval set like above 1m
11. Verify that data is mirrored from the primary to the secondary cluster
12. Verify data consistency using md5sum checksum from primary and secondary
13. Stop all client I/O operations on the image
14. Demote the primary image on cluster-1
15. Promote the non-primary images located on the cluster-2
16. Verify failover by checking mirroring status on cluster2 and cluster1
    # rbd mirror image status pool1/ns1_s/image1
17. Run IO on the promoted image from cluster-2
18. Add a snapshot schedule for the mirrored image in namespace level
19. Wait till snapshot schedule interval set
20. Verify data consistency using md5sum checksum from primary and secondary
21. Repeat the above test on EC pool with default_to_non-default
22. Cleanup the images, namespace, pools along with disk cleanup.
"""

import time

from ceph.rbd.initial_config import initial_mirror_config
from ceph.rbd.utils import check_data_integrity, getdict, random_string
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.krbd_io_handler import krbd_io_handler
from ceph.rbd.workflows.rbd_mirror import enable_image_mirroring, wait_for_status
from ceph.rbd.workflows.snap_scheduling import (
    add_snapshot_scheduling,
    verify_snapshot_schedule,
)
from utility.log import Log

log = Log(__name__)


def test_failover_orderly_shutdown(pri_config, sec_config, pool_types, **kw):
    """
    Test to verify failover orderly shutdown for namespace mirroring
    Args:
        pri_config: Primary cluster configuration
        sec_config: Secondary cluster configuration
        pool_types: Types of pools used in the test
        kw: Key/value pairs of configuration information to be used in the test
    """
    log.info("Starting CEPH-83613955 failover orderly shutdown test")

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

                time.sleep(
                    int(image_config_val["snap_schedule_intervals"][-1][:-1]) * 120
                )

                # Verify the snapshot schedule for the primary image
                verify_args = {
                    "rbd": rbd_primary,
                    "pool": pool,
                    "interval": interval,
                }
                if level == "namespace":
                    verify_args["namespace"] = namespace
                elif level == "image":
                    verify_args["namespace"] = namespace
                    verify_args["image"] = image
                if verify_snapshot_schedule(**verify_args):
                    raise Exception(
                        "Snapshot schedule verification failed at {} level for {} "
                        "with interval: {}".format(level, image, interval)
                    )
                log.info(
                    f"Snapshot schedule verified for {pri_image_spec} at {level} level"
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

                # Verify the failover by checking mirroring status on cluster-2
                wait_for_status(
                    rbd=rbd_secondary,
                    cluster_name=sec_config["cluster"].name,
                    imagespec=sec_image_spec,
                    state_pattern="up+stopped",
                )

                # Verify the failover by checking mirroring status on cluster-1
                wait_for_status(
                    rbd=rbd_primary,
                    cluster_name=pri_config["cluster"].name,
                    imagespec=pri_image_spec,
                    state_pattern="up+replaying",
                )

                # add snapshot schedule for the promoted image
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
                log.info(
                    f"Snapshot schedule added for the promoted image {sec_image_spec}"
                )

                # run IO on the promoted image
                bench_kw = {
                    "image-spec": sec_image_spec,
                    "io-type": "write",
                    "io-total": "200M",
                    "io-threads": 16,
                }

                out, err = rbd_secondary.bench(**bench_kw)

                if err:
                    raise Exception(
                        f"Failed to write IO to the image {sec_image_spec}: {err}"
                    )
                else:
                    log.info(f"Successfully ran IO on image {sec_image_spec}")

                time.sleep(
                    int(image_config_val["snap_schedule_intervals"][-1][:-1]) * 120
                )

                # Verify the snapshot schedule for the promoted image
                verify_args = {
                    "rbd": rbd_secondary,
                    "pool": pool,
                    "interval": interval,
                }
                if level == "namespace":
                    verify_args["namespace"] = remote_namespace
                elif level == "image":
                    verify_args["namespace"] = remote_namespace
                    verify_args["image"] = image

                if verify_snapshot_schedule(**verify_args):
                    raise Exception(
                        "Snapshot schedule verification failed at {} level for {} "
                        "with interval: {}".format(level, image, interval)
                    )
                log.info(
                    f"Snapshot schedule verified for {sec_image_spec} at {level} level"
                )

                # Verify the data on mirrored images is consistent after failover
                data_integrity_spec = {
                    "first": {
                        "image_spec": sec_image_spec,
                        "rbd": rbd_secondary,
                        "client": client_secondary,
                        "file_path": f"/tmp/{random_string(len=3)}",
                    },
                    "second": {
                        "image_spec": pri_image_spec,
                        "rbd": rbd_primary,
                        "client": client_primary,
                        "file_path": f"/tmp/{random_string(len=3)}",
                    },
                }
                if check_data_integrity(**data_integrity_spec):
                    raise Exception("Data integrity check failed for " + sec_image_spec)
                log.info(
                    "Data is consistent with the mirrored image for " + sec_image_spec
                )

        namespace_mirror_type = rbd_config.get("namespace_mirror_type")
        log.info(
            "Test passed for pool_type: {}, namespace_mirror_type: {}".format(
                pool_type, namespace_mirror_type
            )
        )
    return 0


def run(**kw):
    """
    Test to verify default and non-default namespace mirroring failover test cases.

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
            "CEPH-83613955": test_failover_orderly_shutdown,
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
