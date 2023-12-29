from copy import deepcopy

from ceph.rbd.initial_config import initial_mirror_config, random_string
from ceph.rbd.utils import check_data_integrity, getdict
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.snap_clone_operations import (
    clone_ops,
    purge_snap_and_verify,
    remove_snap_and_verify,
    snap_create_list_and_verify,
    test_snap_rollback,
)
from ceph.rbd.workflows.snap_scheduling import run_io_verify_snap_schedule_single_image
from utility.log import Log

log = Log(__name__)


def test_snap_mirror_snap_clone_ops(
    rbd_obj, sec_obj, client_node, sec_client, pool_type, **kw
):
    """
    Test the snapshot mirroring with snap and clone operations.

    Args:
        rbd_obj: RBD object
        sec_obj: Secondary RBD object
        client_node: RBD client node
        sec_client: Secondary client node
        pool_type: rep_pool_config/ec_pool_config
        **kw: test data

    Returns:
        int: The return value. 0 for success, 1 for failure.
    """
    try:
        log.info(
            f"Running snap and clone operations while testing snap mirroring for pool type {pool_type}"
        )

        config = deepcopy(kw.get("config").get(pool_type))
        for pool, pool_config in getdict(config).items():
            multi_image_config = getdict(pool_config)
            multi_image_config.pop("test_config", {})
            for image, image_config in multi_image_config.items():
                image_spec = f"{pool}/{image}"
                mount_path = f"/tmp/mnt_{random_string(len=5)}"
                rc = run_io_verify_snap_schedule_single_image(
                    rbd=rbd_obj,
                    client=client_node,
                    pool=pool,
                    image=image,
                    image_config=image_config,
                    mount_path=f"{mount_path}/file_00",
                    skip_mkfs=False,
                )
                if rc:
                    log.error(
                        f"Run IO and verify snap schedule failed for image {pool}/{image}"
                    )
                    return 1

                data_integrity_spec = {
                    "first": {
                        "image_spec": image_spec,
                        "rbd": rbd_obj,
                        "client": client_node,
                        "file_path": f"/tmp/{random_string(len=3)}",
                    },
                    "second": {
                        "image_spec": image_spec,
                        "rbd": sec_obj,
                        "client": sec_client,
                        "file_path": f"/tmp/{random_string(len=3)}",
                    },
                }
                rc = check_data_integrity(**data_integrity_spec)
                if rc:
                    log.error(f"Data integrity check failed for {image_spec}")
                    return 1

                log.info(f"Perform snapshot operations for image for {image_spec}")

                log.info(
                    f"Create snapshots, list and verify for image {image_spec} for primary cluster"
                )

                rc = snap_create_list_and_verify(
                    pool=pool,
                    image=image,
                    image_config=image_config,
                    rbd=rbd_obj,
                    sec_obj=sec_obj,
                    is_secondary=False,
                    **kw,
                )
                if rc:
                    log.error(
                        f"Snapshot creation, listing and verification failed for {image_spec} in primary cluster"
                    )
                    return 1

                log.info(
                    f"Create snapshots, list and verify for image {image_spec} for secondary cluster"
                )

                rc = snap_create_list_and_verify(
                    pool=pool, image=image, rbd=sec_obj, is_secondary=True, **kw
                )
                if rc:
                    log.error(
                        f"Snapshot creation, listing and verification failed for {image_spec} in secondary cluster"
                    )
                    return 1

                log.info(
                    f"Creating clones and performing clone operations for primary cluster for {image_spec}"
                )
                clone_ops_conf = {
                    "pool": pool,
                    "image": image,
                    "rbd": rbd_obj,
                    "is_secondary": False,
                    "operations": {
                        "protect_snap": True,
                        "create_clone": True,
                        "num_clones_per_snap": 1,
                        "unprotect_snap": True,
                        "flatten_clone": True,
                    },
                    **kw,
                }
                rc = clone_ops(**clone_ops_conf)
                if rc:
                    log.error(
                        f"Clone operations failed for {image_spec} in primary cluster"
                    )
                    return 1

                log.info(
                    f"Creating clones and performing clone operations for secondary cluster for {image_spec}"
                )
                clone_ops_conf["rbd"] = sec_obj
                clone_ops_conf["is_secondary"] = True
                clone_ops_conf["pri_rbd"] = rbd_obj
                clone_ops_conf["image_config"] = image_config
                rc = clone_ops(**clone_ops_conf)
                if rc:
                    log.error(
                        f"Clone operations did not work as expected for {image_spec} in secondary cluster"
                    )
                    return 1

                log.info(
                    f"Testing snap rollback functionality for {image_spec} in primary cluster"
                )
                snap_rollback_conf = {
                    "pool": pool,
                    "image": image,
                    "image_config": image_config,
                    "rbd": rbd_obj,
                    "client": client_node,
                    "is_secondary": False,
                    "mount_path": f"{mount_path}/file_01",
                    **kw,
                }
                rc = test_snap_rollback(**snap_rollback_conf)
                if rc:
                    log.error(
                        f"Snap rollback failed for pool type {image_spec} in primary cluster"
                    )
                    return 1

                log.info(
                    f"Testing snap rollback functionality for {image_spec} in secondary cluster"
                )
                snap_rollback_conf["rbd"] = sec_obj
                snap_rollback_conf["client"] = sec_client
                snap_rollback_conf["is_secondary"] = True
                rc = test_snap_rollback(**snap_rollback_conf)
                if rc:
                    log.error(
                        f"Snap rollback did not work as expected for pool type {image_spec} in secondary cluster"
                    )
                    return 1

                log.info(
                    f"Remove a user defined snap from secondary cluster for {image_spec}"
                )
                rc = remove_snap_and_verify(
                    pool=pool, image=image, rbd=sec_obj, is_secondary=True, **kw
                )
                if rc:
                    log.error(
                        f"Snapshot removal did not work as expected for {image_spec} in secondary cluster"
                    )
                    return 1

                log.info(
                    f"Remove a user defined snap from primary cluster for {image_spec}"
                )
                rc = remove_snap_and_verify(
                    pool=pool, image=image, rbd=rbd_obj, is_secondary=False, **kw
                )
                if rc:
                    log.error(
                        f"Snapshot removal failed for {image_spec} in primary cluster"
                    )
                    return 1

                log.info(
                    f"Testing purge snapshots functionality for {image_spec} in secondary cluster"
                )

                log.info(
                    f"Create snapshots, for image {image_spec} to test purge cluster"
                )

                rc = snap_create_list_and_verify(
                    pool=pool,
                    image=image,
                    image_config=image_config,
                    rbd=rbd_obj,
                    sec_obj=sec_obj,
                    is_secondary=False,
                    **kw,
                )
                if rc:
                    log.error(f"Snapshot creation failed for {image_spec}")
                    return 1

                rc = purge_snap_and_verify(
                    pool=pool, image=image, rbd=sec_obj, is_secondary=True, **kw
                )
                if rc:
                    log.error(
                        f"Snapshot purge did not work as expected for {image_spec} in secondary cluster"
                    )
                    return 1

                log.info(
                    f"Testing purge snapshots functionality for {image_spec} in primary cluster"
                )
                rc = purge_snap_and_verify(
                    pool=pool, image=image, rbd=rbd_obj, is_secondary=False, **kw
                )
                if rc:
                    log.error(
                        f"Snapshot purge failed for {image_spec} in primary cluster"
                    )
                    return 1
    except Exception as err:
        log.error(err)
        return 1
    return 0


def run(**kw):
    """CEPH-83574861 - Configure two-way rbd-mirror (replicated and ec pool) on
    Stand alone CEPH cluster on image with snapshot based mirroring and perform snapshot,
    clone and mirror snapshot schedule operations from primary and secondary clusters.
    CEPH-83574862 - Configure one-way rbd-mirror (replicated pool) on Stand alone CEPH
    cluster on image with snapshot based mirroring and perform clone operations from
    primary and secondary clusters.
    Pre-requisites :
    We need atleast one client node with ceph-common, fio and rbd-nbd packages,
    conf and keyring files in both clusters with snapshot based RBD mirroring
    enabled between the clusters.
    kw:
        clusters:
            ceph-rbd1:
            config:
                rep_pool_config:
                num_pools: 1
                num_images: 5
                size: 10G
                mode: image # compulsory argument if mirroring needs to be setup
                mirrormode: snapshot
                snap_schedule_levels:
                    - image
                snap_schedule_intervals: #one value for each level specified above
                    - 5m
                io_percentage: 30 #percentage of space in each image to be filled
                ec_pool_config:
                num_pools: 1
                num_images: 5
                mode: image # compulsory argument if mirroring needs to be setup
                mirrormode: snapshot
                snap_schedule_levels:
                    - image
                snap_schedule_intervals:
                    - 5m
                io_size: 200M
            ceph-rbd2:
            config:
                rep_pool_config:
                num_pools: 1
                num_images: 5
                size: 10G
                mode: image # compulsory argument if mirroring needs to be setup
                mirrormode: snapshot
                snap_schedule_levels:
                    - image
                snap_schedule_intervals: #one value for each level specified above
                    - 5m
                io_percentage: 30 #percentage of space in each image to be filled
                ec_pool_config:
                num_pools: 1
                num_images: 5
                mode: image # compulsory argument if mirroring needs to be setup
                mirrormode: snapshot
                snap_schedule_levels:
                    - image
                snap_schedule_intervals:
                    - 5m
                io_size: 200M
    Test Case Flow
    1. Bootstrap two CEPH clusters and setup snapshot based mirroring in between these clusters
    2. Create pools and images as specified, enable snapshot based mirroring for all these images
    3. Schedule snapshots for each of these images and run IOs on each of the images
    4. Perform the operations like add snapshots - remove snapshots - list snapshots - rollback
        snapshots - purge snapshots
    5. Try snapshot creation/deletion from secondary cluster
    6. Perform Clone Operations like protect snapshot,  clone the snapshot,  unprotect snapshot,
        flatten clone
    7. Try above operation from secondary
    8. Perform snap schedule operations like, add/list/status/remove from both primary and secondary
        clusters
    Test Case Flow 2
    1. Bootstrap two CEPH clusters and setup one way snapshot based mirroring in between these clusters
    2. Create pools and images as specified, enable snapshot based mirroring for all these images
    3. Schedule snapshots for each of these images and run IOs on each of the images
    4. Perform the operations like add snapshots - remove snapshots - list snapshots - rollback
        snapshots - purge snapshots
    5. Try snapshot creation/deletion from secondary cluster
    6. Perform Clone Operations like protect snapshot,  clone the snapshot,  unprotect snapshot,
        flatten clone
    7. Try above operation from secondary
    8. Perform snap schedule operations like, add/list/status/remove from both primary and secondary
        clusters
    """
    pool_types = ["rep_pool_config", "ec_pool_config"]
    log.info("Running snap and clone operations on snapshot based mirroring clusters")

    try:
        kw.get("config", {})["do_not_run_io"] = True
        mirror_obj = initial_mirror_config(**kw)
        mirror_obj.pop("output", [])

        for val in mirror_obj.values():
            if not val.get("is_secondary", False):
                rbd = val.get("rbd")
                client = val.get("client")
            else:
                sec_rbd = val.get("rbd")
                sec_client = val.get("client")
        log.info("Initial configuration complete")

        pool_types = list(mirror_obj.values())[0].get("pool_types")
        for pool_type in pool_types:
            log.info(
                f"Running snap and clone operations on snap based mirroring for {pool_type}"
            )
            rc = test_snap_mirror_snap_clone_ops(
                rbd, sec_rbd, client, sec_client, pool_type, **kw
            )
            if rc:
                log.error(
                    f"Snap and clone operations on snapshot based mirroring clusters failed for {pool_type}"
                )
                return 1
    except Exception as e:
        log.error(
            f"Testing snap and clone operations on snap mirroring clusters failed with error {str(e)}"
        )
        return 1
    finally:
        cleanup(pool_types=pool_types, multi_cluster_obj=mirror_obj, **kw)
    return 0
