"""
Module to cover test cases related to Snapshot based Namespace
level mirroring from Non default to default and default to Non default.

Pre-requisites :
1. Two ceph clusters version 8.1 or later with mon, mgr, osd
2. Deploy rbd-mirror daemon service on both clusters

Test case covered:

CEPH-83613955:
1. Create pools on both clusters.
2. Create namespaces (e.g., ns1_p) in the pool on cluster-1.
3. Enable non-default namespace mirroring with "init-only" mode on cluster-1.
    - rbd mirror pool enable --pool <pool> init-only
    - rbd mirror pool enable <pool>/<namespace> image --remote-namespace ' '
    Enable default namespace image mode mirroring on cluster-2.
    - rbd mirror pool enable --pool <pool> image --remote-namespace <namespace>
4. Set up peering between the two clusters in two-way mode.
5. Verify mirroring is configured successfully on both clusters.
6. Create an image in the namespace on cluster-1.
7. Enable snapshot-based mirroring for the image.
8. Wait for mirroring status: primary image should be "up+stopped", secondary "up+replaying".
9. Write data to the image on cluster-1 (primary).
10. Demote the primary image on cluster-1 (triggers snapshot and mirroring).
11. Wait for mirroring status: both images should be "up+unknown".
12. Verify data consistency between primary and secondary images.
13. Promote the primary image again on cluster-1.
14. Wait for mirroring status: primary "up+stopped", secondary "up+replaying".
15. Add a snapshot schedule for the primary image at the configured level (pool/namespace/image).
16. Run IO on the primary image.
17. Wait for the snapshot schedule interval.
18. Verify the snapshot schedule is present.
19. Verify data consistency between primary and secondary images.
20. Remove the snapshot schedule from the primary image.
21. Demote the primary image on cluster-1.
22. Wait for the snapshot schedule interval.
23. Promote the secondary image on cluster-2.
24. Wait for mirroring status: secondary "up+stopped", primary "up+replaying".
25. Add a snapshot schedule for the promoted image on cluster-2.
26. Run IO on the promoted image.
27. Wait for the snapshot schedule interval.
28. Verify the snapshot schedule is present on the promoted image.
29. Verify data consistency between promoted and demoted images.
30. Remove the snapshot schedule from the secondary image.
31. Repeate the above steps for the EC pool
32. Cleanup: remove images, namespaces, pools, and perform disk cleanup.

CEPH-83613956:
1. Create pools on both clusters.
2. Create namespaces (e.g., ns1_p) in the pool on cluster-1.
3. Enable non-default namespace mirroring with "init-only" mode on cluster-1.
    - rbd mirror pool enable --pool <pool> init-only
    - rbd mirror pool enable <pool>/<namespace> image --remote-namespace ' '
    Enable default namespace image mode mirroring on cluster-2.
    - rbd mirror pool enable --pool <pool> image --remote-namespace <namespace>
4. Set up peering between the two clusters in two-way mode.
5. Verify mirroring is configured successfully on both clusters.
6. Create an image in the namespace on cluster-1.
7. Enable snapshot-based mirroring for the image.
8. Wait for mirroring status: primary image should be "up+stopped", secondary "up+replaying".
9. Add snapshot schedule for the primary image at the configured level (pool/namespace/image).
10. Write data to the image on cluster-1 (primary).
11. Wait till snapshot schedule interval set like above
12. Verify the snapshot gets generated as per the schedule along with data consistency.
13. Abruptly make the primary cluster down for non-orderly shutdown.
14. stop all client IOs on cluster-1.
15. on cluster-2 force promote the secondary image.
16. verify the failover by checking mirroring status on cluster-2.
17. perform IO on the promoted image on cluster-2.
18. Get the cluster-1 back online
19. cluster-1 make secondary by demoting the primary image.
20. perform resync on cluster-1 to get the data from cluster-2.
21. add snapshot schedule for the promoted image on cluster-2.
22. run IO on the promoted image.
23. verify snapshot gets generated as per the schedule along with data consistency.
24. Remove the snapshot schedule from the secondary image.
25. Repeate the above steps for the EC pool
26. Cleanup: remove images, namespaces, pools, and perform disk cleanup.
"""

import random
import time

from ceph.rbd.initial_config import initial_mirror_config
from ceph.rbd.utils import check_data_integrity, getdict, random_string
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.krbd_io_handler import krbd_io_handler
from ceph.rbd.workflows.rbd_mirror import enable_image_mirroring, wait_for_status
from ceph.rbd.workflows.snap_scheduling import (
    add_snapshot_scheduling,
    remove_snapshot_scheduling,
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
                # Scenario: Data written on site-A, not yet mirrored to site-B (snapshot not taken)
                # Demote site-A, verify data on site-B matches site-A (demote triggers snapshot)

                # Write data to the image on site-A (primary)
                extra_io_config = {
                    "size": "100M",
                    "do_not_create_image": True,
                    "num_jobs": 1,
                    "iodepth": 4,
                    "rwmixread": 100,
                    "direct": 1,
                    "invalidate": 1,
                    "rbd_obj": rbd_primary,
                    "client": client_primary,
                    "config": {
                        "file_size": "100M",
                        "file_path": [f"/mnt/mnt_{random_string(len=5)}/extra_file"],
                        "get_time_taken": True,
                        "operations": {
                            "fs": "ext4",
                            "io": True,
                            "mount": True,
                            "map": True,
                        },
                        "cmd_timeout": 1200,
                        "io_type": "write",
                        "image_spec": [pri_image_spec],
                    },
                }
                io, err = krbd_io_handler(**extra_io_config)
                if err:
                    raise Exception(
                        "IO before demote failed for %s: %s" % (pri_image_spec, err)
                    )
                log.info("IO written to %s before demote" % pri_image_spec)

                # Demote the primary image (this should trigger a snapshot and mirroring)
                out, err = rbd_primary.mirror.image.demote(
                    **{"image-spec": pri_image_spec}
                )
                if err:
                    raise Exception(
                        "Demote image %s failed with error %s" % (pri_image_spec, err)
                    )
                log.info("Demoted image %s successfully" % pri_image_spec)

                # Wait for mirroring to complete on secondary
                wait_for_status(
                    rbd=rbd_primary,
                    cluster_name=pri_config["cluster"].name,
                    imagespec=pri_image_spec,
                    state_pattern="up+unknown",
                )
                wait_for_status(
                    rbd=rbd_secondary,
                    cluster_name=sec_config["cluster"].name,
                    imagespec=sec_image_spec,
                    state_pattern="up+unknown",
                )

                # Verify data consistency between site-A and site-B after demote
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
                    raise Exception(
                        "Data integrity check failed after demote for %s and %s"
                        % (pri_image_spec, sec_image_spec)
                    )
                log.info(
                    "Data is consistent between site-A and site-B after demote for %s"
                    % pri_image_spec
                )

                # promote the primary image again to proceed with further steps
                out, err = rbd_primary.mirror.image.promote(
                    **{"image-spec": pri_image_spec}
                )
                if err:
                    raise Exception(
                        "Promote image %s failed with error %s" % (pri_image_spec, err)
                    )
                log.info("Promoted image %s successfully" % pri_image_spec)

                # check for appropriate mirror status
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

                snap_levels = image_config_val.get("snap_schedule_levels")
                snap_intervals = image_config_val.get("snap_schedule_intervals")

                if snap_levels and snap_intervals:
                    level = snap_levels[0]
                    interval = snap_intervals[0]

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

                bench_kw = {
                    "image-spec": pri_image_spec,
                    "io-type": "write",
                    "io-total": "200M",
                    "io-threads": 16,
                }

                out, err = rbd_secondary.bench(**bench_kw)

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
                    if namespace:
                        verify_args["namespace"] = namespace
                    verify_args["image"] = image
                if verify_snapshot_schedule(**verify_args):
                    raise Exception(
                        "Snapshot schedule verification failed at {} level for {} "
                        "with interval: {}".format(level, image, interval)
                    )
                log.info(
                    "Snapshot schedule verified for %s at %s level"
                    % (pri_image_spec, level)
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
                    log.error(f"Failed to remove snapshot schedule on primary: {err}")

                log.info(f"Demoting primary image {pri_image_spec} on Cluster-1")
                out, err = rbd_primary.mirror.image.demote(
                    **{"image-spec": pri_image_spec}
                )
                if err:
                    raise Exception(
                        "Demote image %s failed with error %s" % (pri_image_spec, err)
                    )
                log.info("Demoted image %s successfully" % pri_image_spec)

                time.sleep(
                    int(image_config_val["snap_schedule_intervals"][-1][:-1]) * 120
                )

                log.info("Promoting secondary image %s on Cluster-2" % sec_image_spec)
                out, err = rbd_secondary.mirror.image.promote(
                    **{"image-spec": sec_image_spec}
                )
                if err:
                    raise Exception(
                        "Promote image %s failed with error %s" % (sec_image_spec, err)
                    )
                log.info("Promoted image %s successfully" % sec_image_spec)

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

                # Handle case when level is 'namespace' but no namespace is defined (i.e., default ns)
                effective_level = (
                    "image" if level == "namespace" and not remote_namespace else level
                )

                out, err = add_snapshot_scheduling(
                    rbd_secondary,
                    pool=pool,
                    image=image,
                    level=effective_level,
                    interval=interval,
                    namespace=remote_namespace,
                )
                if err:
                    raise Exception(
                        "Adding snapshot schedule failed with error %s" % err
                    )

                # Retain original level for remaining tests
                level = effective_level

                log.info(
                    "Snapshot schedule added for the promoted image %s" % sec_image_spec
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
                        "Failed to write IO to the image %s: %s" % (sec_image_spec, err)
                    )
                else:
                    log.info("Successfully ran IO on image %s" % sec_image_spec)

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
                    if remote_namespace:
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
                    log.error(f"Failed to remove snapshot schedule on secondary: {err}")

                log.info(
                    f"Snapshot schedules removed for {pri_image_spec} and {sec_image_spec}"
                )
        namespace_mirror_type = rbd_config.get("namespace_mirror_type")
        log.info(
            "Test passed for pool_type: {}, namespace_mirror_type: {}".format(
                pool_type, namespace_mirror_type
            )
        )
    return 0


def test_failover_non_orderly_shutdown(pri_config, sec_config, pool_types, **kw):
    """
    Test to verify failover non-orderly shutdown for namespace mirroring
    Args:
        pri_config: Primary cluster configuration
        sec_config: Secondary cluster configuration
        pool_types: Types of pools used in the test
        kw: Key/value pairs of configuration information to be used in the test
    """
    log.info("Starting CEPH-83613956 failover non-orderly shutdown test")

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

                snap_levels = image_config_val.get("snap_schedule_levels")
                snap_intervals = image_config_val.get("snap_schedule_intervals")

                if snap_levels and snap_intervals:
                    level = snap_levels[0]
                    interval = snap_intervals[0]

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

                # Write data to the image on site-A (primary) using krbd IO
                extra_io_config = {
                    "size": "100M",
                    "do_not_create_image": True,
                    "num_jobs": 1,
                    "iodepth": 4,
                    "rwmixread": 100,
                    "direct": 1,
                    "invalidate": 1,
                    "rbd_obj": rbd_primary,
                    "client": client_primary,
                    "config": {
                        "file_size": "100M",
                        "file_path": [f"/mnt/mnt_{random_string(len=5)}/extra_file"],
                        "get_time_taken": True,
                        "operations": {
                            "fs": "ext4",
                            "io": True,
                            "mount": True,
                            "map": True,
                        },
                        "cmd_timeout": 1200,
                        "io_type": "write",
                        "image_spec": [pri_image_spec],
                    },
                }
                io, err = krbd_io_handler(**extra_io_config)
                if err:
                    raise Exception(
                        "IO before failover (non-orderly shutdown) failed for %s: %s"
                        % (pri_image_spec, err)
                    )
                log.info(
                    "IO written to %s before failover (non-orderly shutdown)"
                    % pri_image_spec
                )

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
                    if namespace:
                        verify_args["namespace"] = namespace
                    verify_args["image"] = image
                if verify_snapshot_schedule(**verify_args):
                    raise Exception(
                        "Snapshot schedule verification failed at {} level for {} "
                        "with interval: {}".format(level, image, interval)
                    )
                log.info(
                    "Snapshot schedule verified for %s at %s level"
                    % (pri_image_spec, level)
                )
                # Verify data consistency between primary and secondary images
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
                    raise Exception(
                        "Data integrity check failed after failover for %s and %s"
                        % (pri_image_spec, sec_image_spec)
                    )
                log.info(
                    "Data is consistent between primary and secondary images for %s"
                    % pri_image_spec
                )

                # Remove snapshot schedule from primary
                out, err = remove_snapshot_scheduling(
                    rbd_primary,
                    pool=pool,
                    image=image,
                    level=level,
                    interval=interval,
                    namespace=namespace,
                )
                if err:
                    log.error(f"Failed to remove snapshot schedule on primary: {err}")
                log.info(f"Snapshot schedule removed for {pri_image_spec}")

                # Abruptly make the primary cluster down for non-orderly shutdown
                log.info(
                    "Simulating non-orderly shutdown: bringing down primary cluster (Cluster-1)"
                )

                # Get the rbd-mirror daemon node as target_host and stop the host
                ceph_cluster = pri_config["cluster"]
                mirror_nodes = ceph_cluster.get_ceph_objects(role="rbd-mirror")

                if not mirror_nodes:
                    raise Exception("No rbd-mirror nodes found")

                # Get CephNode from CephObject
                target_node = mirror_nodes[0].node
                vm_node = target_node.vm_node
                target_host = target_node.hostname

                log.info("Proceeding to shutdown rbd mirror host %s", target_host)
                vm_node.shutdown(wait=True)
                log.info("Successfully shut down rbd mirror host %s", target_host)
                time.sleep(10)
                log.info("Primary cluster rbd mirror node is down")

                # On cluster-2, force promote the secondary image
                log.info(
                    f"Force promoting secondary image {sec_image_spec} on Cluster-2"
                )
                out, err = rbd_secondary.mirror.image.promote(
                    **{"image-spec": sec_image_spec, "force": True}
                )
                if err:
                    raise Exception(
                        f"Force promote image {sec_image_spec} failed with error {err}"
                    )
                log.info(f"Force promoted image {sec_image_spec} successfully")

                # Verify the failover by checking mirroring status on cluster-2
                wait_for_status(
                    rbd=rbd_secondary,
                    cluster_name=sec_config["cluster"].name,
                    imagespec=sec_image_spec,
                    state_pattern="up+stopped",
                )

                # Perform IO on the promoted image on cluster-2
                bench_kw = {
                    "image-spec": sec_image_spec,
                    "io-type": "write",
                    "io-total": "200M",
                    "io-threads": 16,
                }
                out, err = rbd_secondary.bench(**bench_kw)
                if err:
                    raise Exception(
                        f"Failed to write IO to the promoted image {sec_image_spec}: {err}"
                    )
                else:
                    log.info(f"Successfully ran IO on promoted image {sec_image_spec}")

                # Get the primary cluster mirror daemon back online
                log.info("Bringing primary cluster (Cluster-1) back online")
                vm_node.power_on()
                log.info("Power on the rbd mirror host successfully")
                time.sleep(
                    int(image_config_val["snap_schedule_intervals"][-1][:-1]) * 120
                )
                log.info("Primary cluster rbd mirror node is online")

                # Make cluster-1 secondary by demoting the primary image
                out, err = rbd_primary.mirror.image.demote(
                    **{"image-spec": pri_image_spec}
                )
                if err:
                    raise Exception(
                        f"Demote image {pri_image_spec} failed with error {err}"
                    )
                log.info(f"Demoted image {pri_image_spec} successfully on Cluster-1")

                # Perform resync on cluster-1 to get the data from cluster-2
                out, err = rbd_primary.mirror.image.resync(
                    **{"image-spec": pri_image_spec}
                )
                if err:
                    raise Exception(
                        f"Resync image {pri_image_spec} failed with error {err}"
                    )
                log.info(f"Resync initiated for image {pri_image_spec} on Cluster-1")

                # Wait for a full resync of the freshly built image.
                time.sleep(
                    int(image_config_val["snap_schedule_intervals"][-1][:-1]) * 120
                )
                # check image status after resync
                wait_for_status(
                    rbd=rbd_primary,
                    cluster_name=pri_config["cluster"].name,
                    imagespec=pri_image_spec,
                    state_pattern="up+replaying",
                )
                # Add snapshot schedule for the promoted image on cluster-2
                effective_level = (
                    "image" if level == "namespace" and not remote_namespace else level
                )
                out, err = add_snapshot_scheduling(
                    rbd_secondary,
                    pool=pool,
                    image=image,
                    level=effective_level,
                    interval=interval,
                    namespace=remote_namespace,
                )
                if err:
                    raise Exception(f"Adding snapshot schedule failed with error {err}")
                level = effective_level  # Retain for subsequent steps

                log.info(
                    f"Snapshot schedule added for the promoted image {sec_image_spec}"
                )

                # Run IO on the promoted image
                bench_kw = {
                    "image-spec": sec_image_spec,
                    "io-type": "write",
                    "io-total": "200M",
                    "io-threads": 16,
                }
                out, err = rbd_secondary.bench(**bench_kw)
                if err:
                    raise Exception(
                        f"Failed to write IO to the promoted image {sec_image_spec}: {err}"
                    )
                else:
                    log.info(f"Successfully ran IO on promoted image {sec_image_spec}")

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
                    if remote_namespace:
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

                # Verify data consistency between promoted and demoted images
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
                    raise Exception(
                        "Data integrity check failed for promoted and demoted images: "
                        f"{sec_image_spec}, {pri_image_spec}"
                    )
                log.info(
                    f"Data is consistent between promoted and demoted images: {sec_image_spec}, {pri_image_spec}"
                )

                # Remove the snapshot schedule from the secondary image
                out, err = remove_snapshot_scheduling(
                    rbd_secondary,
                    pool=pool,
                    image=image,
                    level=level,
                    interval=interval,
                    namespace=remote_namespace,
                )
                if err:
                    log.error(f"Failed to remove snapshot schedule on secondary: {err}")

                log.info(
                    f"Snapshot schedule removed for promoted image {sec_image_spec}"
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
        # Random choices for namespace mirror type and snap schedule level
        namespace_mirror_options = ["default_to_non-default", "non-default_to_default"]
        snap_schedule_level_options = ["pool", "namespace", "image"]

        # Track which options have been used
        used_namespace_mirror_types = set()
        used_schedule_levels = set()

        for pool_type in ["rep_pool_config", "ec_pool_config"]:
            if pool_type in kw.get("config", {}):
                pool_config = kw["config"][pool_type]

                # Use namespace_mirror_type from YAML if provided, else pick randomly
                if "namespace_mirror_type" not in pool_config:
                    available_mirror_types = [
                        typ
                        for typ in namespace_mirror_options
                        if typ not in used_namespace_mirror_types
                    ]
                    if not available_mirror_types:
                        available_mirror_types = namespace_mirror_options.copy()
                        used_namespace_mirror_types.clear()

                    selected_mirror_type = random.choice(available_mirror_types)
                    pool_config["namespace_mirror_type"] = selected_mirror_type
                    used_namespace_mirror_types.add(selected_mirror_type)
                else:
                    selected_mirror_type = pool_config["namespace_mirror_type"]

                # Use snap_schedule_levels from YAML if provided, else pick randomly
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
                    "Selected namespace mirror type: %s and snap schedule level: %s for %s",
                    selected_mirror_type,
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
        test_map = {
            "CEPH-83613955": test_failover_orderly_shutdown,
            "CEPH-83613956": test_failover_non_orderly_shutdown,
            "CEPH-83601544": test_failover_orderly_shutdown,
            "CEPH-83601545": test_failover_non_orderly_shutdown,
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
