"""
Module to verify Scale testing for namespace-level mirrored images

Test case covered -
CEPH-83601547 - Scale testing for namespace-level mirrored images

Pre-requisites :
1. Two ceph clusters version 8.0 or later with mon,mgr,osd
2. Deploy rbd-mirror daemon service on both clusters

Test Case Flow:
Test case covered -

CEPH-83601547:
1. Set up two Ceph clusters (cluster1 and cluster2) with version 8.0 or later
2. Deploy the rbd-mirror daemon on both clusters.
3. rbd-mirror daemon is up and running on both clusters.
4. Create and initialize a pool named pool1 on both clusters
5. Enable mirroring for pool1 at the image level on both clusters
6. Set up peering between the two clusters in two-way mode
7. Create multiple namespaces in pool1 on both clusters (e.g., ns1, ns2, ..., nsN).
Repeat for each namespace nsX:
On cluster1: rbd namespace create pool1/nsX
On cluster2: rbd namespace create pool1/nsX_remote
8. Enable mirroring for each namespace between clusters
9. Create multiple images in each namespace on cluster1 (e.g., image1, image2, ..., imageM)
and enable mirroring for each image.
10. Verify mirroring status for all images in each namespace on both clusters
11. Perform I/O operations on all primary images in cluster1 to validate mirroring under scale
12. Verify data consistency by comparing checksums between mirrored images in cluster1 and cluster2
"""

import random

from ceph.rbd.initial_config import initial_mirror_config
from ceph.rbd.utils import getdict, random_string
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.krbd_io_handler import krbd_io_handler
from ceph.rbd.workflows.namespace import enable_namespace_mirroring
from ceph.rbd.workflows.rbd_mirror import enable_image_mirroring, wait_for_status
from utility.log import Log

log = Log(__name__)


def test_namespace_scale(pri_config, sec_config, pool_types, **kw):
    log.info(
        "Starting CEPH-83601547 - Scale testing for namespace-level mirrored images"
    )
    rbd_primary = pri_config.get("rbd")
    rbd_secondary = sec_config.get("rbd")
    client_primary = pri_config.get("client")
    pool_type = random.choice(pool_types)

    rbd_config = kw.get("config", {}).get(pool_type, {})
    multi_pool_config = getdict(rbd_config)

    for pool, pool_config in multi_pool_config.items():
        multi_image_config = getdict(pool_config)
        image_config = {k: v for k, v in multi_image_config.items()}

        scale_option = ["scale_image", "scale_namespace"]
        option = random.choice(scale_option)
        log.info("Selected option for scale is " + option)
        if (
            option == "scale_image"
        ):  # 100 images in a namespace for non_default_to_non_default mirroring
            namespace = pool_config.get("namespace")
            remote_namespace = pool_config.get("remote_namespace")

            # Create 99 more images in the namespace
            tmp = {}
            for i in range(2, 101):
                image_size = kw.get("config", {}).get(pool_type, {}).get("size")
                image_name = "image" + str(i)
                new_image_spec = f"{pool}/{namespace}/{image_name}"
                image_create_status, err = rbd_primary.create(
                    **{"image-spec": new_image_spec, "size": image_size}
                )
                if err:
                    raise Exception(
                        "Failed in creating image: " + new_image_spec + ", err: " + err
                    )
                log.info(
                    "Successfully created image"
                    + image_name
                    + " for mirroring: "
                    + image_create_status
                )
                tmp["size"] = image_size
                image_config[image_name] = tmp

            for image, image_config_val in image_config.items():
                fio = kw.get("config", {}).get("fio", {})
                scale(
                    pool,
                    namespace,
                    remote_namespace,
                    image,
                    fio,
                    client_primary,
                    pri_config,
                    sec_config,
                    rbd_secondary=rbd_secondary,
                    rbd_primary=rbd_primary,
                )

        else:
            # Create 99 namespace on both clusters with 1 image each
            for i in range(2, 101):
                # Create Namespace on Primary Cluster
                namespace_name = "Namespace" + str(i)
                ns_create_kw = {}
                ns_create_kw["namespace"] = namespace_name
                ns_create_kw["pool-name"] = pool
                _, ns_create_err = rbd_primary.namespace.create(**ns_create_kw)
                if not ns_create_err:
                    log.info(
                        f"SUCCESS: Namespace {namespace_name} got created in pool {pool} "
                    )
                else:
                    log.error(
                        f"FAIL: Namespace {namespace_name} creation failed in pool {pool} \
                        with error {ns_create_err}"
                    )
                    return 1
                # Create remote-namespace on Secondary cluster
                remote_namespace_name = "Namespace_remote" + str(i)
                ns_create_kw = {}
                ns_create_kw["namespace"] = remote_namespace_name
                ns_create_kw["pool-name"] = pool
                _, ns_create_err = rbd_secondary.namespace.create(**ns_create_kw)
                if not ns_create_err:
                    log.info(
                        f"SUCCESS: Namespace {remote_namespace_name} got created in pool {pool}"
                    )
                else:
                    log.error(
                        f"FAIL: Namespace {remote_namespace_name} creation failed in pool {pool} \
                        with error {ns_create_err}"
                    )
                    return 1

                # Enable Namespace level mirroring
                namespace_config = {
                    "mode": kw.get("config", {}).get(pool_type, {}).get("mode"),
                    "namespace": namespace_name,
                    "remote_namespace": remote_namespace_name,
                }
                enable_namespace_mirroring(
                    rbd_primary, rbd_secondary, pool, **namespace_config
                )

                # Create One image in each namespace
                image_size = kw.get("config", {}).get(pool_type, {}).get("size")
                image_name = "image" + str(i)
                new_image_spec = f"{pool}/{namespace_name}/{image_name}"
                image_create_status, err = rbd_primary.create(
                    **{"image-spec": new_image_spec, "size": image_size}
                )
                if err:
                    raise Exception(
                        "Failed in creating image: " + new_image_spec + ", err: " + err
                    )
                log.info(
                    "Successfully created image for mirroring: "
                    + str(image_create_status)
                )

                fio = kw.get("config", {}).get("fio", {})
                scale(
                    pool,
                    namespace_name,
                    remote_namespace_name,
                    image_name,
                    fio,
                    client_primary,
                    pri_config,
                    sec_config,
                    rbd_secondary=rbd_secondary,
                    rbd_primary=rbd_primary,
                )

    log.info("Successfully passed Scale for namespace-level mirrored images")

    return 0


def scale(
    pool,
    namespace,
    remote_namespace,
    image,
    fio,
    client_primary,
    pri_config,
    sec_config,
    rbd_secondary,
    rbd_primary,
):
    pri_image_spec = f"{pool}/{namespace}/{image}"
    sec_image_spec = f"{pool}/{remote_namespace}/{image}"
    # Write data on the primary image
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
    io, err = krbd_io_handler(**io_config)
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


def run(**kw):
    """
    Test Scale for namespace-level mirrored images
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
        test_namespace_scale(pri_config, sec_config, pool_types, **kw)

    except Exception as e:
        log.error(
            f"Test Scale for namespace-level mirrored images failed with error {str(e)}"
        )
        return 1
    finally:
        cleanup(pool_types=pool_types, multi_cluster_obj=mirror_obj, **kw)
    return 0
