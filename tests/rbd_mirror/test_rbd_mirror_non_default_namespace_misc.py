"""
Module to Verify :
  - Disabling and re-enabling a namespace level mirroring and enable to a different namespace should fail

Pre-requisites :
1. Create two Ceph clusters with version 8.0 or later and set up mon, mgr, and osd services on each.
2. Deploy the rbd-mirror daemon on both clusters.
Test case covered:

CEPH-83601542:
1. Create a pool named pool1 on both clusters and initialize it
2. Create namespaces ns1_p and ns1_s in pool pool1 on cluster1 and cluster2 respectively
3. Enable mirroring on pool1/ns1_p and pool1/ns1_s in each cluster, linking to the other cluster’s namespace
4. Create an image image1 in pool1/ns1_p on cluster1, then enable mirroring
5. Disable mirroring for image1 on cluster1 namespace ns1_p
6. Verify the mirroring status for the pool1/ns1_p namespace
7. Verify the mirroring status for the pool1/ns1_s namespace on cluster2
8. Re-enable mirroring for image1 in pool1/ns1_p namespace on cluster1
9. Check mirroring status for the pool1/ns1_p namespace after re-enabling mirroring
10. Check mirroring status for the pool1/ns1_s namespace on cluster2 after re-enabling mirroring
11. Attempt to enable mirroring for ns1_p to a different remote namespace ns2_s on cluster2
and verify it fails.

"""

import json

from ceph.rbd.initial_config import initial_mirror_config
from ceph.rbd.utils import getdict, random_string
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.namespace import create_namespace_and_verify
from ceph.rbd.workflows.rbd_mirror import enable_image_mirroring, wait_for_status
from utility.log import Log

log = Log(__name__)


def test_toggle_enable_mirroring_and_diff_ns(pri_config, sec_config, pool_types, **kw):
    """
    Test Verify Disabling and re-enabling a namespace level mirroring
    Args:
        pri_config: Primary cluster configuration
        sec_config: Secondary cluster configuration
        pool_types: Types of pools used in the test
        kw: Key/value pairs of configuration information to be used in the test
    """
    log.info(
        "Starting test CEPH-83601542: Disabling and re-enabling a namespace level mirroring"
    )

    rbd_primary = pri_config.get("rbd")
    rbd_secondary = sec_config.get("rbd")
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

                out, err = rbd_primary.mirror.image.disable(
                    **{"pool": pool, "namespace": namespace, "image": image}
                )
                if "Mirroring disabled" in out:
                    log.info("Successfully disabled mirroring on site-A")

                out, err = rbd_primary.mirror.pool.status(
                    pool=pool, namespace=namespace, verbose=True, format="json"
                )
                data = json.loads(out)

                # Get required values
                if "OK" in data["summary"]["health"]:
                    log.info("Health status is OK after disabling mirror")
                else:
                    raise Exception("Health status is not OK after disabling mirroring")

                if data["images"]:
                    raise Exception(
                        "Images are still being mirrored after disabling mirroring"
                    )
                else:
                    log.info("Image stopped mirroring after disabling mirroring")

                out, err = rbd_secondary.mirror.pool.status(
                    pool=pool, namespace=remote_namespace, verbose=True, format="json"
                )
                if "OK" in data["summary"]["health"]:
                    log.info("Health status is OK after disabling mirror")
                else:
                    raise Exception("Health status is not OK after disabling mirroring")

                if data["images"]:
                    raise Exception(
                        "Images are still being mirrored after disabling mirroring"
                    )
                else:
                    log.info("Image stopped mirroring after disabling mirroring")

                enable_image_mirroring(
                    pri_config,
                    sec_config,
                    pool=pool,
                    image=image,
                    mirrormode="snapshot",
                    namespace=namespace,
                    remote_namespace=remote_namespace,
                )

                out, err = rbd_primary.mirror.pool.status(
                    pool=pool, namespace=namespace, verbose=True, format="json"
                )
                data = json.loads(out)

                # Get required values
                if "OK" in data["summary"]["health"]:
                    log.info("Health status is OK on primary after enabling mirror")
                else:
                    raise Exception(
                        "Health status is not OK on primary after enabling mirroring"
                    )

                if data["summary"]["image_states"].get("replaying") > 0:
                    log.info(
                        "Image being mirrored from primary after enabling mirroring"
                    )
                else:
                    raise Exception(
                        "Image not mirroring from primary after enabling mirroring"
                    )

                out, err = rbd_secondary.mirror.pool.status(
                    pool=pool, namespace=remote_namespace, verbose=True, format="json"
                )
                data = json.loads(out)

                if "OK" in data["summary"]["health"]:
                    log.info("Health status is OK on secondary after enabling mirror")
                else:
                    raise Exception(
                        "Health status is not OK on secondary after enabling mirroring"
                    )

                if data["summary"]["image_states"].get("replaying") > 0:
                    log.info(
                        "Image being mirrored on secondary after enabling mirroring"
                    )
                else:
                    raise Exception(
                        "Image no mirroring on secondary after enabling mirroring"
                    )
            remote_namespace_new = "namespace_" + random_string(len=3)
            rc = create_namespace_and_verify(
                **{
                    "pool-name": pool,
                    "namespace": remote_namespace_new,
                    "client": client_secondary,
                }
            )

            if rc == 0:
                log.info(
                    "New namespace %s created on site-B on pool %s",
                    remote_namespace_new,
                    pool,
                )
            else:
                raise Exception(
                    "Failed to create namespace %s in pool %s on site-B",
                    remote_namespace_new,
                    pool,
                )

            enable_diff_namespace = {
                "pool-spec": f"{pool}/{namespace}",
                "mode": pool_config["mode"],
                "remote-namespace": remote_namespace_new,
            }

            out, err = rbd_primary.mirror.pool.enable(**enable_diff_namespace)
            if not out and "failed to set the remote namespace" in err:
                log.info(
                    "Failed to enable namespace level mirroring for a different namespace on secondary"
                    " cluster as expected: \n %s",
                    err,
                )
            else:
                raise Exception(
                    "Namespace mirroring enabled for a different namespace on secondary cluster: %s",
                    out,
                )
            log.info(
                "Test passed: Verify Disabling and re-enabling a namespace level mirroring "
                "and enabling for different namespace"
            )
    return 0


def run(**kw):
    """
    Test to verify Disabling and re-enabling a namespace level mirroring and enable for different namespace should fail

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
            "CEPH-83601542": test_toggle_enable_mirroring_and_diff_ns,
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
