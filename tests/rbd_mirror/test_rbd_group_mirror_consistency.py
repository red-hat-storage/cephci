"""
Module to verify :
  -  Verify group level data consistency on secondary after primary site disaster

Test case covered:
CEPH-83613291 - Verify group level data consistency on secondary after primary site disaster

Pre-requisites :
1. Cluster must be up in 8.1 and above and running with capacity to create pool
2. We need atleast one client node with ceph-common package,
   conf and keyring files

Test Case Flow:
Step 1: Deploy Two ceph cluster on version 8.1 or above
Step 2: Create RBD pool ‘pool_1’ on both sites with/without namespace
Step 3: Enable Image mode mirroring on pool_1 on both sites
Step 4: Bootstrap the storage cluster peers (Two-way)
Step 5: Create 4 RBD images in pool_1, 2 with smaller size and 2 with large size
Step 6: Run Fio on all images to 20% capacity
Step 7: Calculate md5sum
Step 8: Create Consistency group
Step 9: Add Images in the consistency group
Step 10: Enable Mirroring for the group
Step 11: Validate md5sum should match on both clusters
Step 12: Write IO on remaining 80% of images
Step 13: Note down the md5sum of both the images on site-A.
step 14: Get the md5sum of images on site-B and verify they are still same as that of in step #10
Step 15: Take the mirror group snapshot manually
Step 16: Wait for the md5sum of the smaller image on site-B to be same as that of in step 13.
Step 17: Terminate the rbd-mirror daemon using ceph orch stop and then kill the process with SIGKILL.
Step 18: Force promote on site-B while the larger image in the group snapshot is still mirroring.
step 19: Get the md5sum of small and large image on siteB and compare it with that of step 7.
step 20: Repeat above on EC pool with or without namespace.
Step 21: Cleanup rbd test objects like pool, images, groups etc
"""

import random
import time
from copy import deepcopy

from ceph.rbd.initial_config import initial_mirror_config
from ceph.rbd.utils import exec_cmd, get_md5sum_rbd_image, getdict, random_string
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.group import add_image_to_group_and_verify
from ceph.rbd.workflows.group_mirror import (
    enable_group_mirroring_and_verify_state,
    wait_for_idle,
)
from ceph.rbd.workflows.namespace import enable_namespace_mirroring
from utility.log import Log

log = Log(__name__)


def test_mirror_group_consistency(
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
    Test group level data consistency on secondary after primary site disaster
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
        log.info("Running test CEPH-83613291  for %s", pool_type)

        for pool, pool_config in multi_pool_config.items():
            group_config = {}
            if "data_pool" in pool_config.keys():
                _ = pool_config.pop("data_pool")
            group_spec = pool_config.get("group-spec")
            group_config.update({"group-spec": group_spec})

            image_spec_small = []
            image_spec_large = []
            for image, image_config in pool_config.items():
                if "image" in image:
                    if "namespace" in pool_config:
                        pool_spec = pool + "/" + pool_config.get("namespace") + "/"
                    else:
                        pool_spec = pool + "/"
                    image_spec_small.append(pool_spec + image)
                    image_large_name = pool_spec + "image_" + random_string(len=4)
                    # Create large size images
                    rbd_primary.create(
                        **{"image-spec": image_large_name, "size": "10G"}
                    )
                    image_spec_large.append(image_large_name)
                    if add_image_to_group_and_verify(
                        **{
                            "group-spec": group_spec,
                            "image-spec": image_large_name,
                            "client": client_primary,
                        }
                    ):
                        raise Exception("Failed to add image to group %s", group_spec)

            if "namespace" in pool_config:
                enable_namespace_mirroring(
                    rbd_primary, rbd_secondary, pool, **pool_config
                )

            image_spec_all = []
            image_spec_all = image_spec_small + image_spec_large

            # Enable Group Mirroring and Verify
            enable_group_mirroring_and_verify_state(
                rbd_primary, **{"group-spec": group_spec}
            )

            # Wait for group mirroring to complete
            wait_for_idle(rbd_primary, **{"group-spec": group_spec})
            log.info(
                "Data replay state is idle for all images in the group. Syncing completed"
            )

            md5sum_first_sync_site_b = []
            for image in image_spec_all:
                md5sum_first_sync_site_b.append(
                    get_md5sum_rbd_image(
                        image_spec=image,
                        rbd=rbd_secondary,
                        client=client_secondary,
                        file_path="file" + random_string(len=5),
                    )
                )
            log.info("md5sums on site B after first sync: %s", md5sum_first_sync_site_b)

            bench_kw = {}
            io_small_cfg = kw.get("config", {}).get("io_small", {})
            bench_kw.update(
                {
                    "io-type": io_small_cfg.get("io-type", "write"),
                    "io-total": io_small_cfg.get("io-size_init", "20M"),
                    "io-threads": io_small_cfg.get("io-threads", "16"),
                }
            )

            for image_spec in image_spec_small:
                bench_kw.update({"image-spec": image_spec})
                out, err = rbd_primary.bench(**bench_kw)

            io_large_cfg = kw.get("config", {}).get("io_large", {})
            bench_kw.update({"io-total": io_large_cfg.get("io-size_init", "1G")})

            for image_spec in image_spec_large:
                bench_kw.update({"image-spec": image_spec})
                out, err = rbd_primary.bench(**bench_kw)
                if err:
                    raise Exception("Failed to write IO to the image %s", image_spec)

            # Create first manual mirror group snapshot
            out, err = rbd_primary.mirror.group.snapshot.add(
                **{"group-spec": group_spec}
            )
            if err:
                raise Exception("Failed to add manual mirror group snapshot %s", out)

            # Wait for snapshot to sync on site-B
            wait_for_idle(rbd_primary, **{"group-spec": group_spec})
            log.info(
                "Data replay state is idle for all images in the group. Syncing completed"
            )

            md5sum_second_sync_site_b = []
            for image in image_spec_all:
                md5sum_second_sync_site_b.append(
                    get_md5sum_rbd_image(
                        image_spec=image,
                        rbd=rbd_secondary,
                        client=client_secondary,
                        file_path="file" + random_string(len=5),
                    )
                )
            log.info(
                "md5sums on site B after second sync: %s", md5sum_second_sync_site_b
            )

            bench_kw.update({"io-total": io_small_cfg.get("io-size", "40M")})

            for image_spec in image_spec_small:
                bench_kw.update({"image-spec": image_spec})
                out, err = rbd_primary.bench(**bench_kw)

            bench_kw.update({"io-total": io_large_cfg.get("io-size", "9G")})

            for image_spec in image_spec_large:
                bench_kw.update({"image-spec": image_spec})
                out, err = rbd_primary.bench(**bench_kw)
                if err:
                    raise Exception("Failed to write IO to the image %s", image_spec)

            md5sum_second_write_site_a = []
            for image in image_spec_all:
                md5sum_second_write_site_a.append(
                    get_md5sum_rbd_image(
                        image_spec=image,
                        rbd=rbd_primary,
                        client=client_primary,
                        file_path="file" + random_string(len=5),
                    )
                )
            log.info(
                "md5sums on site A after second write: %s", md5sum_second_write_site_a
            )

            # Create second manual mirror group snapshot
            out, err = rbd_primary.mirror.group.snapshot.add(
                **{"group-spec": group_spec}
            )
            if err:
                raise Exception("Failed to add manual mirror group snapshot %s", out)

            retry = 0
            while retry < 10:
                time.sleep(2)
                md5sum_third_sync_site_b = []
                for image in image_spec_all[0:2]:
                    md5sum_third_sync_site_b.append(
                        get_md5sum_rbd_image(
                            image_spec=image,
                            rbd=rbd_secondary,
                            client=client_secondary,
                            file_path="file" + random_string(len=5),
                        )
                    )
                log.info(
                    "md5sums of small size images on site-B after sync : %s",
                    md5sum_third_sync_site_b,
                )
                log.info(
                    "md5sums of small images on site-A after second write : %s",
                    md5sum_second_write_site_a[0:2],
                )

                if md5sum_third_sync_site_b == md5sum_second_write_site_a[0:2]:
                    if exec_cmd(
                        node=client_secondary,
                        cmd="ceph orch stop rbd-mirror",
                    ):
                        raise Exception("Failed to stop rbd-mirror daemon on site-B")
                    log.info("Stopped rbd-mirror daemon on site-B before force promote")
                    mirror_node = secondary_cluster.get_nodes(role="rbd-mirror")[0]
                    service_name = exec_cmd(
                        node=mirror_node,
                        cmd="systemctl list-units --all | grep rbd-mirror | grep -Ev \\.target | awk {{'print $1'}}",
                        output=True,
                    )
                    # "ceph orch stop" shuts the daemon gracefully. Since, we need to stop the syncing abruptly.
                    # we need to abruptly terminate the daemon. Hence, SIGKILL is passed
                    # to systemctl as below.
                    exec_cmd(
                        node=mirror_node,
                        cmd=f"systemctl kill --signal=SIGKILL {service_name}",
                    )
                    log.info("Force killed rbd-mirror daemon before force promote")
                    time.sleep(40)
                    break
                else:
                    time.sleep(1)
                    retry = retry + 1

                if retry == 10:
                    raise Exception(
                        "Small images on site-B failed to sync with those on site-A"
                    )
            log.info("Small images on site-B synced with those on site-A")

            (out, err) = rbd_secondary.mirror.group.promote(
                **{"group-spec": group_spec, "force": True}
            )
            if err:
                raise Exception("Failed to force promote group on site-B: " + str(err))

            md5sum_after_force_promote_site_b = []
            for image in image_spec_all:
                md5sum_after_force_promote_site_b.append(
                    get_md5sum_rbd_image(
                        image_spec=image,
                        rbd=rbd_secondary,
                        client=client_secondary,
                        file_path="file" + random_string(len=5),
                    )
                )
            log.info(
                "md5sums of images on site B after force promote on site-B: %s",
                md5sum_after_force_promote_site_b,
            )

            # Due to existing BZ Bug 2363632 on 8.1, the force promote as of now
            # rollsback the images to the n-1 group snapshot. Hence,
            # md5sum_after_force_promote_site_b is compared with md5sum_first_sync_site_b
            # as of now. This BZ is planned to be fixed in 8.1z releases. So, in 8.1z
            # md5sum_after_force_promote_site_b should be compared with md5sum_second_sync_site_b.
            # the below line should look something like :
            # if md5sum_after_force_promote_site_b != md5sum_second_sync_site_b:
            if md5sum_after_force_promote_site_b != md5sum_first_sync_site_b:
                raise Exception(
                    "md5sums after force promote are not consistent with previous snapshot \n"
                    f"site-B: {md5sum_after_force_promote_site_b} \n"
                    f"site-B: {md5sum_first_sync_site_b}"
                )
            log.info(
                "Successfully verified that site-B data rollsback to a previous consistent "
                "snapshot after primary disaster"
            )
            exec_cmd(
                node=client_secondary,
                cmd="ceph orch start rbd-mirror",
            )
            log.info("Restored rbd-mirror daemon on site-B")
            time.sleep(5)


def run(**kw):
    """
    This test verifies group level data consistency on secondary after primary site disaster
    Args:
        kw: test data
    Returns:
        int: The return value. 0 for success, 1 otherwise

    """
    try:
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
        test_mirror_group_consistency(
            rbd_primary,
            rbd_secondary,
            client_primary,
            client_secondary,
            primary_cluster,
            secondary_cluster,
            pool_types,
            **kw,
        )
        log.info(
            "Test verifying group level data consistency on secondary after primary site disaster passed"
        )
    except Exception as e:
        log.error(
            "Test verifying group level data consistency on secondary after primary site disaster failed: "
            + str(e)
        )
        return 1

    finally:
        exec_cmd(
            node=client_secondary,
            cmd="ceph orch start rbd-mirror",
        )
        cleanup(pool_types=pool_types, multi_cluster_obj=mirror_obj, **kw)

    return 0
