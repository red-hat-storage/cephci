"""
Module to verify :
  -  Verify failover failback during orderly and non-orderly shutdown

Test case covered:
CEPH-83613277 - Verify failover failback during orderly and non-orderly shutdown

Pre-requisites :
1. Cluster must be up in 8.1 and above and running with capacity to create pool
2. We need atleast one client node with ceph-common package,
   conf and keyring files

Test Case Flow:
Step 1: Deploy Two ceph cluster on version 8.1 or above
Step 2: Create RBD pool ‘pool_1’ on both sites with/without namespace
Step 3: Enable Image mode mirroring on pool_1 on both sites
Step 4: Bootstrap the storage cluster peers (Two-way)
Step 5: Create 2 RBD images in pool_1
Step 6: Add data to the images on site-A using rbd bench or FIO
Step 7: Calculate MD5sum of all files
Step 8: Create Consistency group
Step 9: Add Images in the consistency group
Step 10: Enable Mirroring for the group
Step 11: Wait for replication to complete and check consistency
Step 12: Perform group demote on site-a and group promote on site-b
Step 13: Verify status. Verify md5sum on site-B is same as that of site-A.
step 14: Add more data to site-b images
Step 15: Calculate md5sum, it should not match with Step #7
Step 16: demote group on site-b
Step 17: promote group on site-a
step 18: Calculate MD5sum of site-a.
step 19:  md5sum of site-a should match with that of md5sum of site-b
Step 20: force promote on site-b
Step 21: Add more data to site-b images
Step 22: Md5sum should not match with step #20 md5 hash of site-a
Step 23: demote site-b
Step 24: Perform resync
Step 25: Validate md5sum should match both clusters
Step 26: Verify group mirror status
Step 27: Repeat above on EC pool with or without namespace
Step 28: Cleanup the images, file and pools
"""

import random
import time
from copy import deepcopy

from ceph.rbd.initial_config import initial_mirror_config
from ceph.rbd.utils import get_md5sum_rbd_image, getdict, random_string
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.group_mirror import (
    enable_group_mirroring_and_verify_state,
    group_mirror_status_verify,
    wait_for_idle,
)
from ceph.rbd.workflows.krbd_io_handler import krbd_io_handler
from ceph.rbd.workflows.namespace import enable_namespace_mirroring
from utility.log import Log

log = Log(__name__)


def test_group_mirroring_failover(
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
    Test failover failback during orderly and non-orderly shutdown (without namespace)
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
        log.info("Running test CEPH-83613277 for %s", pool_type)
        # FIO Params Required for ODF workload exclusively in group mirroring
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

        for pool, pool_config in multi_pool_config.items():
            group_config = {}
            if "data_pool" in pool_config.keys():
                _ = pool_config.pop("data_pool")
            group_spec = pool_config.get("group-spec")
            group_config.update({"group-spec": group_spec})
            md5sum_before_mirror_site_a = []
            image_spec = []
            for image, image_config in pool_config.items():
                if "image" in image:
                    if "namespace" in pool_config:
                        image_spec.append(
                            pool + "/" + pool_config.get("namespace") + "/" + image
                        )
                        enable_namespace_mirroring(
                            rbd_primary, rbd_secondary, pool, **pool_config
                        )
                    else:
                        image_spec.append(pool + "/" + image)
            image_spec_copy = deepcopy(image_spec)
            io_config["rbd_obj"] = rbd_primary
            io_config["client"] = client_primary
            io_config["config"]["image_spec"] = image_spec_copy
            io, err = krbd_io_handler(**io_config)
            if err:
                raise Exception("Map, mount and run IOs failed for " + str(image_spec))
            else:
                log.info("Map, mount and IOs successful for " + str(image_spec))
            # 7: Calculate MD5sum of all files
            for image in image_spec:
                md5sum_before_mirror_site_a.append(
                    get_md5sum_rbd_image(
                        image_spec=image,
                        rbd=rbd_primary,
                        client=client_primary,
                        file_path="file" + random_string(len=5),
                    )
                )

            # Enable Group Mirroring and Verify
            enable_group_mirroring_and_verify_state(
                rbd_primary, **{"group-spec": group_spec}
            )

            # Wait for group mirroring to complete
            wait_for_idle(rbd_primary, **{"group-spec": group_spec})
            log.info("Data replay state is idle for all images in the group")

            # Verify group mirroring status on both clusters
            group_mirror_status_verify(
                primary_cluster,
                secondary_cluster,
                rbd_primary,
                rbd_secondary,
                primary_state="up+stopped",
                secondary_state="up+replaying",
                **group_config,
            )
            log.info(
                "Group states reached 'up+stopped' on site-A and 'up+replaying' on site-B"
            )
            md5sum_after_mirror_site_b = []
            for image in image_spec:
                md5sum_after_mirror_site_b.append(
                    get_md5sum_rbd_image(
                        image_spec=image,
                        rbd=rbd_secondary,
                        client=client_secondary,
                        file_path="file" + random_string(len=5),
                    )
                )
            if md5sum_before_mirror_site_a != md5sum_after_mirror_site_b:
                raise Exception(
                    "md5sums are not same. \n"
                    f"site-A: {md5sum_before_mirror_site_a} \n"
                    f"site-B: {md5sum_after_mirror_site_b}"
                )

            # 17: Perform group demote on site-a and group promote on site-b
            out, err = rbd_primary.mirror.group.demote(**{"group-spec": group_spec})
            if err:
                raise Exception("Failed to demote group on site-A: " + str(err))
            log.info("Demoted " + group_spec + " on site-A ")

            # Verify group mirroring status on both clusters
            group_mirror_status_verify(
                primary_cluster,
                secondary_cluster,
                rbd_primary,
                rbd_secondary,
                primary_state="up+unknown",
                secondary_state="up+unknown",
                **group_config,
            )
            log.info(
                "Group states reached 'up+unknown' on site-A and 'up+unknown' on site-B"
            )

            out, err = rbd_secondary.mirror.group.promote(**{"group-spec": group_spec})
            if err:
                raise Exception("Failed to promote group on site-B: " + str(err))
            log.info("Promoted " + group_spec + " on site-B ")

            # Verify group mirroring status on both clusters
            group_mirror_status_verify(
                primary_cluster,
                secondary_cluster,
                rbd_primary,
                rbd_secondary,
                primary_state="up+replaying",
                secondary_state="up+stopped",
                **group_config,
            )
            log.info(
                "Group states reached 'up+replaying' on site-A and 'up+stopped' on site-B"
            )
            wait_for_idle(rbd_secondary, **{"group-spec": group_spec})
            log.info("Data replay state is idle for all images in the group")

            md5sum_after_promote_site_b = []
            for image in image_spec:
                md5sum_after_promote_site_b.append(
                    get_md5sum_rbd_image(
                        image_spec=image,
                        rbd=rbd_secondary,
                        client=client_secondary,
                        file_path="file" + random_string(len=5),
                    )
                )
            if md5sum_before_mirror_site_a != md5sum_after_promote_site_b:
                raise Exception(
                    "md5sums are not same. \n"
                    f"site-A: {md5sum_before_mirror_site_a} \n"
                    f"site-B: {md5sum_after_promote_site_b}"
                )

            # 19: Add more data to site-b images
            io_config["rbd_obj"] = rbd_secondary
            io_config["client"] = client_secondary
            image_spec_copy = deepcopy(image_spec)
            io_config["config"]["image_spec"] = image_spec_copy
            io, err = krbd_io_handler(**io_config)
            if err:
                raise Exception("Map, mount and run IOs failed for " + str(image_spec))
            else:
                log.info("Map, mount and IOs successful for " + str(image_spec))

            md5sum_after_failover_site_b = []
            for image in image_spec:
                md5sum_after_failover_site_b.append(
                    get_md5sum_rbd_image(
                        image_spec=image,
                        rbd=rbd_secondary,
                        client=client_secondary,
                        file_path="file" + random_string(len=5),
                    )
                )

            if md5sum_before_mirror_site_a == md5sum_after_failover_site_b:
                raise Exception(
                    f"md5sums on site-A and site-B are still same after writing data on site-B. \n"
                    f"site-A :{md5sum_before_mirror_site_a} \n"
                    f"site-B : {md5sum_after_failover_site_b}"
                )
            # 21: demote group on site-b and promote group on site-a
            out, err = rbd_secondary.mirror.group.demote(**{"group-spec": group_spec})
            if err:
                raise Exception("Failed to demote group on site-B " + str(err))
            log.info("Demoted " + group_spec + " on site-B ")

            # Verify group mirroring status on both clusters
            group_mirror_status_verify(
                primary_cluster,
                secondary_cluster,
                rbd_primary,
                rbd_secondary,
                primary_state="up+unknown",
                secondary_state="up+unknown",
                **group_config,
            )
            log.info(
                "Group states reached 'up+unknown' on site-A and 'up+unknown' on site-B"
            )
            out, err = rbd_primary.mirror.group.promote(**{"group-spec": group_spec})
            if err:
                raise Exception("Failed to promote group on site-A " + str(err))
            log.info("Promoted " + group_spec + " on site-A ")

            # Verify group mirroring status on both clusters
            group_mirror_status_verify(
                primary_cluster,
                secondary_cluster,
                rbd_primary,
                rbd_secondary,
                primary_state="up+stopped",
                secondary_state="up+replaying",
                **group_config,
            )
            log.info(
                "Group states reached 'up+stopped' on site-A and 'up+replaying' on site-B"
            )
            wait_for_idle(rbd_primary, **{"group-spec": group_spec})
            log.info("Data replay state is idle for all images in the group")
            md5sum_before_force_failover_site_a = []
            for image in image_spec:
                md5sum_before_force_failover_site_a.append(
                    get_md5sum_rbd_image(
                        image_spec=image,
                        rbd=rbd_primary,
                        client=client_primary,
                        file_path="file" + random_string(len=5),
                    )
                )
            # 25: force promote on site-b
            out, err = rbd_secondary.mirror.group.promote(
                **{"group-spec": group_spec, "force": True}
            )
            if err:
                raise Exception("Failed to force promote group on site-B " + str(err))
            log.info("Force Promoted " + group_spec + " on site-B ")

            group_mirror_status_verify(
                primary_cluster,
                secondary_cluster,
                rbd_primary,
                rbd_secondary,
                primary_state="up+stopped",
                secondary_state="up+stopped",
                **group_config,
            )
            log.info(
                "Group states reached 'up+stopped' on site-A and 'up+stopped' on site-B"
            )

            # 26: Add more data to site-b images
            io_config["rbd_obj"] = rbd_secondary
            io_config["client"] = client_secondary
            image_spec_copy = deepcopy(image_spec)
            io_config["config"]["image_spec"] = image_spec_copy
            io, err = krbd_io_handler(**io_config)
            if err:
                raise Exception("Map, mount and run IOs failed for " + str(image_spec))
            else:
                log.info("Map, mount and IOs successful for " + str(image_spec))

            # 28: demote site-b
            out, err = rbd_secondary.mirror.group.demote(**{"group-spec": group_spec})
            if err:
                raise Exception("Failed to demote group on site-B " + str(err))
            log.info("Demoted " + group_spec + " on site-B ")
            group_mirror_status_verify(
                primary_cluster,
                secondary_cluster,
                rbd_primary,
                rbd_secondary,
                primary_state="up+stopped",
                secondary_state="up+error",
                **group_config,
            )
            log.info(
                "Group states reached 'up+stopped' on site-A and 'up+error' on site-B"
            )

            md5sum_before_resync_site_b = []
            for image in image_spec:
                md5sum_before_resync_site_b.append(
                    get_md5sum_rbd_image(
                        image_spec=image,
                        rbd=rbd_secondary,
                        client=client_secondary,
                        file_path="file" + random_string(len=5),
                    )
                )

            # Perform resync
            out, err = rbd_secondary.mirror.group.resync(**{"group-spec": group_spec})
            if err:
                raise Exception("Failed to resync group on site-B " + str(err))
            log.info("Resync group done for " + group_spec + " on site-B ")
            # During resync the images on secondary will be deleted and created
            # so we wait for some time before the status is queried
            time.sleep(30)
            # Verify group mirroring status on both clusters
            group_mirror_status_verify(
                primary_cluster,
                secondary_cluster,
                rbd_primary,
                rbd_secondary,
                primary_state="up+stopped",
                secondary_state="up+replaying",
                **group_config,
            )
            log.info(
                "Group states reached 'up+stopped' on site-A and 'up+replaying' on site-B"
            )
            wait_for_idle(rbd_primary, **{"group-spec": group_spec})
            log.info("Data replay state is idle for all images in the group")
            md5sum_after_resync_site_b = []
            for image in image_spec:
                md5sum_after_resync_site_b.append(
                    get_md5sum_rbd_image(
                        image_spec=image,
                        rbd=rbd_secondary,
                        client=client_secondary,
                        file_path="file" + random_string(len=5),
                    )
                )
            if md5sum_before_resync_site_b == md5sum_after_resync_site_b:
                raise Exception(
                    f"md5sums are same on site-B before and after resync. \n"
                    f"site-A: {md5sum_before_resync_site_b} \n"
                    f"site-B: {md5sum_after_resync_site_b}"
                )
            if md5sum_after_resync_site_b != md5sum_before_force_failover_site_a:
                raise Exception(
                    f"md5sums are not same on site-A and site-B after resync. \n"
                    f"site-A: {md5sum_before_force_failover_site_a} \n"
                    f"site-B: {md5sum_after_resync_site_b}"
                )


def run(**kw):
    """
    This test verifies failover failback during orderly and non-orderly shutdown
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
        test_group_mirroring_failover(
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
            "Test Verify failover failback during orderly and non-orderly shutdown passed"
        )
    except Exception as e:
        log.error(
            "Test Verify failover failback during orderly and non-orderly shutdown failed: "
            + str(e)
        )
        return 1

    finally:
        cleanup(pool_types=pool_types, multi_cluster_obj=mirror_obj, **kw)

    return 0
