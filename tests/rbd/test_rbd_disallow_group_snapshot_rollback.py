"""Module to verify group snap shot rollback is not allowed if memberships don't match.

Test case covered -
CEPH-83594544 -  Disallow Group Snapshot Rollback if Memberships Don't Match

Pre-requisites :
1. Cluster must be up and running with capacity to create pool
2. We need atleast one client node with ceph-common package,
   conf and keyring files

Test Case Flow:
1. Deploy Ceph on version 8.0 or greater.
2. create an RBD pool1 and image1 inside that pool.
3. Create an RBD pool2 and image2 inside that pool.
4. Write data to the images using fio.
5. Create a pool1 group and add the pool1 image and pool2 image to the group using
    E.g: rbd group create --pool pool1 --group group1;
    rbd group image add pool1/group1 pool1/p1_image1
6. Take a snapshot of the group using the rbd group snap create command.
7.  Create a new RBD pool3 image (image3) and add it to the pool1 group.
8. Write data to all three images using fio
9. Remove the previously existing images (e.g., image1 of pool1) from the group.
10. Verify the current membership of the group and the snapshot.
11.  Attempt to rollback to the group snapshot of removed images using
rbd group snap rollback pool1/group1 --snap p1g1snap1
12. Verify that the group state remains unchanged after the failed rollback attempt.
13. Create a new group snapshot with the current group membership.
14.  Attempt rollback to the new snapshot created in Step 13 using
E.g: rbd group snap rollback pool1/new_name --snap snap2
15. Verify the state of the group after successful rollback using rbd group snap ls pool1/group1
"""

from copy import deepcopy

from ceph.rbd.initial_config import initial_rbd_config
from ceph.rbd.utils import get_md5sum_rbd_image, getdict, random_string
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.group import (
    add_image_to_group_and_verify,
    create_group_and_verify,
    create_snap_and_verify,
)
from ceph.rbd.workflows.krbd_io_handler import krbd_io_handler
from ceph.rbd.workflows.rbd import create_single_pool_and_images
from ceph.utils import get_node_by_id
from cli.rbd.rbd import Rbd
from utility.log import Log

log = Log(__name__)


def disallow_group_snapshot_rollback(rbd_obj, client, **kw):
    """
        verify group snap shot rollback is not allowed if memberships don't match.
    Args:
        rbd_obj: RBD object
        client : client node object
        **kw: any other arguments
    """

    kw["client"] = client
    rbd = rbd_obj.get("rbd")
    for pool_type in rbd_obj.get("pool_types"):
        rbd_config = kw.get("config", {}).get(pool_type, {})
        multi_pool_config = deepcopy(getdict(rbd_config))

        pools = list(multi_pool_config.keys())
        test_pool = pools[0]
        test_group = kw.get("config", {}).get("group", "image_group_default")
        group_spec = f"{test_pool}/{test_group}"
        image_spec = []

        for pool, pool_config in multi_pool_config.items():
            if "data_pool" in pool_config.keys():
                _ = pool_config.pop("data_pool")
            # write some data to that image using fio
            fio = kw.get("config", {}).get("fio", {})
            for image, image_config in pool_config.items():
                image_spec.append(f"{pool}/{image}")
                io_config = {
                    "rbd_obj": rbd,
                    "client": client,
                    "size": fio["size"],
                    "do_not_create_image": True,
                    "config": {
                        "file_size": fio["size"],
                        "file_path": [f"/mnt/mnt_{random_string(len=5)}/file"],
                        "get_time_taken": True,
                        "image_spec": [f"{pool}/{image}"],
                        "operations": {
                            "fs": "ext4",
                            "io": True,
                            "mount": True,
                            "map": True,
                        },
                        "skip_mkfs": False,
                        "cmd_timeout": 2400,
                        "io_type": "write",
                    },
                }
                krbd_io_handler(**io_config)

        md5_before_snap = []
        for i in range(0, len(image_spec)):
            md5 = get_md5sum_rbd_image(
                image_spec=image_spec[i],
                rbd=rbd,
                client=client,
                file_path="file" + random_string(len=5),
            )
            md5_before_snap.append(md5)
        log.info(f"md5 sums before snap: {md5_before_snap}")

        # Create a group and add the image to an RBD group
        group_create_kw = {"client": client, "pool": test_pool, "group": test_group}
        rc = create_group_and_verify(**group_create_kw)
        if rc:
            log.error(f"group {test_group} creation failed, test case fail")
            return 1
        else:
            log.info(f"Group {test_group} creation succeeded")

        # Add images to the group
        for ispec in image_spec:
            add_image_group_kw = {
                "group-spec": group_spec,
                "image-spec": ispec,
                "client": client,
            }
            rc = add_image_to_group_and_verify(**add_image_group_kw)
            if rc:
                log.error(f"Image {ispec} adding to group {group_spec} failed.")
                return 1
            else:
                log.info(f"Image {ispec} adding to group {group_spec} succeeded.")

        # Create group snapshot
        snap = kw.get("config", {}).get("snap", "group_snap_default")
        rc = create_snap_and_verify(
            client=client, pool=test_pool, group=test_group, snap=snap
        )
        if rc:
            log.error("Group snap creation failed")
            return 1
        else:
            log.info("Group snap creation with validation done")

        # Create a new RBD image (image3) and add it to the group.
        new_pool = "new_pool_" + random_string()
        log.info(f"Create a new pool {new_pool}")
        config = kw.get("config", {})
        is_ec_pool = True if "ec" in pool_type else False
        new_image = "image_" + random_string(len=5)
        new_pool_config = {new_image: {"size": "1G", "io_total": "1G"}}
        image_spec.append(f"{new_pool}/{new_image}")
        if is_ec_pool:
            data_pool_new = "data_pool_new_" + random_string()
            new_pool_config["data_pool"] = data_pool_new
        rc = create_single_pool_and_images(
            config=config,
            pool=new_pool,
            pool_config=new_pool_config,
            client=client,
            cluster="ceph",
            rbd=rbd,
            ceph_version=int(config.get("rhbuild")[0]),
            is_ec_pool=is_ec_pool,
            is_secondary=False,
        )
        if rc:
            log.error(f"Creation of pool {new_pool} failed")
            return rc
        # Adding the new pool details to config so that they are handled in cleanup
        if pool_type == "rep_pool_config":
            kw["config"]["rep_pool_config"][new_pool] = {}
        elif pool_type == "ec_pool_config":
            kw["config"]["ec_pool_config"][new_pool] = {"data_pool": data_pool_new}

        add_image_group_kw = {
            "group-spec": group_spec,
            "image-spec": image_spec[2],
            "client": client,
        }
        rc = add_image_to_group_and_verify(**add_image_group_kw)
        if rc:
            log.error(f"Image {new_image} add failed, test case fail")
            return 1
        else:
            log.info(f"Image {new_image} add succeeded")

        for j in range(0, len(image_spec)):
            io_config["config"]["image_spec"] = [image_spec[j]]
            krbd_io_handler(**io_config)

        md5_after_snap = []
        for i in range(0, len(image_spec)):
            md5 = get_md5sum_rbd_image(
                image_spec=image_spec[i],
                rbd=rbd,
                client=client,
                file_path="file" + random_string(len=5),
            )
            md5_after_snap.append(md5)
        log.info(f"md5 sums after snap: {md5_after_snap}")
        for i in range(0, len(md5_before_snap)):
            if md5_before_snap[i] == md5_after_snap[i]:
                log.error(
                    f"md5 sum of rbd image {image_spec[i]}is not changed after fio writes"
                )
                return 1

        out, err = rbd.group.image.rm(
            **{"group-spec": group_spec, "image-spec": image_spec[0]}
        )
        if err:
            log.error(f"Failed to remove {image_spec[0]}from group {group_spec}")
            return 1
        else:
            log.info(f"Removed {image_spec[0]} from group {group_spec}")

        out, err = rbd.group.image.list(**{"group-spec": group_spec})
        if not err:
            if image_spec[0] in out:
                log.error(f"{image_spec[0]} listed in group {group_spec} after removal")
                return 1
            else:
                log.info(
                    f"{image_spec[0]} not listed in group {group_spec} after removal"
                )

            if image_spec[1] in out and image_spec[2] in out:
                log.info(f"{image_spec[0]} and {image_spec[2]} exist in {group_spec}")
                log.info("Verified the group membership is as expected")
            else:
                log.error(f"{image_spec[0]} or {image_spec[2]} not in {group_spec}")
                log.error("Group membership is not as expected")
                return 1
        else:
            log.error(f"Group image list failed with error {err}")
            return 1

        # Verify group snapshot rollback fails when memberships do not match
        out, err = rbd.group.snap.rollback(
            **{"group-snap-spec": f"{group_spec}@{snap}"}
        )
        if "group snapshot membership does not match group membership" in err:
            log.info(f"Group snapshot rollback failed as expected with error: {err}")
        else:
            log.error("Group snapshot rollback is successful")
            return 1

        out, err = rbd.group.image.list(**{"group-spec": group_spec})
        if image_spec[1] in out and image_spec[2] in out:
            log.info(f"{image_spec[0]} and {image_spec[2]} exist in {group_spec}")
            log.info("Verified the group membership is as expected")
        else:
            log.error(f"{image_spec[0]} or {image_spec[2]} not in {group_spec}")
            log.error("Group membership is not as expected")
            return 1

        md5_before_snap2 = []
        for i in range(0, len(image_spec)):
            md5 = get_md5sum_rbd_image(
                image_spec=image_spec[i],
                rbd=rbd,
                client=client,
                file_path="file" + random_string(len=5),
            )
            md5_before_snap2.append(md5)
        log.info(f"md5 sums before second snap is: {md5_before_snap2}")

        for i in range(0, len(md5_after_snap)):
            if md5_before_snap2[i] != md5_after_snap[i]:
                log.error(
                    f"md5 sum of image {image_spec[i]} is not same after failed group snap rollback operation"
                )
        snap_new = "snap_new" + random_string(len=5)

        # Create group snapshot
        rc = create_snap_and_verify(
            client=client, pool=test_pool, group=test_group, snap=snap_new
        )
        if rc:
            log.error("Group snap creation failed")
            return 1
        else:
            log.info("Group snap creation with validation done")

        for j in range(0, len(image_spec)):
            io_config["config"]["image_spec"] = [image_spec[j]]
            krbd_io_handler(**io_config)

        md5_after_snap2 = []
        for i in range(0, len(image_spec)):
            md5 = get_md5sum_rbd_image(
                image_spec=image_spec[i],
                rbd=rbd,
                client=client,
                file_path="file" + random_string(len=5),
            )
            md5_after_snap2.append(md5)
        log.info(f"md5 sums after second snap : {md5_after_snap2}")

        for i in range(0, len(md5_after_snap2)):
            if md5_before_snap2[i] == md5_after_snap2[i]:
                log.error(f"md5 sum of image {image_spec[i]} is same after fio writes")
                return 1
            else:
                log.info(f"md5 sum of image {image_spec[i]} changed after fio writes")

        out, err = rbd.group.snap.rollback(
            **{"group-snap-spec": f"{group_spec}@{snap_new}"}
        )
        if out or err and "100% complete" not in err:
            log.error(f"Group snapshot rollback failed with {err}")
            return 1
        else:
            log.info("Group snapshot rollback is successful")

        md5_after_rollback = []
        for i in range(0, len(image_spec)):
            md5 = get_md5sum_rbd_image(
                image_spec=image_spec[i],
                rbd=rbd,
                client=client,
                file_path="file" + random_string(len=5),
            )
            md5_after_rollback.append(md5)
        log.info(f"md5 sums after rollback : {md5_after_rollback}")
        for i in range(1, len(md5_after_rollback)):
            if md5_before_snap2[i] == md5_after_rollback[i]:
                log.info(
                    f"md5 sum of image {image_spec[i]} is same after group snap rollback operation"
                )
            else:
                log.error(
                    f"md5 sum of image {image_spec[i]} is not same after group snap rollback operation"
                )
                return 1
        if pool_type == "rep_pool_config":
            kw["config"]["rep_pool_config"][new_pool] = {}
        elif pool_type == "ec_pool_config":
            kw["config"]["ec_pool_config"][new_pool] = {"data_pool": data_pool_new}
    return 0


def run(**kw):
    """
    This test verifies group snap shot rollback is not allowed if memberships don't match
    Args:
        kw: test data
    Returns:
        int: The return value. 0 for success, 1 otherwise
    """
    try:
        log.info(
            "CEPH-83594544 - Disallow Group Snapshot Rollback if Memberships Don't Match"
        )
        pool_types = list()
        if kw.get("client_node"):
            client = get_node_by_id(kw.get("ceph_cluster"), kw.get("client_node"))
        else:
            client = kw.get("ceph_cluster").get_nodes(role="client")[0]
        rbd_obj = initial_rbd_config(**kw)
        pool_types = rbd_obj.get("pool_types")
        if rbd_obj:
            log.info("Executing test on Replicated and EC pool")
            if disallow_group_snapshot_rollback(rbd_obj, client, **kw):
                return 1
            log.info(
                "Test Disallow Group Snapshot Rollback if Memberships Don't Match is successful"
            )

    except Exception as e:
        log.error(
            f"Test Disallow Group Snapshot Rollback if Memberships Don't Match failed: {str(e)}"
        )
        return 1

    finally:
        cluster_name = kw.get("ceph_cluster", {}).name
        if "rbd_obj" not in locals():
            rbd_obj = Rbd(client)
        obj = {cluster_name: rbd_obj}
        cleanup(pool_types=pool_types, multi_cluster_obj=obj, **kw)

    return 0
