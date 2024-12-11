import json

from ceph.rbd.initial_config import initial_rbd_config
from ceph.rbd.utils import getdict, random_string
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.group import (
    add_image_to_group_and_verify,
    create_group_and_verify,
    create_snap_and_verify,
    group_info,
    group_snap_info,
)
from ceph.rbd.workflows.krbd_io_handler import krbd_io_handler
from ceph.utils import get_node_by_id
from cli.rbd.rbd import Rbd
from utility.log import Log

log = Log(__name__)


def test_rbd_groups_image_clone(rbd_obj, client, **kw):
    """
    Tests the group creation followed by cloning image from group snapshot
    Args:
        client: rbd client obj
        **kw: test data

    Test Steps:
    1) Deploy Ceph on version 8.0 or greater.
    2) Create an RBD pool and image inside that pool.
    3) Write some data to that image using fio or rbd bench
    4) Create a group and add the image to an RBD group
    5) Create one user snapshot and group snapshot
    6) Verify snapshots getting created successfully
    7) Verify rbd group info command outputs group id
        E.g: # rbd group info pool1/group1
        rbd group 'group1':
        id: 410d74942d2
    8) Verify rbd group snap info command output
        E.g: rbd group snap info --pool pool1 --group group1 --snap group_snap1
        rbd group snapshot 'group_snap1':
        id: 71f4c0320582
        state: complete
        image snap: .group.2_410d74942d2_71f4c0320582
        images:
        pool1/image1 (snap id: 4)
        pool2/p2_image1 (snap id: 5692)
    9) Clone the group snapshot using the rbd clone --snap-id option
        E.g: rbd clone --snap-id 4 pool1/image1 pool1/i1clone1 --rbd-default-clone-format 2
    10) Verify that the clone image is getting created successfully using
        E.g: rbd ls -p pool1
    11) Verify the cloned images exist in the mentioned pool along with it’s parent image using
        E.g: rbd info pool1/i1clone1
    12) Map the cloned images as a new block disk using rbd map <pool1/clone1>
    13) Mount directory onto that disk and create some files on that directory and write data to the files
    14) Remove the group snapshot snap_name from the group.
        E.g: rbd group snap rm testpool/testgroup --snap snap_name
    15) Verify that the snapshot has been moved to the trash namespace.
        E.g: rbd snap ls --all testpool/testimage
    16) Verify that the cloned image still exists.
        E.g: rbd info testpool/testimage_clone
    17) Attempt to delete the cloned image.
        E.g: rbd rm testpool/testimage_clone
    18) Verify that the cloned image has been removed and is no longer listed.
        E.g: rbd ls <pool_name>
    """
    kw["client"] = client
    rbd = rbd_obj.get("rbd")
    rbd1 = Rbd(kw["client"])
    fio = kw.get("config", {}).get("fio", {})

    for pool_type in rbd_obj.get("pool_types"):
        rbd_config = kw.get("config", {}).get(pool_type, {})
        multi_pool_config = getdict(rbd_config)
        for pool, pool_config in multi_pool_config.items():
            if "data_pool" in pool_config.keys():
                _ = pool_config.pop("data_pool")

            # Create a group per pool
            log.info(f"Create a group {pool}")
            group = kw.get("config", {}).get("group", "image_group_default")
            group_create_kw = {"client": client, "pool": pool, "group": group}
            rc = create_group_and_verify(**group_create_kw)
            if rc:
                log.error(f"group {group} creation failed, test case fail")
                return 1
            else:
                log.info("STAGE: group creation succeeded")

            for image, image_config in pool_config.items():
                # Running IO on the image
                log.info(f"Run IOs and verify rbd status for images in pool {pool}")
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

                # Add image to the group
                log.info(f"Adding image: {image} in group {group}")
                add_image_group_kw = {
                    "client": client,
                    "pool": pool,
                    "group": group,
                    "image": image,
                }
                rc = add_image_to_group_and_verify(**add_image_group_kw)
                if rc:
                    log.error(f"Image {image} add failed, test case fail")
                    return 1
                else:
                    log.info(f"STAGE: image {image} add succeeded")

                # Create one user snapshot and group snapshot
                group_snap = kw.get("config", {}).get(
                    "group_snap", "group_snap_default"
                )
                log.info(f"Creating group snapshot: {group_snap}")
                snap_create_kw = {
                    "client": client,
                    "pool": pool,
                    "group": group,
                    "snap": group_snap,
                }
                rc = create_snap_and_verify(**snap_create_kw)
                if rc:
                    log.error(f"snap {group_snap} create failure")
                    return 1
                else:
                    log.info(f"STAGE: snap {group_snap} creation with validation done")

                user_snap = kw.get("config", {}).get("user_snap")
                log.info(f"Creating User snapshot: {user_snap}")
                out, err = rbd.snap.create(pool=pool, image=image, snap=user_snap)
                if "100% complete...done" not in out + err:
                    log.error(
                        f"Snapshot creation failed for {pool}/{image}@{user_snap} with error {err}"
                    )
                    return 1

                # Verify rbd group info command outputs group id
                log.info(f"Verify group id for group: {group} snap: {group_snap}")
                group_ls_kw = {
                    "client": client,
                    "pool": pool,
                    "group": group,
                    "format": "json",
                }
                (g_ls_out, _) = group_info(**group_ls_kw)
                log.info(g_ls_out)
                g_ls_out = json.loads(g_ls_out)
                if g_ls_out["group_id"].isalnum():
                    log.info(f"Group id exist for group {group}")
                else:
                    log.error(f"Group id does not exist for group {group}")
                    return 1

                # Verify rbd group snap info command output
                log.info(
                    "Get snap-id of group snapshot to further create clone of image from group snapshot"
                )
                snap_group_info = {
                    "client": client,
                    "pool": pool,
                    "group": group,
                    "snap": group_snap,
                    "format": "json",
                }
                (snap_g_out, _) = group_snap_info(**snap_group_info)
                log.info(snap_g_out)
                snap_g_out = json.loads(snap_g_out)
                snap_id = snap_g_out["images"][0]["snap_id"]

                # Clone the group snapshot using the rbd clone --snap-id option
                log.info(
                    f"Clone the image using group snapshot group: {group} snap: {group_snap}"
                )
                clone = image + "_clone"
                clone_create_kw = {
                    "source-snap-spec": pool + "/" + image,
                    "dest-image-spec": pool + "/" + clone,
                    "snap-id": snap_id,
                    "rbd-default-clone-format": "2",
                }
                rbd1.clone(**clone_create_kw)

                # Verify the cloned images exist in the mentioned pool along with it’s parent image
                log.info(f"Validate clone image {clone} exist")
                info_spec = {"image-or-snap-spec": f"{pool}/{clone}", "format": "json"}
                out, err = rbd1.info(**info_spec)
                if err:
                    log.error(f"Error while fetching info for image {clone}")
                    return 1
                out_json = json.loads(out)
                log.info(f"Image info: {out_json}")

                # 12. Map the cloned images as a new block disk
                log.info(f"Map the cloned image {clone} and run IO")
                io_config["config"]["image_spec"] = [f"{pool}/{clone}"]
                krbd_io_handler(**io_config)

                # Remove the group snapshot snap_name from the group.
                log.info(f"Remove group snapshot {group_snap}")
                rbd.group.snap.rm(pool=pool, snap=group_snap)
                # Verify that the snapshot has been moved to the trash.
                log.info(f"Verify that the group snapshot {group_snap} is removed")
                out, err = rbd.snap.ls(pool=pool, image=image, all="", format="json")
                snap_json = json.loads(out)
                for snap in snap_json:
                    if snap["name"] == group_snap:
                        log.error(
                            f"Group snapshot {group_snap} found even after deletion"
                        )
                        return 1
                    else:
                        log.info(f"Group snapshot {group_snap} deleted successfully")
                # Verify that the cloned image still exists.
                log.info(f"Validate clone image {clone} still exist")
                info_spec = {"image-or-snap-spec": f"{pool}/{clone}", "format": "json"}
                out, err = rbd1.info(**info_spec)
                if err:
                    log.error(f"Error while fetching info for image {clone}")
                    return 1
                out_json = json.loads(out)
                if out_json["name"] != clone:
                    log.error(f"clone image {clone} does not exist")
                    return 1

                # Unmap the cloned image.
                log.info(f"Unmap clone image: {clone}")
                rbd.unmap(pool=pool, image=clone)

                # Attempt to delete the cloned image.
                log.info(f"Delete clone image: {clone}")
                rbd.rm(pool=pool, image=clone)
                # Verify that the cloned image has been removed and is no longer listed
                log.info(f"Verify cloned image {clone} has been removed")
                out, err = rbd.ls(pool=pool, format="json")
                out_json = json.loads(out)
                log.info(out_json)
                if clone in out_json:
                    log.error(f"clone image {clone} still exist even after deleting")
                    return 1
                else:
                    log.info(f"Clone image {clone} deleted Successfully")

    return 0


def run(**kw):
    """Tests the group creation followed by cloning image from group snapshot.

    Args:
        **kw: test data
    """
    log.info("Running test CEPH-83594298 - Cloning an image from a Group Snapshot")

    try:
        if kw.get("client_node"):
            client = get_node_by_id(kw.get("ceph_cluster"), kw.get("client_node"))
        else:
            client = kw.get("ceph_cluster").get_nodes(role="client")[0]
        kw["do_not_create_image"] = True
        rbd_obj = initial_rbd_config(**kw)
        pool_types = rbd_obj.get("pool_types")
        ret_val = test_rbd_groups_image_clone(rbd_obj=rbd_obj, client=client, **kw)
        if ret_val == 0:
            log.info("Testing RBD cloning image from group snapshot Passed")
    except Exception as e:
        log.error(
            f"Testing RBD cloning image from group snapshot failed with Error: {str(e)}"
        )
        ret_val = 1
    finally:
        cluster_name = kw.get("ceph_cluster", {}).name
        if "rbd_obj" not in locals():
            rbd_obj = Rbd(client)
        obj = {cluster_name: rbd_obj}
        if pool_types:
            cleanup(pool_types=pool_types, multi_cluster_obj=obj, **kw)
    return ret_val
