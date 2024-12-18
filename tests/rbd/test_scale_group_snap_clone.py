import json
import time
from copy import deepcopy

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


def test_scale_group_snap_clone(rbd_obj, client, **kw):
    """
    Tests the group creation followed by cloning image from group snapshot at scale
    Args:
        client: rbd client obj
        rbd_obj: Rbd obj
        **kw: test data

    Test Steps:
    1. Deploy Ceph on version 8.0 or greater.
    2. Create an RBD pool named pool1.
    3. Create more than 100 RBD images in the pool using a loop.
    4. Create an RBD group named group1 in pool1 and
    5. Add all 100+ images to the group using a loop.
    6. Take a snapshot of the group using the rbd group snap create command.
    7. Verify the group snapshot exists using the rbd group snap ls command.
    8. Clone the group snapshot for all 100+ images using the rbd clone --snap-id option
    9. Verify the cloned images exist in the pool and check their parent image using rbd info in a loop.
    10. Map the cloned images as new block disks and run fio using rbd map in a loop.
    """
    kw["client"] = client
    rbd = rbd_obj.get("rbd")
    rbd1 = Rbd(kw["client"])
    fio = kw.get("config", {}).get("fio", {})

    for pool_type in rbd_obj.get("pool_types"):
        rbd_config = kw.get("config", {}).get(pool_type, {})
        multi_pool_config = deepcopy(getdict(rbd_config))
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
                log.info(f"All image details : {image} and {image_config}")

                # Add all 100+ images to the group using a loop.
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
                    log.info(f"STAGE: image {image} added successfully to group")

            # Take a snapshot of the group using the rbd group snap create command.
            group_snap = kw.get("config", {}).get("group_snap", "group_snap_default")
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
                log.info(f"STAGE: Group snap {group_snap} created successfully")

            # Verify the group snapshot exists using the rbd group snap ls command.
            log.info(f"Verify group id for group: {group} snap: {group_snap}")
            group_ls_kw = {
                "client": client,
                "pool": pool,
                "group": group,
                "format": "json",
            }
            (group_output, _) = group_info(**group_ls_kw)
            log.info(group_output)
            group_output = json.loads(group_output)
            if group_output["group_id"].isalnum():
                log.info(f"Group id exist for group {group}")
            else:
                log.error(f"Group id does not exist for group {group}")
                return 1

            # Get Snap-id
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
            (snap_group_output, _) = group_snap_info(**snap_group_info)
            log.info(snap_group_output)
            snap_group_output = json.loads(snap_group_output)
            snap_id_dict = {}
            for image, image_config in pool_config.items():
                for i in snap_group_output["images"]:
                    if i["image_name"] == image:
                        snap_id_dict[image] = i["snap_id"]
            log.info(f"{snap_id_dict}")

            for image, image_config in pool_config.items():
                # Clone the group snapshot for all 100+ images using the rbd clone --snap-id option
                log.info(
                    f"Clone the image using group snapshot group: {group} snap: {group_snap}"
                )
                clone = image + "_clone"
                clone_create_kw = {
                    "source-snap-spec": pool + "/" + image,
                    "dest-image-spec": pool + "/" + clone,
                    "snap-id": snap_id_dict[image],
                    "rbd-default-clone-format": "2",
                }
                rbd1.clone(**clone_create_kw)

                # Verify the cloned images exist in the pool and check their parent image using rbd info in a loop.
                log.info("Verify the cloned images exist in the pool")
                info_spec = {"image-or-snap-spec": f"{pool}/{clone}", "format": "json"}
                out, err = rbd1.info(**info_spec)
                if err:
                    log.error(f"Error while fetching info for image {clone}")
                    return 1
                out_json = json.loads(out)
                if out_json["name"] != clone:
                    log.error(f"Clone image {clone} does not exist")
                    return 1

            # Map the cloned images as new block disks and run fio on mapped disk.
            for image, image_config in pool_config.items():
                retry = 0
                while retry < 5:
                    try:
                        clone = image + "_clone"
                        io_config = {
                            "rbd_obj": rbd,
                            "client": client,
                            "size": fio["size"],
                            "do_not_create_image": True,
                            "config": {
                                "file_size": fio["size"],
                                "file_path": [f"/mnt/mnt_{random_string(len=5)}/file"],
                                "get_time_taken": True,
                                "image_spec": [f"{pool}/{clone}"],
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
                        break
                    except Exception as e:
                        log.info(
                            f"Map of the clone {clone} failed: {str(e)} : retrying again.."
                        )
                        time.sleep(10)
                        retry = retry + 1
                if retry == 5:
                    log.error("Failure in FIO even after 5 retries")

    return 0


def run(**kw):
    """Tests the group creation followed by cloning image from group snapshot at scale.

    Args:
        **kw: test data
    """
    log.info(
        "Running test CEPH-83594558 - Cloning an image from a Group Snapshot at scale"
    )

    try:
        if kw.get("client_node"):
            client = get_node_by_id(kw.get("ceph_cluster"), kw.get("client_node"))
        else:
            client = kw.get("ceph_cluster").get_nodes(role="client")[0]

        rbd_obj = initial_rbd_config(**kw)
        pool_types = rbd_obj.get("pool_types")

        test = rbd_obj.get("rbd")
        ret_val = test_scale_group_snap_clone(rbd_obj=rbd_obj, client=client, **kw)
        if ret_val == 0:
            log.info("Testing RBD cloning image from group snapshot at scale Passed")

    except Exception as e:
        log.error(
            f"Testing RBD cloning image from group snapshot at scale failed with Error: {str(e)}"
        )
        ret_val = 1

    finally:
        cluster_name = kw.get("ceph_cluster", {}).name
        if "rbd_obj" not in locals():
            rbd_obj = Rbd(client)
        obj = {cluster_name: rbd_obj}
        cleanup(pool_types=pool_types, multi_cluster_obj=obj, **kw)
    return ret_val
