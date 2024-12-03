"""Module to verify Cloning and Flattening a Group Snapshot.

Test case covered -
CEPH-83594299 -  Cloning and Flattening a Group Snapshot

Pre-requisites :
1. Cluster must be up and running with capacity to create pool
2. We need atleast one client node with ceph-common package,
   conf and keyring files

Test Case Flow:
1. Deploy Ceph on version 8.0 or greater.
2. create an RBD pool and image inside that pool.
3. write some data to that image using fio or rbd bench
4. Create a group and add the image to an RBD group using
    E.g: rbd group create --pool pool1 --group group1;
    rbd group image add pool1/group1 pool1/p1_image1
5. Create one user snapshot and group snapshot
    E.g: rbd snap create pool1/p1_image1@snap1;
    rbd group snap create pool1/group1@p1g1snap1;
6.  Clone the group snapshot using the rbd clone --snap-id option with clone_format=1
    E.g: rbd clone --snap-id <group_snap_id> pool1/image1 pool1/i1clone1
7. Clone the group snapshot using the rbd clone --snap-id option with clone_format=2
    E.g: rbd clone --snap-id 4 pool1/image1 pool1/i1clone1 --rbd-default-clone-format 2
8. Verify the cloned images exist in the mentioned pool along with it’s parent image using
    E.g: rbd info pool1/i1clone1
9. Map the cloned images as a new block disk using rbd map <pool1/clone1>
10.  Mount the directory onto that disk and create some files on that directory and write data to the files
11. Flatten the cloned image using rbd flatten command.
12. Verify the cloned image’s parent information after flattening using rbd info
13. write data to the flattened cloned image
14. Remove the flattened cloned image using rbd rm command
"""

import json

from ceph.rbd.initial_config import initial_rbd_config
from ceph.rbd.utils import getdict, random_string
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.group import (
    add_image_to_group_and_verify,
    create_group_and_verify,
    create_snap_and_verify,
)
from ceph.rbd.workflows.krbd_io_handler import krbd_io_handler
from ceph.rbd.workflows.snap_clone_operations import snap_create_list_and_verify
from ceph.utils import get_node_by_id
from cli.rbd.rbd import Rbd
from utility.log import Log

log = Log(__name__)


def clone_flatten_group_snapshot(rbd_obj, client, **kw):
    """
        Test to verify Cloning and Flattening a Group Snapshot
    Args:
        rbd_obj: RBD object
        client : client node object
        **kw: any other arguments
    """

    kw["client"] = client
    rbd = rbd_obj.get("rbd")

    for pool_type in rbd_obj.get("pool_types"):
        rbd_config = kw.get("config", {}).get(pool_type, {})
        multi_pool_config = getdict(rbd_config)

        for pool, pool_config in multi_pool_config.items():
            if "data_pool" in pool_config.keys():
                _ = pool_config.pop("data_pool")

            # 3. write some data to that image using fio or rbd bench
            fio = kw.get("config", {}).get("fio", {})
            for image, image_config in pool_config.items():
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

            # 4. Create a group and add the image to an RBD group
            group = kw.get("config", {}).get("group", "image_group_default")
            group_create_kw = {"client": client, "pool": pool, "group": group}
            rc = create_group_and_verify(**group_create_kw)
            if rc:
                log.error(f"group {group} creation failed, test case fail")
                return 1
            else:
                log.info("STAGE: group creation succeeded")

            for image, image_config in pool_config.items():
                # Add images to the group
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

            # Create one user snapshot
            rc = snap_create_list_and_verify(
                pool=pool, image=image, rbd=rbd, is_secondary=False
            )
            if rc:
                log.error("Snapshot creation, listing and verification failed")
                return 1

            snap = kw.get("config", {}).get("snap", "group_snap_default")

            # Create group snapshot
            rc = create_snap_and_verify(
                client=client, pool=pool, group=group, snap=snap
            )
            if rc:
                log.error("Group snap creation failed")
                return 1
            else:
                log.info("Group snap creation with validation done")
            # fetch snap id
            out = rbd.snap.ls(all=f"{pool}/{image}", format="json")
            out_json = json.loads(out[0])
            for entry in out_json:
                if "namespace" in entry:
                    if (
                        entry["namespace"].get("type") == "group"
                        and entry["namespace"].get("pool") == pool
                        and entry["namespace"].get("group") == group
                        and entry["namespace"].get("group snap") == snap
                    ):
                        snap_id = int(entry["id"])
            log.info(f"Snap id is {snap_id} ")

            # Clone the group snapshot with clone_format=1
            clone = kw.get("config", {}).get("clone", "clone_group_snap_default")
            clone_spec = {
                "snap-id": snap_id,
                "source-snap-spec": f"{pool}/{image}",
                "dest-image-spec": f"{pool}/{clone}",
            }
            _, err = rbd.clone(**clone_spec)
            if err and "parent snapshot must be protected" in err:
                log.info(
                    f"Clone creation failed  as expected for {pool}/{image}  for clone format 1 , with error {err}"
                )
            else:
                log.error(
                    "Clone creation passed for clone fomrat 1 which is not expected"
                )
                return 1

            # Clone the group snapshot with clone_format=2
            clone_spec = {
                "snap-id": snap_id,
                "source-snap-spec": f"{pool}/{image}",
                "dest-image-spec": f"{pool}/{clone}",
                "rbd-default-clone-format": 2,
            }
            _, err = rbd.clone(**clone_spec)
            if err:
                log.error(f"Clone creation failed for {pool}/{image} with error {err}")
                return 1
            else:
                log.info(f"Cloning of snap {snap} is complete")

            # Verify the cloned images exist in the mentioned pool along with it’s parent image
            info_kw = {"image-or-snap-spec": f"{pool}/{clone}", "format": "json"}
            out = rbd.info(**info_kw)
            out_json = json.loads(out[0])
            if "parent" in out_json:
                if (
                    out_json.get("parent", {}).get("pool") == pool
                    and out_json.get("parent", {}).get("image") == image
                ):
                    log.info(
                        "Parent details are reflected correctly for the cloned image of the group snapshot."
                    )
                else:
                    log.error(
                        "Parent details are not reflected correctly for the cloned image of the group snapshot"
                    )
                    return 1
            else:
                log.error("Parent option is not reflected in the clone info output")
                return 1

            # Map the cloned images as a new block disk using rbd map
            # Mount the directory onto that disk and create some files on that directory and write data to the files
            io_config["config"]["image_spec"] = [f"{pool}/{clone}"]

            krbd_io_handler(**io_config)

            # Flatten the cloned image using rbd flatten command.
            flatten_kw = {"pool": pool, "image": clone}
            out, err = rbd.flatten(**flatten_kw)
            if out or err and "100% complete" not in err:
                log.error(f"Flatten clone failed for {clone_spec}")
                return 1
            else:
                log.info("Flattening clone successful")

            # Verify the cloned image’s parent information after flattening using rbd info
            out = rbd.info(**info_kw)
            out_json = json.loads(out[0])

            if "parent" in out_json:
                log.error("parent information still exist for flattened clone")
                return 1
            else:
                log.info(
                    "Parent details do not exist after flattening clone as expected"
                )

            # write data to the flattened cloned image
            krbd_io_handler(**io_config)
            rm_kw = {"image-spec": f"{pool}/{clone}"}

            # Remove the flattened cloned image using rbd rm command
            out, err = rbd.rm(**rm_kw)
            if out or err and "100% complete" not in err:
                log.error(f"Remove flattened clone failed with error {err}")
                return 1
            else:
                log.info("Flattened clone removed successfully")

    return 0


def run(**kw):
    """
    This test verifies clonign and flattening a group snapshot
    Args:
        kw: test data
    Returns:
        int: The return value. 0 for success, 1 otherwise

    """
    try:
        log.info("CEPH-83594299 -  Cloning and Flattening a Group Snapshot")

        if kw.get("client_node"):
            client = get_node_by_id(kw.get("ceph_cluster"), kw.get("client_node"))
        else:
            client = kw.get("ceph_cluster").get_nodes(role="client")[0]
        rbd_obj = initial_rbd_config(**kw)
        pool_types = rbd_obj.get("pool_types")
        if rbd_obj:
            log.info("Executing test on Replication and EC pool")
            if clone_flatten_group_snapshot(rbd_obj, client, **kw):
                return 1
            log.info(
                "Test Cloning and Flattening a Group Snapshot on Replication and EC pool is successful"
            )

    except Exception as e:
        log.error(f"Test Cloning and Flattening a Group Snapshot failed: {str(e)}")
        return 1

    finally:
        cluster_name = kw.get("ceph_cluster", {}).name
        if "rbd_obj" not in locals():
            rbd_obj = Rbd(client)
        obj = {cluster_name: rbd_obj}
        cleanup(pool_types=pool_types, multi_cluster_obj=obj, **kw)

    return 0
