"""Module to verify creating group snapshot from pool namespace along with clone and rollback.

Test case covered -
CEPH-83594337 - Creating group snapshot, clone from pool namespace

Pre-requisites :
1. Cluster must be up and running with capacity to create pool
2. We need atleast one client node with ceph-common package,
   conf and keyring files

Test Case Flow:
1. Deploy Ceph on version 8.0 or greater.
2. Create one RBD pool
3. Create a namespace testnamespace in the pool.
4. Create two RBD images image1 and image2 in the pool under the namespace testnamespace.
    E.g: rbd create n_pool/namespace1/n_image1 -s 10G
5. write some data to that image using fio or rbd bench
6.  Create an RBD group in the pool under namespace
    E.g: rbd group create n_pool/namespace1/n_group2
7. add image1 and image2 to the namespace  group.
    E.g: rbd group image add n_pool/namespace1/n_group2 n_pool/namespace1/n_image1
8. Take a group snapshot named snap1 using the rbd group snap create command inside namespace group
    E.g: rbd group snap create n_pool/namespace1/n_group2 --snap n_snap1
9. Clone the group snapshot snap1 using the rbd clone --snap-id option.
   E.g: rbd clone --snap-id 4 n_pool/namespace1/n_image1 n_pool/namespace1/i1clone2 --rbd-default-clone-format 2
10. Modify the original images in the group by writing some data to it and take another group snapshot snap2.
11. Rollback to the group snapshot snap1.
    E.g: rbd group snap rollback n_pool/namespace1/n_group2 --snap n_snap1
12. Attempt to delete a group
    E.g: rbd group remove n_pool/namespace1/n_group2

"""

import json

from ceph.rbd.initial_config import initial_rbd_config
from ceph.rbd.utils import get_md5sum_rbd_image, getdict, random_string
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.group import (
    add_image_to_group_and_verify,
    create_group_and_verify,
    create_snap_and_verify,
    rollback_to_snap,
)
from ceph.rbd.workflows.namespace import create_namespace_and_verify
from ceph.utils import get_node_by_id
from cli.rbd.rbd import Rbd
from utility.log import Log

log = Log(__name__)


def clone_rollback_group_snapshot_on_namespace(rbd_obj, client, **kw):
    """
        Test to verify creating group snapshot from pool namespace along with clone and rollback
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

            kw["pool-name"] = pool
            namespace = "namespace_" + random_string(len=5)
            kw["namespace"] = namespace
            rc = create_namespace_and_verify(**kw)
            if rc != 0:
                return rc
            images = (
                "test_image1_" + random_string(len=5),
                "test_image2_" + random_string(len=5),
            )
            for image in images:
                create_kw = {"image-spec": f"{pool}/{namespace}/{image}", "size": "1G"}
                out, err = rbd.create(**create_kw)
                if err:
                    log.error(
                        f"Image {pool}/{namespace}/{image} failed with error {err}"
                    )
                    return 1
                else:
                    log.info(f"Image {pool}/{namespace}/{image} creation is complete")

            bench_kw = kw.get("config", {}).get("io", {})
            bench_kw.update({"image-spec": f"{pool}/{namespace}/{images[0]}"})

            out, err = rbd.bench(**bench_kw)
            if err:
                log.error(
                    f"rbd bench on {pool}/{namespace}/{images[0]} failed with error {err}"
                )
                return 1
            else:
                log.info(f"rbd bench on {pool}/{namespace}/{images[0]} is complete")

            md5_before_snap = get_md5sum_rbd_image(
                image_spec=f"{pool}/{namespace}/{images[0]}",
                rbd=rbd,
                client=client,
                file_path="file" + random_string(len=5),
            )
            log.info(f"md5 before snap is {md5_before_snap}")

            # Create a group and add the image to an RBD group
            group = kw.get("config", {}).get("group", "image_group_default")
            group_create_kw = {
                "client": client,
                "pool": pool,
                "group": group,
                "namespace": namespace,
            }
            rc = create_group_and_verify(**group_create_kw)
            if rc != 0:
                return rc

            # Add images to the group
            add_image_group_kw = {
                "group-spec": f"{pool}/{namespace}/{group}",
                "image-spec": f"{pool}/{namespace}/{images[0]}",
                "client": client,
            }
            rc = add_image_to_group_and_verify(**add_image_group_kw)
            if rc != 0:
                return rc

            snap = kw.get("config", {}).get("snap", "group_snap_default")

            # Create group snapshot
            rc = create_snap_and_verify(
                client=client, pool=pool, group=group, snap=snap, namespace=namespace
            )
            if rc != 0:
                return rc

            # fetch snap id
            out = rbd.snap.ls(all=f"{pool}/{namespace}/{images[0]}", format="json")
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

            # Clone the group snapshot with clone_format=2
            clone = kw.get("config", {}).get("clone", "clone_group_snap_default")
            clone_spec = {
                "snap-id": snap_id,
                "source-snap-spec": f"{pool}/{namespace}/{images[0]}",
                "dest-image-spec": f"{pool}/{namespace}/{clone}",
                "rbd-default-clone-format": 2,
            }
            _, err = rbd.clone(**clone_spec)
            if err:
                log.error(
                    f"Clone creation failed for {pool}/{namespace}/{image} with error {err}"
                )
                return 1
            else:
                log.info(f"Clone created successfully for {pool}/{namespace}/{image}")

            out, err = rbd.bench(**bench_kw)
            if err:
                log.error(
                    f"rbd bench on {pool}/{namespace}/{images[0]} failed with error {err}"
                )
                return 1
            else:
                log.info(f"rbd bench on {pool}/{namespace}/{images[0]} is complete")

            md5_after_modification = get_md5sum_rbd_image(
                image_spec=f"{pool}/{namespace}/{images[0]}",
                rbd=rbd,
                client=client,
                file_path="file" + random_string(len=5),
            )
            log.info(
                f"md5 after modification of {images[0]} is {md5_after_modification}"
            )

            rollback_kw = {
                "client": client,
                "pool": pool,
                "namespace": namespace,
                "group": group,
                "snap": snap,
            }
            rollback_to_snap(**rollback_kw)
            if rc != 0:
                return rc

            md5_after_rollback = get_md5sum_rbd_image(
                image_spec=f"{pool}/{namespace}/{images[0]}",
                rbd=rbd,
                client=client,
                file_path="file" + random_string(len=5),
            )
            log.info(f"md5 after rollback is {md5_after_rollback}")

            if md5_before_snap == md5_after_rollback:
                log.info("md5 before snap is same as after snap rollback")
                log.info("Group is reverted to the state captured in snap")
            else:
                log.error("md5 before snap is not equal to that of after snap rollback")
                log.error("Group is not reverted to the state captured in snap")
                return 1
            _, err = rbd.group.remove(**{"group-spec": f"{pool}/{namespace}/{group}"})
            if err:
                log.error(
                    f"Remove namespace group {pool}/{namespace}/{group} failed with error {err}"
                )
                return 1
            else:
                log.info(
                    f"Remove namespace group {pool}/{namespace}/{images[0]} completed"
                )
    return 0


def run(**kw):
    """
    This test verifies Creating group snapshot, clone, snap rollback  from pool namespace
    Args:
        kw: test data
    Returns:
        int: The return value. 0 for success, 1 otherwise

    """
    try:
        log.info(
            "CEPH-83594337 - Creating group snapshot, clone, snap rollback  from pool namespace"
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
            if clone_rollback_group_snapshot_on_namespace(rbd_obj, client, **kw):
                return 1
            log.info(
                "Test Clone, group snapshot, snap rollback on namespace of Replication and EC pool is successful"
            )

    except Exception as e:
        log.error(
            f"Test group snapshot, clone, snap rollback from pool namespace failed: {str(e)}"
        )
        return 1

    finally:
        cluster_name = kw.get("ceph_cluster", {}).name
        if "rbd_obj" not in locals():
            rbd_obj = Rbd(client)
        obj = {cluster_name: rbd_obj}
        cleanup(pool_types=pool_types, multi_cluster_obj=obj, **kw)

    return 0
