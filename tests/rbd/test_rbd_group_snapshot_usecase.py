from ceph.rbd.initial_config import initial_rbd_config
from ceph.rbd.utils import get_md5sum_rbd_image, getdict, random_string
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.group import (
    add_image_to_group_and_verify,
    create_group_and_verify,
    create_snap_and_verify,
    rollback_to_snap,
)
from ceph.rbd.workflows.rbd import wrapper_for_image_ops
from ceph.utils import get_node_by_id
from cli.rbd.rbd import Rbd
from utility.log import Log

log = Log(__name__)


def test_group_snapshot_create_rollback(rbd_obj, client, **kw):
    """
    Tests the group snapshot creation followed by rollback with all validations
    Args:
        client: rbd client obj
        **kw: test data
    """
    kw["client"] = client
    rbd = rbd_obj.get("rbd")

    for pool_type in rbd_obj.get("pool_types"):
        rbd_config = kw.get("config", {}).get(pool_type, {})
        multi_pool_config = getdict(rbd_config)
        for pool, pool_config in multi_pool_config.items():
            if "data_pool" in pool_config.keys():
                _ = pool_config.pop("data_pool")

            # 1. create a group per pool
            group = kw.get("config", {}).get("group", "image_group_default")
            group_create_kw = {"client": client, "pool": pool, "group": group}
            rc = create_group_and_verify(**group_create_kw)
            if rc:
                log.error(f"group {group} creation failed, test case fail")
                return 1
            else:
                log.info("STAGE: group creation succeeded")

            # 2. Add add images to the group
            for image, image_config in pool_config.items():
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

            # 3. Running IO on the images added to the group
            log.info(f"Run IOs and verify rbd status for images in pool {pool}")
            rc = wrapper_for_image_ops(
                rbd=rbd,
                pool=pool,
                image_config=pool_config,
                client=client,
                ops_module="ceph.rbd.workflows.rbd",
                ops_method="run_io_and_check_rbd_status",
            )
            if rc:
                log.error(f"Run IOs and verify rbd status failed for pool {pool}")
                return 1

            # 4. Take md5sums of images in the group
            md5_sum_images_before_snapshot = {}
            for image, image_config in pool_config.items():
                _md5_sum = get_md5sum_rbd_image(
                    image_spec=f"{pool}/{image}",
                    rbd=rbd,
                    client=client,
                    file_path=f"/tmp/{random_string(len=3)}",
                )
                md5_sum_images_before_snapshot.update({image: _md5_sum})
            log.info(
                f"md5sum of images before snapshot {md5_sum_images_before_snapshot}"
            )

            # 5. Create a group snapshot
            snap = kw.get("config", {}).get("snap", "group_snap_default")
            snap_create_kw = {
                "client": client,
                "pool": pool,
                "group": group,
                "snap": snap,
            }
            rc = create_snap_and_verify(**snap_create_kw)
            if rc:
                log.error(f"snap {snap} create failure")
                return 1
            else:
                log.info(f"STAGE: snap {snap} creation with validation done")

            # 6. Running IO on the images added to the group
            log.info(f"Run IOs and verify rbd status for images in pool {pool}")
            rc = wrapper_for_image_ops(
                rbd=rbd,
                pool=pool,
                image_config=pool_config,
                client=client,
                ops_module="ceph.rbd.workflows.rbd",
                ops_method="run_io_and_check_rbd_status",
            )
            if rc:
                log.error(f"Run IOs and verify rbd status failed for pool {pool}")
                return 1
            md5_sum_images_after_io = {}
            for image, image_config in pool_config.items():
                _md5_sum = get_md5sum_rbd_image(
                    image_spec=f"{pool}/{image}",
                    rbd=rbd,
                    client=client,
                    file_path=f"/tmp/{random_string(len=3)}",
                )
                md5_sum_images_after_io.update({image: _md5_sum})
            log.info(f"md5sum of images after new io {md5_sum_images_after_io}")

            # 7. Rollback to snap given
            rollback_snap_kw = {
                "client": client,
                "pool": pool,
                "group": group,
                "snap": snap,
            }
            rc = rollback_to_snap(**rollback_snap_kw)
            if rc:
                log.error(f"Rollback group to snap {snap} failed")
                return 1
            else:
                log.info(
                    "STAGE: snap {snap} rollback for groupo of images is successfull"
                )

            # 8. Take md5sums of the images in the group and validate with previous md5sums
            md5_sum_images_after_rollback = {}
            for image, image_config in pool_config.items():
                _md5_sum = get_md5sum_rbd_image(
                    image_spec=f"{pool}/{image}",
                    rbd=rbd,
                    client=client,
                    file_path=f"/tmp/{random_string(len=3)}",
                )
                md5_sum_images_after_rollback.update({image: _md5_sum})
            log.info(f"md5sum of images after rollback {md5_sum_images_after_rollback}")
            if md5_sum_images_before_snapshot != md5_sum_images_after_rollback:
                log.error("md5sum validation failed after rollback")
                return 1
    return 0


def run(**kw):
    """RBD group snap creation and rollback.

    Args:
        **kw: test data
    """
    log.info(
        "Running test CEPH-83588302 - Testing RBD group snap creation and rollback."
    )

    try:
        if kw.get("client_node"):
            client = get_node_by_id(kw.get("ceph_cluster"), kw.get("client_node"))
        else:
            client = kw.get("ceph_cluster").get_nodes(role="client")[0]
        kw["do_not_create_image"] = True
        rbd_obj = initial_rbd_config(**kw)
        pool_types = rbd_obj.get("pool_types")
        ret_val = test_group_snapshot_create_rollback(
            rbd_obj=rbd_obj, client=client, **kw
        )
    except Exception as e:
        log.error(f"Testing RBD group snap creation and rollback {str(e)}")
        ret_val = 1
    finally:
        cluster_name = kw.get("ceph_cluster", {}).name
        if "rbd_obj" not in locals():
            rbd_obj = Rbd(client)
        obj = {cluster_name: rbd_obj}
        cleanup(pool_types=pool_types, multi_cluster_obj=obj, **kw)
    return ret_val
