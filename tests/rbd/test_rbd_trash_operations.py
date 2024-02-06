"""RBD Image trash operations at scale

Test Case Covered:
CEPH-83582444 - [Baremetal] [ScaleTest] - Test all rbd trash operations at scale

Steps:
1) Create an RHCS cluster on baremetal nodes as specified in setup. Execute IOs to
fill the cluster to a decent percentage
2) Create 10 RBD pools and 100 images per pool parallely on this cluster
3) Create snaps and clones for half of the images
4) Run IOs on clones
5) Move all images to trash parallely and verify
6) Restore all images back from trash parallely and verify
7) Trash purge all images parallely and verify
8) Restore all images back and verify
9) Enable rbd_move_to_trash_on_remove in client config and remove all images
10) Restore all images back and verify
11) Define a trash purge schedule and verify that all images adhere to this schedule
12) Restore all images back and remove trash purge schedule and verify
13) Disable rbd_move_to_trash_on_remove in client config and remove all images

Pre-requisites :
- need client node with ceph-common package, conf and keyring files
- FIO should be installed on the client.

Environment and limitations:
- The cluster should have enough storage to create atleast 1000 images with 4G each
- cluster/global-config-file: config/reef/baremetal/mero_conf.yaml
- Should be Bare-metal.
"""

import pdb
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from time import sleep

from ceph.parallel import parallel
from ceph.rbd.initial_config import initial_rbd_config
from ceph.rbd.utils import exec_cmd, getdict
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.rbd import (
    create_images,
    create_single_pool_and_images,
    list_images_and_verify_with_input,
    wrapper_for_image_ops,
)
from ceph.rbd.workflows.rbd_trash import purge_images_and_verify
from ceph.utils import get_node_by_id
from cli.rbd.rbd import Rbd
from utility.log import Log

log = Log(__name__)


def restore_and_create_new_image(
    rbd,
    pool,
    pool_type,
    pool_config,
    image_config,
    client,
    multi_image_config,
    images_without_snaps_and_clones,
    test_ops_parallely,
    **kw,
):
    """ """
    pdb.set_trace()
    log.info(f"Restore images from trash and verify for pool {pool}")
    # image_config_to_restore = {
    #     image: multi_image_config.get(image)
    #     for image in images_without_snaps_and_clones
    # }
    rc = wrapper_for_image_ops(
        rbd=rbd,
        pool=pool,
        image_config=image_config,
        client=client,
        ops_module="ceph.rbd.workflows.rbd_trash",
        ops_method="restore_image_from_trash_and_verify",
        test_ops_parallely=test_ops_parallely,
    )
    pdb.set_trace()
    if rc:
        log.error(f"Restore images from trash and verify failed for pool {pool}")
        return 1

    pdb.set_trace()
    image_create_config = {
        image: multi_image_config.get(image)
        for image in images_without_snaps_and_clones
    }

    rc = create_images(
        config=kw.get("config"),
        cluster="ceph",
        rbd=rbd,
        pool=pool,
        pool_config=pool_config,
        image_config=image_create_config,
        is_ec_pool=True if "ec" in pool_type else False,
        create_image_parallely=kw["config"][pool_type].get("create_image_parallely"),
    )

    if rc:
        log.error(f"Image creation failed")
        return 1

    return 0


def test_rbd_trash_operations_for_pool(
    rbd, pool, pool_config, test_ops_parallely, client, pool_type, **kw
):
    """Test image trash operations like trash, restore, purge, trash purge schedule
    and remove for all images with and without snaps and clones in the given pool type

    Args:
        rbd: rbd object
        pool: pool name
        pool_config: config specifying all images in pool
        test_ops_parallely: test operations in parallel/sequential
        client: client node
        pool_type: replicated or ec pool
        **kw: any other args
    """
    try:
        pdb.set_trace()
        multi_image_config = getdict(pool_config)
        images = list(multi_image_config.keys())

        # log.info(f"Run IOs and verify rbd status for images in pool {pool}")
        # rc = wrapper_for_image_ops(
        #     rbd=rbd,
        #     pool=pool,
        #     image_config=multi_image_config,
        #     client=client,
        #     ops_module="ceph.rbd.workflows.rbd",
        #     ops_method="run_io_and_check_rbd_status",
        #     test_ops_parallely=test_ops_parallely,
        # )
        # if rc:
        #     log.error(f"Run IOs and verify rbd status failed for pool {pool}")
        #     return 1

        image_config = dict()
        for index, image in enumerate(images):
            if index % 2 == 0:
                snap_spec = {f"{image}_snap": [f"{image}_clone"]}
                image_config.update({image: {"snap_spec": snap_spec}})

        pdb.set_trace()
        log.info(
            f"Creating snaps and clones for every alternate image in the pool {pool}"
        )
        rc = wrapper_for_image_ops(
            rbd=rbd,
            pool=pool,
            image_config=image_config,
            client=client,
            ops_module="ceph.rbd.workflows.snap_clone_operations",
            ops_method="create_snaps_and_clones",
            test_ops_parallely=test_ops_parallely,
        )
        if rc:
            log.error(
                f"Create snaps and clones for every alternate image failed for pool {pool}"
            )
            return 1

        # pdb.set_trace()
        # log.info(f"Move images to trash and verify for pool {pool}")
        # rc = wrapper_for_image_ops(
        #     rbd=rbd,
        #     pool=pool,
        #     image_config=multi_image_config,
        #     client=client,
        #     ops_module="ceph.rbd.workflows.rbd_trash",
        #     ops_method="move_image_to_trash_and_verify",
        #     test_ops_parallely=test_ops_parallely,
        # )
        # if rc:
        #     log.error(f"Move images to trash and verify failed for pool {pool}")
        #     return 1

        # pdb.set_trace()
        # log.info(f"Trash purge all images in pool {pool} and verify")
        images_without_snaps_and_clones = [
            image for image in images if image not in image_config.keys()
        ]
        # rc = purge_images_and_verify(
        #     rbd=rbd,
        #     pool=pool,
        #     test_ops_parallely=test_ops_parallely,
        #     images_to_be_purged=images_without_snaps_and_clones,
        # )
        # pdb.set_trace()
        # if rc:
        #     log.error(f"Purge images in trash failed for pool {pool}")
        #     return 1

        log.info(
            f"Restore images from trash, create new images and verify for pool {pool}"
        )
        rc = restore_and_create_new_image(
            rbd,
            pool,
            pool_type,
            pool_config,
            image_config,
            client,
            multi_image_config,
            images_without_snaps_and_clones,
            test_ops_parallely,
            **kw,
        )

        if rc:
            log.error(
                f"Restore images from trash and create new images failed for pool {pool}"
            )
            return 1

        pdb.set_trace()
        log.info(f"Purge images by specifying expired-at for pool {pool}")

        curr_time = datetime.now(timezone(timedelta(hours=-5), "EST"))
        image_config = dict()
        for index, image in enumerate(images_without_snaps_and_clones):
            if index % 2 == 0:
                snap_spec = {f"{image}_snap": [f"{image}_clone"]}
                req_date = curr_time + timedelta(minutes=5)
                req_date = req_date.strftime("%a %b %d %H:%M:%S %Z %Y")
                image_config.update({image: {"expires-at": req_date}})

        minutes_before = 5 * (len(images_without_snaps_and_clones) / 2)
        date_before = curr_time + timedelta(minutes=minutes_before)
        # date_before = date_before.strftime("%a %b %d %H:%M:%S %Z %Y")

        rc = wrapper_for_image_ops(
            rbd=rbd,
            pool=pool,
            image_config=image_config,
            client=client,
            ops_module="ceph.rbd.workflows.rbd_trash",
            ops_method="move_image_to_trash_and_verify",
            test_ops_parallely=test_ops_parallely,
        )

        if rc:
            log.error(
                f"Move images to trash and verify with expired-at failed for pool {pool}"
            )
            return 1

        log.info(
            f"Trash purge all images before expiry date {date_before} in pool {pool} and verify"
        )
        images_to_be_purged = [
            image
            for image, config in image_config
            if datetime.strptime(config.get("expires-at"), "%a %b %d %H:%M:%S %Z %Y")
            < date_before
        ]
        rc = purge_images_and_verify(
            rbd=rbd,
            pool=pool,
            test_ops_parallely=test_ops_parallely,
            expired_before=date_before,
            images_to_be_purged=images_to_be_purged,
        )
        pdb.set_trace()
        if rc:
            log.error(f"Purge images in trash with expired-at failed for pool {pool}")
            return 1

        log.info(
            f"Restore images from trash, create new images and verify for pool {pool}"
        )
        rc = restore_and_create_new_image(
            rbd,
            pool,
            pool_type,
            pool_config,
            image_config,
            client,
            multi_image_config,
            images_without_snaps_and_clones,
            test_ops_parallely,
            **kw,
        )

        if rc:
            log.error(
                f"Restore images from trash and create new images failed for pool {pool}"
            )
            return 1

        log.info(
            f"Verifying the functionality of rbd_move_to_trash_on_remove config for pool {pool}"
        )

        cmd = "ceph config set client rbd_move_to_trash_on_remove true"
        client.exec_command(cmd=cmd, sudo=True)

        log.info(
            f"Removing images and verifying that they are moved to trash and not deleted for pool {pool}"
        )
        rc = wrapper_for_image_ops(
            rbd=rbd,
            pool=pool,
            image_config=image_config,
            client=client,
            ops_module="ceph.rbd.workflows.rbd_trash",
            ops_method="remove_single_image_and_verify",
            test_ops_parallely=test_ops_parallely,
        )

        if rc:
            log.error(f"Error while removing images for pool {pool}")
            return 1

        log.info(
            f"Verifying whether images are present in trash after deletion for pool {pool}"
        )
        rc = wrapper_for_image_ops(
            rbd=rbd,
            pool=pool,
            image_config=image_config,
            client=client,
            ops_module="ceph.rbd.workflows.rbd_trash",
            ops_method="is_image_present_in_trash",
            test_ops_parallely=test_ops_parallely,
        )

        # log.info(f"List images in the pool {pool} and verify")
        # rc = list_images_and_verify_with_input(rbd=rbd, pool=pool, images=images)
        # if rc:
        #     log.error(f"List images and verify failed for pool {pool}")
        #     return 1
        # log.info(f"List images and verify success for pool {pool}")

        # log.info(f"Get image info for images in pool {pool} and verify")
        # rc = wrapper_for_image_ops(
        #     rbd=rbd,
        #     pool=pool,
        #     image_config=multi_image_config,
        #     ops_module="ceph.rbd.workflows.rbd",
        #     ops_method="get_image_info_and_verify_for_single_image",
        #     test_ops_parallely=test_ops_parallely,
        # )
        # if rc:
        #     log.error(f"Image info is not as expected for pool {pool}")
        #     return 1
        # log.info(f"Get image info and verify success for pool {pool}")

        # log.info(f"Resize images in pool {pool} and verify")
        # image_config = dict()
        # for image, image_conf in multi_image_config.items():
        #     size = image_conf.get("size")
        #     resize_to = str(int(size[:-1]) * 2) + size[-1]
        #     image_config.update({image: {"resize_to": resize_to}})
        # rc = wrapper_for_image_ops(
        #     rbd=rbd,
        #     pool=pool,
        #     image_config=image_config,
        #     ops_module="ceph.rbd.workflows.rbd",
        #     ops_method="resize_single_image_and_verify",
        #     test_ops_parallely=test_ops_parallely,
        # )
        # if rc:
        #     log.error(f"Resize image and verify failed for pool {pool}")
        #     return 1
        # log.info(f"Resize image and verify success for pool {pool}")

        # log.info(f"Rename images in pool {pool} and verify")
        # image_rename_config = {
        #     k: {"rename_to": k + "_renamed"} for k in multi_image_config.keys()
        # }
        # rc = wrapper_for_image_ops(
        #     rbd=rbd,
        #     pool=pool,
        #     image_config=image_rename_config,
        #     ops_module="ceph.rbd.workflows.rbd",
        #     ops_method="rename_single_image_and_verify",
        #     test_ops_parallely=test_ops_parallely,
        # )
        # if rc:
        #     log.error(f"Rename image and verify failed for pool {pool}")
        #     return 1
        # log.info(f"Rename image and verify success for pool {pool}")

        # image_keys = list(multi_image_config.keys())
        # for image in image_keys:
        #     new_name = image_rename_config[image]["rename_to"]
        #     multi_image_config[new_name] = multi_image_config.pop(image)

        # new_pool = pool + "_new"
        # log.info(f"Create pool {new_pool} to perform pool copy operation")

        # config = kw.get("config", {})
        # is_ec_pool = True if "ec" in pool_type else False
        # new_pool_config = deepcopy(pool_config)
        # if is_ec_pool:
        #     data_pool_new = pool_config["data_pool"] + "_new"
        #     new_pool_config["data_pool"] = data_pool_new
        # rc = create_single_pool_and_images(
        #     config=config,
        #     pool=new_pool,
        #     pool_config=new_pool_config,
        #     client=client,
        #     cluster="ceph",
        #     rbd=rbd,
        #     ceph_version=int(config.get("rhbuild")[0]),
        #     is_ec_pool=is_ec_pool,
        #     is_secondary=False,
        #     do_not_create_image=True,
        # )
        # if rc:
        #     log.error(f"Creation of pool {new_pool} failed")
        #     return rc

        # log.info(f"Copy images from pool {pool} to another pool {new_pool} and verify")
        # rc = wrapper_for_image_ops(
        #     rbd=rbd,
        #     pool=pool,
        #     image_config=multi_image_config,
        #     new_pool=new_pool,
        #     ops_module="ceph.rbd.workflows.rbd",
        #     ops_method="copy_single_image_and_verify",
        #     test_ops_parallely=test_ops_parallely,
        # )
        # if rc:
        #     log.error(f"Copy images from pool {pool} failed")
        #     return 1
        # log.info(f"Copy images from pool {pool} success")

        # exec_cmd(node=client, cmd="ceph config set mon mon_allow_pool_delete true")
        # sleep(20)

        # rc = exec_cmd(
        #     cmd=f"ceph osd pool delete {new_pool} {new_pool} "
        #     "--yes-i-really-really-mean-it",
        #     node=client,
        # )
        # if rc:
        #     log.error(f"Error while deleting new pool {new_pool}")
        #     if test_ops_parallely:
        #         raise Exception(f"Error while deleting new pool {new_pool}")
        #     return 1

        # if is_ec_pool:
        #     rc = exec_cmd(
        #         cmd=f"ceph osd pool delete {data_pool_new} {data_pool_new} "
        #         "--yes-i-really-really-mean-it",
        #         node=client,
        #     )
        #     if rc:
        #         log.error(f"Error while deleting new pool {data_pool_new}")
        #         if test_ops_parallely:
        #             raise Exception(f"Error while deleting new pool {data_pool_new}")
        #         return 1

        # log.info(f"Test image feature disable and enable operations for pool {pool}")
        # rc = wrapper_for_image_ops(
        #     rbd=rbd,
        #     pool=pool,
        #     image_config=multi_image_config,
        #     ops_module="ceph.rbd.workflows.rbd",
        #     ops_method="feature_ops_single_image",
        #     test_ops_parallely=test_ops_parallely,
        # )
        # if rc:
        #     log.error(
        #         f"Image feature disable and enable operations failed for pool {pool}"
        #     )
        #     return 1

        # log.info(f"Perform image metadata operations and verify for images in pool {pool}")
        # rc = wrapper_for_image_ops(
        #     rbd=rbd,
        #     pool=pool,
        #     image_config=multi_image_config,
        #     client=client,
        #     ops_module="ceph.rbd.workflows.rbd",
        #     ops_method="metadata_ops_single_image",
        #     test_ops_parallely=test_ops_parallely,
        # )
        # if rc:
        #     log.error(f"Perform image metadata operations failed for pool {pool}")
        #     return 1

        # log.info(f"Remove images and verify for pool {pool}")
        # rc = wrapper_for_image_ops(
        #     rbd=rbd,
        #     pool=pool,
        #     image_config=multi_image_config,
        #     client=client,
        #     ops_module="ceph.rbd.workflows.rbd",
        #     ops_method="remove_single_image_and_verify",
        #     test_ops_parallely=test_ops_parallely,
        # )
        # if rc:
        #     log.error(f"Remove images and verify rbd status failed for pool {pool}")
        #     return 1
    except Exception as e:
        log.error(
            f"Testing image trash operations failed for pool {pool} with error {e}"
        )
        if test_ops_parallely:
            raise Exception(
                f"Testing image trash operations failed for pool {pool} with error {e}"
            )
        return 1


def test_rbd_trash_operations(rbd_obj, client, **kw):
    """
    Test all image trash operations on each pool created

    Args:
        rbd_obj: rbd object
        client: client node
        **kw: key word arguments
    """
    log.info(f"Performing image trash operations for pool type {rbd_obj['pool_types']}")
    rbd = rbd_obj.get("rbd")
    for pool_type in rbd_obj.get("pool_types"):
        rbd_config = kw.get("config", {}).get(pool_type, {})
        multi_pool_config = getdict(rbd_config)
        test_ops_parallely = rbd_config.get("test_ops_parallely", False)
        if test_ops_parallely:
            with parallel() as p:
                for pool, pool_config in multi_pool_config.items():
                    p.spawn(
                        test_rbd_trash_operations_for_pool,
                        rbd=rbd,
                        pool=pool,
                        pool_config=pool_config,
                        test_ops_parallely=test_ops_parallely,
                        client=client,
                        pool_type=pool_type,
                        **kw,
                    )
        else:
            for pool, pool_config in multi_pool_config.items():
                rc = test_rbd_trash_operations_for_pool(
                    rbd=rbd,
                    pool=pool,
                    pool_config=pool_config,
                    test_ops_parallely=test_ops_parallely,
                    client=client,
                    pool_type=pool_type,
                    **kw,
                )
                if rc:
                    log.error(f"Image trash operations testing failed for pool {pool}")
                    return 1
    return 0


def run(**kw):
    """RBD image trash operations testing.

    Args:
        **kw: test data
    """
    pdb.set_trace()
    log.info("Running test CEPH-83582444 - Testing RBD image trash operations.")

    try:
        if kw.get("client_node"):
            client = get_node_by_id(kw.get("ceph_cluster"), kw.get("client_node"))
        else:
            client = kw.get("ceph_cluster").get_nodes(role="client")[0]
        rbd_obj = initial_rbd_config(**kw)
        pool_types = rbd_obj.get("pool_types")
        ret_val = test_rbd_trash_operations(rbd_obj=rbd_obj, client=client, **kw)
    except Exception as e:
        log.error(f"Testing RBD image trash operations failed with the error {str(e)}")
        ret_val = 1
    finally:
        cluster_name = kw.get("ceph_cluster", {}).name
        if "rbd_obj" not in locals():
            rbd_obj = Rbd(client)
        obj = {cluster_name: rbd_obj}
        cleanup(pool_types=pool_types, multi_cluster_obj=obj, **kw)
    return ret_val
