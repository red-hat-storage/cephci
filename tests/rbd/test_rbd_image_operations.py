"""RBD Image operations at scale

Test Case Covered:
CEPH-83582443 - [Baremetal] [ScaleTest] - Test all rbd image operations at scale

Steps:
1) Create an RHCS cluster on baremetal nodes as specified in setup. Execute IOs to
fill the cluster to a decent percentage
2) Create 10 RBD pools and 100 images per pool parallely on this cluster
3) List all the images in every pool and verify that they match the configuration
specified in the test suite
4) Get image info for all the images and verify that the specified information is
present and correct
5) Resize all images parallely and verify
6) Rename all images parallely and verify
7) Create new pools and copy images from current pool to new pool parallely and verify
8) Enable and disable all image features such as "object-map", "deep-flatten",
"journaling", "exclusive-lock" and verify that they are disabled and enabled
9) Run IOs on the images parallely for a specified size and verify rbd status that
the image has watchers when IOs are in progress
10) Set metadata for all images parallely and verify
11) Remove all images parallely and verify

Pre-requisites :
- need client node with ceph-common package, conf and keyring files
- FIO should be installed on the client.

Environment and limitations:
- The cluster should have enough storage to create atleast 1000 images with 4G each
- cluster/global-config-file: config/reef/baremetal/mero_conf.yaml
- Should be Bare-metal.
"""

from copy import deepcopy
from time import sleep

from ceph.parallel import parallel
from ceph.rbd.initial_config import initial_rbd_config
from ceph.rbd.utils import exec_cmd, getdict
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.rbd import (
    create_single_pool_and_images,
    list_images_and_verify_with_input,
    wrapper_for_image_ops,
)
from ceph.utils import get_node_by_id
from cli.rbd.rbd import Rbd
from utility.log import Log

log = Log(__name__)


def test_rbd_image_operations_for_pool(
    rbd, pool, pool_config, test_ops_parallely, client, pool_type, **kw
):
    """Test image operations like list, image info, resize, rename, copy,
    feature enable, disable, rbd status and remove for all images in the
    given pool type

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
        multi_image_config = getdict(pool_config)
        images = list(multi_image_config.keys())
        log.info(f"List images in the pool {pool} and verify")
        rc = list_images_and_verify_with_input(rbd=rbd, pool=pool, images=images)
        if rc:
            log.error(f"List images and verify failed for pool {pool}")
            return 1
        log.info(f"List images and verify success for pool {pool}")

        log.info(f"Get image info for images in pool {pool} and verify")
        rc = wrapper_for_image_ops(
            rbd=rbd,
            pool=pool,
            image_config=multi_image_config,
            ops_module="ceph.rbd.workflows.rbd",
            ops_method="get_image_info_and_verify_for_single_image",
            test_ops_parallely=test_ops_parallely,
        )
        if rc:
            log.error(f"Image info is not as expected for pool {pool}")
            return 1
        log.info(f"Get image info and verify success for pool {pool}")

        log.info(f"Resize images in pool {pool} and verify")
        image_config = dict()
        for image, image_conf in multi_image_config.items():
            size = image_conf.get("size")
            resize_to = str(int(size[:-1]) * 2) + size[-1]
            image_config.update({image: {"resize_to": resize_to}})
        rc = wrapper_for_image_ops(
            rbd=rbd,
            pool=pool,
            image_config=image_config,
            ops_module="ceph.rbd.workflows.rbd",
            ops_method="resize_single_image_and_verify",
            test_ops_parallely=test_ops_parallely,
        )
        if rc:
            log.error(f"Resize image and verify failed for pool {pool}")
            return 1
        log.info(f"Resize image and verify success for pool {pool}")

        log.info(f"Rename images in pool {pool} and verify")
        image_rename_config = {
            k: {"rename_to": k + "_renamed"} for k in multi_image_config.keys()
        }
        rc = wrapper_for_image_ops(
            rbd=rbd,
            pool=pool,
            image_config=image_rename_config,
            ops_module="ceph.rbd.workflows.rbd",
            ops_method="rename_single_image_and_verify",
            test_ops_parallely=test_ops_parallely,
        )
        if rc:
            log.error(f"Rename image and verify failed for pool {pool}")
            return 1
        log.info(f"Rename image and verify success for pool {pool}")

        image_keys = list(multi_image_config.keys())
        for image in image_keys:
            new_name = image_rename_config[image]["rename_to"]
            multi_image_config[new_name] = multi_image_config.pop(image)

        new_pool = pool + "_new"
        log.info(f"Create pool {new_pool} to perform pool copy operation")

        config = kw.get("config", {})
        is_ec_pool = True if "ec" in pool_type else False
        new_pool_config = deepcopy(pool_config)
        if is_ec_pool:
            data_pool_new = pool_config["data_pool"] + "_new"
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
            do_not_create_image=True,
        )
        if rc:
            log.error(f"Creation of pool {new_pool} failed")
            return rc

        log.info(f"Copy images from pool {pool} to another pool {new_pool} and verify")
        rc = wrapper_for_image_ops(
            rbd=rbd,
            pool=pool,
            image_config=multi_image_config,
            new_pool=new_pool,
            ops_module="ceph.rbd.workflows.rbd",
            ops_method="copy_single_image_and_verify",
            test_ops_parallely=test_ops_parallely,
        )
        if rc:
            log.error(f"Copy images from pool {pool} failed")
            return 1
        log.info(f"Copy images from pool {pool} success")

        exec_cmd(node=client, cmd="ceph config set mon mon_allow_pool_delete true")
        sleep(20)

        rc = exec_cmd(
            cmd=f"ceph osd pool delete {new_pool} {new_pool} "
            "--yes-i-really-really-mean-it",
            node=client,
        )
        if rc:
            log.error(f"Error while deleting new pool {new_pool}")
            if test_ops_parallely:
                raise Exception(f"Error while deleting new pool {new_pool}")
            return 1

        if is_ec_pool:
            rc = exec_cmd(
                cmd=f"ceph osd pool delete {data_pool_new} {data_pool_new} "
                "--yes-i-really-really-mean-it",
                node=client,
            )
            if rc:
                log.error(f"Error while deleting new pool {data_pool_new}")
                if test_ops_parallely:
                    raise Exception(f"Error while deleting new pool {data_pool_new}")
                return 1

        log.info(f"Test image feature disable and enable operations for pool {pool}")
        rc = wrapper_for_image_ops(
            rbd=rbd,
            pool=pool,
            image_config=multi_image_config,
            ops_module="ceph.rbd.workflows.rbd",
            ops_method="feature_ops_single_image",
            test_ops_parallely=test_ops_parallely,
        )
        if rc:
            log.error(
                f"Image feature disable and enable operations failed for pool {pool}"
            )
            return 1

        log.info(f"Run IOs and verify rbd status for images in pool {pool}")
        rc = wrapper_for_image_ops(
            rbd=rbd,
            pool=pool,
            image_config=multi_image_config,
            client=client,
            ops_module="ceph.rbd.workflows.rbd",
            ops_method="run_io_and_check_rbd_status",
            test_ops_parallely=test_ops_parallely,
        )
        if rc:
            log.error(f"Run IOs and verify rbd status failed for pool {pool}")
            return 1

        log.info(
            f"Perform image metadata operations and verify for images in pool {pool}"
        )
        rc = wrapper_for_image_ops(
            rbd=rbd,
            pool=pool,
            image_config=multi_image_config,
            client=client,
            ops_module="ceph.rbd.workflows.rbd",
            ops_method="metadata_ops_single_image",
            test_ops_parallely=test_ops_parallely,
        )
        if rc:
            log.error(f"Perform image metadata operations failed for pool {pool}")
            return 1

        log.info(f"Remove images and verify for pool {pool}")
        rc = wrapper_for_image_ops(
            rbd=rbd,
            pool=pool,
            image_config=multi_image_config,
            client=client,
            ops_module="ceph.rbd.workflows.rbd",
            ops_method="remove_single_image_and_verify",
            test_ops_parallely=test_ops_parallely,
        )
        if rc:
            log.error(f"Remove images and verify rbd status failed for pool {pool}")
            return 1
    except Exception as e:
        log.error(f"Testing image operations failed for pool {pool} with error {e}")
        if test_ops_parallely:
            raise Exception(
                f"Testing image operations failed for pool {pool} with error {e}"
            )
        return 1


def test_rbd_image_operations(rbd_obj, client, **kw):
    """
    Test all image operations on each pool created

    Args:
        rbd_obj: rbd object
        client: client node
        **kw: key word arguments
    """
    log.info(f"Performing image operations for pool type {rbd_obj['pool_types']}")
    rbd = rbd_obj.get("rbd")
    for pool_type in rbd_obj.get("pool_types"):
        rbd_config = kw.get("config", {}).get(pool_type, {})
        multi_pool_config = getdict(rbd_config)
        test_ops_parallely = rbd_config.get("test_ops_parallely", False)
        if test_ops_parallely:
            with parallel() as p:
                for pool, pool_config in multi_pool_config.items():
                    p.spawn(
                        test_rbd_image_operations_for_pool,
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
                rc = test_rbd_image_operations_for_pool(
                    rbd=rbd,
                    pool=pool,
                    pool_config=pool_config,
                    test_ops_parallely=test_ops_parallely,
                    client=client,
                    pool_type=pool_type,
                    **kw,
                )
                if rc:
                    log.error(f"Image operations testing failed for pool {pool}")
                    return 1
    return 0


def run(**kw):
    """RBD image operations testing.

    Args:
        **kw: test data
    """
    log.info("Running test CEPH-83582443 - Testing RBD image operations.")

    try:
        if kw.get("client_node"):
            client = get_node_by_id(kw.get("ceph_cluster"), kw.get("client_node"))
        else:
            client = kw.get("ceph_cluster").get_nodes(role="client")[0]
        rbd_obj = initial_rbd_config(**kw)
        pool_types = rbd_obj.get("pool_types")
        ret_val = test_rbd_image_operations(rbd_obj=rbd_obj, client=client, **kw)
    except Exception as e:
        log.error(f"Testing RBD image operations failed with the error {str(e)}")
        ret_val = 1
    finally:
        cluster_name = kw.get("ceph_cluster", {}).name
        if "rbd_obj" not in locals():
            rbd_obj = Rbd(client)
        obj = {cluster_name: rbd_obj}
        cleanup(pool_types=pool_types, multi_cluster_obj=obj, **kw)
    return ret_val
