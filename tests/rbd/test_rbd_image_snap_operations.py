# This test case is to automate image snap operations
# in scale for baremetal configuration. Image snap operations
# covered as part of this test are - snap create
# and verify the same
# for multiple pools and images at scale.

from ceph.rbd.initial_config import initial_rbd_config
from ceph.rbd.utils import getdict
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.snap_clone_operations import wrapper_for_image_snap_ops
from ceph.utils import get_node_by_id
from cli.rbd.rbd import Rbd
from utility.log import Log

log = Log(__name__)


def test_rbd_image_snap_operations(rbd_obj, **kw):
    """
    This method tests the image snap operations in scale controlled by test_ops_parallely param.
    Steps:
    1. Get the config data for different pool_types
    2. For each pool_type get pool, pool_config
    3. For each pool_config get image_config
    4. For each image_config get the num_of_snaps to create snaps parallely or sequentially

    Args:
        rbd_obj: RBD obj from initial RBD config
        kw: any other kw args
    """
    log.info(f"Performing image snap operations for pool type {rbd_obj['pool_types']}")
    rbd = rbd_obj.get("rbd")
    for pool_type in rbd_obj.get("pool_types"):
        rbd_config = kw.get("config", {}).get(pool_type, {})
        multi_pool_config = getdict(rbd_config)
        test_ops_parallely = rbd_config.get("test_ops_parallely", False)
        for pool, pool_config in multi_pool_config.items():
            if "data_pool" in pool_config.keys():
                _ = pool_config.pop("data_pool")
            for image, image_config in pool_config.items():
                num_of_snaps = image_config.get("num_of_snaps", 2)
                snap_names = [
                    f"snap_{snap_suffix}" for snap_suffix in range(num_of_snaps)
                ]
                # 1. Perform snap create operation, parallel is also supported
                log.info("Test image snap create operation")
                rc = wrapper_for_image_snap_ops(
                    rbd=rbd,
                    pool=pool,
                    image=image,
                    snap_names=snap_names,
                    ops_module="ceph.rbd.workflows.snap_clone_operations",
                    ops_method="snap_create_list_and_verify",
                    test_ops_parallely=test_ops_parallely,
                )
                if rc:
                    log.error(f"Snap create operations on the {pool}/{image} failed")
                    return 1
                else:
                    log.info(f"Snap create operations on the {pool}/{image} succeeded")

                # 2. Perform snap clone operation i.e which in turn calls
                # a. protect snap b. clone snap c. flatten snap d.unprotect snap
                # parallel is also supported
                log.info("Test image snap clone operation")
                rc = wrapper_for_image_snap_ops(
                    rbd=rbd,
                    pool=pool,
                    image=image,
                    snap_names=snap_names,
                    ops_module="ceph.rbd.workflows.snap_clone_operations",
                    ops_method="clone_ops",
                    test_ops_parallely=test_ops_parallely,
                )
                if rc:
                    log.error(f"Snap clone operations on the {pool}/{image} failed")
                    return 1
                else:
                    log.info(f"Snap clone operations on the {pool}/{image} succeeded")

                # 3. Remove the snaps, paralle is also supported
                log.info("Test image snap rm operation")
                rc = wrapper_for_image_snap_ops(
                    rbd=rbd,
                    pool=pool,
                    image=image,
                    snap_names=snap_names,
                    ops_module="ceph.rbd.workflows.snap_clone_operations",
                    ops_method="remove_snap_and_verify",
                    test_ops_parallely=test_ops_parallely,
                )
                if rc:
                    log.error(f"Snap remove operations on the {pool}/{image} failed")
                    return 1
                else:
                    log.info(f"Snap remove operations on the {pool}/{image} succeeded")

    return 0


def run(**kw):
    """RBD image operations testing.

    Args:
        **kw: test data
    """
    log.info("Running test - Testing RBD image operations.")

    try:
        if kw.get("client_node"):
            client = get_node_by_id(kw.get("ceph_cluster"), kw.get("client_node"))
        else:
            client = kw.get("ceph_cluster").get_nodes(role="client")[0]
        rbd_obj = initial_rbd_config(**kw)
        pool_types = rbd_obj.get("pool_types")
        ret_val = test_rbd_image_snap_operations(rbd_obj=rbd_obj, client=client, **kw)
    except Exception as e:
        log.error(f"Testing RBD image snap operations failed with the error {str(e)}")
        ret_val = 1
    finally:
        cluster_name = kw.get("ceph_cluster", {}).name
        if "rbd_obj" not in locals():
            rbd_obj = Rbd(client)
        obj = {cluster_name: rbd_obj}
        cleanup(pool_types=pool_types, multi_cluster_obj=obj, **kw)
    return ret_val
