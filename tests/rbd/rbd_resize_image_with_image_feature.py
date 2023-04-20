"""Module to verify resize operations while changing the image feature.

Test case covered -
CEPH-9861 - Perform resize operations while changing the image feature.

Pre-requisites :
1. Cluster must be up and running with capacity to create pool
2. We need atleast one client node with ceph-common package,
   conf and keyring files

Test Case Flow:
1. Create a pool
2. Create an image
3. Run Ios on Image
4. While disabling the image feature, perform image resize (increase size) operation
    Image size should be increased with disable image feature
5. While enabling the image feature, perform image resize (decrease size) operation
    Image size should be shrinked with enable image feature
6. Repeat steps 1 to 5 for ecpool
"""


from ceph.parallel import parallel
from tests.rbd.exceptions import RbdBaseException
from tests.rbd.rbd_utils import (
    initial_rbd_config,
    verify_image_feature_exist,
    verify_image_size,
)
from utility.log import Log
from utility.utils import run_fio

log = Log(__name__)


def resize_image_with_image_feature(rbd, pool_type, **kw):
    """
        Resize an image while changing the image feature.
    Args:
        rbd: RBD object
        pool_type: pool type (ec_pool_config or rep_pool_config)
        **kw: test data
    """

    try:
        pool = kw["config"][pool_type]["pool"]
        image = kw["config"][pool_type]["image"]
        feature_name = kw["config"]["image_feature"]
        size_grow = kw["config"]["size_increase"]
        size_shrink = kw["config"]["size_decrease"]

        run_fio(image_name=image, pool_name=pool, client_node=rbd.ceph_client)

        with parallel() as p:
            p.spawn(
                rbd.toggle_image_feature, pool, image, feature_name, action="disable"
            )
            p.spawn(
                rbd.image_resize,
                pool_name=pool,
                image_name=image,
                size=size_grow,
                flag="increase",
            )

        verify_image_size(rbd, pool=pool, image=image, size=size_grow)
        if verify_image_feature_exist(rbd, pool, image, feature_name):
            log.info(f"{feature_name} is disabled successfully")
        else:
            log.error(f"{feature_name} is not disabled")
            return 1

        with parallel() as p:
            p.spawn(
                rbd.toggle_image_feature, pool, image, feature_name, action="enable"
            )
            p.spawn(
                rbd.image_resize, pool_name=pool, image_name=image, size=size_shrink
            )

        verify_image_size(rbd, pool=pool, image=image, size=size_shrink)
        if verify_image_feature_exist(rbd, pool, image, feature_name):
            log.error(f"Feature {feature_name} is not enabled")
            return 1
        else:
            log.info(f"{feature_name} is enabled successfully")

        return 0

    except RbdBaseException as error:
        log.error(error.message)
        return 1

    finally:
        rbd.clean_up(pools=[kw["config"][pool_type]["pool"]])


def run(**kw):
    """Perform resize operations while changing the image feature.
    This test disable and enable the image feature while image resize operation in progress
    Args:
        kw: test data
    Returns:
        int: The return value. 0 for success, 1 otherwise

    """
    log.info("CEPH-9861 - Perform resize operations while changing the image feature")
    rbd_obj = initial_rbd_config(**kw)
    if rbd_obj:
        log.info("Executing test on Replication pool")
        if resize_image_with_image_feature(
            rbd_obj.get("rbd_reppool"), "rep_pool_config", **kw
        ):
            return 1
        log.info("Executing test on EC pool")
        if resize_image_with_image_feature(
            rbd_obj.get("rbd_ecpool"), "ec_pool_config", **kw
        ):
            return 1
    return 0
