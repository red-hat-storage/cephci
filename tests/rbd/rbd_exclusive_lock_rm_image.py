from ceph.parallel import parallel
from tests.rbd.exceptions import RbdBaseException
from tests.rbd.rbd_utils import initial_rbd_config, rbd_remove_image_negative_validate
from utility.log import Log

log = Log(__name__)


def rbd_exclusive_verify(rbd, pool_type, **kw):
    """
    Run exclusive-lock test and verify
    Args:
        rbd: rbd object
        pool_type: pool type (ec_pool_config or rep_pool_config)
        **kw: Test data
    """
    pool = kw["config"][pool_type]["pool"]
    image = kw["config"][pool_type]["image"]
    config = kw.get("config")

    try:
        with parallel() as p:
            p.spawn(
                rbd.exec_cmd,
                cmd=f"rbd bench --io-type write --io-total {config.get('io-total')} {pool}/{image}",
            )
            p.spawn(rbd_remove_image_negative_validate, rbd, pool, image)
        return 0

    except RbdBaseException as error:
        log.error(error)
        return 1

    finally:
        rbd.clean_up(pools=[kw["config"][pool_type]["pool"]])


def run(**kw):
    """If the image has an exclusive-lock on it, verify the behaviour on delayed deletion.
    This module verifies rbd remove when image is enabled with exclusive-lock feature
    Args:
        kw: test data
    Returns:
        int: The return value. 0 for success, 1 otherwise
    Pre-requisites :
    We need atleast one client node with ceph-common package,
    conf and keyring files
    Test cases covered -
    1) CEPH-11408- Create a pool and an image, ensure that image has exclusive lock feature enabled
    2) an RBD image has a exclusive lock on it, When IO is progress, remove an image and check the behaviour
    3) Verify the behaviour in this scenario. remove image should fail due to the exclusive lock enabled
    """
    log.info("Running rbd remove operation during IO in progress")
    rbd_obj = initial_rbd_config(**kw)
    if rbd_obj:
        if "rbd_reppool" in rbd_obj:
            log.info("Executing test on Replication pool")
            if rbd_exclusive_verify(
                rbd_obj.get("rbd_reppool"), "rep_pool_config", **kw
            ):
                return 1
        if "rbd_ecpool" in rbd_obj:
            log.info("Executing test on EC pool")
            if rbd_exclusive_verify(rbd_obj.get("rbd_ecpool"), "ec_pool_config", **kw):
                return 1
    return 0
