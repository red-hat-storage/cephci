from ceph.parallel import parallel
from tests.rbd.exceptions import RbdBaseException
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import run_fio

log = Log(__name__)


def flatten_image_feature_disable(rbd, pool_type, **kw):
    """
        Flatten the clone image while changing the image feature.
    Args:
        rbd: RBD object
        pool_type: pool type (ec_pool_config or rep_pool_config)
        **kw: test data
    """

    try:
        pool = kw["config"][pool_type]["pool"]
        image = kw["config"][pool_type]["image"]
        snap = kw["config"][pool_type].get("snap", f"{pool}_{image}_snap")
        clone = kw["config"][pool_type].get("clone", f"{pool}_{image}_clone")
        snap_name = f"{pool}/{image}@{snap}"
        feature_name = "fast-diff"
        if kw.get("config").get("rbd_op_thread_timeout"):
            key = "rbd_op_thread_timeout"
            value = kw["config"]["rbd_op_thread_timeout"]
        image_spec = pool + "/" + clone

        run_fio(image_name=image, pool_name=pool, client_node=rbd.ceph_client)

        if rbd.snap_create(pool, image, snap):
            log.error(f"Creation of snapshot {snap} failed")
            return 1

        if rbd.protect_snapshot(snap_name):
            log.error(f"Snapshot protect failed for {snap_name}")
            return 1

        if rbd.create_clone(snap_name, pool, clone):
            log.error(f"Creation of clone {clone} failed")
            return 1

        # set meta data value on clone
        rbd.image_meta(
            action="set",
            image_spec=image_spec,
            key=key,
            value=value,
        )

        # verify meta data value is set on clone image
        if value != int(
            rbd.image_meta(action="get", image_spec=image_spec, key=key)[:-1]
        ):
            log.error(f"Expected Meta value did not set on {image_spec}")
            return 1

        with parallel() as p:
            p.spawn(
                rbd.toggle_image_feature, pool, clone, feature_name, action="disable"
            )
            p.spawn(rbd.flatten_clone, pool, clone)

        # Verify the feature is disabled and not present in the image info
        image_load = rbd.image_info(pool, clone)
        if feature_name in image_load.get("features", {}):
            log.error(
                f"Feature {feature_name} is present in the rbd info of {pool}/{clone}."
            )
            return 1
        else:
            log.debug(f" Image feature {feature_name} disabled successfully")

        return 0

    except RbdBaseException as error:
        log.error(error.message)
        return 1

    finally:
        rbd.clean_up(pools=[kw["config"][pool_type]["pool"]])


def run(**kw):
    """Perform flatten operations while changing the image feature.
    This test disables the image feature while flatten operation in progress
    Args:
        kw: test data
    Returns:
        int: The return value. 0 for success, 1 otherwise
    Test case covered -
    CEPH-9862 - Perform flatten operations while changing the image feature.
    Pre-requisites :
    1. Cluster must be up and running with capacity to create pool
    2. We need atleast one client node with ceph-common package,
       conf and keyring files
    Test Case Flow:
    1. Create a pool
    2. Create an image
    3. Run Ios on an Image
    4. Take a snapshot
    5. Protect a snapshot
    6. clone a snapshot
    7. Set the image meta-data value on clone image and verify the meta-data value set properly.
    8. While changing the image features, perform flatten operations
        Image features should be changed and flatten operation should succeed
    9. Repeat steps 1 to 8 for ecpool
    """
    log.info("Test to disable image feature when flatten operation is performed")
    rbd_obj = initial_rbd_config(**kw)
    if rbd_obj:
        log.info("Executing test on Replication pool")
        if flatten_image_feature_disable(
            rbd_obj.get("rbd_reppool"), "rep_pool_config", **kw
        ):
            return 1
        log.info("Executing test on EC pool")
        if flatten_image_feature_disable(
            rbd_obj.get("rbd_ecpool"), "ec_pool_config", **kw
        ):
            return 1
    return 0
