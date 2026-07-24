"""Module to verify disable image feature with meta-data set on image.

Pre-requisites :
We need cluster configured with atleast one client node with ceph-common package,
conf and keyring files

Test case covered -
CEPH-9864 - Disable some existing Image Feature on Image having metadata set on it,
with IO in progress from Client Side

Test Case Flow:
1. Create a Replicated pool
2. Create an image
3. Run Ios on an Image
4. Set image meta on the image
5. verify that image meta is set properly.
6. Initiate IOs from client
7. While running the IOs disable the existing image feature.
8. Image features should be disable properly
8. Repeat steps 1 to 7 for ecpool
"""

from ceph.parallel import parallel
from tests.rbd.exceptions import RbdBaseException
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import run_fio

log = Log(__name__)


def image_metadata_feature_disable(rbd, pool_type, **kw):
    """Disable some existing Image Feature on Image having metadata set on it with IO in-progress.

    Args:
        rbd: RBD object
        pool_type: pool type (ec_pool_config or rep_pool_config)
        **kw: test data
    """
    pool = kw["config"][pool_type]["pool"]
    image = kw["config"][pool_type]["image"]
    feature_name = kw["config"]["image_feature"]
    image_spec = pool + "/" + image
    value = 120

    try:
        run_fio(image_name=image, pool_name=pool, client_node=rbd.ceph_client)

        # set meta value on image
        rbd.image_meta(
            action="set",
            image_spec=image_spec,
            key="rbd_op_thread_timeout",
            value=value,
        )

        # verify meta value is set on an image
        if value != int(
            rbd.image_meta(
                action="get", image_spec=image_spec, key="rbd_op_thread_timeout"
            )[:-1]
        ):
            log.error(f"Expected Meta value did not set on {image_spec}")
            return 1

        with parallel() as p:
            p.spawn(
                run_fio, image_name=image, pool_name=pool, client_node=rbd.ceph_client
            )
            p.spawn(
                rbd.toggle_image_feature, pool, image, feature_name, action="disable"
            )

        # Verify the feature is disabled
        image_load = rbd.image_info(pool, image)
        if feature_name in image_load.get("features", {}):
            log.error(
                f"Feature {feature_name} is present in the rbd info of {image_spec}."
            )
            return 1
        log.info(f"Image feature {feature_name} on {image} is disabled successfully")
        return 0

    except RbdBaseException as error:
        log.error(error.message)
        return 1

    finally:
        rbd.clean_up(pools=[kw["config"][pool_type]["pool"]])


def run(**kw):
    """Perform feature disable while running IOs.
    verify feature disables on an image having meta set on it while IO operation in progress
    Args:
        kw: test data
    Returns:
        int: The return value. 0 for success, 1 otherwise
    """
    log.info(
        "CEPH-9864 - Running test to verify disable image feature with meta set on it"
    )
    rbd_obj = initial_rbd_config(**kw)
    if rbd_obj:
        log.info("Executing test on Replication pool")
        if image_metadata_feature_disable(
            rbd_obj.get("rbd_reppool"), "rep_pool_config", **kw
        ):
            return 1
        log.info("Executing test on EC pool")
        if image_metadata_feature_disable(
            rbd_obj.get("rbd_ecpool"), "ec_pool_config", **kw
        ):
            return 1
    return 0
