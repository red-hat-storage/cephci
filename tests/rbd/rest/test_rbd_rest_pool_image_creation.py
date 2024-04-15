from ceph.rbd.initial_config import update_config
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.rbd import run_io_and_check_rbd_status
from ceph.utils import get_node_by_id
from cli.rbd.rbd import Rbd
from rest.common.utils.rest import rest
from rest.workflows.rbd.rbd import create_image_in_pool_verify
from utility.log import Log

log = Log(__name__)


def test_rest_image_creation_in_pool(client, **kw):
    """
    Tests the pool and image creation using REST APIs
    Args:
        client: rbd client obj
        **kw: test data
    """
    _rest = rest()
    config = {}
    rep_pool_config = kw["config"].get("rep_pool_config", None)
    if rep_pool_config:
        config.update({"rep_pool_config": rep_pool_config})
    ec_pool_config = kw["config"].get("ec_pool_config", None)
    if ec_pool_config:
        config.update({"ec_pool_config": ec_pool_config})
    for pool_type, pool_config in config.items():
        for pool, image_config in pool_config.items():
            rc = create_image_in_pool_verify(
                rest=_rest,
                client=client,
                pool=pool,
                pool_type=pool_type,
                image_config=image_config,
            )
            if rc:
                log.error("REST operation of pool and image creation failed")
                return 1

            # run IO on the image created by REST API
            rbd = Rbd(client)
            if pool_type == "ec_pool_config":
                _ = image_config.pop("data_pool", None)
            for image, image_conf in image_config.items():
                io_rc = run_io_and_check_rbd_status(
                    rbd=rbd,
                    pool=pool,
                    image=image,
                    client=client,
                    image_conf=image_conf,
                )
                if io_rc:
                    log.error(f"IO on image {image} failed")
                    return 1

    return 0


def run(**kw):
    """RBD pool and image creation using REST APIs.

    Args:
        **kw: test data
    """
    log.info(
        "Running test CEPH-83587957 - Testing RBD pool and image creation using REST APIs."
    )

    try:
        if kw.get("client_node"):
            client = get_node_by_id(kw.get("ceph_cluster"), kw.get("client_node"))
        else:
            client = kw.get("ceph_cluster").get_nodes(role="client")[0]
        kw["do_not_create_image"] = True
        pool_types = update_config(**kw)
        log.info(f"Pool types got is {pool_types}")
        ret_val = test_rest_image_creation_in_pool(client=client, **kw)
    except Exception as e:
        log.error(
            f"Testing RBD pool and image creation using REST APIs failed with the error {str(e)}"
        )
        ret_val = 1
    finally:
        cluster_name = kw.get("ceph_cluster", {}).name
        if "rbd_obj" not in locals():
            rbd_obj = Rbd(client)
        rbd_clenup = {"rbd_obj": rbd_obj, "client": client}
        obj = {cluster_name: rbd_clenup}
        cleanup(pool_types=pool_types, multi_cluster_obj=obj, **kw)

    return ret_val
