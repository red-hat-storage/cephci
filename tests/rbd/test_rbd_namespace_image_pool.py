from ceph.rbd.initial_config import initial_rbd_config
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.namespace import create_namespace_and_verify
from ceph.rbd.workflows.rbd import create_single_image
from ceph.utils import get_node_by_id
from cli.rbd.rbd import Rbd
from utility.log import Log

log = Log(__name__)


def test_namespace_creation_default_pool(client, **kw):
    """
    Tests the image creation in namespace
    Args:
        client: rbd client obj
        **kw: test data
    """
    kw["client"] = client
    kw["namespace"] = "ns_default_pool"
    image_config_val = {"namespace": kw["namespace"], "size": "1G"}
    pool = "rbd"

    # for all the pools mentioned in testsuite, create a namespace followed by image creation
    for pool, _ in kw["config"]["rep_pool_config"].items():
        ns_rc = create_namespace_and_verify(**kw)
        if ns_rc == 0:
            return create_single_image(
                {}, "ceph", Rbd(client), pool, None, "ns_image", image_config_val, None
            )


def run(**kw):
    """RBD image creation in namespace.

    Args:
        **kw: test data
    """
    log.info("Running test CEPH-83582476 - Testing image creation in namespace.")

    try:
        if kw.get("client_node"):
            client = get_node_by_id(kw.get("ceph_cluster"), kw.get("client_node"))
        else:
            client = kw.get("ceph_cluster").get_nodes(role="client")[0]
        kw["do_not_create_image"] = True
        rbd_obj = initial_rbd_config(**kw)
        pool_types = rbd_obj.get("pool_types")
        ret_val = test_namespace_creation_default_pool(client=client, **kw)
    except Exception as e:
        log.error(
            f"Testing RBD image creation in the namespace failed with the error {str(e)}"
        )
        ret_val = 1
    finally:
        cluster_name = kw.get("ceph_cluster", {}).name
        if "rbd_obj" not in locals():
            rbd_obj = Rbd(client)
        obj = {cluster_name: rbd_obj}
        cleanup(pool_types=pool_types, multi_cluster_obj=obj, **kw)
    return ret_val
