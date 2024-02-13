from ceph.rbd.initial_config import initial_rbd_config
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.namespace import create_namespace_and_verify
from ceph.utils import get_node_by_id
from cli.rbd.rbd import Rbd
from utility.log import Log

log = Log(__name__)


def test_namespace_creation_custom_pool(client, **kw):
    """
    Tests the namespace creation with the same name in diff pool
    Args:
        client: rbd client obj
        **kw: test data
    """
    kw["client"] = client
    kw["namespace"] = "ns_custom_pool"

    # check for all the custom pools, if given in test suite
    for pool, _ in kw["config"]["rep_pool_config"].items():
        kw["pool-name"] = pool
        rc = create_namespace_and_verify(**kw)
        if rc != 0:
            return rc
    return rc


def run(**kw):
    """RBD namespace creation with the same name in diff pool

    Args:
        **kw: test data
    """
    log.info(
        "Running test CEPH-83582478 - Testing RBD namespace creation with the same name in diff pool."
    )

    try:
        if kw.get("client_node"):
            client = get_node_by_id(kw.get("ceph_cluster"), kw.get("client_node"))
        else:
            client = kw.get("ceph_cluster").get_nodes(role="client")[0]
        kw["do_not_create_image"] = True
        rbd_obj = initial_rbd_config(**kw)
        pool_types = rbd_obj.get("pool_types")
        ret_val = test_namespace_creation_custom_pool(client=client, **kw)
    except Exception as e:
        log.error(
            f"Testing RBD namespace creation with the same name in diff pool failed with the error {str(e)}"
        )
        ret_val = 1
    finally:
        cluster_name = kw.get("ceph_cluster", {}).name
        if "rbd_obj" not in locals():
            rbd_obj = Rbd(client)
        obj = {cluster_name: rbd_obj}
        cleanup(pool_types=pool_types, multi_cluster_obj=obj, **kw)
    return ret_val
