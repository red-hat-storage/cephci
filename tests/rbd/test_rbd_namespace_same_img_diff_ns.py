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
    Tests image creation with the same name in different ns
    Args:
        client: rbd client obj
        **kw: test data
    """
    kw["client"] = client

    # for all the pools mentioned in the config perform the test
    for pool, _ in kw["config"]["rep_pool_config"].items():
        # namespace1 creation in pool
        kw["namespace"] = "ns1"
        rc_ns1 = create_namespace_and_verify(**kw)
        if rc_ns1 != 0:
            log.error(f"Namespace creation {kw['namespace']} failed")
            return 1
        # image creation in namespace ns_default_pool_1 in the default rbd pool
        image_config_val = {"namespace": kw["namespace"], "size": "1G"}
        rc_ns1_img1 = create_single_image(
            {}, "ceph", Rbd(client), pool, None, "ns_image", image_config_val, None
        )
        if rc_ns1_img1 != 0:
            log.error(f"Image creation failed in namespace {kw['namespace']}")
            return 1

        # namespace2 creation in pool
        kw["namespace"] = "ns2"
        rc_ns2 = create_namespace_and_verify(**kw)
        if rc_ns2 != 0:
            log.error(f"Namespace creation {kw['namespace']} failed")
            return 1
        # image with same name creation in namespace ns_default_pool_1 in the default rbd pool
        image_config_val = {"namespace": kw["namespace"], "size": "1G"}
        rc_ns1_img2 = create_single_image(
            {}, "ceph", Rbd(client), pool, None, "ns_image", image_config_val, None
        )
        if rc_ns1_img2 != 0:
            log.error(f"Image creation failed in namespace {kw['namespace']}")
            return 1
    return rc_ns1_img2


def run(**kw):
    """RBD image creation with the same name in diff ns.

    Args:
        **kw: test data
    """
    log.info(
        "Running test CEPH-83582479 - Testing RBD image creation with the same name in different ns"
    )

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
            f"Testing RBD image creation with the same name in different ns failed with the error {str(e)}"
        )
        ret_val = 1
    finally:
        cluster_name = kw.get("ceph_cluster", {}).name
        if "rbd_obj" not in locals():
            rbd_obj = Rbd(client)
        obj = {cluster_name: rbd_obj}
        cleanup(pool_types=pool_types, multi_cluster_obj=obj, **kw)
    return ret_val
