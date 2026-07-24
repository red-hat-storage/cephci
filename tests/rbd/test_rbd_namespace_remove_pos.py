from ceph.rbd.initial_config import initial_rbd_config
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.namespace import (
    create_namespace_and_verify,
    remove_namespace_and_verify,
)
from ceph.rbd.workflows.rbd import create_single_image, remove_single_image_and_verify
from ceph.utils import get_node_by_id
from cli.rbd.rbd import Rbd
from utility.log import Log

log = Log(__name__)


def test_namespace_removal_pos(client, **kw):
    """
    Tests the namespace removal positive flow
    Args:
        client: rbd client obj
        **kw: test data
    Steps:
        (1) Create a rbd pool if it does not exist	- pool creation should succeed
        (2) Create a namespace - 	namespace creation should succeed
        (3) Create an image in the namespace - image creation in the namespace should succeed
        (4) Delete an image	image - deletion in the namespace should succeed
        (5) Delete the namespace - namespace deletion should succeed
    """
    kw["client"] = client
    kw["namespace"] = "ns_deletable"
    image_config_val = {"namespace": kw["namespace"], "size": "1G"}

    # check for all the pools, if given in test suite
    for pool, _ in kw["config"]["rep_pool_config"].items():
        kw["pool-name"] = pool
        if create_namespace_and_verify(**kw) != 0:
            return 1
        image_creation_rc = create_single_image(
            {}, "ceph", Rbd(client), pool, None, "ns_image", image_config_val, None
        )
        if image_creation_rc != 0:
            return 1
        rm_image_kw = {}
        rm_image_kw.update(
            {
                "rbd": Rbd(client),
                "pool": pool,
                "namespace": kw["namespace"],
                "image": "ns_image",
            }
        )
        if remove_single_image_and_verify(**rm_image_kw) != 0:
            return 1
        if remove_namespace_and_verify(**kw) != 0:
            return 1
        else:
            return 0


def run(**kw):
    """RBD namespace removal positive testing.

    Args:
        **kw: test data
    """
    log.info(
        "Running test CEPH-83583642 - Testing RBD namespace removal positive flow."
    )

    try:
        if kw.get("client_node"):
            client = get_node_by_id(kw.get("ceph_cluster"), kw.get("client_node"))
        else:
            client = kw.get("ceph_cluster").get_nodes(role="client")[0]
        kw["do_not_create_image"] = True
        rbd_obj = initial_rbd_config(**kw)
        pool_types = rbd_obj.get("pool_types")
        ret_val = test_namespace_removal_pos(client=client, **kw)
    except Exception as e:
        log.error(
            f"Testing RBD namespace removal in rbd pool failed with the error {str(e)}"
        )
        ret_val = 1
    finally:
        cluster_name = kw.get("ceph_cluster", {}).name
        if "rbd_obj" not in locals():
            rbd_obj = Rbd(client)
        obj = {cluster_name: rbd_obj}
        cleanup(pool_types=pool_types, multi_cluster_obj=obj, **kw)
    return ret_val
