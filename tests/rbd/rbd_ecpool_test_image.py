from tests.rbd.exceptions import RbdBaseException
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import run_fio

log = Log(__name__)


def test_rbd_ecpool(rbd, pool_type, **kw):
    """Verify image creation and deletion on ecpools.

    This module verifies image operations on ecpools

    Args:
        kw: test data
            config:
                pool: <poolname>
                image: <imagename>

    Returns:
        int: The return value. 0 for success, 1 otherwise
    """
    try:
        client = kw["ceph_cluster"].get_nodes(role="client")[0]
        run_fio(
            image_name=kw["config"][pool_type]["image"],
            pool_name=kw["config"][pool_type]["pool"],
            client_node=client,
            size="100%",
        )
        rbd.remove_image(
            pool_name=kw["config"][pool_type]["pool"],
            image_name=kw["config"][pool_type]["image"],
        )
        if rbd.image_exists(
            pool_name=kw["config"][pool_type]["pool"],
            image_name=kw["config"][pool_type]["image"],
        ):
            log.error(
                f"Image {kw['config'][pool_type]['image']} not deleted successfully"
            )
            return 1
        return 0
    except RbdBaseException as error:
        log.error(error.message)
        return 1

    finally:
        rbd.clean_up(pools=[kw["config"][pool_type]["pool"]])


def run(**kw):
    """Verify image creation and deletion on ecpools.

    This module verifies image operations on ecpools

    Args:
        kw: test data
            config:
                ec-pool-k-m: <k,m>
                rep_pool_name: <name>
                ec_pool_name: <name>

    Returns:
        int: The return value. 0 for success, 1 otherwise

    Pre-requisites :
    We need atleast one client node with ceph-common package,
    conf and keyring files

    Test case covered -
    1) CEPH-83571600 - Creation and deletion of images on ec pools with overwrites enabled
    Test Case Flow
    1. Create an RBD image on ec pools with overwrites enabled for data pool and metadata on replicated
    2. generate IO in images
    3. Delete images and verify the deleted images are moved to trash automatically.
    """
    log.info("Running rbd image tests on ecpool")

    rbd_obj = initial_rbd_config(**kw)
    if rbd_obj:
        return test_rbd_ecpool(rbd_obj.get("rbd_ecpool"), "ec_pool_config", **kw)

    return 1
