import time

from krbd_io_handler import krbd_io_handler
from rbd.exceptions import RbdBaseException

from ceph.parallel import parallel
from tests.rbd.rbd_utils import Rbd
from utility.log import Log

log = Log(__name__)


def run(**kw):
    """Delete Clones while IO is running on parent.

    Args:
        kw: test data

    Returns:
        int: The return value. 0 for success, 1 otherwise

    Pre-requisites :
    We need atleast one client node with ceph-common package,
    conf and keyring files

    Test cases covered -
    1) CEPH-9225 - RBD-Kernel_Client: Delete all Clone at once

    Test Case Flow
    1. Create a pool, an Image, 10 snapshots, 10 clones.
    2. generate IO on the Image, in parallel delete the clones.

    """
    log.info("Running test - delete clones with IOs")

    scenarios = [Rbd(**kw)]

    kw["config"]["ec-pool-k-m"] = "go_with_default"
    scenarios.append(Rbd(**kw))

    config = kw["config"]

    pools = ["test_rbd_ec_pool_scenario", "test_rbd_rep_pool"]
    image = config.get("image_name", "rbd_test_image")
    size = "10G"

    for rbd in scenarios:
        pool = pools.pop()
        kw["rbd_obj"] = rbd
        try:

            def create_clone(*args):
                if not rbd.create_pool(poolname=pool):
                    return 1

                rbd.create_image(pool_name=pool, image_name=image, size=size)

                for clone_number in range(0, 10):
                    rbd.snap_create(pool, image, f"snap_{clone_number}")
                    rbd.protect_snapshot(f"{pool}/{image}@snap_{clone_number}")
                    rbd.create_clone(
                        f"{pool}/{image}@snap_{clone_number}",
                        pool,
                        f"clone_{clone_number}",
                    )
                    time.sleep(5)

            def delete_clone(*args):
                for clone_number in range(0, 10):
                    rbd.remove_image(pool, f"clone_{clone_number}")
                    time.sleep(3)

            io_config = {
                "file_size": "100M",
                "image_spec": [f"{pool}/{image}"],
                "operations": {
                    "fs": "ext4",
                    "io": True,
                    "map": True,
                    "mount": True,
                    "nounmap": False,
                },
                "runtime": 30,
            }
            kw["config"].update(io_config)

            log.info("Creating image, snaps, clones with IO")
            with parallel() as p:
                p.spawn(create_clone)
                time.sleep(10)
                p.spawn(krbd_io_handler, **kw)

            log.info("Deleting image, snaps, clones with IO")
            with parallel() as p:
                p.spawn(delete_clone)
                p.spawn(krbd_io_handler, **kw)

        except RbdBaseException as error:
            print(error.message)
            return 1

        finally:
            rbd.clean_up(pools=[pool])

    return 0
