import random
import time

from ceph.parallel import parallel
from tests.rbd.rbd_utils import Rbd, initial_rbd_config
from utility.log import Log
from utility.utils import run_fio

log = Log(__name__)


def create_an_image_run_fio(rbd, pool, client, image_name):
    if rbd.create_image(pool, image_name, "10G"):
        log.error(f"Image creation failed for {image_name}")
        return 1
    run_fio(image_name=image_name, pool_name=pool, client_node=client, runtime=10)
    return 0


def create_images_run_fio_in_parallel(rbd, pool, client, num_images):
    log.info(f"Creating {num_images} images in pool {pool} and initiating io")

    with parallel() as p:
        for i in range(0, num_images):
            p.spawn(create_an_image_run_fio, rbd, pool, client, f"image_{i}")
        return 0


def move_all_images_to_trash(rbd, pool, num_images):
    log.info("Moving all images in pool to trash")
    for i in range(0, num_images):
        rbd.move_image_trash(pool, f"image_{i}")


def prepare_images_and_move_to_trash(rbd, pool, client, num_images):
    create_images_run_fio_in_parallel(rbd, pool, client, num_images)
    move_all_images_to_trash(rbd, pool, num_images)


def verify_schedule_add(
    rbd: Rbd,
    verification_iterations: int,
    schedule_interval: int,
    pool: str,
    client,
    num_images: int,
) -> int:
    iterations_remaining = verification_iterations
    log.debug(f"Verifying purge on pool {pool}")
    while iterations_remaining > 0:
        log.debug(f"Sleeping for {schedule_interval} mins with buffer")
        time.sleep(schedule_interval * 60 + 60)
        trash_list = rbd.trash_list(pool)
        log.debug(f"Trash list: {trash_list}")
        if len(trash_list) > 3:
            log.error("Images found in trash unexpectedly")
            return 1
        iterations_remaining -= 1
        if iterations_remaining != 0:
            prepare_images_and_move_to_trash(rbd, pool, client, num_images)

    log.info(
        f"Verified trash purge schedule of {schedule_interval} minutes for {verification_iterations} iterations"
    )
    return 0


def rbd_trash_purge_schedule(rbd, pool_type, **kw):
    """
    Orchestrate rbd trash purge schedule test
    Args:
        rbd: rbd object
        pool_type: pool type (ec_pool_config and/or rep_pool_config)
        **kw: Test data
    """
    pool = kw["config"][pool_type]["pool"]
    num_images = random.randint(2, 5)
    schedule_interval = random.randint(1, 5)
    verification_iterations = random.randint(2, 5)

    log.info(
        "Trash purge schedule will be tested for an interval of "
        f"{schedule_interval} minutes for {verification_iterations} iterations"
    )

    try:
        prepare_images_and_move_to_trash(rbd, pool, rbd.ceph_client, num_images)

        log.info(f"Adding trash purge schedule on pool {pool}")
        if rbd.trash_purge_schedule(pool, "add", schedule_interval):
            return 1
        return verify_schedule_add(
            rbd,
            verification_iterations,
            schedule_interval,
            pool,
            rbd.ceph_client,
            num_images,
        )

    except Exception as error:
        log.error(error)
        return 1

    finally:
        rbd.clean_up(pools=[kw["config"][pool_type]["pool"]])


def run(**kw):
    """Verify the trash purge schedule functionality.

    Args:
        kw: test data

    Returns:
        int: The return value. 0 for success, 1 otherwise

    Pre-requisites :
    We need atleast one client node with ceph-common package,
    conf and keyring files

    Test cases covered -
    CEPH-83573660 - Schedule periodic trash purge operations via the CLI and check the behavior

    Test case work flow:

    1. Create some images with i/o
    2. Move images to trash
    3. add trash purge schedule
    4. Verify trash purge schedule is intact and
    5. images in trash are purged (removed periodically)
    6. Repeat the above steps for EC-pool.
    """
    log.info(
        "Executing test CEPH-83573660 - Schedule periodic trash purge operations "
        "and check the behavior"
    )
    rbd_obj = initial_rbd_config(**kw)
    if rbd_obj:
        if "rbd_reppool" in rbd_obj:
            log.info("Executing test on Replication pool")
            if rbd_trash_purge_schedule(
                rbd_obj.get("rbd_reppool"), "rep_pool_config", **kw
            ):
                log.error(
                    "Verification of trash purge schedule on "
                    "replication pool images failed."
                )
                return 1
        if "rbd_ecpool" in rbd_obj:
            log.info("Executing test on EC pool")
            if rbd_trash_purge_schedule(
                rbd_obj.get("rbd_ecpool"), "ec_pool_config", **kw
            ):
                log.error(
                    "Verification of trash purge schedule on ec pool images failed."
                )
                return 1
    else:
        log.error("Initial configuration failed")
        return 1

    return 0
