"""Module to execute abort Live migration feature test case.

Pre-requisites :
We need cluster configured with atleast one client node with ceph-common package,
conf and keyring files

Test cases covered :
1)CEPH-83573324 - Copy the image from source to destination and stop in between
and check the appropriate message

Test Case Flow :
1. Create EC pool/replicated pool and an image
2. Create an another EC pool/replicated pool for destination pool
3. Mount the images and run io from clients
4. Stop IO and prepare migration process from EC pool to EC pool
5. Start IO from client on destination images
6. Execute migration
7. Abort migration and verify that cross-links from source to destination is removed,
and remove the target image.
"""

from tests.rbd.exceptions import RbdBaseException
from tests.rbd.rbd_utils import (
    Rbd,
    initial_rbd_config,
    verify_migration_abort,
    verify_migration_state,
)
from utility.log import Log
from utility.utils import run_fio

log = Log(__name__)


def migration_abort_verify(
    rbd,
    src_pool,
    dest_pool,
    image,
    client,
):
    """Abort the image migration from source to destination and verify the same
    Args:
        src_pool: source pool name
        dest_pool: destination pool name
        image: name of the image to be migrated
        client: client to run IOS
    """
    run_fio(image_name=image, pool_name=src_pool, client_node=client)
    src_spec1 = src_pool + "/" + image
    dest_spec1 = dest_pool + "/" + image
    rbd.migration_prepare(src_spec1, dest_spec1)
    verify_migration_state(rbd, dest_spec1)

    # run io's on the destination image
    run_fio(image_name=image, pool_name=dest_pool, client_node=client)

    rbd.migration_action(action="execute", dest_spec=dest_spec1)
    verify_migration_state(rbd, dest_spec1)
    rbd.migration_action(action="abort", dest_spec=dest_spec1)
    verify_migration_abort(rbd, dest_pool, image=image)

    # try to run IOS in original source image
    run_fio(image_name=image, pool_name=src_pool, client_node=client)


def run(**kw):
    """verify Abort image migration from source to destination pool.

    Args:
        kw: Key/value pairs of configuration information to be used in the test
            Example::
            config:
                rep_pool_config:
                    pool: rbd_RepPool1
                    image: rbd_image
                    size: 10G
                ec_pool_config:
                    pool: rbd_pool_4
                    data_pool: rbd_ec_pool_4
                    ec_profile: rbd_ec_profile_4
                    image: rbd_image_4
                    size: 10G

    Returns:
        int: The return value. 0 for success, 1 otherwise
    """
    log.info("Starting execution for abort image migration scenario...")

    for key, value in kw["config"].items():
        if key == "source":
            kw["config"].update(value)
            initial_rbd_config(**kw)
            for key in kw["config"]["source"]:
                kw["config"].pop(key)
        elif key == "destination":
            kw["config"].update(value)
            initial_rbd_config(**kw)
            for key in kw["config"]["destination"]:
                kw["config"].pop(key)

    rbd = Rbd(**kw)
    try:
        client = kw["ceph_cluster"].get_nodes(role="client")[0]

        src_pool1 = kw["config"]["source"]["rep_pool_config"]["pool"]
        src_pool2 = kw["config"]["source"]["ec_pool_config"]["pool"]
        rep_image = kw["config"]["source"]["rep_pool_config"]["image"]
        ec_image = kw["config"]["source"]["ec_pool_config"]["image"]
        dest_pool1 = kw["config"]["destination"]["rep_pool_config"]["pool"]
        dest_pool2 = kw["config"]["destination"]["ec_pool_config"]["pool"]

        log.info("CEPH-83573324 - Running test case on Replicated pool")
        migration_abort_verify(
            rbd,
            src_pool=src_pool1,
            dest_pool=dest_pool1,
            image=rep_image,
            client=client,
        )

        log.info("CEPH-83573324 - Running test case on EC pool")
        migration_abort_verify(
            rbd,
            src_pool=src_pool2,
            dest_pool=dest_pool2,
            image=ec_image,
            client=client,
        )
        return 0

    except RbdBaseException as error:
        log.error(error.message)
        return 1

    finally:
        if not kw.get("config").get("do_not_cleanup_pool"):
            rbd.clean_up(
                pools=[
                    src_pool1,
                    src_pool2,
                    dest_pool1,
                    dest_pool2,
                    kw["config"]["source"]["ec_pool_config"]["data_pool"],
                    kw["config"]["destination"]["ec_pool_config"]["data_pool"],
                ]
            )
