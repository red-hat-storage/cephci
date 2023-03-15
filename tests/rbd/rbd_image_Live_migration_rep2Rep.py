"""Module to execute Live migration feature test case.

Pre-requisites :
We need cluster configured with atleast one client node with ceph-common package,
conf and keyring files

Test cases covered :
CEPH-83573293 - image migration from rep pool to rep pool

Test Case Flow :
1. Create Rep pool and an image
2. Create an another Rep pool as destination pool
3. Mount the images and run io from clients
4. Stop IO and prepare migration process from rep pool to rep pool
5. Start IO from client on destination images
6. Execute migration
7. Commit migration and check the destination image info
"""

from tests.rbd.exceptions import RbdBaseException
from tests.rbd.rbd_utils import (
    Rbd,
    initial_rbd_config,
    verify_migration_commit,
    verify_migration_state,
)
from utility.log import Log
from utility.utils import run_fio

log = Log(__name__)


def run(**kw):
    """Image migration from Rep pool to Rep pool.

    Args:
        kw: Key/value pairs of configuration information to be used in the test
            Example::
            config:
                "ec-pool-only": True
                ec_pool_config:
                    pool: rbd_pool_4
                    data_pool: rbd_ec_pool_4
                    ec_profile: rbd_ec_profile_4
                    image: rbd_image_4
                    size: 10G

    Returns:
        int: The return value. 0 for success, 1 otherwise
    """
    log.info("Starting execution of image migration from Rep pool to Rep pool")

    # config for replication pool
    config_data = {
        "config_1": {
            "rep-pool-only": True,
            "rep_pool_config": {
                "pool": "rbd_RepPool1",
                "image": "rbd_image",
                "size": "10G",
            },
        },
        "config_2": {
            "rep-pool-only": True,
            "do_not_create_image": True,
            "rep_pool_config": {
                "pool": "rbd_RepPool2",
            },
        },
    }

    for value in config_data.values():
        kw["config"].update(value)
        initial_rbd_config(**kw)

    image = config_data["config_1"]["rep_pool_config"]["image"]
    repPool1 = config_data["config_2"]["rep_pool_config"]["pool"]
    src_spec = repPool1 + "/" + image

    repPool2 = config_data["config_2"]["rep_pool_config"]["pool"]
    dest_spec = repPool2 + "/" + image
    rbd = Rbd(**kw)
    try:
        client = kw["ceph_cluster"].get_nodes(role="client")[0]
        run_fio(image_name=image, pool_name=repPool1, client_node=client)

        # Perpare the migration process from EC pool to destination EC pool
        rbd.migration_prepare(src_spec, dest_spec)

        # verify migration prepare
        verify_migration_state(rbd, dest_spec)

        # run io's on the destination image
        run_fio(image_name=image, pool_name=repPool2, client_node=client)

        # Execute migration process
        rbd.migration_action(action="execute", dest_spec=dest_spec)

        # verify migration execute
        verify_migration_state(rbd, dest_spec)

        # commit migration process
        rbd.migration_action(action="commit", dest_spec=dest_spec)

        # verify commit migration
        verify_migration_commit(rbd, repPool2, image)

        return 0

    except RbdBaseException as error:
        log.error(error.message)
        return 1

    finally:
        if not kw.get("config").get("do_not_cleanup_pool"):
            rbd.clean_up(pools=[repPool1, repPool2])
