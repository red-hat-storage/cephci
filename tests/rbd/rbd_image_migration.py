"""Module to execute Live migration feature test case.

Pre-requisites :
We need cluster configured with atleast one client node with ceph-common package,
conf and keyring files

Test cases covered :
1)CEPH-83573293 - image migration from rep pool to rep pool

Test Case Flow :
1. Create Rep pool and an image
2. Create an another Rep pool as destination pool
3. Mount the images and run io from clients
4. Stop IO and prepare migration process from rep pool to rep pool
5. Start IO from client on destination images
6. Execute migration
7. Commit migration and check the destination image info

2)CEPH-83573327 - Verify image migration from Ec pool to Ec with default k_m values

Test Case Flow :
1. Create EC pool with default k_m values(2,2) and an image
2. Create an another EC pool with default k_m values(2,2) for destination pool
3. Mount the images and run io from clients
4. Stop IO and prepare migration process from EC pool to EC pool
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
    log.info("Starting execution of image migration")

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

        if kw.get("config").get("source").get("ec_pool_config"):
            src_pool = kw["config"]["source"]["ec_pool_config"]["pool"]
            image = kw["config"]["source"]["ec_pool_config"]["image"]
            src_spec = src_pool + "/" + image
            dest_pool = kw["config"]["destination"]["ec_pool_config"]["pool"]
            dest_spec = dest_pool + "/" + image
        else:
            src_pool = kw["config"]["source"]["rep_pool_config"]["pool"]
            image = kw["config"]["source"]["rep_pool_config"]["image"]
            src_spec = src_pool + "/" + image
            dest_pool = kw["config"]["destination"]["rep_pool_config"]["pool"]
            dest_spec = dest_pool + "/" + image

        run_fio(image_name=image, pool_name=src_pool, client_node=client)

        # Perpare the migration process from EC pool to destination EC pool
        rbd.migration_prepare(src_spec, dest_spec)

        # verify migration prepare
        verify_migration_state(rbd, dest_spec)

        # run io's on the destination image
        run_fio(image_name=image, pool_name=dest_pool, client_node=client)

        # Execute migration process
        rbd.migration_action(action="execute", dest_spec=dest_spec)

        # verify migration execute
        verify_migration_state(rbd, dest_spec)

        # commit migration process
        rbd.migration_action(action="commit", dest_spec=dest_spec)

        # verify commit migration
        verify_migration_commit(rbd, dest_pool, image)

        return 0

    except RbdBaseException as error:
        log.error(error.message)
        return 1

    finally:
        if not kw.get("config").get("do_not_cleanup_pool"):
            if kw.get("config").get("source").get("ec-pool-only"):
                rbd.clean_up(
                    pools=[
                        src_pool,
                        dest_pool,
                        kw["config"]["source"]["ec_pool_config"]["data_pool"],
                        kw["config"]["destination"]["ec_pool_config"]["data_pool"],
                    ]
                )
            else:
                rbd.clean_up(pools=[src_pool, dest_pool])
