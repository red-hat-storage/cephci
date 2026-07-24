"""Module to execute Live migration feature test case.

Pre-requisites :
We need cluster configured with atleast one client node with ceph-common package,
conf and keyring files

Test cases covered -
1) CEPH-83573322 - Migrate the images from 2 different source pools to destination
and check the behaviour
2) CEPH-83573323 - Data integrity and Image info wrt associated pool after image migration

Test Case Flow -
1. Create two source EC pool with different k_m values and an image
2. Create an EC pool for destination pool
3. Mount the images and run io from clients
4. Stop IO and prepare migration process from 2 source pools to destination pool
5. Start IO from client on destination images
6. Execute migration
7. Commit migration.
8. verify for data integrity and Image info wrt associated pool after image migration
"""

from ceph.parallel import parallel
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
    """Image migration from EC pool to EC pool.

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
    log.info("Starting execution of 2 image migration from EC pool to EC pool")

    config_data = {
        "config_1": {
            "ec-pool-only": True,
            "ec-pool-k-m": "2,1",
            "ec_pool_config": {
                "pool": "rbd_ECpool1",
                "image": "rbd_image_ec1",
                "data_pool": "data_pool_ec1",
                "size": "10G",
            },
        },
        "config_2": {
            "ec-pool-only": True,
            "ec-pool-k-m": "2,2",
            "crush-failure-domain": "osd",
            "ec_pool_config": {
                "pool": "rbd_ECpool2",
                "image": "rbd_image_ec2",
                "data_pool": "data_pool_ec2",
                "size": "10G",
            },
        },
        "config_3": {
            "ec-pool-only": True,
            "ec-pool-k-m": "3,2",
            "do_not_create_image": True,
            "crush-failure-domain": "osd",
            "ec_pool_config": {
                "pool": "rbd_ECpool3",
                "data_pool": "data_pool_ec3",
            },
        },
    }

    for value in config_data.values():
        kw["config"].update(value)
        initial_rbd_config(**kw)

    image1 = config_data["config_1"]["ec_pool_config"]["image"]
    srcpool1 = config_data["config_1"]["ec_pool_config"]["pool"]
    datapool1 = config_data["config_1"]["ec_pool_config"]["data_pool"]
    src_spec1 = srcpool1 + "/" + image1

    image2 = config_data["config_2"]["ec_pool_config"]["image"]
    srcpool2 = config_data["config_2"]["ec_pool_config"]["pool"]
    datapool2 = config_data["config_2"]["ec_pool_config"]["data_pool"]
    src_spec2 = srcpool2 + "/" + image2

    destpool = config_data["config_3"]["ec_pool_config"]["pool"]
    datapool3 = config_data["config_3"]["ec_pool_config"]["data_pool"]
    dest_spec1 = destpool + "/" + image1
    dest_spec2 = destpool + "/" + image2

    rbd = Rbd(**kw)
    try:
        client = kw["ceph_cluster"].get_nodes(role="client")[0]
        with parallel() as p:
            p.spawn(run_fio, image_name=image1, pool_name=srcpool1, client_node=client)
            p.spawn(run_fio, image_name=image2, pool_name=srcpool2, client_node=client)

        # get MD5 of image2 before migration
        rbd.export_image(imagespec=src_spec2, path=f"export_{src_spec2.split('/')[0]}")
        out_1 = rbd.exec_cmd(
            cmd=f"md5sum export_{src_spec2.split('/')[0]}", output=True
        )
        md51 = out_1.split(" ")[0]

        # Perpare the migration process from 2 source pool to destination pool
        with parallel() as p:
            p.spawn(rbd.migration_prepare, src_spec1, dest_spec1)
            p.spawn(rbd.migration_prepare, src_spec2, dest_spec2)

        # verify migration prepare
        with parallel() as p:
            p.spawn(verify_migration_state, rbd, dest_spec1)
            p.spawn(verify_migration_state, rbd, dest_spec2)

        # run io's on the destination image
        run_fio(image_name=image1, pool_name=destpool, client_node=client)

        # Execute migration process
        with parallel() as p:
            p.spawn(rbd.migration_action, action="execute", dest_spec=dest_spec1)
            p.spawn(rbd.migration_action, action="execute", dest_spec=dest_spec2)

        # verify execute migration process
        with parallel() as p:
            p.spawn(verify_migration_state, rbd, dest_spec1)
            p.spawn(verify_migration_state, rbd, dest_spec2)

        # commit migration process
        with parallel() as p:
            p.spawn(rbd.migration_action, action="commit", dest_spec=dest_spec1)
            p.spawn(rbd.migration_action, action="commit", dest_spec=dest_spec2)

        # verify commit migration
        with parallel() as p:
            p.spawn(verify_migration_commit, rbd, destpool, image1)
            p.spawn(verify_migration_commit, rbd, destpool, image2)

        # get MD5 of image2 after migration and verify.
        rbd.export_image(
            imagespec=dest_spec2, path=f"export_{dest_spec2.split('/')[0]}"
        )
        out_2 = rbd.exec_cmd(
            cmd=f"md5sum export_{dest_spec2.split('/')[0]}", output=True
        )
        md52 = out_2.split(" ")[0]
        if md51 == md52:
            log.info("Data integrity check passed")
            return 0
        return 1

    except RbdBaseException as error:
        log.error(error.message)
        return 1

    finally:
        if not kw.get("config").get("do_not_cleanup_pool"):
            rbd.clean_up(
                pools=[srcpool1, srcpool2, destpool, datapool1, datapool2, datapool3]
            )
        rbd.exec_cmd(cmd=f"rm export_{src_spec2.split('/')[1]}", output=True)
        rbd.exec_cmd(cmd=f"rm export_{dest_spec2.split('/')[1]}", output=True)
