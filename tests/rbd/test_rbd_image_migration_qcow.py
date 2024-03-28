"""Module to execute Live image migration with qcow data format.

Pre-requisites :
- Cluster configured with atleast one client node with ceph-common package,
conf and keyring files
- access qcow2 data from http://download.ceph.com/qa/ubuntu-12.04.qcow2

Test cases covered :
CEPH-83584070 - Live migration of images with external data source as QCOW2 data format

Test Case Flow :
1. Create pools with rbd application enabled
   for migration from external source and destination pool
2. Store the external qcow2 data in json spec
    '{"type":"qcow","stream":{"type":"http","url":"http://download.ceph.com/qa/ubuntu-12.04.qcow2"}}'
3. Perform live migration commands and verify it's status at each stage.
4. Verify data got migrated successfully using used bytes of image.
5. perform the test on both replicated and EC pools.
"""

from ceph.rbd.initial_config import initial_rbd_config
from ceph.rbd.utils import getdict, random_string
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.migration import verify_migration_state
from ceph.utils import get_node_by_id
from cli.rbd.rbd import Rbd
from utility.log import Log

log = Log(__name__)


def test_qcow_image_migration(rbd_obj, client, **kw):
    """
    Test to perform live migration of images with qcow data format.
    Args:
        rbd_obj: rbd object
        client: Client node object
        kw: Key/value pairs of configuration information to be used in the test
    """
    qcow_spec = {
        "type": "qcow",
        "stream": {
            "type": "http",
            "url": "http://download.ceph.com/qa/ubuntu-12.04.qcow2",
        },
    }

    client.exec_command(
        cmd="sudo curl -o /mnt/ubuntu-12.04.qcow2 http://download.ceph.com/qa/ubuntu-12.04.qcow2"
    )
    out, err = client.exec_command(
        cmd="sudo du -h /mnt/ubuntu-12.04.qcow2", output=True
    )
    qcow_data_size = out.split()[0]

    client.exec_command(
        cmd="sudo rm -rf /mnt/ubuntu-12.04.qcow2",
    )

    rbd = rbd_obj.get("rbd")
    kw["client"] = client

    for pool_type in rbd_obj.get("pool_types"):
        rbd_config = kw.get("config", {}).get(pool_type, {})
        multi_pool_config = getdict(rbd_config)

        for pool, pool_config in multi_pool_config.items():
            image_spec = pool + "/" + random_string(len=5)

            # prepare migration
            rbd.migration.prepare(
                source_spec=qcow_spec,
                dest_spec=image_spec,
                client_node=client,
                **kw,
            )

            # verify prepare migration status
            if verify_migration_state(
                action="prepare",
                image_spec=image_spec,
                **kw,
            ):
                log.error("Failed to prepare migration")
                return 1
            else:
                log.info("Migration prepare status verfied successfully")

            # execute migration

            rbd.migration.action(
                action="execute",
                dest_spec=image_spec,
                client_node=client,
                **kw,
            )

            # verify execute migration status
            if verify_migration_state(
                action="execute",
                image_spec=image_spec,
                **kw,
            ):
                log.error("Failed to execute migration")
                return 1
            else:
                log.info("Migration executed successfully")

            # commit migration
            rbd.migration.action(
                action="commit",
                dest_spec=image_spec,
                client_node=client,
                **kw,
            )

            # verify commit migration status
            if verify_migration_state(
                action="commit",
                image_spec=image_spec,
                **kw,
            ):
                log.error("Failed to commit migration")
                return 1
            else:
                log.info("Migration committed successfully")

            image_config = {"image-spec": image_spec}

            out = rbd.image_usage(**image_config)
            image_data = out[0]

            migrated_image_size = image_data.split("\n")[1].split()[3].strip() + "G"

            log.info(f"External qcow data format original size {qcow_data_size}")
            log.info(
                f"After migration to RBD image qcow data size {migrated_image_size}"
            )

            # Verification of external data migration.
            if migrated_image_size == qcow_data_size:
                log.info("Data migrated successfully through live image migration")
            else:
                log.error("Data migration failed through live image migration")
                return 1

    return 0


def run(**kw):
    """Test to execute Live image migration with qcow data format.
    Args:
        kw: Key/value pairs of configuration information to be used in the test
            Example::
            config:
                do_not_create_image: True
                rep_pool_config:
                    num_pools: 2
                    size: 4G
                ec_pool_config:
                    num_pools: 2
                    size: 4G
                create_pool_parallely: true
    """
    log.info(
        "Executing CEPH-83584070 - Live migration of images with external data source as QCOW2 data format"
    )
    try:
        if kw.get("client_node"):
            client = get_node_by_id(kw.get("ceph_cluster"), kw.get("client_node"))
        else:
            client = kw.get("ceph_cluster").get_nodes(role="client")[0]

        rbd_obj = initial_rbd_config(**kw)

        pool_types = rbd_obj.get("pool_types")

        ret_val = test_qcow_image_migration(rbd_obj=rbd_obj, client=client, **kw)

    except Exception as e:
        log.error(
            f"RBD image migration with qcow format failed with the error {str(e)}"
        )
        ret_val = 1

    finally:
        cluster_name = kw.get("ceph_cluster", {}).name
        if "rbd_obj" not in locals():
            rbd_obj = Rbd(client)
        obj = {cluster_name: rbd_obj}
        cleanup(pool_types=pool_types, multi_cluster_obj=obj, **kw)

    return ret_val
