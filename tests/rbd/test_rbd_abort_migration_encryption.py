"""Module to verify Rollback after Failed Migration

Test case covered -
CEPH-83595961 - Rollback after Failed Migration using migration abort for encrypted images

Pre-requisites :
1. Cluster must be up and running with capacity to create pool
2. We need atleast one client node with ceph-common package,
   conf and keyring files

Test Case Flow:
1. Deploy Ceph on version 8.0 or greater.
2. Create an RBD pool.
3.  Create two RBD images in the pool.
    rbd create pool1/image1 --size 20G
4. Create a snapshot and clone the Source RBD Image to two new images
5. Format one cloned image to an encrypted format with LUKS1
    E.g:
    rbd encryption format pool1/image1 luks1 /tmp/passphrase.bin
6. Write data to both encrypted images and take md5sum
 rbd device map -t nbd -o encryption-format=luks1,encryption-passphrase-file=/tmp/passphrase.bin
 rpool1/ec_image1; mkfs -t ext4 /dev/nbd1; mkdir /tmp/test1; mount /dev/nbd1 /tmp/test1;
 cp /var/log/messages /tmp/test1/ec_luks1_data1; df -h /tmp/test1/ec_luks1_data1; md5sum /tmp/test1/ec_luks1_data1
7. Execute live migration with source as encrypted image, before migration,
    make sure device is unmounted and unmapped using as below
    umount /dev/nbd0;
    rbd device unmap -t nbd -o encryption-format=luks1,encryption-passphrase-file=luks1_passphrase.bin rep_pool/image1;
8. Intentionally introduce a failure during live migration or before committing the migration
9. Rollback the migration using rbd migration abort
10. Re-calculate md5sum and Cross-verify data consistency along with used size

"""

from copy import deepcopy

from ceph.rbd.initial_config import initial_rbd_config
from ceph.rbd.utils import get_md5sum_rbd_image, getdict, random_string
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.encryption import create_passphrase_file
from ceph.rbd.workflows.krbd_io_handler import krbd_io_handler
from ceph.rbd.workflows.migration import verify_migration_state
from ceph.rbd.workflows.rbd import create_single_pool_and_images
from ceph.utils import get_node_by_id
from cli.rbd.rbd import Rbd
from tests.rbd.rbd_utils import Rbd as rbdutils
from utility.log import Log

log = Log(__name__)


def rollback_migration_encrypted(rbd_obj, client, **kw):
    """
        Test to verify Rollback after Failed Migration
    Args:
        rbd_obj: RBD object
        client : client node object
        **kw: any other arguments
    """

    kw["client"] = client
    rbd = rbd_obj.get("rbd")

    for pool_type in rbd_obj.get("pool_types"):
        rbd_config = kw.get("config", {}).get(pool_type, {})
        multi_pool_config = deepcopy(getdict(rbd_config))

        for pool, pool_config in multi_pool_config.items():
            kw["pool-name"] = pool

            for encryption_type in kw.get("config", {}).get("encryption_type", {}):
                # Create an RBD image in pool
                image = "image_" + encryption_type + "_" + random_string(len=4)
                out, err = rbd.create(**{"image-spec": f"{pool}/{image}", "size": 1024})
                if err:
                    log.error(f"Create image {pool}/{image} failed with error {err}")
                    return 1
                else:
                    log.info(f"Successfully created image {pool}/{image}")
                snap = "snap_" + random_string(len=4)
                out, err = rbd.snap.create(**{"snap-spec": f"{pool}/{image}@{snap}"})
                if "100% complete" in err:
                    log.info("Successfully created snap")
                else:
                    log.error("Failed to create snap")
                    return 1
                out, err = rbd.snap.protect(**{"snap-spec": f"{pool}/{image}@{snap}"})
                if err:
                    log.error("Failed to protect snap")
                    return 1
                else:
                    log.info("Successfully protected snap")
                clone = "clone_" + random_string(len=4)
                out, err = rbd.clone(
                    **{
                        "source-snap-spec": f"{pool}/{image}@{snap}",
                        "dest-image-spec": f"{pool}/{clone}",
                    }
                )
                if err:
                    log.error("Failed to create clone")
                    return 1
                else:
                    log.info("Successfully created clone")

                # Format the clone with encryption
                passphrase = f"{encryption_type}_passphrase.bin"
                create_passphrase_file(client, passphrase)

                out, err = rbd.encryption_format(
                    **{
                        "image-spec": f"{pool}/{clone}",
                        "format": encryption_type,
                        "passphrase-file": passphrase,
                    }
                )
                if err:
                    log.error(
                        f"Encryption format with {encryption_type} failed on {pool}/{clone}"
                    )
                else:
                    log.info(
                        f"Successfully formatted the clone {pool}/{clone} with encryption type {encryption_type}"
                    )

                # Map, mount and run IOs
                fio = kw.get("config", {}).get("fio", {})
                io_config = {
                    "rbd_obj": rbd,
                    "client": client,
                    "size": fio["size"],
                    "do_not_create_image": True,
                    "config": {
                        "file_size": fio["size"],
                        "file_path": [f"/mnt/mnt_{random_string(len=5)}/file"],
                        "get_time_taken": True,
                        "image_spec": [f"{pool}/{clone}"],
                        "operations": {
                            "fs": "ext4",
                            "io": True,
                            "mount": True,
                            "device_map": True,
                        },
                        "cmd_timeout": 2400,
                        "io_type": "write",
                    },
                }

                # Include the encryption details in io config
                encryption_config = list()
                encryption_config.append({"encryption-format": encryption_type})
                encryption_config.append({"encryption-passphrase-file": passphrase})
                io_config["config"]["encryption_config"] = encryption_config
                out, err = krbd_io_handler(**io_config)
                if err:
                    log.error(
                        f"Map, mount and run IOs failed for encrypted {pool}/{clone}"
                    )
                    return 1
                else:
                    log.info(
                        f"Map, mount and IOs successful for encrypted {pool}/{clone}"
                    )

                md5_before_migration = get_md5sum_rbd_image(
                    image_spec=f"{pool}/{clone}",
                    rbd=rbd,
                    client=client,
                    file_path="file" + random_string(len=5),
                )
                log.info(
                    f"md5sum of source image clone before migration  is {md5_before_migration}"
                )

                # Create a target pool where the encrypted clone is to be migrated
                is_ec_pool = True if "ec" in pool_type else False
                config = kw.get("config", {})
                target_pool = "target_pool_" + random_string()
                target_pool_config = {}
                if is_ec_pool:
                    data_pool_target = "data_pool_new_" + random_string()
                    target_pool_config["data_pool"] = data_pool_target
                rc = create_single_pool_and_images(
                    config=config,
                    pool=target_pool,
                    pool_config=target_pool_config,
                    client=client,
                    cluster="ceph",
                    rbd=rbd,
                    ceph_version=int(config.get("rhbuild")[0]),
                    is_ec_pool=is_ec_pool,
                    is_secondary=False,
                    do_not_create_image=True,
                )
                if rc:
                    log.error(f"Creation of target pool {target_pool} failed")
                    return rc

                # Adding the new pool details to config so that they are handled in cleanup
                if pool_type == "rep_pool_config":
                    kw["config"]["rep_pool_config"][target_pool] = {}
                elif pool_type == "ec_pool_config":
                    kw["config"]["ec_pool_config"][target_pool] = {
                        "data_pool": data_pool_target
                    }

                # Prepare Migration
                target_image = "target_image_" + random_string()
                rbd.migration.prepare(
                    source_spec=f"{pool}/{clone}",
                    dest_spec=f"{target_pool}/{target_image}",
                    client_node=client,
                )

                # Verify prepare migration status
                if verify_migration_state(
                    action="prepare",
                    image_spec=f"{target_pool}/{target_image}",
                    **kw,
                ):
                    log.error("Failed to prepare migration")
                    return 1
                else:
                    log.info("Migration prepare status verfied successfully")

                # execute migration
                rbd.migration.action(
                    action="execute",
                    dest_spec=f"{target_pool}/{target_image}",
                    client_node=client,
                )

                # verify execute migration status
                if verify_migration_state(
                    action="execute",
                    image_spec=f"{target_pool}/{target_image}",
                    **kw,
                ):
                    log.error("Failed to execute migration")
                    return 1
                else:
                    log.info("Migration executed successfully")

                # Abort the migration
                rbd.migration.action(
                    action="abort",
                    dest_spec=f"{target_pool}/{target_image}",
                    client_node=client,
                )
                log.info("Migration abort executed successfully")

                # verify target image does not exist after abort
                rbdutil = rbdutils(**kw)
                if rbdutil.image_exists(target_pool, target_image):
                    log.error(
                        f"Image still exist after aborting the image migration in pool {target_pool}"
                    )
                    return 1
                else:
                    log.info(
                        f"Image {target_image} is not found in pool {target_pool} after aborting migration"
                    )

                # get md5sum of the source after aborting migration
                md5_after_abort = get_md5sum_rbd_image(
                    image_spec=f"{pool}/{clone}",
                    rbd=rbd,
                    client=client,
                    file_path="file" + random_string(len=5),
                )
                log.info(
                    f"md5sum of source after aborting migration  is {md5_after_abort}"
                )
                if md5_before_migration == md5_after_abort:
                    log.info(
                        "md5sum of source remains same before and after aborting migration"
                    )
                else:
                    log.error(
                        "md5sum of source is not same before and after aborting migration"
                    )
                    return 1
    return 0


def run(**kw):
    """
    This test verifies Rollback after Failed Migration using migration abort for encrypted images
    Args:
        kw: test data
    Returns:
        int: The return value. 0 for success, 1 otherwise

    """
    try:
        log.info(
            "CEPH-83595961 - Rollback after Failed Migration using migration abort for encrypted images"
        )

        if kw.get("client_node"):
            client = get_node_by_id(kw.get("ceph_cluster"), kw.get("client_node"))
        else:
            client = kw.get("ceph_cluster").get_nodes(role="client")[0]
        rbd_obj = initial_rbd_config(**kw)
        pool_types = rbd_obj.get("pool_types")
        if rbd_obj:
            log.info("Executing test on Replicated and EC pool")
            if rollback_migration_encrypted(rbd_obj, client, **kw):
                return 1
            log.info(
                "Test Rollback after Failed Migration using migration abort for encrypted images passed"
            )

    except Exception as e:
        log.error(
            f"Test Rollback after Failed Migration using migration abort for encrypted images failed: {str(e)}"
        )
        return 1

    finally:
        cluster_name = kw.get("ceph_cluster", {}).name
        if "rbd_obj" not in locals():
            rbd_obj = Rbd(client)
        obj = {cluster_name: rbd_obj}
        cleanup(pool_types=pool_types, multi_cluster_obj=obj, **kw)

    return 0
