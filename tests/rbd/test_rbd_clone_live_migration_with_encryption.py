"""
Module to verify :
  - Live migration of cloned rbd images with encryption

Test case covered:
CEPH-83595960 - Live migration of cloned rbd images with encryption

Pre-requisites :
1. Cluster must be up and running with capacity to create pool
2. We need atleast one client node with ceph-common package,
   conf and keyring files

Test Case Flow:
1. Deploy Ceph on version 8.0 or greater.
2. Create an RBD pool.
3. Create two RBD images in the pool.
rbd create pool1/image1 --size 20G
rbd create pool1/image2 --size 20G
4. Create a snapshot and clone the Source RBD Image to two new images
5. Format one cloned image to an encrypted format with LUKS1
E.g:
rbd encryption format pool1/image1 luks1 /tmp/passphrase.bin
6. Format another cloned image to an encrypted format with LUKS2
E.g: rbd encryption format pool1/image2 luks2 /tmp/passphrase.bin
7. Write data to both encrypted images and take md5sum
rbd device map -t nbd -o encryption-format=luks1,encryption-passphrase-file=/tmp/passphrase.bin rpool1/ec_image1;
mkfs -t ext4 /dev/nbd1; mkdir /tmp/test1; mount /dev/nbd1 /tmp/test1;
cp /var/log/messages /tmp/test1/ec_luks1_data1; df -h /tmp/test1/ec_luks1_data1; md5sum /tmp/test1/ec_luks1_data1
8. Execute live migration with source as encrypted image, before migration,
make sure device is unmounted and unmapped using as below
umount /dev/nbd0;
rbd device unmap -t nbd -o encryption-format=luks1,encryption-passphrase-file=luks1_passphrase.bin rep_pool/image1;
9. Re-calculate md5sum and Cross-verify data consistency along with used size
10. map and mount migrated image and write and read the data
11. Unmap the disk and cleanup the pools and image
12. Repeat the test on EC pool
"""

from copy import deepcopy

from ceph.rbd.initial_config import initial_rbd_config
from ceph.rbd.utils import (
    create_map_options,
    get_md5sum_rbd_image,
    getdict,
    random_string,
)
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.encryption import create_passphrase_file
from ceph.rbd.workflows.krbd_io_handler import krbd_io_handler
from ceph.rbd.workflows.migration import verify_migration_state
from ceph.rbd.workflows.rbd import create_single_pool_and_images
from ceph.utils import get_node_by_id
from cli.rbd.rbd import Rbd
from utility.log import Log

log = Log(__name__)


def migration_encrypted_rbd_clone_images(rbd_obj, client, **kw):
    """
    Test to verify Live migration of cloned rbd images with encryption
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
                kw.update({f"{pool}": {}})
                kw[pool].update({"encryption_type": encryption_type})
                kw[pool].update({"pool_type": pool_type})
                image = (
                    "image_" + kw[pool]["encryption_type"] + "_" + random_string(len=3)
                )
                kw[pool].update({"image": image})

                # Create an RBD image in pool
                out, err = rbd.create(**{"image-spec": f"{pool}/{image}", "size": 1024})
                if err:
                    log.error(f"Create image {pool}/{image} failed with error {err}")
                    return 1
                else:
                    log.info(f"Successfully created image {pool}/{image}")

                # Creating snapshot of an image
                snap_name = "snap1"
                out, err = rbd.snap.create(pool=pool, image=image, snap=snap_name)
                if "failed to create snapshot: (30) Read-only file system" in out + err:
                    log.error(f"Snapshot creation failed for {pool}/{image}")
                    return 1
                else:
                    log.info(
                        f"Successfully created snapshot {snap_name} for {pool}/{image}"
                    )

                # Protect snapshot of an image
                snap_name = "snap1"
                out, err = rbd.snap.protect(pool=pool, image=image, snap=snap_name)
                if "failed to Protect snapshot" in out + err:
                    log.error(f"Snapshot Protection failed for {pool}/{image}")
                    return 1
                else:
                    log.info(
                        f"Successfully Protected snapshot {snap_name} for {pool}/{image}"
                    )

                # Clone the snapshot to new image
                clone_image = f"clone_{image}"
                clone_spec = {
                    "source-snap-spec": f"{pool}/{image}@{snap_name}",
                    "dest-image-spec": f"{pool}/{clone_image}",
                }
                _, err = rbd.clone(**clone_spec)
                if err:
                    log.error(
                        f"Clone creation failed for {pool}/{image}@{snap_name} with error {err}"
                    )
                    return 1
                else:
                    log.info(f"Cloning of snap {snap_name} is complete")

                err = run_io_on_encryption_formatted_image(rbd, pool, clone_image, **kw)
                if err:
                    return 1

                err = migrate_check_consistency(rbd, pool, clone_image, **kw)
                if err:
                    return 1

    return 0


def run_io_on_encryption_formatted_image(rbd, pool, image, **kw):
    """
    Function to carry out the following:
      - Create source rbd image
      - format the image with ecryption
      - create passphrase file
      - map, mount, write IO, unmount and unmap of source image
    Args:
        kw: rbd object, pool, image, test data
    Returns:
        int: The return value. 0 for success, 1 otherwise

    """

    # Format the image with encryption
    passphrase = (
        f"{kw[pool]['encryption_type']}_passphrase_" + random_string(len=3) + ".bin"
    )
    create_passphrase_file(kw["client"], passphrase)
    kw["cleanup_files"].append(passphrase)
    out, err = rbd.encryption_format(
        **{
            "image-spec": f"{pool}/{image}",
            "format": kw[pool]["encryption_type"],
            "passphrase-file": passphrase,
        }
    )
    if err:
        log.error(
            f"Encryption format with {kw[pool]['encryption_type']} failed on {pool}/{image}"
        )
        return 1
    else:
        log.info(
            f"Successfully formatted the clone image {pool}/{image} with encryption type {kw[pool]['encryption_type']}"
        )

    # Map, mount and run IOs
    fio = kw.get("config", {}).get("fio", {})
    io_config = {
        "rbd_obj": rbd,
        "client": kw["client"],
        "size": fio["size"],
        "do_not_create_image": True,
        "config": {
            "file_size": fio["size"],
            "file_path": [f"/mnt/mnt_{random_string(len=3)}/file"],
            "get_time_taken": True,
            "image_spec": [f"{pool}/{image}"],
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
    encryption_config.append({"encryption-format": kw[pool]["encryption_type"]})
    encryption_config.append({"encryption-passphrase-file": passphrase})
    io_config["config"]["encryption_config"] = encryption_config
    kw[pool].update({"encryption_config": encryption_config})
    out, err = krbd_io_handler(**io_config)
    if err:
        log.error(f"Map, mount and run IOs failed for encrypted {pool}/{image}")
        return 1
    else:
        log.info(f"Map, mount and IOs successful for encrypted {pool}/{image}")

    out, err = rbd.map(**{"image-or-snap-spec": f"{pool}/{image}"})
    if err:
        log.info(err)
        log.error(
            f"Failed to map the source cloned image {pool}/{image} without encryption"
        )
        return 1
    else:
        log.info(
            f"Successfully mapped the source cloned image {pool}/{image} without encryption"
        )
    kw[pool].update({image: {}})
    kw[pool][image].update({"dev": out.strip()})


def migrate_check_consistency(rbd, pool, image, **kw):
    """
    Function to carry out the following:
      - Create target pool and image
      - Prepare, execute and commit migration
      - Verify map an unmap migrated image
      - verify md5sum of source image and target image for data consistency
    Args:
        kw: rbd object, pool, image, test data
    Returns:
        int: The return value. 0 for success, 1 otherwise

    """
    # Create a target pool where the encrypted image is to be migrated
    is_ec_pool = True if "ec" in kw[pool]["pool_type"] else False
    config = kw.get("config", {})
    target_pool = "target_pool_" + random_string(len=3)
    target_pool_config = {}
    if is_ec_pool:
        data_pool_target = "data_pool_new_" + random_string(len=3)
        target_pool_config["data_pool"] = data_pool_target
    rc = create_single_pool_and_images(
        config=config,
        pool=target_pool,
        pool_config=target_pool_config,
        client=kw["client"],
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
    if kw[pool]["pool_type"] == "rep_pool_config":
        kw["config"]["rep_pool_config"][target_pool] = {}
    elif kw[pool]["pool_type"] == "ec_pool_config":
        kw["config"]["ec_pool_config"][target_pool] = {"data_pool": data_pool_target}

    out = rbd.lock_ls(
        **{
            "image-spec": f"{pool}/{image}",
        }
    )
    locker_name = out[0].split("\n")[2].split(" ")[0]
    locker_id = (
        out[0].split("\n")[2].split(" ")[2] + " " + out[0].split("\n")[2].split(" ")[3]
    )

    out, err = rbd.lock_rm(
        **{
            "lock-spec": f"'{locker_id}' {locker_name}",
            "image": image,
            "pool": pool,
        }
    )
    if err:
        log.error(f"Lock remove failed for {pool}/{image}")
    else:
        log.info(f"Lock removed for {pool}/{image}")

    out = rbd.blocklist_ls()
    ip_dict = {}
    blocklist_ip = out[0].split("\n")
    for ip in blocklist_ip:
        ip_dict[ip.split(" ")[0]] = ip.split(" ")[0].split(":")[0]
    ip_dict = {key: value for (key, value) in ip_dict.items() if value != ""}
    for i in ip_dict.values():
        if list(ip_dict.values()).count(i) == 1:
            ip = [key for key, val in ip_dict.items() if val == i]
            break
    out = rbd.blocklist_rm(
        **{
            "ip-spec": ip[0],
        }
    )

    try:
        err = kw["client"].exec_command(
            sudo=True, cmd=f"umount {kw[pool][image]['dev']}"
        )
    except Exception as error:
        if "not mounted" in str(error):
            log.info("Device is not mounted")
        else:
            log.error("Error in unmounting the device before migration")
            return 1

    map_config = {
        "pool": pool,
        "image": image,
        "device-type": config.get("device_type", "nbd"),
    }
    out, err = rbd.device.unmap(**map_config)
    if err:
        if "not mapped" in err:
            log.info("Device is not mapped")
        else:
            log.error(f"Failed to unmap source image {pool}/{image}")
            return 1
    else:
        log.info(f"Successfully unmapped cloned source image {pool}/{image}")

    md5_before_migration = get_md5sum_rbd_image(
        image_spec=f"{pool}/{image}",
        rbd=rbd,
        client=kw["client"],
        file_path=f"/tmp/{random_string(len=3)}",
    )
    log.info(f"md5sum of source clone image before migration is {md5_before_migration}")

    # Prepare Migration
    target_image = "target_image_clone_" + random_string(len=3)
    rbd.migration.prepare(
        source_spec=f"{pool}/{image}",
        dest_spec=f"{target_pool}/{target_image}",
        client_node=kw["client"],
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
        client_node=kw["client"],
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

    # commit migration
    rbd.migration.action(
        action="commit",
        dest_spec=f"{target_pool}/{target_image}",
        client_node=kw["client"],
    )

    # verify commit migration status
    if verify_migration_state(
        action="commit",
        image_spec=f"{target_pool}/{target_image}",
        **kw,
    ):
        log.error("Failed to commit migration")
        return 1
    else:
        log.info("Migration committed successfully")

    map_config = {
        "pool": target_pool,
        "image": target_image,
        "device-type": config.get("device_type", "nbd"),
    }
    options = create_map_options(kw[pool]["encryption_config"])
    map_config.update(
        {
            "options": options,
        }
    )
    out, err = rbd.device.map(**map_config)

    if err:
        log.error(f"Failed to map migrated cloned image {target_pool}/{target_image}")
        return 1
    else:
        log.info(
            f"Successfully mapped the migrated cloned image {target_pool}/{target_image}"
        )

    out, err = rbd.device.unmap(**map_config)
    if err:
        log.error(f"Failed to unmap migrated clone image {target_pool}/{target_image}")
        return 1
    else:
        log.info(
            f"Successfully unmapped the migrated cloned image {target_pool}/{target_image}"
        )

    md5_after_migration = get_md5sum_rbd_image(
        image_spec=f"{target_pool}/{target_image}",
        rbd=rbd,
        client=kw["client"],
        file_path=f"/tmp/{random_string(len=3)}",
    )
    log.info(f"md5sum of target image after migration is {md5_after_migration}")

    if md5_before_migration != md5_after_migration:
        log.error(f"Data Integrity check failed for {target_pool}/{target_image}")
        return 1
    else:
        log.info(f"Data Integrity check passed for {target_pool}/{target_image}")


def run(**kw):
    """
    This test verifies Live migration of cloned rbd images with encryption
    Args:
        kw: test data
    Returns:
        int: The return value. 0 for success, 1 otherwise

    """
    try:

        if kw.get("client_node"):
            client = get_node_by_id(kw.get("ceph_cluster"), kw.get("client_node"))
        else:
            client = kw.get("ceph_cluster").get_nodes(role="client")[0]
        rbd_obj = initial_rbd_config(**kw)
        pool_types = rbd_obj.get("pool_types")
        kw.update({"cleanup_files": []})

        if rbd_obj:
            log.info("Executing test on Replicated and EC pool")
            if migration_encrypted_rbd_clone_images(rbd_obj, client, **kw):
                return 1
            log.info("Test rbd clone live migration with encryption is successful")

    except Exception as e:
        log.error(f"Test rbd clone live migration with encryption failed: {str(e)}")
        return 1

    finally:
        try:
            for file in kw["cleanup_files"]:
                out, err = client.exec_command(sudo=True, cmd=f"rm -f {file}")
                if err:
                    log.error(f"Failed to delete file {file}")
        except Exception as e:
            log.error(f"Failed to cleanup temp files with err {e}")
        cluster_name = kw.get("ceph_cluster", {}).name
        if "rbd_obj" not in locals():
            rbd_obj = Rbd(client)
        obj = {cluster_name: rbd_obj}
        cleanup(pool_types=pool_types, multi_cluster_obj=obj, **kw)

    return 0
