"""
Module to verify :
  - Live migration of RAW format images with encryption
  - Live migration of qcow2 format images with encryption

Test case covered:
CEPH-83596444 - Live migration of RAW format images with encryption

Pre-requisites :
1. Cluster must be up and running with capacity to create pool
2. We need atleast one client node with ceph-common package,
   conf and keyring files

Test Case Flow:
1. Deploy Ceph on version 8.0 or greater.
2. Create a Pool and RBD image in the pool.
   ceph osd pool create pool 128 128
   rbd pool init pool
   rbd create pool/image1 --size 1G
3.  Format image to an encrypted format with LUKS1
    E.g:
    rbd encryption format pool1/image1 luks1 /tmp/passphrase.bin
4. Write data to encrypted images and take md5sum
   rbd device map -t nbd -o encryption-format=luks1,encryption-passphrase-file=/tmp/passphrase.bin pool1/image1;
   mkfs -t ext4 -E nodiscard /dev/nbd1; mkdir /tmp/test1; mount /dev/nbd1 /tmp/test1;
   cp /var/log/messages /tmp/test1/data1; df -h /tmp/test1/data1; md5sum /tmp/test1/data1
5. Create RAW data of rbd image using rbd export
   E.g: rbd export rep_pool/rimage1 /tmp/image1_raw
6. Create spec file raw data as below
    cat /tmp/raw_spec1.json
    {
    "type": "raw",
    "stream": {
    "type": "file",
    "file_path": "/tmp/image1_raw",
    }
    }
7. Make sure no client should holds that image before Initiate migration using
    rbd migration prepare --import-only
    E.g: rbd migration prepare --import-only --source-spec-path /tmp/raw_spec.json m_pool/mr_image1
8. Execute and commit the migration
    rbd migration execute pool/image
    rbd migration commit pool/image
9. map that migrated image using rbd device map with encryption keys,
    create mount directory and mount it and calculate md5sum and Cross-verify data consistency
    E.g: rbd device map -t nbd -o encryption-format=luks2,encryption-passphrase-file=/tmp/passphrase.bin
    m_pool/mr_image2
10. Unmap the disk and cleanup the pools, namespace and image
11. Repeat the test on LUKS2


Test case covered -
CEPH-83596588 - Live migration of qcow 2format images with encryption

Pre-requisites :
1. Cluster must be up and running with capacity to create pool
2. We need atleast one client node with ceph-common package,
   conf and keyring files

Test Case Flow:
1. Deploy Ceph on version 8.0 or greater.
2. Create a Pool and RBD image in the pool.
   ceph osd pool create pool 128 128
   rbd pool init pool
   rbd create pool/image1 --size 1G
3.  Format image to an encrypted format with LUKS1
    E.g:
    rbd encryption format pool1/image1 luks1 /tmp/passphrase.bin
4. Write data to encrypted images and take md5sum
   rbd device map -t nbd -o encryption-format=luks1,encryption-passphrase-file
   =/tmp/passphrase.bin pool1/image1;
   mkfs -t ext4 -E nodiscard /dev/nbd1; mkdir /tmp/test1; mount /dev/nbd1 /tmp/test1;
   cp /var/log/messages /tmp/test1/data1; df -h /tmp/test1/data1; md5sum /tmp/test1/data1
5. Unmap the encrypted disk using device unmap with proper keys
    E.g: rbd device unmap -t nbd -o encryption-format=luks1,encryption-passphrase-file
    =luks1_passphrase.bin pool1/qimage1
6. Then map it without encryption
    Using rbd map
    E.g: rbd map pool1/qimage1
7. Convert that raw disk data to qcow2 data format using qemu-img convert
    E.g: qemu-img convert -f raw -O qcow2 /dev/rbd0 /tmp/qimage1.qcow2
    8.Create spec file with qcow format data
    [root@ceph-sangadi-bz-sqd86e-node4 tmp]# cat /tmp/qcow_spec.json
    {
    "type": "qcow",
    "stream": {
    "type": "file",
    "file_path": "/tmp/qimage1.qcow2"
    }
    }
9. Make sure no client should holds that image before Initiate migration using
    rbd migration prepare --import-only
    E.g: rbd migration prepare --import-only --source-spec-path /tmp/qcow_spec.json m_pool/mr_image1
    10. Execute and commit the migration
    rbd migration execute pool/image
    rbd migration commit pool/image
11.  map that migrated image using rbd device map with encryption keys,
    create mount directory and mount it and calculate md5sum and Cross-verify data consistency
    E.g: rbd device map -t nbd -o encryption-format=luks2,encryption-passphrase-file=
    /tmp/passphrase.bin m_pool/mr_image2
12. Unmap the disk and cleanup the pools, namespace and image
13. Repeat the test on LUKS2

"""

import tempfile
from copy import deepcopy

from ceph.rbd.initial_config import initial_rbd_config
from ceph.rbd.utils import (
    check_data_integrity,
    create_map_options,
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


def migration_encrypted_raw_images(rbd_obj, client, **kw):
    """
        Test to verify Live migration of raw format images with encryption
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
                err = run_io_on_encryption_formatted_image(rbd, pool, image, **kw)
                if err:
                    return 1

                # Create spec file raw data
                raw_file = tempfile.mktemp(prefix=f"{image}_", suffix=".raw")
                rbd.export(
                    **{
                        "source-image-or-snap-spec": f"{pool}/{image}",
                        "path-name": raw_file,
                    }
                )
                raw_spec = {
                    "type": "raw",
                    "stream": {"type": "file", "file_path": f"{raw_file}"},
                }

                kw["cleanup_files"].append(raw_file)

                kw[pool].update({"spec": raw_spec})
                err = migrate_check_consistency(rbd, pool, image, **kw)
                if err:
                    return 1

    return 0


def migration_encrypted_qcow_images(rbd_obj, client, **kw):
    """
        Test to verify Live migration of qcow2 format images with encryption
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

                err = run_io_on_encryption_formatted_image(rbd, pool, image, **kw)
                if err:
                    return 1

                qcow_file = "/tmp/qcow_" + random_string(len=3) + ".qcow2"
                out, err = client.exec_command(
                    sudo=True,
                    cmd=f"qemu-img convert -f raw -O qcow2 {kw[pool][image]["dev"]} {qcow_file}",
                )
                if err:
                    log.error(f"Image convert to qcow2 failed with err {err}")
                    return 1
                else:
                    log.info("Successfully converted image to qcow2")

                kw["cleanup_files"].append(qcow_file)
                qcow_spec = {
                    "type": "qcow",
                    "stream": {"type": "file", "file_path": f"{qcow_file}"},
                }
                kw[pool].update({"spec": qcow_spec})
                err = migrate_check_consistency(rbd, pool, image, **kw)
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

    # Create an RBD image in pool
    out, err = rbd.create(**{"image-spec": f"{pool}/{image}", "size": 1024})
    if err:
        log.error(f"Create image {pool}/{image} failed with error {err}")
        return 1
    else:
        log.info(f"Successfully created image {pool}/{image}")

    # Format the image with encryption
    passphrase = (
        f"{kw[pool]["encryption_type"]}_passphrase_" + random_string(len=3) + ".bin"
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
            f"Encryption format with {kw[pool]["encryption_type"]} failed on {pool}/{image}"
        )
        return 1
    else:
        log.info(
            f"Successfully formatted the image {pool}/{image} with encryption type {kw[pool]["encryption_type"]}"
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
        log.error(f"Failed to map the source image {pool}/{image} without encryption")
        return 1
    else:
        log.info(
            f"Successfully mapped the source image {pool}/{image} without encryption"
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

    # Prepare Migration
    target_image = "target_image_" + random_string(len=3)
    out, err = rbd.migration.prepare(
        source_spec=kw[pool]["spec"],
        dest_spec=f"{target_pool}/{target_image}",
        client_node=kw["client"],
    )
    if err:
        log.error(f"Migration prepare failed with error {err}")
        return 1
    else:
        log.info("Successfully prepared for migration")

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
    if err:
        log.error(f"Migration execute failed with error {err}")
        return 1
    else:
        log.info("Successfully executed migration")

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
    if err:
        log.error(f"Migration Commit failed with error {err}")
        return 1
    else:
        log.info("Successfully committed migration")

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
        log.error(f"Failed to map migrated image {target_pool}/{target_image}")
        return 1
    else:
        log.info(f"Successfully mapped the migrated image {target_pool}/{target_image}")

    out, err = rbd.device.unmap(**map_config)
    if err:
        log.error(f"Failed to unmap migrated image {target_pool}/{target_image}")
        return 1
    else:
        log.info(
            f"Successfully unmapped the migrated image {target_pool}/{target_image}"
        )

    data_integrity_spec = {
        "first": {
            "image_spec": f"{pool}/{image}",
            "rbd": rbd,
            "client": kw["client"],
            "file_path": f"/tmp/{random_string(len=3)}",
        },
        "second": {
            "image_spec": f"{target_pool}/{target_image}",
            "rbd": rbd,
            "client": kw["client"],
            "file_path": f"/tmp/{random_string(len=3)}",
        },
    }

    rc = check_data_integrity(**data_integrity_spec)
    if rc:
        log.error(f"Data consistency check failed for {target_pool}/{target_image}")
        return 1
    else:
        log.info("Data is consistent between the source and target images.")


def run(**kw):
    """
    This test verifies Live migration of qcow2 format images with encryption
    Args:
        kw: test data
    Returns:
        int: The return value. 0 for success, 1 otherwise

    """
    try:

        log.info(
            f"{kw['config']['testconfig']['id']} - {kw['config']['testconfig']['name']}"
        )

        if kw.get("client_node"):
            client = get_node_by_id(kw.get("ceph_cluster"), kw.get("client_node"))
        else:
            client = kw.get("ceph_cluster").get_nodes(role="client")[0]
        rbd_obj = initial_rbd_config(**kw)
        pool_types = rbd_obj.get("pool_types")
        kw.update({"cleanup_files": []})
        if kw["config"]["testconfig"]["id"] == "CEPH-83596444":
            func = migration_encrypted_raw_images

        elif kw["config"]["testconfig"]["id"] == "CEPH-83596588":
            func = migration_encrypted_qcow_images

        if rbd_obj:
            log.info("Executing test on Replicated and EC pool")
            if func(rbd_obj, client, **kw):
                return 1
            log.info(f"Test {kw['config']['testconfig']['name']} is successful")

    except Exception as e:
        log.error(f"Test {kw['config']['testconfig']['name']} failed: {str(e)}")
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
