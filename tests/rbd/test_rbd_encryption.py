from time import sleep

from krbd_io_handler import krbd_io_handler

from tests.rbd.rbd_utils import (
    create_passphrase_file,
    device_cleanup,
    initial_rbd_config,
)
from utility.log import Log

log = Log(__name__)


def test_rbd_encryption(rbd, pool_type, format, **kw):
    """
    Perform luks encryption on the given rbd pool, image and its clone and verify data integrity
    Args:
        rbd: rbd object
        pool_type: pool type (ec_pool_config or rep_pool_config)
        format: encryption format of parent and clone Ex: luks1,luks2
        **kw: Test data
    """
    [parent_encryption_type, clone_encryption_type] = format.split(",")
    pool = kw["config"][pool_type]["pool"]
    image = f"{parent_encryption_type}_{rbd.random_string(len=5)}"
    size = "1G"
    kw["config"][pool_type]["size"] = size
    snap = f"{clone_encryption_type}_snap_{rbd.random_string(len=5)}"
    clone = f"{clone_encryption_type}_clone_{rbd.random_string(len=5)}"
    image_spec = f"{pool}/{image}"
    clone_spec = f"{pool}/{clone}"
    snap_spec = f"{pool}/{image}@{snap}"
    encryption_config = list()
    parent_file_path = f"/tmp/{image}_dir/{image}_file"
    clone_file_path = f"/tmp/{clone}_dir/{image}_file"

    try:
        io_config = {
            "file_size": "100M",
            "file_path": [parent_file_path],
            "image_spec": [f"{image_spec}"],
            "operations": {
                "fs": "ext4",
                "io": True,
                "mount": True,
                "nounmap": True,
                "device_map": True,
            },
            "runtime": 30,
        }

        rbd.create_image(pool, image, size)
        if parent_encryption_type.startswith("luks"):
            log.info(
                f"Applying luks encryption of type {parent_encryption_type} on {image_spec}"
            )
            passphrase = f"{parent_encryption_type}_passphrase.bin"
            create_passphrase_file(rbd, passphrase)
            if rbd.encrypt(image_spec, parent_encryption_type, passphrase):
                log.error(
                    f"Apply RBD encryption {parent_encryption_type} failed on {image_spec}"
                )

            encryption_passphrase = {
                "passphrase": [{"encryption-passphrase-file": passphrase}]
            }

            if rbd.image_resize(pool, image, size, **encryption_passphrase):
                log.error(
                    "Image resize to compensate overhead due to "
                    f"{parent_encryption_type} header failed for {image_spec}"
                )
                return 1

            encryption_config.append({parent_encryption_type: passphrase})
        else:
            log.info("Parent image is not encrypted")

        io_config["encryption_config"] = encryption_config
        kw["config"].update(io_config)
        kw.update({"rbd_obj": rbd, "do_not_create_image": True})
        if krbd_io_handler(**kw):
            log.error(f"Map, mount and run IOs failed for {image_spec}")
            return 1

        # Sleep for sometime after running IOs before creating snap
        sleep(60)

        kw["config"].pop("device_names", [])

        parent_md5sum = rbd.exec_cmd(
            output=True, cmd="md5sum {}".format(parent_file_path)
        ).split()[0]

        log.info(f"Creating snap and clone for {image_spec}")
        if rbd.snap_create(pool, image, snap):
            log.error(f"Snapshot with name {snap} creation failed for {image_spec}")
            return 1

        if rbd.protect_snapshot(snap_spec):
            log.error(f"Snapshot protect failed for {image_spec}")
            return 1

        if rbd.create_clone(snap_spec, pool, clone):
            log.error(f"Clone creation failed for {clone_spec}")
            return 1

        if clone_encryption_type.startswith("luks"):
            log.info(
                f"Applying encryption of type {clone_encryption_type} on the clone {clone_spec}"
            )
            clone_passphrase = f"clone_{clone_encryption_type}_passphrase.bin"
            create_passphrase_file(rbd, clone_passphrase)
            if rbd.encrypt(clone_spec, clone_encryption_type, clone_passphrase):
                log.error(
                    f"Apply RBD encryption {clone_encryption_type} failed on {image_spec}"
                )
                return 1

            encryption_config.append({clone_encryption_type: clone_passphrase})
        else:
            log.info("Clone image is not encrypted")

        kw["config"]["encryption_config"] = list(reversed(encryption_config))
        kw["config"]["image_spec"] = [clone_spec]
        kw["config"]["file_path"] = [clone_file_path]
        kw["config"]["operations"].update({"io": False})
        kw["config"]["skip_mkfs"] = True

        if krbd_io_handler(**kw):
            log.error(f"Map, and mount failed for {clone_spec}")
            return 1
        kw["config"]["skip_mkfs"] = False

        kw["config"].pop("device_names", [])

        clone_md5sum = rbd.exec_cmd(
            output=True, cmd="md5sum {}".format(clone_file_path)
        ).split()[0]

        log.info(
            f"Verifying data integrity between original image: {image_spec} and clone: {clone_spec}"
        )

        log.info(
            f"md5sum values of parent and clone are {parent_md5sum} and {clone_md5sum} respectively"
        )
        if parent_md5sum == clone_md5sum:
            log.info(f"Data is consistent between {image_spec} and {clone_spec}")
        else:
            log.error(f"Data inconsistency found between {image_spec} and {clone_spec}")

        flatten_config = {"encryption_config": list(reversed(encryption_config))}
        if rbd.flatten_clone(pool, clone, **flatten_config):
            log.error(f"Flatten clone with encryption failed for {clone_spec}")
            return 1

        kw["config"]["device_names"] = []
        return 0
    except Exception as error:
        log.error(str(error))
        return 1
    finally:
        log.info("Clean up parent device")
        encrypt_config = list()
        if parent_encryption_type.startswith("luks"):
            encrypt_config.append({parent_encryption_type: passphrase})
        device_cleanup(
            rbd=rbd,
            file_name=parent_file_path.rsplit("/", 1)[0],
            image_spec=image_spec,
            encryption_config=encrypt_config,
        )

        log.info("Cleanup clone device")
        if clone_encryption_type.startswith("luks"):
            encrypt_config = encryption_config
        device_cleanup(
            rbd=rbd,
            file_name=clone_file_path.rsplit("/", 1)[0],
            image_spec=clone_spec,
            encryption_config=encrypt_config,
        )


def cleanup(rbd_obj, **kw):
    """Cleanup the replicated and ecpools created for the test module

    Args:
        rbd_obj (dict): dictionary containing rbd ecpool and replicated pool object
    """
    log.info("Cleaning up pools")
    obj = rbd_obj.get("rbd_ecpool", rbd_obj["rbd_reppool"])
    obj.clean_up(
        pools=[
            kw["config"].get("rep_pool_config", {})["pool"],
            kw["config"].get("ec_pool_config", {})["pool"],
        ]
    )


def run(**kw):
    """Verify the luks encryption functionality for image and clone

    Pre-requisites :
    We need atleast one client node with ceph-common, fio and rbd-nbd packages,
    conf and keyring files

    Test cases covered -
    1) CEPH-83575263 - Encrypt & decrypt file using same keys and different keys

    kw:
        config:
            encryption_format: #parent,clone
                - luks1, luks1
    Test Case Flow
    1. Create a pool and an Image
    2. Apply Encryption LUKS1 format to the image
    3. Perform resize operations to compensate the overhead due to LUKS1 header
    4. Load encryption using rbd device map
    5. Mount the image and run IOs
    6. Create snapshot, Protect the snapshot and run IOs
    7. Perform clone from the snapshot of the image created
    8. Apply encryption with different passphrase key other than parent image
    9. Load the encryption using rbd device map on cloned image
    10. Mount the clone and run Ios
    11. Flatten the clone
    12. Verify data integrity between clone image and the original image

    2) CEPH-83575251 - Apply different combinations of luks encryption on an RBD image
    and its clone and verify data integrity between parent and clone

    kw:
        config:
            encryption_format: #parent,clone
                - NA, luks1
                - NA, luks2
                - luks1, luks1
                - luks1, luks2
                - luks1, NA
                - luks2, luks1
                - luks2, luks2
                - luks2, NA
    Test Case Flow
    1. Create a pool and an Image
    2. Apply encryption on image as per formats specified above
    3. Mount image and run IOs
    4. Create snapshot, protect, create clone
    5. Apply encryption on clone as per formats specified above
    6. Mount clone and run IOs
    7. Flatten clone and verify data integrity between clone and parent
    8. Repeat the above steps for all encryption formats mentioned in config
    """
    log.info("Running luks encryption tests")

    kw.get("config", {}).update({"do_not_create_image": True})
    rbd_obj = initial_rbd_config(**kw)

    for format in kw["config"].get("encryption_type"):
        log.info(f"Executing encryption test for format parent,clone: {format}")
        if rbd_obj:
            if "rbd_reppool" in rbd_obj:
                log.info("Executing test on Replicated pool")
                if test_rbd_encryption(
                    rbd_obj.get("rbd_reppool"), "rep_pool_config", format, **kw
                ):
                    log.error(
                        f"Encryption test failed for format parent,clone: {format} on replicated pool"
                    )
                    cleanup(rbd_obj, **kw)
                    return 1
                log.info(f"{kw['config'].get('device_names')}")
            if "rbd_ecpool" in rbd_obj:
                log.info("Executing test on EC pool")
                if test_rbd_encryption(
                    rbd_obj.get("rbd_ecpool"), "ec_pool_config", format, **kw
                ):
                    log.error(
                        f"Encryption test failed for format parent,clone: {format} on ec pool"
                    )
                    cleanup(rbd_obj, **kw)
                    return 1
                log.info(f"{kw['config'].get('device_names')}")
    cleanup(rbd_obj, **kw)
    return 0
