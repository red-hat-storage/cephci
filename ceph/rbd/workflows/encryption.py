# -*- code: utf-8 -*-
import random
from copy import deepcopy
from logging import getLogger
from time import sleep

from ceph.rbd.utils import check_data_integrity, copy_file, random_string
from ceph.rbd.workflows.krbd_io_handler import krbd_io_handler
from ceph.rbd.workflows.rbd import create_snap_and_clone

log = getLogger(__name__)

IO_CONFIG = {
    "rbd": None,
    "client": None,
    "read_only": False,
    "size": "",
    "do_not_create_image": True,
    "config": {
        "file_size": "100M",
        "file_path": [],
        "image_spec": [],
        "operations": {
            "fs": "ext4",
            "io": True,
            "mount": True,
            "nounmap": True,
            "device_map": True,
        },
        "skip_mkfs": False,
        "runtime": 30,
        "encryption_config": [],
    },
}


def create_passphrase_file(client, file):
    """Creates a file in the given path and adds a random passphrase in the file
    The passphrase generated and saved in the file will be used for luks encryption

    Args:
        file (str): file_path/file_name in which the passphrase needs to be saved
    """
    passphrase = random_string()

    passphrase_file = client.remote_file(sudo=True, file_name=file, file_mode="w")
    passphrase_file.write(passphrase)
    passphrase_file.flush()


def encrypt_image_and_run_io(**kw):
    """Apply encryption to an image or its clone based on the args specified
    Run IOs on the encrypted image by mapping the image onto a device
    and mounting it to a filepath

    Args:
        kw: <Should contain all parameters in IO_CONFIG variable and the configs below>
            config:
                image_spec: <pool>/<image> for which encryption needs to be applied
                encryption_config: encryption config details for image and its parent
                        <[{encryption-type: <>, encryption-passphrase-file: <>}]>
            size: current size of the image (needed to perform resize post encryption)
            is_clone: True if image is clone
            is_parent_encrypted: True, if image is clone and its parent is encrypted
            is_clone_encrypted: True, if image is clone and it is encrypted
            rbd: RBD object
            client: client CephNode object
            do_not_map_and_mount: True if image need not be mounted and IO not run.
    """
    config = kw.get("config", {})
    size = kw.get("size")
    if config.get("encryption_config"):
        if kw.get("is_clone"):
            if kw.get("is_parent_encrypted"):
                if kw.get("is_clone_encrypted"):
                    parent_clone_spec = {
                        "parent_encryption_type": config.get("encryption_config")[
                            1
                        ].get("encryption-format"),
                        "parent_spec": kw.get("parent_spec"),
                        "parent_passphrase": config.get("encryption_config")[1].get(
                            "encryption-passphrase-file"
                        ),
                        "clone_encryption_type": config.get("encryption_config")[0].get(
                            "encryption-format"
                        ),
                        "clone_spec": config.get("image_spec")[0],
                        "clone_passphrase": config.get("encryption_config")[0].get(
                            "encryption-passphrase-file"
                        ),
                        "size": size,
                    }
                else:
                    parent_clone_spec = {
                        "parent_encryption_type": config.get("encryption_config")[
                            0
                        ].get("encryption-format"),
                        "parent_spec": kw.get("parent_spec"),
                        "parent_passphrase": config.get("encryption_config")[0].get(
                            "encryption-passphrase-file"
                        ),
                        "clone_spec": config.get("image_spec")[0],
                        "size": size,
                    }
            else:
                parent_clone_spec = {
                    "clone_encryption_type": config.get("encryption_config")[0].get(
                        "encryption-format"
                    ),
                    "clone_spec": config.get("image_spec")[0],
                    "parent_spec": kw.get("parent_spec"),
                    "clone_passphrase": config.get("encryption_config")[0].get(
                        "encryption-passphrase-file"
                    ),
                    "size": size,
                }
            resize_sequence = "after_clone"
        else:
            parent_clone_spec = {
                "parent_encryption_type": config.get("encryption_config")[0].get(
                    "encryption-format"
                ),
                "parent_spec": config.get("image_spec")[0],
                "parent_passphrase": config.get("encryption_config")[0].get(
                    "encryption-passphrase-file"
                ),
                "size": size,
            }
            resize_sequence = "before_clone"
        rbd = kw.get("rbd")
        client = kw.get("client")
        image_spec = config.get("image_spec")[0]
        passphrase = config.get("encryption_config")[0].get(
            "encryption-passphrase-file"
        )
        encryption_type = config.get("encryption_config")[0].get("encryption-format")
        if not kw.get("is_clone") or (
            kw.get("is_clone") and kw.get("is_clone_encrypted")
        ):
            create_passphrase_file(client, passphrase)
        encrypt_config = {
            "image-spec": image_spec,
            "format": encryption_type,
            "passphrase-file": passphrase,
        }

        out, _ = rbd.encryption_format(**encrypt_config)
        if out:
            log.error(f"Apply RBD encryption {encryption_type} failed on {image_spec}")
            return 1

        resize_parent_and_clone(
            rbd=rbd,
            resize_sequence=resize_sequence,
            parent_clone_spec=parent_clone_spec,
        )

    if kw.get("do_not_map_and_mount"):
        return 0

    out, _ = krbd_io_handler(**kw)
    if out != 0:
        log.error(f"Map, and mount failed for {image_spec}")
        return 1

    return 0


def resize_parent_and_clone(rbd, resize_sequence, parent_clone_spec):
    """
    Resize parent and clone images to compensate overhead due to the header size.
    Headers required for luks encryption take up some space in the image, so we perform
    resize to compensate for the same.
    Note: Please refer official documentation for more info.

    Scenarios and corresponding steps
    1) Parent and clone are luks1 encrypted
        Resize parent at beginning to compensate for header size,
        clone resize not required
    2) Parent luks1/luks2 and clone is not encrypted
        Resize parent at beginning to compensate for header size,
        clone resize not required since it has no header
    3) Parent luks2 and clone luks1
        Resize parent at beginning to compensate for header size,
        resize clone with --allow-shrink since luks2 header is bigger than luks1
    4) Parent not encrypted and clone is luks1/luks2
        Resize not required before clone since parent is not encrypted, resize
        clone with --allow-shrink post cloning to compensate for header
    5) Parent luks1 and clone luks2
        Resize parent before clone, resize both parent and
        clone with --allow-shrink post cloning

    Justification:
    Since LUKS2 header is usually bigger than LUKS1 header, the rbd resize command at
    the beginning temporarily grows the parent image to reserve some extra space in the
    parent snapshot and consequently the cloned image. This is necessary to make all parent
    data accessible in the cloned image. The rbd resize command at the end shrinks the
    parent image back to its original size and does not impact the parent snapshot and
    the cloned image to get rid of the unused reserved space

    The same applies to creating a formatted clone of an unformatted image,
    since an unformatted image does not have a header at all.
    Args:
        rbd: rbd object
        resize_sequence: before_clone if resize is being performed before cloning, else after_clone
        parent_clone_spec:
            Example: {
                "parent_encryption_type": "luks1",
                "parent_spec": "pool/image",
                "parent_passphrase": "passphrase.bin",
                "clone_encryption_type": "luks2",
                "clone_spec": "pool/clone",
                "clone_passphrase": "clone_passphrase.bin",
                "size": <image_size>
            }
    """
    log.info(
        "Performing resize operation on encrypted image to "
        f"compensate for the overhead of luks header: {resize_sequence}"
    )

    parent_encryption_type = parent_clone_spec.get("parent_encryption_type", "NA")
    clone_encryption_type = parent_clone_spec.get("clone_encryption_type", "NA")
    parent_spec = parent_clone_spec.get("parent_spec")
    [pool, image] = parent_spec.split("/")
    clone_spec = parent_clone_spec.get("clone_spec")
    if clone_spec:
        clone = clone_spec.split("/")[1]
    size = parent_clone_spec.get("size")

    if resize_sequence == "before_clone":
        if parent_encryption_type.startswith("luks"):
            parent_encryption_passphrase = {
                "encryption_config": [
                    {
                        "encryption-passphrase-file": parent_clone_spec.get(
                            "parent_passphrase"
                        )
                    }
                ]
            }
            out = rbd.resize(
                pool=pool, image=image, size=size, **parent_encryption_passphrase
            )
            log.info(f"Output of rbd resize for parent {resize_sequence}: {out}")
            if out[0]:
                log.error(
                    "Image resize to compensate overhead due to "
                    f"{parent_encryption_type} header failed for {parent_spec}"
                )
                return 1
        else:
            log.info("Parent image resize not required since it is not encrypted")
        return 0

    if resize_sequence == "after_clone":
        if parent_encryption_type == "luks1" and clone_encryption_type == "luks2":
            parent_encryption_passphrase = {
                "encryption_config": [
                    {
                        "encryption-passphrase-file": parent_clone_spec.get(
                            "parent_passphrase"
                        )
                    }
                ]
            }
            resize_config = {
                "pool": pool,
                "image": image,
                "size": size,
                "allow-shrink": True,
                "encryption_config": parent_encryption_passphrase["encryption_config"],
            }
            out = rbd.resize(**resize_config)
            log.info(f"Output of rbd resize for parent {resize_sequence}: {out}")
            if "new size is equal to original size" not in str(out[1]):
                log.error(
                    "Image resize to compensate overhead due to "
                    f"{parent_encryption_type} header failed for {parent_spec}"
                )
                return 1

        if (
            clone_encryption_type.startswith("luks")
            and parent_encryption_type != clone_encryption_type
        ):
            if parent_encryption_type.startswith("luks"):
                clone_encryption_passphrase = {
                    "encryption_config": [
                        {
                            "encryption-passphrase-file": parent_clone_spec.get(
                                "clone_passphrase"
                            )
                        },
                        {
                            "encryption-passphrase-file": parent_clone_spec.get(
                                "parent_passphrase"
                            )
                        },
                    ]
                }
            else:
                clone_encryption_passphrase = {
                    "encryption_config": [
                        {
                            "encryption-passphrase-file": parent_clone_spec.get(
                                "clone_passphrase"
                            )
                        }
                    ]
                }
            resize_config = {
                "pool": pool,
                "image": clone,
                "size": size,
                "allow-shrink": True,
                "encryption_config": clone_encryption_passphrase["encryption_config"],
            }
            out = rbd.resize(**resize_config)
            log.info(f"Output of rbd resize for clone {resize_sequence}: {out}")
            if out[0]:
                log.error(
                    "Clone resize to compensate overhead due to "
                    f"{clone_encryption_type} header failed for {clone_spec}"
                )
                return 1
        else:
            log.info(
                "Clone resize not required since clone is either not encrypted or encrypted with same format as parent"
            )
        return 0


def create_and_encrypt_clone(**kw):
    """Create a snap and clone for the given image
    Also apply specified encryption on the clone and mount clone to a file

    Args:
        kw: {
            "rbd": rbd,
            "client": client,
            "format": format,
            "size": size,
            "image_spec": image_spec,
            "clone_spec": clone_spec,
            "snap_spec": snap_spec,
            "parent_passphrase": passphrase,
            "clone_file_path": kw["clone_file_path"],
            "clone_passphrase": None,
            "do_not_map_and_mount": True,
        }
    """
    rbd = kw.get("rbd")
    client = kw.get("client")
    [parent_encryption_type, clone_encryption_type] = kw.get("format").split(",")
    is_parent_encrypted = parent_encryption_type.startswith("luks")
    is_clone_encrypted = clone_encryption_type.startswith("luks")
    size = kw.get("size")
    image_spec = kw.get("image_spec")
    clone_spec = kw.get("clone_spec")
    snap_spec = kw.get("snap_spec")
    passphrase = kw.get("parent_passphrase")
    ret_config = list()

    encrypt_and_io_config = deepcopy(IO_CONFIG)
    encrypt_and_io_config.update(
        {
            "rbd": rbd,
            "client": client,
            "size": size,
            "is_clone": True,
            "is_parent_encrypted": is_parent_encrypted,
            "is_clone_encrypted": is_clone_encrypted,
            "parent_spec": image_spec,
            "do_not_map_and_mount": kw.get("do_not_map_and_mount"),
        }
    )

    encrypt_and_io_config["config"].update(
        {
            "file_path": [kw["clone_file_path"]],
            "image_spec": [clone_spec],
            "skip_mkfs": True,
        }
    )

    encrypt_and_io_config["config"]["operations"].update({"io": False})

    if create_snap_and_clone(rbd=rbd, snap_spec=snap_spec, clone_spec=clone_spec):
        log.error(f"Snapshot and/or clone creation failed for {image_spec}")
        return None

    if is_clone_encrypted:
        log.info(
            f"Applying encryption of type {clone_encryption_type} on the clone {clone_spec}"
        )
        clone_passphrase = (
            kw.get("clone_passphrase")
            or f"clone_{clone_encryption_type}_passphrase.bin"
        )
        if is_parent_encrypted:
            ret_config = encrypt_and_io_config["config"]["encryption_config"] = [
                {
                    "encryption-format": clone_encryption_type,
                    "encryption-passphrase-file": clone_passphrase,
                },
                {
                    "encryption-format": parent_encryption_type,
                    "encryption-passphrase-file": passphrase,
                },
            ]
        else:
            encrypt_and_io_config["config"]["encryption_config"] = ret_config = [
                {
                    "encryption-format": clone_encryption_type,
                    "encryption-passphrase-file": clone_passphrase,
                }
            ]
    elif is_parent_encrypted:
        encrypt_and_io_config["config"]["encryption_config"] = ret_config = [
            {
                "encryption-format": parent_encryption_type,
                "encryption-passphrase-file": passphrase,
            }
        ]

    out = encrypt_image_and_run_io(**encrypt_and_io_config)
    if out:
        log.error(f"Encryption and/or IO execution failed on image {clone_spec}")
        return None

    return ret_config


def test_encryption_between_image_and_clone(**kw):
    """Apply specified encryption to parent, run IOs
    Create a snap and clone for this parent,
    Apply specified encryption to clone, mount the clone to a file
    Verify data integrity between image and its clone.

    Args:
        kw:
            {
                "rbd": RBD object,
                "client": client node,
                "image_spec": <pool>/<parent_image>,
                "snap_spec": <pool>/<parent_image>@<snap>,
                "clone_spec": <pool>/<clone_image>,
                "parent_file_path": <path/file> where parent should be mounted,
                "clone_file_path": <path/file> where clone should be mounted,
                "size": image size,
                "format": encryption format Ex: luks1,luks2 (parent,clone),
            }
    """
    rbd = kw.get("rbd")
    client = kw.get("client")
    [parent_encryption_type, clone_encryption_type] = kw.get("format").split(",")
    size = kw.get("size")
    image_spec = kw.get("image_spec")
    clone_spec = kw.get("clone_spec")
    snap_spec = kw.get("snap_spec")
    ret_config = dict()

    encrypt_and_io_config = deepcopy(IO_CONFIG)
    encrypt_and_io_config.update(
        {
            "rbd": rbd,
            "client": client,
            "size": size,
        }
    )
    encrypt_and_io_config["config"].update(
        {
            "file_path": [kw["parent_file_path"]],
            "image_spec": [image_spec],
        }
    )

    passphrase = ""
    if parent_encryption_type.startswith("luks"):
        log.info(
            f"Applying luks encryption of type {parent_encryption_type} on {image_spec}"
        )
        passphrase = f"{parent_encryption_type}_passphrase.bin"
        encrypt_and_io_config["config"]["encryption_config"] = ret_config[
            "parent_encryption_config"
        ] = [
            {
                "encryption-format": parent_encryption_type,
                "encryption-passphrase-file": passphrase,
            }
        ]
    if encrypt_image_and_run_io(**encrypt_and_io_config):
        log.error(f"Encryption and/or IO execution failed on image {image_spec}")
        return None

    # Sleep for sometime after running IOs before creating snap
    sleep(60)

    clone_config = {
        "rbd": rbd,
        "client": client,
        "format": kw.get("format"),
        "size": size,
        "image_spec": image_spec,
        "clone_spec": clone_spec,
        "snap_spec": snap_spec,
        "parent_passphrase": passphrase,
        "clone_file_path": kw["clone_file_path"],
    }
    ret_config["clone_encryption_config"] = create_and_encrypt_clone(**clone_config)

    if not ret_config["clone_encryption_config"]:
        return None

    data_check_config = {
        "first": {"file_path": kw["parent_file_path"], "rbd": rbd, "client": client},
        "second": {"file_path": kw["clone_file_path"], "rbd": rbd, "client": client},
    }
    if check_data_integrity(**data_check_config):
        log.error(f"Data inconsistency found between {image_spec} and {clone_spec}")
        return None
    else:
        log.info(f"Data is consistent between {image_spec} and {clone_spec}")

    return ret_config


def map_and_mount_image(**kw):
    """Map the given image to a device
    and Mount it to a file specified

    Args:
        kw:
            All parameters required by IO_CONFIG variable and
            return_output: True, if stdout for commands executed is to be returned
    """

    io_config = deepcopy(IO_CONFIG)
    io_config.update(
        {
            "rbd": kw.get("rbd"),
            "client": kw.get("client"),
            "size": kw.get("size"),
            "read_only": kw.get("read_only"),
        }
    )
    io_config["config"].update(
        {
            "file_path": [kw.get("file_path")],
            "image_spec": [kw.get("image_spec")],
            "skip_mkfs": kw.get("skip_mkfs"),
            "encryption_config": kw.get("encryption_config"),
        }
    )
    io_config["config"]["operations"].update(
        {
            "io": kw.get("io"),
            "nounmap": kw.get("nounmap", True),
        }
    )

    out, err = krbd_io_handler(**io_config)

    if err and not kw.get("return_output"):
        log.error(f"Map, and mount failed for {kw.get('image_spec')}")
        return 1

    if kw.get("return_output"):
        return out, err

    return 0


def mount_image_and_mirror_and_check_data(**kw):
    """Check data integrity between an image and its mirror
    Prerequisites:
        An image which is mapped to a device and mounted to a file
        A mirror image to the above original image
    Steps:
        Map the mirrored image and mount it to specified file path
        Verify that the data in original image and mirrored image match

    Args:
        kw:
            "rbd": RBD object pointing to the original image
            "client": Client node for primary cluster
            "mirror_rbd": RBD object pointing to mirrored image
            "mirror_client": Client node for mirrored cluster
            "size": image size
            "encryption_config": encryption applied to the given image
                    <[{encryption-type: <>, encryption-passphrase-file: <>}]>
            "file_path": File where image is mounted in primary cluster
                    and mirrored image to be mounted in mirrored cluster
    """
    rbd = kw.get("rbd")
    client = kw.get("client")
    mirror_rbd = kw.get("mirror_rbd")
    mirror_client = kw.get("mirror_client")
    image_spec = kw.get("image_spec")
    size = kw.get("size")

    if kw.get("encryption_config"):
        passphrase = kw.get("encryption_config")[0].get("encryption-passphrase-file")
        copy_file(passphrase, client, mirror_client)

    mount_config = {
        "rbd": mirror_rbd,
        "client": mirror_client,
        "size": size,
        "file_path": kw.get("file_path"),
        "image_spec": image_spec,
        "encryption_config": kw.get("encryption_config"),
        "io": False,
        "skip_mkfs": True,
        "return_output": False,
        "read_only": True,
    }

    if map_and_mount_image(**mount_config):
        log.error(f"Map and mount failed for {image_spec}")
        return 1

    data_check_config = {
        "first": {"file_path": kw["file_path"], "rbd": rbd, "client": client},
        "second": {
            "file_path": kw["file_path"],
            "rbd": mirror_rbd,
            "client": mirror_client,
        },
    }
    if check_data_integrity(**data_check_config):
        log.error(f"Data inconsistency found between {image_spec} and its mirror")
        return 1
    else:
        log.info(f"Data is consistent between {image_spec} and its mirror")
    return 0


def get_wrong_format(**kw):
    """Fetch the encryption format other than the
    format specified in input

    Args:
        kw:
            input_: Input format
    Returns:
        luks2 if luks1 is input and viceversa
        returns a random choice luks1/luks2 if input is not specified
    """
    format = kw.get("input_")
    encryption_format = ["luks1", "luks2"]

    if format not in encryption_format:
        return random.choice(encryption_format)

    return "luks1" if "luks1" != format else "luks2"


def get_wrong_passphrase(**kw):
    """Create a dummy passphrase file and return it

    Args:
        kw:
            client: Client node where passphrase file needs to be created
    """
    client = kw.get("client")
    dummy_passphrase_file = "dummy_passphrase.bin"
    create_passphrase_file(client, dummy_passphrase_file)
    return dummy_passphrase_file


def return_input(**kw):
    """Returns the value specified in kw["input_"]"""
    return kw.get("input_")


def return_empty(**kw):
    """Method to return None"""
    return None


def validate_neg_scenario(
    description, expected_error, actual_output, actual_error, alternate_error=[]
):
    """Compare the actual error to expected and alternate errors specified

    Args:
        description: Negative scenario being tested
        expected_error: List of strings defining the error message required for the scenario
        actual_output: actual output code, 0 if neg scenario command passed, 1 if command failed
        actual_error: Error message received by executing command for given negative scenario
        alternate_error: If any alternate error message is also acceptable for a scenario

    Returns:
        0 if validation is successful, 1 otherwise
    """
    if not actual_output:
        log.error(f"Negative scenario {description} did not throw any error")
        return 1
    else:
        is_error_expected = False
        if all(error in actual_error for error in expected_error):
            is_error_expected = True
        elif alternate_error and all(
            error in actual_error for error in alternate_error
        ):
            is_error_expected = True
        if is_error_expected:
            log.info(
                f"Negative scenario {description} failed appropriately with error {actual_error}"
            )
            return 0
        else:
            log.error(
                f"Error message {actual_error} is not appropriate for scenario {description}"
            )
            return 1


class NegativeScenario:
    def __init__(
        self,
        desc="",
        format_methods=None,
        passphrase_methods=None,
        error_message=[],
        alternate_error=[],
    ):
        self.desc = desc
        self.format_methods = format_methods or []
        self.passphrase_methods = passphrase_methods or []
        self.error_message = error_message
        self.alternate_error = alternate_error
        assert len(self.format_methods) == len(self.passphrase_methods)


def execute_neg_encryption_scenarios(**kw):
    """Execute all the negative scenarios specified in kw["scenarios"]
    and validate the same for the given image

    Args:
        kw:
            scenarios: list of scenarios to be executed, Ex: [1,4,10]
            encryption_config: current encryption config
                                <[{encryption-type: <>, encryption-passphrase-file: <>}]>
            rbd: RBD object
            client: client node
            size: image size
            image_spec: <pool>/<image> for which scenarios to be tested

    Scenarios:
        1. wrong image format (when image has no parent)
        2. wrong image passphrase (when image has no parent)
        3. no image config (when image has no parent)
        4. wrong image config (both format and passphrase are wrong)(when image has no parent)
        5. wrong parent format
        6. wrong parent passphrase
        7. no parent config
        8. wrong parent config (both format and passphrase are wrong)
        9. wrong image format and parent format
        10. wrong image passphrase and parent passphrase
        11. wrong image config (both format and passphrase are wrong)(when parent in present)
        12. wrong image config and parent config (both format and passphrase are wrong)
        13. no image config(when parent in present and encrypted)
        14. no image config(when parent in present and not encrypted)
        15. no image config(when parent is present and clone not encrypted)
        16. mount read-only image as read-write
    """
    # scenario metadata is a dictionary for each scenario as given below
    scenario_metadata = {
        1: NegativeScenario(
            desc="wrong image format (when image has no parent)",
            format_methods=[get_wrong_format],
            passphrase_methods=[return_input],
            error_message=[
                "image name: image_name",
                "failed to load encryption",
                "(22)",
                "Invalid argument",
            ],
        ),
        2: NegativeScenario(
            desc="wrong image passphrase (when image has no parent)",
            format_methods=[return_input],
            passphrase_methods=[get_wrong_passphrase],
            error_message=[
                "image name: image_name",
                "failed to load encryption",
                "(1)",
                "Operation not permitted",
            ],
        ),
        3: NegativeScenario(
            desc="no image config (when image has no parent)",
            format_methods=[return_empty],
            passphrase_methods=[return_empty],
            error_message=["unknown filesystem type 'crypto_LUKS'"],
        ),
        4: NegativeScenario(
            desc="wrong image config (both format and passphrase are wrong)(when image has no parent)",
            format_methods=[get_wrong_format],
            passphrase_methods=[get_wrong_passphrase],
            error_message=[
                "image name: image_name",
                "failed to load encryption",
                "(22)",
                "Invalid argument",
            ],
        ),
        5: NegativeScenario(
            desc="wrong parent format",
            format_methods=[return_input, get_wrong_format],
            passphrase_methods=[return_input, return_input],
            error_message=[
                "image name: parent_name",
                "failed to load encryption",
                "(22)",
                "Invalid argument",
            ],
        ),
        6: NegativeScenario(
            desc="wrong parent passphrase",
            format_methods=[return_input, return_input],
            passphrase_methods=[return_input, get_wrong_passphrase],
            error_message=[
                "image name: parent_name",
                "failed to load encryption",
                "(1)",
                "Operation not permitted",
            ],
        ),
        7: NegativeScenario(
            desc="no parent config",
            format_methods=[return_input, return_empty],
            passphrase_methods=[return_input, return_empty],
            error_message=[
                "image name: parent_name",
                "failed to load encryption",
                "(1)",
                "Operation not permitted",
            ],
            alternate_error=[
                "image name: parent_name",
                "failed to load encryption",
                "(22)",
                "Invalid argument",
            ],
            # alternate error occurs when encryption is luks2,luks1
            # or luks1,luks2 since by default luks1 is taken
            # as encryption format when nothing is specified
        ),
        8: NegativeScenario(
            desc="wrong parent config (both format and passphrase are wrong)",
            format_methods=[return_input, get_wrong_format],
            passphrase_methods=[return_input, get_wrong_passphrase],
            error_message=[
                "image name: parent_name",
                "failed to load encryption",
                "(22)",
                "Invalid argument",
            ],
        ),
        9: NegativeScenario(
            desc="wrong image format and parent format",
            format_methods=[get_wrong_format, get_wrong_format],
            passphrase_methods=[return_input, return_input],
            error_message=[
                "image name: image_name",
                "failed to load encryption",
                "(22)",
                "Invalid argument",
            ],
        ),
        10: NegativeScenario(
            desc="wrong image passphrase and parent passphrase",
            format_methods=[return_input, return_input],
            passphrase_methods=[get_wrong_passphrase, get_wrong_passphrase],
            error_message=[
                "image name: image_name",
                "failed to load encryption",
                "(1)",
                "Operation not permitted",
            ],
        ),
        11: NegativeScenario(
            desc="wrong image config (both format and passphrase are wrong)(when parent in present)",
            format_methods=[get_wrong_format, return_input],
            passphrase_methods=[get_wrong_passphrase, return_input],
            error_message=[
                "image name: image_name",
                "failed to load encryption",
                "(22)",
                "Invalid argument",
            ],
        ),
        12: NegativeScenario(
            desc="wrong image config and parent config (both format and passphrase are wrong)",
            format_methods=[get_wrong_format, get_wrong_format],
            passphrase_methods=[get_wrong_passphrase, get_wrong_passphrase],
            error_message=[
                "image name: image_name",
                "failed to load encryption",
                "(22)",
                "Invalid argument",
            ],
        ),
        13: NegativeScenario(
            desc="no image config(when parent in present and encrypted)",
            format_methods=[return_empty, return_input],
            passphrase_methods=[return_empty, return_input],
            error_message=[
                "image name: image_name",
                "failed to load encryption",
                "(1)",
                "Operation not permitted",
            ],
            alternate_error=[
                "image name: image_name",
                "failed to load encryption",
                "(22)",
                "Invalid argument",
            ],
            # alternate error occurs when encryption is luks2,luks1
            # or luks1,luks2 since by default luks1 is taken
            # as encryption format when nothing is specified
        ),
        14: NegativeScenario(
            desc="no image config(when parent in present and not encrypted)",
            format_methods=[return_empty, return_input],
            passphrase_methods=[return_empty, return_input],
            error_message=[
                "wrong fs type, bad option, bad superblock",
                "missing codepage or helper program, or other error",
            ],
        ),
        15: NegativeScenario(
            desc="no image config(when parent is present and clone not encrypted)",
            format_methods=[return_empty],
            passphrase_methods=[return_empty],
            error_message=[
                "wrong fs type, bad option, bad superblock",
                "missing codepage or helper program, or other error",
            ],
        ),
    }

    encryption_config = kw.get("encryption_config")
    scenarios = kw.get("scenarios")

    if len(encryption_config) == 1 and any(
        s > 4 and s not in [15, 16] for s in scenarios
    ):
        log.error("Parent clone scenario requested for an image with no parent")
        return 1

    mount_config = {
        "rbd": kw.get("rbd"),
        "client": kw.get("client"),
        "size": kw.get("size"),
        "file_path": "",
        "image_spec": kw.get("image_spec"),
        "encryption_config": encryption_config,
        "io": False,
        "skip_mkfs": True,
        "return_output": True,
        "read_only": kw.get("read_only"),
        "nounmap": False,
    }

    if 16 in scenarios:
        desc = "mount read-only image as read-write"
        error_message = ["can't read superblock"]
        scenarios.remove(16)
        file_path = f"/tmp/{random_string(len=5)}_dir/{random_string(len=5)}_file"

        dummy_encryption_config = list()
        for enc_config in mount_config["encryption_config"]:
            if enc_config.get("encryption-format") != "NA":
                dummy_encryption_config.append(enc_config)

        mount_config.update(
            {
                "file_path": file_path,
                "read_only": False,
                "encryption_config": dummy_encryption_config,
            }
        )
        out, err = map_and_mount_image(**mount_config)
        if not isinstance(err, str):
            err = str(err)
        if validate_neg_scenario(desc, error_message, out, err):
            return 1
        mount_config.update({"read_only": kw.get("read_only")})

    for s_id in scenarios:
        dummy_encryption_config = list()
        scenario_obj = scenario_metadata.get(s_id)

        for format_method, passphrase_method, config in zip(
            scenario_obj.format_methods,
            scenario_obj.passphrase_methods,
            encryption_config,
        ):
            new_format = format_method(input_=config.get("encryption-format"))
            new_passphrase = passphrase_method(
                client=kw.get("client"), input_=config.get("encryption-passphrase-file")
            )
            if new_passphrase:
                encrypt_config = {
                    "encryption-passphrase-file": new_passphrase,
                }
                if new_format != "NA":
                    encrypt_config.update({"encryption-format": new_format})
                dummy_encryption_config.append(encrypt_config)

        file_path = f"/tmp/{random_string(len=5)}_dir/{random_string(len=5)}_file"
        mount_config.update(
            {"file_path": file_path, "encryption_config": dummy_encryption_config}
        )

        out, err = map_and_mount_image(**mount_config)

        if not isinstance(err, str):
            err = str(err)

        image = kw.get("image_spec").split("/")[1] if kw.get("image_spec") else ""
        parent = kw.get("parent_spec").split("/")[1] if kw.get("parent_spec") else ""
        expected_err = [
            err.replace("image_name", image).replace("parent_name", parent)
            for err in scenario_obj.error_message
        ]
        alternate_error = []
        if scenario_obj.alternate_error:
            alternate_error = [
                err.replace("image_name", image).replace("parent_name", parent)
                for err in scenario_obj.alternate_error
            ]
        if validate_neg_scenario(
            scenario_obj.desc, expected_err, out, err, alternate_error
        ):
            return 1

    return 0
