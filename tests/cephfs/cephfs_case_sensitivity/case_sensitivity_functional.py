import os
import random
import string

from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.exceptions import (
    CaseSensitivityValidationError,
    CharMapValidationError,
    FileDoesNotExistError,
    FileNotFound,
    FileOperationError,
    FsBaseException,
    LinkDeletionError,
    NormalizationValidationError,
    RenameDirectoryError,
    SnapshotValidationError,
    log_and_fail,
)
from tests.cephfs.lib.cephfs_attributes_lib import CephFSAttributeUtilities
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from tests.smb.smb_operations import remove_smb_cluster, remove_smb_share
from utility.log import Log

log = Log(__name__)


def test_case_sensitivity():
    """Tests setting and verifying case sensitivity for a directory."""
    global dir_path
    dir_path = os.path.join(fuse_mounting_dir, "step-1")
    attr_util.create_directory(client1, dir_path)

    # Set case sensitivity to sensitive and verify
    attr_util.set_attributes(client1, dir_path, casesensitive=1)
    attr_util.validate_charmap(client1, dir_path, {"casesensitive": True})
    log.info("Passed: casesensitivity set to sensitive")

    # Set case sensitivity to insensitive and verify
    attr_util.set_attributes(client1, dir_path, casesensitive=0)
    attr_util.validate_charmap(client1, dir_path, {"casesensitive": False})
    log.info("Passed: casesensitivity set to insensitive")


def conflicting_directories():
    """Tests creating directories with conflicting names in case-sensitive mode."""
    attr_util.set_attributes(client1, dir_path, casesensitive=1)
    attr_util.create_directory(client1, os.path.join(dir_path, "Dir1"))
    attr_util.create_directory(client1, os.path.join(dir_path, "dir1"))
    log.info("Passed: Conflicting directories created successfully")


def check_default_value():
    """Tests the default values for normalization and encoding attributes."""
    default_dir = os.path.join(fuse_mounting_dir, "step-3")
    attr_util.create_directory(client1, default_dir)
    attr_util.set_attributes(client1, default_dir, casesensitive=1)

    attr_util.validate_charmap(
        client1,
        default_dir,
        {"normalization": "nfd", "encoding": "utf8", "casesensitive": True},
    )
    attr_util.delete_directory(client1, default_dir, recursive=True)

    attr_util.create_directory(client1, default_dir)
    attr_util.set_attributes(client1, default_dir, normalization="nfc")

    attr_util.validate_charmap(
        client1,
        default_dir,
        {"normalization": "nfc", "encoding": "utf8", "casesensitive": True},
    )

    log.info("Passed: Default values are validated")


def sub_directory_inheritance_case_True():
    """Tests subdirectory inheritance of attributes when case sensitivity is True."""
    normalization_types = ["nfkd", "nfkc", "nfd", "nfc"]
    for norm_type in normalization_types:
        parent_dir = attr_util.create_special_character_directories(
            client1, fuse_mounting_dir
        )
        rel_parent_dir = os.path.relpath(parent_dir, fuse_mounting_dir)

        attr_util.set_attributes(
            client1, parent_dir, casesensitive=1, normalization=norm_type
        )

        alter_dict = attr_util.fetch_alternate_name(client1, fs_name, "/")
        if not attr_util.validate_alternate_name(
            alter_dict, rel_parent_dir, norm_type.upper(), empty_name=True
        ):
            log.error("Validation failed for alternate name")
            return 1

        log.info("Validating for Normalization: {}".format(norm_type))
        child_dir = attr_util.create_special_character_directories(client1, parent_dir)
        rel_child_dir = os.path.relpath(child_dir, fuse_mounting_dir)
        actual_child_dir_root = rel_child_dir.split("/")[0]
        actual_child_dir_name = rel_child_dir.split("/")[1]

        attr_util.validate_charmap(
            client1,
            child_dir,
            {"normalization": norm_type, "encoding": "utf8", "casesensitive": True},
        )

        if not attr_util.validate_normalization(
            client1,
            fs_name,
            actual_child_dir_root,
            actual_child_dir_name,
            norm_type.upper(),
        ):
            raise NormalizationValidationError(
                "Normalization validation failed for dir '{}' using type '{}'.".format(
                    actual_child_dir_name, norm_type
                )
            )

        log.info("Validating alternate name for %s", rel_child_dir)
        alter_dict = attr_util.fetch_alternate_name(client1, fs_name, "/")
        if not attr_util.validate_alternate_name(
            alter_dict, rel_child_dir, norm_type.upper()
        ):
            log.error("Validation failed for alternate name")
            return 1

        log.info("** Cleanup ** ")
        attr_util.delete_directory(client1, parent_dir, recursive=True)

    log.info("Passed: Subdirectory inherited attribute")


def test_empty_normalization_and_encoding_inheritance():
    """Tests the inheritance of empty normalization and encoding attributes."""
    dir_step_5 = os.path.join(fuse_mounting_dir, "step-5")

    for attribute in ["normalization", "encoding"]:
        log.info("Setting empty {} and validating the default values".format(attribute))
        attr_util.create_directory(client1, dir_step_5)

        try:
            attr_util.get_charmap(client1, dir_step_5)
            log.error(
                "Charmap expected to fail for new directory when it's parent directory does not have charmap"
            )
        except ValueError:
            log.info(
                "Get Charmap expected to fail when there it's a new folder and "
                " parent directory does not have charmap set"
            )

        attr_util.set_attributes(client1, dir_step_5, **{attribute: '""'})

        attr_util.validate_charmap(
            client1,
            dir_step_5,
            {"normalization": "nfd", "encoding": "utf8", "casesensitive": True},
        )

        dir_step_5a = os.path.join(dir_step_5, "step-5a")
        attr_util.create_directory(client1, dir_step_5a)

        attr_util.validate_charmap(
            client1,
            dir_step_5a,
            {"normalization": "nfd", "encoding": "utf8", "casesensitive": True},
        )

        attr_util.delete_directory(client1, dir_step_5a, recursive=True)
        attr_util.delete_directory(client1, dir_step_5, recursive=True)

    log.info("Passed: Empty normalization and encoding")


def test_directory_normalization():
    """Tests setting and validating normalization for a new directory."""
    dir_path_6 = os.path.join(fuse_mounting_dir, "step-6")
    attr_util.create_directory(client1, dir_path_6)
    attr_util.set_attributes(client1, dir_path_6, normalization="''")
    log.info(attr_util.get_charmap(client1, dir_path_6).get("normalization"))

    normalize_supported = ["nfc", "nfd", "nfkd", "nfkc"]

    for supported_value in normalize_supported:
        log.info("Test for Normalization: {}".format(supported_value))
        uni_names = attr_util.generate_random_unicode_names()
        dir_path_6_child = os.path.join(dir_path_6, uni_names[0])
        rel_dir_path_6_child = os.path.relpath(dir_path_6_child, fuse_mounting_dir)
        attr_util.create_directory(client1, dir_path_6_child)

        attr_util.set_attributes(
            client1, dir_path_6_child, normalization=supported_value
        )

        attr_util.validate_charmap(
            client1, dir_path_6_child, {"normalization": supported_value}
        )

        alter_dict = attr_util.fetch_alternate_name(client1, fs_name, "/")
        if not attr_util.validate_alternate_name(
            alter_dict,
            rel_dir_path_6_child,
            supported_value.upper(),
        ):
            log.error("Validation failed for alternate name")
            return 1

        log.info("Passed: Normalization set to {}".format(supported_value))

    log.info("Passed: Set normalization for a new directory")


def test_subdirectory_inheritance_special_chars():
    """Tests subdirectory inheritance of attributes when case sensitivity is False."""
    normalization_types = ["nfkd", "nfkc", "nfd", "nfc"]
    for norm_type in normalization_types:
        parent_dir = attr_util.create_special_character_directories(
            client1, fuse_mounting_dir
        )
        rel_parent_dir = os.path.relpath(parent_dir, fuse_mounting_dir)
        attr_util.set_attributes(
            client1, parent_dir, casesensitive=0, normalization=norm_type
        )

        alter_dict = attr_util.fetch_alternate_name(client1, fs_name, "/")
        if not attr_util.validate_alternate_name(
            alter_dict,
            rel_parent_dir,
            norm_type.upper(),
            empty_name=True,
            casesensitive=False,
        ):
            log.error("Validation failed for alternate name")
            return 1

        log.info("Validating for Normalization: {}".format(norm_type))
        child_dir = attr_util.create_special_character_directories(client1, parent_dir)
        rel_child_dir = os.path.relpath(child_dir, fuse_mounting_dir)
        actual_child_dir_root = rel_child_dir.split("/")[0]
        actual_child_dir_name = rel_child_dir.split("/")[1]

        attr_util.validate_charmap(
            client1,
            child_dir,
            {"normalization": norm_type, "encoding": "utf8", "casesensitive": False},
        )

        if not attr_util.validate_normalization(
            client1,
            fs_name,
            actual_child_dir_root,
            actual_child_dir_name,
            norm_type.upper(),
            casesensitive=False,
        ):
            raise NormalizationValidationError(
                "Normalization validation failed for dir '{}' using type '{}'.".format(
                    actual_child_dir_name, norm_type
                )
            )

        log.info("Validating alternate name for %s", rel_child_dir)
        alter_dict = attr_util.fetch_alternate_name(client1, fs_name, "/")
        if not attr_util.validate_alternate_name(
            alter_dict, rel_child_dir, norm_type.upper(), casesensitive=False
        ):
            log.error("Validation failed for alternate name")
            return 1

        log.info("** Cleanup ** ")
        attr_util.delete_directory(client1, parent_dir, recursive=True)

    log.info("Passed: Subdirectory inherited attribute")


def test_snapshot_functionality_with_attributes():
    """Tests the functionality of snapshots with attributes set on a subvolume."""
    sv_group = "sv_group"
    sv_name = "subvol1"
    snap_name = "snap_1"
    fs_util.create_subvolumegroup(client1, fs_name, sv_group)
    fs_util.create_subvolume(client1, fs_name, sv_name, group_name=sv_group)

    log.info("Get the path of sub volume")
    subvol_path_test_snap, rc = client1.exec_command(
        sudo=True,
        cmd="ceph fs subvolume getpath {} {} {}".format(fs_name, sv_name, sv_group),
    )

    mounting_dir = "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in list(range(10))
    )
    snap_fuse_mounting_dir = "/mnt/cephfs_fuse_snap_{}/".format(mounting_dir)
    attr_util.create_directory(client1, snap_fuse_mounting_dir)

    fs_util.fuse_mount(
        [client1],
        snap_fuse_mounting_dir,
        extra_params=" -r {} --client_fs {}".format(
            subvol_path_test_snap.strip(), fs_name
        ),
    )

    attr_util.set_attributes(
        client1, snap_fuse_mounting_dir, casesensitive=0, normalization="nfkc"
    )
    fs_util.create_snapshot(client1, fs_name, sv_name, snap_name, group_name=sv_group)

    if not attr_util.validate_snapshot_from_mount(
        client1, snap_fuse_mounting_dir, [snap_name]
    ):
        raise SnapshotValidationError(
            "Snapshot validation failed for {}".format(snap_fuse_mounting_dir)
        )

    attr_util.validate_charmap(
        client1,
        snap_fuse_mounting_dir,
        {"casesensitive": False, "normalization": "nfkc", "encoding": "utf8"},
    )

    try:
        attr_util.set_attributes(
            client1, snap_fuse_mounting_dir, casesensitive=1, normalization="nfd"
        )
        log.error("Expected to fail when snapshot exists")
        return 1
    except Exception:
        log.info(
            "Passed: Failed as Expected. Attributes should not be set on a snapshot directory"
        )

    log.info("Removing the snapshot")
    fs_util.remove_snapshot(
        client1,
        fs_name,
        sv_name,
        snap_name,
        **{"group_name": sv_group},
    )

    attr_util.set_attributes(
        client1, snap_fuse_mounting_dir, casesensitive=1, normalization="nfd"
    )
    attr_util.validate_charmap(
        client1,
        snap_fuse_mounting_dir,
        {"casesensitive": True, "normalization": "nfd", "encoding": "utf8"},
    )

    log.info("Passed: Validated snapshot functionality")


def test_directory_rename_attribute_validation():
    """Tests renaming a directory and validating the attributes after the rename."""
    dir_9_root = os.path.join(fuse_mounting_dir, "step-9")
    child_1 = os.path.join(dir_9_root, "child-1")
    child_1_renamed = os.path.join(dir_9_root, "gá")
    child_2 = os.path.join(child_1_renamed, "child-2")
    attr_util.create_directory(client1, dir_9_root, force=True)

    attr_util.set_attributes(client1, dir_9_root, normalization="nfkc")
    attr_util.create_directory(client1, child_1)

    attr_util.validate_charmap(
        client1,
        child_1,
        {"casesensitive": True, "normalization": "nfkc", "encoding": "utf8"},
    )

    if not attr_util.rename_directory(client1, child_1, child_1_renamed):
        raise RenameDirectoryError(
            "Failed to rename directory from {} to {}".format(child_1, child_1_renamed)
        )

    log.info("Validating the attributes after renaming the directory")
    attr_util.validate_charmap(
        client1,
        child_1_renamed,
        {"casesensitive": True, "normalization": "nfkc", "encoding": "utf8"},
    )

    if not attr_util.validate_normalization(
        client1,
        fs_name,
        os.path.relpath(child_1_renamed, fuse_mounting_dir).split("/")[0],
        os.path.relpath(child_1_renamed, fuse_mounting_dir).split("/")[-1],
        "NFKC",
    ):
        raise NormalizationValidationError(
            "Normalization validation failed for renamed directory '{}'.".format(
                os.path.relpath(child_1_renamed, fuse_mounting_dir)
            )
        )

    log.info(
        "Validating alternate name for %s",
        os.path.relpath(child_1_renamed, fuse_mounting_dir),
    )
    alter_dict = attr_util.fetch_alternate_name(client1, fs_name, "/")
    if not attr_util.validate_alternate_name(
        alter_dict, os.path.relpath(child_1_renamed, fuse_mounting_dir), "NFKC"
    ):
        log.error("Validation failed for alternate name")
        return 1
    log.info(
        "Validating the attributes after creating the directory followed by rename"
    )

    attr_util.create_directory(client1, child_2)
    attr_util.validate_charmap(
        client1,
        child_2,
        {"casesensitive": True, "normalization": "nfkc", "encoding": "utf8"},
    )
    log.info("Passed: Renaming the directory and validate the attribute")


def test_softlink_attribute_validation():
    """Tests the creation of soft links and validates the attributes of the source directory and the links."""
    global dir_10_child, soft_link_1, soft_link_2

    dir_10_parent = os.path.join(fuse_mounting_dir, "step-10-parent-dir")
    dir_10_child = os.path.join(dir_10_parent, "step-10-child-dir")
    dir_10_link_charmap = os.path.join(fuse_mounting_dir, "step-10-link-charmap")

    attr_util.create_directory(client1, dir_10_parent)
    attr_util.create_directory(client1, dir_10_link_charmap)

    attr_util.set_attributes(
        client1, dir_10_parent, casesensitive=0, normalization="nfc"
    )
    attr_util.create_directory(client1, dir_10_child)

    attr_util.validate_charmap(
        client1,
        dir_10_child,
        {"casesensitive": False, "normalization": "nfc", "encoding": "utf8"},
    )

    log.info(
        "** Creating soft links in the root folder where charmap does not exist **"
    )
    soft_link_1 = os.path.join(fuse_mounting_dir, "link-1")
    attr_util.create_links(client1, dir_10_child, soft_link_1, "soft")

    attr_util.validate_charmap(
        client1,
        soft_link_1,
        {"casesensitive": False, "normalization": "nfc", "encoding": "utf8"},
    )

    log.info(
        "** Creating soft links in different folder where different charmap exist **"
    )
    soft_link_2 = os.path.join(dir_10_link_charmap, "link-2")
    attr_util.set_attributes(
        client1, dir_10_link_charmap, casesensitive=1, normalization="nfkd"
    )

    attr_util.validate_charmap(
        client1,
        dir_10_link_charmap,
        {"casesensitive": True, "normalization": "nfkd", "encoding": "utf8"},
    )

    attr_util.create_links(client1, dir_10_child, soft_link_2, "soft")

    attr_util.validate_charmap(
        client1,
        dir_10_child,
        {"casesensitive": False, "normalization": "nfc", "encoding": "utf8"},
    )

    log.info("Passed: Creation of softlink for dir and validation of the attribute")


def test_softlink_modification_attribute_validation():
    """Tests modifying the attributes of the source directory and validating the soft links."""
    log.info("Making changes to the source dir and validating the soft link 1 & 2")
    for attribute in ["casesensitive", "normalization", "encoding"]:
        attr_util.remove_attributes(client1, dir_10_child, attribute)

    attr_util.validate_charmap(
        client1,
        dir_10_child,
        {"casesensitive": True, "normalization": "nfd", "encoding": "utf8"},
    )

    attr_util.validate_charmap(
        client1,
        soft_link_1,
        {"casesensitive": True, "normalization": "nfd", "encoding": "utf8"},
    )

    attr_util.validate_charmap(
        client1,
        soft_link_2,
        {"casesensitive": True, "normalization": "nfd", "encoding": "utf8"},
    )

    # Validation
    if attr_util.check_ls_case_sensitivity(client1, soft_link_1):
        log.error("Failed: Case sensitivity is True, expected check to fail")
        return 1

    log.info(
        "Passed: check_ls_case_sensitivity failed as expected due to case sensitivity"
    )

    log.info(
        "Making changes to the soft link 2 and validating the source dir & soft link 1"
    )
    attr_util.set_attributes(
        client1, soft_link_2, casesensitive=0, normalization="nfkd"
    )

    attr_util.validate_charmap(
        client1,
        soft_link_2,
        {"casesensitive": False, "normalization": "nfkd", "encoding": "utf8"},
    )

    attr_util.validate_charmap(
        client1,
        dir_10_child,
        {"casesensitive": False, "normalization": "nfkd", "encoding": "utf8"},
    )

    attr_util.validate_charmap(
        client1,
        soft_link_1,
        {"casesensitive": False, "normalization": "nfkd", "encoding": "utf8"},
    )

    log.info("Passed: Making changes to the softlink and validating the attribute")


def test_softlink_file_attribute_validation():
    """Tests the creation of a file in the source directory and validating the soft links."""
    log.info("Creating a file in the source dir and validating the soft link 1 & 2")

    file_name = "step-12-file.txt"
    file_path_soft_link = attr_util.create_file(
        client1, os.path.join(dir_10_child, file_name)
    )

    if not attr_util.check_if_file_exists(client1, file_path_soft_link):
        raise FileNotFound(
            "File does not exist at the soft link path: {}".format(file_path_soft_link)
        )

    if not attr_util.check_if_file_exists(
        client1, os.path.join(soft_link_1, file_name)
    ):
        raise FileNotFound(
            "File does not exist at the soft link 1 path: {}".format(
                os.path.join(soft_link_1, file_name)
            )
        )

    if not attr_util.check_if_file_exists(
        client1, os.path.join(soft_link_2, file_name)
    ):
        raise FileNotFound(
            "File does not exist at the soft link 2 path: {}".format(
                os.path.join(soft_link_2, file_name)
            )
        )

    if not attr_util.check_ls_case_sensitivity(client1, file_path_soft_link):
        raise CaseSensitivityValidationError(
            "Expected case sensitivity check to pass for path: {}".format(
                file_path_soft_link
            )
        )

    if not attr_util.check_ls_case_sensitivity(
        client1, os.path.join(soft_link_1, file_name)
    ):
        raise CaseSensitivityValidationError(
            "Expected case sensitivity check to pass for path: {}".format(
                os.path.join(soft_link_1, file_name)
            )
        )

    if not attr_util.check_ls_case_sensitivity(
        client1, os.path.join(soft_link_2, file_name)
    ):
        raise CaseSensitivityValidationError(
            "Expected case sensitivity check to pass for path: {}".format(
                os.path.join(soft_link_2, file_name)
            )
        )

    log.info(
        "Creating a file in the soft link 2 and validating the source directory & soft link 1"
    )

    file_name_2 = "step-12-file-2.txt"
    file_path_soft_link_2 = attr_util.create_file(
        client1, os.path.join(soft_link_2, file_name_2)
    )

    if not attr_util.check_if_file_exists(client1, file_path_soft_link_2):
        raise FileDoesNotExistError(
            "File does not exist at path: {}".format(file_path_soft_link_2)
        )

    if not attr_util.check_if_file_exists(
        client1, os.path.join(soft_link_1, file_name)
    ):
        raise FileDoesNotExistError(
            "File does not exist at path: {}".format(
                os.path.join(soft_link_1, file_name)
            )
        )

    if not attr_util.check_if_file_exists(
        client1, os.path.join(soft_link_2, file_name)
    ):
        raise FileDoesNotExistError(
            "File does not exist at path: {}".format(
                os.path.join(soft_link_2, file_name)
            )
        )

    if not attr_util.check_ls_case_sensitivity(client1, file_path_soft_link_2):
        raise CaseSensitivityValidationError(
            "Case sensitivity check failed at path: {}".format(file_path_soft_link_2)
        )

    if not attr_util.check_ls_case_sensitivity(
        client1, os.path.join(soft_link_1, file_name)
    ):
        raise CaseSensitivityValidationError(
            "Case sensitivity check failed at path: {}".format(
                os.path.join(soft_link_1, file_name)
            )
        )

    if not attr_util.check_ls_case_sensitivity(
        client1, os.path.join(soft_link_2, file_name)
    ):
        raise CaseSensitivityValidationError(
            "Case sensitivity check failed at path: {}".format(
                os.path.join(soft_link_2, file_name)
            )
        )

    log.info("** Cleanup of Link ** ")
    if not attr_util.delete_links(client1, soft_link_1):
        raise LinkDeletionError("Failed to delete link at: {}".format(soft_link_1))

    if not attr_util.delete_links(client1, soft_link_2):
        raise LinkDeletionError("Failed to delete link at: {}".format(soft_link_2))

    log.info("Passed: Creation of softlink for File and validation of the attribute")


def test_hardlink_file_attribute_validation():
    """Tests the creation of a file in the source directory and validating the hard links."""
    log.info("Creating a file in the source dir and validating the hard link 1")

    dir_13_parent = os.path.join(fuse_mounting_dir, "step-13-parent-dir")
    file_name_1 = "File-1.log"
    dir_13_child_file = os.path.join(dir_13_parent, file_name_1)
    dir_13_link_same_config = os.path.join(
        fuse_mounting_dir, "step_13_link_same_config"
    )
    dir_13_link_diff_config = os.path.join(
        fuse_mounting_dir, "step_13_link_diff_config"
    )

    attr_util.create_directory(client1, dir_13_parent)
    attr_util.create_directory(client1, dir_13_link_same_config)
    attr_util.create_directory(client1, dir_13_link_diff_config)

    attr_util.set_attributes(
        client1, dir_13_parent, casesensitive=0, normalization="nfc"
    )

    attr_util.create_file(client1, dir_13_child_file)

    attr_util.validate_charmap(
        client1,
        dir_13_parent,
        {"casesensitive": False, "normalization": "nfc", "encoding": "utf8"},
    )

    log.info(
        "** Creating hard link in the folder where charmap exist with the same config **"
    )

    attr_util.set_attributes(
        client1, dir_13_link_same_config, casesensitive=0, normalization="nfc"
    )

    attr_util.validate_charmap(
        client1,
        dir_13_link_same_config,
        {"casesensitive": False, "normalization": "nfc", "encoding": "utf8"},
    )

    hard_link_1 = os.path.join(dir_13_link_same_config, file_name_1)
    attr_util.create_links(client1, dir_13_child_file, hard_link_1, "hard")

    # Validation
    if not attr_util.check_ls_case_sensitivity(client1, hard_link_1):
        raise CaseSensitivityValidationError(
            "Case sensitivity check failed for hard link: {}".format(hard_link_1)
        )

    log.info(
        "** Creating hard link in the folder where charmap exist with different config **"
    )

    attr_util.set_attributes(
        client1, dir_13_link_diff_config, casesensitive=1, normalization="nfd"
    )

    attr_util.validate_charmap(
        client1,
        dir_13_link_diff_config,
        {"casesensitive": True, "normalization": "nfd", "encoding": "utf8"},
    )

    hard_link_2 = os.path.join(dir_13_link_diff_config, file_name_1)
    attr_util.create_links(client1, dir_13_child_file, hard_link_2, "hard")

    # Validation
    if attr_util.check_ls_case_sensitivity(client1, hard_link_2):
        log.error("Failed: Expected to fail when case sensitivity is set to True")
        return 1

    log.info("Passed: Failed as expected when case sensitivity is set to True")

    log.info("Passed: Creation of hardlink for File and validation of the attribute")


def test_subvolume_non_default_group_fuse_mount():
    """Tests the creation of a subvolume with a non-default group and validates the attributes using FUSE mount."""
    dir_path = os.path.join(fuse_mounting_dir_14, "step-14")
    attr_util.create_directory(client1, dir_path)

    norm_type = "nfkd"
    attr_util.set_attributes(
        client1, dir_path, casesensitive=0, normalization=norm_type
    )

    attr_util.validate_charmap(
        client1,
        dir_path,
        {"casesensitive": False, "normalization": norm_type, "encoding": "utf8"},
    )

    unicode_name = attr_util.generate_random_unicode_names()[0]
    log.info("Unicode Dir Name: %s", unicode_name)

    child_dir_path = os.path.join(dir_path, unicode_name)
    rel_child_dir = os.path.relpath(child_dir_path, fuse_mounting_dir_14)

    # Removing the first slash for the subvolumes. Need to check
    actual_child_dir_root = os.path.join(
        subvol_path.strip().lstrip("/"), rel_child_dir.split("/")[0]
    )

    attr_util.create_directory(client1, child_dir_path)
    attr_util.validate_charmap(
        client1,
        child_dir_path,
        {"casesensitive": False, "normalization": norm_type, "encoding": "utf8"},
    )

    if not attr_util.validate_normalization(
        client1,
        fs_name,
        actual_child_dir_root,
        unicode_name,
        norm_type.upper(),
        casesensitive=False,
    ):
        raise NormalizationValidationError(
            f"Normalization validation failed for {unicode_name} with norm_type {norm_type.upper()}."
        )

    log.info(
        "Validating alternate name for %s",
        os.path.join(actual_child_dir_root, unicode_name),
    )
    alter_dict = attr_util.fetch_alternate_name(client1, fs_name, "/")
    if not attr_util.validate_alternate_name(
        alter_dict,
        os.path.join(actual_child_dir_root, unicode_name),
        norm_type.upper(),
        casesensitive=False,
    ):
        log.error("Validation failed for alternate name")
        return 1

    if not attr_util.check_ls_case_sensitivity(client1, child_dir_path):
        raise CaseSensitivityValidationError(
            f"Case sensitivity check failed for directory: {child_dir_path}."
        )

    log.info(
        "Passed: Validated subvolume with non-default sub volume group and mount using FUSE"
    )


def test_subvolume_default_group_fuse_mount():
    dir_path = os.path.join(fuse_mounting_dir_15, "step-15")
    attr_util.create_directory(client1, dir_path)

    for attribute in ["casesensitive", "normalization", "encoding"]:
        attr_util.remove_attributes(client1, dir_path, attribute)

    attr_util.validate_charmap(
        client1,
        dir_path,
        {"casesensitive": True, "normalization": "nfd", "encoding": "utf8"},
    )

    unicode_name = attr_util.generate_random_unicode_names()[0]
    log.info("Unicode Dir Name: %s", unicode_name)

    child_dir_path = os.path.join(dir_path, unicode_name)
    rel_child_dir = os.path.relpath(child_dir_path, fuse_mounting_dir_15)

    # Removing the first slash for the subvolumes. Need to check
    actual_child_dir_root = os.path.join(
        subvol_path_2.strip().lstrip("/"), rel_child_dir.split("/")[0]
    )

    attr_util.create_directory(client1, child_dir_path)
    charmap = attr_util.get_charmap(client1, child_dir_path)
    attr_util.validate_charmap(
        client1,
        child_dir_path,
        {"casesensitive": True, "normalization": "nfd", "encoding": "utf8"},
    )

    if not attr_util.validate_normalization(
        client1,
        fs_name,
        actual_child_dir_root,
        unicode_name,
        charmap.get("normalization").upper(),
    ):
        raise NormalizationValidationError(
            f"Normalization validation failed for {unicode_name} in {fs_name}."
        )

    log.info(
        "Validating alternate name for %s",
        os.path.join(actual_child_dir_root, unicode_name),
    )
    alter_dict = attr_util.fetch_alternate_name(client1, fs_name, "/")
    if not attr_util.validate_alternate_name(
        alter_dict,
        os.path.join(actual_child_dir_root, unicode_name),
        charmap.get("normalization").upper(),
    ):
        log.error("Validation failed for alternate name")
        return 1

    if attr_util.check_ls_case_sensitivity(client1, child_dir_path):
        log.error("Failed: Expected to fail if case sensitivity is True")
        return 1

    log.info(
        "Passed: check_ls_case_sensitivity failed as expected when case sensitivity is True"
    )

    log.info("Passed: Validated subvolume in default group and mount using FUSE")

    log.info("*** Case Sensitivity: Functional Workflow completed ***")


def mount_fuse(client1, mount_type):
    global subvol_path
    global subvol_path_2

    log.info("*** Mounting Base File System via FUSE ***")
    fuse_mounting_dir = common_util.setup_cephfs_mount(
        client1,
        fs_name,
        mount_type,
    )
    log.info("Mounting dir: {}".format(fuse_mounting_dir))

    # Usecase 14
    log.info("*** Pre-Req for Usecase 14 {} ***".format(mount_type))
    subvolume_name = "subvolume-1"
    subvolume_group = "subvolumegroup-1"

    fuse_mounting_dir_14 = common_util.setup_cephfs_mount(
        client1,
        fs_name,
        mount_type,
        subvolume_name=subvolume_name,
        subvolume_group=subvolume_group,
    )
    log.info("Mounting dir: {}".format(fuse_mounting_dir_14))
    log.info("Get the path of sub volume")
    subvol_path, _ = client1.exec_command(
        sudo=True,
        cmd="ceph fs subvolume getpath {} {} {}".format(
            fs_name, subvolume_name, subvolume_group
        ),
    )

    # Usecase 15
    log.info("*** Pre-Req for Usecase 15 {} ***".format(mount_type))
    subvolume_name = "subvolume-2"
    fuse_mounting_dir_15 = common_util.setup_cephfs_mount(
        client1,
        fs_name,
        mount_type,
        subvolume_name=subvolume_name,
    )
    log.info("Mounting dir: {}".format(fuse_mounting_dir_15))
    log.info("Get the path of sub volume")
    subvol_path_2, _ = client1.exec_command(
        sudo=True,
        cmd="ceph fs subvolume getpath {} {}".format(fs_name, subvolume_name),
    )
    return fuse_mounting_dir, fuse_mounting_dir_14, fuse_mounting_dir_15


def mount_nfs(client1, mount_type):
    global nfs_params

    log.info("*** Mounting via NFS ***")

    mds = fs_util.get_active_mdss(client1, fs_name)
    active_mds_hostnames = [i.split(".")[1] for i in mds]
    nfs_server_name = active_mds_hostnames[0]

    nfs_params = {
        "nfs_cluster_name": "nfs-1",
        "nfs_server_name": nfs_server_name,
        "binding": "/export_binding1",
    }
    fuse_mounting_dir = common_util.setup_cephfs_mount(
        client1,
        fs_name,
        mount_type,
        nfs_server_name=nfs_params.get("nfs_server_name"),
        nfs_cluster_name=nfs_params.get("nfs_cluster_name"),
        binding=nfs_params.get("binding"),
    )
    log.info("Mounting dir: {}".format(fuse_mounting_dir))
    return fuse_mounting_dir


def mount_smb(client1, mount_type):
    global smb_params

    log.info("*** Mounting via SMB ***")
    smb_params = {
        "smb_cluster_id": "smb-1",
        "smb_shares": ["share-1"],
        "installer": installer,
    }

    fuse_mounting_dir = common_util.setup_cephfs_mount(
        client1,
        fs_name,
        mount_type,
        smb_cluster_id=smb_params.get("smb_cluster_id"),
        smb_shares=smb_params.get("smb_shares"),
        installer=smb_params.get("installer"),
    )
    log.info("Mounting dir: {}".format(fuse_mounting_dir))
    return fuse_mounting_dir


def run(ceph_cluster, **kw):
    test_data = kw.get("test_data")

    global fs_util, attr_util, common_util, client1, installer

    fs_util = FsUtils(ceph_cluster, test_data=test_data)
    attr_util = CephFSAttributeUtilities(ceph_cluster)
    common_util = CephFSCommonUtils(ceph_cluster)
    config = kw.get("config")
    clients = ceph_cluster.get_ceph_objects("client")
    build = config.get("build", config.get("rhbuild"))
    installer = ceph_cluster.get_nodes(role="installer")[0]
    ibm_build = config.get("ibm_build", False)
    fs_util.auth_list(clients)
    log.info("checking Pre-requisites")
    if len(clients) < 1:
        log.error(
            "This test requires minimum 1 client nodes. This has only {} clients".format(
                len(clients)
            )
        )
        return 1
    client1 = clients[0]
    fs_util.prepare_clients([client1], build)

    log.info(
        "\n"
        "\n---------------***************-----------------------------"
        "\n  Pre-Requisite : Create file system, volumes, subvolumes and mount using FUSE, SMB  "
        "\n---------------***************-----------------------------"
    )

    # mount_type_list = ["fuse", "nfs", "smb"]
    mount_type_list = ["fuse"]
    for mount_type in mount_type_list:
        global fuse_mounting_dir, fuse_mounting_dir_14, fuse_mounting_dir_15
        global fs_name

        # Create File System
        fs_name = "case-sensitivity-functional-{}".format(mount_type)
        fs_util.create_fs(client1, fs_name)
        fs_util.wait_for_mds_process(client1, fs_name)

        if mount_type == "fuse":
            fuse_mounting_dir, fuse_mounting_dir_14, fuse_mounting_dir_15 = mount_fuse(
                client1, mount_type
            )
        if mount_type == "nfs":
            fuse_mounting_dir = mount_nfs(client1, mount_type)

        if mount_type == "smb":
            if ibm_build:
                fuse_mounting_dir = mount_smb(client1, mount_type)
            else:
                log.info(
                    "\n---------------***************-----------------------------"
                    "\n  Current build is not IBM build, skipping SMB mount test."
                    "\n---------------***************-----------------------------"
                )
                continue

        try:
            log.info(
                "\n"
                "\n---------------***************---------------------"
                "\nUsecase 1: Set casesensitivity for a new directory "
                "\n---------------***************---------------------"
            )
            log.info("Check Ceph health")
            if common_util.wait_for_healthy_ceph(client1, 300):
                log.error("Cluster health is not OK even after waiting for 300secs")
                return 1

            test_case_sensitivity()

            log.info(
                "\n"
                "\n---------------***************-----------------------------------"
                "\n     Usecase 2: Allow conflicting names in sensitive mode        "
                "\n---------------***************-----------------------------------"
            )
            conflicting_directories()

            log.info(
                "\n"
                "\n---------------***************-----------------------------------"
                "\n       Usecase 3: Check the default value                        "
                "\n---------------***************-----------------------------------"
            )

            check_default_value()

            log.info(
                "\n"
                "\n---------------***************---------------------------------------------"
                "\n   Usecase 4: Validate subdirectory inheritance, special character dir     "
                "\n              and validate normalization with case sensitive True          "
                "\n---------------***************---------------------------------------------"
            )
            sub_directory_inheritance_case_True()

            log.info(
                "\n"
                "\n---------------***************----------------------------------------"
                "\n    Usecase 5: Set empty normalisation and encoding                   "
                "\n               and ensure it fetches default value and gets inherited "
                "\n---------------***************----------------------------------------"
            )

            test_empty_normalization_and_encoding_inheritance()

            log.info(
                "\n"
                "\n---------------***************---------------------"
                "\nUsecase 6: Set normalization for a new directory   "
                "\n---------------***************---------------------"
            )
            log.info("Check Ceph health")
            if common_util.wait_for_healthy_ceph(client1, 300):
                log.error("Cluster health is not OK even after waiting for 300secs")
                return 1

            test_directory_normalization()

            log.info(
                "\n"
                "\n---------------***************---------------------------------------------"
                "\n   Usecase 7: Validate subdirectory inheritance, special character dir     "
                "\n              and validate normalization with case sensitive False         "
                "\n---------------***************---------------------------------------------"
            )
            test_subdirectory_inheritance_special_chars()

            log.info(
                "\n"
                "\n---------------***************---------------------------------------------"
                "\n   Usecase 8: Validate snapshot functionality after setting the attribute  "
                "\n---------------***************---------------------------------------------"
            )
            test_snapshot_functionality_with_attributes()

            log.info(
                "\n"
                "\n---------------***************---------------------------------------------"
                "\n   Usecase 9: Renaming the directory and validate the attribute            "
                "\n---------------***************---------------------------------------------"
            )
            test_directory_rename_attribute_validation()

            log.info(
                "\n"
                "\n---------------***************---------------------------------------------"
                "\n   Usecase 10: Create softlink for dir and validate the attribute          "
                "\n---------------***************---------------------------------------------"
            )
            test_softlink_attribute_validation()

            log.info(
                "\n"
                "\n---------------***************--------------------------------------------------"
                "\nUsecase 11:Create softlink for dir, make modification and validate the attribute"
                "\n---------------***************--------------------------------------------------"
            )
            log.info("Check Ceph health")
            if common_util.wait_for_healthy_ceph(client1, 300):
                log.error("Cluster health is not OK even after waiting for 300secs")
                return 1
            test_softlink_modification_attribute_validation()

            log.info(
                "\n"
                "\n---------------***************---------------------------------------------------"
                "\nUsecase 12:Create softlink for File  and validate the attribute                  "
                "\n---------------***************---------------------------------------------------"
            )
            test_softlink_file_attribute_validation()

            log.info(
                "\n"
                "\n---------------***************---------------------------------------------------"
                "\n   Usecase 13:Create hard link for File and validate the attribute               "
                "\n---------------***************---------------------------------------------------"
            )
            test_hardlink_file_attribute_validation()

            log.info(
                "\n"
                "\n---------------***************------------------------------------------------------"
                "\n  Usecase 14 : Create subvolume with non-default subvolume group and mount using FUSE"
                "\n---------------***************------------------------------------------------------"
            )
            if mount_type == "fuse":
                test_subvolume_non_default_group_fuse_mount()

            log.info(
                "\n"
                "\n---------------***************---------------------------------------"
                "\n  Usecase 15 : Create subvolume in default group and mount using FUSE"
                "\n---------------***************---------------------------------------"
            )
            if mount_type == "fuse":
                test_subvolume_default_group_fuse_mount()

        except CharMapValidationError as e:
            return log_and_fail("Failed to validate charmap attribute", e)

        except SnapshotValidationError as e:
            return log_and_fail(
                "Failed to validate snapshot functionality",
                e,
            )

        except FileOperationError as e:
            return log_and_fail("File operation failed", e)

        except LinkDeletionError as e:
            return log_and_fail("Failed to delete link", e)

        except FsBaseException as e:
            return log_and_fail("Test execution failed", e)

        finally:
            log.info(
                "\n"
                "\n---------------***************----------------------------------------"
                "\n                 Cleanup                                              "
                "\n---------------***************----------------------------------------"
            )

            log.info("Check Ceph health")
            if common_util.wait_for_healthy_ceph(client1, 300):
                log.error("Cluster health is not OK even after waiting for 300secs")
                return 1

            if mount_type == "fuse":
                for mount_dir in [
                    fuse_mounting_dir,
                    fuse_mounting_dir_14,
                    fuse_mounting_dir_15,
                ]:
                    fs_util.client_clean_up(
                        "umount", fuse_clients=[client1], mounting_dir=mount_dir
                    )

            if mount_type == "nfs":
                fs_util.client_clean_up(
                    "umount", kernel_clients=[client1], mounting_dir=fuse_mounting_dir
                )

                fs_util.remove_nfs_export(
                    client1,
                    nfs_params.get("nfs_cluster_name"),
                    nfs_params.get("binding"),
                )

                fs_util.remove_nfs_cluster(client1, nfs_params.get("nfs_cluster_name"))

            elif mount_type == "smb":
                client1.exec_command(
                    sudo=True,
                    cmd=f"umount {fuse_mounting_dir}",
                )

                client1.exec_command(
                    sudo=True,
                    cmd=f"rm -rf {fuse_mounting_dir}",
                )

                remove_smb_share(
                    smb_params.get("installer"),
                    smb_params.get("smb_shares"),
                    smb_params.get("smb_cluster_id"),
                )

                remove_smb_cluster(
                    smb_params.get("installer"), smb_params.get("smb_cluster_id")
                )

            fs_util.remove_fs(client1, fs_name)

    return 0
