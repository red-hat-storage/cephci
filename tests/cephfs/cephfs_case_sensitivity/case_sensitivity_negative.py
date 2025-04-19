import os
import random
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.lib.cephfs_attributes_lib import CephFSAttributeUtilities
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from tests.smb.smb_operations import remove_smb_cluster, remove_smb_share
from utility.log import Log

log = Log(__name__)


def test_fail_set_casesensitivity_with_files():
    """Test case to set casesensitivity on a directory with files"""
    global dir_with_files
    dir_with_files = os.path.join(fuse_mounting_dir, "step-1")
    attr_util.create_directory(client1, dir_with_files)
    list_filenames = ["file.txt", ".tmp"]
    for fnames in list_filenames:
        file_name = attr_util.create_file(client1, os.path.join(dir_with_files, fnames))
        try:
            attr_util.set_attributes(client1, dir_with_files, casesensitive=1)
            log.error("Failed: Attribute was set despite files present")
            return 1
        except CommandFailed:
            log.info("Passed: Failed to set attribute as expected")
            log.info("Deleting the file {}".format(file_name))
            attr_util.delete_file(client1, file_name)
            attr_util.set_attributes(client1, dir_with_files, casesensitive=0)
            attr_util.validate_charmap(
                client1, dir_with_files, {"casesensitive": False}
            )
            log.info(
                "Passed: Attribute set successfully on emptied directory for {}".format(
                    dir_with_files
                )
            )
        finally:
            attr_util.delete_file(client1, os.path.join(dir_with_files, fnames))

    log.info("Passed: Setting casesensitivity on a directory with files")


def test_fail_create_conflicting_names_insensitive_mode():
    """Test case to create conflicting names in insensitive mode"""
    attr_util.create_directory(client1, os.path.join(dir_with_files, "Dir1"))
    try:
        attr_util.create_directory(client1, os.path.join(dir_with_files, "dir1"))
        log.error("Failed: Allowed conflicting directory names")
        return 1
    except CommandFailed:
        log.info("Passed: Conflict prevented as expected")
        attr_util.delete_directory(
            client1, os.path.join(dir_with_files, "Dir1"), recursive=True
        )

    log.info("Passed: Creating conflicting names in insensitive mode")


def test_remove_attributes_and_inherit_defaults():
    """Test case to remove case sensitivity, normalization, and encoding,
    and ensure it fetches the default value and gets inherited."""
    dir_step_3 = os.path.join(fuse_mounting_dir, "step-3")

    for attribute in ["casesensitive", "normalization", "encoding"]:
        log.info("Removing {} and validating the default values".format(attribute))
        attr_util.create_directory(client1, dir_step_3)

        try:
            attr_util.get_charmap(client1, dir_step_3)
            log.error(
                "Charmap expected to fail for new directory when it's parent directory does not have charmap"
            )
        except ValueError:
            log.info(
                "Get Charmap expected to fail when there it's a new folder and "
                "parent directory does not have charmap set"
            )

        attr_util.remove_attributes(client1, dir_step_3, attribute)

        attr_util.validate_charmap(
            client1,
            dir_step_3,
            {"casesensitive": True, "normalization": "nfd", "encoding": "utf8"},
        )

        dir_step_3a = os.path.join(dir_step_3, "step-3a")
        attr_util.create_directory(client1, dir_step_3a)

        attr_util.validate_charmap(
            client1,
            dir_step_3a,
            {"casesensitive": True, "normalization": "nfd", "encoding": "utf8"},
        )

        attr_util.delete_directory(client1, dir_step_3a, recursive=True)
        attr_util.delete_directory(client1, dir_step_3, recursive=True)

    log.info("Passed: Removed casesensitive, normalisation and encoding")


def test_fail_to_set_normalization_with_existing_files():
    """Test case to set normalization on a directory with files"""
    dir_with_files = os.path.join(fuse_mounting_dir, "step-4")
    attr_util.create_directory(client1, dir_with_files)
    list_filenames = ["file.txt", ".tmp"]
    for fnames in list_filenames:
        file_name = attr_util.create_file(client1, os.path.join(dir_with_files, fnames))
        try:
            attr_util.set_attributes(client1, dir_with_files, normalization="nfkc")
            log.error("Failed: Attribute was set despite files present")
            return 1
        except CommandFailed:
            log.info("Passed: Failed to set attribute as expected")
            log.info("Deleting the file {}".format(file_name))
            attr_util.delete_file(client1, file_name)
            attr_util.set_attributes(client1, dir_with_files, normalization="nfkc")
            attr_util.validate_charmap(
                client1, dir_with_files, {"normalization": "nfkc"}
            )
            log.info(
                "Passed: Attribute set successfully on emptied directory for {}".format(
                    dir_with_files
                )
            )
        finally:
            attr_util.delete_file(client1, os.path.join(dir_with_files, fnames))

    log.info("Passed: Setting normalization on a directory with files")


def test_fail_to_create_file_with_unsupported_encoding():
    """Test case to create file with unsupported encoding type"""
    dir_with_files = os.path.join(fuse_mounting_dir, "step-5")
    attr_util.create_directory(client1, dir_with_files)
    fnames = "file.log"
    encoding_value = random.choice(["ASCII", "utf16", "utf32"])
    file_name = attr_util.create_file(client1, os.path.join(dir_with_files, fnames))
    try:
        attr_util.set_attributes(client1, dir_with_files, encoding=encoding_value)
        log.error("Failed: Attribute was set despite files present")
        return 1
    except CommandFailed:
        log.info("Passed: Failed to set attribute as expected")
        log.info("Deleting the file {}".format(file_name))
        attr_util.delete_file(client1, file_name)
        attr_util.set_attributes(client1, dir_with_files, encoding=encoding_value)
        attr_util.validate_charmap(
            client1, dir_with_files, {"encoding": encoding_value}
        )
        try:
            file_name = attr_util.create_file(
                client1, os.path.join(dir_with_files, fnames)
            )
            log.error("Failed: Attribute was set despite unsupported value")
            return 1
        except CommandFailed:
            log.info("Passed: Failed to create file under unsupported encoding type")

    log.info("Passed: Creating file with unsupported encoding type")


def test_set_invalid_normalization_value():
    """Test case to set invalid value for normalization"""
    dir_6 = os.path.join(fuse_mounting_dir, "step-6")
    attr_util.create_directory(client1, dir_6)
    norm_invalid_name = "".join(
        random.choices(string.ascii_letters, k=random.choice([3, 4]))
    )

    attr_util.set_attributes(client1, dir_6, normalization=norm_invalid_name)
    attr_util.validate_charmap(client1, dir_6, {"normalization": norm_invalid_name})
    try:
        attr_util.create_special_character_directories(client1, dir_6)
        log.error("Expected to fail creating directories but successful")
        return 1
    except CommandFailed:
        log.info("Passed: Failed as Expected")

    log.info("Passed: Setting invalid value for normalization")


def test_fail_remove_charmap_with_files():
    """Test case to remove charmap on a directory with files"""
    dir_with_files = os.path.join(fuse_mounting_dir, "step-7")
    attr_util.create_directory(client1, dir_with_files)
    attr_util.set_attributes(
        client1, dir_with_files, casesensitive=0, normalization="nfkc"
    )
    attr_util.validate_charmap(
        client1,
        dir_with_files,
        {"casesensitive": False, "normalization": "nfkc", "encoding": "utf8"},
    )

    supported_attributes = ["casesensitive", "normalization", "encoding", "charmap"]
    for attribute in supported_attributes:
        log.info(
            "Trying to remove attribute {} for the directory {}".format(
                attribute, dir_with_files
            )
        )
        file_name = attr_util.create_file(
            client1, os.path.join(dir_with_files, "file1.log")
        )
        try:
            attr_util.remove_attributes(client1, dir_with_files, attribute)
            log.error("Failed: Attribute was removed despite files present")
            return 1
        except CommandFailed:
            log.info("Passed: Failed to remove attribute as expected")
            log.info("Deleting the file {}".format(file_name))
            attr_util.delete_file(client1, file_name)

    log.info("Passed: Removing charmap on a directory with files")


def test_kernel_mount_fail_set_attributes():
    """Test case to fail setting the attributes on kernel mount"""
    log.info("Mount file system on Kernel Client")
    kernel_mount_dir = "/mnt/cephfs_kernel_{}_1/".format(mounting_dir)
    mon_node_ips = fs_util.get_mon_node_ips()

    fs_util.kernel_mount(
        [client1],
        kernel_mount_dir,
        ",".join(mon_node_ips),
        extra_params=",fs={}".format(fs_name),
    )

    dir_path = os.path.join(kernel_mount_dir, "step-8")
    attr_util.create_directory(client1, dir_path)

    attr_util.set_attributes(client1, dir_path, casesensitive=0, normalization="nfkd")

    attr_util.validate_charmap(
        client1,
        dir_path,
        {"casesensitive": False, "normalization": "nfkd", "encoding": "utf8"},
    )

    try:
        child_dir_path = os.path.join(dir_path, "step-8-child")
        attr_util.create_directory(client1, child_dir_path)
        log.error("Failed: Created child directory under kernel mount")
        return 1
    except CommandFailed:
        log.info("Passed: Failed to create child directory as expected")

    log.info("Cleaning up kernel mount")

    fs_util.client_clean_up(
        "umount",
        kernel_clients=[client1],
        mounting_dir=kernel_mount_dir,
        retain_keyring=True,
    )

    log.info("Passed: Kernel mount should fail setting the attributes")


def test_create_client_users_without_p_flag():
    """Test case to create client users without p flag and validate attributes"""
    log.info("Create directories and set attributes")
    dir_path = os.path.join(fuse_mounting_dir, "step-9")
    attr_util.create_directory(client1, dir_path)

    attr_util.set_attributes(client1, dir_path, casesensitive=0, normalization="nfkd")

    attr_util.validate_charmap(
        client1,
        dir_path,
        {"casesensitive": False, "normalization": "nfkd", "encoding": "utf8"},
    )

    log.info("Create client user without p flag and validate attributes")

    new_client1_name = client1.node.hostname + "_"
    new_mount_dir = "/mnt/{}new/".format(new_client1_name)
    attr_util.create_directory(client1, new_mount_dir, force=True)

    rc1 = fs_util.auth_list(
        [client1],
        path="",
        permission="rw",
        mds=True,
    )
    if rc1 != 0:
        log.error("auth list failed")
        return 1

    fs_util.fuse_mount(
        [client1],
        new_mount_dir,
        new_client_hostname=new_client1_name,
        extra_params=" --client_fs {}".format(fs_name),
    )

    new_dir_path = os.path.join(new_mount_dir, "step-9")

    if not attr_util.validate_charmap(
        client1,
        new_dir_path,
        {"casesensitive": False, "normalization": "nfkd", "encoding": "utf8"},
    ):
        log.error("Failed: Attributes validation failed for client user without p flag")
        return 1

    log.info("Validated attributes for client user without p flag")
    try:
        attr_util.set_attributes(
            client1, new_dir_path, casesensitive=1, normalization="nfc"
        )
        log.error("Failed: Attribute was set when the user doesn't have p flag")
        return 1
    except CommandFailed:
        log.info("Passed: Failed to set attribute as expected for user without p flag")

    log.info("** Cleanup **")
    fs_util.client_clean_up(
        "umount", fuse_clients=[client1], mounting_dir=new_mount_dir
    )
    fs_util.auth_list([client1])

    log.info("Passed: Creating client users without p flag and validating attributes")


def run(ceph_cluster, **kw):
    global fs_util, attr_util, client1, mounting_dir, fuse_mounting_dir, fs_name, installer
    test_data = kw.get("test_data")
    fs_util = FsUtils(ceph_cluster, test_data=test_data)
    common_util = CephFSCommonUtils(ceph_cluster)
    attr_util = CephFSAttributeUtilities(ceph_cluster)
    config = kw.get("config")
    clients = ceph_cluster.get_ceph_objects("client")
    build = config.get("build", config.get("rhbuild"))
    installer = ceph_cluster.get_nodes(role="installer")[0]
    ibm_build = config.get("ibm_build", False)
    fs_util.prepare_clients(clients, build)
    fs_util.auth_list(clients)
    log.info("checking Pre-requisites")
    if len(clients) < 1:
        log.error(
            "This test requires minimum 1 client nodes. This has only {} clients".format(
                len(clients)
            )
        )
        return 1
    client1 = clients[1]

    mount_type_list = ["fuse"]
    for mount_type in mount_type_list:
        try:
            log.info(
                "\n"
                "\n---------------***************-----------------------------"
                "\n  Pre-Requisite : Create file system and mount using {}    ".format(
                    mount_type
                )
                + "\n---------------***************-----------------------------"
            )

            fs_name = "case-sensitivity-negative-fs-1"
            fs_util.create_fs(client1, fs_name)
            fs_util.wait_for_mds_process(client1, fs_name)

            mounting_dir = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(10))
            )

            fuse_mounting_dir = "/mnt/cephfs_fuse_{}/".format(mounting_dir)
            if mount_type == "fuse":
                log.info("*** Mounting via FUSE ***")
                fs_util.fuse_mount(
                    [client1],
                    fuse_mounting_dir,
                    extra_params=" --client_fs {}".format(fs_name),
                )
            elif mount_type == "nfs":
                log.info("*** Mounting via nfs ***")
                mds = fs_util.get_active_mdss(client1, fs_name)
                active_mds_hostnames = [i.split(".")[1] for i in mds]
                nfs_server_name = active_mds_hostnames[0]
                nfs_params = {
                    "nfs_cluster_name": "nfs-1",
                    "nfs_server_name": nfs_server_name,
                    "binding": "/export_binding1",
                }

                log.info("*** Mounting Base File System via NFS ***")
                fuse_mounting_dir = common_util.setup_cephfs_mount(
                    client1,
                    fs_name,
                    mount_type,
                    nfs_server_name=nfs_params.get("nfs_server_name"),
                    nfs_cluster_name=nfs_params.get("nfs_cluster_name"),
                    binding=nfs_params.get("binding"),
                )
            elif mount_type == "smb":
                if ibm_build:
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
                    )
                    log.info("Mounting dir: {}".format(fuse_mounting_dir))
                else:
                    log.info(
                        "\n---------------***************-----------------------------"
                        "\n  Current build is not IBM build, skipping SMB mount test."
                        "\n---------------***************-----------------------------"
                    )
                    continue

            log.info(
                "\n"
                "\n---------------***************-----------------------------------"
                "\nUsecase 1: Fail to set casesensitivity on a directory with files "
                "\n---------------***************-----------------------------------"
            )
            test_fail_set_casesensitivity_with_files()

            log.info(
                "\n"
                "\n---------------***************-----------------------------------"
                "\nUsecase 2: Fail to create conflicting names in insensitive mode  "
                "\n---------------***************-----------------------------------"
            )
            test_fail_create_conflicting_names_insensitive_mode()

            log.info(
                "\n"
                "\n---------------***************----------------------------------------"
                "\n    Usecase 3 : Remove casesensitive, normalisation and encoding      "
                "\n              and ensure it fetches default value and gets inherited  "
                "\n---------------***************----------------------------------------"
            )
            test_remove_attributes_and_inherit_defaults()

            log.info(
                "\n"
                "\n---------------***************-----------------------------------"
                "\nUsecase 4: Fail to set normalization on a directory with files   "
                "\n---------------***************-----------------------------------"
            )
            test_fail_to_set_normalization_with_existing_files()

            log.info(
                "\n"
                "\n---------------***************-----------------------------------"
                "\nUsecase 5: Fail to create file with unsupported encoding type    "
                "\n---------------***************-----------------------------------"
            )
            test_fail_to_create_file_with_unsupported_encoding()

            log.info(
                "\n"
                "\n---------------***************-----------------------------------"
                "\n     Usecase 6: Setting invalid value for normalization          "
                "\n---------------***************-----------------------------------"
            )
            test_set_invalid_normalization_value()

            log.info(
                "\n"
                "\n---------------***************-----------------------------------"
                "\nUsecase 7: Fail to remove charmap on a directory with files      "
                "\n---------------***************-----------------------------------"
            )
            test_fail_remove_charmap_with_files()

            log.info(
                "\n"
                "\n---------------***************-----------------------------------"
                "\n  Usecase 8: Kernel mount should fail setting the attributes     "
                "\n---------------***************-----------------------------------"
            )
            test_kernel_mount_fail_set_attributes()

            log.info(
                "\n"
                "\n---------------***************-----------------------------------"
                "\n  Usecase 9: Create client users without p flag and validate     "
                "\n---------------***************-----------------------------------"
            )
            test_create_client_users_without_p_flag()

            log.info("*** Case Sensitivity: Negative Workflow completed ***")

        except Exception as e:
            log.error("Test execution failed: {}".format(str(e)))
            log.error(traceback.format_exc())
            return 1

        finally:
            log.info(
                "\n"
                "\n---------------***************----------------------------------------"
                "\n                 Cleanup                                              "
                "\n---------------***************----------------------------------------"
            )
            if mount_type == "fuse":
                fs_util.client_clean_up(
                    "umount", fuse_clients=[client1], mounting_dir=fuse_mounting_dir
                )
            elif mount_type == "nfs":
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
                if ibm_build:
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
