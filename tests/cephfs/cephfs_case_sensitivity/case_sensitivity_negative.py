import os
import random
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.lib.cephfs_attributes_lib import CephFSAttributeUtilities
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    try:
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        attr_util = CephFSAttributeUtilities(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
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
        client1 = clients[0]

        log.info(
            "\n"
            "\n---------------***************-----------------------------"
            "\n  Pre-Requisite : Create file system and mount using FUSE  "
            "\n---------------***************-----------------------------"
        )

        fs_name = "case-sensitivity-negative-fs-1"
        fs_util.create_fs(client1, fs_name)
        fs_util.wait_for_mds_process(client1, fs_name)

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        fuse_mounting_dir = "/mnt/cephfs_fuse_{}/".format(mounting_dir)
        fs_util.fuse_mount(
            [client1], fuse_mounting_dir, extra_params=" --client_fs {}".format(fs_name)
        )

        log.info(
            "\n"
            "\n---------------***************-----------------------------------"
            "\nUsecase 1: Fail to set casesensitivity on a directory with files "
            "\n---------------***************-----------------------------------"
        )
        dir_with_files = os.path.join(fuse_mounting_dir, "step-1")
        attr_util.create_directory(client1, dir_with_files)
        list_filenames = ["file.txt", ".tmp"]
        for fnames in list_filenames:
            file_name = attr_util.create_file(
                client1, os.path.join(dir_with_files, fnames)
            )
            try:
                attr_util.set_attributes(client1, dir_with_files, casesensitive=1)
                log.error("Failed: Attribute was set despite files present")
                return 1
            except CommandFailed:
                log.info("Passed: Failed to set attribute as expected")
                log.info("Deleting the file {}".format(file_name))
                attr_util.delete_file(client1, file_name)
                attr_util.set_attributes(client1, dir_with_files, casesensitive=0)
                assert (
                    attr_util.get_charmap(client1, dir_with_files).get("casesensitive")
                    is False
                )
                log.info(
                    "Passed: Attribute set successfully on emptied directory for {}".format(
                        dir_with_files
                    )
                )
            finally:
                attr_util.delete_file(client1, os.path.join(dir_with_files, fnames))

        log.info("Passed: Setting casesensitivity on a directory with files")

        log.info(
            "\n"
            "\n---------------***************-----------------------------------"
            "\nUsecase 2: Fail to create conflicting names in insensitive mode  "
            "\n---------------***************-----------------------------------"
        )
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

        log.info(
            "\n"
            "\n---------------***************----------------------------------------"
            "\n    Usecase 3 : Remove casesensitive, normalisation and encoding      "
            "\n              and ensure it fetches default value and gets inherited  "
            "\n---------------***************----------------------------------------"
        )
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

            assert (
                attr_util.get_charmap(client1, dir_step_3).get("casesensitive") is True
            )
            assert (
                attr_util.get_charmap(client1, dir_step_3).get("normalization") == "nfd"
            )
            assert attr_util.get_charmap(client1, dir_step_3).get("encoding") == "utf8"

            dir_step_3a = os.path.join(dir_step_3, "step-3a")
            attr_util.create_directory(client1, dir_step_3a)

            assert (
                attr_util.get_charmap(client1, dir_step_3a).get("casesensitive") is True
            )
            assert (
                attr_util.get_charmap(client1, dir_step_3a).get("normalization")
                == "nfd"
            )
            assert attr_util.get_charmap(client1, dir_step_3a).get("encoding") == "utf8"

            attr_util.delete_directory(client1, dir_step_3a, recursive=True)
            attr_util.delete_directory(client1, dir_step_3, recursive=True)

        log.info("Passed: Removed casesensitive, normalisation and encoding")

        log.info(
            "\n"
            "\n---------------***************-----------------------------------"
            "\nUsecase 4: Fail to set normalization on a directory with files   "
            "\n---------------***************-----------------------------------"
        )
        dir_with_files = os.path.join(fuse_mounting_dir, "step-4")
        attr_util.create_directory(client1, dir_with_files)
        list_filenames = ["file.txt", ".tmp"]
        for fnames in list_filenames:
            file_name = attr_util.create_file(
                client1, os.path.join(dir_with_files, fnames)
            )
            try:
                attr_util.set_attributes(client1, dir_with_files, normalization="nfkc")
                log.error("Failed: Attribute was set despite files present")
                return 1
            except CommandFailed:
                log.info("Passed: Failed to set attribute as expected")
                log.info("Deleting the file {}".format(file_name))
                attr_util.delete_file(client1, file_name)
                attr_util.set_attributes(client1, dir_with_files, normalization="nfkc")
                assert (
                    attr_util.get_charmap(client1, dir_with_files).get("normalization")
                    == "nfkc"
                )
                log.info(
                    "Passed: Attribute set successfully on emptied directory for {}".format(
                        dir_with_files
                    )
                )
            finally:
                attr_util.delete_file(client1, os.path.join(dir_with_files, fnames))

        log.info("Passed: Setting normalization on a directory with files")

        log.info(
            "\n"
            "\n---------------***************-----------------------------------"
            "\nUsecase 5: Fail to create file with unsupported encoding type    "
            "\n---------------***************-----------------------------------"
        )
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
            assert (
                attr_util.get_charmap(client1, dir_with_files).get("encoding")
                == encoding_value
            )
            try:
                file_name = attr_util.create_file(
                    client1, os.path.join(dir_with_files, fnames)
                )
                log.error("Failed: Attribute was set despite unsupported value")
                return 1
            except CommandFailed:
                log.info(
                    "Passed: Failed to create file under unsupported encoding type"
                )

        log.info("Passed: Creating file with unsupported encoding type")

        log.info(
            "\n"
            "\n---------------***************-----------------------------------"
            "\n     Usecase 6: Setting invalid value for normalization          "
            "\n---------------***************-----------------------------------"
        )
        dir_6 = os.path.join(fuse_mounting_dir, "step-6")
        attr_util.create_directory(client1, dir_6)
        norm_invalid_name = "".join(
            random.choices(string.ascii_letters, k=random.choice([3, 4]))
        )

        attr_util.set_attributes(client1, dir_6, normalization=norm_invalid_name)
        assert (
            attr_util.get_charmap(client1, dir_6).get("normalization")
            == norm_invalid_name
        )
        try:
            attr_util.create_special_character_directories(client1, dir_6)
            log.error("Expected to fail creating directories but successful")
            return 1
        except CommandFailed:
            log.info("Passed: Failed as Expected")

        log.info("Passed: Setting invalid value for normalization")

        log.info(
            "\n"
            "\n---------------***************-----------------------------------"
            "\nUsecase 7: Fail to remove charmap on a directory with files      "
            "\n---------------***************-----------------------------------"
        )
        dir_with_files = os.path.join(fuse_mounting_dir, "step-7")
        attr_util.create_directory(client1, dir_with_files)
        attr_util.set_attributes(
            client1, dir_with_files, casesensitive=0, normalization="nfkc"
        )
        assert (
            attr_util.get_charmap(client1, dir_with_files).get("casesensitive") is False
        )
        assert (
            attr_util.get_charmap(client1, dir_with_files).get("normalization")
            == "nfkc"
        )
        assert attr_util.get_charmap(client1, dir_with_files).get("encoding") == "utf8"

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

        log.info("*** Case Sensitivity: Negative Workflow completed ***")
        return 0

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
        fs_util.client_clean_up(
            "umount", fuse_clients=[client1], mounting_dir=fuse_mounting_dir
        )
        fs_util.remove_fs(client1, fs_name)
