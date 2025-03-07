import os
import random
import string
import traceback

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
            log.info(
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

        fs_name = "case-sensitivity-functional-1"
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
            "\n---------------***************---------------------"
            "\nUsecase 1: Set casesensitivity for a new directory "
            "\n---------------***************---------------------"
        )
        dir_path = os.path.join(fuse_mounting_dir, "step-1")

        attr_util.create_directory(client1, dir_path)
        attr_util.set_attributes(client1, dir_path, casesensitive=1)
        assert attr_util.get_charmap(client1, dir_path).get("casesensitive") is True
        log.info("Passed: casesensitivity set to sensitive")

        attr_util.set_attributes(client1, dir_path, casesensitive=0)
        assert attr_util.get_charmap(client1, dir_path).get("casesensitive") is False
        log.info("Passed: casesensitivity set to insensitive")

        log.info(
            "\n"
            "\n---------------***************-----------------------------------"
            "\n     Usecase 2: Allow conflicting names in sensitive mode        "
            "\n---------------***************-----------------------------------"
        )
        attr_util.set_attributes(client1, dir_path, casesensitive=1)
        attr_util.create_directory(client1, os.path.join(dir_path, "Dir1"))
        attr_util.create_directory(client1, os.path.join(dir_path, "dir1"))
        log.info("Passed: Conflicting directories created successfully")

        log.info(
            "\n"
            "\n---------------***************-----------------------------------"
            "\n       Usecase 3: Check the default value                        "
            "\n---------------***************-----------------------------------"
        )
        # Note: Default value cannot be checked until we set value for atleast one of the attirbute
        default_dir = os.path.join(fuse_mounting_dir, "step-3")
        attr_util.create_directory(client1, default_dir)
        attr_util.set_attributes(client1, default_dir, casesensitive=1)
        assert attr_util.get_charmap(client1, default_dir).get("normalization") == "nfd"
        assert attr_util.get_charmap(client1, default_dir).get("encoding") == "utf8"
        attr_util.delete_directory(client1, default_dir, recursive=True)

        attr_util.create_directory(client1, default_dir)
        attr_util.set_attributes(client1, default_dir, normalization="nfc")
        assert attr_util.get_charmap(client1, default_dir).get("casesensitive") is True

        log.info("Passed: Default values are validated")

        log.info(
            "\n"
            "\n---------------***************-----------------------------------"
            "\n    Usecase 4: Validate subdirectory inheritance                 "
            "\n---------------***************-----------------------------------"
        )
        parent_dir = os.path.join(fuse_mounting_dir, "step-4-parent_dir")
        rel_parent_dir = os.path.relpath(parent_dir, fuse_mounting_dir)
        child_dir = os.path.join(parent_dir, "step-4-child_dir")
        rel_child_dir = os.path.relpath(child_dir, fuse_mounting_dir)
        attr_util.create_directory(client1, parent_dir)
        attr_util.set_attributes(client1, parent_dir, casesensitive=0)
        attr_util.set_attributes(client1, parent_dir, normalization="nfkd")

        alter_dict = attr_util.fetch_alternate_name(client1, fs_name, "/")
        if not attr_util.validate_alternate_name(
            alter_dict, rel_parent_dir, empty_name=True
        ):
            log.error("Validation failed for alternate name")

        attr_util.create_directory(client1, child_dir)
        assert attr_util.get_charmap(client1, child_dir).get("casesensitive") is False
        assert attr_util.get_charmap(client1, child_dir).get("normalization") == "nfkd"
        assert attr_util.get_charmap(client1, child_dir).get("encoding") == "utf8"

        log.info("Validating alternate name for %s", rel_child_dir)
        alter_dict = attr_util.fetch_alternate_name(client1, fs_name, "/")
        if not attr_util.validate_alternate_name(alter_dict, rel_child_dir):
            log.error("Validation failed for alternate name")

        log.info("Passed: Subdirectory inherited attribute")

        log.info(
            "\n"
            "\n---------------***************----------------------------------------"
            "\n    Usecase 5: Set empty normalisation and encoding                   "
            "\n               and ensure it fetches default value and gets inherited "
            "\n---------------***************----------------------------------------"
        )
        dir_step_5 = os.path.join(fuse_mounting_dir, "step-5")

        for attribute in ["normalization", "encoding"]:
            log.info(
                "Setting empty {} and validating the default values".format(attribute)
            )
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

            assert (
                attr_util.get_charmap(client1, dir_step_5).get("casesensitive") is True
            )
            assert (
                attr_util.get_charmap(client1, dir_step_5).get("normalization") == "nfd"
            )
            assert attr_util.get_charmap(client1, dir_step_5).get("encoding") == "utf8"

            dir_step_5a = os.path.join(dir_step_5, "step-5a")
            attr_util.create_directory(client1, dir_step_5a)

            assert (
                attr_util.get_charmap(client1, dir_step_5a).get("casesensitive") is True
            )
            assert (
                attr_util.get_charmap(client1, dir_step_5a).get("normalization")
                == "nfd"
            )
            assert attr_util.get_charmap(client1, dir_step_5a).get("encoding") == "utf8"

            attr_util.delete_directory(client1, dir_step_5a, recursive=True)

    except Exception as e:
        log.error("Test execution failed: {}".format(str(e)))
        log.error(traceback.format_exc())

    finally:
        fs_util.client_clean_up(
            "umount", fuse_clients=[client1], mounting_dir=fuse_mounting_dir
        )
        fs_util.remove_fs(client1, fs_name)
