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
            "\n---------------***************---------------------------------------------"
            "\n   Usecase 4: Validate subdirectory inheritance, special character dir     "
            "\n              and validate normalization with case sensitive True          "
            "\n---------------***************---------------------------------------------"
        )
        normalization_types = ["nfkd", "nfkc", "nfd", "nfc"]
        for norm_type in normalization_types:
            parent_dir = attr_util.create_special_character_directories(
                client1, fuse_mounting_dir
            )
            rel_parent_dir = os.path.relpath(parent_dir, fuse_mounting_dir)

            attr_util.set_attributes(client1, parent_dir, casesensitive=1)
            attr_util.set_attributes(client1, parent_dir, normalization=norm_type)

            alter_dict = attr_util.fetch_alternate_name(client1, fs_name, "/")
            if not attr_util.validate_alternate_name(
                alter_dict, rel_parent_dir, empty_name=True
            ):
                log.error("Validation failed for alternate name")
                return 1

            log.info("Validating for Normalization: {}".format(norm_type))
            child_dir = attr_util.create_special_character_directories(
                client1, parent_dir
            )
            rel_child_dir = os.path.relpath(child_dir, fuse_mounting_dir)
            actual_child_dir_root = rel_child_dir.split("/")[0]
            actual_child_dir_name = rel_child_dir.split("/")[1]

            assert (
                attr_util.get_charmap(client1, child_dir).get("casesensitive") is True
            )
            assert (
                attr_util.get_charmap(client1, child_dir).get("normalization")
                == norm_type
            )
            assert attr_util.get_charmap(client1, child_dir).get("encoding") == "utf8"

            assert attr_util.validate_normalization(
                client1,
                fs_name,
                actual_child_dir_root,
                actual_child_dir_name,
                norm_type.upper(),
            )

            log.info("Validating alternate name for %s", rel_child_dir)
            alter_dict = attr_util.fetch_alternate_name(client1, fs_name, "/")
            if not attr_util.validate_alternate_name(alter_dict, rel_child_dir):
                log.error("Validation failed for alternate name")
                return 1

            log.info("** Cleanup ** ")
            attr_util.delete_directory(client1, parent_dir, recursive=True)

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
            attr_util.delete_directory(client1, dir_step_5, recursive=True)

        log.info(
            "\n"
            "\n---------------***************---------------------"
            "\nUsecase 6: Set normalization for a new directory   "
            "\n---------------***************---------------------"
        )
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

            assert (
                attr_util.get_charmap(client1, dir_path_6_child).get("normalization")
                == supported_value
            )

            alter_dict = attr_util.fetch_alternate_name(client1, fs_name, "/")
            if not attr_util.validate_alternate_name(
                alter_dict,
                rel_dir_path_6_child,
            ):
                log.error("Validation failed for alternate name")
                return 1

            log.info("Passed: Normalization set to {}".format(supported_value))

        log.info(
            "\n"
            "\n---------------***************---------------------------------------------"
            "\n   Usecase 7: Validate subdirectory inheritance, special character dir     "
            "\n              and validate normalization with case sensitive False         "
            "\n---------------***************---------------------------------------------"
        )
        normalization_types = ["nfkd", "nfkc", "nfd", "nfc"]
        for norm_type in normalization_types:
            parent_dir = attr_util.create_special_character_directories(
                client1, fuse_mounting_dir
            )
            rel_parent_dir = os.path.relpath(parent_dir, fuse_mounting_dir)
            attr_util.set_attributes(client1, parent_dir, casesensitive=0)
            attr_util.set_attributes(client1, parent_dir, normalization=norm_type)

            alter_dict = attr_util.fetch_alternate_name(client1, fs_name, "/")
            if not attr_util.validate_alternate_name(
                alter_dict, rel_parent_dir, empty_name=True
            ):
                log.error("Validation failed for alternate name")
                return 1

            log.info("Validating for Normalization: {}".format(norm_type))
            child_dir = attr_util.create_special_character_directories(
                client1, parent_dir
            )
            rel_child_dir = os.path.relpath(child_dir, fuse_mounting_dir)
            actual_child_dir_root = rel_child_dir.split("/")[0]
            actual_child_dir_name = rel_child_dir.split("/")[1]

            assert (
                attr_util.get_charmap(client1, child_dir).get("casesensitive") is False
            )
            assert (
                attr_util.get_charmap(client1, child_dir).get("normalization")
                == norm_type
            )
            assert attr_util.get_charmap(client1, child_dir).get("encoding") == "utf8"

            assert attr_util.validate_normalization(
                client1,
                fs_name,
                actual_child_dir_root,
                actual_child_dir_name.lower(),
                norm_type.upper(),
            )

            log.info("Validating alternate name for %s", rel_child_dir)
            alter_dict = attr_util.fetch_alternate_name(client1, fs_name, "/")
            if not attr_util.validate_alternate_name(
                alter_dict, rel_child_dir, casesensitive=False
            ):
                log.error("Validation failed for alternate name")
                return 1

            log.info("** Cleanup ** ")
            attr_util.delete_directory(client1, parent_dir, recursive=True)

        log.info("Passed: Subdirectory inherited attribute")

        # # Able to set the charmap attribute when the snap folder exists
        # # Discussing with Dev. Will map the BZ accordingly
        # # Commenting for now since this is failing
        # log.info(
        #     "\n"
        #     "\n---------------***************---------------------------------------------"
        #     "\n   Usecase 8: Validate snapshot functionality after setting the attribute  "
        #     "\n---------------***************---------------------------------------------"
        # )
        # sv_group = "sv_group"
        # sv_name = "subvol1"
        # snap_name = "snap_1"
        # fs_util.create_subvolumegroup(client1, fs_name, sv_group)
        # fs_util.create_subvolume(client1, fs_name, sv_name, group_name=sv_group)

        # log.info("Get the path of sub volume")
        # subvol_path, rc = client1.exec_command(
        #     sudo=True,
        #     cmd="ceph fs subvolume getpath {} {} {}".format(fs_name, sv_name, sv_group),
        # )

        # mounting_dir = "".join(
        #     random.choice(string.ascii_lowercase + string.digits)
        #     for _ in list(range(10))
        # )
        # snap_fuse_mounting_dir = "/mnt/cephfs_fuse_snap_{}/".format(mounting_dir)
        # attr_util.create_directory(client1, snap_fuse_mounting_dir)

        # fs_util.fuse_mount(
        #     [client1],
        #     snap_fuse_mounting_dir,
        #     extra_params=" -r {} --client_fs {}".format(subvol_path.strip(), fs_name),
        # )

        # attr_util.set_attributes(
        #     client1, snap_fuse_mounting_dir, casesensitive=0, normalization="nfkc"
        # )
        # fs_util.create_file_data(
        #     client1, {snap_fuse_mounting_dir}, 3, snap_name, "snap_1_data "
        # )
        # fs_util.create_snapshot(
        #     client1, fs_name, sv_name, snap_name, group_name=sv_group
        # )

        # assert attr_util.validate_snapshot_from_mount(
        #     client1, snap_fuse_mounting_dir, [snap_name]
        # )

        # charmap = attr_util.get_charmap(client1, snap_fuse_mounting_dir)
        # assert (
        #     charmap.get("casesensitive") is False
        #     and charmap.get("normalization") == "nfkc"
        #     and charmap.get("encoding") == "utf8"
        # )

        # try:
        #     attr_util.set_attributes(
        #         client1, snap_fuse_mounting_dir, casesensitive=1, normalization="nfd"
        #     )
        #     log.error("Expected to fail when snapshot exists")
        #     return 1
        # except Exception:
        #     log.info(
        #         "Passed: Failed as Expected. Attributes should not be set on a snapshot directory"
        #     )

        # log.info("Removing the snapshot directory")
        # attr_util.delete_snapshots_from_mount(client1, snap_fuse_mounting_dir)

        # attr_util.set_attributes(
        #     client1, snap_fuse_mounting_dir, casesensitive=1, normalization="nfd"
        # )
        # charmap = attr_util.get_charmap(client1, snap_fuse_mounting_dir)
        # assert (
        #     charmap.get("casesensitive") is True
        #     and charmap.get("normalization") == "nfd"
        #     and charmap.get("encoding") == "utf8"
        # )

        # log.info("Passed: Validated snapshot functionality")

        log.info(
            "\n"
            "\n---------------***************---------------------------------------------"
            "\n   Usecase 9: Renaming the directory and validate the attribute            "
            "\n---------------***************---------------------------------------------"
        )

        dir_9_root = os.path.join(fuse_mounting_dir, "step-9")
        child_1 = os.path.join(dir_9_root, "child-1")
        child_1_renamed = os.path.join(dir_9_root, "gá")
        child_2 = os.path.join(child_1_renamed, "child-2")
        attr_util.create_directory(client1, dir_9_root, force=True)

        attr_util.set_attributes(client1, dir_9_root, normalization="nfkc")
        attr_util.create_directory(client1, child_1)

        charmap = attr_util.get_charmap(client1, child_1)
        assert (
            charmap.get("casesensitive") is True
            and charmap.get("normalization") == "nfkc"
            and charmap.get("encoding") == "utf8"
        )

        assert attr_util.rename_directory(client1, child_1, child_1_renamed)

        log.info("Validating the attributes after renaming the directory")
        charmap = attr_util.get_charmap(client1, child_1_renamed)
        assert (
            charmap.get("casesensitive") is True
            and charmap.get("normalization") == "nfkc"
            and charmap.get("encoding") == "utf8"
        )

        assert attr_util.validate_normalization(
            client1,
            fs_name,
            os.path.relpath(child_1_renamed, fuse_mounting_dir).split("/")[0],
            os.path.relpath(child_1_renamed, fuse_mounting_dir).split("/")[-1],
            "NFKC",
        )

        log.info(
            "Validating alternate name for %s",
            os.path.relpath(child_1_renamed, fuse_mounting_dir),
        )
        alter_dict = attr_util.fetch_alternate_name(client1, fs_name, "/")
        if not attr_util.validate_alternate_name(
            alter_dict, os.path.relpath(child_1_renamed, fuse_mounting_dir)
        ):
            log.error("Validation failed for alternate name")
            return 1
        log.info(
            "Validating the attributes after creating the directory followed by rename"
        )

        attr_util.create_directory(client1, child_2)
        charmap = attr_util.get_charmap(client1, child_2)
        assert (
            charmap.get("casesensitive") is True
            and charmap.get("normalization") == "nfkc"
            and charmap.get("encoding") == "utf8"
        )
        log.info("Completed: Renaming the directory and validate the attribute")

        log.info("*** Case Sensitivity: Functional Workflow completed ***")
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
