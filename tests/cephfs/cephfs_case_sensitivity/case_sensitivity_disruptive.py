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

        fs_name = "case-sensitivity-disruptive-1"
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

        parent_dir = os.path.join(fuse_mounting_dir, "parent_dir")
        child_dir = os.path.join(parent_dir, "child_dir")
        rel_child_dir = os.path.relpath(child_dir, fuse_mounting_dir)
        attr_util.create_directory(client1, parent_dir)
        attr_util.set_attributes(client1, parent_dir, casesensitive=0)
        attr_util.set_attributes(client1, parent_dir, normalization="nfkd")

        attr_util.create_directory(client1, child_dir)
        assert attr_util.get_charmap(client1, child_dir).get("casesensitive") is False
        assert attr_util.get_charmap(client1, child_dir).get("normalization") == "nfkd"
        assert attr_util.get_charmap(client1, child_dir).get("encoding") == "utf8"

        log.info("Validating alternate name for %s", rel_child_dir)
        alter_dict = attr_util.fetch_alternate_name(client1, fs_name, "/")
        if not attr_util.validate_alternate_name(alter_dict, rel_child_dir):
            log.error("Validation failed for alternate name")

        log.info(
            "\n"
            "\n---------------***************-----------------------------------"
            "\n    Usecase 1: Persistence after MDS reboot                      "
            "\n---------------***************-----------------------------------"
        )

        num_of_osds = config.get("num_of_osds")
        fs_util.runio_reboot_active_mds_nodes(
            fs_util,
            ceph_cluster,
            fs_name,
            client1,
            num_of_osds,
            build,
            child_dir,
        )

        assert attr_util.get_charmap(client1, child_dir).get("casesensitive") is False
        assert attr_util.get_charmap(client1, child_dir).get("normalization") == "nfkd"
        assert attr_util.get_charmap(client1, child_dir).get("encoding") == "utf8"

        log.info("Validating alternate name for %s", rel_child_dir)
        alter_dict = attr_util.fetch_alternate_name(client1, fs_name, "/")
        if not attr_util.validate_alternate_name(alter_dict, rel_child_dir):
            log.error("Validation failed for alternate name")

        log.info("Passed: Attribute persisted after MDS reboot")

        log.info(
            "\n"
            "\n---------------***************-----------------------------------"
            "\n    Usecase 2: Persistence after remount                         "
            "\n---------------***************-----------------------------------"
        )

        log.info("Unmounting fuse client:")
        cmd = "fusermount -u {} -z".format(fuse_mounting_dir)
        client1.exec_command(sudo=True, cmd=cmd)

        fs_util.fuse_mount(
            [client1], fuse_mounting_dir, extra_params=" --client_fs {}".format(fs_name)
        )

        assert attr_util.get_charmap(client1, child_dir).get("casesensitive") is False
        assert attr_util.get_charmap(client1, child_dir).get("normalization") == "nfkd"
        assert attr_util.get_charmap(client1, child_dir).get("encoding") == "utf8"

        log.info("Passed: Attribute persisted after remount")

    except Exception as e:
        log.error("Test execution failed: {}".format(str(e)))
        log.error(traceback.format_exc())

    finally:
        fs_util.client_clean_up(
            "umount", fuse_clients=[client1], mounting_dir=fuse_mounting_dir
        )
        fs_util.remove_fs(client1, fs_name)
