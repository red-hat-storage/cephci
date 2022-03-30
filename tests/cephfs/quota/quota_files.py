import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test Cases Covered :
    CEPH-83573399	Test to validate the quota.max_files
                    Create a FS and create 10 directories and mount them on kernel client and fuse client(5 mounts each)
                    Set max files quota to a number(say 50) and add up to that number of files to that directory and
                    verify if the set quota limit is working fine. Similarly set different limit
                    on different directories and verify quota.

    Pre-requisites :
    1. We need atleast one client node to execute this test case
    2. create fs volume create cephfs if the volume is not there
    3. ceph fs subvolumegroup create <vol_name> <group_name> --pool_layout <data_pool_name>
        Ex : ceph fs subvolumegroup create cephfs subvolgroup_clone_attr_vol_1
    4. ceph fs subvolume create <vol_name> <subvol_name> [--size <size_in_bytes>] [--group_name <subvol_group_name>]
       [--pool_layout <data_pool_name>] [--uid <uid>] [--gid <gid>] [--mode <octal_mode>]  [--namespace-isolated]
       Ex: ceph fs subvolume create cephfs subvol_1 --size 5368706371 --group_name subvolgroup_1
    5. ceph fs subvolume create <vol_name> <subvol_name> [--size <size_in_bytes>] [--group_name <subvol_group_name>]
       [--pool_layout <data_pool_name>] [--uid <uid>] [--gid <gid>] [--mode <octal_mode>]  [--namespace-isolated]
       Ex: ceph fs subvolume create cephfs subvol_2 --size 5368706371 --group_name subvolgroup_1

    Test Case Flow:
    1. Mount the subvolume_1 on the client using fuse
    2. Mount the subvolume_2 on the client using kernel
    3. set file attribute 10 on both mount points
    4. Create 11 files and check it fails at 11 iteration
    5. Perform same on kernel mount
    6. Create a directory inside fuse mount and set file attribute and verify
    7. Create a directory inside kernel mount and set file attribute and verify
    """
    try:
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        log.info("checking Pre-requisites")
        if not clients:
            log.info(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1
        default_fs = "cephfs"
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        client1 = clients[0]
        fs_details = fs_util.get_fs_info(client1)
        if not fs_details:
            fs_util.create_fs(client1, "cephfs")

        log.info("Cheking the file quota on root directory")
        root_folder_fuse_mount = f"/mnt/cephfs_fuse{mounting_dir}_2/"
        fs_util.fuse_mount([clients[0]], root_folder_fuse_mount)

        clients[0].exec_command(
            sudo=True,
            cmd=f"rm -rf {root_folder_fuse_mount}*;mkdir {root_folder_fuse_mount}test_fuse;",
        )
        fs_util.set_quota_attrs(
            clients[0], 20, 10000000, f"{root_folder_fuse_mount}test_fuse"
        )
        quota_attrs = fs_util.get_quota_attrs(
            clients[0], f"{root_folder_fuse_mount}test_fuse"
        )
        fs_util.file_quota_test(
            clients[0], f"{root_folder_fuse_mount}test_fuse", quota_attrs
        )
        log.info("Setting Back the file quota to 0 on root directory ")
        fs_util.set_quota_attrs(
            clients[0], "0", 10000000, f"{root_folder_fuse_mount}test_fuse"
        )

        root_folder_kernel_mount = f"/mnt/cephfs_kernel{mounting_dir}_2/"
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount(
            [clients[0]], root_folder_kernel_mount, ",".join(mon_node_ips)
        )
        clients[0].exec_command(
            sudo=True,
            cmd=f"rm -rf {root_folder_kernel_mount}*;mkdir {root_folder_kernel_mount}test_kernel",
        )
        fs_util.set_quota_attrs(
            clients[0], 20, 10000000, f"{root_folder_kernel_mount}test_kernel"
        )
        quota_attrs = fs_util.get_quota_attrs(
            clients[0], f"{root_folder_kernel_mount}test_kernel"
        )
        fs_util.file_quota_test(
            clients[0], f"{root_folder_kernel_mount}test_kernel", quota_attrs
        )
        log.info("Setting Back the file quota to 0 on root directory ")
        fs_util.set_quota_attrs(
            clients[0], "0", 10000000, f"{root_folder_kernel_mount}test_kernel"
        )

        subvolumegroup_list = [
            {"vol_name": default_fs, "group_name": "subvolgroup_quota_file_limit_1"},
        ]
        for subvolumegroup in subvolumegroup_list:
            fs_util.create_subvolumegroup(client1, **subvolumegroup)
        subvolume_list = [
            {
                "vol_name": default_fs,
                "subvol_name": "subvol_file_fuse",
                "group_name": "subvolgroup_quota_file_limit_1",
                "size": "5368706371",
            },
            {
                "vol_name": default_fs,
                "subvol_name": "subvol_file_kernel",
                "group_name": "subvolgroup_quota_file_limit_1",
                "size": "5368706371",
            },
        ]
        for subvolume in subvolume_list:
            fs_util.create_subvolume(clients[0], **subvolume)

        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        log.info("Get the path of sub volume")
        subvol_path, rc = clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} subvol_file_kernel subvolgroup_quota_file_limit_1",
        )
        fs_util.kernel_mount(
            [clients[0]],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            sub_dir=f"{subvol_path.strip()}",
        )

        subvol_path, rc = clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} subvol_file_fuse subvolgroup_quota_file_limit_1",
        )
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"

        fs_util.fuse_mount(
            [clients[0]],
            fuse_mounting_dir_1,
            extra_params=f" -r {subvol_path.strip()}",
        )

        fs_util.set_quota_attrs(clients[0], 200, 10000000, kernel_mounting_dir_1)
        quota_attrs = fs_util.get_quota_attrs(clients[0], kernel_mounting_dir_1)
        fs_util.file_quota_test(clients[0], kernel_mounting_dir_1, quota_attrs)

        fs_util.set_quota_attrs(clients[0], 100, 10000000, fuse_mounting_dir_1)
        quota_attrs = fs_util.get_quota_attrs(clients[0], fuse_mounting_dir_1)
        fs_util.file_quota_test(clients[0], fuse_mounting_dir_1, quota_attrs)

        fs_util.set_quota_attrs(clients[0], 100, 10000000, kernel_mounting_dir_1)
        quota_attrs = fs_util.get_quota_attrs(clients[0], kernel_mounting_dir_1)
        fs_util.file_quota_test(clients[0], kernel_mounting_dir_1, quota_attrs)

        clients[0].exec_command(
            sudo=True, cmd=f"cd {fuse_mounting_dir_1};rm -rf *;mkdir test;"
        )
        fs_util.set_quota_attrs(clients[0], 30, 10000000, f"{fuse_mounting_dir_1}/test")
        quota_attrs = fs_util.get_quota_attrs(clients[0], f"{fuse_mounting_dir_1}/test")
        fs_util.file_quota_test(clients[0], f"{fuse_mounting_dir_1}/test", quota_attrs)

        clients[0].exec_command(
            sudo=True, cmd=f"cd {kernel_mounting_dir_1};rm -rf *;mkdir test;"
        )
        fs_util.set_quota_attrs(
            clients[0], 30, 10000000, f"{kernel_mounting_dir_1}/test"
        )
        quota_attrs = fs_util.get_quota_attrs(
            clients[0], f"{kernel_mounting_dir_1}/test"
        )
        fs_util.file_quota_test(
            clients[0], f"{kernel_mounting_dir_1}/test", quota_attrs
        )

        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1

    finally:
        log.info("Clean Up in progess")
        if "5" in build:
            fs_util.set_quota_attrs(clients[0], "0", "0", root_folder_fuse_mount)
        for subvolume in subvolume_list:
            fs_util.remove_subvolume(client1, **subvolume)
        for subvolumegroup in subvolumegroup_list:
            fs_util.remove_subvolumegroup(client1, **subvolumegroup, force=True)
