import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test Cases Covered:
    CEPH-11319	Create first snap add more data to original then create a second snap.
                Rollback 1st snap do data validation.
                Rollback 2nd snap and do data validation.Perform cross platform rollback
                i.e. take snap on kernel mount and perform rollback using fuse mount
    Pre-requisites :
    1. We need atleast one client node to execute this test case
    1. creats fs volume create cephfs if the volume is not there
    2. ceph fs subvolumegroup create <vol_name> <group_name> --pool_layout <data_pool_name>
        Ex : ceph fs subvolumegroup create cephfs subvol_cross_platform_snapshot_1
    3. ceph fs subvolume create <vol_name> <subvol_name> [--size <size_in_bytes>] [--group_name <subvol_group_name>]
       [--pool_layout <data_pool_name>] [--uid <uid>] [--gid <gid>] [--mode <octal_mode>]  [--namespace-isolated]
       Ex: ceph fs subvolume create cephfs subvol_2 --size 5368706371 --group_name subvolgroup_
    4. Create Data on the subvolume. We will add known data as we are going to verify the files
        Ex: create_file_data()
    5. Create snapshot of the subvolume
        Ex: ceph fs subvolume snapshot create cephfs subvol_2 snap_1 --group_name subvol_cross_platform_snapshot_1

    Script Flow:
    1. Mount the subvolume on the client using Kernel and fuse mount
    2. Write data into the fuse mount point i.e., data_from_fuse_mount
    3. Collect the checksum of the files
    4. Take snapshot at this point i.e., snap_1
    5. Write data into the kernel mount point i.e., data_from_kernel_mount
    6. Collect the checksum of the files
    7. Take snapshot at this point i.e., snap_2
    8. On Kernel mount revert the snapshot to snap_1 and compare the checksum of the files collected in step 3
    9. On Fuse mount revert the snapshot to snap_2 and compare the checksum of the files colleced in step 6

    """
    try:
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        log.info("checking Pre-requisites")
        if len(clients) < 1:
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
        subvolumegroup_list = [
            {"vol_name": default_fs, "group_name": "subvol_cross_platform_snapshot_1"},
        ]
        for subvolumegroup in subvolumegroup_list:
            fs_util.create_subvolumegroup(client1, **subvolumegroup)
        subvolume = {
            "vol_name": default_fs,
            "subvol_name": "subvol_cross_platform_snapshot",
            "group_name": "subvol_cross_platform_snapshot_1",
            "size": "5368706371",
        }
        fs_util.create_subvolume(client1, **subvolume)
        log.info("Get the path of sub volume")
        subvol_path, rc = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} subvol_cross_platform_snapshot"
            f" subvol_cross_platform_snapshot_1",
        )
        subvol_path = subvol_path.strip()
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
        fs_util.fuse_mount(
            [client1],
            fuse_mounting_dir_1,
            extra_params=f" -r {subvol_path}",
        )
        fs_util.create_file_data(
            client1, fuse_mounting_dir_1, 3, "snap1", "data_from_fuse_mount "
        )
        fuse_files_checksum = fs_util.get_files_and_checksum(
            client1, fuse_mounting_dir_1
        )
        fuse_snapshot = {
            "vol_name": default_fs,
            "subvol_name": "subvol_cross_platform_snapshot",
            "snap_name": "snap_1",
            "group_name": "subvol_cross_platform_snapshot_1",
        }
        fs_util.create_snapshot(client1, **fuse_snapshot)
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount(
            [clients[0]],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            sub_dir=f"{subvol_path}",
        )
        fs_util.create_file_data(
            client1, kernel_mounting_dir_1, 3, "snap1", "data_from_kernel_mount "
        )
        kernel_snapshot = {
            "vol_name": default_fs,
            "subvol_name": "subvol_cross_platform_snapshot",
            "snap_name": "snap_2",
            "group_name": "subvol_cross_platform_snapshot_1",
        }
        fs_util.create_snapshot(client1, **kernel_snapshot)
        kernel_files_checksum = fs_util.get_files_and_checksum(
            client1, kernel_mounting_dir_1
        )
        client1.exec_command(
            sudo=True, cmd=f"cd {kernel_mounting_dir_1};cp .snap/_snap_1_*/* ."
        )
        kernel_mount_revert_snap_fuse = fs_util.get_files_and_checksum(
            client1, kernel_mounting_dir_1
        )
        if fuse_files_checksum != kernel_mount_revert_snap_fuse:
            log.error(
                "checksum is not when reverted to snap1 i.e., from fuse mount snapshot revert"
            )
            return 1
        client1.exec_command(
            sudo=True, cmd=f"cd {fuse_mounting_dir_1};cp .snap/_snap_2_*/* ."
        )
        fuse_mount_revert_snap_kernel = fs_util.get_files_and_checksum(
            client1, kernel_mounting_dir_1
        )
        if kernel_files_checksum != fuse_mount_revert_snap_kernel:
            log.error(
                "checksum is not when reverted to snap2 i.e., from kernel mount snapshot revert"
            )
            return 1
        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info("Clean Up in progess")
        fs_util.remove_snapshot(client1, **kernel_snapshot)
        fs_util.remove_snapshot(client1, **fuse_snapshot)
        fs_util.remove_subvolume(client1, **subvolume)
        for subvolumegroup in subvolumegroup_list:
            fs_util.remove_subvolumegroup(client1, **subvolumegroup, force=True)
