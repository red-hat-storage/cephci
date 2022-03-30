import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test Cases Covered:
    CEPH-83573418	Create a Snapshot, reboot the node and rollback the snapshot

    Pre-requisites :
    1. We need atleast one client node to execute this test case
    2. creats fs volume create cephfs if the volume is not there
    3. ceph fs subvolumegroup create <vol_name> <group_name> --pool_layout <data_pool_name>
        Ex : ceph fs subvolumegroup create cephfs subvolgroup_reboot_snapshot_1
    4. ceph fs subvolume create <vol_name> <subvol_name> [--size <size_in_bytes>] [--group_name <subvol_group_name>]
       [--pool_layout <data_pool_name>] [--uid <uid>] [--gid <gid>] [--mode <octal_mode>]  [--namespace-isolated]
       Ex: ceph fs subvolume create cephfs subvol_2 --size 5368706371 --group_name subvolgroup_
    5. Create Data on the subvolume. We will add known data as we are going to verify the files
        Ex: create_file_data()
    6. Create snapshot of the subvolume
        Ex: ceph fs subvolume snapshot create cephfs subvol_2 snap_1 --group_name subvolgroup_reboot_snapshot_1

    Script Flow:
    1. Mount the subvolume on the client using Kernel
    2. Write data into the mount point
    3. Get the checksum of the files inside the mount point
    4. Reboot the node
    5. Mount again the subvolume and revert the snapshot
    6. get the checksum of the files
    7. Validate the checksums
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
            {"vol_name": default_fs, "group_name": "subvolgroup_reboot_snapshot_1"},
        ]
        for subvolumegroup in subvolumegroup_list:
            fs_util.create_subvolumegroup(client1, **subvolumegroup)
        subvolume = {
            "vol_name": default_fs,
            "subvol_name": "subvol_reboot_snapshot",
            "group_name": "subvolgroup_reboot_snapshot_1",
            "size": "5368706371",
        }
        fs_util.create_subvolume(client1, **subvolume)
        log.info("Get the path of sub volume")
        subvol_path, rc = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} subvol_reboot_snapshot subvolgroup_reboot_snapshot_1",
        )
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount(
            [clients[0]],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            sub_dir=f"{subvol_path.strip()}",
        )
        fs_util.create_file_data(
            client1, kernel_mounting_dir_1, 3, "snap1", "snap_1_data "
        )
        snapshot = {
            "vol_name": default_fs,
            "subvol_name": "subvol_reboot_snapshot",
            "snap_name": "snap_1",
            "group_name": "subvolgroup_reboot_snapshot_1",
        }
        fs_util.create_snapshot(client1, **snapshot)
        files_checksum_before_reboot = fs_util.get_files_and_checksum(
            client1, f"/mnt/cephfs_kernel{mounting_dir}_1"
        )
        fs_util.reboot_node(client1)
        kernel_mounting_dir_2 = f"/mnt/cephfs_kernel{mounting_dir}_2/"
        subvol_path, rc = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} subvol_reboot_snapshot subvolgroup_reboot_snapshot_1",
        )
        fs_util.kernel_mount(
            [clients[0]],
            kernel_mounting_dir_2,
            ",".join(mon_node_ips),
            sub_dir=f"{subvol_path.strip()}",
        )
        client1.exec_command(
            sudo=True, cmd=f"cd {kernel_mounting_dir_2};cp .snap/_snap_1_*/* ."
        )
        files_checksum_after_reboot = fs_util.get_files_and_checksum(
            client1, kernel_mounting_dir_2
        )
        if files_checksum_before_reboot != files_checksum_after_reboot:
            log.error("checksum is not matching after snapshot1 revert")
            return 1
        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info("Clean Up in progess")
        fs_util.remove_snapshot(client1, **snapshot)
        fs_util.remove_subvolume(client1, **subvolume)
