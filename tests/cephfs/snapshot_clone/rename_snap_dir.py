import random
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
     Test Cases Covered:
     CEPH-83573255	Try renaming the snapshot directory and rollbackCreate a FS and
                     create 10 directories and mount them on kernel client and fuse client(5 mounts each)
                     Add data (~ 100 GB). Create a Snapshot and verify the content in snap directory.
                     Try modifying the snapshot name.

     Pre-requisites :
     1. We need atleast one client node to execute this test case
     2. creats fs volume create cephfs if the volume is not there
     3. ceph fs subvolumegroup create <vol_name> <group_name> --pool_layout <data_pool_name>
         Ex : ceph fs subvolumegroup create cephfs subvol_rename_sanp_1
     4. ceph fs subvolume create <vol_name> <subvol_name> [--size <size_in_bytes>] [--group_name <subvol_group_name>]
        [--pool_layout <data_pool_name>] [--uid <uid>] [--gid <gid>] [--mode <octal_mode>]  [--namespace-isolated]
        Ex: ceph fs subvolume create cephfs subvol_2 --size 5368706371 --group_name subvolgroup_
    5. Create snapshot of the subvolume
         Ex: ceph fs subvolume snapshot create cephfs subvol_2 snap_1 --group_name subvol_rename_sanp_1

     Script Flow:
     1. Mount the subvolume on the client using fuse mount.
     2. Rename the snapshot snap/_snap_1 to snap/_snap_rename.
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
            {"vol_name": default_fs, "group_name": "subvol_rename_sanp_1"},
        ]
        for subvolumegroup in subvolumegroup_list:
            fs_util.create_subvolumegroup(client1, **subvolumegroup)
        subvolume = {
            "vol_name": default_fs,
            "subvol_name": "subvol_rename_sanp",
            "group_name": "subvol_rename_sanp_1",
            "size": "5368706371",
        }
        fs_util.create_subvolume(client1, **subvolume)
        log.info("Get the path of sub volume")
        subvol_path, rc = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} subvol_rename_sanp subvol_rename_sanp_1",
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
        fuse_snapshot = {
            "vol_name": default_fs,
            "subvol_name": "subvol_rename_sanp",
            "snap_name": "snap_1",
            "group_name": "subvol_rename_sanp_1",
        }
        fs_util.create_snapshot(client1, **fuse_snapshot)
        out, rc = client1.exec_command(
            sudo=True,
            cmd=f"cd {fuse_mounting_dir_1};mv .snap/_snap_1_* .snap/_snap_rename_",
            check_ec=False,
        )
        if rc == 0:
            raise CommandFailed(
                "we are able to rename the snap directory.. which is not correct"
            )
        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info("Clean Up in progess")
        fs_util.remove_snapshot(client1, **fuse_snapshot)
        fs_util.remove_subvolume(client1, **subvolume)
        for subvolumegroup in subvolumegroup_list:
            fs_util.remove_subvolumegroup(client1, **subvolumegroup, force=True)
