import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test Cases Covered :
    CEPH-83573524	Ensure the subvolume attributes are retained post clone operations

    Pre-requisites :
    1. We need atleast one client node to execute this test case
    2. create fs volume create cephfs if the volume is not there
    3. ceph fs subvolumegroup create <vol_name> <group_name> --pool_layout <data_pool_name>
        Ex : ceph fs subvolumegroup create cephfs subvolgroup_clone_attr_vol_1
    4. ceph fs subvolume create <vol_name> <subvol_name> [--size <size_in_bytes>] [--group_name <subvol_group_name>]
       [--pool_layout <data_pool_name>] [--uid <uid>] [--gid <gid>] [--mode <octal_mode>]  [--namespace-isolated]
       Ex: ceph fs subvolume create cephfs subvol_2 --size 5368706371 --group_name subvolgroup_1
    5. Create Data on the subvolume
        Ex:  python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 400 --files
            100 --files-per-dir 10 --dirs-per-dir 2 --top /mnt/cephfs_fuse1baxgbpaia_1/
    6. Create snapshot of the subvolume
        Ex: ceph fs subvolume snapshot create cephfs subvol_2 snap_1 --group_name subvolgroup_clone_attr_vol_1
    7. Set file and size xattributes on dir

    Test Case Flow:
    1. Create Clone out of subvolume.
    2. Mount the cloned volume.
    3. Validate the contents of cloned volume with contents present of subvolume
    4. Validate file and size xattributes on dir in cloned volume

    Clean Up:
    1. Delete Cloned volume
    2. Delete subvolumegroup
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
            {"vol_name": default_fs, "group_name": "subvolgroup_clone_attr_vol_1"},
        ]
        for subvolumegroup in subvolumegroup_list:
            fs_util.create_subvolumegroup(client1, **subvolumegroup)
        subvolume = {
            "vol_name": default_fs,
            "subvol_name": "subvol_clone_attr_vol",
            "group_name": "subvolgroup_clone_attr_vol_1",
            "size": "5368706371",
        }
        fs_util.create_subvolume(client1, **subvolume)
        log.info("Get the path of sub volume")
        subvol_path, rc = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} subvol_clone_attr_vol subvolgroup_clone_attr_vol_1",
        )
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount(
            [clients[0]],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            sub_dir=f"{subvol_path.strip()}",
        )
        client1.exec_command(
            sudo=True,
            cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 400 "
            f"--files 100 --files-per-dir 10 --dirs-per-dir 1 --top "
            f"{kernel_mounting_dir_1}",
            long_running=True,
        )
        fs_util.set_quota_attrs(clients[0], 9999, 999, kernel_mounting_dir_1)
        quota_attrs = fs_util.get_quota_attrs(clients[0], kernel_mounting_dir_1)
        snapshot = {
            "vol_name": default_fs,
            "subvol_name": "subvol_clone_attr_vol",
            "snap_name": "snap_1",
            "group_name": "subvolgroup_clone_attr_vol_1",
        }
        fs_util.create_snapshot(client1, **snapshot)
        client1.exec_command(sudo=True, cmd=f"mkdir -p /tmp/{mounting_dir}")

        log.info("Clone a subvolume from snapshot")
        clone_attr_vol_1 = {
            "vol_name": default_fs,
            "subvol_name": "subvol_clone_attr_vol",
            "snap_name": "snap_1",
            "target_subvol_name": "clone_attr_vol_1",
            "group_name": "subvolgroup_clone_attr_vol_1",
        }
        fs_util.create_clone(client1, **clone_attr_vol_1)
        fs_util.validate_clone_state(client1, clone_attr_vol_1)
        clonevol_path, rc = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} {clone_attr_vol_1['target_subvol_name']}",
        )
        fuse_mounting_dir_2 = f"/mnt/cephfs_fuse{mounting_dir}_2/"
        fs_util.fuse_mount(
            [client1],
            fuse_mounting_dir_2,
            extra_params=f" -r {clonevol_path.strip()}",
        )
        quota_attrs_clone = fs_util.get_quota_attrs(clients[0], fuse_mounting_dir_2)
        client1.exec_command(
            sudo=True, cmd=f"diff -qr {kernel_mounting_dir_1} {fuse_mounting_dir_2}"
        )
        if quota_attrs_clone != quota_attrs:
            log.info(f"attributes of cloned volumes{quota_attrs_clone}")
            log.info(f"attributes of volumes{quota_attrs}")
            log.error(
                "Quota attributes of the clone is not matching with quota attributes of subvolume"
            )
            return 1
        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info("Clean Up in progess")
        rmclone_list = [
            {"vol_name": default_fs, "subvol_name": "clone_attr_vol_1"},
        ]
        for clone_vol in rmclone_list:
            fs_util.remove_subvolume(client1, **clone_vol)
        fs_util.remove_snapshot(client1, **snapshot, validate=False, check_ec=False)
        fs_util.remove_subvolume(client1, **subvolume, validate=False, check_ec=False)
        for subvolumegroup in subvolumegroup_list:
            fs_util.remove_subvolumegroup(client1, **subvolumegroup, force=True)
