import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test Cases Covered:
    CEPH-83573501	Create a Cloned Volume using a snapshot

    Pre-requisites :
    1. We need atleast one client node to execute this test case
    2. creats fs volume create cephfs if the volume is not there
    3. Create 2 sub volume groups
        Ex : ceph fs subvolumegroup create cephfs subvolgroup_1
             ceph fs subvolumegroup create cephfs subvolgroup_2
    4. ceph fs subvolume create <vol_name> <subvol_name> [--size <size_in_bytes>] [--group_name <subvol_group_name>]
       [--pool_layout <data_pool_name>] [--uid <uid>] [--gid <gid>] [--mode <octal_mode>]  [--namespace-isolated]
       Ex: ceph fs subvolume create cephfs subvol_clone_status --size 5368706371 --group_name subvolgroup_
    5. Create Data on the subvolume
        Ex:  python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 400 --files
            100 --files-per-dir 10 --dirs-per-dir 2 --top /mnt/cephfs_fuse1baxgbpaia_1/
    6. Create snapshot of the subvolume
        Ex: ceph fs subvolume snapshot create cephfs subvol_clone_status snap_1 --group_name subvolgroup_1

    Clone Operations and Clone States:
    1. Create a clone in default locaction.
        ceph fs subvolume snapshot clone cephfs subvol_clone_status snap_1 clone_status_1 --group_name subvolgroup_1
    2. Validate all the states of clone creation progress
        pending : Clone operation has not started
        in-progress : Clone operation is in progress
        complete : Clone operation has successfully finished
    3. Mount the cloned volume and check the contents
    4. Create a clone in different subvolumegroup ie., subvolumegroup 2
        ceph fs subvolume snapshot clone cephfs subvol_clone_status snap_1 clone_status_1 --group_name subvolgroup_1
            --target_group_name subvolgroup_2
    5. Validate all the states of clone creation progress
        pending : Clone operation has not started
        in-progress : Clone operation is in progress
        complete : Clone operation has successfully finished
    6.Once all the clones moved to complete state we are deleting all the clones

    Clean-up:
    1. ceph fs snapshot rm <vol_name> <subvol_name> snap_name [--group_name <subvol_group_name>]
    2. ceph fs subvolume rm <vol_name> <subvol_name> [--group_name <subvol_group_name>]
    3. ceph fs subvolumegroup rm <vol_name> <group_name>
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
            {"vol_name": default_fs, "group_name": "subvolgroup_clone_status_1"},
            {"vol_name": default_fs, "group_name": "subvolgroup_clone_status_2"},
        ]
        for subvolumegroup in subvolumegroup_list:
            fs_util.create_subvolumegroup(client1, **subvolumegroup)
        subvolume = {
            "vol_name": default_fs,
            "subvol_name": "subvol_clone_status",
            "group_name": "subvolgroup_clone_status_1",
            "size": "5368706371",
        }
        fs_util.create_subvolume(client1, **subvolume)
        log.info("Get the path of sub volume")
        subvol_path, rc = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} subvol_clone_status subvolgroup_clone_status_1",
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
            f"--files 100 --files-per-dir 10 --dirs-per-dir 2 --top "
            f"{kernel_mounting_dir_1}",
            long_running=True,
        )
        snapshot = {
            "vol_name": default_fs,
            "subvol_name": "subvol_clone_status",
            "snap_name": "snap_1",
            "group_name": "subvolgroup_clone_status_1",
        }
        fs_util.create_snapshot(client1, **snapshot)
        clone_status_1 = {
            "vol_name": default_fs,
            "subvol_name": "subvol_clone_status",
            "snap_name": "snap_1",
            "target_subvol_name": "clone_status_1",
            "group_name": "subvolgroup_clone_status_1",
        }
        fs_util.create_clone(client1, **clone_status_1)
        transitation_states = fs_util.validate_clone_state(client1, clone_status_1)
        valid_state_flow = [
            ["pending", "in-progress", "complete"],
            ["in-progress", "complete"],
        ]
        if transitation_states in valid_state_flow:
            return 1
        clonevol_path, rc = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} {clone_status_1['target_subvol_name']}",
        )
        fuse_mounting_dir_2 = f"/mnt/cephfs_fuse{mounting_dir}_2/"
        fs_util.fuse_mount(
            [client1],
            fuse_mounting_dir_2,
            extra_params=f" -r {clonevol_path.strip()}",
        )
        client1.exec_command(
            sudo=True, cmd=f"diff -qr {kernel_mounting_dir_1} {fuse_mounting_dir_2}"
        )

        clone_status_2 = {
            "vol_name": default_fs,
            "subvol_name": "subvol_clone_status",
            "snap_name": "snap_1",
            "target_subvol_name": "clone_status_2",
            "group_name": "subvolgroup_clone_status_1",
            "target_group_name": "subvolgroup_clone_status_2",
        }
        fs_util.create_clone(client1, **clone_status_2)
        transitation_states = fs_util.validate_clone_state(client1, clone_status_2)
        if transitation_states in valid_state_flow:
            return 1
        clonevol_path, rc = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} "
            f"{clone_status_2['target_subvol_name']} {clone_status_2['target_group_name']}",
        )
        fuse_mounting_dir_3 = f"/mnt/cephfs_fuse{mounting_dir}_3/"
        fs_util.fuse_mount(
            [client1],
            fuse_mounting_dir_3,
            extra_params=f" -r {clonevol_path.strip()}",
        )
        client1.exec_command(
            sudo=True, cmd=f"diff -qr {kernel_mounting_dir_1} {fuse_mounting_dir_3}"
        )
        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info("Clean Up in progess")
        rmclone_list = [
            {"vol_name": default_fs, "subvol_name": "clone_status_1"},
            {
                "vol_name": default_fs,
                "subvol_name": "clone_status_2",
                "group_name": "subvolgroup_clone_status_2",
            },
        ]
        for clone_vol in rmclone_list:
            fs_util.remove_subvolume(client1, **clone_vol)
        fs_util.remove_snapshot(client1, **snapshot)
        fs_util.remove_subvolume(client1, **subvolume)
        for subvolumegroup in subvolumegroup_list:
            fs_util.remove_subvolumegroup(client1, **subvolumegroup, force=True)
