import json
import random
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test Cases Covered :
    CEPH-83573522   Verify the retained snapshot details with "ceph fs info" command

    Pre-requisites :
    1. We need atleast one client node to execute this test case
    1. creats fs volume create cephfs if the volume is not there
    2. ceph fs subvolumegroup create <vol_name> <group_name> --pool_layout <data_pool_name>
        Ex : ceph fs subvolumegroup create cephfs subvolgroup_info_retain
    3. ceph fs subvolume create <vol_name> <subvol_name> [--size <size_in_bytes>] [--group_name <subvol_group_name>]
       [--pool_layout <data_pool_name>] [--uid <uid>] [--gid <gid>] [--mode <octal_mode>]  [--namespace-isolated]
       Ex: ceph fs subvolume create cephfs subvol_2 --size 5368706371 --group_name subvolgroup_
    4. Create Data on the subvolume
        Ex:  python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 400 --files
            100 --files-per-dir 10 --dirs-per-dir 2 --top /mnt/cephfs_fuse1baxgbpaia_1/
    5. Create snapshot of the subvolume
        Ex: ceph fs subvolume snapshot create cephfs subvol_2 snap_1 --group_name subvolgroup_info_retain

    Retain the snapshots nad verify the data after cloning:
    1. Check the state od subvolume it should be in complete.
    2. ceph fs snapshot rm <vol_name> <subvol_name> snap_name [--group_name <subvol_group_name>
        --force --retain-snapshots]
    3. Remove the sub volume.
    4. Check the state of subvolume it should be in snapshot-retained.

    Clean Up:
    1. Del all the snapshots created
    2. Del Subvolumes
    3. Del SubvolumeGroups
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
            {"vol_name": default_fs, "group_name": "subvolgroup_info_retain"},
        ]
        for subvolumegroup in subvolumegroup_list:
            fs_util.create_subvolumegroup(client1, **subvolumegroup)
        subvolume = {
            "vol_name": default_fs,
            "subvol_name": "subvol_retain_info",
            "group_name": "subvolgroup_info_retain",
            "size": "5368706371",
        }
        fs_util.create_subvolume(client1, **subvolume)
        log.info("Get the path of sub volume")
        subvol_path, rc = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} subvol_retain_info subvolgroup_info_retain",
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
            "subvol_name": "subvol_retain_info",
            "snap_name": "snap_1",
            "group_name": "subvolgroup_info_retain",
        }
        fs_util.create_snapshot(client1, **snapshot)
        client1.exec_command(sudo=True, cmd=f"mkdir -p /tmp/{mounting_dir}")
        client1.exec_command(
            sudo=True, cmd=f"cp -r {kernel_mounting_dir_1}/* /tmp/{mounting_dir}"
        )
        subvol_info, rc = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume info {default_fs} subvol_retain_info subvolgroup_info_retain --format json",
            check_ec=False,
        )
        subvol_info_state = json.loads(subvol_info)
        log.info(
            f"subvol state before removing the volume with --retain-snapshots {subvol_info_state['state']}"
        )
        if subvol_info_state["state"] != "complete":
            raise CommandFailed(
                f"subvol state should be in complete state "
                f"but current state is {subvol_info_state['state']}"
            )
        fs_util.remove_subvolume(
            client1, **subvolume, retain_snapshots=True, force=True, validate=False
        )
        log.info(
            "Verifying Get the path of sub volume as subvolume will still be listed in filesystem"
        )
        subvol_path, rc = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} subvol_retain_info subvolgroup_info_retain",
            check_ec=False,
        )
        if rc == 0:
            raise CommandFailed(
                "Remove subvolume with --retainSnapshots has not succeeded."
                "We are still able to fetch path of subvolume after deletion"
            )
            return 1
        subvol_info, rc = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume info {default_fs} subvol_retain_info subvolgroup_info_retain --format json",
            check_ec=False,
        )
        subvol_info_state = json.loads(subvol_info)
        if subvol_info_state["state"] != "snapshot-retained":
            raise CommandFailed(
                f"subvol state should be in snapshot-retained state "
                f"but current state is {subvol_info_state['state']}"
            )
        log.info(
            f"subvol state after removing the volume with --retain-snapshots {subvol_info_state['state']}"
        )
        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info("Clean Up in progess")
        rmclone_list = [
            {"vol_name": default_fs, "subvol_name": "subvol_retain_info"},
        ]
        for clone_vol in rmclone_list:
            fs_util.remove_subvolume(
                client1, **clone_vol, validate=False, force=True, check_ec=False
            )
        fs_util.remove_snapshot(client1, **snapshot, validate=False, force=True)
        for subvolumegroup in subvolumegroup_list:
            fs_util.remove_subvolumegroup(
                client1, **subvolumegroup, force=True, check_ec=False, validate=False
            )
