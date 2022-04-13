import json
import random
import string
import traceback

from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Pre-requisites :
    1. We need atleast one client node to execute this test case
    1. creats fs volume create cephfs if the volume is not there
    2. ceph fs subvolumegroup create <vol_name> <group_name> --pool_layout <data_pool_name>
        Ex : ceph fs subvolumegroup create cephfs subvolgroup_1
    3. ceph fs subvolume create <vol_name> <subvol_name> [--size <size_in_bytes>] [--group_name <subvol_group_name>]
       [--pool_layout <data_pool_name>] [--uid <uid>] [--gid <gid>] [--mode <octal_mode>]  [--namespace-isolated]
       Ex: ceph fs subvolume create cephfs subvol_2 --size 5368706371 --group_name subvolgroup_
    4. Create Data on the subvolume
        Ex:  python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 400 --files
            100 --files-per-dir 10 --dirs-per-dir 2 --top /mnt/cephfs_fuse1baxgbpaia_1/
    5. Create snapshot of the subvolume
        Ex: ceph fs subvolume snapshot create cephfs subvol_2 snap_1 --group_name subvolgroup_1

    Concurrent Clone Operations:
    1. Validate default value foe the clones i.e., 4
    2. Create 5 clones of the snap_1
        Ex: ceph fs subvolume snapshot clone cephfs subvol_2 snap_1 clone_1 --group_name subvolgroup_1
            ceph fs subvolume snapshot clone cephfs subvol_2 snap_1 clone_2 --group_name subvolgroup_1
            ceph fs subvolume snapshot clone cephfs subvol_2 snap_1 clone_3 --group_name subvolgroup_1
            ceph fs subvolume snapshot clone cephfs subvol_2 snap_1 clone_4 --group_name subvolgroup_1
            ceph fs subvolume snapshot clone cephfs subvol_2 snap_1 clone_5 --group_name subvolgroup_1
    3. Get the status of each clone using below command
        Ex: ceph fs clone status cephfs clone_1
            ceph fs clone status cephfs clone_2
            ceph fs clone status cephfs clone_3
            ceph fs clone status cephfs clone_4
            ceph fs clone status cephfs clone_5
    4. We are validating the total clones in_progress should not be greater than 4
    5. Once all the clones moved to complete state we are deleting all the clones
    6. Set the concurrent threads to 2
        Ex: ceph config set mgr mgr/volumes/max_concurrent_clones 2
    7. Create 5 clones of the snap_1
        Ex: ceph fs subvolume snapshot clone cephfs subvol_2 snap_1 clone_1 --group_name subvolgroup_1
            ceph fs subvolume snapshot clone cephfs subvol_2 snap_1 clone_2 --group_name subvolgroup_1
            ceph fs subvolume snapshot clone cephfs subvol_2 snap_1 clone_3 --group_name subvolgroup_1
            ceph fs subvolume snapshot clone cephfs subvol_2 snap_1 clone_4 --group_name subvolgroup_1
            ceph fs subvolume snapshot clone cephfs subvol_2 snap_1 clone_5 --group_name subvolgroup_1
    8. Get the status of each clone using below command
        Ex: ceph fs clone status cephfs clone_1
            ceph fs clone status cephfs clone_2
            ceph fs clone status cephfs clone_3
            ceph fs clone status cephfs clone_4
            ceph fs clone status cephfs clone_5
    9. We are validating the total clones in_progress should not be greater than 2
    10.Once all the clones moved to complete state we are deleting all the clones
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
        subvolumegroup = {"vol_name": default_fs, "group_name": "subvolgroup_1"}
        fs_util.create_subvolumegroup(client1, **subvolumegroup)
        subvolume = {
            "vol_name": default_fs,
            "subvol_name": "subvol_2",
            "group_name": "subvolgroup_1",
            "size": "5368706371",
        }
        fs_util.create_subvolume(client1, **subvolume)
        log.info("Get the path of sub volume")
        subvol_path, rc = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} subvol_2 subvolgroup_1",
        )
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
        fs_util.fuse_mount(
            [client1],
            fuse_mounting_dir_1,
            extra_params=f" -r {subvol_path.strip()}",
        )
        client1.exec_command(
            sudo=True,
            cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 400 "
            f"--files 100 --files-per-dir 10 --dirs-per-dir 2 --top "
            f"{fuse_mounting_dir_1}",
            long_running=True,
        )
        snapshot = {
            "vol_name": default_fs,
            "subvol_name": "subvol_2",
            "snap_name": "snap_1",
            "group_name": "subvolgroup_1",
        }
        fs_util.create_snapshot(client1, **snapshot)
        clone_list = [
            {
                "vol_name": default_fs,
                "subvol_name": "subvol_2",
                "snap_name": "snap_1",
                "target_subvol_name": f"clone_{x}",
                "group_name": "subvolgroup_1",
            }
            for x in range(1, 6)
        ]
        with parallel() as p:
            for clone in clone_list:
                p.spawn(fs_util.create_clone, client1, **clone, validate=False)
        status_list = []
        iteration = 0
        while status_list.count("complete") < len(clone_list):
            status_list.clear()
            iteration += 1
            for clone in clone_list:
                cmd_out, cmd_rc = fs_util.get_clone_status(
                    client1, clone["vol_name"], clone["target_subvol_name"]
                )
                status = json.loads(cmd_out)
                status_list.append(status["status"]["state"])
                log.info(
                    f"{clone['target_subvol_name']} status is {status['status']['state']}"
                )
            if status_list.count("in-progress") > 4:
                return 1
            else:
                log.info(
                    f"cloneing is in progress for {status_list.count('in-progress')} out of {len(clone_list)}"
                )
            log.info(f"Iteration {iteration} has been completed")

        rmclone_list = [
            {"vol_name": default_fs, "subvol_name": f"clone_{x}"} for x in range(1, 6)
        ]
        for clonevolume in rmclone_list:
            fs_util.remove_subvolume(client1, **clonevolume)
        log.info("Set clone threads to 2 and verify only 2 clones are in progress")
        client1.exec_command(
            sudo=True, cmd="ceph config set mgr mgr/volumes/max_concurrent_clones 2"
        )
        for clone in clone_list:
            fs_util.create_clone(client1, **clone)
        status_list = []
        iteration = 0
        while status_list.count("complete") < len(clone_list):
            iteration += 1
            status_list.clear()
            for clone in clone_list:
                cmd_out, cmd_rc = fs_util.get_clone_status(
                    client1, clone["vol_name"], clone["target_subvol_name"]
                )
                status = json.loads(cmd_out)
                status_list.append(status["status"]["state"])
                log.info(
                    f"{clone['target_subvol_name']} status is {status['status']['state']}"
                )
            if status_list.count("in-progress") > 2:
                return 1
            else:
                log.info(
                    f"cloneing is in progress for {status_list.count('in-progress')} out of {len(clone_list)}"
                )
            log.info(f"Iteration {iteration} has been completed")
        for clonevolume in rmclone_list:
            fs_util.remove_subvolume(client1, **clonevolume)
        log.info("Clean Up in progess")
        fs_util.remove_snapshot(client1, **snapshot)
        fs_util.remove_subvolume(client1, **subvolume)
        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
