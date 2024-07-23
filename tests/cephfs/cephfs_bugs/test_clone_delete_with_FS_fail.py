import random
import string
import traceback

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log
from utility.retry import retry

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test Cases Covered:
    CEPH-83583723 - Ensure deletion of clones are allowed when the clones are stuck and in pending state
    Pre-requisites :

    1. creats fs volume create cephfs if the volume is not there
    2. Create 1 sub volume groups
        Ex : ceph fs subvolumegroup create cephfs subvolgroup_1
    3. ceph fs subvolume create <vol_name> <subvol_name> [--size <size_in_bytes>] [--group_name <subvol_group_name>]
       [--pool_layout <data_pool_name>] [--uid <uid>] [--gid <gid>] [--mode <octal_mode>]  [--namespace-isolated]
       Ex: ceph fs subvolume create cephfs subvol_clone_cancel --size 5368706371 --group_name subvolgroup_
    4. Create Data on the subvolume
        Ex:  python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 400 --files
            100 --files-per-dir 10 --dirs-per-dir 2 --top /mnt/cephfs_fuse1baxgbpaia_1/
    5. Create snapshot of the subvolume
        Ex: ceph fs subvolume snapshot create cephfs subvol_clone_cancel snap_1 --group_name subvolgroup_1

    Test Steps:
    Created 7 clones in which 3 will be going to pending state and 4 in-progess state.
    we are waiting for all clones to get created and deleted them and recreating them in for loop
        for i in {1..10};do echo $i; for i in {1..7};do echo $i;
        ceph fs subvolume snapshot clone cephfs subvol_2 snap_1 clone_${i};
        echo "Creation of clone Done"; done; sh clone_status.sh;for i in {1..7};do echo $i;
        ceph fs subvolume rm cephfs clone_${i};echo "Deletion of volume done"; done;
        echo "##########################################"; done

    In parallel failed mds with ceph mds fail 0 in loop and
    waited for it to become active and waited for 30 seconds this is done for 100 times in loop
        for i in {1..100};do echo $i; ceph mds fail 0; sh test_mds.sh;
        echo "##########################################"; done


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
        log.info("setting 'snapshot_clone_no_wait' to false")
        clients[0].exec_command(
            sudo=True,
            cmd="ceph config set mgr mgr/volumes/snapshot_clone_no_wait false",
        )
        default_fs = "cephfs_clone"
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        client1 = clients[0]
        fs_details = fs_util.get_fs_info(client1, fs_name=default_fs)
        if not fs_details:
            fs_util.create_fs(client1, default_fs)
        subvolumegroup_list = [
            {"vol_name": default_fs, "group_name": "subvolgroup_clone_cancel_1"}
        ]
        for subvolumegroup in subvolumegroup_list:
            fs_util.create_subvolumegroup(client1, **subvolumegroup)
        subvolume = {
            "vol_name": default_fs,
            "subvol_name": "subvol_clone_cancel",
            "group_name": "subvolgroup_clone_cancel_1",
            "size": "5368706371",
        }
        fs_util.create_subvolume(client1, **subvolume)
        log.info("Get the path of sub volume")
        subvol_path, rc = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} subvol_clone_cancel subvolgroup_clone_cancel_1",
        )
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount(
            [clients[0]],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            sub_dir=f"{subvol_path.strip()}",
            extra_params=f",fs={default_fs}",
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
            "subvol_name": "subvol_clone_cancel",
            "snap_name": "snap_1",
            "group_name": "subvolgroup_clone_cancel_1",
        }
        fs_util.create_snapshot(client1, **snapshot)
        clone_list = [
            {
                "vol_name": default_fs,
                "subvol_name": "subvol_clone_cancel",
                "snap_name": "snap_1",
                "target_subvol_name": f"clone_{x}",
                "group_name": "subvolgroup_clone_cancel_1",
            }
            for x in range(1, 8)
        ]
        rmclone_list = [
            {"vol_name": default_fs, "subvol_name": f"clone_{x}"} for x in range(1, 8)
        ]
        for i in range(1, 10):
            log.info(f"Iteration {i} ")
            with parallel() as p:
                p.spawn(
                    clone_ops, clone_list, fs_util, client1, default_fs, rmclone_list
                )
                p.spawn(fail_mds, client1, fs_util, default_fs)
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean Up in progess")
        log.info("setting 'snapshot_clone_no_wait' to true")
        clients[0].exec_command(
            sudo=True, cmd="ceph config set mgr mgr/volumes/snapshot_clone_no_wait true"
        )
        fs_util.remove_snapshot(client1, **snapshot)
        fs_util.remove_subvolume(client1, **subvolume)
        for subvolumegroup in subvolumegroup_list:
            fs_util.remove_subvolumegroup(client1, **subvolumegroup, force=True)
        client1.exec_command(
            sudo=True, cmd=f"ceph fs volume rm {default_fs} --yes-i-really-mean-it"
        )


def fail_mds(client, fs_util, default_fs):
    mds_ls = fs_util.get_active_mdss(client, fs_name=default_fs)
    for mds in mds_ls:
        out, rc = client.exec_command(cmd=f"ceph mds fail {mds}", client_exec=True)
        log.info(out)
    retry_mds_status = retry(CommandFailed, tries=3, delay=30)(fs_util.get_mds_status)
    retry_mds_status(
        client,
        1,
        vol_name=default_fs,
        expected_status="active",
    )


def get_clone_status(client, cmd):
    out, rc = client.exec_command(sudo=True, cmd=cmd)
    log.info(out)


def clone_ops(clone_list, fs_util, client1, default_fs, rmclone_list):
    for clone in clone_list:
        fs_util.create_clone(client1, **clone, validate=False)

    for clone in clone_list:
        get_clone_status(
            client1,
            cmd=f"ceph fs clone status {default_fs} {clone['target_subvol_name']}",
        )
    for clone in clone_list:
        get_clone_status(
            client1,
            cmd=f"ceph fs clone cancel {default_fs} {clone['target_subvol_name']}",
        )
        get_clone_status(
            client1,
            cmd=f"ceph fs clone status {default_fs} {clone['target_subvol_name']}",
        )

    for clonevolume in rmclone_list:
        fs_util.remove_subvolume(
            client1, **clonevolume, force=True, validate=False, check_ec=False
        )
