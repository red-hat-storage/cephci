import json
import random
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def get_available_space(client, fs_name="cephfs"):
    out, rc = client.exec_command(
        sudo=True, cmd=f"ceph fs status {fs_name} --format json"
    )
    output = json.loads(out)
    return next(
        (pool["avail"] for pool in output["pools"] if pool["type"] == "data"), None
    )


def collect_ceph_details(client, cmd):
    out, rc = client.exec_command(sudo=True, cmd=f"{cmd} --format json")
    output = json.loads(out)
    log.info(output)


def run(ceph_cluster, **kw):
    """
    Test Cases Covered:
    CEPH-83573520	Validate the max snapshot that can be created under a root FS sub volume level.

    Pre-requisites :
    1. We need atleast one client node to execute this test case
    2. creats fs volume create cephfs if the volume is not there
    3. ceph fs subvolume create <vol_name> <subvol_name> [--size <size_in_bytes>] [--group_name <subvol_group_name>]
       [--pool_layout <data_pool_name>] [--uid <uid>] [--gid <gid>] [--mode <octal_mode>]  [--namespace-isolated]
       Ex: ceph fs subvolume create cephfs subvol_max_snap --size 5368706371 --group_name subvolgroup_1
    4. Create Data on the subvolume
        Ex:  python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 400 --files
            100 --files-per-dir 10 --dirs-per-dir 2 --top /mnt/cephfs_fuse1baxgbpaia_1/

    Test Script Flow :
    1. We will set max snapshots to maximum allowed value 4096.
    2. We create subvolume and write data to subvolume and create snapshot
    3. For every snapshot we are going to write new file

    Clean up:
    1. Deletes all the snapshots created
    2. Deletes snapshot and subvolume created.
    """
    try:
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        max_snap = 4096
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
        subvolume = {
            "vol_name": default_fs,
            "subvol_name": "subvol_max_snap",
        }
        fs_util.create_subvolume(client1, **subvolume)
        log.info("Get the path of sub volume")
        subvol_path, rc = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} subvol_max_snap",
        )
        client1.exec_command(
            sudo=True,
            cmd=f"ceph config set mds mds_max_snaps_per_dir {max_snap}",
        )
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount(
            [clients[0]],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            sub_dir=f"{subvol_path.strip()}",
        )
        available_space = int(get_available_space(client1, default_fs))
        log.info(available_space)
        data_size_iter = int(available_space / 1024000000)
        snapshot_list = [
            {
                "vol_name": default_fs,
                "subvol_name": "subvol_max_snap",
                "snap_name": f"snap_limit_{x}",
            }
            for x in range(0, data_size_iter)
        ]
        for snapshot in snapshot_list:
            try:
                clients[0].exec_command(
                    sudo=True,
                    cmd=f"mkdir -p {kernel_mounting_dir_1}/{snapshot['snap_name']}",
                )

                clients[0].exec_command(
                    sudo=True,
                    cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 "
                    f"--file-size 512 "
                    f"--files 20480 --top "
                    f"{kernel_mounting_dir_1}/{snapshot['snap_name']}",
                    long_running=True,
                )
                fs_util.create_snapshot(clients[0], **snapshot, validate=False)
            except CommandFailed:
                log.info(
                    f"Max Snapshots allowed under a root FS sub volume level is {snapshot}"
                )

        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        cmd_list = [
            "ceph crash ls",
            "ceph fs status",
            f"ceph fs subvolume snapshot ls {default_fs} subvol_max_snap",
        ]
        map(collect_ceph_details, cmd_list)
        for cmd in cmd_list:
            collect_ceph_details(client1, cmd)
        log.info("Clean Up in progess")
        for snapshot in snapshot_list:
            fs_util.remove_snapshot(client1, **snapshot)
        fs_util.remove_subvolume(client1, **subvolume, check_ec=False)
