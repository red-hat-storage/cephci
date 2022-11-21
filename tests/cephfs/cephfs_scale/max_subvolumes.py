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
    out, rc = client.exec_command(sudo=True, cmd=f"{cmd} --format json-pretty")
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
    1. We will get the total available space.
    2. Based on total Available space we create a number of volumes with 1 GB each
    3. Verify there is no error after that

    Clean up:
    1. Deletes all the subvolumes created
    """
    try:
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        default_fs = "cephfs"
        client1 = clients[0]
        available_space = int(get_available_space(client1, default_fs))
        log.info(available_space)
        data_size_iter = int(available_space / 1024000000)
        data_size_iter_20 = data_size_iter - round(data_size_iter * 0.2)
        subvol_list = [
            {
                "vol_name": default_fs,
                "subvol_name": f"subvol_max_{x}",
            }
            for x in range(0, data_size_iter_20)
        ]
        log.info(f"Total Sub Volumes that will be created : {data_size_iter_20}")
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(5))
        )
        for idx, subvol in enumerate(subvol_list):
            try:
                fs_util.create_subvolume(clients[0], **subvol, validate=False)
                fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_{idx}/"
                subvol_path, rc = clients[0].exec_command(
                    sudo=True,
                    cmd=f"ceph fs subvolume getpath {default_fs} {subvol['subvol_name']}",
                )
                fs_util.fuse_mount(
                    [clients[0]],
                    fuse_mounting_dir_1,
                    extra_params=f" -r {subvol_path.strip()}",
                )
                clients[0].exec_command(
                    sudo=True,
                    cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 1 "
                    f"--file-size 1024 "
                    f"--files 1024 --top "
                    f"{fuse_mounting_dir_1}",
                    long_running=True,
                )
                cmd = f"fusermount -u {fuse_mounting_dir_1} -z"
                clients[0].exec_command(sudo=True, cmd=cmd)
                log.info(f"{'*' * 20} Report for Iteration {idx} {'*' * 20} ")
                cmd_list = [
                    "ceph crash ls",
                    "ceph fs status",
                    f"ceph fs subvolume ls {default_fs}",
                    "ceph df",
                ]
                for cmd in cmd_list:
                    collect_ceph_details(clients[0], cmd)
                log.info(f"{'*' * 20} Iteration {idx} completed {'*' * 20} ")
            except CommandFailed:
                log.info(f"Subvolume Create command failed at index: {idx}")
                break
        log.info(f"Subvolume creation is successful from {idx} to {idx + 50}")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean Up in progess")
        for subvol in subvol_list[:-1]:
            fs_util.remove_subvolume(clients[0], **subvol)
        return 0
