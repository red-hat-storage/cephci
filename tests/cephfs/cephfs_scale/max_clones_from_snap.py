import datetime
import json
import random
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log
from utility.retry import retry
from utility.utils import get_storage_stats

log = Log(__name__)


@retry(AssertionError, tries=60, delay=10, backoff=1)
def fill_volume(data_params):
    """
    To fill volume at mountpoint upto specified percentage

    Args:
        client: client where volume is mounted
        mounting_dir: volume mount_point path where data needs to be added
        expected_percent: desired used_percentage in 'df -h mnt_path' upto which data needs to be added
    Returns:
        None
    Raises:
        AssertionError
    """
    rand_str = "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in list(range(3))
    )
    mounting_dir = data_params["mounting_dir"]
    client = data_params["client"]
    mounting_dir_path = f"{mounting_dir}dir_{rand_str}"
    client.exec_command(
        sudo=True,
        cmd=f"mkdir {mounting_dir_path}",
    )
    file_size = data_params["file_size"]
    files = data_params["files"]
    files_per_dir = data_params["files_per_dir"]
    client.exec_command(
        sudo=True,
        cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 2 --file-size {file_size} "
        f"--files {files} --files-per-dir {files_per_dir} --dirs-per-dir 2 --top "
        f"{mounting_dir_path}",
    )
    used_percent = int(get_subvol_usage(client, mounting_dir))
    if used_percent < data_params["expected_percent"]:
        raise AssertionError("Volume is not filled to expected percent")


def get_pool_usage(client, cephfs_name):
    """
    To get current cephfs data pool usage

    Args:
        client: client to run ceph df cmd
        cephfs_name: cephfs volume whose data pool usage is required
    Returns:
        percent_used : data pool usage in percent
    """
    pool_name = f"cephfs.{cephfs_name}.data"
    pool_stats = get_storage_stats(client, pool_name)
    percent_used = pool_stats["percent_used"] * 100
    log.info(f"pool used percent:{percent_used} \n")
    return percent_used


def get_subvol_usage(client, mnt_pt):
    """
    To get subvolume used_percent from cmd 'df -h'

    Args:
        client: client where volume is mounted
        mnt_pt: volume mount_point path to get used percentage
    Returns:
        percent_used : used percentage of volume at mnt_pt
    """
    df_op, rc = client.exec_command(
        sudo=True, cmd=f"df -h {mnt_pt}" + "| awk '{print $5}' "
    )
    df_list = df_op.splitlines()
    log.info(df_list)
    percent_used = df_list[1].replace("%", "")
    log.info(f"subvol used percent:{percent_used} \n")
    return percent_used


@retry(AssertionError, tries=90, delay=20, backoff=1)
def wait_for_clone(client, clone, timeout=600):
    """
    To wait for clone status to be complete
    Args:
        client: client to check clone current status
        clone: clone dict with below args,
               clonevol = {
                    "vol_name": default_fs,
                    "target_subvol_name": clone_name,
                    "group_name": group_name
                }
        timeout : optional, to wait for clone status command to run.
    Returns:
        None
    Raises:
        AssertionError
    """
    clone_status_cmd = f"ceph fs clone status {clone.get('vol_name')} {clone.get('target_subvol_name')} "
    clone_status_cmd = (
        clone_status_cmd + f"--group_name {clone.get('group_name')} --format json"
    )
    cmd_out, cmd_rc = client.exec_command(
        sudo=True,
        cmd=clone_status_cmd,
        check_ec=clone.get("check_ec", True),
        timeout=timeout,
    )
    status = json.loads(cmd_out)
    clone_state = status["status"]["state"]
    if "complete" not in clone_state:
        raise AssertionError(f"Clone state : {clone_state}")


def run(ceph_cluster, **kw):
    """
    Test Cases Covered :
    CEPH-83575629 - Validate that max clones can be created using a snapshot of single subvolume
    Pre-requisites :
    1. Atleast one client node

    Test Steps:
    1. Creats CephFS Volume, if doesn't exist.
    2. Create subvolumegroup, cmd : ceph fs subvolumegroup create <cephfs_name> <subvolumegroup_name>
        Ex : ceph fs subvolumegroup create cephfs subvolgroup_max_clones
    3. Create subvolume, cmd : ceph fs subvolume create <cephfs_name>  <subvolume_name> --size <subvolume_size>
       --group_name <subvolumegroup_name>
       Ex: ceph fs subvolume create cephfs  subvolume_max_clones --size 5G --group_name subvolgroup_max_clones
    4. Get subvolume path with cmd,
        ceph fs subvolume getpath <subvolume_name> <subvolume_group>
    5. Kernel mount the subvolume,
       cmd : mount -t ceph <mon_ip>:6789:/<subvolume_path> mnt_pt -o name=client_id,fs=cephfs_name
    6. fuse mount the subvolume, cmd : ceph-fuse -n admin -r /subvolume_path mnt_pt --client_fs cephfs_name
    5. On kernel mnt_pt, Create Data on the subvolume to fill cephfs.data pool by 70%
       Ex: smallfile_cli.py --operation create --threads 2 --file-size 500 --files 100 --files-per-dir 10
           --dirs-per-dir 2 --top mounting_dir_path
        cmd to verify cephfs data pool usage, 'ceph df'
    6. Create snapshot of the subvolume.
        Ex: ceph fs subvolume snapshot create cephfs subvolume_max_clones snap_max_clones
          --group_name subvolgroup_max_clones
    7. Create clone from snapshot created in step6, verify cephfs.data pool usage, if not 100%, repeat step7.
        Ex: ceph fs subvolume snapshot create cephfs subvolume_max_clones snap_max_clones clonevol_1
        --group_name subvolgroup_max_clones
    8. Verify max clones can be created until data pool is full.

    Clean Up:
    1. Unmount subvolume
    2. Delete all Clones
    3. Delete subvolume, subvolumegroup.

    test params template for yaml suite:

    RHOS VM: Default values in script are applicable to VM
    - test:
      abort-on-fail: false
      config:
        subvol_size: 5368706371
        test_timeout: 18000
        subvol_data_fill: 2
      desc: "Validate max clones from single snapshot of subvolume"
      module: cephfs_scale.max_clones_from_snap.py
      name: "Clone scale testing"
      polarion-id: "CEPH-83575629"
    Baremetal: override default values as below
    - test:
      abort-on-fail: false
      config:
        subvol_size: 53687063710
        test_timeout: 259200
        subvol_data_fill: 30
        platform_type:
      desc: "Validate max clones from single snapshot of subvolume"
      module: cephfs_scale.max_clones_from_snap.py
      name: "Clone scale testing"
      polarion-id: "CEPH-83575629"
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
        default_subvol_size = config.get("subvol_size", 5368706371)
        expected_pool_used = config.get("expected_pool_usage", 98)
        subvol_data_fill = config.get("subvol_data_fill", 2)
        test_timeout = config.get("test_timeout", 18000)
        cmd_timeout = 3600
        platform_type = config.get("platform_type", "default")
        data_fill_params = {
            "expected_percent": subvol_data_fill,
        }
        if platform_type == "baremetal":
            data_fill_params.update(
                {"file_size": 2000, "files": 1000, "files_per_dir": 100}
            )
        else:
            data_fill_params.update(
                {"file_size": 500, "files": 100, "files_per_dir": 10}
            )
        rand_str = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(3))
        )
        group_name = f"subvolgroup_max_clones_{rand_str}"
        subvol_name = f"subvol_max_clones_{rand_str}"
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        client1 = clients[0]

        fs_details = fs_util.get_fs_info(client1)
        if not fs_details:
            fs_util.create_fs(client1, "cephfs")
        subvolumegroup = {"vol_name": default_fs, "group_name": group_name}

        log.info("Create Subvolumegroup")
        fs_util.create_subvolumegroup(client1, **subvolumegroup)
        subvolume = {
            "vol_name": default_fs,
            "subvol_name": subvol_name,
            "group_name": group_name,
            "size": default_subvol_size,
        }

        log.info("Create Subvolume")
        fs_util.create_subvolume(client1, **subvolume)

        log.info("Get the path of Subvolume")
        subvol_path, rc = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} {subvol_name} {group_name}",
        )
        log.info(f"sub_vol_path : {subvol_path} \n")

        kernel_mounting_dir = f"/mnt/cephfs_kernel_{mounting_dir}_1/"
        fuse_mounting_dir = f"/mnt/cephfs_fuse_{mounting_dir}_1/"
        mon_node_ips = fs_util.get_mon_node_ips()

        log.info("Kernel mount on Subvolume \n")
        fs_util.kernel_mount(
            [client1],
            kernel_mounting_dir,
            ",".join(mon_node_ips),
            sub_dir=f"{subvol_path.strip()}",
        )
        log.info("Ceph-Fuse mount on Subvolume")
        fs_util.fuse_mount(
            [client1],
            fuse_mounting_dir,
            extra_params=f" -r {subvol_path.strip()}",
        )
        log.info(f"Add data to Subvolume to fill {subvol_data_fill}% \n")
        data_fill_params.update(
            {"client": client1, "mounting_dir": kernel_mounting_dir}
        )
        log.info(f"IO params:{data_fill_params}")
        fill_volume(data_fill_params)

        log.info("Create snapshot on Subvolume \n")
        snapshot = {
            "vol_name": default_fs,
            "subvol_name": subvol_name,
            "snap_name": "snap_max_clones",
            "group_name": group_name,
        }
        fs_util.create_snapshot(client1, **snapshot)

        log.info("Create Max Clones on Subvolume until pool is full\n")
        clonevol = {
            "vol_name": default_fs,
            "subvol_name": subvol_name,
            "snap_name": "snap_max_clones",
            "group_name": group_name,
            "target_group_name": group_name,
        }
        clone_cnt = 1
        rmclone_list = []
        used_percent = int(get_pool_usage(client1, default_fs))
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=test_timeout)

        while (used_percent < expected_pool_used) and (
            end_time > datetime.datetime.now()
        ):
            try:
                clonevol_name = f"clonevol_max_clones_{clone_cnt}"
                clonevol_tmp = clonevol
                clonevol_tmp.update({"target_subvol_name": clonevol_name})
                rmclone = {
                    "vol_name": default_fs,
                    "subvol_name": clonevol_name,
                    "group_name": group_name,
                }
                rmclone_list.append(rmclone)
                fs_util.create_clone(
                    client1, **clonevol_tmp, validate=True, timeout=cmd_timeout
                )
                wait_for_clone(client1, clonevol_tmp, timeout=cmd_timeout)
                log.info(f"Clone created : {clonevol_name}")

                used_percent = int(get_pool_usage(client1, default_fs))
                log.info(f"Current usage on Pool {default_fs} is {used_percent} \n")
                log.info(f"Total Clones created on a FS sub volume is {clone_cnt} \n")
                clone_cnt = clone_cnt + 1

            except CommandFailed:
                log.info("Clone creation on a FS sub volume failed")
                break

        if used_percent >= expected_pool_used:
            log.info(f"Max Clones allowed on a FS sub volume level is {clone_cnt} \n")
        elif end_time > datetime.datetime.now():
            raise AssertionError(
                f"TIMEOUT: Max clones creation failed,pool usage - {used_percent}%,Test time - {test_timeout}secs\n"
            )

        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean Up in progess")
        try:
            for clone_vol in rmclone_list:
                fs_util.remove_subvolume(client1, **clone_vol, timeout=cmd_timeout)
        except Exception as e:
            log.info(e)
        fs_util.remove_snapshot(
            client1, **snapshot, validate=False, check_ec=False, timeout=cmd_timeout
        )
        fs_util.client_clean_up(
            "umount",
            kernel_clients=[client1],
            mounting_dir=kernel_mounting_dir,
        )
        fs_util.client_clean_up(
            "umount", fuse_clients=[client1], mounting_dir=fuse_mounting_dir
        )
        fs_util.remove_subvolume(client1, **subvolume, validate=False, check_ec=False)
        fs_util.remove_subvolumegroup(client1, **subvolumegroup, force=True)
