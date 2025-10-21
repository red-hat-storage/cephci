import json
import os
import random
import string
import time
import traceback
from time import sleep

from looseversion import LooseVersion

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from tests.cephfs.lib.cephfs_refinode_utils import RefInodeUtils
from tests.cephfs.snapshot_clone.cephfs_snap_utils import SnapUtils
from utility.log import Log
from utility.retry import retry
from utility.utils import get_ceph_version_from_cluster

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test Cases Covered :
    CEPH-83575569 - Enable snap-schedule on file system and verify if snapshots are created

    Pre-requisites :
    1. We need atleast one client node to execute this test case
    2. creats fs volume create cephfs if the volume is not there
    3. ceph fs subvolumegroup create <vol_name> <group_name> --pool_layout <data_pool_name>
        Ex : ceph fs subvolumegroup create cephfs subvolgroup_snap_schedule
    4. ceph fs subvolume create <vol_name> <subvol_name> [--size <size_in_bytes>] [--group_name <subvol_group_name>]
       [--pool_layout <data_pool_name>] [--uid <uid>] [--gid <gid>] [--mode <octal_mode>]  [--namespace-isolated]
       Ex: ceph fs subvolume create cephfs subvol_2 --size 5368706371 --group_name subvolgroup_
    5. Create Data on the subvolume
        Ex:  python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 400 --files
            100 --files-per-dir 10 --dirs-per-dir 2 --top /mnt/cephfs_fuse1baxgbpaia_1/
    6. Enable snpa-schedule
        ceph fs snap-schedule add / 1h
        ceph fs snap-schedule retention add / h 14
        ceph fs snap-schedule activate /
        ceph fs snap-schedule status /

    Clean Up:
    1. Del all the snapshots created
    2. Del Subvolumes
    3. Del SubvolumeGroups
    4. Deactivate and remove sanp_Schedule
    5. Remove FS
    """
    try:
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        snap_util = SnapUtils(ceph_cluster)
        cephfs_common_utils = CephFSCommonUtils(ceph_cluster)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        ceph_version = get_ceph_version_from_cluster(clients[0])
        ref_inode_utils = RefInodeUtils(ceph_cluster)
        if len(clients) < 1:
            log.info(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1
        client1 = clients[0]
        log.info("Setting OSD config to avoid impact on snap-schedule due to Scrubbing")
        osd_cmds = {
            "osd_stats_update_period_not_scrubbing": 2,
            "osd_stats_update_period_scrubbing": 2,
            "osd_pg_stat_report_interval_max": 5,
        }
        for osd_cmd in osd_cmds:
            cmd = f"ceph config set osd {osd_cmd} {osd_cmds[osd_cmd]}"
            client1.exec_command(sudo=True, cmd=cmd, check_ec=False)
        log.info("Verify OSD config")
        for osd_cmd in osd_cmds:
            cmd = f"ceph config get osd {osd_cmd}"
            out, _ = client1.exec_command(sudo=True, cmd=cmd, check_ec=False)
            log.info(out)
            if str(osd_cmds[osd_cmd]) not in str(out):
                log.warning(
                    f"OSD config {osd_cmd} couldn't be set to {osd_cmds[osd_cmd]}"
                )

        time.sleep(10)
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        log.info("checking Pre-requisites")

        default_fs = "cephfs_snap_1" if not erasure else "cephfs_snap_1_ec"
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )

        log.info("Enable Snap Schedule")
        snap_util.enable_snap_schedule(client1)
        fs_details = fs_util.get_fs_info(client1, fs_name=default_fs)
        if not fs_details:
            fs_util.create_fs(client1, default_fs)
        subvolumegroup_list = [
            {"vol_name": default_fs, "group_name": "subvolgroup_snap_schedule"},
        ]
        for subvolumegroup in subvolumegroup_list:
            fs_util.create_subvolumegroup(client1, **subvolumegroup)
        log.info("Kernel mount")
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        mon_node_ips = fs_util.get_mon_node_ips()
        retry_mount = retry(CommandFailed, tries=3, delay=30)(fs_util.kernel_mount)
        retry_mount(
            [client1],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            extra_params=f",fs={default_fs}",
        )

        if LooseVersion(ceph_version) > LooseVersion("20.1.1"):
            ref_inode_utils.allow_referent_inode_feature_enablement(
                client1, default_fs, enable=True
            )
            log.info("Ceph version >= 20.1 detected. Creating referent inode setup.")
            ref_base_path = kernel_mounting_dir_1
            ref_inode_utils.create_directories(
                client1, ref_base_path, ["dirA", "dirA/dirB"]
            )
            ref_inode_utils.create_file_with_content(
                client1, "%s/dirA/fileA1" % ref_base_path, "data in file A1"
            )
            ref_inode_utils.create_file_with_content(
                client1, "%s/dirA/dirB/fileB1" % ref_base_path, "data in file B1"
            )
            main_file = "%s/file1" % ref_base_path
            ref_inode_utils.create_file_with_content(
                client1, main_file, "original file content"
            )
            hardlinks = [
                "%s/file1_hardlink1" % ref_base_path,
                "%s/file1_hardlink2" % ref_base_path,
                "%s/dirA/file1_hardlink_in_dirA" % ref_base_path,
            ]
            for hl in hardlinks:
                ref_inode_utils.create_hardlink_and_validate(
                    client1,
                    fs_util,
                    main_file,
                    hl,
                    "cephfs.%s.data" % default_fs,
                    default_fs,
                )

        log.info("Run IO")
        client1.exec_command(
            sudo=True,
            cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 400 "
            f"--files 100 --files-per-dir 10 --dirs-per-dir 2 --top "
            f"{kernel_mounting_dir_1}",
            timeout=3600,
        )
        client1.exec_command(
            sudo=True, cmd=f"mkdir -p {kernel_mounting_dir_1}/dir_kernel"
        )
        client1.exec_command(
            sudo=True, cmd=f"mkdir -p {kernel_mounting_dir_1}/snap_schedule"
        )
        log.info("Fuse mount")
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
        fs_util.fuse_mount(
            [client1],
            fuse_mounting_dir_1,
            extra_params=f"--client_fs {default_fs}",
        )
        client1.exec_command(sudo=True, cmd=f"mkdir -p {fuse_mounting_dir_1}/dir_fuse")
        sanp_schedule_list = ["/dir_kernel", "/dir_fuse"]
        m_granularity = (
            "m" if LooseVersion(ceph_version) >= LooseVersion("17.2.6") else "M"
        )
        log.info("Verify Ceph Status is healthy before starting test")
        wait_time_secs = 300
        if cephfs_common_utils.wait_for_healthy_ceph(client1, wait_time_secs):
            raise CommandFailed(
                f"Cluster health is not OK even after waiting for {wait_time_secs}sec",
            )

        commands = [
            f"ceph fs subvolume ls {default_fs}",
            "ceph config set mgr mgr/snap_schedule/allow_m_granularity true",
            f"ceph fs snap-schedule add path 1{m_granularity} --fs {default_fs}",
        ]
        modified_commands = [
            cmd.replace("path", item) for item in sanp_schedule_list for cmd in commands
        ]

        for cmd in modified_commands:
            client1.exec_command(sudo=True, cmd=cmd)
        sleep(300)
        verify_snap_schedule(
            client1,
            f"{fuse_mounting_dir_1}dir_fuse/",
            fs_name=default_fs,
            schedule=f"1{m_granularity}",
        )
        verify_snap_schedule(
            client1,
            f"{kernel_mounting_dir_1}dir_kernel/",
            fs_name=default_fs,
            schedule=f"1{m_granularity}",
        )

        if LooseVersion(ceph_version) > LooseVersion("20.1.1"):
            ref_inode_utils.unlink_hardlinks(
                client1, default_fs, main_file, ref_base_path
            )
            ref_inode_utils.allow_referent_inode_feature_enablement(
                client1, default_fs, enable=False
            )

        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean Up in progess")
        commands = [
            f"ceph fs snap-schedule deactivate path --fs {default_fs}",
            f"ceph fs snap-schedule remove path --fs {default_fs}",
            "ceph config set mgr mgr/snap_schedule/allow_m_granularity false",
        ]
        modified_commands = [
            cmd.replace("path", item) for item in sanp_schedule_list for cmd in commands
        ]
        for cmd in modified_commands:
            client1.exec_command(sudo=True, cmd=cmd)
        commands = [
            "ceph config set mon mon_allow_pool_delete true",
            f"ceph fs volume rm {default_fs} --yes-i-really-mean-it",
        ]
        for cmd in commands:
            client1.exec_command(sudo=True, cmd=cmd)


def verify_snap_schedule(client, path, fs_name, schedule):
    out, rc = client.exec_command(sudo=True, cmd=f"ls -lrt {path}.snap/ | wc -l")
    log.info(out)
    log.info(int(out))
    if not (int(out) >= 4):
        raise CommandFailed("It has not created the snaps")
    schedule_path = os.path.basename(os.path.normpath(path))
    out, rc = client.exec_command(
        sudo=True,
        cmd=f"ceph fs snap-schedule list /{schedule_path} --recursive --fs {fs_name}",
    )
    log.info("snap-schedule list")
    log.info(out)

    if schedule not in out:
        raise CommandFailed("Snap Schedule is not getting listed")
    out, rc = client.exec_command(
        sudo=True,
        cmd=f"ceph fs snap-schedule status /{schedule_path} -f json --fs {fs_name}",
    )
    log.info("snap-schedule Status")
    log.info(out)
    schedule_ls = json.loads(out)
    log.info(schedule_ls[0]["schedule"])
    if schedule_ls[0]["schedule"] != schedule:
        raise CommandFailed("Snap Schedule is not returning status")
