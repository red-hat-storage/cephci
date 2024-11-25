import datetime
import json
import os
import random
import string
import time
import traceback
from distutils.version import LooseVersion
from time import sleep

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.snapshot_clone.cephfs_snap_utils import SnapUtils
from utility.log import Log
from utility.retry import retry
from utility.utils import get_ceph_version_from_cluster

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test Cases Covered :
    CEPH-83600860 - Verify Kernel and FUSE Mount Behavior with Snapshot Scheduling and MDS Restarts

    Test Steps :
    1. Create a user and mount the CephFS file system using both FUSE and Kernel clients.
    2. Continuously write data to the mounted file system.
    3. Enable snapshot scheduling on the root directory, setting the schedule for every 2 minutes.
    4. Unmount the file system and remount it using the same user.
    5. Run the stat command on a file located inside the snapshot directory, both on the FUSE and Kernel mounts.
    6. Restart the active MDS.
    7. Repeat the unmount and remount process using the same user.
    8. Run the stat command again on a file inside the snapshot directory on both the FUSE and Kernel mounts.

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
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        ceph_version = get_ceph_version_from_cluster(clients[0])
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

        default_fs = "cephfs_snap_3" if not erasure else "cephfs_snap_3_ec"
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )

        log.info("Enable Snap Schedule")
        client1.exec_command(sudo=True, cmd="ceph mgr module enable snap_schedule")
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
        log.info("Run IO")
        client1.exec_command(
            sudo=True,
            cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 400 "
            f"--files 100 --files-per-dir 10 --dirs-per-dir 2 --top "
            f"{kernel_mounting_dir_1}",
            long_running=True,
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
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=300)
        ceph_healthy = 0
        while (datetime.datetime.now() < end_time) and (ceph_healthy == 0):
            try:
                fs_util.get_ceph_health_status(client1)
                ceph_healthy = 1
            except Exception as ex:
                log.info(ex)
                log.info("Wait for few secs and recheck ceph status")
                time.sleep(5)
        if ceph_healthy == 0:
            assert False, "Ceph remains unhealthy even after wait for 300secs"
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
        snap_util.validate_snap_schedule(
            client1,
            f"{fuse_mounting_dir_1}dir_fuse/",
            sched_val=f"1{m_granularity}",
        )
        verify_snap_schedule(
            client1,
            f"{kernel_mounting_dir_1}dir_kernel/",
            fs_name=default_fs,
            schedule=f"1{m_granularity}",
        )
        snap_util.validate_snap_schedule(
            client1,
            f"{kernel_mounting_dir_1}dir_kernel/",
            sched_val=f"1{m_granularity}",
        )
        for i in range(1, 5):
            client1.exec_command(
                sudo=True, cmd=f"umount {fuse_mounting_dir_1} {kernel_mounting_dir_1}"
            )
            fs_util.fuse_mount(
                [client1],
                fuse_mounting_dir_1,
                extra_params=f"--client_fs {default_fs}",
            )
            retry_mount(
                [client1],
                kernel_mounting_dir_1,
                ",".join(mon_node_ips),
                extra_params=f",fs={default_fs}",
            )
            client1.exec_command(
                sudo=True, cmd=f"stat {fuse_mounting_dir_1}dir_fuse/.snap/"
            )
            client1.exec_command(
                sudo=True, cmd=f"stat {kernel_mounting_dir_1}dir_kernel/.snap/"
            )

            fs_util.get_active_mdss(client1, default_fs)
            mds = fs_util.get_active_mdss(client1, default_fs)
            mds_active_node = ceph_cluster.get_node_by_hostname(mds[0].split(".")[1])
            log.info("Get the mdthresh_evicted ceph mds perf cntr value")
            fs_util.reboot_node_v1(mds_active_node)
            fs_util.check_active_mds_count(client1, default_fs, 1)

            out, rc = client1.exec_command(
                sudo=True, cmd=f"ls {fuse_mounting_dir_1}dir_fuse/.snap/"
            )
            log.info(f"Info of ls command :{out}")
            out, rc = client1.exec_command(
                sudo=True, cmd=f"ls {kernel_mounting_dir_1}dir_kernel/.snap/"
            )
            log.info(f"Info of ls command :{out}")

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
