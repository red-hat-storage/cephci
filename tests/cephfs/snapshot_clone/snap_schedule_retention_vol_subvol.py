import datetime
import json
import random
import re
import secrets
import string
import time
import traceback
from distutils.version import LooseVersion

import dateutil.parser as parser

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsv1
from tests.cephfs.cephfs_volume_management import wait_for_process
from tests.cephfs.snapshot_clone.cephfs_snap_utils import SnapUtils
from utility.log import Log
from utility.utils import get_ceph_version_from_cluster

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Workflows Covered : Validate Snapshot Schedule, Retention feature works on both volumes and subvolumes.
    Verify .snap across kernel, fuse and nfs mounts(future-ready) for snaps created by schedule.

    Type - Functional
    Workflow1 - snap_sched_vol: Verify Snapshot schedule on volume.Validate snaphots in .snap across
    mount types - kernel,fuse,nfs
    Steps:
    1. Create Snapshot schedule on ceph FS volume. Verify snapshot schedule created and active
    2. Add data to volume and wait for scheduled snapshots to be created
    3. Verify scheduled snapshtos are getting created.
    4. Validate snapshot schedule by checking if snapshots are created as per schedule.
    5. Verify snap list across all mount types - kernel,nfs,fuse
    Workflow2 - snap_sched_subvol: Verify Snapshot schedule on subvolume.Validate snaphots in .snap across
    mount types - kernel,fuse,nfs
    Steps : Repeat workflow1 steps on subvolume
    Workflow3 - snap_retention_vol: Verify Snapshot Retention on volume
    Steps:
    1. Create Snapshot schedule on ceph FS volume. Verify snapshot schedule created and active
    2. Create Snapshot retention policy on volume. Verify retention status is active.
    3. Add data to volume and wait for scheduled snapshots to be created
    4. Validate scheduled snapshots retained are as per retention policy.
    Workflow4 - snap_retention_subvol : Verify Snapshot Retention on subvolume
    Steps: Repeat workflow3 steps on subvolume.

    Type - Longevity
    (As high run time is acceptible in longevity setup, hourly schedule validation can be done. This test can
    be executed in parallel to scale tests in cephfs_scale suite on Baremetal.)
    Workflow5 - snap_sched_validate: Verify Snapshot schedule and Retention with hourly schedule.
    Workflow6 - snap_sched_snapcount_validate: Verify Snapshot schedule and retention for default snapshot count 50,
      non-default value say '75' and
    upto 'mds_max_snaps_per_dir' i.e., 100(default), 150(non-default). Verify no traceback with a snapshot creation
    when limit is reached. Verify with both manual and schedule snapshot creation(Add new minutely schedule to
    validate this)

    Type - Negative
    Workflow6 - snap_retention_service_restart: Verify Snapshot retention works even after service restarts.
    Verify for mgr, mon and mds.
    Workflow7 - snap_sched_non_existing_path: Verify Snapshot schedule can be created for non-existing path.
    After the start-time is hit for the
    schedule, the snap-schedule module should have deactivated the schedule without throwing a traceback error.
    The snap-scheule module should continue to remain responsive and a log message should be seen in the logs
    about deactivating the schedule.

    Type - Systemic
    Workflow8 - snap_sched_multi_fs: Create multiFS(3), Create snap-schedules across all FS except first one
    from the list obtained
    with ceph fs ls. Create snap-schedule with and without specifying FS name and validate behaviour, without
    FS name it should generate error. Remove snap-schedule with and without FS name and validate behaviour.
    Repeat for all subcommands - retention add, retention remove, status,activate,deactivate

    Note : For each workflow except 5, all minutely, hourly, daily, weekly schedules will be applied and
    verified with status, but only minutely schedule is validated due to run time limitation.

    Clean Up:
    1. Del all the snapshots created
    2. Del Subvolumes
    3. Del SubvolumeGroups
    4. Deactivate and remove Snap_Schedule
    5. Remove FS

    """
    try:
        fs_util_v1 = FsUtilsv1(ceph_cluster)
        snap_util = SnapUtils(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        mgr_node = ceph_cluster.get_ceph_objects("mgr")[0]

        build = config.get("build", config.get("rhbuild"))
        fs_util_v1.prepare_clients(clients, build)
        fs_util_v1.auth_list(clients)
        multi_fs = 0
        log.info("checking Pre-requisites")
        if len(clients) < 1:
            log.info(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1
        default_fs = "cephfs"
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        nfs_server = nfs_servers[0].node.hostname
        nfs_name = "cephfs-nfs"
        client1 = clients[0]
        client1.exec_command(
            sudo=True, cmd=f"ceph nfs cluster create {nfs_name} {nfs_server}"
        )
        if wait_for_process(client=client1, process_name=nfs_name, ispresent=True):
            log.info("ceph nfs cluster created successfully")
        else:
            raise CommandFailed("Failed to create nfs cluster")
        nfs_export_name = "/export_" + "".join(
            secrets.choice(string.digits) for i in range(3)
        )
        snap_util.allow_minutely_schedule(client1, allow=True)
        fs_details = fs_util_v1.get_fs_info(client1, fs_name=default_fs)
        if not fs_details:
            fs_util_v1.create_fs(client1, default_fs)

        test_name_type = config.get("test_name", "all_tests")
        test_functional = [
            "snap_sched_vol",
            "snap_sched_subvol",
            "snap_retention_vol",
            "snap_retention_subvol",
        ]
        test_negative = [
            "snap_retention_service_restart",
            "snap_sched_non_existing_path",
        ]
        test_longevity = [
            "snap_retention_count_validate_vol",
            "snap_retention_count_validate_subvol",
        ]
        test_systemic = ["snap_sched_multi_fs"]
        test_name_all = test_functional + test_negative + test_longevity + test_systemic

        test_dict = {
            "all_tests": test_name_all,
            "functional": test_functional,
            "longevity": test_longevity,
            "systemic": test_systemic,
            "negative": test_negative,
        }
        if test_name_type in test_dict:
            test_list = test_dict[test_name_type]
        else:
            test_list = [test_name_type]

        export_created = 0
        snap_test_params = {
            "ceph_cluster": ceph_cluster,
            "fs_name": default_fs,
            "nfs_export_name": nfs_export_name,
            "nfs_server": nfs_server,
            "export_created": export_created,
            "nfs_name": nfs_name,
            "fs_util": fs_util_v1,
            "snap_util": snap_util,
            "client": client1,
            "mgr_node": mgr_node,
        }
        for test_case_name in test_list:
            log.info(
                f"\n\n                                   ============ {test_case_name} ============ \n"
            )
            snap_test_params.update({"test_case": test_case_name})
            if "multi_fs" in test_case_name:
                log.info("Verify that setup has atleast 2 Ceph FS")
                total_fs = fs_util_v1.get_fs_details(client1)
                if len(total_fs) == 1:
                    log.info("We need atleast two ceph FS to perform this test")
                    for i in range(1, 3):
                        out, rc = client1.exec_command(
                            sudo=True, cmd="ceph orch ps --daemon_type mds -f json"
                        )
                        daemon_ls_before = json.loads(out)
                        daemon_count_before = len(daemon_ls_before)
                        client1.exec_command(
                            sudo=True,
                            cmd=f"ceph fs volume create cephfs_{i}",
                            check_ec=False,
                        )
                        fs_util_v1.wait_for_mds_process(client1, f"cephfs_{i}")
                        out_after, rc = client1.exec_command(
                            sudo=True, cmd="ceph orch ps --daemon_type mds -f json"
                        )
                        daemon_ls_after = json.loads(out_after)
                        daemon_count_after = len(daemon_ls_after)
                        assert daemon_count_after > daemon_count_before, (
                            f"daemon count not increased after creating FS,daemon count before : {daemon_count_before}"
                            f"after:{daemon_count_after}"
                        )
                        multi_fs = 1
            log.info(
                f"Verify Ceph Status is healthy before starting test {test_case_name}"
            )
            ceph_healthy = 0
            end_time = datetime.datetime.now() + datetime.timedelta(seconds=60)
            while (datetime.datetime.now() < end_time) and (ceph_healthy == 0):
                try:
                    fs_util_v1.get_ceph_health_status(client1)
                    ceph_healthy = 1
                except Exception as ex:
                    log.info(ex)
                    log.info("Wait for few secs and recheck ceph status")
                    time.sleep(5)
            if ceph_healthy == 0:
                assert False, "Ceph remains unhealthy even after wait for 60secs"
            cleanup_params = run_snap_test(snap_test_params)
            log.info(f"post_test_params:{cleanup_params}")
            snap_test_params["export_created"] = cleanup_params["export_created"]
            if cleanup_params["test_status"] == 1:
                assert False, f"Test {test_case_name} failed"
            else:
                log.info(f"Test {test_case_name} passed \n")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean Up in progess")
        snap_util.allow_minutely_schedule(client1, allow=False)
        client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs export delete {nfs_name} {nfs_export_name}",
            check_ec=False,
        )
        client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs cluster delete {nfs_name}",
            check_ec=False,
        )
        if int(multi_fs) == 1:
            client1.exec_command(
                sudo=True,
                cmd="ceph config set mon mon_allow_pool_delete true",
                check_ec=False,
            )
            [fs_util_v1.remove_fs(client1, f"cephfs_{i}") for i in range(1, 3)]
            client1.exec_command(
                sudo=True,
                cmd="ceph config set mon mon_allow_pool_delete false",
                check_ec=False,
            )


def run_snap_test(snap_test_params):
    test_case_name = snap_test_params["test_case"]
    # Create subvolume and group
    if "subvol" in snap_test_params.get("test_case"):
        subvolumegroup = {
            "vol_name": snap_test_params["fs_name"],
            "group_name": "subvolgroup_snap_schedule",
        }
        dir_suffix = "".join(
            [random.choice(string.ascii_lowercase + string.digits) for _ in range(3)]
        )
        subvolume = {
            "vol_name": snap_test_params["fs_name"],
            "subvol_name": f"snap_subvolume_{dir_suffix}",
            "group_name": "subvolgroup_snap_schedule",
            "size": "6442450944",
        }
        snap_test_params["fs_util"].create_subvolumegroup(
            snap_test_params["client"], **subvolumegroup
        )
        time.sleep(30)
        snap_test_params["fs_util"].create_subvolume(
            snap_test_params["client"], **subvolume
        )
        snap_test_params["subvol_name"] = subvolume["subvol_name"]
        snap_test_params["group_name"] = subvolumegroup["group_name"]

    # Call test case modules
    if "snap_sched_vol" in test_case_name or "snap_sched_subvol" in test_case_name:
        post_test_params = snap_sched_test(snap_test_params)
    elif (
        "snap_retention_vol" in test_case_name
        or "snap_retention_subvol" in test_case_name
    ):
        post_test_params = snap_retention_test(snap_test_params)
    elif "snap_retention_service_restart" in test_case_name:
        post_test_params = snap_retention_service_restart(snap_test_params)
    elif "snap_retention_count_validate" in test_case_name:
        post_test_params = snap_retention_count_validate(snap_test_params)
    elif "snap_sched_multi_fs" in test_case_name:
        post_test_params = snap_sched_multi_fs(snap_test_params)
    elif "non_existing_path" in test_case_name:
        post_test_params = snap_sched_non_existing_path(snap_test_params)
        log.info(f"post_test_params:{post_test_params}")
    # Cleanup subvolume and group
    if "subvol" in snap_test_params.get("test_case"):
        cmd = f"ceph fs subvolume rm {snap_test_params['fs_name']} {snap_test_params['subvol_name']} "
        cmd += f"{snap_test_params['group_name']}"
        snap_test_params["client"].exec_command(
            sudo=True,
            cmd=cmd,
            check_ec=False,
        )
        snap_test_params["client"].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolumegroup rm {snap_test_params['fs_name']} {snap_test_params['group_name']}",
            check_ec=False,
        )
    return post_test_params


def snap_sched_test(snap_test_params):
    snap_util = snap_test_params["snap_util"]
    fs_util = snap_test_params["fs_util"]
    client = snap_test_params["client"]
    snap_util.enable_snap_schedule(client)
    ceph_version = get_ceph_version_from_cluster(client)
    snap_test_params["path"] = "/"
    if "subvol" in snap_test_params.get("test_case"):
        cmd = f"ceph fs subvolume getpath {snap_test_params['fs_name']} {snap_test_params.get('subvol_name')} "
        cmd += f"{snap_test_params.get('group_name')}"
        subvol_path, rc = snap_test_params["client"].exec_command(
            sudo=True,
            cmd=cmd,
        )
        snap_test_params["path"] = f"{subvol_path.strip()}/.."
    snap_test_params["validate"] = True
    m_granularity = "m" if LooseVersion(ceph_version) >= LooseVersion("17.2.6") else "M"
    sched_list = (
        ["2m", "1h", "1d", "1w"]
        if LooseVersion(ceph_version) >= LooseVersion("17.2.6")
        else ["2M", "1h", "1d", "1w"]
    )
    mnt_list = ["kernel", "fuse", "nfs"]
    test_fail = 0
    mnt_paths = {}
    snap_list_type = {}
    post_test_params = {}
    for sched_val in sched_list:
        log.info(
            f"Running snapshot schedule test workflow for schedule value {sched_val}"
        )
        snap_test_params["sched"] = sched_val
        snap_test_params["start_time"] = get_iso_time(client)
        if snap_util.create_snap_schedule(snap_test_params) == 1:
            log.info("Snapshot schedule creation/verification failed")
            test_fail = 1
        if m_granularity in sched_val:
            sched_list = re.split(r"(\d+)", sched_val)
            duration_min = int(sched_list[1]) * 3
            for mnt_type in mnt_list:
                path, post_test_params["export_created"] = fs_util.mount_ceph(
                    mnt_type, snap_test_params
                )
                mnt_paths[mnt_type] = path
            snap_path = f"{mnt_paths['nfs']}"
            io_path = f"{mnt_paths['kernel']}/"
            deactivate_path = snap_test_params["path"]
            if "subvol" in snap_test_params.get("test_case"):
                snap_path = f"{mnt_paths['nfs']}{snap_test_params['path']}"
                io_path = f"{mnt_paths['kernel']}{subvol_path.strip()}/"
                deactivate_path = f"{subvol_path.strip()}/.."
            log.info(f"add sched data params : {client} {io_path} {duration_min}")
            fs_util.run_ios_V1(
                snap_test_params["client"], io_path, run_time=duration_min
            )
            if snap_util.validate_snap_schedule(client, snap_path, sched_val):
                log.info("Snapshot schedule validation failed")
                test_fail = 1
            snap_util.deactivate_snap_schedule(
                client,
                deactivate_path,
                sched_val=sched_val,
                fs_name=snap_test_params.get("fs_name", "cephfs"),
            )
            time.sleep(10)
            log.info(
                "Verify if scheduled snaps in .snap are same across mount types - kernel,fuse and nfs"
            )
            for mnt_type in mnt_list:
                snap_path = mnt_paths[mnt_type]
                if "subvol" in snap_test_params.get("test_case"):
                    snap_path = f"{mnt_paths[mnt_type]}{subvol_path.strip()}/.."
                snap_list_type[mnt_type] = snap_util.get_scheduled_snapshots(
                    client, snap_path
                )
                if len(snap_list_type[mnt_type]) == 0:
                    log.info(
                        f"No scheduled snapshots listed in .snap - mount_type:{mnt_type},mount_path:{snap_path}"
                    )
                    test_fail = 1
            first_value = list(snap_list_type.values())[0]
            all_equal = all(value == first_value for value in snap_list_type.values())
            if all_equal is False:
                log.info(
                    "Scheduled snapshot list is not same across mount points - kernel, fuse, nfs"
                )
                test_fail = 1
    log.info(f"Performing test {snap_test_params['test_case']} cleanup")
    snap_util.deactivate_snap_schedule(
        snap_test_params["client"],
        deactivate_path,
        fs_name=snap_test_params.get("fs_name", "cephfs"),
    )
    schedule_path = snap_test_params["path"]
    if "subvol" in snap_test_params.get("test_case"):
        schedule_path = f"{subvol_path.strip()}/.."
    post_test_params["test_status"] = test_fail
    snap_util.remove_snap_schedule(
        client, schedule_path, fs_name=snap_test_params.get("fs_name", "cephfs")
    )

    if "subvol" in snap_test_params.get("test_case"):
        subvol = snap_test_params["subvol_name"]
        group = snap_test_params["group_name"]
        snap_util.sched_snap_cleanup(client, io_path, subvol, group)
    else:
        snap_util.sched_snap_cleanup(client, io_path)
    umount_all(mnt_paths, snap_test_params)
    return post_test_params


def snap_retention_test(snap_test_params):
    client = snap_test_params["client"]
    snap_util = snap_test_params["snap_util"]
    fs_util = snap_test_params["fs_util"]
    snap_util.enable_snap_schedule(client)
    ceph_version = get_ceph_version_from_cluster(client)
    snap_test_params["validate"] = True
    snap_test_params["path"] = "/"
    m_granularity = "m" if LooseVersion(ceph_version) >= LooseVersion("17.2.6") else "M"
    sched_list = (
        ["2m", "1h", "7d", "4w"]
        if LooseVersion(ceph_version) >= LooseVersion("17.2.6")
        else ["2M", "1h", "7d", "4w"]
    )
    snap_test_params["retention"] = (
        "3m1h5d4w"
        if LooseVersion(ceph_version) >= LooseVersion("17.2.6")
        else "3M1h5d4w"
    )
    test_fail = 0
    mnt_list = ["kernel", "fuse", "nfs"]
    mnt_paths = {}
    post_test_params = {}
    if "subvol" in snap_test_params.get("test_case"):
        cmd = f"ceph fs subvolume getpath {snap_test_params['fs_name']} {snap_test_params.get('subvol_name')} "
        cmd += f"{snap_test_params.get('group_name')}"
        subvol_path, rc = snap_test_params["client"].exec_command(
            sudo=True,
            cmd=cmd,
        )
        snap_test_params["path"] = f"{subvol_path.strip()}/.."
    for sched_val in sched_list:
        if m_granularity in sched_val:
            sched_val_ret = sched_val
        snap_test_params["sched"] = sched_val
        snap_test_params["start_time"] = get_iso_time(client)
        snap_util.create_snap_schedule(snap_test_params)
    snap_util.create_snap_retention(snap_test_params)
    ret_list = re.split(r"([0-9]+[ A-Za-z]?)", snap_test_params["retention"])
    snap_util.remove_snap_retention(
        client,
        snap_test_params["path"],
        ret_val="1h",
        fs_name=snap_test_params.get("fs_name", "cephfs"),
    )
    snap_test_params["retention"] = (
        "3m5d4w" if LooseVersion(ceph_version) >= LooseVersion("17.2.6") else "3M5d4w"
    )
    m_granularity = "m" if LooseVersion(ceph_version) >= LooseVersion("17.2.6") else "M"
    for ret_item in ret_list:
        if m_granularity in ret_item:
            temp_list = re.split(r"(\d+)", ret_item)
            duration_min = int(temp_list[1]) * 2
            for mnt_type in mnt_list:
                path, post_test_params["export_created"] = fs_util.mount_ceph(
                    mnt_type, snap_test_params
                )
                mnt_paths[mnt_type] = path
            snap_path = f"{mnt_paths['nfs']}"
            io_path = f"{mnt_paths['kernel']}/"
            if "subvol" in snap_test_params.get("test_case"):
                snap_path = f"{mnt_paths['nfs']}{snap_test_params['path']}"
                io_path = f"{mnt_paths['kernel']}{subvol_path.strip()}/"
            log.info(f"add sched data params : {client} {io_path} {duration_min}")
            fs_util.run_ios_V1(client, io_path, run_time=duration_min)
            temp_list = re.split(r"(\d+)", sched_val_ret)
            wait_retention_check = int(temp_list[1]) * 60
            log.info(
                f"Wait for additional time {wait_retention_check}secs to validate retention"
            )
            time.sleep(wait_retention_check)
            if snap_util.validate_snap_retention(
                client, snap_path, snap_test_params["path"]
            ):
                test_fail = 1
    log.info(f"Performing test{snap_test_params['test_case']} cleanup")

    post_test_params["test_status"] = test_fail
    schedule_path = snap_test_params["path"]
    snap_util.deactivate_snap_schedule(
        client, schedule_path, fs_name=snap_test_params.get("fs_name", "cephfs")
    )
    snap_util.remove_snap_retention(
        client,
        snap_test_params["path"],
        ret_val=snap_test_params["retention"],
        fs_name=snap_test_params.get("fs_name", "cephfs"),
    )
    snap_util.remove_snap_schedule(
        client, schedule_path, fs_name=snap_test_params.get("fs_name", "cephfs")
    )

    if "subvol" in snap_test_params.get("test_case"):
        subvol = snap_test_params["subvol_name"]
        group = snap_test_params["group_name"]
        snap_util.sched_snap_cleanup(client, io_path, subvol, group)
    else:
        snap_util.sched_snap_cleanup(client, io_path)
    umount_all(mnt_paths, snap_test_params)
    return post_test_params


def snap_retention_count_validate(snap_test_params):
    """
    1. Read mds_max_snaps_per_dir default value(100), verify with no retention field, max scheduled snap count retained
       is this default value - 1 i.e., 99 , perform snap cleanup(remove retention value and delete sched snaps)
    2. Add retention count as 5n, verify only 5 snapshots are retained, perform snap cleanup
    3. Enable mgr debug logs, Note default value for mds_max_snaps_per_dir, Set mds_max_snaps_per_dir as 8, verify 8
       scheduled snaps are retained, try manual snap creation in same path, verify error, but no
       traceback or exception in mgr logs,perform snap clean up, set back default mds_max_snaps_per_dir value
    """
    client = snap_test_params["client"]
    mgr_node = snap_test_params["mgr_node"]
    snap_util = snap_test_params["snap_util"]
    fs_util = snap_test_params["fs_util"]
    snap_util.enable_snap_schedule(client)
    ceph_version = get_ceph_version_from_cluster(client)
    snap_test_params["validate"] = True
    snap_test_params["path"] = "/"
    sched_list = (
        ["1m"] if LooseVersion(ceph_version) >= LooseVersion("17.2.6") else ["1M"]
    )

    test_fail = 0
    mnt_list = ["kernel", "fuse", "nfs"]
    mnt_paths = {}
    post_test_params = {}
    if "subvol" in snap_test_params.get("test_case"):
        log.info(
            "BZ 2254477 - Need to wait for 10mins for snapshot rotation issue to be gone"
        )
        time.sleep(600)
        cmd = f"ceph fs subvolume getpath {snap_test_params['fs_name']} {snap_test_params.get('subvol_name')} "
        cmd += f"{snap_test_params.get('group_name')}"
        subvol_path, rc = snap_test_params["client"].exec_command(
            sudo=True,
            cmd=cmd,
        )
        snap_test_params["path"] = f"{subvol_path.strip()}/.."

    out, rc = client.exec_command(
        sudo=True, cmd="ceph config get mds mds_max_snaps_per_dir"
    )
    default_retention = int(out.strip()) - 1
    time.sleep(60)
    log.info(
        f"Test to verify that max scheduled snaps retained is mds_max_snaps_per_dir default {default_retention}"
    )
    snap_test_params["sched"] = sched_list[0]
    snap_test_params["start_time"] = get_iso_time(client)
    snap_util.create_snap_schedule(snap_test_params)

    snap_test_params["retention"] = default_retention
    duration_min = int(default_retention)
    for mnt_type in mnt_list:
        path, post_test_params["export_created"] = fs_util.mount_ceph(
            mnt_type, snap_test_params
        )
        mnt_paths[mnt_type] = path
    snap_path = f"{mnt_paths['fuse']}"
    io_path = f"{mnt_paths['kernel']}/"
    if "subvol" in snap_test_params.get("test_case"):
        snap_path = f"{mnt_paths['fuse']}{snap_test_params['path']}"
        io_path = f"{mnt_paths['kernel']}{subvol_path.strip()}/"
    log.info(f"add sched data params : {client} {io_path} {duration_min}")
    dd_params = {
        "bs": "10k",
        "count": 10,
    }
    smallfile_params = {
        "threads": 5,
        "file-size": 1024,
        "files": 5,
    }
    fs_util.run_ios_V1(
        client,
        io_path,
        run_time=duration_min,
        dd_params=dd_params,
        smallfile_params=smallfile_params,
    )
    wait_retention_check = 60
    log.info(
        f"Wait for additional time {wait_retention_check}secs to validate retention"
    )
    time.sleep(wait_retention_check)
    if snap_util.validate_snap_retention(client, snap_path, snap_test_params["path"]):
        test_fail = 1
        log.error(
            f"max scheduled snaps retained is NOT mds_max_snaps_per_dir default {default_retention}"
        )

    log.info("Perform schedule snaps cleanup")
    snap_util.sched_snap_cleanup(client, snap_path)
    snap_util.remove_snap_schedule(
        client,
        snap_test_params["path"],
        fs_name=snap_test_params.get("fs_name", "cephfs"),
    )

    log.info(
        "Verify if retention field is 5n, only 5 scheduled snapshots are retained,manual snap can be created"
    )
    snap_util.create_snap_schedule(snap_test_params)
    snap_test_params["retention"] = "5n"
    snap_util.create_snap_retention(snap_test_params)

    duration_min = 5
    log.info(f"add sched data params : {client} {io_path} {duration_min}")
    fs_util.run_ios_V1(client, io_path, run_time=duration_min)
    wait_retention_check = 60
    log.info(
        f"Wait for additional time {wait_retention_check}secs to validate retention"
    )
    time.sleep(wait_retention_check)
    if snap_util.validate_snap_retention(
        client, snap_path, snap_test_params["path"], ret_type="n"
    ):
        test_fail = 1
        log.error("A check to verify only 5 scheduled snaps retained has failed")

    log.info("Verify a manual snapshot can still be created")
    if "subvol" in snap_test_params.get("test_case"):
        snapshot = {
            "vol_name": snap_test_params["fs_name"],
            "subvol_name": snap_test_params["subvol_name"],
            "snap_name": "manual_test_snap",
            "group_name": snap_test_params["group_name"],
        }
        fs_util.create_snapshot(client, **snapshot)
        log.info("Manual snapshot creation suceeded")
    else:
        cmd = f"mkdir {snap_path}/.snap/manual_test_snap"
        out, rc = client.exec_command(sudo=True, cmd=cmd)
        log.info("Manual snapshot creation suceeded")
    snap_util.remove_snap_retention(
        client,
        snap_test_params["path"],
        ret_val=snap_test_params["retention"],
        fs_name=snap_test_params.get("fs_name", "cephfs"),
    )
    log.info("Perform schedule snaps cleanup")
    snap_util.sched_snap_cleanup(client, snap_path)
    snap_util.remove_snap_schedule(
        client,
        snap_test_params["path"],
        fs_name=snap_test_params.get("fs_name", "cephfs"),
    )
    out, rc = client.exec_command(
        sudo=True, cmd=f"rmdir {snap_path}/.snap/manual_test_snap"
    )
    log.info("Verify retention when mds_max_snaps_per_dir set to non-default value - 8")
    out, rc = client.exec_command(sudo=True, cmd="ceph config set mgr log_to_file true")

    log.info("Note default value for mds_max_snaps_per_dir")
    out, rc = client.exec_command(
        sudo=True, cmd="ceph config get mds mds_max_snaps_per_dir"
    )
    default_retention = int(out.strip())
    log.info(f"Default value for mds_max_snaps_per_dir : {default_retention}")
    log.info("Set mds_max_snaps_per_dir as 8")
    out, rc = client.exec_command(
        sudo=True, cmd="ceph config set mds mds_max_snaps_per_dir 8"
    )
    snap_util.create_snap_schedule(snap_test_params)
    log.info("Verify 8 scheduled snaps are retained")
    duration_min = 8
    log.info(f"add sched data params : {client} {io_path} {duration_min}")
    fs_util.run_ios_V1(client, io_path, run_time=duration_min)
    wait_retention_check = 60
    log.info(
        f"Wait for additional time {wait_retention_check}secs to validate retention"
    )
    time.sleep(wait_retention_check)
    if snap_util.validate_snap_retention(client, snap_path, snap_test_params["path"]):
        test_fail = 1
        log.error("A check to verify only 8 scheduled snaps retained, has failed")
    log.info("Note last entry in mgr logs")
    log.info("Fetch the FSID of the ceph cluster")
    out, _ = mgr_node.exec_command(sudo=True, cmd="cephadm ls")
    data = json.loads(out)
    fsid = data[0]["fsid"]
    cmd = f"cd /var/log/ceph/{fsid}/;ls *mgr*log"
    out, _ = mgr_node.exec_command(sudo=True, cmd=cmd)
    mgr_file = out.strip()
    cmd = f"cat /var/log/ceph/{fsid}/{mgr_file}|grep Traceback|wc -l"
    traceback_before, _ = mgr_node.exec_command(sudo=True, cmd=cmd)
    cmd = f"cat /var/log/ceph/{fsid}/{mgr_file}|grep Exception|wc -l"
    exception_before, _ = mgr_node.exec_command(sudo=True, cmd=cmd)
    log.info(
        f"Traceback info before: {traceback_before},Exception info : {exception_before}"
    )
    log.info("Try manual snap creation")
    try:
        if "subvol" in snap_test_params.get("test_case"):
            snapshot = {
                "vol_name": snap_test_params["fs_name"],
                "subvol_name": snap_test_params["subvol_name"],
                "snap_name": "manual_test_snap1",
                "group_name": snap_test_params["group_name"],
            }
            fs_util.create_snapshot(client, **snapshot)
        else:
            cmd = f"mkdir {snap_path}/.snap/manual_test_snap1"
            out, rc = client.exec_command(sudo=True, cmd=cmd)
    except Exception as e:
        log.info(e)
        if ("Too many links" in str(e)) or ("EMLINK" in str(e)):
            log.info("Manual snap creation failed as max limit reached")
        else:
            test_fail = 1
            log.error("Unexpected error during manual snap creation")
    cmd = f"cat /var/log/ceph/{fsid}/{mgr_file}|grep Traceback|wc -l"
    traceback_after, _ = mgr_node.exec_command(sudo=True, cmd=cmd)
    cmd = f"cat /var/log/ceph/{fsid}/{mgr_file}|grep Exception|wc -l"
    exception_after, _ = mgr_node.exec_command(sudo=True, cmd=cmd)

    log.info("Verify no exception or traceback in mgr logs since last entry")
    log.info(
        f"Traceback info after: {traceback_after},Exception info after: {exception_after}"
    )
    if (int(traceback_after) != int(traceback_before)) or (
        int(exception_after) != int(exception_before)
    ):
        log.error(
            "There seems traceback/exception during snapshot create, its not expected"
        )

    log.info("Reset mds_max_snaps_per_dir to default")
    out, rc = client.exec_command(
        sudo=True, cmd=f"ceph config set mds mds_max_snaps_per_dir {default_retention}"
    )

    log.info(f"Performing test{snap_test_params['test_case']} cleanup")
    snap_util.remove_snap_schedule(
        client,
        snap_test_params["path"],
        fs_name=snap_test_params.get("fs_name", "cephfs"),
    )

    if "subvol" in snap_test_params.get("test_case"):
        subvol = snap_test_params["subvol_name"]
        group = snap_test_params["group_name"]
        snap_util.sched_snap_cleanup(client, snap_path, subvol, group)
    else:
        snap_util.sched_snap_cleanup(client, snap_path)

    post_test_params["test_status"] = test_fail

    umount_all(mnt_paths, snap_test_params)

    return post_test_params


def snap_sched_multi_fs(snap_test_params):
    snap_util = snap_test_params["snap_util"]
    fs_util = snap_test_params["fs_util"]
    client = snap_test_params["client"]
    snap_util.enable_snap_schedule(client)
    ceph_version = get_ceph_version_from_cluster(client)
    snap_test_params["path"] = "/"

    snap_test_params["validate"] = True
    sched_list = (
        ["1m", "1h", "1d", "1w"]
        if LooseVersion(ceph_version) >= LooseVersion("17.2.6")
        else ["1M", "1h", "1d", "1w"]
    )

    test_fail = 0
    mnt_list = ["kernel", "fuse", "nfs"]
    mnt_paths = {}
    post_test_params = {}
    mnt_path_fs = []
    log.info("Perform ceph fs ls to get the list")
    total_fs = fs_util.get_fs_details(client)
    log.info(total_fs)
    log.info(
        "Verify fs-name is prompted when snap-schedule is tried to create without fs-name"
    )
    exp_err_str = (
        "filesystem argument is required when there is more than one file system"
    )
    try:
        out, rc = client.exec_command(sudo=True, cmd="ceph fs snap-schedule add / 2m")
        out, rc = client.exec_command(sudo=True, cmd="ceph fs snap-schedule remove /")
        test_fail = 1
        log.error(
            "BZ 2224060: snap-schedule create without fs-name passed but it was not expected"
        )

    except Exception as e:
        log.info(e)
        if exp_err_str in str(e):
            log.info(
                "fs-name is prompted when snap-schedule is tried to create without fs-name"
            )
        else:
            test_fail = 1
            log.error(
                "snap-schedule create without fs-name failed for unexpected error"
            )

    log.info("Create snap-schedule on all ceph FS except first one in list")
    fs_cnt = len(total_fs)
    fs_snap_path = {}
    for i in range(1, fs_cnt):
        snap_test_params["fs_name"] = total_fs[i]["name"]
        snap_test_params["sched"] = sched_list[0]
        snap_test_params["start_time"] = get_iso_time(client)

        sched_list_tmp = re.split(r"(\d+)", sched_list[0])
        duration_min = int(sched_list_tmp[1]) * 2
        for mnt_type in mnt_list:
            path, post_test_params["export_created"] = fs_util.mount_ceph(
                mnt_type, snap_test_params
            )
            mnt_paths[mnt_type] = path
            mnt_path_fs.append(path)
        fs_snap_path[total_fs[i]["name"]] = f"{mnt_paths['fuse']}"
        io_path = f"{mnt_paths['kernel']}/"

        for sched_val in sched_list:
            snap_test_params["sched"] = sched_val
            snap_util.create_snap_schedule(snap_test_params)
        log.info(f"add sched data params : {client} {io_path} {duration_min}")
        fs_util.run_ios_V1(snap_test_params["client"], io_path, run_time=duration_min)
        if snap_util.validate_snap_schedule(
            client, fs_snap_path[total_fs[i]["name"]], sched_list[0]
        ):
            log.error("Snapshot schedule validation failed")
            test_fail = 1

    log.info(
        "Verify fs-name is prompted when snap-schedule status is run without fs-name"
    )

    try:
        out, rc = client.exec_command(sudo=True, cmd="ceph fs snap-schedule status /")
        test_fail = 1
        log.error(
            "BZ 2224060: snap-schedule status without fs-name passed but it was not expected"
        )

    except Exception as e:
        log.info(e)
        if exp_err_str in str(e):
            log.info(
                "fs-name is prompted when snap-schedule status is tried without fs-name"
            )
        else:
            test_fail = 1
            log.error(
                "snap-schedule status without fs-name failed for unexpected error"
            )
    snap_test_params["fs_name"] = total_fs[1]["name"]
    log.info("Verify snap-schedule status with fs-name in multi-fs setup suceeds")
    client.exec_command(
        sudo=True,
        cmd=f"ceph fs snap-schedule status / --fs {snap_test_params['fs_name']}",
    )

    log.info(
        "Verify fs-name is prompted when snap-schedule retention add is run without fs-name"
    )
    try:
        out, rc = client.exec_command(
            sudo=True, cmd="ceph fs snap-schedule retention add / M 5"
        )
        test_fail = 1
        log.error(
            "BZ 2224060: snap-schedule retention add without fs-name passed but it was not expected"
        )

    except Exception as e:
        log.info(e)
        if exp_err_str in str(e):
            log.info(
                "fs-name is prompted when snap-schedule retention add is tried without fs-name"
            )
        else:
            test_fail = 1
            log.error(
                "snap-schedule retention add without fs-name failed for unexpected error"
            )
    log.info("Verify retention add with fs-name in multi-fs setup suceeds")
    snap_test_params["retention"] = (
        "3m1h5d4w"
        if LooseVersion(ceph_version) >= LooseVersion("17.2.6")
        else "3M1h5d4w"
    )
    snap_util.create_snap_retention(snap_test_params)

    log.info(
        "Verify fs-name is prompted when snap-schedule retention remove is run without fs-name"
    )
    try:
        ret_cmd = (
            f"ceph fs snap-schedule retention remove /  {snap_test_params['retention']}"
        )
        out, rc = client.exec_command(sudo=True, cmd=ret_cmd)
        test_fail = 1
        log.error(
            "BZ 2224060: snap-schedule retention remove without fs-name passed but it was not expected"
        )

    except Exception as e:
        log.info(e)
        if exp_err_str in str(e):
            log.info(
                "fs-name is prompted when snap-schedule retention remove is tried without fs-name"
            )
        else:
            test_fail = 1
            log.error(
                "snap-schedule retention remove without fs-name failed for unexpected error"
            )
    log.info("Verify retention remove with fs-name in multi-fs setup suceeds")
    snap_util.remove_snap_retention(
        client,
        snap_test_params["path"],
        ret_val=snap_test_params["retention"],
        fs_name=snap_test_params["fs_name"],
    )

    log.info(
        "Verify fs-name is prompted when snap-schedule deactivate is run without fs-name"
    )
    try:
        out, rc = client.exec_command(
            sudo=True, cmd="ceph fs snap-schedule deactivate /"
        )
        test_fail = 1
        log.error(
            "BZ 2224060: snap-schedule deactivate without fs-name passed but it was not expected"
        )

    except Exception as e:
        log.info(e)
        if exp_err_str in str(e):
            log.info(
                "fs-name is prompted when snap-schedule deactivate is tried without fs-name"
            )
        else:
            test_fail = 1
            log.error(
                "snap-schedule deactivate without fs-name failed for unexpected error"
            )
    log.info("Verify deactivate with fs-name in multi-fs setup suceeds")
    snap_util.deactivate_snap_schedule(
        client,
        snap_test_params["path"],
        fs_name=snap_test_params.get("fs_name", "cephfs"),
    )

    log.info(
        "Verify fs-name is prompted when snap-schedule activate is run without fs-name"
    )
    try:
        out, rc = client.exec_command(sudo=True, cmd="ceph fs snap-schedule activate /")
        test_fail = 1
        log.error(
            "BZ 2224060: snap-schedule activate without fs-name passed but it was not expected"
        )

    except Exception as e:
        log.info(e)
        if exp_err_str in str(e):
            log.info(
                "fs-name is prompted when snap-schedule activate is tried without fs-name"
            )
        else:
            test_fail = 1
            log.error(
                "snap-schedule activate without fs-name failed for unexpected error"
            )
    log.info("Verify snap-schedule activate with fs-name in multi-fs setup suceeds")
    snap_util.activate_snap_schedule(
        client, snap_test_params["path"], fs_name=snap_test_params["fs_name"]
    )

    log.info("Verify snap-schedule remove without fs-name gives an error")
    try:
        out, rc = client.exec_command(sudo=True, cmd="ceph fs snap-schedule remove /")
        test_fail = 1
        log.error("snap-schedule remove without fs-name passed but it was not expected")

    except Exception as e:
        log.info(e)
        if exp_err_str in str(e):
            log.info(
                "fs-name is prompted when snap-schedule remove is tried without fs-name"
            )
        else:
            test_fail = 1
            log.error(
                "BZ 2224060: snap-schedule remove without fs-name failed for unexpected error"
            )

    log.info("Verify snap-schedule remove with fs-name works")
    for i in range(1, fs_cnt):
        snap_util.remove_snap_schedule(
            client, snap_test_params["path"], fs_name=total_fs[i]["name"]
        )

    for i in range(1, fs_cnt):
        snap_util.sched_snap_cleanup(client, fs_snap_path[total_fs[i]["name"]])

    log.info(mnt_path_fs)
    for mnt_pt in mnt_path_fs:
        cmd = f"rm -rf {mnt_pt}/*file*"
        client.exec_command(sudo=True, cmd=cmd, timeout=1800)
        client.exec_command(sudo=True, cmd=f"umount {mnt_pt}")
    post_test_params["test_status"] = test_fail
    return post_test_params


def snap_sched_non_existing_path(snap_test_params):
    """Verify Snapshot schedule can be created for non-existing path.
    After the start-time is hit for the
    schedule, the snap-schedule module should have deactivated the schedule without throwing a traceback error.
    The snap-scheule module should continue to remain responsive and a log message should be seen in the logs
    about deactivating the schedule."""
    client = snap_test_params["client"]
    snap_util = snap_test_params["snap_util"]
    fs_util = snap_test_params["fs_util"]
    snap_util.enable_snap_schedule(client)
    ceph_version = get_ceph_version_from_cluster(client)
    snap_test_params["validate"] = True
    snap_test_params["path"] = "/non-existent"
    sched_list = (
        ["1m"] if LooseVersion(ceph_version) >= LooseVersion("17.2.6") else ["1M"]
    )
    snap_test_params["retention"] = (
        "3m" if LooseVersion(ceph_version) >= LooseVersion("17.2.6") else "3M"
    )
    snap_test_params["start_time"] = get_iso_time(client)
    snap_test_params["sched"] = sched_list[0]
    mgr_node = snap_test_params["mgr_node"]
    post_test_params = {}
    test_fail = 0
    out, rc = client.exec_command(sudo=True, cmd="ceph config set mgr log_to_file true")
    log.info("Create snap-schedule for non-existing path")
    snap_util.create_snap_schedule(snap_test_params)
    log.info("Verify schedule for non-existent path got deactivated")
    if snap_util.check_snap_sched_active(client, snap_test_params["path"], "False"):
        test_fail = 1
        log.error("Snap Schedule is still active for non-existent path")

    log.info("Verify message in mgr log for non-existing path")
    out, _ = mgr_node.exec_command(sudo=True, cmd="cephadm ls")
    data = json.loads(out)
    fsid = data[0]["fsid"]
    cmd = f"cd /var/log/ceph/{fsid}/;ls *mgr*log"
    out, _ = mgr_node.exec_command(sudo=True, cmd=cmd)
    mgr_file = out.strip()
    cmd = f'cat /var/log/ceph/{fsid}/{mgr_file}|grep "ERROR snap_schedule.fs.schedule_client" -A 5'
    path_err_msg, _ = mgr_node.exec_command(sudo=True, cmd=cmd)
    path_err_msg = path_err_msg.strip()
    exp_str = "cephfs.ObjectNotFound: error in mkdir"
    if exp_str in path_err_msg:
        log.info(f"Message for non-existing path is logged : {path_err_msg}")
    else:
        test_fail = 1
        log.error("Message not logged in mgr module")
    snap_util.remove_snap_schedule(
        client,
        snap_test_params["path"],
        fs_name=snap_test_params.get("fs_name", "cephfs"),
    )
    log.info("Create snap-schedule for existing path, verify it works")
    snap_test_params["path"] = "/"
    sched_list = ["1h"]
    snap_test_params["sched"] = sched_list[0]
    snap_util.create_snap_schedule(snap_test_params)
    time.sleep(10)
    log.info("Verify snap-schedule is still active after 10secs")
    if snap_util.check_snap_sched_active(client, snap_test_params["path"], "True"):
        test_fail = 1
        log.info("Snap Schedule is not active for existing path")

    log.info("Performing cleanup")
    snap_path, post_test_params["export_created"] = fs_util.mount_ceph(
        "fuse", snap_test_params
    )
    snap_util.remove_snap_schedule(
        client,
        snap_test_params["path"],
        fs_name=snap_test_params.get("fs_name", "cephfs"),
    )
    client.exec_command(sudo=True, cmd=f"umount {snap_path}")
    post_test_params["test_status"] = test_fail
    return post_test_params


def snap_retention_service_restart(snap_test_params):
    client = snap_test_params["client"]
    snap_util = snap_test_params["snap_util"]
    fs_util = snap_test_params["fs_util"]
    snap_util.enable_snap_schedule(client)
    ceph_version = get_ceph_version_from_cluster(client)
    snap_test_params["validate"] = True
    snap_test_params["path"] = "/"
    sched_list = (
        ["1m"] if LooseVersion(ceph_version) >= LooseVersion("17.2.6") else ["1M"]
    )
    snap_test_params["retention"] = (
        "3m" if LooseVersion(ceph_version) >= LooseVersion("17.2.6") else "3M"
    )
    svc_list = ["mgr", "mds", "mon"]
    test_fail = 0
    post_test_params = {}
    snap_test_params["sched"] = sched_list[0]
    snap_test_params["start_time"] = get_iso_time(client)
    snap_util.create_snap_schedule(snap_test_params)
    snap_util.create_snap_retention(snap_test_params)
    snap_path, post_test_params["export_created"] = fs_util.mount_ceph(
        "fuse", snap_test_params
    )
    log.info("Perform service restart and verify Snapshot Retention policy is active")
    for svc_name in svc_list:
        for node in fs_util.ceph_cluster.get_nodes(role=svc_name):
            fs_util.deamon_op(node, svc_name, "restart")
            if svc_name in ["mon", "mgr"]:
                fs_util.validate_services(client, svc_name)
            else:
                out, rc = client.exec_command(
                    sudo=True,
                    cmd=f"ceph orch ls --service_type={svc_name} --format json",
                )
                service_ls = json.loads(out)
                log.info(service_ls)
                if (
                    service_ls[0]["status"]["running"]
                    != service_ls[0]["status"]["size"]
                ):
                    raise CommandFailed(f"All {svc_name} are Not UP")
            if snap_util.verify_snap_retention(
                client=client,
                sched_path=snap_test_params["path"],
                ret_val=snap_test_params["retention"],
            ):
                test_fail = 1
            time.sleep(10)
    time.sleep(60)
    if snap_util.validate_snap_retention(client, snap_path, snap_test_params["path"]):
        test_fail = 1
    log.info(f"Performing test{snap_test_params['test_case']} cleanup")
    post_test_params["test_status"] = test_fail
    schedule_path = snap_test_params["path"]
    snap_util.remove_snap_retention(
        client,
        snap_test_params["path"],
        ret_val=snap_test_params["retention"],
        fs_name=snap_test_params["fs_name"],
    )
    snap_util.remove_snap_schedule(
        client, schedule_path, fs_name=snap_test_params["fs_name"]
    )
    snap_util.sched_snap_cleanup(client, snap_path)
    client.exec_command(sudo=True, cmd=f"umount {snap_path}")
    return post_test_params


####################
# HELPER ROUTINES
####################
def umount_all(mnt_paths, umount_params):
    cmd = f"rm -rf {mnt_paths['kernel']}/*file*"
    if "subvol" in umount_params.get("test_case"):
        cmd = f"rm -rf {mnt_paths['kernel']}{umount_params['path']}/*file*"
    umount_params["client"].exec_command(sudo=True, cmd=cmd, timeout=1800)
    for mnt_type in mnt_paths:
        umount_params["client"].exec_command(
            sudo=True, cmd=f"umount {mnt_paths[mnt_type]}"
        )


def get_iso_time(client):
    date_utc = client.exec_command(sudo=True, cmd="date")
    log.info(date_utc[0])
    date_utc_parsed = parser.parse(date_utc[0], ignoretz="True")
    log.info(f"date_parsed_iso:{date_utc_parsed.isoformat()}")
    return date_utc_parsed.isoformat()
