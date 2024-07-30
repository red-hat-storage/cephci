import concurrent.futures
import datetime
import json
import random
import string
import time
import traceback
from threading import Thread

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.cephfs_volume_management import wait_for_process
from utility.log import Log

log = Log(__name__)


def test_setup(fs_util, ceph_cluster, client):
    """
    This method is Setup to create test configuration - subvolumegroup,subvolumes,nfs servers
    """
    log.info("Create fs volume if the volume is not there")
    default_fs = "cephfs"
    fs_details = fs_util.get_fs_info(client)

    if not fs_details:
        fs_util.create_fs(client, default_fs)

    nfs_servers = ceph_cluster.get_ceph_objects("nfs")
    nfs_server = nfs_servers[0].node.hostname
    nfs_name = "cephfs-nfs"

    client.exec_command(
        sudo=True, cmd=f"ceph nfs cluster create {nfs_name} {nfs_server}"
    )
    if wait_for_process(client=client, process_name=nfs_name, ispresent=True):
        log.info("ceph nfs cluster created successfully")
    else:
        raise CommandFailed("Failed to create nfs cluster")
    nfs_export_name = "/export_" + "".join(
        random.choice(string.digits) for i in range(3)
    )
    log.info(
        "Create subvolumegroup, Create subvolume in subvolumegroup and default group"
    )
    subvolumegroup = {"vol_name": default_fs, "group_name": "subvolgroup_1"}
    fs_util.create_subvolumegroup(client, **subvolumegroup)
    sv_def = {
        "vol_name": default_fs,
        "subvol_name": "sv_def_1",
        "size": "5368706371",
    }
    fs_util.create_subvolume(client, **sv_def)
    sv_non_def = {
        "vol_name": default_fs,
        "subvol_name": "sv_non_def_1",
        "group_name": "subvolgroup_1",
        "size": "5368706371",
    }
    fs_util.create_subvolume(client, **sv_non_def)
    sv_list = [sv_def, sv_non_def]
    setup_params = {
        "default_fs": default_fs,
        "subvolumegroup": subvolumegroup,
        "sv_list": sv_list,
        "nfs_name": nfs_name,
        "nfs_export_name": nfs_export_name,
        "nfs_server": nfs_server,
    }
    return setup_params


def clone_test_prechecks(client):
    """
    This method will verify that pre-requisites for max concurrent clone when snapshot_clone_no_wait as True, is met.
    """
    log.info("Verify default value for ceph config snapshot_clone_no_wait is True")
    out, rc = client.exec_command(
        sudo=True,
        cmd="ceph config get mgr mgr/volumes/snapshot_clone_no_wait",
    )
    if "true" not in out:
        log.error("default value for ceph config snapshot_clone_no_wait is not True")
        return 1
    if set_verify_clone_config(client, "4", set_config=False) == 1:
        return 1
    log.info("Simulate clone create delay")
    out, rc = client.exec_command(
        sudo=True,
        cmd="ceph config set mgr mgr/volumes/snapshot_clone_delay 2",
    )
    return 0


def clone_test_io(default_fs, client1, run_time, fs_util, sv_list, nfs_params):
    """
    This method is to run IO - dd,smallfile and crefi, before clone test, on test subvolumes
    """
    try:
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        log.info("Run IO on subvolumes")
        dd_params = {
            "file_name": "dd_test_file",
            "input_type": "random",
            "bs": "1M",
            "count": 10,
        }
        smallfile_params = {
            "testdir_prefix": "smallfile_io_dir",
            "threads": 2,
            "file-size": 10240,
            "files": 10,
        }
        crefi_params = {
            "testdir_prefix": "crefi_io_dir",
            "files": 10,
            "max": "100k",
            "min": "10k",
            "type": "tar",
            "breadth": 3,
            "depth": 3,
            "threads": 2,
        }
        io_run_time_mins = run_time
        io_args = {
            "run_time": io_run_time_mins,
            "dd_params": dd_params,
            "smallfile_params": smallfile_params,
            "crefi_params": crefi_params,
        }

        io_tools = ["dd", "smallfile", "crefi"]
        mnt_type_list = ["kernel", "fuse", "nfs"]
        write_procs = []
        for sv in sv_list:
            cmd = f"ceph fs subvolume getpath {default_fs} {sv['subvol_name']}"
            if sv.get("group_name"):
                cmd += f" {sv['group_name']}"

            subvol_path, rc = client1.exec_command(
                sudo=True,
                cmd=cmd,
            )
            mnt_path = subvol_path.strip()
            mount_params = {
                "fs_util": fs_util,
                "client": client1,
                "mnt_path": mnt_path,
                "fs_name": default_fs,
                "export_created": 0,
                "nfs_export_name": nfs_params["nfs_export_name"],
                "nfs_server": nfs_params["nfs_server"],
                "nfs_name": nfs_params["nfs_name"],
            }
            mnt_type = random.choice(mnt_type_list)
            mounting_dir, _ = fs_util.mount_ceph(mnt_type, mount_params)
            p = Thread(
                target=fs_util.run_ios_V1,
                args=(client1, mounting_dir, io_tools),
                kwargs=io_args,
            )
            p.start()
            write_procs.append(p)
        return write_procs
    except Exception as ex:
        log.info(ex)
        return 1


def set_verify_clone_config(client, concurrent_limit, set_config=True):
    """
    This method applies clone max concurrent config and verifies it
    """
    if set_config:
        out, rc = client.exec_command(
            sudo=True,
            cmd=f"ceph config set mgr mgr/volumes/max_concurrent_clones {concurrent_limit}",
        )
    log.info(
        f"Verify value for ceph config mgr/volumes/max_concurrent_clones is {concurrent_limit}"
    )
    out, rc = client.exec_command(
        sudo=True,
        cmd="ceph config get mgr mgr/volumes/max_concurrent_clones",
    )
    if concurrent_limit not in out:
        log.error(
            f"Value for ceph config mgr/volumes/max_concurrent_clones is not {concurrent_limit}"
        )
        return 1
    return 0


def get_clone_status(client, fs_util, clone):
    """
    This method captures clone status and returns state as complete,in-progress,pending or not started
    """
    try:
        cmd_out, cmd_rc = fs_util.get_clone_status(
            client,
            clone["vol_name"],
            clone["target_subvol_name"],
            group_name=clone.get("target_group_name", None),
        )
        status = json.loads(cmd_out)
        log.info(f"Clone status for {clone['target_subvol_name']}: {status}")
        log.info(f"{clone['target_subvol_name']} state is {status['status']['state']}")
        return status["status"]["state"]
    except Exception as ex:
        log.info(ex)
        if "does not exist" in str(ex):
            state = "not started"
            return state
        log.error("Unexpected error during clone status")


def max_concurrent_clone_test(client1, fs_util, concurrent_limit, default_fs, sv_list):
    """
    This routine method is to perform max concurrent clone workflow validation, wherein appropriate error
    returned for additional clone request.
    """
    exp_str = "Error EAGAIN: all cloner threads are busy, please try again later"
    clone_list = []
    for sv in sv_list:
        if sv.get("group_name"):
            non_def_group = sv["group_name"]
    clone_placement = ["no_group", non_def_group]
    clone_cnt = int(concurrent_limit) + 1
    log.info(f"clone count:{clone_cnt}")
    clone_cnt += 1
    for x in range(1, clone_cnt):
        sv = random.choice(sv_list)
        clone_iter = {
            "vol_name": default_fs,
            "subvol_name": sv["subvol_name"],
            "snap_name": "clone_snap",
            "target_subvol_name": f"clone_{x}",
        }
        if sv.get("group_name"):
            clone_iter.update({"group_name": sv["group_name"]})
        clone_grp = random.choice(clone_placement)
        if "no_group" not in clone_grp:
            clone_iter.update({"target_group_name": non_def_group})
        clone_list.append(clone_iter)

    log.info(f"clone list : {clone_list}")
    hit_clone_err = 0
    try:
        with parallel() as p:
            for clone in clone_list:
                p.spawn(fs_util.create_clone, client1, **clone, validate=False)
    except Exception as ex:
        log.info(ex)
        if exp_str in str(ex):
            hit_clone_err = 1
            log.info(
                f"Expected clone error message obtained during max+1 clone create:{ex}"
            )
    if hit_clone_err == 0:
        clone_cnt = int(concurrent_limit) + 1
        log.error(
            f"All {clone_cnt} clones creating concurrently, {exp_str} wasn't generated"
        )
        return 1
    status_list = []
    iteration = 0
    clone_target = len(clone_list) - 1
    log.info(f"clone_target:{clone_target}")
    while int(status_list.count("complete")) < int(clone_target):
        status_list.clear()
        iteration += 1
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(get_clone_status, client1, fs_util, clone)
                for clone in clone_list
            ]
        status_list = [f.result() for f in futures]
        log.info(f"status_list:{status_list}")
        if status_list.count("in-progress") > int(concurrent_limit):
            log.error("Clones more than concurrency limit are in-progress")
            return 1
        else:
            log.info(
                f"cloning is in progress for {status_list.count('in-progress')} out of {len(clone_list)}"
            )
        log.info(f"Iteration {iteration} has been completed")
        complete_cnt = status_list.count("complete")
        progress_cnt = status_list.count("in-progress")
        log.info(
            f"status_list with count, complete:{complete_cnt},progress:{progress_cnt}"
        )

    for clonevolume in clone_list:
        clone_obj = {
            "vol_name": clonevolume["vol_name"],
            "subvol_name": clonevolume["target_subvol_name"],
        }
        if clonevolume.get("target_group_name"):
            clone_obj.update({"group_name": clonevolume["target_group_name"]})
        try:
            fs_util.remove_subvolume(client1, **clone_obj)
        except Exception as ex:
            if "does not exist" not in str(ex):
                return 1
    return 0


def run(ceph_cluster, **kw):
    """
    Polarion TC CEPH-83593402, BZ 2290711:
    1. We need atleast one client node to execute this test case
    2. Verify default value for ceph config 'ceph config get mgr mgr/volumes/snapshot_clone_no_wait' is True.
    3. Verify default value for ceph config 'ceph config get mgr mgr/volumes/max_concurrent_clones' is 4.
    4. Creats fs volume if the volume is not there
    5. Create subvolumegroup, Create subvolume in subvolumegroup and default group
    6. Run IO on subvolume, Create Snapshot.
    7. Note current max concurrent clome limit 4, and create 5 clones of both subvolumes in parallel across default
       and non-default subvolume groups.
    8. Verify only 4/5 clones suceed and 1 will fail with appropriate error message
       "Error EAGAIN: all cloner threads are busy, please try again later"
        Verify clone creation progress with command,
        ceph fs clone status <vol_name> <clone_name> [--group_name <group_name>]

        Use below config to simulate delay during cloning, to achieve concurrency limit,
        'ceph config set mgr mgr/volumes/snapshot_clone_delay <value_secs>'

    9. Increase max_concurrent_clones limit to 5 and Repeat step7-8 with 6 conclurrent clones.
       Verify error message on 1/6 clones.
    10. Decrease max_concurrent_clones to 3 and repeat step7-8 with 4 clones and verify error message for 1/4 clones

    Clean-up: Delete Snapshot, Clones, Subvolumes and subvolumegroup in order.
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
        client1 = clients[0]
        log.info("Verify the pre-requisites for Clone_no_wait_if_max_concurrent test")
        if clone_test_prechecks(client1) == 1:
            return 1

        log.info("Setup test configuration")
        setup_params = test_setup(fs_util, ceph_cluster, client1)
        default_fs = setup_params["default_fs"]
        rmclone_list = [
            {"vol_name": default_fs, "subvol_name": f"clone_{x}"} for x in range(1, 7)
        ]
        sv_list = setup_params["sv_list"]
        nfs_params = {
            "nfs_name": setup_params["nfs_name"],
            "nfs_server": setup_params["nfs_server"],
            "nfs_export_name": setup_params["nfs_export_name"],
        }
        log.info("Start IO on test subvolumes")
        io_run_time_mins = 2
        write_procs = clone_test_io(
            default_fs, client1, io_run_time_mins, fs_util, sv_list, nfs_params
        )
        if write_procs == 1:
            log.error("IO could not be started")
            return 1

        time.sleep(10)
        log.info("Create Snapshot on subvolumes")
        snap_list = []
        for sv in sv_list:
            snapshot = {
                "vol_name": default_fs,
                "subvol_name": sv["subvol_name"],
                "snap_name": "clone_snap",
            }
            if sv.get("group_name"):
                snapshot.update({"group_name": "subvolgroup_1"})
            fs_util.create_snapshot(client1, **snapshot)
            snap_list.append(snapshot)

        log.info(
            "Verify additional clone create doesn't suceed when default max concurrent clone in-progress"
        )
        concurrent_limit = "4"
        test_status = 1
        test_status = max_concurrent_clone_test(
            client1, fs_util, concurrent_limit, default_fs, sv_list
        )
        if test_status == 1:
            log.error(f"Max concurrent clone test for limit {concurrent_limit} failed")
            return 1

        log.info(
            "Increase concurrent clone limit to 5 and validate that 6th concurrent clone creation doesn't suceed"
        )
        concurrent_limit = "5"
        if set_verify_clone_config(client1, concurrent_limit) == 1:
            return 1
        test_status = max_concurrent_clone_test(
            client1, fs_util, concurrent_limit, default_fs, sv_list
        )
        if test_status == 1:
            log.error(f"Max concurrent clone test for limit {concurrent_limit} failed")
            return 1

        log.info(
            "Decrease concurrent clone limit to 3 and validate that 4th concurrent clone creation doesn't suceed"
        )
        concurrent_limit = "3"
        if set_verify_clone_config(client1, concurrent_limit) == 1:
            return 1
        test_status = max_concurrent_clone_test(
            client1, fs_util, concurrent_limit, default_fs, sv_list
        )
        if test_status == 1:
            log.error(f"Max concurrent clone test for limit {concurrent_limit} failed")
            return 1

        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1

    finally:
        log.info("Setting back the max concurrent clones to default value 4")
        client1.exec_command(
            sudo=True, cmd="ceph config set mgr mgr/volumes/max_concurrent_clones 4"
        )
        log.info("setting 'snapshot_clone_no_wait' to true")
        clients[0].exec_command(
            sudo=True, cmd="ceph config set mgr mgr/volumes/snapshot_clone_no_wait true"
        )
        for p in write_procs:
            if p.is_alive():
                proc_stop = 0
                io_run_time_secs = io_run_time_mins * 4 * 60
                log.info("IO is running after clone test")
                end_time = datetime.datetime.now() + datetime.timedelta(
                    seconds=io_run_time_secs
                )
                while (datetime.datetime.now() < end_time) and (proc_stop == 0):
                    if p.is_alive():
                        time.sleep(10)
                    else:
                        proc_stop = 1
                if proc_stop == 1:
                    log.info("IO completed")
                elif proc_stop == 0:
                    log.error("IO has NOT completed")
        if test_status == 1:
            for clonevolume in rmclone_list:
                fs_util.remove_subvolume(
                    client1, **clonevolume, validate=False, check_ec=False
                )
                clonevolume.update({"group_name": "subvolgroup_1"})
                fs_util.remove_subvolume(
                    client1, **clonevolume, validate=False, check_ec=False
                )

        log.info("Clean Up in progess")

        client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs cluster delete {setup_params['nfs_name']}",
            check_ec=False,
        )
        for sv_snap in snap_list:
            fs_util.remove_snapshot(client1, **sv_snap, validate=False, check_ec=False)
        for sv in sv_list:
            fs_util.remove_subvolume(client1, **sv, validate=False, check_ec=False)
        fs_util.remove_subvolumegroup(
            client1, default_fs, "subvolgroup_1", validate=True
        )
