import datetime
import random
import re
import string
import time
import traceback
from threading import Thread

from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsv1
from tests.cephfs.snapshot_clone.cephfs_cg_io import CG_snap_IO
from tests.cephfs.snapshot_clone.cg_snap_utils import CG_Snap_Utils
from utility.log import Log

log = Log(__name__)
global cg_test_io_status


def run(ceph_cluster, **kw):
    """
    Workflows Covered :

    Type - Scale test

    Workflow1 - Scale Client sessions and verify QS snap quiesce of IO on those subvolume client sessions.

    Type - Stress test

    Workflow2 - Stress the quiesce set with repeated quiesce ops and IO on 10 subvolumes- quiesce, snapshot,release,
    cancel,reset,include,exclude

    Clean Up:
    1.

    """
    try:
        fs_util_v1 = FsUtilsv1(ceph_cluster)
        cg_snap_util = CG_Snap_Utils(ceph_cluster)
        cg_snap_io = CG_snap_IO(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        default_subvol_size = config.get("subvol_size", 8589934592)
        platform_type = config.get("platform_type", "default")
        if platform_type == "baremetal":
            default_subvol_size = default_subvol_size * 10
        log.info("checking Pre-requisites")
        if len(clients) < 11:
            log.info(
                f"This test requires minimum 11 client nodes.This has only {len(clients)} clients"
            )
            return 1
        mgr_node = ceph_cluster.get_ceph_objects("mgr")[0]
        build = config.get("build", config.get("rhbuild"))
        fs_util_v1.prepare_clients(clients, build)
        default_fs = config.get("fs_name", "cephfs")
        qs_cnt_def = random.randrange(5, 11)
        qs_cnt = config.get("qs_cnt", qs_cnt_def)
        fs_util_v1.auth_list(clients)

        client1 = clients[0]

        fs_details = fs_util_v1.get_fs_info(client1, fs_name=default_fs)
        if not fs_details:
            fs_util_v1.create_fs(client1, default_fs)

        test_case_name = config.get("test_name", "all_tests")
        test_systemic = [
            "cg_snap_scale_workflow",
            "cg_snap_stress_workflow",
        ]

        if test_case_name in test_systemic:
            test_list = [test_case_name]
        else:
            test_list = test_systemic

        # Setup
        subvolumegroup = {
            "vol_name": default_fs,
            "group_name": "subvolgroup_cg",
        }
        fs_util_v1.create_subvolumegroup(client1, **subvolumegroup)
        qs_cnt += 1
        sv_def_list = []
        sv_non_def_list = []
        for i in range(1, qs_cnt):
            sv_name = f"sv_def_{i}"
            sv_def_list.append(sv_name)
            subvolume = {
                "vol_name": default_fs,
                "subvol_name": sv_name,
                "size": default_subvol_size,
            }
            fs_util_v1.create_subvolume(client1, **subvolume)
        for i in range(1, qs_cnt):
            sv_name = f"sv_non_def_{i}"
            subvolume = {
                "vol_name": default_fs,
                "subvol_name": sv_name,
                "group_name": "subvolgroup_cg",
                "size": default_subvol_size,
            }
            fs_util_v1.create_subvolume(client1, **subvolume)
            sv_name = f"subvolgroup_cg/sv_non_def_{i}"
            sv_non_def_list.append(sv_name)

        client1.exec_command(
            sudo=True,
            cmd="ceph config set mds log_to_file true;ceph config set mds debug_mds_quiesce 10",
            check_ec=False,
        )
        crash_status_before = fs_util_v1.get_crash_ls_new(client1)
        log.info(f"Crash status before Test: {crash_status_before}")
        fs_util_v1.get_ceph_health_status(client1)
        sv_mixed_list = []
        qs_cnt -= 1
        for i in range(0, qs_cnt):
            sv_name1 = random.choice(sv_def_list)
            sv_name2 = random.choice(sv_non_def_list)
            if (sv_name2 not in sv_mixed_list) and (len(sv_mixed_list) < qs_cnt):
                sv_mixed_list.append(sv_name2)
            if (sv_name1 not in sv_mixed_list) and (len(sv_mixed_list) < qs_cnt):
                sv_mixed_list.append(sv_name1)
            if len(sv_mixed_list) == qs_cnt:
                break

        qs_sets = [
            sv_def_list,
            sv_non_def_list,
            sv_mixed_list,
        ]
        log.info(f"Test config attributes : qs_cnt - {qs_cnt}, qs_sets - {qs_sets}")
        cg_test_params = {
            "ceph_cluster": ceph_cluster,
            "fs_name": default_fs,
            "fs_util": fs_util_v1,
            "platform_type": platform_type,
            "cg_snap_util": cg_snap_util,
            "cg_snap_io": cg_snap_io,
            "clients": clients,
            "mgr_node": mgr_node,
            "qs_sets": qs_sets,
            "cg_run_time": 2700,  # beyond 45mins the container's rootFS get's full
        }
        for test_name in test_list:
            log.info(
                f"\n\n                                   ============ {test_name} ============ \n"
            )
            cg_test_params.update({"test_case": test_name})
            test_status = cg_system_test(cg_test_params)

            if test_status == 1:
                assert False, f"Test {test_name} failed"
            else:
                log.info(f"Test {test_name} passed \n")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean Up in progess")
        crash_status_after = fs_util_v1.get_crash_ls_new(client1)
        log.info(f"Crash status after Test: {crash_status_after}")
        health_wait = 300
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=health_wait)
        health_ok = 0
        wait_time = 0
        while (datetime.datetime.now() < end_time) and health_ok == 0:
            try:
                fs_util_v1.get_ceph_health_status(client1)
                health_ok = 1
            except Exception as ex:
                log.info(
                    f"Wait for sometime to check if Cluster health can be OK, current state : {ex}"
                )
                time.sleep(10)
                wait_time += 10
        if health_ok == 0:
            assert (
                False
            ), f"Cluster health is not OK even after waiting for {health_wait}secs"
        log.info(f"Cluster Health is OK in {wait_time}secs")

        if len(crash_status_after) > len(crash_status_before):
            assert False, "Post test validation failed, please check crash report above"
        qs_cnt += 1
        for i in range(1, qs_cnt):
            subvol_name = f"sv_def_{i}"
            fs_util_v1.remove_subvolume(client1, default_fs, subvol_name, validate=True)
        for i in range(1, qs_cnt):
            subvol_name = f"sv_non_def_{i}"
            fs_util_v1.remove_subvolume(
                client1,
                default_fs,
                subvol_name,
                validate=True,
                group_name="subvolgroup_cg",
            )
        fs_util_v1.remove_subvolumegroup(
            client1, default_fs, "subvolgroup_cg", validate=True
        )


def cg_system_test(cg_test_params):
    if cg_test_params["test_case"] == "cg_snap_scale_workflow":
        test_status = cg_scale(cg_test_params)
        return test_status
    if cg_test_params["test_case"] == "cg_snap_stress_workflow":
        test_status = cg_stress(cg_test_params)
        return test_status


def cg_scale(cg_test_params):
    log.info("Workflow 1a - Test quiesce lifecycle with Scaled IO")
    fs_name = cg_test_params["fs_name"]
    fs_util = cg_test_params["fs_util"]
    platform_type = cg_test_params["platform_type"]
    clients = cg_test_params["clients"]
    client = cg_test_params["clients"][0]
    log.info(f"clients : {client},clients : {clients}")
    qs_clients = clients.copy()
    log.info(f"qs_clients before pop: {qs_clients}")
    qs_clients.pop(0)
    log.info(f"qs_clients after pop: {qs_clients}")
    qs_sets = cg_test_params["qs_sets"]
    cg_snap_util = cg_test_params["cg_snap_util"]

    total_fail = 0
    test_fail = 0
    qs_set = random.choice(qs_sets)
    client_mnt_dict = {}
    write_procs = []
    i = 0
    for qs_member in qs_set:
        client_obj = qs_clients[i]
        if "/" in qs_member:
            group_name, subvol_name = re.split("/", qs_member)
            cmd = f"ceph fs subvolume getpath {fs_name} {subvol_name} --group_name {group_name}"
        else:
            subvol_name = qs_member
            cmd = f"ceph fs subvolume getpath {fs_name} {subvol_name}"

        subvol_path, rc = client.exec_command(
            sudo=True,
            cmd=cmd,
        )
        mnt_path = subvol_path.strip()
        mount_params = {
            "fs_util": fs_util,
            "client": client_obj,
            "mnt_path": mnt_path,
            "fs_name": fs_name,
            "export_created": 0,
        }
        mounting_dir, _ = fs_util.mount_ceph("kernel", mount_params)
        client_mnt_dict.update({client_obj: mounting_dir})
        i += 1

    log.info(f"Start the IO on quiesce set members - {qs_set}")
    if platform_type == "baremetal":
        dd_params = {
            "file_name": "dd_test_file",
            "input_type": "random",
            "bs": "1M",
            "count": 500,
        }
        smallfile_params = {
            "testdir_prefix": "smallfile_io_dir",
            "threads": 4,
            "file-size": 10240,
            "files": 5000,
        }
        crefi_params = {
            "testdir_prefix": "crefi_io_dir",
            "files": 5000,
            "max": "100k",
            "min": "10k",
            "type": "tar",
            "breadth": 5,
            "depth": 5,
            "threads": 3,
        }
    else:
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

    io_run_time_mins = 3

    io_args = {
        "run_time": io_run_time_mins,
        "dd_params": dd_params,
        "smallfile_params": smallfile_params,
        "crefi_params": crefi_params,
    }

    io_tools = ["dd", "smallfile", "crefi"]
    tar_compile_test = 0
    for client_tmp in client_mnt_dict:
        mounting_dir = client_mnt_dict[client_tmp]
        if tar_compile_test == 0:
            file_extract_params = {"compile_test": 1}
            io_args_tmp = io_args.copy()
            io_args_tmp.update({"file_extract_params": file_extract_params})
            io_tools_tmp = io_tools.copy()
            io_tools_tmp.insert(0, "file_extract")
            tar_compile_test = 1
            p = Thread(
                target=fs_util.run_ios_V1,
                args=(client_tmp, mounting_dir, io_tools_tmp),
                kwargs=io_args_tmp,
            )
        else:
            p = Thread(
                target=fs_util.run_ios_V1,
                args=(client_tmp, mounting_dir, io_tools),
                kwargs=io_args,
            )
        p.start()
        write_procs.append(p)

    time.sleep(60)
    repeat_cnt = 5
    snap_qs_dict = {}
    for qs_member in qs_set:
        snap_qs_dict.update({qs_member: []})
    i = 0
    while i < repeat_cnt:
        log.info(f"Quiesce Lifecycle : Iteration {i}")
        # time taken for 1 lifecycle : ~5secs

        out, rc = client.exec_command(
            sudo=True,
            cmd="ceph status;ceph fs status;ceph fs dump",
            check_ec=False,
        )
        log.info(f"Ceph fs status and fs dump output : {out}")

        rand_str = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(4))
        )
        qs_id_val = f"cg_scale_{rand_str}"
        log.info(f"Quiesce the set {qs_set}")
        cg_snap_util.cg_quiesce(
            client, qs_set, qs_id=qs_id_val, timeout=600, expiration=600
        )

        time.sleep(30)
        log.info("Perform snapshot creation on all members")
        rand_str = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(3))
        )
        snap_name = f"cg_snap_{rand_str}"
        for qs_member in qs_set:
            snap_list = snap_qs_dict[qs_member]
            snapshot = {
                "vol_name": fs_name,
                "snap_name": snap_name,
            }
            if "/" in qs_member:
                group_name, subvol_name = re.split("/", qs_member)
                snapshot.update(
                    {
                        "subvol_name": subvol_name,
                        "group_name": group_name,
                    }
                )
            else:
                subvol_name = qs_member
                snapshot.update(
                    {
                        "subvol_name": subvol_name,
                    }
                )
            try:
                fs_util.create_snapshot(client, **snapshot)
            except Exception as ex:
                log.info(ex)
            log.info(f"Created snapshot {snap_name} on {subvol_name}")
            snap_list.append(snap_name)
            snap_qs_dict.update({subvol_name: snap_list})
        log.info(f"Release quiesce set {qs_id_val}")
        cg_snap_util.cg_quiesce_release(client, qs_id_val, if_await=True)
        i += 1
        time.sleep(20)

    for p in write_procs:
        if p.is_alive():
            proc_stop = 0
            io_run_time_secs = io_run_time_mins * 4 * 60
            log.info("IO is running after quiesce lifecycle")
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

    log.info(f"Perform cleanup for {qs_set}")
    log.info("Remove CG IO files and unmount")
    for client_tmp in client_mnt_dict:
        mounting_dir = client_mnt_dict[client_tmp]
        cg_snap_util.cleanup_cg_io(client_tmp, [mounting_dir])

    snap_name = f"cg_snap_{rand_str}"
    log.info("Remove CG snapshots")
    for qs_member in qs_set:
        group_name = None
        if "/" in qs_member:
            group_name, subvol_name = re.split("/", qs_member)
        else:
            subvol_name = qs_member
        snap_list = snap_qs_dict[subvol_name]
        for snap_name in snap_list:
            if group_name is not None:
                fs_util.remove_snapshot(
                    client,
                    fs_name,
                    subvol_name,
                    snap_name,
                    validate=True,
                    group_name=group_name,
                )
            else:
                fs_util.remove_snapshot(
                    client, fs_name, subvol_name, snap_name, validate=True
                )

    if test_fail == 1:
        log.error("FAIL: Workflow 1 - quiesce lifecycle with scaled config")
        test_fail = 0
        total_fail += 1
    if total_fail > 0:
        return 1
    return 0


def cg_stress(cg_test_params):
    log.info("Workflow - Stress test on quiesce set")
    fs_name = cg_test_params["fs_name"]
    fs_util = cg_test_params["fs_util"]
    platform_type = cg_test_params["platform_type"]
    clients = cg_test_params["clients"]
    client = cg_test_params["clients"][0]
    qs_clients = clients.copy()
    qs_clients.pop(0)
    qs_sets = cg_test_params["qs_sets"]
    cg_snap_util = cg_test_params["cg_snap_util"]

    cg_run_time = cg_test_params["cg_run_time"]
    total_fail = 0
    test_fail = 0
    end_time = datetime.datetime.now() + datetime.timedelta(seconds=cg_run_time)
    cnt = 0
    log.info(f"cg_run_time:{cg_run_time},end_time:{end_time}")

    while datetime.datetime.now() < end_time:
        now_time = datetime.datetime.now()
        log.info(f"now_time:{now_time},end_time:{end_time}")
        log.info(f"CG stress test : Iteration {cnt}")
        qs_set = random.choice(qs_sets)

        client_mnt_dict = {}
        write_procs = []
        i = 0
        for qs_member in qs_set:
            client_obj = qs_clients[i]
            if "/" in qs_member:
                group_name, subvol_name = re.split("/", qs_member)
                cmd = f"ceph fs subvolume getpath {fs_name} {subvol_name} --group_name {group_name}"
            else:
                subvol_name = qs_member
                cmd = f"ceph fs subvolume getpath {fs_name} {subvol_name}"

            subvol_path, rc = client.exec_command(
                sudo=True,
                cmd=cmd,
            )
            mnt_path = subvol_path.strip()
            mount_params = {
                "fs_util": fs_util,
                "client": client_obj,
                "mnt_path": mnt_path,
                "fs_name": fs_name,
                "export_created": 0,
            }
            mounting_dir, _ = fs_util.mount_ceph("kernel", mount_params)
            client_mnt_dict.update({client_obj: mounting_dir})
            i += 1

        log.info(f"Start the IO on quiesce set members - {qs_set}")
        if platform_type == "baremetal":
            dd_params = {
                "file_name": "dd_test_file",
                "input_type": "random",
                "bs": "1M",
                "count": 500,
            }
            smallfile_params = {
                "testdir_prefix": "smallfile_io_dir",
                "threads": 4,
                "file-size": 10240,
                "files": 5000,
            }
            crefi_params = {
                "testdir_prefix": "crefi_io_dir",
                "files": 5000,
                "max": "100k",
                "min": "10k",
                "type": "tar",
                "breadth": 5,
                "depth": 5,
                "threads": 3,
            }
        else:
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
        io_run_time_mins = 3
        io_args = {
            "run_time": io_run_time_mins,
            "dd_params": dd_params,
            "smallfile_params": smallfile_params,
            "crefi_params": crefi_params,
        }

        io_tools = ["dd", "smallfile", "crefi"]
        tar_compile_test = 0
        for client_tmp in client_mnt_dict:
            mounting_dir = client_mnt_dict[client_tmp]
            # Run file_extract tool just once
            if tar_compile_test == 0:
                file_extract_params = {"compile_test": 1}
                io_args_tmp = io_args.copy()
                io_args_tmp.update({"file_extract_params": file_extract_params})
                io_tools_tmp = io_tools.copy()
                io_tools_tmp.insert(0, "file_extract")
                tar_compile_test = 1
                p = Thread(
                    target=fs_util.run_ios_V1,
                    args=(client_tmp, mounting_dir, io_tools_tmp),
                    kwargs=io_args_tmp,
                )
            else:
                p = Thread(
                    target=fs_util.run_ios_V1,
                    args=(client_tmp, mounting_dir, io_tools),
                    kwargs=io_args,
                )
            p.start()
            write_procs.append(p)

        time.sleep(60)
        repeat_cnt = 5
        snap_qs_dict = {}
        for qs_member in qs_set:
            snap_qs_dict.update({qs_member: []})
        i = 0
        while i < repeat_cnt:
            log.info(f"Quiesce Lifecycle : Iteration {i}")
            # time taken for 1 lifecycle : ~5secs
            rand_str = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(3))
            )
            qs_id_val = f"cg_test1_{rand_str}"
            out, rc = client.exec_command(
                sudo=True,
                cmd="ceph status;ceph fs status;ceph fs dump",
                check_ec=False,
            )
            log.info(f"Ceph fs status and fs dump output : {out}")

            log.info(f"Quiesce the set {qs_set}")
            cg_snap_util.cg_quiesce(
                client, qs_set, qs_id=qs_id_val, timeout=600, expiration=600
            )

            time.sleep(30)
            log.info(f"Query quiesce set {qs_id_val}")
            cg_snap_util.get_qs_query(client, qs_id_val)
            log.info("Perform snapshot creation on all members")
            rand_str = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(3))
            )
            snap_name = f"cg_snap_{rand_str}"
            for qs_member in qs_set:
                snap_list = snap_qs_dict[qs_member]
                snapshot = {
                    "vol_name": fs_name,
                    "snap_name": snap_name,
                }
                if "/" in qs_member:
                    group_name, subvol_name = re.split("/", qs_member)
                    snapshot.update(
                        {
                            "subvol_name": subvol_name,
                            "group_name": group_name,
                        }
                    )
                else:
                    subvol_name = qs_member
                    snapshot.update(
                        {
                            "subvol_name": subvol_name,
                        }
                    )
                try:
                    fs_util.create_snapshot(client, **snapshot)
                except Exception as ex:
                    log.info(ex)
                log.info(f"Created snapshot {snap_name} on {subvol_name}")
                snap_list.append(snap_name)
                snap_qs_dict.update({subvol_name: snap_list})
            log.info(f"Query quiesce set {qs_id_val}")
            cg_snap_util.get_qs_query(client, qs_id_val)
            log.info(f"Release quiesce set {qs_id_val}")
            cg_snap_util.cg_quiesce_release(client, qs_id_val, if_await=True)
            log.info(f"Query quiesce set {qs_id_val}")
            cg_snap_util.get_qs_query(client, qs_id_val)
            log.info(f"Ceph fs status and fs dump output : {out}")
            cmd = (
                "ceph config set mds log_to_file true;ceph config set mds debug_mds 10;"
            )
            cmd += "ceph config set mds debug_mds_quiesce 10"
            client.exec_command(
                sudo=True,
                cmd=cmd,
                check_ec=False,
            )
            log.info(f"Reset the quiesce set - {qs_set}")
            cg_snap_util.cg_quiesce_reset(client, qs_id_val, qs_set)
            client.exec_command(
                sudo=True,
                cmd="ceph config set mds log_to_file false",
                check_ec=False,
            )
            log.info(f"Query quiesce set {qs_id_val}")
            cg_snap_util.get_qs_query(client, qs_id_val)
            log.info("Perform snapshot creation on all members")
            rand_str = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(4))
            )
            snap_name = f"cg_snap_{rand_str}"
            for qs_member in qs_set:
                snap_list = snap_qs_dict[qs_member]
                snapshot = {
                    "vol_name": fs_name,
                    "snap_name": snap_name,
                }
                if "/" in qs_member:
                    group_name, subvol_name = re.split("/", qs_member)
                    snapshot.update(
                        {
                            "subvol_name": subvol_name,
                            "group_name": group_name,
                        }
                    )
                else:
                    subvol_name = qs_member
                    snapshot.update(
                        {
                            "subvol_name": subvol_name,
                        }
                    )
                try:
                    fs_util.create_snapshot(client, **snapshot)
                except Exception as ex:
                    log.info(ex)
                log.info(f"Created snapshot {snap_name} on {subvol_name}")
                snap_list.append(snap_name)
                snap_qs_dict.update({subvol_name: snap_list})
            log.info(f"Query quiesce set {qs_id_val}")
            cg_snap_util.get_qs_query(client, qs_id_val)
            log.info(f"Cancel quiesce set {qs_id_val}")
            try:
                cg_snap_util.cg_quiesce_cancel(client, qs_id_val)
            except Exception as ex:
                log.info(ex)
            log.info(f"Query quiesce set {qs_id_val}")
            out = cg_snap_util.get_qs_query(client, qs_id_val)
            log.info(out)
            cmd = (
                "ceph config set mds log_to_file true;ceph config set mds debug_mds 10;"
            )
            cmd += "ceph config set mds debug_mds_quiesce 10"
            client.exec_command(
                sudo=True,
                cmd=cmd,
                check_ec=False,
            )
            log.info(f"Reset the quiesce set - {qs_set}")
            cg_snap_util.cg_quiesce_reset(client, qs_id_val, qs_set)
            client.exec_command(
                sudo=True,
                cmd="ceph config set mds log_to_file false",
                check_ec=False,
            )
            log.info(f"Exclude a subvolume from quiesce set {qs_id_val}")
            qs_query_out = cg_snap_util.get_qs_query(client, qs_id_val)
            exclude_sv_name = random.choice(qs_set)
            cg_snap_util.cg_quiesce_exclude(
                client, qs_id_val, [exclude_sv_name], if_await=True
            )

            log.info(f"Verify quiesce set {qs_id_val} state after exclude")
            qs_query_out = cg_snap_util.get_qs_query(client, qs_id_val)
            state = qs_query_out["sets"][qs_id_val]["state"]["name"]
            if state == "QUIESCED":
                log.info(f"State of qs set {qs_id_val} after exclude is QUIESCED")
            else:
                log.error(
                    f"State of qs set {qs_id_val} after exclude is not as expected - {state}"
                )
                test_fail = 1
            include_sv_name = exclude_sv_name
            log.info(
                f"Include a subvolume {include_sv_name} in quiesce set {qs_id_val}"
            )
            qs_query_out = cg_snap_util.get_qs_query(client, qs_id_val)
            for qs_member in qs_query_out["sets"][qs_id_val]["members"]:
                if exclude_sv_name in qs_member:
                    exclude_state = qs_query_out["sets"][qs_id_val]["members"][
                        qs_member
                    ]["excluded"]
                    log.info(
                        f"excluded value of {exclude_sv_name} before include : {exclude_state}"
                    )
            cg_snap_util.cg_quiesce_include(
                client, qs_id_val, [include_sv_name], if_await=True
            )

            log.info(f"Verify quiesce set {qs_id_val} state after include")
            qs_query_out = cg_snap_util.get_qs_query(client, qs_id_val)
            state = qs_query_out["sets"][qs_id_val]["state"]["name"]
            if state == "QUIESCED":
                log.info(f"State of qs set {qs_id_val} after include is QUIESCED")
            else:
                log.error(
                    f"State of qs set {qs_id_val} after include is not as expected - {state}"
                )
                test_fail = 1
            log.info(f"Release quiesce set {qs_id_val}")
            cg_snap_util.cg_quiesce_release(client, qs_id_val, if_await=True)
            log.info(f"Query quiesce set {qs_id_val}")
            qs_query = cg_snap_util.get_qs_query(client, qs_id_val)
            log.info(f"Query of qs set {qs_id_val} : {qs_query}")
            i += 1
            time.sleep(30)

        for p in write_procs:
            if p.is_alive():
                proc_stop = 0
                log.info("IO is running after quiesce lifecycle")
                io_run_time_secs = io_run_time_mins * 4 * 60
                end_time_sub = datetime.datetime.now() + datetime.timedelta(
                    seconds=io_run_time_secs
                )
                while (datetime.datetime.now() < end_time_sub) and (proc_stop == 0):
                    if p.is_alive():
                        time.sleep(10)
                    else:
                        proc_stop = 1
                if proc_stop == 1:
                    log.info("IO completed")
                elif proc_stop == 0:
                    log.error("IO has NOT completed")

        log.info(f"Perform cleanup for {qs_set}")
        log.info("Remove CG IO files and unmount")
        for client_tmp in client_mnt_dict:
            mounting_dir = client_mnt_dict[client_tmp]
            cg_snap_util.cleanup_cg_io(client_tmp, [mounting_dir])

        snap_name = f"cg_snap_{rand_str}"
        log.info("Remove CG snapshots")
        for qs_member in qs_set:
            group_name = None
            if "/" in qs_member:
                group_name, subvol_name = re.split("/", qs_member)
            else:
                subvol_name = qs_member
            snap_list = snap_qs_dict[subvol_name]
            for snap_name in snap_list:
                if group_name is not None:
                    fs_util.remove_snapshot(
                        client,
                        fs_name,
                        subvol_name,
                        snap_name,
                        validate=True,
                        group_name=group_name,
                    )
                else:
                    fs_util.remove_snapshot(
                        client, fs_name, subvol_name, snap_name, validate=True
                    )
                time.sleep(2)

        cnt += 1

    if test_fail == 1:
        log.error("FAIL: Workflow 2 - Stress test on quiesce set")
        test_fail = 0
        total_fail += 1
    if total_fail > 0:
        return 1
    return 0
