import datetime
import json
import logging
import os
import random
import string
import threading
import time
import traceback
from multiprocessing import Value
from threading import Thread

from ceph.parallel import parallel

# from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_system.cephfs_system_utils import CephFSSystemUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from utility.log import Log

log = Log(__name__)
logger = logging.getLogger("run_log")

logging_thread = None
mds_logging_thread = None
stop_event = threading.Event()


def run(ceph_cluster, **kw):
    """
    System test - SV and Clone ops : Run below workflows in parallel and repeat until runtime,

    1. Create 100 subvolumes. Delete the subvolumes and in parallel create another 100 subvolumes.
    2. Create 10 clones using snapshots creates on 10 shared subvolumes. Delete clones and create
       another 10 in parallel.

    """
    try:
        fs_system_utils = CephFSSystemUtils(ceph_cluster)
        fs_util = FsUtilsV1(ceph_cluster)
        config = kw.get("config")
        cephfs_config = {}
        run_time = config.get("run_time_hrs", 4)
        sv_cnt = config.get("sv_cnt", 100)
        clone_cnt = config.get("clone_cnt", 10)
        clients = ceph_cluster.get_ceph_objects("client")

        file = "cephfs_systest_data.json"

        client1 = clients[0]

        f = client1.remote_file(
            sudo=True,
            file_name=f"/home/cephuser/{file}",
            file_mode="r",
        )
        cephfs_config = json.load(f)
        log.info(f"cephfs_config:{cephfs_config}")

        io_tests = {
            "sv_test_workflow_1": "Create subvolumes,delete and create subvolumes in parallel",
            "clone_test_workflow_2": "Create clones,delete and create clones in parallel",
        }
        test_case_name = config.get("test_name", "all_tests")
        if test_case_name in io_tests:
            test_list = [test_case_name]
        else:
            test_list = io_tests.keys()
        proc_status_list = []
        write_procs = []
        io_test_fail = 0
        log_base_dir = os.path.dirname(log.logger.handlers[0].baseFilename)
        log_path = f"{log_base_dir}/sv_clone_subtests"
        try:
            os.mkdir(log_path)
        except BaseException as ex:
            log.info(ex)
            if "File exists" not in str(ex):
                return 1

        set_verify_clone_config(client1, clone_cnt)
        for io_test in test_list:
            log.info(f"Running {io_test} : {io_tests[io_test]}")
            io_proc_check_status = Value("i", 0)
            proc_status_list.append(io_proc_check_status)

            p = Thread(
                target=io_test_runner,
                args=(
                    io_proc_check_status,
                    io_test,
                    run_time,
                    sv_cnt,
                    clone_cnt,
                    log_path,
                    clients,
                    fs_system_utils,
                    fs_util,
                    cephfs_config,
                ),
            )
            p.start()
            write_procs.append(p)
        for write_proc in write_procs:
            write_proc.join()
        for proc_status in proc_status_list:
            if proc_status.value == 1:
                log.error(f"{io_test} failed")
                io_test_fail = 1
        if io_test_fail == 1:
            return 1

        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Stop logging the Ceph Cluster Cluster status to a log dir")


def io_test_runner(
    io_proc_check_status,
    io_test,
    run_time,
    sv_cnt,
    clone_cnt,
    log_path,
    clients,
    fs_system_utils,
    fs_util,
    cephfs_config,
):
    io_tests = {
        "sv_test_workflow_1": sv_test_workflow_1,
        "clone_test_workflow_2": clone_test_workflow_2,
    }
    for i in cephfs_config:
        if "CLUS_MONITOR" not in i:
            fs_name = i

    if io_test == "sv_test_workflow_1":
        client = random.choice(clients)
        io_proc_check_status = io_tests[io_test](
            run_time, fs_name, sv_cnt, log_path, client, fs_system_utils, fs_util
        )
    else:
        sv_info_list = []
        sv_name_list = []
        k = 0

        while len(sv_name_list) < 10 and k < 20:
            sv_info = fs_system_utils.get_test_object(cephfs_config, "shared")
            for i in sv_info:
                sv_name = i
            if sv_name not in sv_name_list:
                sv_info_list.append(sv_info)
                sv_name_list.append(sv_name)
            k += 1
        client = random.choice(clients)
        io_proc_check_status = io_tests[io_test](
            run_time,
            fs_name,
            clone_cnt,
            log_path,
            client,
            fs_system_utils,
            fs_util,
            sv_info_list,
        )

    log.info(f"{io_test} status after test : {io_proc_check_status}")
    return io_proc_check_status


def sv_test_workflow_1(
    run_time, fs_name, sv_cnt, log_path, client, fs_system_utils, fs_util
):
    log_name = "sv_create_delete_in_bulk"
    log1 = fs_system_utils.configure_logger(log_path, log_name)
    log1.info(f"Start {log_name}")
    cmd = f"ceph fs subvolumegroup ls {fs_name} --f json"
    out, _ = client.exec_command(sudo=True, cmd=cmd)
    output = json.loads(out)
    group_name = output[0]["name"]
    sv_rm_list = []
    k = 0
    while k < sv_cnt:
        rand_str = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(3))
        )
        sv_name = f"systest_sv_{rand_str}"
        subvolume = {
            "vol_name": fs_name,
            "subvol_name": sv_name,
            "group_name": group_name,
        }
        fs_util.create_subvolume(client, **subvolume, validate=False)
        log1.info(f"Created {sv_name} on {group_name}")
        sv_rm_list.append(sv_name)
        k += 1

    end_time = datetime.datetime.now() + datetime.timedelta(hours=run_time)
    cluster_healthy = 1

    while datetime.datetime.now() < end_time and cluster_healthy:
        i = 0
        while i <= sv_cnt:
            with parallel() as p:
                j = i + 100
                for n in range(i, j):
                    sv_name = sv_rm_list.pop(0)
                    subvolume = {
                        "vol_name": fs_name,
                        "subvol_name": sv_name,
                        "group_name": group_name,
                    }
                    p.spawn(
                        fs_util.remove_subvolume, client, **subvolume, validate=True
                    )
                    log1.info(f"started {sv_name} remove")
                    rand_str = "".join(
                        random.choice(string.ascii_lowercase + string.digits)
                        for _ in list(range(3))
                    )
                    sv_name = f"systest_sv_{rand_str}"
                    subvolume1 = {
                        "vol_name": fs_name,
                        "subvol_name": sv_name,
                        "group_name": group_name,
                    }
                    p.spawn(
                        fs_util.create_subvolume, client, **subvolume1, validate=False
                    )
                    log1.info(f"started {sv_name} create")
                    sv_rm_list.append(sv_name)
            i += 100
        time.sleep(30)
        cluster_healthy = is_cluster_healthy(client)
        log1.info(f"cluster_health {cluster_healthy}")

    log1.info("sv_test_workflow_1 completed")
    return 0


def clone_test_workflow_2(
    run_time,
    fs_name,
    clone_cnt,
    log_path,
    client,
    fs_system_utils,
    fs_util,
    sv_info_list,
):
    # Bulk Clone create and deletes in parallel
    log_name = "clone_bulk_create_delete"
    log1 = fs_system_utils.configure_logger(log_path, log_name)
    cluster_healthy = 1
    log1.info(f"Start {log_name} on {sv_info_list}")
    for sv_info in sv_info_list:
        for i in sv_info:
            sv_name = i
        snap_name = f"{sv_name}_snap"
        snapshot = {
            "vol_name": fs_name,
            "subvol_name": sv_name,
            "snap_name": snap_name,
            "group_name": sv_info[sv_name].get("group_name", None),
        }
        fs_util.create_snapshot(client, **snapshot, validate=False)
        log1.info(f"Snapshot {snap_name} created on {sv_name}")
        sv_info[sv_name].update({"snap_name": snap_name})
    k = 0
    clone_rm_list = []
    while k <= int(clone_cnt):
        with parallel() as p:
            for sv_info in sv_info_list:
                if k <= int(clone_cnt):
                    for i in sv_info:
                        sv_name = i
                    snap_name = sv_info[sv_name]["snap_name"]
                    rand_str = "".join(
                        random.choice(string.ascii_lowercase + string.digits)
                        for _ in list(range(3))
                    )
                    clone_name = f"{sv_name}_clone_{rand_str}"
                    clone_obj = {
                        "clone_name": clone_name,
                        "sv_name": sv_name,
                        "snap_name": snap_name,
                        "group_name": sv_info[sv_name].get("group_name", None),
                    }
                    clone_create = {
                        "vol_name": fs_name,
                        "subvol_name": sv_name,
                        "snap_name": snap_name,
                        "target_subvol_name": clone_name,
                        "group_name": sv_info[sv_name].get("group_name", None),
                        "target_group_name": sv_info[sv_name].get("group_name", None),
                    }
                    p.spawn(
                        fs_util.create_clone, client, **clone_create, validate=False
                    )
                    log1.info(f"Started {clone_name} on {sv_name}")
                    k += 1
                    clone_rm_list.append(clone_obj)
    end_time = datetime.datetime.now() + datetime.timedelta(hours=run_time)
    iter_cnt = 0
    while datetime.datetime.now() < end_time and cluster_healthy:
        rm_cmd_list = []
        clone_cmd_list = []
        for i in range(0, clone_cnt):
            clone_obj = clone_rm_list.pop(0)
            sv_name = clone_obj["sv_name"]
            clone_rm = {
                "vol_name": fs_name,
                "subvol_name": clone_obj["clone_name"],
                "group_name": clone_obj.get("group_name", None),
            }
            rm_cmd_list.append(clone_rm)
            rand_str = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(3))
            )
            clone_name = f"{sv_name}_clone_{rand_str}"
            clone_obj_new = {
                "sv_name": clone_obj["sv_name"],
                "clone_name": clone_name,
                "snap_name": clone_obj["snap_name"],
                "group_name": clone_obj.get("group_name", None),
            }
            clone_create = {
                "vol_name": fs_name,
                "subvol_name": clone_obj["sv_name"],
                "snap_name": clone_obj["snap_name"],
                "target_subvol_name": clone_name,
                "group_name": clone_obj.get("group_name", None),
                "target_group_name": clone_obj.get("group_name", None),
            }
            clone_cmd_list.append(clone_create)
            clone_rm_list.append(clone_obj_new)
        with parallel() as p:
            for clone_rm, clone_create in zip(rm_cmd_list, clone_cmd_list):
                sv_name = clone_rm["subvol_name"]
                p.spawn(fs_util.remove_subvolume, client, **clone_rm, validate=True)
                log1.info(f"Deleting {clone_rm['subvol_name']}")
                p.spawn(fs_util.create_clone, client, **clone_create, validate=False)
                log1.info(f"Creating {clone_create['subvol_name']}")
        time.sleep(30)
        log1.info(f"clone_test_workflow_1 iteration {iter_cnt} complete")
        iter_cnt += 1
        cluster_healthy = is_cluster_healthy(client)

    log1.info("clone_test_workflow_2 completed")
    return 0


def is_cluster_healthy(client):
    """
    returns '0' if failed, '1' if passed
    """
    file = "cephfs_systest_data.json"
    f = client.remote_file(
        sudo=True,
        file_name=f"/home/cephuser/{file}",
        file_mode="r",
    )
    cephfs_config = json.load(f)
    if cephfs_config.get("CLUS_MONITOR"):
        if cephfs_config["CLUS_MONITOR"] == "fail":
            return 0
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
    if str(concurrent_limit) not in out:
        log.error(
            f"Value for ceph config mgr/volumes/max_concurrent_clones is not {concurrent_limit}"
        )
        return 1
    return 0
