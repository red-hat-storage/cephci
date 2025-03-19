import datetime
import json
import logging
import os
import random
import re
import secrets
import string
import threading
import time
import traceback
from multiprocessing import Value
from threading import Thread

from ceph.parallel import parallel
from tests.cephfs.cephfs_IO_lib import FSIO

# from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_system.cephfs_system_utils import CephFSSystemUtils
from utility.log import Log

log = Log(__name__)
logger = logging.getLogger("run_log")

logging_thread = None
mds_logging_thread = None
stop_event = threading.Event()


def run(ceph_cluster, **kw):
    """
    System test - Client IO : Run below IO workflows in parallel on shared subvolumes.

    IO workflows:
    1. Read, write and getattr to same file from different client sessions
    2. Overwrite and Read to same file from different client sessions
    3. Truncate & Read to same file from different client sessions
    4. Random Read to same file from different client sessions
    5. Perform continuous overwrites on large files to generate Fragmented data
    6. Read(find) and delete(rm) in parallel to same file and concurrently to many files
    7. Scale number of requests/sec to an MDS until 6k
    8. unlink and rename to same file in parallel
    9. Client umount and mount in parallel
    10.Continuous IO for given run time such that request seq_num can overflow
    11.Download large file to cephfs mount that does read/write locks

    """
    try:
        fs_system_utils = CephFSSystemUtils(ceph_cluster)
        fs_io = FSIO(ceph_cluster)
        config = kw.get("config")
        cephfs_config = {}
        run_time = config.get("run_time_hrs", 4)
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
            "io_test_workflow_1": "Read, write and getattr to same file from different client sessions",
            "io_test_workflow_2": "Overwrite and Read to same file from different client sessions",
            "io_test_workflow_3": "Truncate & Read to same file from different client sessions",
            "io_test_workflow_4": "Random Read to same file from different client sessions",
            "io_test_workflow_5": "Perform continuous overwrites on large files to generate Fragmented data",
            "io_test_workflow_6": "Read(find) and delete(rm) in parallel to same file and concurrently to many files",
            "io_test_workflow_7": "Scale number of requests/sec to an MDS until 6k",
            "io_test_workflow_8": "unlink and rename to same file in parallel",
            "io_test_workflow_9": "Client umount and mount in parallel",
            "io_test_workflow_10": "Continuous IO for given run time such that request seq_num can overflow",
            "io_test_workflow_11": "Download large file to cephfs mount that does read/write locks",
            "io_test_workflow_12": "Run IOZone",
        }
        test_case_name = config.get("test_name", "all_tests")
        cleanup = config.get("cleanup", 0)
        if test_case_name in io_tests:
            test_list = [test_case_name]
        else:
            test_list = io_tests.keys()
        proc_status_list = []
        write_procs = []
        io_test_fail = 0
        log_base_dir = os.path.dirname(log.logger.handlers[0].baseFilename)
        log_path = f"{log_base_dir}/client_io_subtests"
        try:
            os.mkdir(log_path)
        except BaseException as ex:
            log.info(ex)
            if "File exists" not in str(ex):
                return 1

        for io_test in test_list:
            if (io_test == "io_test_workflow_7") and (
                "io_test_workflow_7" not in test_case_name
            ):
                log.info(
                    "Skipping TC io_test_workflow_7 as it cant be run in parallel to other workflows"
                )
                continue
            log.info(f"Running {io_test} : {io_tests[io_test]}")
            io_proc_check_status = Value("i", 0)
            proc_status_list.append(io_proc_check_status)

            p = Thread(
                target=io_test_runner,
                args=(
                    io_proc_check_status,
                    io_test,
                    run_time,
                    log_path,
                    cleanup,
                    clients,
                    fs_system_utils,
                    fs_io,
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
    log_path,
    cleanup,
    clients,
    fs_system_utils,
    fs_io,
    cephfs_config,
):
    io_tests = {
        "io_test_workflow_1": io_test_workflow_1,
        "io_test_workflow_2": io_test_workflow_2,
        "io_test_workflow_3": io_test_workflow_3,
        "io_test_workflow_4": io_test_workflow_4,
        "io_test_workflow_5": io_test_workflow_5,
        "io_test_workflow_6": io_test_workflow_6,
        "io_test_workflow_7": io_test_workflow_7,
        "io_test_workflow_8": io_test_workflow_8,
        "io_test_workflow_9": io_test_workflow_9,
        "io_test_workflow_10": io_test_workflow_10,
        "io_test_workflow_11": io_test_workflow_11,
        "io_test_workflow_12": io_test_workflow_12,
    }
    if io_test == "io_test_workflow_9" or io_test == "io_test_workflow_11":
        sv_info = fs_system_utils.get_test_object(cephfs_config, "shared")
        io_proc_check_status = io_tests[io_test](
            run_time, log_path, cleanup, clients, fs_system_utils, sv_info
        )
    else:
        sv_info_list = []
        sv_name_list = []
        k = 0
        if io_test == "io_test_workflow_7":
            log.info("Undo Ephemeral Random pinning")
            cmd = "ceph config set mds mds_export_ephemeral_random false;"
            clients[0].exec_command(sudo=True, cmd=cmd)
        while len(sv_name_list) < 10 and k < 20:
            sv_info = fs_system_utils.get_test_object(cephfs_config, "shared")
            for i in sv_info:
                sv_name = i
            if sv_name not in sv_name_list:
                sv_info_list.append(sv_info)
                sv_name_list.append(sv_name)
            k += 1
        if io_test == "io_test_workflow_12":
            io_proc_check_status = io_tests[io_test](
                run_time,
                log_path,
                cleanup,
                clients,
                fs_system_utils,
                fs_io,
                sv_info_list,
            )
        else:
            io_proc_check_status = io_tests[io_test](
                run_time, log_path, cleanup, clients, fs_system_utils, sv_info_list
            )
        if io_test == "io_test_workflow_7":
            log.info("Setup Ephemeral Random pinning")
            cmd = "ceph config set mds mds_export_ephemeral_random true;"
            cmd += "ceph config set mds mds_export_ephemeral_random_max 0.75"
            clients[0].exec_command(sudo=True, cmd=cmd)

    log.info(f"{io_test} status after test : {io_proc_check_status}")
    return io_proc_check_status


def io_test_workflow_1(
    run_time, log_path, cleanup, clients, fs_system_utils, sv_info_list
):
    # Read, write and getattr to same file from different client sessions
    log_name = "parallel_read_write_getattr"
    log1 = fs_system_utils.configure_logger(log_path, log_name)
    log1.info(f"Start {log_name} on {sv_info_list}")
    attr_list = [
        "ceph.file.layout",
        "ceph.file.layout.pool",
        "ceph.file.layout.stripe_unit",
    ]
    attr_list.extend(["ceph.file.layout.object_size", "ceph.file.layout.stripe_count"])
    sv_info_objs = {}
    for sv_info in sv_info_list:
        for i in sv_info:
            sv_name = i
        client_name1 = sv_info[sv_name]["mnt_client1"]
        client_name2 = sv_info[sv_name]["mnt_client2"]
        for i in clients:
            if client_name1 == i.node.hostname:
                client1 = i
            if client_name2 == i.node.hostname:
                client2 = i

        mnt_pt1 = sv_info[sv_name]["mnt_pt1"]
        dir_path1 = f"{mnt_pt1}client_io"
        cmd = f"mkdir -p {dir_path1}"
        log1.info(f"Executing cmd {cmd}")
        try:
            client1.exec_command(sudo=True, cmd=cmd)
        except BaseException as ex:
            log1.info(ex)
        mnt_pt2 = sv_info[sv_name]["mnt_pt2"]
        dir_path2 = f"{mnt_pt2}client_io"
        cmd = f"mkdir -p {dir_path2}"
        try:
            client2.exec_command(sudo=True, cmd=cmd)
        except BaseException as ex:
            log1.info(ex)
        sv_info_objs.update(
            {
                sv_name: {
                    "dir_path1": dir_path1,
                    "client1": client1,
                    "dir_path2": dir_path2,
                    "client2": client2,
                }
            }
        )
    # Run Read,Write and getattr in parallel
    log1.info(
        f"Run write,read and getattr in parallel, Repeat test until {run_time}hrs"
    )
    end_time = datetime.datetime.now() + datetime.timedelta(hours=run_time)
    cluster_healthy = 1
    while datetime.datetime.now() < end_time and cluster_healthy:
        rand_str = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(3))
        )
        with parallel() as p:
            for sv_name in sv_info_objs:
                client1 = sv_info_objs[sv_name]["client1"]
                dir_path1 = sv_info_objs[sv_name]["dir_path1"]
                client2 = sv_info_objs[sv_name]["client2"]
                dir_path2 = sv_info_objs[sv_name]["dir_path2"]

                file_path1 = f"{dir_path1}/io_test_workflow_1_{rand_str}"
                file_path2 = f"{dir_path2}/io_test_workflow_1_{rand_str}"
                file_ops = {
                    "write": f"fio --name={file_path1} --ioengine=libaio --size 10M --rw=write --direct=0",
                    "read": f"fio --name={file_path2} --ioengine=libaio --size 10M --rw=read --direct=0 --startdelay=1",
                }
                get_attr_cmds1 = "sleep 2;"
                get_attr_cmds2 = "sleep 2;"
                for attr in attr_list:
                    get_attr_cmds1 += f"getfattr -n {attr} {file_path1}*;"
                    get_attr_cmds2 += f"getfattr -n {attr} {file_path2}*;"
                if "nfs" not in mnt_pt1:
                    file_ops.update({"getattr1": f"{get_attr_cmds1}"})
                if "nfs" not in mnt_pt2:
                    file_ops.update({"getattr2": f"{get_attr_cmds2}"})
                client_op = {
                    "write": client1,
                    "read": client2,
                    "getattr1": client1,
                    "getattr2": client2,
                }
                for io_type in file_ops:
                    cmd = file_ops[io_type]
                    client = client_op[io_type]
                    p.spawn(run_cmd, cmd, client, log1)

        if cleanup == 1:
            with parallel() as p:
                for sv_name in sv_info_objs:
                    client1 = sv_info_objs[sv_name]["client1"]
                    dir_path1 = sv_info_objs[sv_name]["dir_path1"]
                    file_path1 = f"{dir_path1}/io_test_workflow_1_{rand_str}"
                    cmd = f"rm -f {file_path1}*"
                    log1.info(f"Running cmd {cmd}")
                    p.spawn(run_cmd, cmd, client1, log1)
                    client2 = sv_info_objs[sv_name]["client2"]
                    dir_path2 = sv_info_objs[sv_name]["dir_path2"]
                    file_path2 = f"{dir_path2}/io_test_workflow_1_{rand_str}"
                    cmd = f"rm -f {file_path2}*"
                    log1.info(f"Running cmd {cmd}")
                    p.spawn(run_cmd, cmd, client2, log1)
        time.sleep(30)
        cluster_healthy = is_cluster_healthy(clients[0])
        log1.info(f"cluster_health{cluster_healthy}")

    log1.info("io_test_workflow_1 completed")
    return 0


def io_test_workflow_2(
    run_time, log_path, cleanup, clients, fs_system_utils, sv_info_list
):
    # Overwrite and Read to same file from different client sessions
    log_name = "parallel_overwrite_read"
    log1 = fs_system_utils.configure_logger(log_path, log_name)
    smallfile_cmd = "python3 /home/cephuser/smallfile/smallfile_cli.py"
    sv_info_objs = {}
    end_time = datetime.datetime.now() + datetime.timedelta(hours=run_time)
    log1.info(f"Run overwrite and read in parallel, Repeat test until {run_time}hrs")
    cluster_healthy = 1
    log1.info(f"Start {log_name} on {sv_info_list}")
    while datetime.datetime.now() < end_time and cluster_healthy:
        rand_str = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(3))
        )
        with parallel() as p:
            for sv_info in sv_info_list:
                for i in sv_info:
                    sv_name = i
                client_name1 = sv_info[sv_name]["mnt_client1"]
                client_name2 = sv_info[sv_name]["mnt_client2"]
                for i in clients:
                    if client_name1 == i.node.hostname:
                        client1 = i
                    if client_name2 == i.node.hostname:
                        client2 = i
                mnt_pt1 = sv_info[sv_name]["mnt_pt1"]
                dir_path1 = f"{mnt_pt1}/client_io/io_test_workflow_2_{rand_str}"
                cmd = f"mkdir -p {dir_path1}"
                log1.info(f"Executing cmd {cmd}")
                client1.exec_command(sudo=True, cmd=cmd)
                mnt_pt2 = sv_info[sv_name]["mnt_pt2"]
                dir_path2 = f"{mnt_pt2}/client_io/io_test_workflow_2_{rand_str}"

                sv_info_objs.update(
                    {
                        sv_name: {
                            "dir_path1": dir_path1,
                            "client1": client1,
                            "dir_path2": dir_path2,
                            "client2": client2,
                        }
                    }
                )
                cmd = f"{smallfile_cmd} --operation create --threads 1 --file-size 10240 --files 1 --top {dir_path1}"
                log1.info(f"Executing cmd {cmd}")
                p.spawn(run_cmd, cmd, client1, log1)

        with parallel() as p:
            for sv_name in sv_info_objs:
                client1 = sv_info_objs[sv_name]["client1"]
                dir_path1 = sv_info_objs[sv_name]["dir_path1"]
                client2 = sv_info_objs[sv_name]["client2"]
                dir_path2 = sv_info_objs[sv_name]["dir_path2"]
                for client in [client1, client2]:
                    client.exec_command(
                        sudo=True, cmd=f"mkdir -p /var/tmp/smallfile_dir_{rand_str}"
                    )
                for io_type in ["overwrite", "read"]:
                    if io_type == "overwrite":
                        client = client1
                        dir_path = dir_path1
                    else:
                        client = client2
                        dir_path = dir_path2
                    cmd = f"{smallfile_cmd} --operation {io_type} --threads 1 --file-size 10240 --files 1 "
                    cmd += f"--top {dir_path} --network-sync-dir /var/tmp/smallfile_dir_{rand_str}"
                    p.spawn(run_cmd, cmd, client, log1)
        for sv_name in sv_info_objs:
            client1 = sv_info_objs[sv_name]["client1"]
            client2 = sv_info_objs[sv_name]["client2"]
            for client in [client1, client2]:
                client.exec_command(
                    sudo=True, cmd=f"rm -rf /var/tmp/smallfile_dir_{rand_str}"
                )
        log1.info("Completed test iteration")
        cluster_healthy = is_cluster_healthy(clients[0])
        log1.info(f"cluster_health{cluster_healthy}")
        if cleanup == 1:
            with parallel() as p:
                for sv_name in sv_info_objs:
                    client1 = sv_info_objs[sv_name]["client1"]
                    dir_path1 = sv_info_objs[sv_name]["dir_path1"]
                    cmd = f"rm -rf {dir_path1}"
                    p.spawn(run_cmd, cmd, client1, log1)
                    client2 = sv_info_objs[sv_name]["client2"]
                    dir_path2 = sv_info_objs[sv_name]["dir_path2"]
                    cmd = f"rm -rf {dir_path2}"
                    p.spawn(run_cmd, cmd, client2, log1)
    log1.info("io_test_workflow_2 completed")
    return 0


def io_test_workflow_3(
    run_time, log_path, cleanup, clients, fs_system_utils, sv_info_list
):
    # Truncate & Read to same file from different client sessions
    log_name = "parallel_truncate_read"
    log1 = fs_system_utils.configure_logger(log_path, log_name)
    smallfile_cmd = "python3 /home/cephuser/smallfile/smallfile_cli.py"
    sv_info_objs = {}
    end_time = datetime.datetime.now() + datetime.timedelta(hours=run_time)

    log1.info(f"Run Truncate and read in parallel, Repeat test until {run_time}hrs")
    cluster_healthy = 1
    while datetime.datetime.now() < end_time and cluster_healthy:
        rand_str = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(3))
        )
        with parallel() as p:
            for sv_info in sv_info_list:
                for i in sv_info:
                    sv_name = i
                client_name1 = sv_info[sv_name]["mnt_client1"]
                client_name2 = sv_info[sv_name]["mnt_client2"]
                for i in clients:
                    if client_name1 == i.node.hostname:
                        client1 = i
                    if client_name2 == i.node.hostname:
                        client2 = i

                mnt_pt1 = sv_info[sv_name]["mnt_pt1"]
                dir_path1 = f"{mnt_pt1}/client_io/io_test_workflow_3"
                cmd = f"mkdir -p {dir_path1}"
                log1.info(f"Executing cmd {cmd}")
                client1.exec_command(sudo=True, cmd=cmd)
                mnt_pt2 = sv_info[sv_name]["mnt_pt2"]
                dir_path2 = f"{mnt_pt2}/client_io/io_test_workflow_3"
                sv_info_objs.update(
                    {
                        sv_name: {
                            "dir_path1": dir_path1,
                            "client1": client1,
                            "dir_path2": dir_path2,
                            "client2": client2,
                        }
                    }
                )
                cmd = f"{smallfile_cmd} --operation create --threads 1 --file-size 10240 --files 1 --top {dir_path1}"
                log1.info(f"Executing cmd {cmd}")
                p.spawn(run_cmd, cmd, client1, log1)

        with parallel() as p:
            for sv_name in sv_info_objs:
                client1 = sv_info_objs[sv_name]["client1"]
                dir_path1 = sv_info_objs[sv_name]["dir_path1"]
                client2 = sv_info_objs[sv_name]["client2"]
                dir_path2 = sv_info_objs[sv_name]["dir_path2"]
                for client in [client1, client2]:
                    client.exec_command(
                        sudo=True, cmd=f"mkdir -p /var/tmp/smallfile_dir_{rand_str}"
                    )
                for io_type in ["read", "truncate-overwrite"]:
                    if io_type == "read":
                        client = client1
                        dir_path = dir_path1
                    else:
                        client = client2
                        dir_path = dir_path2
                    cmd = f"{smallfile_cmd} --operation {io_type} --threads 1 --file-size 10240 --files 1 "
                    cmd += f"--top {dir_path} --network-sync-dir /var/tmp/smallfile_dir_{rand_str}"

                    p.spawn(run_cmd, cmd, client, log1)
        with parallel() as p:
            for sv_name in sv_info_objs:
                client1 = sv_info_objs[sv_name]["client1"]
                dir_path1 = sv_info_objs[sv_name]["dir_path1"]
                client2 = sv_info_objs[sv_name]["client2"]
                dir_path2 = sv_info_objs[sv_name]["dir_path2"]
                for client in [client1, client2]:
                    client.exec_command(
                        sudo=True, cmd=f"rm -rf /var/tmp/smallfile_dir_{rand_str}"
                    )
                    if cleanup == 1:
                        cmd = f"rm -rf {dir_path1}"
                        p.spawn(run_cmd, cmd, client1, log1)
        cluster_healthy = is_cluster_healthy(clients[0])
        log1.info(f"cluster_health{cluster_healthy}")
    log1.info("io_test_workflow_3 completed")
    return 0


def io_test_workflow_4(
    run_time, log_path, cleanup, clients, fs_system_utils, sv_info_list
):
    # Random Read to same file from different client sessions
    log_name = "parallel_random_reads"
    log1 = fs_system_utils.configure_logger(log_path, log_name)
    sv_info_objs = {}
    with parallel() as p:
        for sv_info in sv_info_list:
            for i in sv_info:
                sv_name = i
            client_name1 = sv_info[sv_name]["mnt_client1"]
            client_name2 = sv_info[sv_name]["mnt_client2"]
            for i in clients:
                if client_name1 == i.node.hostname:
                    client1 = i
                if client_name2 == i.node.hostname:
                    client2 = i
            mnt_pt1 = sv_info[sv_name]["mnt_pt1"]
            dir_path1 = f"{mnt_pt1}/client_io"

            cmd = f"mkdir -p {dir_path1}"
            log1.info(f"Executing cmd {cmd}")
            try:
                client1.exec_command(sudo=True, cmd=cmd)
            except BaseException as ex:
                log1.info(ex)
            mnt_pt2 = sv_info[sv_name]["mnt_pt2"]
            dir_path2 = f"{mnt_pt2}/client_io"
            sv_info_objs.update(
                {
                    sv_name: {
                        "dir_path1": dir_path1,
                        "client1": client1,
                        "dir_path2": dir_path2,
                        "client2": client2,
                    }
                }
            )
            cmd = f"fio --name={dir_path1}/io_test_workflow_4 --ioengine=libaio --size 100M --rw=write --direct=0"
            log1.info(f"Create FIO file for random read test,executing cmd {cmd}")
            p.spawn(run_cmd, cmd, client1, log1)

    log1.info(f"Run random reads in parallel, Repeat test until {run_time}hrs")
    end_time = datetime.datetime.now() + datetime.timedelta(hours=run_time)
    cluster_healthy = 1
    while datetime.datetime.now() < end_time and cluster_healthy:
        with parallel() as p:
            for sv_name in sv_info_objs:
                client1 = sv_info_objs[sv_name]["client1"]
                dir_path1 = sv_info_objs[sv_name]["dir_path1"]
                client2 = sv_info_objs[sv_name]["client2"]
                dir_path2 = sv_info_objs[sv_name]["dir_path2"]

                for i in range(0, 5):
                    client = random.choice([client1, client2])
                    dir_path = (
                        dir_path1
                        if client.node.hostname == client1.node.hostname
                        else dir_path2
                    )
                    cmd = f"fio --name={dir_path}/io_test_workflow_4 --ioengine=libaio --size 100M --rw=randread"
                    cmd += " --direct=0"
                    log1.info(
                        f"Running cmd Iteration {i} on {sv_name} on {client.node.hostname}"
                    )
                    p.spawn(run_cmd, cmd, client, log1)
        if cleanup == 1:
            with parallel() as p:
                for sv_name in sv_info_objs:
                    dir_path = sv_info_objs[sv_name]["dir_path1"]
                    client = sv_info_objs[sv_name]["client1"]
                    cmd = f"rm -f {dir_path}/io_test_workflow_4*"
                    log1.info(f"Executing cmd {cmd}")
                    p.spawn(run_cmd, cmd, client, log1)
        cluster_healthy = is_cluster_healthy(clients[0])
        log1.info(f"cluster_health{cluster_healthy}")
    log1.info("io_test_workflow_4 completed")
    return 0


def io_test_workflow_5(
    run_time, log_path, cleanup, clients, fs_system_utils, sv_info_list
):
    # Perform continuous overwrites on large files to generate Fragmented data
    log_name = "continuous_overwrites_large_file"
    log1 = fs_system_utils.configure_logger(log_path, log_name)
    sv_info_objs = {}
    with parallel() as p:
        for sv_info in sv_info_list:
            for i in sv_info:
                sv_name = i
            client_name = sv_info[sv_name]["mnt_client1"]

            for i in clients:
                if client_name == i.node.hostname:
                    client = i
                    break
            mnt_pt = sv_info[sv_name]["mnt_pt1"]
            rand_str = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(3))
            )
            dir_path = f"{mnt_pt}/client_io"
            cmd = f"mkdir -p {dir_path}"
            log1.info(f"Executing cmd {cmd}")
            try:
                client.exec_command(sudo=True, cmd=cmd)
            except BaseException as ex:
                log1.info(ex)
            dir_path = f"{mnt_pt}/client_io/io_test_workflow_5_{rand_str}"
            sv_info_objs.update({sv_name: {"dir_path": dir_path, "client": client}})
            cmd = f"fio --name={dir_path} --ioengine=libaio --size 1Gb --rw=write"
            cmd += " --direct=0"
            log1.info(f"Executing cmd {cmd}")
            p.spawn(run_cmd, cmd, client, log1)

    log1.info(
        f"Run continuous overwrites on large file, Repeat test until {run_time}hrs"
    )
    end_time = datetime.datetime.now() + datetime.timedelta(hours=run_time)
    cluster_healthy = 1
    while datetime.datetime.now() < end_time and cluster_healthy:
        with parallel() as p:
            for sv_name in sv_info_objs:
                client = sv_info_objs[sv_name]["client"]
                dir_path = sv_info_objs[sv_name]["dir_path"]
                cmd = (
                    f"fio --name={dir_path} --ioengine=libaio --size 1Gb --rw=randwrite"
                )
                cmd += " --bs=10M --direct=0"
                log1.info(f"Executing cmd {cmd}")
                p.spawn(run_cmd, cmd, client, log1)

        cmd = "for osd in `ceph osd ls` ; do ceph tell osd.$osd bluestore allocator score block ; done"
        log1.info(f"Executing cmd {cmd}")
        out, _ = clients[0].exec_command(sudo=True, cmd=cmd)
        out = out.strip()
        out_list = out.split("}")
        frag_list = []
        for i in out_list:
            x = re.search(r"(\d+.\d+)", i)
            if x:
                y = round(float(x.group()), 2)
                frag_list.append(y)
        log1.info(f"Maximum Fragmentation seen across OSDs : {max(frag_list)}")
        time.sleep(10)
        cluster_healthy = is_cluster_healthy(clients[0])
        log1.info(f"cluster_health{cluster_healthy}")
    if cleanup == 1:
        with parallel() as p:
            for sv_name in sv_info_objs:
                dir_path = sv_info_objs[sv_name]["dir_path"]
                client = sv_info_objs[sv_name]["client"]
                cmd = f"rm -f {dir_path}"
                log1.info(f"Executing cmd {cmd}")
                p.spawn(run_cmd, cmd, client, log1)
    log1.info("io_test_workflow_5 completed")
    return 0


def io_test_workflow_6(
    run_time, log_path, cleanup, clients, fs_system_utils, sv_info_list
):
    # Read(find) and delete(rm) in parallel to same file and concurrently to many files
    log_name = "Parallel_find_delete_many_files"
    log1 = fs_system_utils.configure_logger(log_path, log_name)
    smallfile_cmd = "python3 /home/cephuser/smallfile/smallfile_cli.py"
    sv_info_objs = {}
    end_time = datetime.datetime.now() + datetime.timedelta(hours=run_time)
    log1.info(
        f"Run find and delete in parallel on many files, Repeat test until {run_time}hrs"
    )
    cluster_healthy = 1
    while datetime.datetime.now() < end_time and cluster_healthy:
        rand_str = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(3))
        )
        with parallel() as p:
            for sv_info in sv_info_list:
                for i in sv_info:
                    sv_name = i
                client_name = sv_info[sv_name]["mnt_client1"]

                for i in clients:
                    if client_name == i.node.hostname:
                        client = i
                        break
                mnt_pt = sv_info[sv_name]["mnt_pt1"]
                dir_path = f"{mnt_pt}/client_io/io_test_workflow_6_{rand_str}"

                cmd = f"mkdir -p {dir_path}"
                log1.info(f"Executing cmd {cmd}")
                try:
                    client.exec_command(sudo=True, cmd=cmd)
                except BaseException as ex:
                    log1.info(ex)

                sv_info_objs.update({sv_name: {"dir_path": dir_path, "client": client}})
                cmd = f"{smallfile_cmd} --operation create --threads 5 --file-size 10 --dirs-per-dir 50 "
                cmd += f"--same-dir false --files 1000000 --top {dir_path}"
                log1.info(f"Executing cmd {cmd}")
                p.spawn(run_cmd, cmd, client, log1)

        with parallel() as p:
            for sv_name in sv_info_objs:
                client = sv_info_objs[sv_name]["client"]
                dir_path = sv_info_objs[sv_name]["dir_path"]
                cmd = f"find {dir_path} -name *{client.node.hostname}* > {dir_path}/tmp_file"
                p.spawn(run_cmd, cmd, client, log1)

                cmd = f"{smallfile_cmd} --operation delete --threads 5 --file-size 10 --dirs-per-dir 50 "
                cmd += f"--files 1000000 --top {dir_path}"
                p.spawn(run_cmd, cmd, client, log1)
        if cleanup == 1:
            with parallel() as p:
                for sv_name in sv_info_objs:
                    dir_path = sv_info_objs[sv_name]["dir_path"]
                    client = sv_info_objs[sv_name]["client"]
                    cmd = f"rm -rf {dir_path}"
                    log1.info(f"Executing cmd {cmd}")
                    p.spawn(run_cmd, cmd, client, log1)
        cluster_healthy = is_cluster_healthy(clients[0])
        log1.info(f"cluster_health{cluster_healthy}")
    log1.info("io_test_workflow_6 completed")
    return 0


def io_test_workflow_7(
    run_time, log_path, cleanup, clients, fs_system_utils, sv_info_list
):
    # Scale number of requests/sec to an MDS until 6k
    log_name = "Scale_MDS_requests_to_6k"
    log1 = fs_system_utils.configure_logger(log_path, log_name)
    sv_info_objs = {}
    smallfile_cmd = "python3 /home/cephuser/smallfile/smallfile_cli.py"
    for sv_info in sv_info_list:
        for i in sv_info:
            sv_name = i
        client_name = sv_info[sv_name]["mnt_client1"]

        for i in clients:
            if client_name == i.node.hostname:
                client = i
                break
        mnt_pt = sv_info[sv_name]["mnt_pt1"]
        fs_name = sv_info[sv_name]["fs_name"]

        dir_path = f"{mnt_pt}/client_io/io_test_workflow_7"
        sv_info_objs.update({sv_name: {"dir_path": dir_path, "client": client}})

        cmd = f"mkdir -p {dir_path}"
        log1.info(f"Executing cmd {cmd}")
        try:
            client.exec_command(sudo=True, cmd=cmd)
        except BaseException as ex:
            log1.info(ex)
        cmd = f"{smallfile_cmd} --operation create --threads 5 --file-size 10 --files 1000000 --dirs-per-dir 50 "
        cmd += f"--same-dir false --top {dir_path}"
        log1.info(f"Executing cmd {cmd}")
        client.exec_command(
            sudo=True,
            cmd=cmd,
            long_running=True,
            timeout=3600,
        )

    log1.info(
        f"Run Scale test to increase IO requests to MDS until 6k, Repeat test until {run_time}hrs"
    )
    file_ops = ["read", "overwrite", "symlink", "stat"]
    request_limit = 6000
    end_time = datetime.datetime.now() + datetime.timedelta(hours=run_time)
    cluster_healthy = 1
    while datetime.datetime.now() < end_time and cluster_healthy:
        retry_cnt = 0
        mds_req_limit = Value("i", 0)
        rand_str = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(3))
        )
        for sv_name in sv_info_objs:
            client = sv_info_objs[sv_name]["client1"]
            try:
                client.exec_command(
                    sudo=True, cmd=f"mkdir -p /var/tmp/smallfile_dir_{rand_str}"
                )
            except BaseException as ex:
                log.info(ex)
        while (retry_cnt < 5) and (mds_req_limit.value == 0):
            log1.info(f"Iteration:{retry_cnt}")
            with parallel() as p:
                p.spawn(
                    check_mds_requests,
                    mds_req_limit,
                    log1,
                    request_limit,
                    fs_name,
                    client,
                    fs_system_utils,
                )
                for sv_name in sv_info_objs:
                    dir_path = sv_info_objs[sv_name]["dir_path"]
                    client = sv_info_objs[sv_name]["client"]
                    for op in file_ops:
                        cmd = f"{smallfile_cmd} --operation {op} --threads 5 --file-size 10 --files 1000000 "
                        cmd += "--dirs-per-dir 50 --same-dir false "
                        cmd += f"--top {dir_path} --network-sync-dir /var/tmp/smallfile_dir_{rand_str}"
                        p.spawn(run_cmd, cmd, client, log1)

            retry_cnt += 1
        cluster_healthy = is_cluster_healthy(clients[0])
        log1.info(f"cluster_health{cluster_healthy}")

    if mds_req_limit.value == 0:
        log1.error("MDS requests count not reach 6k")
    if cleanup == 1:
        with parallel() as p:
            for sv_name in sv_info_objs:
                dir_path = sv_info_objs[sv_name]["dir_path"]
                client = sv_info_objs[sv_name]["client"]
                cmd = f"rm -rf {dir_path}"
                log1.info(f"Executing cmd {cmd}")
                p.spawn(run_cmd, cmd, client, log1)
    log1.info("io_test_workflow_7 completed")
    return 0


def io_test_workflow_8(
    run_time, log_path, cleanup, clients, fs_system_utils, sv_info_list
):
    # unlink and rename to same file in parallel

    log_name = "parallel_unlink_rename"
    log1 = fs_system_utils.configure_logger(log_path, log_name)
    sv_info_objs = {}
    for sv_info in sv_info_list:
        for i in sv_info:
            sv_name = i
        client_name1 = sv_info[sv_name]["mnt_client1"]
        client_name2 = sv_info[sv_name]["mnt_client2"]
        for i in clients:
            if client_name1 == i.node.hostname:
                client1 = i
            if client_name2 == i.node.hostname:
                client2 = i
        mnt_pt1 = sv_info[sv_name]["mnt_pt1"]
        dir_path1 = f"{mnt_pt1}/client_io/io_test_workflow_8"
        cmd = f"mkdir -p {dir_path1}"
        log1.info(f"Executing cmd {cmd}")
        try:
            client1.exec_command(sudo=True, cmd=cmd)
        except BaseException as ex:
            log1.info(ex)
        mnt_pt2 = sv_info[sv_name]["mnt_pt2"]
        dir_path2 = f"{mnt_pt2}/client_io/io_test_workflow_8"
        sv_info_objs.update(
            {
                sv_name: {
                    "dir_path1": dir_path1,
                    "client1": client1,
                    "dir_path2": dir_path2,
                    "client2": client2,
                }
            }
        )
    log1.info(f"Run unlink and rename in parallel, Repeat test until {run_time}hrs")
    end_time = datetime.datetime.now() + datetime.timedelta(hours=run_time)
    cluster_healthy = 1
    while datetime.datetime.now() < end_time and cluster_healthy:
        for sv_name in sv_info_objs:
            dir_path1 = sv_info_objs[sv_name]["dir_path1"]
            client1 = sv_info_objs[sv_name]["client1"]
            dir_path2 = sv_info_objs[sv_name]["dir_path2"]
            client2 = sv_info_objs[sv_name]["client2"]
            cmd = f"fio --name={dir_path1}/symlink_parent --ioengine=libaio --size 10MB --rw=randwrite --direct=0"
            log1.info(f"Running cmd {cmd}")
            out, _ = client1.exec_command(sudo=True, cmd=cmd)
            log1.info(out)
            testfile = f"{dir_path1}/symlink_parent.0.0"
            cmd = f"mkdir {dir_path1}/symlink_dir"
            log1.info(f"Running cmd {cmd}")
            try:
                out, _ = client1.exec_command(sudo=True, cmd=cmd)
            except BaseException as ex:
                log1.info(ex)
            symlink_file1 = f"{dir_path1}/symlink_dir/symlink_file"
            symlink_file2 = f"{dir_path2}/symlink_dir/symlink_file"
            cmd = f"ln -s {testfile} {symlink_file1}"
            log1.info(f"Running cmd {cmd}")
            try:
                out, _ = client1.exec_command(sudo=True, cmd=cmd)
            except BaseException as ex:
                log1.info(ex)

            symlink_file_new = f"{dir_path2}/symlink_dir/symlink_file_new"
            file_ops = {
                "unlink": f"unlink {symlink_file1}",
                "rename": f"mv {symlink_file2} {symlink_file_new}",
            }

            sv_info_objs[sv_name].update({"file_ops": file_ops})
            cmd = f"ls {symlink_file1}"
            log1.info(f"Executing cmd {cmd}")
            try:
                out, _ = client1.exec_command(sudo=True, cmd=cmd)
                log1.info(out)
            except BaseException as ex:
                log1.info(ex)
        with parallel() as p:
            for sv_name in sv_info_objs:
                file_ops = sv_info_objs[sv_name]["file_ops"]
                client1 = sv_info_objs[sv_name]["client1"]
                client2 = sv_info_objs[sv_name]["client2"]
                for op_type in file_ops:
                    cmd = file_ops[op_type]
                    client = client1 if op_type == "unlink" else client2
                    try:
                        p.spawn(run_cmd, cmd, client, log1)
                    except BaseException as ex:
                        log.info(ex)
        time.sleep(10)
        for sv_name in sv_info_objs:
            dir_path = sv_info_objs[sv_name]["dir_path1"]
            client = sv_info_objs[sv_name]["client1"]
            cmd = f"rm -rf {dir_path}/symlink_*"
            log1.info(f"Executing cmd {cmd}")
            try:
                client.exec_command(sudo=True, cmd=cmd)
            except BaseException as ex:
                log.info(ex)
        cluster_healthy = is_cluster_healthy(clients[0])
        log1.info(f"cluster_health{cluster_healthy}")
    if cleanup == 1:
        with parallel() as p:
            for sv_name in sv_info_objs:
                dir_path = sv_info_objs[sv_name]["dir_path1"]
                client = sv_info_objs[sv_name]["client1"]
                cmd = f"rm -rf {dir_path}"
                log1.info(f"Executing cmd {cmd}")
                p.spawn(run_cmd, cmd, client, log1)
    log1.info("io_test_workflow_8 completed")
    return 0


def io_test_workflow_9(run_time, log_path, cleanup, clients, fs_system_utils, sv_info):
    # Client umount and mount in parallel
    log_name = "parallel_unmount_mount"
    log1 = fs_system_utils.configure_logger(log_path, log_name)
    for i in sv_info:
        sv_name = i
    client_name = sv_info[sv_name]["mnt_client1"]
    for i in clients:
        if client_name == i.node.hostname:
            client = i
            break

    fs_name = sv_info[sv_name]["fs_name"]
    fs_util = sv_info[sv_name]["fs_util"]
    cmd = f"ceph fs subvolume getpath {fs_name} {sv_name}"
    if sv_info.get("group_name"):
        cmd += f" {sv_info['group_name']}"
    log1.info(f"Executing cmd {cmd}")
    out, _ = client.exec_command(sudo=True, cmd=cmd)
    subvol_path = out.strip()
    mnt_type_list = ["kernel", "fuse", "nfs"]

    log1.info(
        f"Run mount and unmount in parallel test, Repeat test until {run_time}hrs"
    )
    end_time = datetime.datetime.now() + datetime.timedelta(hours=run_time)
    cluster_healthy = 1
    while datetime.datetime.now() < end_time and cluster_healthy:
        mount_params = {
            "fs_util": fs_util,
            "client": client,
            "mnt_path": subvol_path,
            "fs_name": fs_name,
            "export_created": 0,
        }
        mount_params_1 = mount_params.copy()
        mnt_type = random.choice(mnt_type_list)
        if "nfs" in mnt_type:
            if sv_info[sv_name].get("nfs_name"):
                log1.info(f"mnt_type:{mnt_type}")

                nfs_export = "/export_" + "".join(
                    secrets.choice(string.digits) for i in range(3)
                )
                nfs_export_name = f"{nfs_export}_{sv_name}"
                mount_params.update(
                    {
                        "nfs_name": sv_info[sv_name]["nfs_name"],
                        "nfs_export_name": nfs_export_name,
                        "nfs_server": sv_info[sv_name]["nfs_server"],
                    }
                )
            else:
                mnt_type = random.choice(["kernel", "fuse"])
        log1.info(f"Perform {mnt_type} mount of {sv_name}")
        mnt_path, _ = fs_util.mount_ceph(mnt_type, mount_params)
        if "nfs" in mnt_type:
            nfs_export1 = "/export1_" + "".join(
                secrets.choice(string.digits) for i in range(3)
            )
            nfs_export_name1 = f"{nfs_export1}_{sv_name}"
            mount_params_1.update(
                {
                    "nfs_name": sv_info[sv_name]["nfs_name"],
                    "nfs_export_name": nfs_export_name1,
                    "nfs_server": sv_info[sv_name]["nfs_server"],
                }
            )

        log1.info("Running mount and unmount in parallel")
        with parallel() as p:
            cmd = f"umount -l {mnt_path}"
            p.spawn(run_cmd, cmd, client, log1)
            mnt_path1, _ = fs_util.mount_ceph(mnt_type, mount_params_1)

        log1.info(
            f"Mount done in parallel to unmount:umount - {mnt_path},mount - {mnt_path1}"
        )
        time.sleep(5)
        cmd = f"umount -l {mnt_path1}"
        log1.info(f"Executing cmd {cmd}")
        try:
            out = client.exec_command(sudo=True, cmd=cmd)
        except BaseException as ex:
            log1.info(ex)
        cmd = f"rm -rf {mnt_path};rm -rf {mnt_path1}"
        try:
            out = client.exec_command(sudo=True, cmd=cmd)
        except BaseException as ex:
            log1.info(ex)
        time.sleep(10)
        cluster_healthy = is_cluster_healthy(clients[0])
        log1.info(f"cluster_health{cluster_healthy}")
    log1.info(f"cleanup:{cleanup}")
    log1.info("io_test_workflow_9 completed")
    return 0


def io_test_workflow_10(
    run_time, log_path, cleanup, clients, fs_system_utils, sv_info_list
):
    # Continuous IO for given run time such that request seq_num can overflow
    log_name = "Continuous_io_check_seq_num"
    log1 = fs_system_utils.configure_logger(log_path, log_name)
    sv_info_objs = {}
    for sv_info in sv_info_list:
        for i in sv_info:
            sv_name = i
        client_name = sv_info[sv_name]["mnt_client1"]
        for i in clients:
            if client_name == i.node.hostname:
                client = i
                break
        mnt_pt = sv_info[sv_name]["mnt_pt1"]
        dir_path = f"{mnt_pt}/io_test_workflow_10"
        sv_info_objs.update({sv_name: {"dir_path": dir_path, "client": client}})
    log1.info(
        f"Run continuous io and check request seq num test, Repeat test until {run_time}hrs"
    )
    smallfile_cmd = "python3 /home/cephuser/smallfile/smallfile_cli.py"
    end_time = datetime.datetime.now() + datetime.timedelta(hours=run_time)
    cluster_healthy = 1
    while datetime.datetime.now() < end_time and cluster_healthy:
        rand_str = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(3))
        )
        with parallel() as p:
            for sv_name in sv_info_objs:
                dir_path = sv_info_objs[sv_name]["dir_path"]
                dir_path += f"_{rand_str}"
                client = sv_info_objs[sv_name]["client"]
                cmd = f"mkdir {dir_path};"
                cmd += "for i in create read append read delete create overwrite rename delete-renamed mkdir rmdir "
                cmd += f"create symlink stat chmod ls-l delete cleanup ;do {smallfile_cmd} --operation $i --thread 2 "
                cmd += f"--file-size 2048 --files 10000 --dirs-per-dir 50 --same-dir false --top {dir_path}; done"
                log1.info(f"Executing cmd {cmd}")
                p.spawn(run_cmd, cmd, client, log1)
        if cleanup == 1:
            with parallel() as p:
                for sv_name in sv_info_objs:
                    dir_path = sv_info_objs[sv_name]["dir_path"]
                    dir_path += f"_{rand_str}"
                    client = sv_info_objs[sv_name]["client"]
                    cmd = f"rm -rf {dir_path}"
                    p.spawn(run_cmd, cmd, client, log1)
        cluster_healthy = is_cluster_healthy(clients[0])
        log1.info(f"cluster_health{cluster_healthy}")
    log1.info("io_test_workflow_10 completed")
    return 0


def io_test_workflow_11(run_time, log_path, cleanup, clients, fs_system_utils, sv_info):
    # 11.Download large file to cephfs mount that does read/write locks
    log_name = "Download_large_file_with_rw_locks"
    log1 = fs_system_utils.configure_logger(log_path, log_name)
    for i in sv_info:
        sv_name = i
    client_name = sv_info[sv_name]["mnt_client1"]
    for i in clients:
        if client_name in i.node.hostname:
            client = i
            break
    mnt_pt = sv_info[sv_name]["mnt_pt1"]

    run_time = 300
    log1.info(
        f"Run large file download that does read/write locks, Repeat test until {run_time}secs"
    )
    end_time = datetime.datetime.now() + datetime.timedelta(seconds=run_time)
    cluster_healthy = 1
    while datetime.datetime.now() < end_time and cluster_healthy:
        rand_str = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(3))
        )
        dir_path = f"{mnt_pt}/io_test_workflow_11_{rand_str}"
        cmd = f"mkdir {dir_path};touch {dir_path}/tmp_workflow11_data"
        client.exec_command(sudo=True, cmd=cmd)
        cmd = f"cd {dir_path};wget -O linux.tar.xz http://download.ceph.com/qa/linux-5.4.tar.gz > tmp_workflow11_data"
        log1.info(f"Executing cmd {cmd}")
        client.exec_command(
            sudo=True,
            cmd=cmd,
            long_running=True,
            timeout=3600,
        )
        if cleanup == 1:
            cmd = f"rm -rf {dir_path}"
            client.exec_command(sudo=True, cmd=cmd)
        cluster_healthy = is_cluster_healthy(clients[0])
        log1.info(f"cluster_health{cluster_healthy}")
    log1.info("io_test_workflow_11 completed")
    return 0


def io_test_workflow_12(
    run_time, log_path, cleanup, clients, fs_system_utils, fs_io, sv_info_list_tmp
):
    log_name = "run_iozone"
    log1 = fs_system_utils.configure_logger(log_path, log_name)
    sv_info_objs = {}
    sv_info_list = random.sample(sv_info_list_tmp, 1)
    for sv_info in sv_info_list:
        for i in sv_info:
            sv_name = i
        client_name = sv_info[sv_name]["mnt_client1"]
        for i in clients:
            if client_name == i.node.hostname:
                client = i
                break
        mnt_pt = sv_info[sv_name]["mnt_pt1"]
        dir_path = f"{mnt_pt}/io_test_workflow_12"
        sv_info_objs.update({sv_name: {"dir_path": dir_path, "client": client}})
    log1.info(f"Run iozone on subvolumes, Repeat test until {run_time}hrs")
    io_tools = ["iozone"]
    io_args = {
        "iozone_params": {"file_size": "4G", "reclen": "16K"},
    }
    end_time = datetime.datetime.now() + datetime.timedelta(hours=run_time)
    cluster_healthy = 1
    while datetime.datetime.now() < end_time and cluster_healthy:
        rand_str = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(3))
        )
        write_procs = []
        for sv_name in sv_info_objs:
            dir_path = sv_info_objs[sv_name]["dir_path"]
            dir_path += f"_{rand_str}"
            client = sv_info_objs[sv_name]["client"]
            cmd = f"mkdir {dir_path};"
            client.exec_command(sudo=True, cmd=cmd)
            p = Thread(
                target=fs_io.run_ios_V1,
                args=(
                    client,
                    dir_path,
                    io_tools,
                ),
                kwargs=io_args,
            )
            p.start()
            write_procs.append(p)
        if wait_procs(3600, write_procs):
            log.error("IOZONE test didnt complete in wait_time 3600secs")
            return 1
        if cleanup == 1:
            with parallel() as p:
                for sv_name in sv_info_objs:
                    dir_path = sv_info_objs[sv_name]["dir_path"]
                    dir_path += f"_{rand_str}"
                    client = sv_info_objs[sv_name]["client"]
                    cmd = f"rm -rf {dir_path}"
                    p.spawn(run_cmd, cmd, client, log1)
        time.sleep(30)
        cluster_healthy = is_cluster_healthy(clients[0])
        log1.info(f"cluster_health{cluster_healthy}")

    log1.info("io_test_workflow_12 completed")
    return 0


def run_cmd(cmd, client, logger):
    logger.info(f"Executing cmd {cmd}")
    try:
        client.exec_command(
            sudo=True,
            cmd=cmd,
            long_running=True,
            timeout=3600,
        )
    except BaseException as ex:
        logger.info(ex)


def wait_procs(wait_time, procs_list):
    for p in procs_list:
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=wait_time)
        proc_stop = 0
        while (datetime.datetime.now() < end_time) and (proc_stop == 0):
            if p.is_alive():
                time.sleep(10)
            else:
                proc_stop = 1
        if proc_stop == 0:
            return 1
    return 0


def check_mds_requests(
    mds_req_limit, logger, request_limit, fs_name, client, fs_system_utils
):
    logger.info(f"MDS requests check status:{mds_req_limit.value}")
    for i in range(0, 10):
        mds_requests = fs_system_utils.get_mds_requests(fs_name, client)
        if mds_requests >= request_limit:
            logger.info(f"MDS requests reached 6k:{mds_requests}")
            mds_req_limit.value = 1
            logger.info(f"MDS requests check status:{mds_req_limit.value}")
            break
        logger.info(f"MDS requests:{mds_requests}")
        logger.info(f"MDS requests check status:{mds_req_limit.value}")
        time.sleep(1)


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
