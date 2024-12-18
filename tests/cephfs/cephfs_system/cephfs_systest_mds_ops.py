import datetime
import json
import logging
import os
import random
import threading
import time
import traceback
from multiprocessing import Value
from threading import Thread

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
    MDS ops : Run below workflows in parallel and repeat until runtime,

    1. Perform MDS failover on list of Active MDS at regular interval.
    2. Perform MDS journal flush on Active MDS at regular interval but not during or just before/after MDS failover.

    """
    try:
        fs_system_utils = CephFSSystemUtils(ceph_cluster)
        fs_util = FsUtilsV1(ceph_cluster)
        config = kw.get("config")
        cephfs_config = {}
        run_time = config.get("run_time_hrs", 4)
        replay_interval = config.get("replay_interval_hrs", 4)
        flush_interval = config.get("flush_interval_hrs", 10)
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

        mds_tests = {
            "mds_test_workflow_1": "Failover Active MDSes at regular interval",
            "mds_test_workflow_2": "Flush MDS journal at regular interval",
        }
        test_case_name = config.get("test_name", "all_tests")
        if test_case_name in mds_tests:
            test_list = [test_case_name]
        else:
            test_list = mds_tests.keys()
        proc_status_list = []
        write_procs = []
        mds_test_fail = 0
        log_base_dir = os.path.dirname(log.logger.handlers[0].baseFilename)
        log_path = f"{log_base_dir}/mds_subtests"
        try:
            os.mkdir(log_path)
        except BaseException as ex:
            log.info(ex)
            if "File exists" not in str(ex):
                return 1

        for mds_test in test_list:
            if (mds_test == "mds_test_workflow_2") and (
                int(run_time) < int(flush_interval)
            ):
                log.info("Run time is less than Flush interval,skipping test")
                continue

            log.info(f"Running {mds_test} : {mds_tests[mds_test]}")
            mds_proc_check_status = Value("i", 0)
            proc_status_list.append(mds_proc_check_status)

            p = Thread(
                target=mds_test_runner,
                args=(
                    mds_proc_check_status,
                    mds_test,
                    run_time,
                    replay_interval,
                    flush_interval,
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
                log.error(f"{mds_test} failed")
                mds_test_fail = 1
        if mds_test_fail == 1:
            return 1

        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Stop logging the Ceph Cluster Cluster status to a log dir")


def mds_test_runner(
    mds_proc_check_status,
    mds_test,
    run_time,
    replay_interval,
    flush_interval,
    log_path,
    clients,
    fs_system_utils,
    fs_util,
    cephfs_config,
):
    mds_tests = {
        "mds_test_workflow_1": mds_test_workflow_1,
        "mds_test_workflow_2": mds_test_workflow_2,
    }
    for i in cephfs_config:
        if "CLUS_MONITOR" not in i:
            fs_name = i

    if mds_test == "mds_test_workflow_2":
        client = random.choice(clients)
        mds_proc_check_status = mds_tests[mds_test](
            run_time, fs_name, flush_interval, log_path, client, fs_system_utils
        )
    else:
        client = random.choice(clients)
        mds_proc_check_status = mds_tests[mds_test](
            run_time,
            fs_name,
            replay_interval,
            log_path,
            client,
            fs_system_utils,
            fs_util,
        )

    log.info(f"{mds_test} status after test : {mds_proc_check_status}")
    return mds_proc_check_status


def mds_test_workflow_1(
    run_time, fs_name, replay_interval, log_path, client, fs_system_utils, fs_util
):
    log_name = "mds_failover_ops"
    log1 = fs_system_utils.configure_logger(log_path, log_name)
    log1.info(f"Start {log_name}")
    mds_interval_secs = int(replay_interval) * 60 * 60
    end_time = datetime.datetime.now() + datetime.timedelta(hours=run_time)
    cluster_healthy = 1

    while datetime.datetime.now() < end_time and cluster_healthy:
        time.sleep(mds_interval_secs)
        log1.info("Get active MDSes")
        mds_ls = fs_util.get_active_mdss(client, fs_name=fs_name)
        log1.info("Perform rolling failures of active MDS")
        out, rc = client.exec_command(
            sudo=True, cmd=f"ceph fs status {fs_name} --format json"
        )
        log1.info(out)
        output = json.loads(out)
        standby_mds = [
            mds["name"] for mds in output["mdsmap"] if "standby" in mds["state"]
        ]
        sample_cnt = min(len(mds_ls), len(standby_mds))
        mds_to_fail = random.sample(mds_ls, sample_cnt)
        for mds in mds_to_fail:
            out, rc = client.exec_command(cmd=f"ceph mds fail {mds}", client_exec=True)
            log1.info(out)
            time.sleep(1)

        log1.info(
            f"Waiting for atleast 2 active MDS after failing {len(mds_to_fail)} MDS"
        )
        if not fs_system_utils.wait_for_two_active_mds(client, fs_name):
            log1.error("Wait for 2 active MDS failed")
            return 1
        out, rc = client.exec_command(cmd=f"ceph fs status {fs_name}", client_exec=True)
        log1.info(f"Status of {fs_name} after MDS failover:\n {out}")
        out, rc = client.exec_command(cmd="ceph -s -f json", client_exec=True)
        ceph_status = json.loads(out)
        log1.info(
            f"Ceph status after MDS failover: {json.dumps(ceph_status, indent=4)}"
        )
        if "HEALTH_OK" not in ceph_status["health"]["status"]:
            log1.error(f"Ceph Health is NOT OK after mds{mds} failover")
            return 1

        cluster_healthy = is_cluster_healthy(client)
        log1.info(f"cluster_health {cluster_healthy}")
    log1.info("mds_test_workflow_1 completed")
    return 0


def mds_test_workflow_2(
    run_time, fs_name, flush_interval, log_path, client, fs_system_utils
):
    log_name = "mds_journal_ops"
    log1 = fs_system_utils.configure_logger(log_path, log_name)
    end_time = datetime.datetime.now() + datetime.timedelta(hours=run_time)
    mds_interval_secs = int(flush_interval) * 60 * 60
    cluster_healthy = 1
    log1.info(f"Start {log_name}")

    while datetime.datetime.now() < end_time and cluster_healthy:
        time.sleep(mds_interval_secs)
        mds_perf_before_flush = {}
        mds_perf_after_flush = {}

        out, rc = client.exec_command(
            cmd=f"ceph fs status {fs_name} --f json", client_exec=True
        )
        mds_out = json.loads(out)
        log1.info("Get the journal stats before flush")
        for mds in mds_out["mdsmap"]:
            if mds["state"] == "active":
                out, rc = client.exec_command(
                    cmd=f"ceph tell mds.{mds['rank']} perf dump -f json",
                    client_exec=True,
                )
                mds_perf = json.loads(out)
                mds_perf_before_flush.update({mds["rank"]: mds_perf["mds_log"]})

        for mds_rank in mds_perf_before_flush:
            rdpos = mds_perf_before_flush[mds_rank]["rdpos"]
            wrpos = mds_perf_before_flush[mds_rank]["wrpos"]
            expos = mds_perf_before_flush[mds_rank]["expos"]
            log1.info(
                f"MDS Rank {mds_rank} : expos - {expos}, wrpos - {wrpos}, rdpos - {rdpos}"
            )

        log1.info("Flush the journal for Active MDS")
        for mds in mds_out["mdsmap"]:
            if mds["state"] == "active":
                out, rc = client.exec_command(
                    cmd=f"ceph tell mds.{mds['rank']} flush journal", client_exec=True
                )

        log1.info("Get the journal stats after flush")
        for mds in mds_out["mdsmap"]:
            if mds["state"] == "active":
                out, rc = client.exec_command(
                    cmd=f"ceph tell mds.{mds['rank']} perf dump -f json",
                    client_exec=True,
                )
                mds_perf = json.loads(out)
                mds_perf_after_flush.update({mds["rank"]: mds_perf["mds_log"]})

        for mds_rank in mds_perf_after_flush:
            rdpos = mds_perf_after_flush[mds_rank]["rdpos"]
            wrpos = mds_perf_after_flush[mds_rank]["wrpos"]
            expos = mds_perf_after_flush[mds_rank]["expos"]
            log1.info(
                f"MDS Rank {mds_rank} : expos - {expos}, wrpos - {wrpos}, rdpos - {rdpos}"
            )

            if (
                mds_perf_after_flush[mds_rank]["expos"]
                == mds_perf_before_flush[mds_rank]["wrpos"]
            ):
                log1.info(f"Journal flush Validated for MDS {mds_rank}")
            else:
                log1.error(
                    "Journal flush validation failed, please check journal stats before and after flush above"
                )
                return 1
        cluster_healthy = is_cluster_healthy(client)

    log1.info("mds_test_workflow_2 completed")
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
