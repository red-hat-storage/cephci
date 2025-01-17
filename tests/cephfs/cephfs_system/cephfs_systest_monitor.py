import datetime
import json
import logging
import os
import threading
import time
import traceback

from tests.cephfs.cephfs_scale.cephfs_scale_utils import CephfsScaleUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from utility.log import Log

log = Log(__name__)
logger = logging.getLogger("run_log")

logging_thread = None
mds_logging_thread = None
stop_event = threading.Event()


def run(ceph_cluster, **kw):
    """
    This module is to monitor cluster during CephFS system testing for
    cluster health, crash info, cpu, memory and disk usage.
    It reports failure if ceph goes unhealthy, crash exists or any of
    cpu/mem/disk usage crossed the limit
    """
    try:
        fs_util_v1 = FsUtilsV1(ceph_cluster)
        fs_scale_utils = CephfsScaleUtils(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")

        mds_nodes = ceph_cluster.get_ceph_objects("mds")
        mem_limit = config.get("mds_mem_limit", 95)
        cpu_limit = config.get("mds_cpu_limit", 180)
        disk_limit = config.get("disk_limit", 80)
        mds_failover = config.get("mds_failover", 0)
        client = clients[0]
        update_system_status(client, "NA")
        mds_names_hostname = []
        for mds in mds_nodes:
            mds_names_hostname.append(mds.node.hostname)

        log_base_dir = os.path.dirname(log.logger.handlers[0].baseFilename)

        mds_list = fs_scale_utils.get_daemon_names(clients[0], "mds")
        cmd_list = [
            "ceph fs status",
            "ceph fs perf stats",
            "ceph -s",
            "ceph df",
        ]
        for daemon in mds_list:
            cmd_list.append(f"ceph tell {daemon} perf dump --format json")
        log.info(f"Ceph commands to run at an interval : {cmd_list}")
        log_dir = f"{log_base_dir}/ceph_monitor_logs"

        try:
            os.mkdir(log_dir)
        except BaseException as ex:
            log.info(ex)

        log.info("Start logging the Ceph Cluster Cluster status to a log dir")
        fs_scale_utils.start_logging(client, cmd_list, log_dir)
        run_time = config.get("run_time", 3600)
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=run_time)

        while datetime.datetime.now() < end_time:
            log.info("Check for Ceph Cluster health")
            clus_health = check_health(client, mds_failover=mds_failover)
            log.info("Check for CPU and MEM usage by MDS")
            cpu_mem_disk_use = check_mds_cpu_mem_disk_use(
                mem_limit, cpu_limit, disk_limit, client, fs_util_v1, mds_nodes
            )
            log.info("Check for crash")
            crash_status = check_crash_info(client)
            status_str = f"Cluster Health-{clus_health},CPU_MEM_DISK_usage-{cpu_mem_disk_use},Crash-{crash_status}"

            if (clus_health or cpu_mem_disk_use or crash_status) == 1:
                log.error(
                    f"Cluster monitor returned error, check status : {status_str}"
                )
                update_system_status(clients, "fail")
                return 1

            time.sleep(60)
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Stop logging the Ceph Cluster Cluster status to a log dir")
        fs_scale_utils.stop_logging()


def check_health(client, mds_failover=0):
    out, _ = client.exec_command(sudo=True, cmd="ceph status --f json")
    output = json.loads(out)
    exp_status = "HEALTH_OK"
    status = output["health"]["status"]
    log.info(f"status:{status}")
    if exp_status not in status:
        log.error(f"Ceph Health is not OK: {out}")
        out, _ = client.exec_command(sudo=True, cmd="ceph health detail")
        log.info(out)
        if mds_failover == 1 and "filesystem is degraded" in out:
            log.info(f"MDS failover tests have been run, so ignoring {status}")
            return 0
        return 1
    return 0


def check_mds_cpu_mem_disk_use(
    mem_limit, cpu_limit, disk_limit, client, fs_util, mds_nodes
):
    out, rc = client.exec_command(cmd="ceph fs ls --f json", client_exec=True)
    fs_list = json.loads(out)
    log.info(f"fs_list:{fs_list}")
    for fs_obj in fs_list:
        fs_name = fs_obj["name"]
        disk_usage = get_disk_usage(client, fs_name)
        out, rc = client.exec_command(
            cmd=f"ceph fs status {fs_name} --f json", client_exec=True
        )
        log.info(f"fs status:{out}")
        parsed_data = json.loads(out)

        for mds in parsed_data.get("mdsmap"):
            if mds.get("state") == "active":
                active_mds = mds["name"]
                log.info(f"Active MDS : {active_mds}")
                for mds_nodes_iter in mds_nodes:
                    log.info(
                        f"mds_nodes_iter.node.hostname:{mds_nodes_iter.node.hostname}"
                    )
                    if mds_nodes_iter.node.hostname in active_mds:
                        active_mds_node = mds_nodes_iter
                mds_mem_used_pct, mds_cpu_used_pct = fs_util.get_mds_cpu_mem_usage(
                    client, active_mds, active_mds_node
                )
                usage_str = (
                    f"MEM_LIMIT:{mem_limit},Current MEM in_USE:{mds_mem_used_pct} \t"
                )
                usage_str += (
                    f"CPU_LIMIT:{cpu_limit},Current CPU in_USE:{mds_cpu_used_pct} \t"
                )
                usage_str += f"DISK_LIMIT:{disk_limit},Current DISK in_USE:{disk_usage}"
                mds_mem_used_pct = float(mds_mem_used_pct)
                mds_cpu_used_pct = float(mds_cpu_used_pct)
                if (
                    (mds_mem_used_pct > mem_limit)
                    or (mds_cpu_used_pct > cpu_limit)
                    or (disk_usage > disk_limit)
                ):
                    log.info(usage_str)
                    log.error(
                        f"MEM/CPU/DISK usage has exceeded the user limit:\n {usage_str}"
                    )
                    return 1

                log.info(usage_str)
    return 0


def check_crash_info(client):
    out, _ = client.exec_command(sudo=True, cmd="ceph crash ls-new")
    if len(out) > 0:
        log.error(f"Crash status report:{out}")
        return 1
    return 0


def get_disk_usage(client, fs_name):
    out, rc = client.exec_command(cmd="ceph df --f json", client_exec=True)
    log.info(out)
    ceph_df = json.loads(out)
    disk_usage_list = []
    for pool in ceph_df["pools"]:
        if fs_name in pool["name"]:
            disk_usage_pct = pool["stats"]["percent_used"]
            disk_usage_pct = disk_usage_pct * 100
            if "e" in str(disk_usage_pct):
                disk_usage_pct = 0
            disk_usage_list.append(disk_usage_pct)
    return max(disk_usage_list)


def update_system_status(clients, status):
    for client in clients:
        file = "cephfs_systest_data.json"
        f = client.remote_file(
            sudo=True,
            file_name=f"/home/cephuser/{file}",
            file_mode="r",
        )
        cephfs_config = json.load(f)
        cephfs_config.update({"CLUS_MONITOR": status})
        f = client.remote_file(
            sudo=True,
            file_name=f"/home/cephuser/{file}",
            file_mode="w",
        )
        f.write(json.dumps(cephfs_config, indent=4))
        f.write("\n")
        f.flush()
