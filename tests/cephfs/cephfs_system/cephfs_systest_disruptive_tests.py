import datetime
import json
import logging
import os
import random
import string
import threading
import traceback
from multiprocessing import Value
from threading import Thread

from ceph.rados.utils import add_osd, osd_remove
from tests.cephfs.cephfs_system.cephfs_system_utils import CephFSSystemUtils
from utility.log import Log

log = Log(__name__)
logger = logging.getLogger("run_log")

logging_thread = None
mds_logging_thread = None
stop_event = threading.Event()


def run(ceph_cluster, **kw):
    """
    Disruptive tests : Run below workflows in parallel and repeat until runtime,

    1. Add OSD and run snapshot creation parallel, verify snapshot create suceeds
    2. Remove OSD and run snapshot creation parallel, verify snapshot create suceeds
    """
    try:
        fs_system_utils = CephFSSystemUtils(ceph_cluster)
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
        log.info("cephfs_config:%s", cephfs_config)

        disruptive_tests = {
            "add_osd_during_systest": "Add OSD and run snapshot create on test subvolumes in parallel",
            "remove_osd_during_systest": "Remove OSD and run snapshot create on test subvolumes in parallel",
        }
        disk_limit = {
            "add_osd_during_systest": config.get("disk_add_limit_pct", 70),
            "remove_osd_during_systest": config.get("disk_rm_limit_pct", 25),
        }
        test_case_name = config.get("test_name", "all_tests")
        if test_case_name in disruptive_tests:
            test_list = [test_case_name]
        else:
            test_list = disruptive_tests.keys()
        proc_status_list = []
        write_procs = []
        mds_test_fail = 0
        log_base_dir = os.path.dirname(log.logger.handlers[0].baseFilename)
        log_path = f"{log_base_dir}/disruptive_subtests"
        try:
            os.mkdir(log_path)
        except BaseException as ex:
            log.info(ex)
            if "File exists" not in str(ex):
                return 1

        for test_name in test_list:
            log.info("Running %s : %s", test_name, disruptive_tests[test_name])
            test_proc_check_status = Value("i", 0)
            proc_status_list.append(test_proc_check_status)
            disk_limit_pct = disk_limit[test_name]
            p = Thread(
                target=disruptive_test_runner,
                args=(
                    test_proc_check_status,
                    test_name,
                    run_time,
                    disk_limit_pct,
                    log_path,
                    clients,
                    fs_system_utils,
                    cephfs_config,
                ),
            )
            p.start()
            write_procs.append(p)
        for write_proc in write_procs:
            write_proc.join()
        for proc_status in proc_status_list:
            if proc_status.value == 1:
                log.error("%s failed", test_name)
                mds_test_fail = 1
        if mds_test_fail == 1:
            return 1

        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("No cleanup")


def disruptive_test_runner(
    test_proc_check_status,
    test_name,
    run_time,
    disk_limit,
    log_path,
    clients,
    fs_system_utils,
    cephfs_config,
):
    disruptive_tests = {
        "add_osd_during_systest": add_osd_during_systest,
        "remove_osd_during_systest": remove_osd_during_systest,
    }
    for i in cephfs_config:
        if "CLUS_MONITOR" not in i:
            fs_name = i

    client = random.choice(clients)
    test_proc_check_status.value = disruptive_tests[test_name](
        run_time, disk_limit, fs_name, log_path, client, fs_system_utils, cephfs_config
    )
    log.info("%s status after test : %s", test_name, test_proc_check_status.value)


def add_osd_during_systest(
    run_time, disk_add_limit, fs_name, log_path, client, fs_system_utils, cephfs_config
):
    log_name = "add_osd_during_test"
    log1 = fs_system_utils.configure_logger(log_path, log_name)
    ceph_cluster = getattr(fs_system_utils, "ceph_cluster")
    osd_nodes = ceph_cluster.get_ceph_objects("osd")
    log1.info(f"Start {log_name}")

    osd_nodes_1 = []
    osd_node_names = []
    for osd_node in osd_nodes:
        if osd_node.node.hostname not in osd_node_names:
            osd_nodes_1.append(osd_node)
            osd_node_names.append(osd_node.node.hostname)
    end_time = datetime.datetime.now() + datetime.timedelta(hours=run_time)
    cluster_healthy = is_cluster_healthy(client)
    osd_add_status = 0
    while datetime.datetime.now() < end_time and cluster_healthy:
        osd_add_info = {}
        # Check for cluster storage space usage
        disk_use_pct = fs_system_utils.get_disk_usage(client, fs_name)
        log1.info("Current disk usage : %s", disk_use_pct)
        available_disks = get_available_disks(client, osd_nodes_1)
        log1.info("available_disks : %s", available_disks)
        max_osd_id = get_max_osd_id(client)

        if (disk_use_pct >= int(disk_add_limit)) and (
            len(available_disks) == len(osd_nodes_1)
        ):
            # add OSD across all osd nodes
            for osd_node in osd_nodes_1:
                osd_node_name = osd_node.node.hostname
                if len(available_disks[osd_node_name]):
                    device_path = available_disks[osd_node_name][0]
                    max_osd_id += 1
                    # Run snapshot create task in background
                    p = Thread(
                        target=snap_create_op,
                        args=(client, cephfs_config, fs_system_utils),
                    )
                    p.start()
                    add_osd(ceph_cluster, osd_node_name, device_path, max_osd_id)
                    osd_add_info.update({osd_node_name: device_path})
                    if fs_system_utils.wait_for_proc(p, 300):
                        log1.error("Snapshot create background task didnt complete")
                        return 1
        if len(osd_nodes_1) == len(osd_add_info):
            osd_add_status += 1
        cluster_healthy = is_cluster_healthy(client)

    if osd_add_status == 0:
        log1.error(
            "Could not add OSD to all nodes in cluster, Added path list:%s",
            osd_add_info,
        )
        return 1
    log1.info("add_osd_during_systest completed")
    return 0


def remove_osd_during_systest(
    run_time, disk_rm_limit, fs_name, log_path, client, fs_system_utils, cephfs_config
):
    log_name = "remove_osd_during_test"
    log1 = fs_system_utils.configure_logger(log_path, log_name)
    ceph_cluster = getattr(fs_system_utils, "ceph_cluster")
    osd_nodes = ceph_cluster.get_ceph_objects("osd")
    log1.info(f"Start {log_name}")

    osd_nodes_1 = []
    osd_node_names = []
    for osd_node in osd_nodes:
        if osd_node.node.hostname not in osd_node_names:
            osd_nodes_1.append(osd_node)
            osd_node_names.append(osd_node.node.hostname)
    end_time = datetime.datetime.now() + datetime.timedelta(hours=run_time)
    cluster_healthy = is_cluster_healthy(client)
    osd_rm_status = 0

    while datetime.datetime.now() < end_time and cluster_healthy:
        osd_remove_info = {}
        # Check for cluster storage space usage
        disk_use_pct = fs_system_utils.get_disk_usage(client, fs_name)
        log.info("Current disk usage : %s", disk_use_pct)
        non_available_disks = get_non_available_disks(client, osd_nodes_1)
        log.info("non_available_disks : %s", non_available_disks)
        if (disk_use_pct <= int(disk_rm_limit)) and (
            len(non_available_disks) == len(osd_nodes_1)
        ):
            # Remove OSD across all osd nodes
            for osd_node in osd_nodes_1:
                osd_node_name = osd_node.node.hostname
                osd_id_list = non_available_disks[osd_node_name]
                osd_id = max(osd_id_list)
                if len(non_available_disks[osd_node_name]) >= 3:
                    # Run snapshot create task in background
                    p = Thread(
                        target=snap_create_op,
                        args=(client, cephfs_config, fs_system_utils),
                    )
                    p.start()
                    osd_remove(ceph_cluster, osd_id, zap=True)
                    osd_remove_info.update({osd_node_name: osd_id})
                    if fs_system_utils.wait_for_proc(p, 300):
                        log.error("Snapshot create background task didnt complete")
                        return 1
            
        if len(osd_nodes_1) == len(osd_remove_info):
            osd_rm_status += 1
        cluster_healthy = is_cluster_healthy(client)

    if osd_rm_status == 0:
        log.error(
            "Could not remove OSD from all nodes in cluster, Removed ID list:%s",
            osd_remove_info,
        )
        return 1
    log1.info("remove_osd_during_systest completed")
    return 0


# HELPER ROUTINES


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


def get_available_disks(client, osd_nodes):
    """
    This function will return device path of all disks available to be added
    """
    available_disks = {}
    for osd_node in osd_nodes:
        available_disks.update({osd_node.node.hostname: []})
        cmd = f"ceph orch device ls {osd_node.node.hostname} --wide --f json --refresh"
        out, _ = client.exec_command(sudo=True, cmd=cmd)
        parsed_data = json.loads(out)[0]
        device_list = []
        for i in range(0, len(parsed_data["devices"])):
            if parsed_data["devices"][i]["available"]:
                device_path = parsed_data["devices"][i]["path"]
                device_list.append(device_path)
        if len(device_list) > 0:
            available_disks.update({osd_node.node.hostname: device_list})
    return available_disks


def get_non_available_disks(client, osd_nodes):
    """
    This function will return list OSD IDs currently in use
    """
    non_available_disks = {}
    for osd_node in osd_nodes:
        non_available_disks.update({osd_node.node.hostname: []})
        cmd = f"ceph orch device ls {osd_node.node.hostname} --wide --f json --refresh"
        out, _ = client.exec_command(sudo=True, cmd=cmd)
        parsed_data = json.loads(out)[0]
        device_list = []
        for i in range(0, len(parsed_data["devices"])):
            if not parsed_data["devices"][i]["available"]:
                osd_id = parsed_data["devices"][i]["lvs"][0]["osd_id"]
                device_list.append(int(osd_id))
        if len(device_list) > 0:
            non_available_disks.update({osd_node.node.hostname: device_list})
    return non_available_disks


def get_max_osd_id(client):
    """
    This function will return max ID from list of OSD IDs
    """
    cmd = "ceph osd ls --f json"
    out, _ = client.exec_command(sudo=True, cmd=cmd)
    osd_ids = json.loads(out)
    max_osd_id = max(osd_ids)
    return max_osd_id


def snap_create_op(client, cephfs_config, fs_system_utils):
    """
    To create snapshot across all test subvolumes and validate
    """
    k = 0
    sv_list = []
    sv_info_list = []
    while len(sv_list) < 10 and k < 20:
        sv_info = fs_system_utils.get_test_object(cephfs_config, "shared")
        for i in sv_info:
            sv_name = i
        if sv_name not in sv_list:
            sv_info_list.append(sv_info)
            sv_list.append(sv_name)
        k += 1
    rand_str = "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in list(range(3))
    )
    for sv_info in sv_info_list:
        for i in sv_info:
            sv_name = i
        snap_name = f"{sv_name}_snap_osd_{rand_str}"
        snapshot = {
            "vol_name": sv_info[sv_name]["fs_name"],
            "subvol_name": sv_name,
            "snap_name": snap_name,
            "group_name": sv_info[sv_name].get("group_name", None),
        }
        fs_system_utils.fs_util.create_snapshot(client, **snapshot, validate=True)
