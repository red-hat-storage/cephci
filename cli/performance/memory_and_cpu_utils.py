import os
from logging import getLogger
from threading import Thread

from cli.utilities.configs import get_cephci_config

log = getLogger(__name__)


SCRIPT = "memory_and_cpu_logger.py"
SCRIPT_PATH = "cli/performance/utilities/"
SCRIPT_DST_PATH = "/home/"


def upload_mem_and_cpu_logger_script(cluster):
    """Check and upload memory_and_cpu_logger.py to servers if not present

    Args:
     cluster(ceph): List of all nodes where script has to be uploaded
    """
    is_present = []
    for node in cluster:
        # Copy file only if not present, avoid repetitive copy
        if SCRIPT not in node.get_dir_list(dir_path=f"{SCRIPT_DST_PATH}"):
            node.upload_file(
                sudo=True,
                src=f"{SCRIPT_PATH}{SCRIPT}",
                dst=f"{SCRIPT_DST_PATH}{SCRIPT}",
            )

            # Check if the upload was successful or not
            if SCRIPT not in node.get_dir_list(dir_path=f"{SCRIPT_DST_PATH}"):
                is_present.append(False)
                log.error(f"Failed to copy script to {node.hostname}")
            else:
                is_present.append(True)
        else:
            log.info("Memory monitoring script already present, skipping copy")
            is_present.append(True)
    return all(is_present)


def start_logging_processes(ceph_cluster, test_name):
    """
    Start logging processes on all nodes for a given process
    Args:
        ceph_cluster(ceph): Ceph cluster object
        test_name(str): Name of the test
        interval(int): Interval of monitoring
    """

    logging_process = []
    tracker = {}
    proc_mon_details = _get_process_list_to_monitor()
    interval = proc_mon_details["interval"]
    for ceph_process in proc_mon_details["process_list"]:
        role = ceph_process.replace("ceph-", "").strip()
        nodes = ceph_cluster.get_nodes(role)
        if not nodes:
            log.info(
                f"No nodes found specific to given process {ceph_process}, running on all nodes"
            )
            nodes = ceph_cluster.get_nodes()
        for node in nodes:
            log.info(f"Triggering monitoring for {ceph_process} on {node.hostname}")
            if node not in tracker.keys():
                tracker[node] = []
            node_specific_test_name = f"{node.hostname}-{test_name}"
            logger_cmd = (
                f"/usr/bin/env python {SCRIPT_DST_PATH}{SCRIPT} -p {ceph_process.strip()} "
                f"-t {node_specific_test_name} -i {interval}"
            )
            # Set the stop_flag as 0 before starting the test
            cmd = f"echo '0' > {SCRIPT_DST_PATH}status"
            node.exec_command(cmd=cmd, sudo=True)
            th = Thread(
                target=lambda: node.exec_command(
                    cmd=logger_cmd, sudo=True, long_running=True
                ),
                args=(),
            )
            logging_process.append(th)
            th.start()
            tracker[node].append(f"{ceph_process.strip()}-{node_specific_test_name}")
    return logging_process, tracker


def stop_logging_process(ceph_cluster, logging_process, download_path, tracker):
    """
    Stops the logging process
    Args:
        ceph_cluster(ceph): Ceph cluster object
        logging_process(dict): Dictionary of all the active logging processes
        download_path(str): Path to where the data has to be downloaded
        tracker(dict): Dict keeping track of each mon job
    """
    nodes = ceph_cluster.get_nodes()
    for node in nodes:
        # Set the stop_flag as 1 to stop the script running on the node
        cmd = f"echo '1' > {SCRIPT_DST_PATH}status"
        node.exec_command(cmd=cmd, sudo=True)

    # Wait for all the process to complete
    wait_for_logging_processes_to_stop(logging_process)

    # Collect the mem logs from all the nodes
    download_logger_data_from_nodes(download_path, tracker)


def wait_for_logging_processes_to_stop(logging_process):
    """Wait for all given logging processes to stop
    Args:
     logging_process(list): List of all the active logging processes
    """
    for th in logging_process:
        th.join()


def download_logger_data_from_nodes(download_path, tracker):
    """
    Downloads the logger data from all the nodes to the given path. The data will be collected and stored under folders
    specific to each test case
    Args:
        download_path(str): Path where the logs are to be downloaded
        tracker(dict): Tracker Dictionary containing nodes and files to be copied for each node
    """
    try:
        for node, files in tracker.items():
            for file in files:
                src_file = f"/root/{file}.csv"
                download_dir = (
                    f"{download_path}/performance-metrics/{file.split('-')[-1]}"
                )
                os.makedirs(download_dir, exist_ok=True)
                node.download_file(
                    src=src_file, dst=f"{download_dir}/{file}.csv", sudo=True
                )
                log.info(f"Downloading {file} from {node.hostname} to {download_dir}")
        log.info("All files downloaded from all the nodes")
    except Exception:
        log.error(f"Failed to download {file} from {node.hostname}")


def _get_process_list_to_monitor():
    config = get_cephci_config()
    perf_mon_data = config.get("performace_monitoring")
    return {
        "process_list": perf_mon_data.get("processes_to_monitor").split(","),
        "interval": perf_mon_data.get("interval"),
    }
