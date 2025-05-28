"""
This is cephfs Scale  utility module
It contains all the re-useable functions related to cephfs scale tests

"""

import gzip
import json
import logging
import os
import random
import re
import shutil
import string
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)
logger = logging.getLogger("run_log")


logging_thread = None
mds_logging_thread = None
stop_event = threading.Event()

results = {}


class CephfsScaleUtils(object):
    def __init__(self, ceph_cluster):
        """
        CephFS Scale Utility object
        Args:
            ceph_cluster (ceph.ceph.Ceph): ceph cluster
        """
        self.mons = ceph_cluster.get_ceph_objects("mon")
        self.mgrs = ceph_cluster.get_ceph_objects("mgr")
        self._mdss = ceph_cluster.get_ceph_objects("mds")
        self.osds = ceph_cluster.get_ceph_objects("osd")
        self.clients = ceph_cluster.get_ceph_objects("client")
        self.fs_util = FsUtils(ceph_cluster)

    def mount_and_create_subvolumes(
        self,
        fs_util_v1,
        clients,
        subvolume_count,
        default_fs,
        subvolume_name,
        subvolume_group_name,
        mount_type,
        nfs_cluster_name=None,
        nfs_server=None,
        nfs_server_ip=None,
    ):
        """
        Create subvolumes and mount them using the specified mount type.

        :param fs_util_v1: FsUtilsV1 object
        :param clients: List of client objects
        :param subvolume_count: Number of subvolumes to create
        :param default_fs: Default file system name
        :param subvolume_name: Base name for subvolumes
        :param subvolume_group_name: Name of the subvolume group
        :param mount_type: Type of mount to use ("fuse", "kernel", or "nfs")
        :param nfs_cluster_name: Name of the NFS cluster (required if mount_type is "nfs")
        :param nfs_server: NFS server IP or hostname (required if mount_type is "nfs")
        :return: List of dictionaries containing client, mount path, and subvolume name
        """

        if not clients:
            log.error("Clients list is empty. Aborting operation.")
            return []
        if mount_type == "nfs" and (not nfs_cluster_name or not nfs_server):
            log.error("NFS cluster name or server not provided for NFS mount.")
            return []

        log.info(
            f"Received {len(clients)} clients for subvolume creation and mounting."
        )

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(10)
        )
        subvolume_list = []
        for idx in range(1, subvolume_count + 1):
            name = f"{subvolume_name}_{idx}"
            fs_util_v1.create_subvolume(
                clients[0], default_fs, name, group_name=subvolume_group_name
            )
            fs_util_v1.enable_distributed_pin_on_subvolumes(
                clients[0],
                default_fs,
                subvolume_group_name,
                name,
                pin_type="distributed",
                pin_setting=1,
            )
            subvolume_list.append(name)

        # Divide subvolumes across clients
        subvols_per_client = subvolume_count // len(clients)
        remainder = subvolume_count % len(clients)
        start_idx = 0
        mount_paths = []

        for i, client in enumerate(clients):
            end_idx = start_idx + subvols_per_client + (1 if i < remainder else 0)
            assigned_subvols = subvolume_list[start_idx:end_idx]

            for sv in assigned_subvols:
                # Get subvolume path
                subvol_path, _ = client.exec_command(
                    sudo=True,
                    cmd=f"ceph fs subvolume getpath {default_fs} {sv} {subvolume_group_name}",
                )
                subvol_path = subvol_path.strip()

                # Define mount directory
                mount_dir = f"/mnt/cephfs_{mount_type}{mounting_dir}_{sv}/"
                client.exec_command(sudo=True, cmd=f"mkdir -p {mount_dir}")

                if mount_type == "fuse":
                    fs_util_v1.fuse_mount(
                        [client],
                        mount_dir,
                        extra_params=f" -r {subvol_path} --client_fs {default_fs}",
                    )

                elif mount_type == "kernel":
                    mon_ips = ",".join(fs_util_v1.get_mon_node_ips())
                    fs_util_v1.kernel_mount(
                        [client],
                        mount_dir,
                        mon_ips,
                        sub_dir=subvol_path,
                        extra_params=f",fs={default_fs}",
                    )

                elif mount_type == "nfs":
                    binding = f"/export_{sv}"
                    fs_util_v1.create_nfs_export(
                        client,
                        nfs_cluster_name,
                        binding,
                        default_fs,
                        path=subvol_path,
                    )
                    fs_util_v1.cephfs_nfs_mount(
                        client, nfs_server_ip, binding, mount_dir
                    )

                else:
                    log.error(f"Invalid mount type: {mount_type}")
                    return 1

                mount_paths.append(
                    {"client": client, "mount_path": mount_dir, "subvolume_name": sv}
                )
                log.info(
                    f"Mounted subvolume {sv} using {mount_type} at {mount_dir} for {client.node.hostname}"
                )

            start_idx = end_idx

        return mount_paths

    def collect_and_compress_mds_logs(self, clients, fs_util_v1, log_dir):
        """
        Collects MDS logs from the MDS nodes, compresses them, and removes the original files.

        Parameters:
        - clients: list of client nodes
        - fs_util_v1: filesystem utility instance
        - log_dir: directory where logs will be stored

        Returns:
        - list of compressed log file paths
        """
        mds_nodes = fs_util_v1.get_mds_nodes(clients)
        fsid = fs_util_v1.get_fsid(clients)
        log_files = []

        for mds in mds_nodes:
            file_list = mds.node.get_dir_list(f"/var/log/ceph/{fsid}/", sudo=True)
            log.info(file_list)
            for file_name in file_list:
                if "mds" in file_name:
                    src_path = os.path.join(f"/var/log/ceph/{fsid}", file_name)
                    dst_path = os.path.join(log_dir, file_name)
                    mds.download_file(src=src_path, dst=dst_path, sudo=True)
                    log_files.append(dst_path)

        compressed_log_files = []
        for file_path in log_files:
            gz_file_path = f"{file_path}.gz"
            with open(file_path, "rb") as f_in:
                with gzip.open(gz_file_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.remove(file_path)
            log.info(
                f"Compressed {file_path} to {gz_file_path} and deleted the original file"
            )
            compressed_log_files.append(gz_file_path)

        log.info("Log files compressed and original files deleted successfully.")
        return compressed_log_files

    def get_daemon_names(self, client, deamon_type):
        """
        Retrieve the names of daemons of a specified type.

        This function executes the "ceph orch ps" command to get the list of
        daemons in the Ceph cluster and filters the list to return the names
        of daemons that match the specified type.

        :param client: Client object to execute the command
        :param daemon_type: Type of daemon to filter (e.g., "mds", "osd", "mgr")
        :return: List of daemon names of the specified type
        """
        out, rc = client.exec_command(sudo=True, cmd="ceph orch ps --format json")
        json_data = json.loads(out)

        daemon_names = []
        for data in json_data:
            if data.get("daemon_type") == deamon_type:
                daemon_names.append(data.get("daemon_name"))
        return daemon_names

    def collect_ceph_details(self, client, cmd, log_dir):
        """
        Collect Ceph cluster details by executing the provided command and write the output to a file.

        :param client: Client object to execute the command
        :param cmd: Command to execute
        """
        out, rc = client.exec_command(sudo=True, cmd=f"{cmd} --format json")
        output = json.loads(out)

        log.info(f"Log Dir : {log_dir}")
        # Create a file name based on the command
        cmd_name = cmd.replace(" ", "_")
        log_file_path = os.path.join(log_dir, f"{cmd_name}.log")

        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        separator = "-" * 40

        with open(log_file_path, "a") as log_file:
            log_file.write(f"{current_time}\n")
            log_file.write(json.dumps(output, indent=4))
            log_file.write("\n")
            log_file.write(f"{separator}\n")

    def get_ceph_mds_top_output(self, ceph_cluster, log_dir):
        mds_nodes = ceph_cluster.get_ceph_objects("mds")
        for mds in mds_nodes:
            # Get  process ID of the ceph-mds process
            mds_hostnames = mds.node.hostname
            process_id_out = mds.exec_command(
                sudo=True,
                cmd="pgrep ceph-mds",
            )
            process_id = process_id_out[0].strip()
            log.info(f"Process ID : {process_id}")

            # Get the top output for the ceph-mds process
            top_out = mds.exec_command(
                sudo=True,
                cmd=f"top -b -n 1 -p {process_id}",
            )
            top_out1 = mds.exec_command(
                sudo=True,
                cmd=f"ps -p {process_id} -o rss=",
            )
            top_out_rss_value = top_out1[0].strip()
            mds_log_name = mds_hostnames.replace(" ", "_")
            log_file_path = os.path.join(
                log_dir, f"{mds_log_name}_ceph-mds_top_output.log"
            )
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            separator = "-" * 40

            with open(log_file_path, "a") as log_file:
                log_file.write(f"{current_time}\n")
                log_file.write(top_out[0])
                log_file.write("\n")
                log_file.write(top_out_rss_value)
                log_file.write("\n")
                log_file.write(f"{separator}\n")
            log.info(f"Top output of ceph-mds on {mds_hostnames} is {top_out[0]}")
            log.info(f"rss Value of ceph-mds on {mds_hostnames} is {top_out_rss_value}")

    def run_mds_commands_periodically(self, ceph_cluster, log_dir, interval=300):
        """
        Run the get_ceph_mds_top_output function periodically.

        :param ceph_cluster: Ceph cluster object
        :param interval: Time interval in seconds between each run
        """
        while not stop_event.is_set():
            self.get_ceph_mds_top_output(ceph_cluster, log_dir)
            stop_event.wait(interval)

    def start_mds_logging(self, ceph_cluster, log_dir):
        """
        Start the periodic logging of ceph-mds top output.

        :param ceph_cluster: Ceph cluster object
        """
        global mds_logging_thread, stop_event
        stop_event.clear()  # Clear the stop event before starting the thread
        mds_logging_thread = threading.Thread(
            target=self.run_mds_commands_periodically, args=(ceph_cluster, log_dir)
        )
        mds_logging_thread.daemon = (
            True  # Make the thread a daemon thread to exit when the main program exits
        )
        mds_logging_thread.start()

    def stop_mds_logging(self):
        """
        Stop the periodic logging of ceph-mds top output.
        """
        global stop_event
        stop_event.set()  # Set the stop event to stop the logging thread
        if mds_logging_thread is not None:
            mds_logging_thread.join()

    def run_commands_periodically(
        self,
        client,
        cmd_list,
        log_dir,
        interval=300,
    ):
        """
        Run the provided commands periodically.

        :param client: Client object to execute the commands
        :param cmd_list: List of commands to execute
        :param interval: Time interval in seconds between each run
        """

        while not stop_event.is_set():
            for cmd in cmd_list:
                self.collect_ceph_details(client, cmd, log_dir)
            stop_event.wait(interval)

    def start_logging(self, client, cmd_list, log_dir):
        """
        Start the logging of Ceph commands periodically.

        :param client: Client object to execute the commands
        :param cmd_list: List of commands to execute
        """
        global logging_thread, stop_event
        stop_event.clear()  # Clear the stop event before starting the thread
        logging_thread = threading.Thread(
            target=self.run_commands_periodically, args=(client, cmd_list, log_dir)
        )
        logging_thread.daemon = (
            True  # Make the thread a daemon thread to exit when the main program exits
        )
        logging_thread.start()

    def stop_logging(self):
        """
        Stop the periodic logging of Ceph commands.
        """
        global stop_event
        stop_event.set()  # Set the stop event to stop the logging thread
        if logging_thread is not None:
            logging_thread.join()

    def run_io_operations_parallel(
        self,
        mount_paths,
        io_type,
        io_operation=None,
        io_threads=None,
        io_file_size=None,
        io_files=None,
        fio_engine=None,
        fio_operation=None,
        fio_direct=None,
        fio_block_size=None,
        fio_depth=None,
        fio_file_size=None,
    ):
        """
        Run IO operations on the mounted paths in parallel based on the specified IO type.

        :param mount_paths: List of dictionaries containing client and mount path
        :param io_type: Type of IO operation ("smallfile", "largefile", or "fio")
        :param io_operation: Operation type for smallfile (e.g., create, read, etc.)
        :param io_threads: Number of threads for smallfile
        :param io_file_size: File size for smallfile
        :param io_files: Number of files for smallfile
        :param fio_engine: IO engine for FIO
        :param fio_operation: Operation type for FIO (e.g., read, write, randwrite, randread, etc.)
        :param fio_direct: Direct IO flag for FIO
        :param fio_block_size: Block size for FIO
        :param fio_depth: IO depth for FIO
        :param fio_file_size: File size for FIO
        :return: Dictionary with metrics outputs from IO operations
        """

        def run_smallfile_io(client, mount_path):
            """
            Run smallfile IO operations on the specified mount path.

            :param client: Client object to run the operation
            :param mount_path: Mount path to perform the IO operation
            :return: Dictionary with metrics from the IO operation
            """
            try:
                log.info(
                    f"Running IO operations on {mount_path} from client {client.node.hostname}"
                )
                log.info("Creating Directory for running smallfile writes")
                dirname_suffix = "".join(
                    random.choice(string.ascii_lowercase + string.digits)
                    for _ in list(range(2))
                )
                dir_name = f"smallfile_dir_{dirname_suffix}"
                client.exec_command(sudo=True, cmd=f"mkdir {mount_path}{dir_name}")
                smallfilepath = "/home/cephuser/smallfile/smallfile_cli.py"
                out, _ = client.exec_command(
                    sudo=True,
                    cmd=f"python3 {smallfilepath} --operation {io_operation} --threads {io_threads} "
                    f"--file-size {io_file_size} --files {io_files} "
                    f"--top {mount_path}{dir_name}",
                    timeout=36000,
                )
                log.info(f"smallfile out : {out}")
                log.info(
                    f"IO operation completed on {mount_path} from client {client.node.hostname}"
                )
                metrics = self.extract_smallfile_metrics(out)
                log.info(f"Metrics : {metrics}")

                return {client.node.hostname: metrics}

            except CommandFailed as e:
                log.error(
                    f"IO operation failed on {mount_path} from client {client.node.hostname}: {e}"
                )
                return {client.node.hostname: None}

            except Exception as e:
                log.error(
                    f"An unexpected error occurred on {mount_path} from client {client.node.hostname}: {e}"
                )
                return {client.node.hostname: None}

        def run_largefile_io(client, mount_path):
            """
            Run largefile IO operations on the specified mount path.

            :param client: Client object to run the operation
            :param mount_path: Mount path to perform the IO operation
            :return: None
            """
            log.info(
                f"Running IO operations on {mount_path} from client {client.node.hostname}"
            )
            try:
                client.exec_command(
                    sudo=True,
                    cmd=f"bash /root/create_large_file.sh {mount_path}",
                    timeout=36000,
                )
                log.info(
                    f"IO operation completed on {mount_path} from client {client.node.hostname}"
                )
            except CommandFailed as e:
                log.error(
                    f"IO operation failed on {mount_path} from client {client.node.hostname}: {e}"
                )

            except Exception as e:
                log.error(
                    f"An unexpected error occurred on {mount_path} from client {client.node.hostname}: {e}"
                )
                return {client.node.hostname: None}

        def run_fio(client, mount_path):
            """
            Run FIO operations on the specified mount path.

            :param client: Client object to run the operation
            :param mount_path: Mount path to perform the IO operation
            :return: None
            """
            try:
                log.info(
                    f"Running FIO IO operations on {mount_path} from client {client.node.hostname}"
                )
                log.info("Creating Directory for running fio writes")
                dirname_suffix = "".join(
                    random.choice(string.ascii_lowercase + string.digits)
                    for _ in list(range(2))
                )
                dir_name = f"fio_dir_{dirname_suffix}"
                client.exec_command(sudo=True, cmd=f"mkdir {mount_path}{dir_name}")
                out, err = client.exec_command(
                    sudo=True,
                    cmd=f"cd {mount_path}{dir_name}; fio --name={fio_operation} --rw={fio_operation}"
                    f" --direct={fio_direct} --ioengine={fio_engine} --bs={fio_block_size}"
                    f" --iodepth={fio_depth} --size={fio_file_size} --group_reporting=1"
                    f" --output=/root/fio_{client.node.hostname}.json",
                    timeout=36000,
                )
                log.info(
                    f"IO operation completed on {mount_path} from client {client.node.hostname}"
                )

            except CommandFailed as e:
                log.error(
                    f"IO operation failed on {mount_path} from client {client.node.hostname}: {e}"
                )
                return {client.node.hostname: None}

            except Exception as e:
                log.error(
                    f"An unexpected error occurred on {mount_path} from client {client.node.hostname}: {e}"
                )
                return {client.node.hostname: None}

        metrics_outputs = {}
        with ThreadPoolExecutor(max_workers=len(mount_paths)) as executor:
            if io_type == "smallfile":
                futures = [
                    executor.submit(
                        run_smallfile_io, mount["client"], mount["mount_path"]
                    )
                    for mount in mount_paths
                ]
            elif io_type == "largefile":
                futures = [
                    executor.submit(
                        run_largefile_io, mount["client"], mount["mount_path"]
                    )
                    for mount in mount_paths
                ]
            elif io_type == "fio":
                futures = [
                    executor.submit(run_fio, mount["client"], mount["mount_path"])
                    for mount in mount_paths
                ]
            else:
                raise ValueError(
                    "Invalid io_type specified. Use 'smallfile', 'largefile', or 'fio'."
                )

            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        metrics_outputs.update(result)
                except Exception as e:
                    log.error(f"Exception occurred during IO operation: {e}")

        log.info(f"Metrics Outputs : {metrics_outputs}")
        return metrics_outputs

    def extract_smallfile_metrics(self, output):
        try:
            output = str(output)  # Convert to string if not already
            # Regular expressions to match each metric
            total_threads_pattern = r"total threads = (\d+)"
            total_files_pattern = r"total files = (\d+)"
            total_iops_pattern = r"total IOPS = ([\d.]+)"
            total_data_pattern = r"total data =\s+([\d.]+\s+\w+)"
            elapsed_time_pattern = r"elapsed time =\s+([\d.]+)"
            files_per_sec_pattern = r"files/sec = ([\d.]+)"
            iops_pattern = r"IOPS = ([\d.]+)"
            mib_per_sec_pattern = r"MiB/sec = ([\d.]+)"

            # Initialize variables to store extracted values
            total_threads = None
            total_files = None
            total_iops = None
            total_data = None
            elapsed_time = None
            files_per_sec = None
            iops = None
            mib_per_sec = None

            # Search for each pattern in the output
            match = re.search(total_threads_pattern, output)
            if match:
                total_threads = int(match.group(1))

            match = re.search(total_files_pattern, output)
            if match:
                total_files = int(match.group(1))

            match = re.search(total_iops_pattern, output)
            if match:
                total_iops = float(match.group(1))

            match = re.search(total_data_pattern, output)
            if match:
                total_data = match.group(1)

            match = re.search(elapsed_time_pattern, output)
            if match:
                elapsed_time = float(match.group(1))

            match = re.search(files_per_sec_pattern, output)
            if match:
                files_per_sec = float(match.group(1))

            match = re.search(iops_pattern, output)
            if match:
                iops = float(match.group(1))

            match = re.search(mib_per_sec_pattern, output)
            if match:
                mib_per_sec = float(match.group(1))

            # Return a dictionary with extracted metrics
            return {
                "total_threads": total_threads,
                "total_files": total_files,
                "total_iops": total_iops,
                "total_data": total_data,
                "elapsed_time": elapsed_time,
                "files_per_sec": files_per_sec,
                "iops": iops,
                "mib_per_sec": mib_per_sec,
            }
        except Exception as e:
            log.info(f"Error in extract_metrics: {e}")
            return None

    def setup_log_dir(
        self,
        io_type,
        mount_type,
        client_count,
        subvolume_count,
        io_operation=None,
        io_threads=None,
        io_files=None,
        io_file_size=None,
        fio_operation=None,
        fio_depth=None,
        fio_block_size=None,
        fio_file_size=None,
    ):
        """
        Creates a log directory based on the I/O type and parameters.
        Returns the path to the log directory.
        """
        base_dir = os.path.dirname(log.logger.handlers[0].baseFilename)
        os.makedirs(f"{base_dir}/scale_logs", exist_ok=True)
        log_base_dir = f"{base_dir}/scale_logs"

        if io_type == "largefile":
            log_dir = f"{log_base_dir}/{io_type}_wr_{mount_type}_{client_count}_cl_{subvolume_count}_sv_100GB_file"
        elif io_type == "smallfile":
            log_dir = (
                f"{log_base_dir}/{io_type}_wr_{mount_type}_{client_count}_cl_{subvolume_count}_sv_"
                f"{io_operation}_ops_{io_threads}_th_{io_files}_files_{io_file_size}_size"
            )
        elif io_type == "fio":
            log_dir = (
                f"{log_base_dir}/{io_type}_{mount_type}_{client_count}_cl_{subvolume_count}_subv_"
                f"{fio_operation}_ops_{fio_depth}_depth_{fio_block_size}_bs_{fio_file_size}_size"
            )
        else:
            raise ValueError(f"Unsupported io_type: {io_type}")

        os.makedirs(log_dir, exist_ok=True)
        log.info(f"Log Dir : {log_dir}")

        return log_dir

    def collect_logs(self, ceph_cluster, clients, default_fs, fs_util_v1, log_dir):
        """
        Collects logs from the Ceph MDS nodes, enables debug logs, and starts logging.
        """
        log.info("Redirecting top output of all MDS nodes to a file")
        self.get_ceph_mds_top_output(ceph_cluster, log_dir)

        log.info("Enabling debug logs for all MDS daemons")
        fs_util_v1.enable_mds_logs(clients[0], default_fs)

        mds_list = self.get_daemon_names(clients[0], "mds")
        cmd_list = [
            "ceph crash ls",
            "ceph fs status",
            "ceph fs dump",
            "ceph -s",
            "ceph df",
        ]

        for daemon in mds_list:
            cmd_list.append(f"ceph tell {daemon} perf dump --format json")
        log.info(f"These ceph commands will run at an interval: {cmd_list}")

        log.info("Starting logging of Ceph Cluster status to log directory")
        self.start_logging(clients[0], cmd_list, log_dir)
        self.start_mds_logging(ceph_cluster, log_dir)

    def stop_all_logging(self):
        """
        Stops logging for Ceph cluster and MDS daemons.
        """
        log.info("Stopping logging of Ceph Cluster status")
        self.stop_logging()
        self.stop_mds_logging()

    def collect_and_compress_logs(self, clients, fs_util_v1, log_dir):
        """
        Collects and compresses MDS logs.
        """
        log.info("Collecting MDS Logs from all MDS Daemons")
        compressed_logs = self.collect_and_compress_mds_logs(
            clients, fs_util_v1, log_dir
        )
        return compressed_logs
