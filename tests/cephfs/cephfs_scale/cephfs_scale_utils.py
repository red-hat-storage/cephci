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
from itertools import islice

import yaml

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
        **kwargs,
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
        :param nfs_cluster_name: Name of the NFS cluster (used if mount_type is "nfs" and multiple_cluster is False)
        :param nfs_server: NFS server IP or hostname (used for single-cluster NFS mount)
        :param nfs_server_ip: NFS server IP (used for single or multiple NFS clusters)
        :param multiple_cluster: Whether multiple NFS clusters are used (default: False)
        :param nfs_cluster_names: List of NFS cluster names (required if multiple_cluster=True)
        :param nfs_server_ips: List of NFS server IPs (optional; should align with nfs_cluster_names)
        :param start_port: Starting port for NFS clusters (used to assign different ports per cluster)
        :param byok_enabled: Whether to create subvolumes using encryption (default: False)
        :param key_id: KMIP Key ID used for BYOK subvolumes (required if byok_enabled=True)
        :return: List of dictionaries with client, mount_path, and subvolume_name
        """

        if not clients:
            log.error("Clients list is empty. Aborting operation.")
            return []

        if subvolume_count == 0:
            log.warning("Subvolume count is zero. Nothing to create or mount.")
            return []

        multiple_cluster = kwargs.get("multiple_cluster", False)
        nfs_clusters = kwargs.get("nfs_cluster_names", [nfs_cluster_name])
        nfs_server_ips = kwargs.get("nfs_server_ips", [nfs_server_ip])
        start_port = kwargs.get("start_port", 2049)
        byok_enabled = kwargs.get("byok_enabled", False)
        key_id = kwargs.get("key_id") if byok_enabled else None

        if mount_type == "nfs" and not multiple_cluster:
            if not nfs_cluster_name or not nfs_server:
                log.error(
                    "NFS cluster name or server not provided for single-cluster NFS mount."
                )
                return []

        if multiple_cluster and not nfs_clusters:
            log.error("Multiple cluster enabled but cluster list is empty.")
            return []

        if multiple_cluster:
            log.info(
                "Distributing subvolumes across %s NFS clusters", len(nfs_clusters)
            )

        log.info(
            "Received %s clients for subvolume creation and mounting.",
            len(clients),
        )

        mount_id = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(10)
        )

        cluster_port_map = {
            nfs_clusters[i]: start_port + i for i in range(len(nfs_clusters))
        }

        subvolume_list = []
        for idx in range(1, subvolume_count + 1):
            subvol_name = "%s_%s" % (subvolume_name, idx)
            if byok_enabled and key_id:
                fs_util_v1.create_subvolume(
                    clients[0],
                    default_fs,
                    subvol_name,
                    group_name=subvolume_group_name,
                    extra_params="--enctag %s" % key_id,
                )
            else:
                fs_util_v1.create_subvolume(
                    clients[0], default_fs, subvol_name, group_name=subvolume_group_name
                )

            fs_util_v1.enable_distributed_pin_on_subvolumes(
                clients[0],
                default_fs,
                subvolume_group_name,
                subvol_name,
                pin_type="distributed",
                pin_setting=1,
            )
            subvolume_list.append(subvol_name)

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
                    cmd="ceph fs subvolume getpath %s %s %s"
                    % (default_fs, sv, subvolume_group_name),
                )
                subvol_path = subvol_path.strip()

                # Define mount directory
                mount_dir = "/mnt/cephfs_%s%s_%s/" % (mount_type, mount_id, sv)
                client.exec_command(sudo=True, cmd="mkdir -p %s" % mount_dir)

                if mount_type == "fuse":
                    fs_util_v1.fuse_mount(
                        [client],
                        mount_dir,
                        extra_params=" -r %s --client_fs %s"
                        % (subvol_path, default_fs),
                    )

                elif mount_type == "kernel":
                    mon_ips = ",".join(fs_util_v1.get_mon_node_ips())
                    fs_util_v1.kernel_mount(
                        [client],
                        mount_dir,
                        mon_ips,
                        sub_dir=subvol_path,
                        extra_params=",fs=%s" % default_fs,
                    )

                elif mount_type == "nfs":
                    binding = "/export_%s" % sv
                    cluster_index = subvolume_list.index(sv) % len(nfs_clusters)
                    active_nfs_cluster = nfs_clusters[cluster_index]
                    active_nfs_ip = (
                        nfs_server_ips[cluster_index]
                        if len(nfs_server_ips) > cluster_index
                        else nfs_server_ip
                    )

                    export_kwargs = {}
                    if byok_enabled:
                        export_kwargs["extra_args"] = "--kmip_key_id=%s" % key_id

                    fs_util_v1.create_nfs_export(
                        client,
                        active_nfs_cluster,
                        binding,
                        default_fs,
                        path=subvol_path,
                        **export_kwargs,
                    )

                    fs_util_v1.cephfs_nfs_mount(
                        client,
                        active_nfs_ip,
                        binding,
                        mount_dir,
                        port=cluster_port_map.get(active_nfs_cluster, 2049),
                    )

                else:
                    log.error("Invalid mount type: %s", mount_type)
                    return 1

                mount_paths.append(
                    {"client": client, "mount_path": mount_dir, "subvolume_name": sv}
                )
                log.info(
                    "Mounted subvolume %s using %s at %s for %s"
                    % (sv, mount_type, mount_dir, client.node.hostname)
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
            try:
                file_list = mds.node.get_dir_list("/var/log/ceph/%s/" % fsid, sudo=True)
                log.info("%s MDS log list: %s" % (mds.node.hostname, file_list))
                for file_name in file_list:
                    if file_name.startswith("mds.") and file_name.endswith(".log"):
                        src_path = os.path.join("/var/log/ceph/%s" % fsid, file_name)
                        dst_path = os.path.join(log_dir, file_name)
                        try:
                            mds.download_file(src=src_path, dst=dst_path, sudo=True)
                            log_files.append(dst_path)
                        except Exception as e:
                            log.warning(
                                "Failed to download %s from %s: %s"
                                % (src_path, mds.node.hostname, e)
                            )
            except Exception as e:
                log.error("Error retrieving logs from %s: %s" % (mds.node.hostname, e))

        compressed_log_files = []
        for file_path in log_files:
            gz_file_path = "%s.gz" % file_path
            try:
                with open(file_path, "rb") as f_in:
                    with gzip.open(gz_file_path, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)
                os.remove(file_path)
                log.info(
                    "Compressed %s to %s and deleted the original file"
                    % (file_path, gz_file_path)
                )
                compressed_log_files.append(gz_file_path)
            except Exception as e:
                log.error("Error compressing %s: %s" % (file_path, e))

        log.info("Log files compressed and original files deleted successfully.")
        return compressed_log_files

    def get_daemon_names(self, client, daemon_type):
        """
        Retrieve the names of daemons of a specified type.

        This function executes the "ceph orch ps" command to get the list of
        daemons in the Ceph cluster and filters the list to return the names
        of daemons that match the specified type.

        :param client: Client object to execute the command
        :param daemon_type: Type of daemon to filter (e.g., "mds", "osd", "mgr")
        :return: List of daemon names of the specified type
        """
        daemon_names = []

        try:
            out, rc = client.exec_command(sudo=True, cmd="ceph orch ps --format json")
            json_data = json.loads(out)

            for data in json_data:
                if data.get("daemon_type") == daemon_type:
                    daemon_names.append(data.get("daemon_name"))

        except json.JSONDecodeError as e:
            log.error("Failed to decode JSON from 'ceph orch ps': %s" % e)
        except Exception as e:
            log.error("Error retrieving %s daemon names: %s" % (daemon_type, e))

        return daemon_names

    def collect_ceph_details(self, client, cmd, log_dir):
        """
        Collect Ceph cluster details by executing the provided command and write the output to a file.

        :param client: Client object to execute the command
        :param cmd: Command to execute (e.g., 'ceph status')
        :param log_dir: Directory where the log file should be written
        """
        try:
            out, rc = client.exec_command(sudo=True, cmd="%s --format json" % cmd)
            output = json.loads(out)

            log.info("Log Dir : %s" % log_dir)

            # Create a file name based on the command
            cmd_name = cmd.replace(" ", "_")
            log_file_path = os.path.join(log_dir, "%s.log" % cmd_name)

            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            separator = "-" * 40

            with open(log_file_path, "a") as log_file:
                log_file.write("%s\n" % current_time)
                log_file.write(json.dumps(output, indent=4))
                log_file.write("\n")
                log_file.write("%s\n" % separator)
        except json.JSONDecodeError as e:
            log.error("Failed to parse JSON output for '%s': %s" % (cmd, e))
        except Exception as e:
            log.error("Error while collecting Ceph details for '%s': %s" % (cmd, e))

    def get_ceph_mds_top_output(self, ceph_cluster, log_dir):
        """
        Collect and log the top and RSS output of ceph-mds processes from all MDS nodes.

        :param ceph_cluster: Ceph cluster object
        :param log_dir: Directory where logs will be stored
        """
        mds_nodes = ceph_cluster.get_ceph_objects("mds")
        for mds in mds_nodes:
            # Get  process ID of the ceph-mds process
            mds_hostnames = mds.node.hostname
            process_id_out = mds.exec_command(
                sudo=True,
                cmd="pgrep ceph-mds",
            )
            process_id = process_id_out[0].strip()
            log.info("Process ID : %s", process_id)

            # Get the top output for the ceph-mds process
            top_out = mds.exec_command(
                sudo=True,
                cmd="top -b -n 1 -p %s" % process_id,
            )
            top_out1 = mds.exec_command(
                sudo=True,
                cmd="ps -p %s -o rss=" % process_id,
            )
            top_out_rss_value = top_out1[0].strip()
            mds_log_name = mds_hostnames.replace(" ", "_")
            log_file_path = os.path.join(
                log_dir, "%s_ceph-mds_top_output.log" % mds_log_name
            )
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            separator = "-" * 40

            with open(log_file_path, "a") as log_file:
                log_file.write("%s\n" % current_time)
                log_file.write(top_out[0])
                log_file.write("\n")
                log_file.write(top_out_rss_value)
                log_file.write("\n")
                log_file.write("%s\n" % separator)
            log.info("Top output of ceph-mds on %s is %s", mds_hostnames, top_out[0])
            log.info(
                "rss Value of ceph-mds on %s is %s", mds_hostnames, top_out_rss_value
            )

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
        Start a background thread to periodically log ceph-mds top output.

        :param ceph_cluster: Ceph cluster object
        :param log_dir: Directory where the log files will be stored
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
        Periodically execute a list of Ceph commands and log their outputs.

        :param client: Client object to execute the commands
        :param cmd_list: List of shell commands to execute (e.g., ['ceph df', 'ceph -s'])
        :param log_dir: Directory to store the command outputs
        :param interval: Time interval in seconds between each run
        """

        while not stop_event.is_set():
            for cmd in cmd_list:
                self.collect_ceph_details(client, cmd, log_dir)
            stop_event.wait(interval)

    def start_logging(self, client, cmd_list, log_dir):
        """
        Start logging Ceph commands periodically in a background thread.

        :param client: Client object to execute the commands
        :param cmd_list: List of Ceph commands to execute periodically
        :param log_dir: Directory to store command logs
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
        dd_block_size="1M",
        dd_count=1024,
        cthonLogPath=None,
        batch_size=None,
    ):
        """
        Run IO operations on the mounted paths in parallel based on the specified IO type.

        :param mount_paths: List of dictionaries containing client and mount path
        :param io_type: Type of IO operation ("smallfile", "largefile", "fio", "dd", or "cthon")
        :param io_operation: Operation type for smallfile (e.g., create, read, etc.)
        :param io_threads: Number of threads for smallfile
        :param io_file_size: File size for smallfile
        :param io_files: Number of files for smallfile
        :param fio_engine: IO engine for FIO
        :param fio_operation: Operation type for FIO (e.g., read, write, randwrite, randread)
        :param fio_direct: Direct IO flag for FIO
        :param fio_block_size: Block size for FIO
        :param fio_depth: IO depth for FIO
        :param fio_file_size: File size for FIO
        :param dd_block_size: Block size for dd command (default: 1M)
        :param dd_count: Count for dd command (default: 1024)
        :param cthonLogPath: Log directory path for cthon
        :param batch_size: Batch size for parallel execution (optional)
        :return: Dictionary with metrics outputs from IO operations
        """

        def run_dd_io(client, mount_path):
            """
            Run dd-based IO operations on the specified mount path.

            :param client: Client object to run the operation
            :param mount_path: Mount path to perform the IO operation
            :return: Dictionary with dd metrics
            """
            try:
                log.info(
                    "Running dd IO on %s from client %s"
                    % (mount_path, client.node.hostname)
                )
                output_file = "%s/dd_file_%s" % (mount_path, client.node.hostname)
                cmd = "dd if=/dev/urandom of=%s bs=%s count=%s oflag=direct" % (
                    output_file,
                    dd_block_size,
                    dd_count,
                )
                out, err = client.exec_command(sudo=True, cmd=cmd, timeout=72000)
                dd_output = err.strip() if err.strip() else out.strip()

                log.info("dd output:\n%s" % dd_output)

                match = re.search(
                    r"(\d+)\s+bytes.*copied.*,?\s*([0-9.]+)\s+s(?:ec)?,?\s*([0-9.]+)\s+([A-Za-z/]+)",
                    dd_output,
                )
                if match:
                    metrics = {
                        "bytes_written": match.group(1),
                        "time_taken_sec": match.group(2),
                        "throughput": "%s %s" % (match.group(3), match.group(4)),
                    }
                else:
                    metrics = {"raw_output": out.strip()}

                return {client.node.hostname: metrics}

            except CommandFailed as e:
                log.error(
                    "dd IO failed on %s from client %s: %s"
                    % (mount_path, client.node.hostname, e)
                )
                return {client.node.hostname: None}
            except Exception as e:
                log.error("Unexpected error during dd IO on %s: %s" % (mount_path, e))
                return {client.node.hostname: None}

        def run_smallfile_io(client, mount_path):
            """
            Run smallfile IO operations on the specified mount path.

            :param client: Client object to run the operation
            :param mount_path: Mount path to perform the IO operation
            :return: Dictionary with metrics from the IO operation
            """
            try:
                log.info(
                    "Running IO operations on %s from client %s"
                    % (mount_path, client.node.hostname)
                )
                log.info("Creating Directory for running smallfile writes")
                dirname_suffix = "".join(
                    random.choice(string.ascii_lowercase + string.digits)
                    for _ in list(range(2))
                )
                dir_name = "smallfile_dir_%s" % dirname_suffix
                client.exec_command(
                    sudo=True, cmd="mkdir %s%s" % (mount_path, dir_name)
                )
                smallfilepath = "/home/cephuser/smallfile/smallfile_cli.py"
                out, _ = client.exec_command(
                    sudo=True,
                    cmd=(
                        "python3 %s --operation %s --threads %s "
                        "--file-size %s --files %s "
                        "--top %s%s"
                    )
                    % (
                        smallfilepath,
                        io_operation,
                        io_threads,
                        io_file_size,
                        io_files,
                        mount_path,
                        dir_name,
                    ),
                )
                log.info("smallfile out : %s" % out)
                log.info(
                    "IO operation completed on %s from client %s"
                    % (mount_path, client.node.hostname)
                )

                metrics = self.extract_smallfile_metrics(out)
                log.info("Metrics : %s" % metrics)

                return {client.node.hostname: metrics}

            except CommandFailed as e:
                log.error(
                    "IO operation failed on %s from client %s: %s"
                    % (mount_path, client.node.hostname, e)
                )

                return {client.node.hostname: None}

            except Exception as e:
                log.error(
                    "An unexpected error occurred on %s from client %s: %s"
                    % (mount_path, client.node.hostname, e)
                )

                return {client.node.hostname: {mount_path: metrics}}

        def run_largefile_io(client, mount_path):
            """
            Run largefile IO operations on the specified mount path.

            :param client: Client object to run the operation
            :param mount_path: Mount path to perform the IO operation
            :return: None
            """
            log.info(
                "Running IO operations on %s from client %s"
                % (mount_path, client.node.hostname)
            )

            try:
                client.exec_command(
                    sudo=True,
                    cmd="bash /root/create_large_file.sh %s" % mount_path,
                    timeout=36000,
                )
                log.info(
                    "IO operation completed on %s from client %s"
                    % (mount_path, client.node.hostname)
                )

            except CommandFailed as e:
                log.error(
                    "IO operation failed on %s from client %s: %s"
                    % (mount_path, client.node.hostname, e)
                )

            except Exception as e:
                log.error(
                    "An unexpected error occurred on %s from client %s: %s"
                    % (mount_path, client.node.hostname, e)
                )

                return {client.node.hostname: None}

        def run_cthon_io(client, mount_path, cthonLogPath, iterations=1):
            """
            Run Cthon IO operations on the specified mount path by parsing mount details for NFS info.

            :param client: Client object
            :param mount_path: Mount point
            :param cthonLogPath: Log directory path
            :param iterations: Number of Cthon iterations
            :return: Dict with status and log
            """

            try:
                log.info(
                    "Running Cthon IO on %s from %s"
                    % (mount_path, client.node.hostname)
                )

                # Get full mount output
                export_mounted_path = mount_path.rstrip("/")  # Remove trailing slash
                mount_output, _ = client.exec_command(
                    sudo=True,
                    cmd="mount | grep 'on %s '" % export_mounted_path,
                    timeout=60,
                )
                log.debug("Mount info for %s:\n%s" % (mount_path, mount_output))

                # Parse server_ip, export_path, port
                match = re.search(
                    (
                        r"(?P<server_ip>\d{1,3}(?:\.\d{1,3}){3}):"
                        r"(?P<export_path>\S+)\s+on\s+\S+\s+type\s+nfs\S*\s+"
                        r"\((?P<options>[^)]+)\)"
                    ),
                    mount_output.strip(),
                )
                if not match:
                    raise ValueError("Could not parse mount info for %s" % mount_path)

                server_ip = match.group("server_ip")
                export_path = match.group("export_path")
                options = match.group("options")
                port_match = re.search(r"port=(\d+)", options)
                nfs_port = int(port_match.group(1)) if port_match else 2049

                # Prepare log file path
                client.exec_command(sudo=True, cmd="mkdir -p %s" % cthonLogPath)
                sv_suffix = mount_path.strip("/").split("/")[-1]
                log_file = "%s/cthon_%s.log" % (cthonLogPath, sv_suffix)

                # Setup environment
                base_url = (
                    "https://mirrors.vcea.wsu.edu/rocky/9/devel/x86_64/os/Packages/l"
                )
                libtirpc = "%s/libtirpc-1.3.3-9.el9.x86_64.rpm" % base_url
                libtirpc_devel = "%s/libtirpc-devel-1.3.3-9.el9.x86_64.rpm" % base_url
                setup_cmds = [
                    "dnf install -y git",
                    "dnf groupinstall -y 'Development Tools'",
                    "dnf install -y %s" % libtirpc,
                    "dnf install -y %s" % libtirpc_devel,
                    "rm -rf cthon04",
                    "git clone git://git.linux-nfs.org/projects/steved/cthon04.git",
                    "cd cthon04 && make",
                ]
                for cmd in setup_cmds:
                    client.exec_command(sudo=True, cmd=cmd, timeout=300)

                # Run cthon with parsed info
                run_cmd = (
                    "cd cthon04 && "
                    "./server -a -o port=%s -N %s "
                    "-p %s -m %s %s > %s 2>&1"
                    % (
                        nfs_port,
                        iterations,
                        export_path,
                        mount_path,
                        server_ip,
                        log_file,
                    )
                )
                client.exec_command(sudo=True, cmd=run_cmd, timeout=18000)

                log.info(
                    "Cthon started on %s (IP=%s, port=%s). Log: %s"
                    % (mount_path, server_ip, nfs_port, log_file)
                )
                return {client.node.hostname: {"status": "started", "log": log_file}}

            except Exception as e:
                log.error(
                    "Cthon IO failed on %s from %s: %s"
                    % (mount_path, client.node.hostname, e)
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
                    "Running FIO IO operations on %s from client %s"
                    % (mount_path, client.node.hostname)
                )

                log.info("Creating Directory for running fio writes")
                dirname_suffix = "".join(
                    random.choice(string.ascii_lowercase + string.digits)
                    for _ in list(range(2))
                )
                dir_name = "fio_dir_%s" % dirname_suffix
                client.exec_command(
                    sudo=True, cmd="mkdir %s%s" % (mount_path, dir_name)
                )
                out, err = client.exec_command(
                    sudo=True,
                    cmd=(
                        "cd %s%s; fio --name=%s --rw=%s"
                        " --direct=%s --ioengine=%s --bs=%s"
                        " --iodepth=%s --size=%s --group_reporting=1"
                        " --output=/root/fio_%s.json"
                    )
                    % (
                        mount_path,
                        dir_name,
                        fio_operation,
                        fio_operation,
                        fio_direct,
                        fio_engine,
                        fio_block_size,
                        fio_depth,
                        fio_file_size,
                        client.node.hostname,
                    ),
                )
                log.info(
                    "IO operation completed on %s from client %s"
                    % (mount_path, client.node.hostname)
                )

            except CommandFailed as e:
                log.error(
                    "IO operation failed on %s from client %s: %s"
                    % (mount_path, client.node.hostname, e)
                )

                return {client.node.hostname: None}

            except Exception as e:
                log.error(
                    "An unexpected error occurred on %s from client %s: %s"
                    % (mount_path, client.node.hostname, e)
                )

                return {client.node.hostname: None}

        metrics_outputs = {}

        if batch_size:  # Run IO in batches only if batch_size is provided
            log.info("Running IOs in batches of size: %s" % batch_size)
            for batch_num, batch in enumerate(
                self.chunks(mount_paths, batch_size), start=1
            ):
                log.info(
                    "Starting batch %s with %s mount paths" % (batch_num, len(batch))
                )

                with ThreadPoolExecutor(max_workers=len(batch)) as executor:
                    if io_type == "smallfile":
                        futures = [
                            executor.submit(
                                run_smallfile_io, m["client"], m["mount_path"]
                            )
                            for m in batch
                        ]
                    elif io_type == "largefile":
                        futures = [
                            executor.submit(
                                run_largefile_io, m["client"], m["mount_path"]
                            )
                            for m in batch
                        ]
                    elif io_type == "fio":
                        futures = [
                            executor.submit(run_fio, m["client"], m["mount_path"])
                            for m in batch
                        ]
                    elif io_type == "dd":
                        futures = [
                            executor.submit(run_dd_io, m["client"], m["mount_path"])
                            for m in batch
                        ]
                    elif io_type == "cthon":
                        futures = [
                            executor.submit(
                                run_cthon_io, m["client"], m["mount_path"], cthonLogPath
                            )
                            for m in batch
                        ]
                    else:
                        raise ValueError("Invalid io_type specified.")

                    for future in as_completed(futures):
                        try:
                            result = future.result()
                            if result:
                                for hostname, mount_data in result.items():
                                    if hostname not in metrics_outputs:
                                        metrics_outputs[hostname] = {}
                                    metrics_outputs[hostname].update(mount_data)
                        except Exception as e:
                            log.error(
                                "Exception occurred during IO batch %s: %s"
                                % (batch_num, e)
                            )

                    log.info("Completed batch %s" % batch_num)

        else:  # Run all IOs in full parallel mode
            log.info("Running IOs in full parallel mode (no batching)")

            with ThreadPoolExecutor(max_workers=len(mount_paths)) as executor:
                if io_type == "smallfile":
                    futures = [
                        executor.submit(run_smallfile_io, m["client"], m["mount_path"])
                        for m in mount_paths
                    ]
                elif io_type == "largefile":
                    futures = [
                        executor.submit(run_largefile_io, m["client"], m["mount_path"])
                        for m in mount_paths
                    ]
                elif io_type == "fio":
                    futures = [
                        executor.submit(run_fio, m["client"], m["mount_path"])
                        for m in mount_paths
                    ]
                elif io_type == "dd":
                    futures = [
                        executor.submit(run_dd_io, m["client"], m["mount_path"])
                        for m in mount_paths
                    ]
                elif io_type == "cthon":
                    futures = [
                        executor.submit(
                            run_cthon_io, m["client"], m["mount_path"], cthonLogPath
                        )
                        for m in mount_paths
                    ]
                else:
                    raise ValueError("Invalid io_type specified.")

                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if result:
                            for hostname, mount_data in result.items():
                                if hostname not in metrics_outputs:
                                    metrics_outputs[hostname] = {}
                                metrics_outputs[hostname].update(mount_data)
                    except Exception as e:
                        log.error("Exception occurred during IO operation: %s" % e)

        log.info("Metrics Outputs : %s" % metrics_outputs)
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
            log.info("Error in extract_metrics: %s" % e)
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
        dd_block_size="1M",
        dd_count=1024,
    ):
        """
        Creates a structured log directory based on IO type and test parameters.

        :param io_type: Type of IO ("smallfile", "largefile", "fio", "dd", "cthon")
        :param mount_type: Mount type used ("fuse", "kernel", or "nfs")
        :param client_count: Number of clients used
        :param subvolume_count: Number of subvolumes
        :param io_operation: (Optional) Operation type for smallfile
        :param io_threads: (Optional) Threads used in smallfile
        :param io_files: (Optional) Number of files used in smallfile
        :param io_file_size: (Optional) File size used in smallfile
        :param fio_operation: (Optional) FIO operation type
        :param fio_depth: (Optional) IO depth for FIO
        :param fio_block_size: (Optional) Block size for FIO
        :param fio_file_size: (Optional) File size for FIO
        :param dd_block_size: (Optional) Block size for dd
        :param dd_count: (Optional) Count for dd
        :return: Full path to the created log directory
        """
        base_dir = os.path.dirname(log.logger.handlers[0].baseFilename)
        os.makedirs("%s/scale_logs" % base_dir, exist_ok=True)
        log_base_dir = "%s/scale_logs" % base_dir

        if io_type == "largefile":
            log_dir = "%s/%s_wr_%s_%s_cl_%s_sv_100GB_file" % (
                log_base_dir,
                io_type,
                mount_type,
                client_count,
                subvolume_count,
            )
        elif io_type == "smallfile":
            log_dir = "%s/%s_wr_%s_%s_cl_%s_sv_%s_ops_%s_th_%s_files_%s_size" % (
                log_base_dir,
                io_type,
                mount_type,
                client_count,
                subvolume_count,
                io_operation,
                io_threads,
                io_files,
                io_file_size,
            )
        elif io_type == "fio":
            log_dir = "%s/%s_%s_%s_cl_%s_subv_%s_ops_%s_depth_%s_bs_%s_size" % (
                log_base_dir,
                io_type,
                mount_type,
                client_count,
                subvolume_count,
                fio_operation,
                fio_depth,
                fio_block_size,
                fio_file_size,
            )
        elif io_type == "dd":
            log_dir = "%s/%s_%s_%s_cl_%s_subv_%s_bs_%s_count" % (
                log_base_dir,
                io_type,
                mount_type,
                client_count,
                subvolume_count,
                dd_block_size,
                dd_count,
            )
        elif io_type == "cthon":
            log_dir = "%s/%s_%s_%s_cl_%s_subv_cthon" % (
                log_base_dir,
                io_type,
                mount_type,
                client_count,
                subvolume_count,
            )
        else:
            raise ValueError("Unsupported io_type: %s" % io_type)

        os.makedirs(log_dir, exist_ok=True)
        log.info("Log Dir : %s" % log_dir)

        return log_dir

    def collect_logs(self, ceph_cluster, clients, default_fs, fs_util_v1, log_dir):
        """
        Collects MDS top output, enables debug logs, and starts periodic logging of Ceph and MDS status.

        :param ceph_cluster: Ceph cluster object
        :param clients: List of client nodes
        :param default_fs: Default filesystem name
        :param fs_util_v1: Filesystem utility instance
        :param log_dir: Directory to store collected logs
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
            cmd_list.append("ceph tell %s perf dump --format json" % daemon)
        log.info("These ceph commands will run at an interval: %s" % cmd_list)

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
        Collects and compresses MDS logs from the cluster.

        :param clients: List of client nodes
        :param fs_util_v1: Filesystem utility instance
        :param log_dir: Directory to store compressed logs
        :return: List of compressed log file paths
        """
        log.info("Collecting MDS Logs from all MDS Daemons")
        compressed_logs = self.collect_and_compress_mds_logs(
            clients, fs_util_v1, log_dir
        )
        return compressed_logs

    def generate_nfs_yaml(
        self,
        nfs_base_name,
        host,
        start_monitoring_port,
        start_port,
        count,
        output_path="/root/nfs_clusters.yaml",
    ):
        """
        Generates and saves a Ceph NFS YAML configuration for multiple clusters.
        """
        data = []

        for i in range(count):
            cluster = {
                "placement": {"hosts": [host]},
                "service_id": "%s" % (nfs_base_name + str(i + 1)),
                "service_type": "nfs",
                "spec": {
                    "monitoring_port": start_monitoring_port + i,
                    "port": start_port + i,
                },
            }
            data.append(cluster)

        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        with open(output_path, "w") as f:
            yaml.dump_all(data, f, sort_keys=False, default_flow_style=False)

        log.info("YAML file generated at: %s" % output_path)
        return output_path  # Return path for remote upload

    def chunks(self, data, size):
        """
        Yield successive chunks of a specified size from the input iterable.

        This method is primarily used to divide mount paths or subvolumes into batches
        for parallel I/O execution (e.g., smallfile, fio, dd, Cthon).

        :param data: Iterable (e.g., list of mount paths or clients) to split into chunks
        :param size: Number of elements per chunk (i.e., batch size)
        :return: Generator yielding lists of up to 'size' elements each
        """
        it = iter(data)
        while True:
            chunk = list(islice(it, size))
            if not chunk:
                break
            yield chunk

    def create_multiple_nfs_clusters_with_kmip(
        self,
        client,
        fs_util_v1,
        yaml_src_path="tests/cephfs/lib/nfs_kmip.yaml",
        yaml_dst_path="/root/nfs_kmip.yaml",
        nfs_base_name="ceph-nfs",
        count=10,
        start_port=25501,
        start_monitoring_port=24501,
        hosts=None,
    ):
        """
        Create multiple NFS clusters with KMIP support using a static YAML and validate each cluster.

        :param client: The client node to run the commands on
        :param fs_util_v1: FS utility object to validate services
        :param yaml_src_path: Local path to the static KMIP YAML
        :param yaml_dst_path: Remote path on client to upload the YAML
        :param nfs_base_name: Base name for NFS clusters (e.g., 'ceph-nfs')
        :param count: Number of NFS clusters to create
        :param start_port: (Optional) Start port for NFS (not currently used in static YAML)
        :param start_monitoring_port: (Optional) Start port for monitoring (not currently used)
        :param hosts: List of hosts to place NFS clusters on (used only for logging now)
        :return: List of created NFS cluster names
        """
        try:
            log.info("Uploading KMIP NFS cluster YAML to client")
            client.upload_file(sudo=True, src=yaml_src_path, dst=yaml_dst_path)

            log.info("Applying NFS cluster spec using 'ceph orch apply'")
            client.exec_command(sudo=True, cmd="ceph orch apply -i %s" % yaml_dst_path)

            log.info("Waiting for NFS clusters to come up...")
            cluster_names = []
            for i in range(1, count + 1):
                service_name = "%s%s" % (nfs_base_name, i)
                fs_util_v1.validate_services(client, "nfs.%s" % service_name)
                cluster_names.append(service_name)

            log.info("%s NFS clusters with KMIP support created and validated." % count)
            return cluster_names

        except CommandFailed as e:
            log.error("Failed to create or validate NFS cluster(s): %s" % e)
            return []
