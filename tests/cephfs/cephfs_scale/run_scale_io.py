import json
import logging
import os
import random
import string
import threading
import traceback

import tests.cephfs.cephfs_scale.cleanup as cleanup
from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_scale.cephfs_scale_utils import CephfsScaleUtils
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
        Main function to execute the test workflow.

        This function sets up the clients, prepares the CephFS environment, creates subvolumes,
        mounts them, runs IO operations, and collects logs and status information.

        :param ceph_cluster: Ceph cluster object
        :param kw: Additional keyword arguments for configuration
        :return: 0 if successful, 1 if an error occurs

    ## To enable standby_replay - include standby_replay as true or false in the config as below :
      - test:
            name: large_file_writes
            module: cephfs_scale.large_file_writes.py
            config:
              num_of_subvolumes: 50
              num_of_clients: 50
              mount_type : fuse
              enable_standby_replay : true
            desc: Write Large IOs
            abort-on-fail: false
    """
    try:
        fs_util_v1 = FsUtilsV1(ceph_cluster)
        fs_scale_utils = CephfsScaleUtils(ceph_cluster)
        fs_system_utils = CephFSSystemUtils(ceph_cluster)
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        fs_util_v1.prepare_clients(clients, build)
        fs_util_v1.auth_list(clients)
        mds_nodes = ceph_cluster.get_ceph_objects("mds")
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        default_fs = "cephfs-scale"

        mds_names_hostname = []
        for mds in mds_nodes:
            mds_names_hostname.append(mds.node.hostname)
        log.info(f"MDS List : {mds_names_hostname}")

        mds_names = []
        for mds in mds_nodes:
            mds_names.append(mds.node.hostname)
        mds_hosts_list1 = mds_names[:3]
        mds_hosts_1 = " ".join(mds_hosts_list1) + " "
        mds_host_list1_length = len(mds_hosts_list1)

        nfs_server_ip_address = []
        nfs_servers_hosts = []
        hostname_to_ip_map = {}

        for nfs in nfs_servers:
            hostname = nfs.node.hostname
            ip_addr = nfs.node.ip_address
            nfs_servers_hosts.append(hostname)
            nfs_server_ip_address.append(ip_addr)
            hostname_to_ip_map[hostname] = ip_addr

        log.info(f"NFS Server IP Address List : {nfs_server_ip_address}")
        log.info(f"NFS Server Hosts List      : {nfs_servers_hosts}")

        mount_type = config.get("mount_type", "fuse")
        io_type = config.get("io_type", "largefile")

        subvolume_group_name = "subvolgroup1"
        subvolume_name = (
            f"subvol_{io_type}_{mount_type}_"
            f"{''.join(random.choices(string.ascii_lowercase + string.digits, k=4))}"
        )
        subvolume_count = config.get("num_of_subvolumes")
        client_count = config.get("num_of_clients")

        io_operation = config.get("smallfile_operation")
        io_threads = config.get("smallfile_threads")
        io_file_size = config.get("smallfile_file_size")
        io_files = config.get("smallfile_files")

        fio_engine = config.get("fio_engine")
        fio_operation = config.get("fio_operation")
        fio_direct = config.get("fio_direct")
        fio_block_size = config.get("fio_block_size")
        fio_depth = config.get("fio_depth")
        fio_file_size = config.get("fio_file_size")

        large_file_count = config.get("large_file_count")
        large_file_size = config.get("large_file_size")

        nfs_base_name = "ceph-nfs"
        count = 10
        start_port = 25501
        start_monitoring_port = 24501

        enable_standby_replay = config.get("enable_standby_replay", False)
        max_mds_default_fs = 2 if enable_standby_replay else 1
        log.info(
            f"Create Filesystems with {'standby-replay' if enable_standby_replay else 'active MDS'}"
        )
        fs_details = [
            (default_fs, mds_host_list1_length, mds_hosts_1, max_mds_default_fs),
        ]
        for fs_name, mds_host_list_length, mds_hosts, max_mds in fs_details:
            clients[0].exec_command(
                sudo=True,
                cmd=f'ceph fs volume create {fs_name} --placement="{mds_host_list_length} {mds_hosts}"',
            )
            clients[0].exec_command(
                sudo=True, cmd=f"ceph fs set {fs_name} max_mds {max_mds}"
            )
            fs_util_v1.wait_for_mds_process(clients[0], fs_name)

        if enable_standby_replay:
            for fs_name in [default_fs]:
                result = fs_util_v1.set_and_validate_mds_standby_replay(
                    clients[0],
                    fs_name,
                    1,
                )
                log.info(f"Ceph fs status after enabling standby-replay : {result}")

        try:
            if mount_type == "nfs":
                multi_cluster_kmip_enabled = config.get(
                    "multi_cluster_kmip_enabled", False
                )
                byok_enabled = config.get("byok_enabled", False)
                if multi_cluster_kmip_enabled:
                    log.info(
                        "Multi-cluster KMIP is enabled, proceeding with parallel NFS cluster creation."
                    )

                    # Create multiple NFS clusters using KMIP YAML
                    nfs_cluster_names = (
                        fs_scale_utils.create_multiple_nfs_clusters_with_kmip(
                            clients[0],
                            fs_util_v1,
                            yaml_src_path="tests/cephfs/lib/nfs_kmip.yaml",
                            yaml_dst_path="/root/nfs_kmip.yaml",
                            nfs_base_name=nfs_base_name,
                            count=count,
                            start_port=start_port,
                            start_monitoring_port=start_monitoring_port,
                            hosts=nfs_servers_hosts,
                        )
                    )

                    log.info(f"Created NFS Clusters: {nfs_cluster_names}")

                else:
                    log.info(
                        "Multi-cluster KMIP not enabled, proceeding with single NFS cluster creation."
                    )

                    nfs_name = "cephfs-nfs"
                    selected_nfs_nodes = nfs_servers_hosts[-4:]
                    log.info(f"Selected NFS Server Hosts  : {selected_nfs_nodes}")

                    port = "2050"
                    placement = (
                        f"{len(selected_nfs_nodes)} {' '.join(selected_nfs_nodes)}"
                    )
                    fs_util_v1.create_nfs(
                        clients[0],
                        nfs_name,
                        validate=False,
                        placement=placement,
                        port=port,
                    )
                    nfs_cluster_names = [f"{nfs_name}"]
                    log.info(f"Created Single NFS Cluster: {nfs_cluster_names}")
                    try:
                        service_name = f"nfs.{nfs_name}"
                        log.info(f"Validating NFS cluster: {service_name}")
                        fs_util_v1.validate_services(clients[0], service_name)
                        log.info(f"NFS cluster {service_name} is active.")
                    except Exception as ve:
                        log.error(
                            f"Validation failed for NFS cluster {service_name}: {ve}"
                        )
                        return 1
            else:
                log.info("Skipping NFS cluster creation since mount_type is not 'nfs'")

        except CommandFailed as e:
            log.error("Failed to create NFS cluster: %s" % e)
            return 1

        try:
            if mount_type == "nfs":
                # QoS Enablement for all NFS clusters
                for cluster_id in nfs_cluster_names:
                    try:
                        write_bw = "10MB"
                        read_bw = "10MB"
                        # Enable PerShare QoS for the cluster
                        fs_scale_utils.enable_cluster_qos(
                            client=clients[0],
                            cluster_id=cluster_id,
                            qos_type="PerShare",
                            max_export_write_bw=write_bw,
                            max_export_read_bw=read_bw,
                        )
                        log.info(
                            f"QoS enabled for cluster {cluster_id} with Write: {write_bw}, Read: {read_bw}"
                        )

                    except Exception as e:
                        log.error(
                            f"Failed to enable QoS on cluster {cluster_id}: {str(e)}"
                        )
            else:
                log.info("Skipping NFS QOS Settings since mount_type is not 'nfs'")

        except CommandFailed as e:
            log.error("Failed to Set QOS on NFS cluster: %s" % e)
            return 1

        if client_count > len(clients):
            log.error(
                f"Clients required to perform the test is {client_count} but "
                f"the conf file has only {len(clients)}"
            )
            return 1

        log_dir = fs_scale_utils.setup_log_dir(
            io_type,
            mount_type,
            client_count,
            subvolume_count,
            io_operation,
            io_threads,
            io_files,
            io_file_size,
            fio_operation,
            fio_depth,
            fio_block_size,
            fio_file_size,
        )

        log.info("Creating a Subvolume Group")
        subvolumegroup = {
            "vol_name": default_fs,
            "group_name": subvolume_group_name,
        }
        fs_util_v1.create_subvolumegroup(clients[0], **subvolumegroup)

        byok_key_id = "KEY-fb9b533-93e0d3f4-2543-4d38-8783-451018fceaa1"
        log.info("Create Subvolumes and mount on clients")
        clients_to_use = clients[:client_count]

        # Common args for all mount types
        mount_kwargs = {
            "fs_util_v1": fs_util_v1,
            "clients": clients_to_use,
            "subvolume_count": subvolume_count,
            "default_fs": default_fs,
            "subvolume_name": subvolume_name,
            "subvolume_group_name": subvolume_group_name,
            "mount_type": mount_type,
        }

        # Only for NFS
        if mount_type == "nfs":
            # Use the cluster_index if defined and valid, else default to 0
            cluster_index = config.get("cluster_index", 0)
            if not isinstance(cluster_index, int) or cluster_index >= len(
                nfs_server_ip_address
            ):
                cluster_index = 0

            # Build NFS cluster → IP map
            nfs_cluster_ip_map = {}
            for cluster in nfs_cluster_names:
                try:
                    out, _ = clients[0].exec_command(
                        sudo=True, cmd=f"ceph nfs cluster info {cluster} -f json"
                    )
                    cluster_info = json.loads(out)
                    backend_info = cluster_info.get(cluster, {}).get("backend", [])
                    if backend_info:
                        ip = backend_info[0].get("ip")
                        port = backend_info[0].get("port", 2049)
                        nfs_cluster_ip_map[cluster] = {"ip": ip, "port": port}
                    else:
                        log.warning(f"No backend info found for NFS cluster: {cluster}")
                except Exception as e:
                    log.error(f"Failed to fetch IP for {cluster}: {e}")
                    continue

            log.info(f"NFS Cluster to IP Map: {nfs_cluster_ip_map}")

            mount_kwargs.update(
                {
                    "byok_enabled": byok_enabled,
                    "key_id": byok_key_id,
                    "multiple_cluster": True,
                    "start_port": start_port,
                    "nfs_cluster_names": nfs_cluster_names,
                    "nfs_cluster_ip_map": nfs_cluster_ip_map,
                    "nfs_server_ips": nfs_server_ip_address,
                }
            )

        mount_paths = fs_scale_utils.mount_and_create_subvolumes(**mount_kwargs)

        if config.get("collect_logs", False):
            fs_scale_utils.collect_logs(
                ceph_cluster, clients, default_fs, fs_util_v1, log_dir
            )

        batch_size = 50
        log.info("Start Running IO's in parallel on all Clients")

        if io_type == "largefile":
            file_generator_script = "create_large_file.py"
            for client in clients:
                client.upload_file(
                    sudo=True,
                    src="tests/cephfs/cephfs_mirroring/snapdiff_scripts/generate_files_for_snapdiff.py",
                    dst=f"/root/{file_generator_script}",
                )
                client.exec_command(
                    sudo=True, cmd=f"chmod +x /root/{file_generator_script}"
                )

            large_file_metrics = fs_scale_utils.run_io_operations_parallel(
                mount_paths=mount_paths,
                io_type="largefile",
                io_args={
                    "file_count": large_file_count,
                    "file_size": large_file_size,
                    "file_unit": "GB",
                },
                batch_size=batch_size,
            )
            log.info("Large File Metrics : %s", large_file_metrics)

        elif io_type == "fio":
            fs_scale_utils.run_io_operations_parallel(
                mount_paths=mount_paths,
                io_type="fio",
                io_args={
                    "operation": fio_operation,
                    "direct": fio_direct,
                    "engine": fio_engine,
                    "block_size": fio_block_size,
                    "iodepth": fio_depth,
                    "file_size": fio_file_size,
                },
                batch_size=batch_size,
            )

            try:
                for client in clients:
                    os.makedirs(f"{log_dir}/fio_{client.node.hostname}", exist_ok=True)
                    src_path = f"/root/fio_{client.node.hostname}.json"
                    dst_path = f"{log_dir}/fio_{client.node.hostname}/fio_{client.node.hostname}.json"

                    try:
                        client.exec_command(sudo=True, cmd=f"test -e {src_path}")
                        log.info(f"File {src_path} exists. Proceeding with download.")
                        client.download_file(src=src_path, dst=dst_path, sudo=True)
                        log.info(f"Downloaded {src_path} to {dst_path}")
                        client.exec_command(sudo=True, cmd=f"rm -f {src_path}")
                        log.info(f"Deleted source file {src_path}")
                    except CommandFailed:
                        log.warning(
                            f"File {src_path} does not exist on client {client.node.hostname}. Skipping download."
                        )
            except Exception as e:
                log.error(
                    f"Failed to download FIO file for client {client.node.hostname} : {e}"
                )

        elif io_type == "smallfile":
            metrics = fs_scale_utils.run_io_operations_parallel(
                mount_paths=mount_paths,
                io_type="smallfile",
                io_args={
                    "operation": io_operation,
                    "threads": io_threads,
                    "file_size": io_file_size,
                    "files": io_files,
                },
                batch_size=batch_size,
            )
            log.info(f"Metrics of all Runs : {metrics}")

        elif io_type == "dd":
            fs_scale_utils.run_io_operations_parallel(
                mount_paths=mount_paths,
                io_type="dd",
                io_args={"block_size": "1M", "count": "1024"},
                batch_size=batch_size,
            )

        elif io_type == "cthon":
            for client in clients:
                fs_scale_utils.setup_cthon_environment(client)
            fs_scale_utils.run_io_operations_parallel(
                mount_paths=mount_paths,
                io_type="cthon",
                io_args={"iterations": 10, "log_path": "/root/cthon_logs"},
                batch_size=batch_size,
            )
            try:
                base_dir = os.path.join(log_dir, "cthon_logs")
                os.makedirs(base_dir, exist_ok=True)

                for client in clients:
                    try:
                        remote_log_dir = "/root/cthon_logs"
                        cmd = f"ls {remote_log_dir}/cthon_{client.node.hostname}_*.log"
                        out, _ = client.exec_command(sudo=True, cmd=cmd)

                        for line in out.strip().splitlines():
                            log_file_name = os.path.basename(line.strip())
                            src_path = os.path.join(remote_log_dir, log_file_name)
                            dst_path = os.path.join(base_dir, log_file_name)

                            client.download_file(src=src_path, dst=dst_path, sudo=True)
                            log.info(f"Downloaded {src_path} to {dst_path}")

                            client.exec_command(sudo=True, cmd=f"rm -f {src_path}")
                            log.info(f"Deleted source file {src_path}")

                    except CommandFailed:
                        log.warning(
                            f"No Cthon logs found on {client.node.hostname}. Skipping."
                        )
                    except Exception as e:
                        log.error(
                            f"Failed to download logs from {client.node.hostname}: {e}"
                        )
            except Exception as e:
                log.error(f"Failed to collect Cthon logs: {e}")

        if config.get("collect_logs", False):
            collected_logs = fs_scale_utils.collect_and_compress_logs(
                clients[0], fs_util_v1, log_dir
            )
            log.info(f"Logs captured : {collected_logs}")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        try:
            log.info("Stop logging the Ceph Cluster Cluster status to a log dir")
            fs_scale_utils.stop_all_logging()
            daemon_list = ["mds", "osd", "mgr", "mon"]
            log.info(f"Check for crash from : {daemon_list}")
            fs_system_utils.crash_check(
                clients[0], crash_copy=1, daemon_list=daemon_list
            )
        except Exception as e:
            log.error(f"Error in crash validation block: {e}")

        if config.get("cleanup", True):
            try:
                log.info("Starting cleanup process.")
                cleanup_exit_code = cleanup.run(ceph_cluster, **kw)
                if cleanup_exit_code == 0:
                    log.info("Cleanup completed successfully.")
                else:
                    log.error("Cleanup failed.")

            except Exception as e:
                log.error(f"Cleanup process encountered an error: {e}")
                log.error(traceback.format_exc())
