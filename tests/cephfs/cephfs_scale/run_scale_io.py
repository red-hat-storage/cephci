import logging
import os
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
        default_fs = "cephfs"

        mds_names = [mds.node.hostname for mds in mds_nodes]
        log.info("MDS List: %s", mds_names)
        mds_hosts_list1 = mds_names[:3]
        mds_hosts_1 = " ".join(mds_hosts_list1)
        mds_host_list1_length = len(mds_hosts_list1)

        enable_standby_replay = config.get("enable_standby_replay", False)
        max_mds_default_fs = 2 if enable_standby_replay else 1
        log.info(
            "Create Filesystems with %s",
            "standby-replay" if enable_standby_replay else "active MDS",
        )
        fs_details = [
            (default_fs, mds_host_list1_length, mds_hosts_1, max_mds_default_fs),
        ]
        for fs_name, mds_host_list_length, mds_hosts, max_mds in fs_details:
            clients[0].exec_command(
                sudo=True,
                cmd='ceph fs volume create %s --placement="%s %s"'
                % (fs_name, mds_host_list_length, mds_hosts),
            )
            clients[0].exec_command(
                sudo=True, cmd="ceph fs set %s max_mds %s" % (fs_name, max_mds)
            )
            fs_util_v1.wait_for_mds_process(clients[0], fs_name)

        if enable_standby_replay:
            for fs_name in [default_fs]:
                result = fs_util_v1.set_and_validate_mds_standby_replay(
                    clients[0],
                    fs_name,
                    1,
                )
                log.info("Ceph fs status after enabling standby-replay : %s", result)

        # nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        # log.info("NFS Servers are : %s", nfs_servers)
        # nfs_server = nfs_servers[0].node.hostname
        # log.info("NFS Servers 1 hostname is  : %s", nfs_server)
        #
        # nfs_server_ip = nfs_servers[0].node.ipaddress
        # log.info("NFS Server 1 IP Address ; %s", nfs_server_ip)
        nfs_server = "cali027"
        hosts = ["cali027"]
        nfs_base_name = "ceph-nfs"
        count = 10
        start_port = 25501
        start_monitoring_port = 24501

        try:
            multi_cluster_kmip_enabled = config.get("multi_cluster_kmip_enabled", False)
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
                        hosts=hosts,
                    )
                )

                log.info("Created NFS Clusters: %s", nfs_cluster_names)

            else:
                log.info(
                    "Multi-cluster KMIP not enabled, proceeding with single NFS cluster creation."
                )

                nfs_name = "cephfs-nfs"
                fs_util_v1.create_nfs(
                    clients[0],
                    nfs_name,
                    validate=False,
                    placement="1 %s" % nfs_server,
                    port=start_port,
                )
                nfs_cluster_names = ["%s" % nfs_name]
                log.info("Created Single NFS Cluster: %s", nfs_cluster_names)
        except CommandFailed as e:
            log.error("Failed to create NFS cluster: %s" % e)
            return 1

        subvolume_group_name = "subvolgroup1"
        subvolume_name = "subvol"
        subvolume_count = config.get("num_of_subvolumes")
        client_count = config.get("num_of_clients")

        mount_type = config.get("mount_type", "fuse")
        io_type = config.get("io_type", "largefile")

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

        nfs_server_ip_address = ["10.8.130.27"]

        if client_count > len(clients):
            log.error(
                "Clients required to perform the test is %s but the conf file has only %s",
                client_count,
                len(clients),
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

        # Static Key ID for BYOK NFS exports (KMIP)
        byok_key_id = "KEY-f49f500-6142373d-637b-4afa-98fe-19252bc2f0e0"
        log.info("Create Subvolumes and mount on clients")
        clients_to_use = clients[:client_count]
        mount_paths = fs_scale_utils.mount_and_create_subvolumes(
            fs_util_v1,
            clients_to_use,
            subvolume_count,
            default_fs,
            subvolume_name,
            subvolume_group_name,
            mount_type,
            nfs_cluster_name=nfs_cluster_names if mount_type == "nfs" else None,
            nfs_server=nfs_server if mount_type == "nfs" else None,
            nfs_server_ip=nfs_server_ip_address if mount_type == "nfs" else None,
            byok_enabled=byok_enabled,
            key_id=byok_key_id,
            multiple_cluster=True,
            start_port=start_port,
            nfs_cluster_names=nfs_cluster_names,
            nfs_server_ips=nfs_server_ip_address,
        )

        fs_scale_utils.collect_logs(
            ceph_cluster, clients, default_fs, fs_util_v1, log_dir
        )

        batch_size = 50
        log.info("Start Running IO's in parallel on all Clients")
        if io_type == "largefile":
            create_large_file_script = "create_large_file.sh"
            for client in clients:
                client.upload_file(
                    sudo=True,
                    src="tests/cephfs/cephfs_scale/create_large_file.sh",
                    dst="/root/%s" % create_large_file_script,
                )
                client.exec_command(
                    sudo=True,
                    cmd="chmod 777 /root/%s" % create_large_file_script,
                    long_running=True,
                )
            fs_scale_utils.run_io_operations_parallel(mount_paths, "largefile")
        elif io_type == "fio":
            fs_scale_utils.run_io_operations_parallel(
                mount_paths,
                io_type="fio",
                fio_operation=fio_operation,
                fio_direct=fio_direct,
                fio_engine=fio_engine,
                fio_block_size=fio_block_size,
                fio_depth=fio_depth,
                fio_file_size=fio_file_size,
                batch_size=batch_size,
            )
            try:
                for client in clients:
                    os.makedirs(
                        "%s/fio_%s" % (log_dir, client.node.hostname), exist_ok=True
                    )
                    src_path = "/root/fio_%s.json" % client.node.hostname
                    dst_path = "%s/fio_%s/fio_%s.json" % (
                        log_dir,
                        client.node.hostname,
                        client.node.hostname,
                    )
                    try:
                        client.exec_command(sudo=True, cmd="test -e %s" % src_path)
                        log.info("File %s exists. Proceeding with download.", src_path)
                        client.download_file(src=src_path, dst=dst_path, sudo=True)
                        log.info("Downloaded %s to %s", src_path, dst_path)
                        client.exec_command(sudo=True, cmd="rm -f %s" % src_path)
                        log.info("Deleted source file %s", src_path)
                    except CommandFailed:
                        log.warning(
                            "File %s does not exist on client %s. Skipping download.",
                            src_path,
                            client.node.hostname,
                        )
            except Exception as e:
                log.error(
                    "Failed to download FIO file for client %s : %s",
                    client.node.hostname,
                    str(e),
                )
        elif io_type == "smallfile":
            metrics = fs_scale_utils.run_io_operations_parallel(
                mount_paths,
                "smallfile",
                io_operation=io_operation,
                io_threads=io_threads,
                io_file_size=io_file_size,
                io_files=io_files,
            )
            log.info("Metrics of all Runs : %s", metrics)
        elif io_type == "dd":
            fs_scale_utils.run_io_operations_parallel(
                mount_paths,
                io_type="dd",
                dd_block_size="1M",
                dd_count="1024",
            )
        elif io_type == "cthon":
            fs_scale_utils.run_io_operations_parallel(
                mount_paths,
                io_type="cthon",
                cthonLogPath=log_dir,
            )

        collected_logs = fs_scale_utils.collect_and_compress_logs(
            clients[0], fs_util_v1, log_dir
        )
        log.info("Logs captures : %s", collected_logs)

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
            log.info("Check for crash from : %s", daemon_list)
            fs_system_utils.crash_check(
                clients[0], crash_copy=1, daemon_list=daemon_list
            )
        except Exception as e:
            log.error("Error in crash validation block: %s", e)

        if config.get("cleanup", True):
            try:
                log.info("Starting cleanup process.")
                cleanup_exit_code = cleanup.run(ceph_cluster, **kw)
                if cleanup_exit_code == 0:
                    log.info("Cleanup completed successfully.")
                else:
                    log.error("Cleanup failed.")
            except Exception as e:
                log.error("Cleanup process encountered an error: %s", e)
                log.error("Stack trace:\n%s", traceback.format_exc())
