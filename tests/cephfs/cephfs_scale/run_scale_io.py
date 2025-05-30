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

        # nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        # nfs_server = nfs_servers[0].node.hostname
        nfs_server = "extensa023"
        nfs_name = "cephfs-nfs"
        nfs_server_ip = "10.1.172.123"
        # nfs_server_ip = nfs_servers[0].node.ipaddress
        # log.info(f"NFS Server IP Adderess ; {nfs_server_ip}")

        try:
            fs_util_v1.create_nfs(
                clients[0],
                nfs_name,
                validate=False,
                placement="1 %s" % nfs_server,
            )
            log.info("NFS cluster %s created successfully." % nfs_name)
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
            nfs_cluster_name=nfs_name if mount_type == "nfs" else None,
            nfs_server=nfs_server if mount_type == "nfs" else None,
            nfs_server_ip=nfs_server_ip if mount_type == "nfs" else None,
        )

        fs_scale_utils.collect_logs(
            ceph_cluster, clients, default_fs, fs_util_v1, log_dir
        )

        log.info("Start Running IO's in parallel on all Clients")
        if io_type == "largefile":
            create_large_file_script = "create_large_file.sh"
            for client in clients:
                client.upload_file(
                    sudo=True,
                    src="tests/cephfs/cephfs_scale/create_large_file.sh",
                    dst=f"/root/{create_large_file_script}",
                )
                client.exec_command(
                    sudo=True,
                    cmd=f"chmod 777 /root/{create_large_file_script}",
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
                mount_paths,
                "smallfile",
                io_operation=io_operation,
                io_threads=io_threads,
                io_file_size=io_file_size,
                io_files=io_files,
            )
            log.info(f"Metrics of all Runs : {metrics}")

        collected_logs = fs_scale_utils.collect_and_compress_logs(
            clients[0], fs_util_v1, log_dir
        )
        log.info(f"Logs captures : {collected_logs}")

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
