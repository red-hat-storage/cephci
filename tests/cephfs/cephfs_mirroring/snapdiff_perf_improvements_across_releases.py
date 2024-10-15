import csv
import os
import random
import string
import time
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_mirroring.cephfs_mirroring_utils import CephfsMirroringUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)

log_base_dir = os.path.dirname(log.logger.handlers[0].baseFilename)
log_dir = f"{log_base_dir}/SnapDiff_Results/"
os.mkdir(log_dir)
log.info(f"Log Dir : {log_dir}")


def initialize_csv_file(csv_file):
    try:
        with open(csv_file, mode="x", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(
                [
                    "Snapshot Type",
                    "Snapshot Name",
                    "Sync Duration",
                    "Sync Timestamp",
                    "Snaps Synced",
                ]
            )
    except FileExistsError:
        pass


def log_snapshot_info(snapshot_type, snapshot_info, csv_file):
    with open(csv_file, mode="a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                snapshot_type,
                snapshot_info["snapshot_name"],
                snapshot_info["sync_duration"],
                snapshot_info["sync_time_stamp"],
                snapshot_info["snaps_synced"],
            ]
        )


def run(ceph_cluster, **kw):
    """
    CEPH-83595260 - Performance evaluation of snapdiff feature using CephFS Mirroring after upgrading to RHCS 8.0

    Steps:
        1. Prepare the Ceph clients and authenticate them on both clusters.
        2. Set up CephFS volumes and MDS placement on both source and target clusters.
        3. Enable CephFS mirroring modules and establish a peer connection between the two clusters.
        4. Create subvolumes and mount them on kernel and FUSE clients.
        5. Create directories for I/O operations.
        6. Add paths for mirroring and create large files for data sync tests.
        7. Take initial snapshots and validate the sync across the clusters.
        8. Perform incremental snapshots after modifying files, and log the results.

    Raises:
        CommandFailed: If any command execution fails during the setup or snapshot process.

    Logs:
        - Snapshot information such as sync duration, timestamp, and synced snapshots are logged
          to CSV files for both kernel and FUSE clients.

    Returns:
        int: 0 if the test completes successfully, or 1 if any critical step fails.
    """
    try:
        config = kw.get("config")
        ceph_cluster_dict = kw.get("ceph_cluster_dict")
        test_data = kw.get("test_data")
        # fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        fs_util_ceph1 = FsUtils(ceph_cluster_dict.get("ceph1"), test_data=test_data)
        fs_util_ceph2 = FsUtils(ceph_cluster_dict.get("ceph2"), test_data=test_data)
        fs_mirroring_utils = CephfsMirroringUtils(
            ceph_cluster_dict.get("ceph1"), ceph_cluster_dict.get("ceph2")
        )
        build = config.get("build", config.get("rhbuild"))
        source_clients = ceph_cluster_dict.get("ceph1").get_ceph_objects("client")
        target_clients = ceph_cluster_dict.get("ceph2").get_ceph_objects("client")
        cephfs_mirror_node = ceph_cluster_dict.get("ceph1").get_ceph_objects(
            "cephfs-mirror"
        )

        log.info("checking Pre-requisites")
        if not source_clients or not target_clients:
            log.info(
                "This test requires a minimum of 1 client node on both ceph1 and ceph2."
            )
            return 1
        log.info("Preparing Clients...")
        fs_util_ceph1.prepare_clients(source_clients, build)
        fs_util_ceph2.prepare_clients(target_clients, build)
        fs_util_ceph1.auth_list(source_clients)
        fs_util_ceph2.auth_list(target_clients)
        mds_nodes = ceph_cluster_dict.get("ceph1").get_ceph_objects("mds")

        result_file = config.get("result_file")
        csv_file = f"{log_dir}/{result_file}"

        ceph_version_cmd = source_clients[0].exec_command(sudo=True, cmd="ceph version")
        log.info(f"Version : {ceph_version_cmd}")
        ceph_version_out = ceph_version_cmd[0].strip()
        log.info(f"Ceph Version: {ceph_version_out}")

        with open(csv_file, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(
                [
                    "Snapshot Type",
                    "Snapshot Name",
                    "Sync Duration",
                    "Sync Timestamp",
                    "Snaps Synced",
                    # f"Ceph Version: {ceph_version_out}",
                ]
            )

        log.info("Initialize the CSV file with headers if it does not exist")
        initialize_csv_file(csv_file)

        log.info(f"Available MDS Nodes {mds_nodes[0]}")
        log.info(len(mds_nodes))
        source_fs = (
            config.get("source_fs") if not erasure else f'{config.get("source_fs")}-ec'
        )
        io_dir = "snapdiff_io_dir"
        mds_names = []
        for mds in mds_nodes:
            mds_names.append(mds.node.hostname)
        hosts_list1 = mds_names[-6:-4]
        mds_hosts_1 = " ".join(hosts_list1) + " "
        log.info(f"MDS host list 1 {mds_hosts_1}")
        source_clients[0].exec_command(
            sudo=True,
            cmd=f'ceph fs volume create {source_fs} --placement="2 {mds_hosts_1}"',
        )
        fs_util_ceph1.wait_for_mds_process(source_clients[0], source_fs)

        log.info("Create required filesystem on Target Cluster...")
        mds_nodes = ceph_cluster_dict.get("ceph2").get_ceph_objects("mds")
        log.info(f"Available MDS Nodes {mds_nodes[0]}")
        log.info(len(mds_nodes))
        target_fs = (
            config.get("target_fs") if not erasure else f'{config.get("target_fs")}-ec'
        )

        mds_names = []
        for mds in mds_nodes:
            mds_names.append(mds.node.hostname)
        hosts_list1 = mds_names[-4:-2]
        mds_hosts_1 = " ".join(hosts_list1) + " "
        log.info(f"MDS host list 1 {mds_hosts_1}")
        target_clients[0].exec_command(
            sudo=True,
            cmd=f'ceph fs volume create {target_fs} --placement="2 {mds_hosts_1}"',
        )
        fs_util_ceph1.wait_for_mds_process(target_clients[0], target_fs)

        target_user = "mirror_remote_user_snap_diff"
        target_site_name = "remote_site_snap_diff"

        log.info("Enable mirroring mgr module on source and destination")
        enable_mirroring_on_source = fs_mirroring_utils.enable_mirroring_module(
            source_clients[0]
        )
        if enable_mirroring_on_source:
            log.error("Mirroring module not enabled on Source Cluster.")
            raise CommandFailed("Enable mirroring mgr module failed")

        enable_mirroring_on_target = fs_mirroring_utils.enable_mirroring_module(
            target_clients[0]
        )
        if enable_mirroring_on_target:
            log.error("Mirroring module not enabled on Target Cluster.")
            raise CommandFailed("Enable mirroring mgr module failed")

        log.info("Create a user on target cluster for peer connection")
        target_user_for_peer = fs_mirroring_utils.create_authorize_user(
            target_fs,
            target_user,
            target_clients[0],
        )
        if target_user_for_peer:
            log.error("User Creation Failed with the expected caps")
            raise CommandFailed("User Creation Failed")

        log.info(f"Enable cephfs mirroring module on the {source_fs}")
        fs_mirroring_utils.enable_snapshot_mirroring(source_fs, source_clients[0])

        log.info("Create the peer bootstrap")
        bootstrap_token = fs_mirroring_utils.create_peer_bootstrap(
            target_fs, target_user, target_site_name, target_clients[0]
        )

        log.info("Import the bootstrap on source")
        fs_mirroring_utils.import_peer_bootstrap(
            source_fs, bootstrap_token, source_clients[0]
        )

        log.info("Get Peer Connection Information")
        validate_peer_connection = fs_mirroring_utils.validate_peer_connection(
            source_clients[0], source_fs, target_site_name, target_user, target_fs
        )
        if validate_peer_connection:
            log.error("Peer Connection not established")
            raise CommandFailed("Peer Connection failed to establish")

        log.info("Create Subvolumes for adding Data")
        log.info("Scenario 1 : ")
        subvolumegroup_list = [
            {"vol_name": source_fs, "group_name": "subvolgroup_1"},
        ]
        for subvolumegroup in subvolumegroup_list:
            fs_util_ceph1.create_subvolumegroup(source_clients[0], **subvolumegroup)

        subvolume_list = [
            {
                "vol_name": source_fs,
                "subvol_name": "subvol_1",
                "group_name": "subvolgroup_1",
                "size": "5368709120",
            },
            {
                "vol_name": source_fs,
                "subvol_name": "subvol_2",
                "group_name": "subvolgroup_1",
                "size": "5368709120",
            },
        ]
        for subvolume in subvolume_list:
            fs_util_ceph1.create_subvolume(source_clients[0], **subvolume)

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        log.info("Mount 1 subvolume on kernel and 1 subvloume on Fuse → Client1")
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        mon_node_ips = fs_util_ceph1.get_mon_node_ips()
        log.info("Get the path of subvolume1 on  filesystem")
        subvol_path1, rc = source_clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {source_fs} subvol_1 subvolgroup_1",
        )
        index = subvol_path1.find("subvol_1/")
        if index != -1:
            subvol1_path = subvol_path1[: index + len("subvol_1/")]
        else:
            subvol1_path = subvol_path1
        log.info(subvol1_path)

        fs_util_ceph1.kernel_mount(
            [source_clients[0]],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            extra_params=f",fs={source_fs}",
        )
        log.info("Get the path of subvolume2 on filesystem")
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
        subvol_path2, rc = source_clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {source_fs} subvol_2 subvolgroup_1",
        )
        index = subvol_path2.find("subvol_2/")
        if index != -1:
            subvol2_path = subvol_path2[: index + len("subvol_2/")]
        else:
            subvol2_path = subvol_path2
        log.info(subvol2_path)
        fs_util_ceph1.fuse_mount(
            [source_clients[0]],
            fuse_mounting_dir_1,
            extra_params=f"--client_fs {source_fs}",
        )

        source_clients[0].exec_command(
            sudo=True,
            cmd=f"mkdir {kernel_mounting_dir_1}{subvol1_path}{io_dir}",
        )
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"mkdir {fuse_mounting_dir_1}{subvol2_path}{io_dir}",
        )
        log.info("Add paths for mirroring to remote location")
        fs_mirroring_utils.add_path_for_mirroring(
            source_clients[0], source_fs, f"{subvol1_path}"
        )
        fs_mirroring_utils.add_path_for_mirroring(
            source_clients[0], source_fs, f"{subvol2_path}"
        )

        fsid = fs_mirroring_utils.get_fsid(cephfs_mirror_node[0])
        daemon_name = fs_mirroring_utils.get_daemon_name(source_clients[0])
        asok_file = fs_mirroring_utils.get_asok_file(
            cephfs_mirror_node[0], fsid, daemon_name
        )
        filesystem_id = fs_mirroring_utils.get_filesystem_id_by_name(
            source_clients[0], source_fs
        )
        peer_uuid = fs_mirroring_utils.get_peer_uuid_by_name(
            source_clients[0], source_fs
        )

        file_paths = [
            f"{kernel_mounting_dir_1}{subvol1_path}{io_dir}",
            f"{fuse_mounting_dir_1}{subvol2_path}{io_dir}",
        ]

        for path in file_paths:
            create_large_files(source_clients[0], path, 50, 20)

        log.info("Create Initial Snapshots and Validate the Sync")
        snap_k_initial = "snap_k_initial"
        snap_f_initial = "snap_f_initial"
        initial_snapshot_k_info = create_snapshot_and_validate(
            fs_util_ceph1,
            source_clients[0],
            kernel_mounting_dir_1,
            subvol1_path,
            snap_k_initial,
            io_dir,
            fs_mirroring_utils,
            cephfs_mirror_node[0],
            source_fs,
            fsid,
            asok_file,
            filesystem_id,
            peer_uuid,
        )
        initial_snapshot_f_info = create_snapshot_and_validate(
            fs_util_ceph1,
            source_clients[0],
            fuse_mounting_dir_1,
            subvol2_path,
            snap_f_initial,
            io_dir,
            fs_mirroring_utils,
            cephfs_mirror_node[0],
            source_fs,
            fsid,
            asok_file,
            filesystem_id,
            peer_uuid,
        )

        log.info("Log initial snapshot info to a csv file")
        if initial_snapshot_k_info:
            log.info(
                f"Kernel Snapshot Info - Name: {initial_snapshot_k_info['snapshot_name']}, "
                f"Duration: {initial_snapshot_k_info['sync_duration']}, "
                f"Time Stamp: {initial_snapshot_k_info['sync_time_stamp']}, "
                f"Snaps Synced: {initial_snapshot_k_info['snaps_synced']}"
            )
            log_snapshot_info("Kernel Initial", initial_snapshot_k_info, csv_file)
        if initial_snapshot_f_info:
            log.info(
                f"Fuse Snapshot Info - Name: {initial_snapshot_f_info['snapshot_name']}, "
                f"Duration: {initial_snapshot_f_info['sync_duration']}, "
                f"Time Stamp: {initial_snapshot_f_info['sync_time_stamp']}, "
                f"Snaps Synced: {initial_snapshot_f_info['snaps_synced']}"
            )
            log_snapshot_info("Fuse Initial", initial_snapshot_f_info, csv_file)

        log.info("Modify few files and take 1st incremental snapshots")
        num_files = 10  # Number of files to modify
        lines_to_remove = 5  # Number of lines to remove from each file
        snap_k_incr1 = "snap_k_inc1"
        snap_f_incr1 = "snap_f_inc1"
        incr1_snapshot_k_info = modify_files_and_take_snapshot(
            source_clients[0],
            f"{kernel_mounting_dir_1}{subvol1_path}{io_dir}",
            num_files,
            lines_to_remove,
            snap_k_incr1,
            fs_util_ceph1,
            kernel_mounting_dir_1,
            subvol1_path,
            io_dir,
            fs_mirroring_utils,
            cephfs_mirror_node[0],
            source_fs,
            fsid,
            asok_file,
            filesystem_id,
            peer_uuid,
        )
        incr1_snapshot_f_info = modify_files_and_take_snapshot(
            source_clients[0],
            f"{fuse_mounting_dir_1}{subvol2_path}{io_dir}",
            num_files,
            lines_to_remove,
            snap_f_incr1,
            fs_util_ceph1,
            fuse_mounting_dir_1,
            subvol2_path,
            io_dir,
            fs_mirroring_utils,
            cephfs_mirror_node[0],
            source_fs,
            fsid,
            asok_file,
            filesystem_id,
            peer_uuid,
        )

        log.info("Log incremental snapshot info to CSV file")
        if incr1_snapshot_k_info:
            log.info(
                f"Kernel Snapshot Info - Name: {incr1_snapshot_k_info['snapshot_name']}, "
                f"Duration: {incr1_snapshot_k_info['sync_duration']}, "
                f"Time Stamp: {incr1_snapshot_k_info['sync_time_stamp']}, "
                f"Snaps Synced: {incr1_snapshot_k_info['snaps_synced']}"
            )
            log_snapshot_info("Kernel Incremental 1", incr1_snapshot_k_info, csv_file)
        if incr1_snapshot_f_info:
            log.info(
                f"Fuse Snapshot Info - Name: {incr1_snapshot_f_info['snapshot_name']}, "
                f"Duration: {incr1_snapshot_f_info['sync_duration']}, "
                f"Time Stamp: {incr1_snapshot_f_info['sync_time_stamp']}, "
                f"Snaps Synced: {incr1_snapshot_f_info['snaps_synced']}"
            )
            log_snapshot_info("Fuse Incremental 1", incr1_snapshot_f_info, csv_file)

        log.info("Modify few files and take 2nd incremental snapshots")
        num_files = 5
        lines_to_remove = 2
        snap_k_incr2 = "snap_k_inc2"
        snap_f_incr2 = "snap_f_inc2"
        incr2_snapshot_k_info = modify_files_and_take_snapshot(
            source_clients[0],
            f"{kernel_mounting_dir_1}{subvol1_path}{io_dir}",
            num_files,
            lines_to_remove,
            snap_k_incr2,
            fs_util_ceph1,
            kernel_mounting_dir_1,
            subvol1_path,
            io_dir,
            fs_mirroring_utils,
            cephfs_mirror_node[0],
            source_fs,
            fsid,
            asok_file,
            filesystem_id,
            peer_uuid,
        )
        incr2_snapshot_f_info = modify_files_and_take_snapshot(
            source_clients[0],
            f"{fuse_mounting_dir_1}{subvol2_path}{io_dir}",
            num_files,
            lines_to_remove,
            snap_f_incr2,
            fs_util_ceph1,
            fuse_mounting_dir_1,
            subvol2_path,
            io_dir,
            fs_mirroring_utils,
            cephfs_mirror_node[0],
            source_fs,
            fsid,
            asok_file,
            filesystem_id,
            peer_uuid,
        )

        log.info("Log incremental snapshot info to CSV file")
        if incr2_snapshot_k_info:
            log.info(
                f"Kernel Snapshot Info - Name: {incr2_snapshot_k_info['snapshot_name']}, "
                f"Duration: {incr2_snapshot_k_info['sync_duration']}, "
                f"Time Stamp: {incr2_snapshot_k_info['sync_time_stamp']}, "
                f"Snaps Synced: {incr2_snapshot_k_info['snaps_synced']}"
            )
            log_snapshot_info("Kernel Incremental 2", incr2_snapshot_k_info, csv_file)
        if incr2_snapshot_f_info:
            log.info(
                f"Fuse Snapshot Info - Name: {incr2_snapshot_f_info['snapshot_name']}, "
                f"Duration: {incr2_snapshot_f_info['sync_duration']}, "
                f"Time Stamp: {incr2_snapshot_f_info['sync_time_stamp']}, "
                f"Snaps Synced: {incr2_snapshot_f_info['snaps_synced']}"
            )
            log_snapshot_info("Fuse Incremental 2", incr2_snapshot_f_info, csv_file)

        log.info("Modify few files and take 3rd incremental snapshots")
        num_files = 1
        lines_to_remove = 1
        snap_k_incr3 = "snap_k_inc3"
        snap_f_incr3 = "snap_f_inc3"
        incr3_snapshot_k_info = modify_files_and_take_snapshot(
            source_clients[0],
            f"{kernel_mounting_dir_1}{subvol1_path}{io_dir}",
            num_files,
            lines_to_remove,
            snap_k_incr3,
            fs_util_ceph1,
            kernel_mounting_dir_1,
            subvol1_path,
            io_dir,
            fs_mirroring_utils,
            cephfs_mirror_node[0],
            source_fs,
            fsid,
            asok_file,
            filesystem_id,
            peer_uuid,
        )
        incr3_snapshot_f_info = modify_files_and_take_snapshot(
            source_clients[0],
            f"{fuse_mounting_dir_1}{subvol2_path}{io_dir}",
            num_files,
            lines_to_remove,
            snap_f_incr3,
            fs_util_ceph1,
            fuse_mounting_dir_1,
            subvol2_path,
            io_dir,
            fs_mirroring_utils,
            cephfs_mirror_node[0],
            source_fs,
            fsid,
            asok_file,
            filesystem_id,
            peer_uuid,
        )

        log.info("Log incremental snapshot info to CSV file")
        if incr3_snapshot_k_info:
            log.info(
                f"Kernel Snapshot Info - Name: {incr3_snapshot_k_info['snapshot_name']}, "
                f"Duration: {incr3_snapshot_k_info['sync_duration']}, "
                f"Time Stamp: {incr3_snapshot_k_info['sync_time_stamp']}, "
                f"Snaps Synced: {incr3_snapshot_k_info['snaps_synced']}"
            )
            log_snapshot_info("Kernel Incremental 3", incr3_snapshot_k_info, csv_file)
        if incr3_snapshot_f_info:
            log.info(
                f"Fuse Snapshot Info - Name: {incr3_snapshot_f_info['snapshot_name']}, "
                f"Duration: {incr3_snapshot_f_info['sync_duration']}, "
                f"Time Stamp: {incr3_snapshot_f_info['sync_time_stamp']}, "
                f"Snaps Synced: {incr3_snapshot_f_info['snaps_synced']}"
            )
            log_snapshot_info("Fuse Incremental 3", incr3_snapshot_f_info, csv_file)

        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        if config.get("cleanup", True):
            log.info("Delete the snapshots")
            snapshots_to_delete = [
                f"{kernel_mounting_dir_1}{subvol1_path}/.snap/*",
                f"{fuse_mounting_dir_1}{subvol2_path}.snap/*",
            ]
            for snapshot_path in snapshots_to_delete:
                source_clients[0].exec_command(sudo=True, cmd=f"rmdir {snapshot_path}")

            log.info("Unmount the paths")
            paths_to_unmount = [kernel_mounting_dir_1, fuse_mounting_dir_1]
            for path in paths_to_unmount:
                source_clients[0].exec_command(sudo=True, cmd=f"umount -l {path}")

            log.info("Remove paths used for mirroring")
            paths_to_remove = [subvol1_path, subvol2_path]
            for path in paths_to_remove:
                fs_mirroring_utils.remove_path_from_mirroring(
                    source_clients[0], source_fs, path
                )

            log.info("Destroy CephFS Mirroring setup.")
            fs_mirroring_utils.destroy_cephfs_mirroring(
                source_fs,
                source_clients[0],
                target_fs,
                target_clients[0],
                target_user,
                peer_uuid,
            )

            log.info("Remove Subvolumes")
            for subvolume in subvolume_list:
                fs_util_ceph1.remove_subvolume(
                    source_clients[0],
                    **subvolume,
                )

            log.info("Remove Subvolume Group")
            for subvolumegroup in subvolumegroup_list:
                fs_util_ceph1.remove_subvolumegroup(source_clients[0], **subvolumegroup)

            log.info("Delete the mounted paths")
            source_clients[0].exec_command(
                sudo=True, cmd=f"rm -rf {kernel_mounting_dir_1}"
            )
            source_clients[0].exec_command(
                sudo=True, cmd=f"rm -rf {fuse_mounting_dir_1}"
            )
            fs_util_ceph1.remove_fs(source_clients[0], source_fs, validate=False)
            fs_util_ceph1.remove_fs(target_clients[0], target_fs, validate=False)


def create_large_files(clients, io_path, file_size_mb, file_count=1):
    """
    Function to create multiple large files with random content using base64.

    :param io_path: Path where the files will be created.
    :param file_size_mb: Size of each file in MB.
    :param file_count: Number of files to create.
    """
    file_size_bytes = file_size_mb * 1024 * 1024

    for i in range(1, file_count + 1):
        file_name = f"{io_path}/large_file_{file_size_mb}MB_{i}.txt"

        start_time = time.time()
        base64_cmd = f"base64 /dev/urandom | head -c {file_size_bytes} > {file_name}"
        clients.exec_command(
            sudo=True,
            cmd=base64_cmd,
            long_running=True,
        )
        end_time = time.time()
        time_taken = end_time - start_time

        log.info(f"File '{file_name}' created with a size of {file_size_mb} MB.")
        log.info(f"Time taken: {time_taken:.2f} seconds.")


def remove_random_lines_from_path(
    client, folder_path, num_files_to_modify, num_lines_to_remove
):
    """
    Removes a specified number of random lines from randomly selected files in a given folder on a client.

    :param client: The remote client where the command will be executed (e.g., source_clients[0]).
    :param folder_path: Path where the files are located.
    :param num_files_to_modify: Number of files to randomly select for modification.
    :param num_lines_to_remove: Number of lines to remove from each selected file.
    """

    out, err = client.exec_command(f"ls {folder_path}/")
    log.info(f"Out : {out}")
    files = out.splitlines()
    log.info(f"files : {files}")
    log.info(f" length : {len(files)}")
    if len(files) < num_files_to_modify:
        log.error(f"Not enough files to modify. Total files: {len(files)}")
        return

    random_files = random.sample(files, num_files_to_modify)
    log.info(f"random files : {random_files}")
    for file in random_files:
        log.info(f"{file}")
        full_path = f"{folder_path}/{file}"
        log.info(f"Full Path : {full_path}")
        out, _ = client.exec_command(f"wc -l < {full_path}")
        total_lines = int(out.strip())
        log.info(f"total lines : {total_lines}")

        if total_lines < num_lines_to_remove:
            log.info(f"{file} has less than {num_lines_to_remove} lines, skipping.")
            continue

        lines_to_remove = sorted(
            random.sample(range(1, total_lines + 1), num_lines_to_remove), reverse=True
        )
        log.info(f"lines_to_remove : {lines_to_remove}")

        for line_number in lines_to_remove:
            client.exec_command(sudo=True, cmd=f"sed -i '{line_number}d' {full_path}")

        log.info(f"Removed {num_lines_to_remove} random lines from: {file}")


def create_snapshot_and_validate(
    fs_util,
    client,
    mounting_dir,
    subvol_path,
    snap_name,
    io_dir,
    fs_mirroring_utils,
    cephfs_mirror_node,
    source_fs,
    fsid,
    asok_file,
    filesystem_id,
    peer_uuid,
):
    """
    function to create a snapshot and validate synchronization.
    Returns snapshot details for comparison.
    """
    client.exec_command(
        sudo=True, cmd=f"mkdir {mounting_dir}{subvol_path}/.snap/{snap_name}"
    )

    result = fs_mirroring_utils.validate_snapshot_sync_status(
        cephfs_mirror_node,
        source_fs,
        snap_name,
        fsid,
        asok_file,
        filesystem_id,
        peer_uuid,
    )

    if result:
        log.info(f"Snapshot '{result['snapshot_name']}' has been synced:")
        log.info(
            f"Sync Duration: {result['sync_duration']} of '{result['snapshot_name']}'"
        )
        log.info(
            f"Sync Time Stamp: {result['sync_time_stamp']} of '{result['snapshot_name']}'"
        )
        log.info(
            f"Snaps Synced: {result['snaps_synced']} of '{result['snapshot_name']}'"
        )

        return {
            "snapshot_name": result["snapshot_name"],
            "sync_duration": result["sync_duration"],
            "sync_time_stamp": result["sync_time_stamp"],
            "snaps_synced": result["snaps_synced"],
        }
    else:
        log.error(f"Snapshot '{snap_name}' not found or not synced.")
        raise CommandFailed(f"Snapshot '{snap_name}' not found or not synced.")


def modify_files_and_take_snapshot(
    client,
    folder_to_modify,
    num_files,
    lines_to_remove,
    snap_name,
    fs_util,
    mounting_dir,
    subvol_path,
    io_dir,
    fs_mirroring_utils,
    cephfs_mirror_node,
    source_fs,
    fsid,
    asok_file,
    filesystem_id,
    peer_uuid,
):
    """
    Modify a few files and take incremental snapshots.
    Returns snapshot information for further comparison.
    """
    remove_random_lines_from_path(client, folder_to_modify, num_files, lines_to_remove)

    snapshot_info = create_snapshot_and_validate(
        fs_util,
        client,
        mounting_dir,
        subvol_path,
        snap_name,
        io_dir,
        fs_mirroring_utils,
        cephfs_mirror_node,
        source_fs,
        fsid,
        asok_file,
        filesystem_id,
        peer_uuid,
    )

    return snapshot_info
