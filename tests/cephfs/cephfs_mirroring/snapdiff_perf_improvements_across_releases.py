import concurrent.futures
import os
import time
import traceback

from tests.cephfs.cephfs_mirroring.cephfs_mirroring_utils import CephfsMirroringUtils
from utility.log import Log

log = Log(__name__)

log_base_dir = os.path.dirname(log.logger.handlers[0].baseFilename)
log_dir = f"{log_base_dir}/SnapDiff_Results/"
os.mkdir(log_dir)
log.info(f"Log Dir : {log_dir}")


def run(ceph_cluster, **kw):
    """
    CEPH-83595260 - Performance evaluation of snapdiff feature using CephFS Mirroring after upgrading to RHCS 8.0
    """
    try:
        config = kw.get("config")
        ceph_cluster_dict = kw.get("ceph_cluster_dict")
        test_data = kw.get("test_data")
        fs_mirroring_utils = CephfsMirroringUtils(
            ceph_cluster_dict.get("ceph1"), ceph_cluster_dict.get("ceph2")
        )
        env = fs_mirroring_utils.prepare_env_snapdiff(
            config, ceph_cluster_dict, test_data
        )
        if not env:
            log.error("Failed to prepare environment for snapdiff testing.")
            return 1

        source_clients = env["source_clients"]
        target_clients = env["target_clients"]
        fs_util_ceph1 = env["fs_util_ceph1"]
        source_fs = env["source_fs"]
        target_fs = env["target_fs"]
        fs_mirroring_utils = env["fs_mirroring_utils"]
        cephfs_mirror_node = env["cephfs_mirror_node"]
        nfs_server = env["nfs_server"]
        nfs_name = env["nfs_name"]

        target_user = "mirror_remote_user_snap_diff_1"
        target_site = "remote_site_snap_diff_1"

        fs_mirroring_utils.deploy_cephfs_mirroring(
            source_fs,
            source_clients[0],
            target_fs,
            target_clients[0],
            target_user,
            target_site,
        )

        result_file = config.get("result_file")
        csv_file = f"{log_dir}/{result_file}"

        ceph_version_cmd = source_clients[0].exec_command(sudo=True, cmd="ceph version")
        log.info(f"Version : {ceph_version_cmd}")
        ceph_version_out = ceph_version_cmd[0].strip()
        log.info(f"Ceph Version: {ceph_version_out}")

        fs_mirroring_utils.initialize_csv_file_snapdiff(csv_file, ceph_version_out)

        log.info("Create Subvolumes for adding Data")
        subvol_group_name = "subvolgroup_1"
        subvolume_names = ["subvol_1", "subvol_2", "subvol_3"]

        fs_util_ceph1.create_subvolumegroup(
            source_clients[0], vol_name=source_fs, group_name=subvol_group_name
        )

        for subvol in subvolume_names:
            fs_util_ceph1.create_subvolume(
                source_clients[0],
                vol_name=source_fs,
                subvol_name=subvol,
                group_name=subvol_group_name,
            )

        mount_paths, subvol_paths = fs_mirroring_utils.mount_subvolumes_snapdiff(
            source_client=source_clients[0],
            fs_util_ceph1=fs_util_ceph1,
            default_fs=source_fs,
            subvolume_names=subvolume_names,
            subvol_group_name=subvol_group_name,
            nfs_server=nfs_server,
            nfs_name=nfs_name,
        )
        log.info(f"Mount Paths : {mount_paths}")
        log.info(f"Sub Volume Paths : {subvol_paths}")

        if mount_paths == 1:
            log.error("Mounting of subvolumes failed")
            raise Exception("Mount operation failed")

        io_dir = "snapdiff_io_dir"
        io_dir_paths = {}
        for mount_type in ["kernel", "fuse", "nfs"]:
            mount_path = mount_paths[mount_type]
            subvol_base = subvol_paths[mount_type].split("/")[0] + "/"
            full_path = f"{mount_path}{subvol_base}{io_dir}"
            log.info(f"Creating I/O directory at: {full_path}")
            source_clients[0].exec_command(
                sudo=True,
                cmd=f"mkdir -p {full_path}",
            )
            io_dir_paths[mount_type] = full_path

        subvol_paths_without_uuid = {}
        for mtype, path in subvol_paths.items():
            subvol_paths_without_uuid[mtype] = path.split("/")[0] + "/"
        log.info(f"Subvolume Paths without UUID: {subvol_paths_without_uuid}")

        log.info("Add paths for mirroring to remote location")
        for mount_type in ["kernel", "fuse", "nfs"]:
            subvol_path_without_uuid = subvol_paths_without_uuid[mount_type]
            fs_mirroring_utils.add_path_for_mirroring(
                source_clients[0],
                source_fs,
                f"/volumes/{subvol_group_name}/{subvol_path_without_uuid}",
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

        log.info(f"I/O directory paths for future use: {io_dir_paths}")

        overall_start = time.time()
        log.info("Starting file creation in all I/O directories...")

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(
                    fs_mirroring_utils.create_files_for_snapdiff,
                    source_clients[0],
                    path,
                    10,
                    1,
                )
                for path in io_dir_paths.values()
            ]
            for future in concurrent.futures.as_completed(futures):
                future.result()

        overall_duration = time.time() - overall_start
        log.info(f"All file creation tasks completed in {overall_duration:.2f} seconds")

        log.info("Create Initial Snapshots, Validate the Sync, and Log Info")

        snap_suffix = "initial"
        client_types = {
            "kernel": "Kernel",
            "fuse": "Fuse",
            "nfs": "NFS",
        }

        snapshot_sync_info = {}

        # Create, Validate, and Log for each client type
        for ctype, label in client_types.items():
            snapshot_name = f"snap_{ctype[0]}_{snap_suffix}"

            # Create snapshot
            fs_mirroring_utils.create_snapshot_snapdiff(
                fs_util_ceph1,
                source_clients[0],
                mount_paths[ctype],
                subvol_paths_without_uuid[ctype],
                snapshot_name,
                source_fs,
                subvolume=True,
                subvol_name=subvol_paths_without_uuid[ctype].rstrip("/"),
                subvol_group=subvol_group_name,
            )

            # Validate snapshot sync
            sync_info = fs_mirroring_utils.validate_snapshot_sync(
                fs_mirroring_utils,
                cephfs_mirror_node[0],
                source_fs,
                snapshot_name,
                fsid,
                asok_file,
                filesystem_id,
                peer_uuid,
            )

            snapshot_sync_info[label] = sync_info

            # Log sync info
            if sync_info:
                log.info(
                    f"{label} Initial Snapshot Info - Name: {sync_info['snapshot_name']}, "
                    f"Duration: {sync_info['sync_duration']}, "
                    f"Time Stamp: {sync_info['sync_time_stamp']}, "
                    f"Snaps Synced: {sync_info['snaps_synced']}"
                )
                fs_mirroring_utils.log_snapshot_info_snapdiff(
                    f"{label} Initial", sync_info, csv_file
                )

        common_args = {
            "io_dir_paths": io_dir_paths,
            "source_clients": source_clients,
            "mount_paths": mount_paths,
            "subvol_paths_without_uuid": subvol_paths_without_uuid,
            "source_fs": source_fs,
            "subvol_group_name": subvol_group_name,
            "fs_mirroring_utils": fs_mirroring_utils,
            "cephfs_mirror_node": cephfs_mirror_node,
            "fsid": fsid,
            "asok_file": asok_file,
            "filesystem_id": filesystem_id,
            "peer_uuid": peer_uuid,
            "csv_file": csv_file,
        }

        # Run incremental snapshots
        fs_mirroring_utils.modify_and_create_snapshot_snapdiff(
            fs_util_ceph1,
            num_files=1,
            snap_suffix="inc1",
            label_suffix="1",
            **common_args,
        )
        fs_mirroring_utils.modify_and_create_snapshot_snapdiff(
            fs_util_ceph1,
            num_files=2,
            snap_suffix="inc2",
            label_suffix="2",
            **common_args,
        )
        fs_mirroring_utils.modify_and_create_snapshot_snapdiff(
            fs_util_ceph1,
            num_files=5,
            snap_suffix="inc3",
            label_suffix="3",
            **common_args,
        )
        fs_mirroring_utils.modify_and_create_snapshot_snapdiff(
            fs_util_ceph1,
            num_files=10,
            snap_suffix="inc4",
            label_suffix="4",
            **common_args,
        )

        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        if config.get("cleanup", True):
            log.info("Delete the snapshots")
            snap_suffixes = ["initial", "inc1", "inc2", "inc3", "inc4"]
            client_types = {
                "kernel": "Kernel",
                "fuse": "Fuse",
                "nfs": "NFS",
            }
            for snap_suffix in snap_suffixes:
                for ctype in client_types:
                    snapshot_name = f"snap_{ctype[0]}_{snap_suffix}"
                    subvol_name = subvol_paths_without_uuid[ctype].rstrip("/")

                    fs_util_ceph1.remove_snapshot(
                        client=source_clients[0],
                        vol_name=source_fs,
                        subvol_name=subvol_name,
                        snap_name=snapshot_name,
                        validate=True,
                        group_name=subvol_group_name,
                        force=True,
                    )
                    log.info(
                        f"Successfully removed snapshot: {snapshot_name} for {ctype}"
                    )

            log.info("Unmount the paths")
            paths_to_unmount = [
                mount_paths["kernel"],
                mount_paths["fuse"],
                mount_paths["nfs"],
            ]
            for path in paths_to_unmount:
                source_clients[0].exec_command(sudo=True, cmd=f"umount -l {path}")

            log.info("Remove paths used for mirroring")
            for mount_type in ["kernel", "fuse", "nfs"]:
                subvol_path_without_uuid = subvol_paths_without_uuid[mount_type]
                fs_mirroring_utils.remove_path_from_mirroring(
                    source_clients[0],
                    source_fs,
                    f"/volumes/{subvol_group_name}/{subvol_path_without_uuid}",
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
            for subvol in subvolume_names:
                fs_util_ceph1.remove_subvolume(
                    source_clients[0],
                    vol_name=source_fs,
                    subvol_name=subvol,
                    group_name=subvol_group_name,
                )

            log.info("Remove Subvolume Group")
            fs_util_ceph1.remove_subvolumegroup(
                source_clients[0],
                vol_name=source_fs,
                group_name=subvol_group_name,
            )

            log.info("Delete the mounted paths")
            for mount_type in ["kernel", "fuse", "nfs"]:
                path = io_dir_paths.get(mount_type)
                if path:
                    source_clients[0].exec_command(sudo=True, cmd=f"rm -rf {path}")

            fs_util_ceph1.remove_fs(source_clients[0], source_fs, validate=False)
            fs_util_ceph1.remove_fs(target_clients[0], target_fs, validate=False)
