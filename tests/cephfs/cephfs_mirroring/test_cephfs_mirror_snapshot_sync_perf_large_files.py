import csv
import os
import secrets
import string
import time
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_mirroring.cephfs_mirroring_utils import CephfsMirroringUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from tests.cephfs.cephfs_volume_management import wait_for_process
from utility.log import Log

log = Log(__name__)

DEFAULT_THREAD_COUNTS = [1, 3, 6, 9, 12]
DEFAULT_NUM_FILES = 5
DEFAULT_FILE_SIZE_MB = 1024  # 1 GB per file
DEFAULT_SUBVOL_SIZE_BYTES = "68719476736"  # 64 GiB

MOUNT_TYPES = ["kernel", "fuse", "nfs"]


def run(ceph_cluster, **kw):
    """
    Snapshot Sync Performance Test -- Large Files (CephFS Mirroring)

    Measures how the cephfs_mirror_max_datasync_threads tunable affects
    snapshot synchronisation throughput for a workload of a few large files
    across kernel, fuse and nfs mount types.

    Three subvolumes (one per mount type) are created once at the start.
    For each configured thread count the test:
        1. Tunes cephfs_mirror_max_datasync_threads on the mirror daemon.
        2. Creates a fresh data directory inside each subvolume.
        3. Generates N large files (dd) sequentially into those directories.
        4. Takes a snapshot on each subvolume and waits for sync.
        5. Records sync duration, files/sec, MB/sec, speedup vs 1-thread.
        6. Removes snapshots and data directories, then repeats for next
           thread count.

    Results are written to a CSV file in the log directory.

    Config keys (all optional, sane defaults provided):
        result_file      : str        -- CSV filename, default large_file_sync_perf.csv
        thread_counts    : list[int]  -- thread counts to test, default [1,3,6,9,12]
        num_files        : int        -- files per subvolume, default 5
        file_size_mb     : int        -- size of each file in MB, default 1024 (1 GB)
        subvol_size      : str        -- subvolume quota in bytes, default 64 GiB
        file_gen_timeout : int        -- seconds for file generation cmd, default 7200
        sync_poll_tries  : int        -- max poll attempts for sync, default 240
        sync_poll_delay  : int        -- seconds between sync polls, default 15

    Returns:
        0 on success, 1 on failure.
    """

    source_clients = None
    target_clients = None
    source_fs = None
    target_fs = None
    target_user = "mirror_remote"
    target_site_name = "remote_site"
    fs_mirroring_utils = None
    fs_util_ceph1 = None
    peer_uuid = None
    subvol_group = "subvolgroup_perf_lg"
    subvolumegroup_list = []
    created_subvols = {}
    subvol_paths = {}
    mirroring_paths = []
    mount_dirs = {}
    nfs_name = "cephfs-nfs"
    nfs_export_name = None
    nfs_created = False

    try:
        config = kw.get("config")
        ceph_cluster_dict = kw.get("ceph_cluster_dict")
        test_data = kw.get("test_data")

        # Move log_dir setup inside run() to avoid module-level side-effects
        # at import time (would crash test discovery if no file log handler exists).
        log_base_dir = os.path.dirname(log.logger.handlers[0].baseFilename)
        log_dir = os.path.join(log_base_dir, "LargeFileSyncPerf_Results")
        os.makedirs(log_dir, exist_ok=True)
        log.info("Log Dir: %s", log_dir)

        erasure = (
            FsUtilsV1.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )

        fs_util_ceph1 = FsUtilsV1(ceph_cluster_dict.get("ceph1"), test_data=test_data)
        fs_util_ceph2 = FsUtilsV1(ceph_cluster_dict.get("ceph2"), test_data=test_data)
        fs_util_v1_ceph1 = FsUtilsV1(
            ceph_cluster_dict.get("ceph1"), test_data=test_data
        )
        fs_mirroring_utils = CephfsMirroringUtils(
            ceph_cluster_dict.get("ceph1"), ceph_cluster_dict.get("ceph2")
        )

        build = config.get("build", config.get("rhbuild"))
        source_clients = ceph_cluster_dict.get("ceph1").get_ceph_objects("client")
        target_clients = ceph_cluster_dict.get("ceph2").get_ceph_objects("client")
        cephfs_mirror_node = ceph_cluster_dict.get("ceph1").get_ceph_objects(
            "cephfs-mirror"
        )

        log.info("Checking pre-requisites")
        if not source_clients or not target_clients:
            log.error(
                "This test requires at least 1 client node on both ceph1 and ceph2."
            )
            return 1

        log.info("Preparing clients")
        fs_util_ceph1.prepare_clients(source_clients, build)
        fs_util_ceph2.prepare_clients(target_clients, build)
        fs_util_ceph1.auth_list(source_clients)
        fs_util_ceph2.auth_list(target_clients)

        source_fs = "cephfs" if not erasure else "cephfs-ec"
        target_fs = "cephfs" if not erasure else "cephfs-ec"

        fs_details_source = fs_util_ceph1.get_fs_info(source_clients[0], source_fs)
        if not fs_details_source:
            fs_util_ceph1.create_fs(source_clients[0], source_fs)
            fs_util_ceph1.wait_for_mds_process(source_clients[0], source_fs)
        fs_details_target = fs_util_ceph2.get_fs_info(target_clients[0], target_fs)
        if not fs_details_target:
            fs_util_ceph2.create_fs(target_clients[0], target_fs)
            fs_util_ceph2.wait_for_mds_process(target_clients[0], target_fs)

        # ---- NFS cluster setup ----
        nfs_servers = ceph_cluster_dict.get("ceph1").get_ceph_objects("nfs")
        if not nfs_servers:
            log.error("No NFS server nodes found in ceph1 cluster.")
            return 1
        nfs_server = nfs_servers[0].node.hostname

        log.info("Enable NFS module on source cluster")
        source_clients[0].exec_command(sudo=True, cmd="ceph mgr module enable nfs")

        try:
            source_clients[0].exec_command(
                sudo=True,
                cmd=f"ceph nfs cluster info {nfs_name}",
                check_ec=True,
            )
            log.info("Stale NFS cluster '%s' found, removing it first", nfs_name)
            fs_util_ceph1.remove_nfs_cluster(source_clients[0], nfs_name, validate=True)
            time.sleep(15)
        except CommandFailed:
            log.info("No stale NFS cluster '%s' found, proceeding", nfs_name)

        log.info("Creating NFS cluster '%s' on '%s'", nfs_name, nfs_server)
        fs_util_v1_ceph1.create_nfs(
            source_clients[0],
            nfs_cluster_name=nfs_name,
            nfs_server_name=nfs_server,
        )
        time.sleep(30)
        if wait_for_process(
            client=source_clients[0], process_name=nfs_name, ispresent=True
        ):
            log.info("NFS cluster created successfully on source cluster")
        else:
            log.error("NFS cluster process not found after creation")
            return 1
        nfs_created = True

        log.info("Deploy CephFS Mirroring Configuration")
        fs_mirroring_utils.deploy_cephfs_mirroring(
            source_fs,
            source_clients[0],
            target_fs,
            target_clients[0],
            target_user,
            target_site_name,
        )

        # ---- Read test parameters from config ----
        thread_counts = config.get("thread_counts", DEFAULT_THREAD_COUNTS)
        num_files = config.get("num_files", DEFAULT_NUM_FILES)
        file_size_mb = config.get("file_size_mb", DEFAULT_FILE_SIZE_MB)
        subvol_size = config.get("subvol_size", DEFAULT_SUBVOL_SIZE_BYTES)
        file_gen_timeout = config.get("file_gen_timeout", 7200)
        sync_poll_tries = config.get("sync_poll_tries", 240)
        sync_poll_delay = config.get("sync_poll_delay", 15)

        est_total_mb = num_files * file_size_mb

        log.info(
            "Test parameters: thread_counts=%s, num_files=%d, "
            "file_size=%dMB, est_total=%dMB per subvolume",
            thread_counts,
            num_files,
            file_size_mb,
            est_total_mb,
        )

        # Prepare CSV result file
        ceph_version_out, _ = source_clients[0].exec_command(
            sudo=True, cmd="ceph version"
        )
        ceph_version_out = ceph_version_out.strip()
        mirror_node_hostname = cephfs_mirror_node[0].node.hostname
        result_file = config.get("result_file", "large_file_sync_perf.csv")
        csv_file = f"{log_dir}/{result_file}"
        _initialize_csv(csv_file, ceph_version_out, mirror_node_hostname, config)

        # ---- Create subvolume group (shared) ----
        subvolumegroup_list = [
            {"vol_name": source_fs, "group_name": subvol_group},
        ]
        fs_util_ceph1.create_subvolumegroup(source_clients[0], **subvolumegroup_list[0])

        # ---- Create 3 subvolumes once (one per mount type) ----
        for mtype in MOUNT_TYPES:
            sv_name = f"subvol_lperf_{mtype}"
            log.info("Creating subvolume %s", sv_name)
            fs_util_ceph1.create_subvolume(
                source_clients[0],
                vol_name=source_fs,
                subvol_name=sv_name,
                group_name=subvol_group,
                size=subvol_size,
            )
            created_subvols[mtype] = sv_name

            subvol_path_raw, _ = source_clients[0].exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume getpath {source_fs} {sv_name} {subvol_group}",
            )
            subvol_path_raw = subvol_path_raw.strip()
            idx = subvol_path_raw.find(f"{sv_name}/")
            if idx != -1:
                subvol_path = subvol_path_raw[: idx + len(f"{sv_name}/")]
            else:
                subvol_path = subvol_path_raw
            subvol_paths[mtype] = subvol_path
            log.info("Subvolume %s path: %s", sv_name, subvol_path)

        # ---- Add paths for mirroring once ----
        for mtype in MOUNT_TYPES:
            fs_mirroring_utils.add_path_for_mirroring(
                source_clients[0], source_fs, subvol_paths[mtype]
            )
            mirroring_paths.append(subvol_paths[mtype])

        # ---- Mount each subvolume once ----
        rand_suffix = "".join(
            secrets.choice(string.ascii_lowercase + string.digits) for _ in range(10)
        )
        mon_node_ips = fs_util_ceph1.get_mon_node_ips()

        for mtype in MOUNT_TYPES:
            mounting_dir = f"/mnt/cephfs_{mtype}_{rand_suffix}/"
            source_clients[0].exec_command(sudo=True, cmd=f"mkdir -p {mounting_dir}")

            if mtype == "kernel":
                fs_util_ceph1.kernel_mount(
                    [source_clients[0]],
                    mounting_dir,
                    ",".join(mon_node_ips),
                    extra_params=f",fs={source_fs}",
                )
            elif mtype == "fuse":
                fs_util_ceph1.fuse_mount(
                    [source_clients[0]],
                    mounting_dir,
                    extra_params=f" --client_fs {source_fs}",
                )
            elif mtype == "nfs":
                nfs_export_name = (
                    f"/export_lperf_nfs_"
                    f"{''.join(secrets.choice(string.digits) for _ in range(5))}"
                )
                fs_util_v1_ceph1.create_nfs_export(
                    source_clients[0],
                    nfs_name,
                    nfs_export_name,
                    source_fs,
                    path=subvol_paths[mtype],
                )
                log.info(
                    "NFS export %s created, waiting for propagation", nfs_export_name
                )
                time.sleep(15)
                rc = fs_util_ceph1.cephfs_nfs_mount(
                    source_clients[0],
                    nfs_server,
                    nfs_export_name,
                    mounting_dir,
                )
                if not rc:
                    log.error("NFS mount failed")
                    return 1

            mount_dirs[mtype] = mounting_dir
            log.info("Mounted %s at %s", mtype, mounting_dir)

        # ---- Resolve mirror daemon ASOK details once ----
        fsid = fs_mirroring_utils.get_fsid(cephfs_mirror_node[0])
        daemon_name = fs_mirroring_utils.get_daemon_name(source_clients[0])
        asok_file = fs_mirroring_utils.get_asok_file(
            cephfs_mirror_node, fsid, daemon_name
        )
        filesystem_id = fs_mirroring_utils.get_filesystem_id_by_name(
            source_clients[0], source_fs
        )
        peer_uuid = fs_mirroring_utils.get_peer_uuid_by_name(
            source_clients[0], source_fs
        )

        results = []
        baselines = {}

        # ================================================================
        # Per-thread-count iteration loop
        # ================================================================
        for thread_count in thread_counts:
            log.info(
                "=" * 70 + f"\n  ITERATION: thread_count={thread_count}\n" + "=" * 70
            )

            # ---- 1. Tune cephfs_mirror_max_datasync_threads ----
            log.info("Setting cephfs_mirror_max_datasync_threads = %d", thread_count)
            source_clients[0].exec_command(
                sudo=True,
                cmd=(
                    f"ceph config set client.cephfs-mirror "
                    f"cephfs_mirror_max_datasync_threads {thread_count}"
                ),
            )
            log.info("Restarting cephfs-mirror daemon for config change")
            source_clients[0].exec_command(
                sudo=True, cmd="ceph orch restart cephfs-mirror"
            )
            time.sleep(30)

            asok_file = fs_mirroring_utils.get_asok_file(
                cephfs_mirror_node, fsid, daemon_name
            )

            # ---- 2. Create data directories for this iteration ----
            if not _is_nfs_mount_alive(source_clients[0], mount_dirs["nfs"]):
                log.warning(
                    "NFS mount stale before iteration t%d, remounting", thread_count
                )
                _remount_nfs(
                    source_clients[0],
                    mount_dirs["nfs"],
                    nfs_server,
                    nfs_export_name,
                    fs_util_ceph1,
                )

            io_dir_paths = {}
            for mtype in MOUNT_TYPES:
                if mtype == "nfs":
                    io_dir = f"{mount_dirs[mtype]}data_t{thread_count}"
                else:
                    io_dir = (
                        f"{mount_dirs[mtype]}"
                        f"{subvol_paths[mtype].lstrip('/')}data_t{thread_count}"
                    )
                source_clients[0].exec_command(sudo=True, cmd=f"mkdir -p {io_dir}")
                io_dir_paths[mtype] = io_dir

            # ---- 3. Generate large files sequentially per mount type ----
            log.info(
                "Generating %d x %dMB files sequentially on kernel/fuse/nfs",
                num_files,
                file_size_mb,
            )
            gen_start = time.time()
            for mtype in MOUNT_TYPES:
                log.info("Generating files on %s...", mtype)
                _generate_large_files_on_client(
                    source_clients[0],
                    io_dir_paths[mtype],
                    num_files,
                    file_size_mb,
                    file_gen_timeout,
                )
                log.info("File generation completed for %s", mtype)

            gen_duration = time.time() - gen_start
            log.info("All file generation completed in %.1f seconds", gen_duration)

            # Measure actual data size per mount type
            actual_mb = {}
            for mtype in MOUNT_TYPES:
                du_out, _ = source_clients[0].exec_command(
                    sudo=True, cmd=f"du -sm {io_dir_paths[mtype]}"
                )
                actual_mb[mtype] = float(du_out.strip().split()[0])
                log.info("Data size on %s: %.1f MB", mtype, actual_mb[mtype])

            # ---- 4. Create snapshots on all 3 subvolumes ----
            iter_snapshots = {}
            for mtype in MOUNT_TYPES:
                snap_name = f"snap_lperf_t{thread_count}_{mtype[0]}"
                log.info("Creating snapshot %s on %s", snap_name, mtype)
                fs_util_ceph1.create_snapshot(
                    client=source_clients[0],
                    vol_name=source_fs,
                    subvol_name=created_subvols[mtype],
                    snap_name=snap_name,
                    validate=True,
                    group_name=subvol_group,
                )
                iter_snapshots[mtype] = snap_name

            # ---- 5. Wait for sync and measure duration per mount type ----
            for mtype in MOUNT_TYPES:
                snap_name = iter_snapshots[mtype]
                log.info("Waiting for snapshot sync: %s (%s)", snap_name, mtype)
                sync_start = time.time()

                result_snap = _poll_snapshot_sync(
                    fs_mirroring_utils,
                    cephfs_mirror_node,
                    source_fs,
                    snap_name,
                    fsid,
                    asok_file,
                    filesystem_id,
                    peer_uuid,
                    max_tries=sync_poll_tries,
                    delay=sync_poll_delay,
                )
                wall_clock_sec = time.time() - sync_start

                if not result_snap:
                    log.error(
                        "Snapshot %s (%s) did not sync within timeout "
                        "for thread_count=%d",
                        snap_name,
                        mtype,
                        thread_count,
                    )
                    return 1

                raw_duration = result_snap.get("sync_duration")
                if raw_duration is not None:
                    daemon_sync_duration = float(raw_duration)
                else:
                    daemon_sync_duration = None
                log.info(
                    "Snapshot %s synced: daemon_duration=%s, "
                    "wall_clock=%.1fs, snaps_synced=%s",
                    snap_name,
                    (
                        f"{daemon_sync_duration:.1f}s"
                        if daemon_sync_duration is not None
                        else "None"
                    ),
                    wall_clock_sec,
                    result_snap["snaps_synced"],
                )

                # ---- 6. Compute metrics ----
                data_mb = actual_mb[mtype]
                if daemon_sync_duration is not None and daemon_sync_duration > 0:
                    files_per_sec = num_files / daemon_sync_duration
                    mb_per_sec = data_mb / daemon_sync_duration
                elif daemon_sync_duration == 0:
                    files_per_sec = 0
                    mb_per_sec = 0
                else:
                    files_per_sec = None
                    mb_per_sec = None

                if daemon_sync_duration is not None:
                    if mtype not in baselines:
                        baselines[mtype] = daemon_sync_duration
                    speedup = (
                        baselines[mtype] / daemon_sync_duration
                        if daemon_sync_duration > 0
                        else 0
                    )
                else:
                    speedup = None

                result_entry = {
                    "thread_count": thread_count,
                    "mount_type": mtype,
                    "num_files": num_files,
                    "data_size_mb": data_mb,
                    "sync_duration_sec": (
                        round(daemon_sync_duration, 1)
                        if daemon_sync_duration is not None
                        else None
                    ),
                    "sync_duration_min": (
                        round(daemon_sync_duration / 60, 1)
                        if daemon_sync_duration is not None
                        else None
                    ),
                    "files_per_sec": (
                        round(files_per_sec, 1) if files_per_sec is not None else None
                    ),
                    "mb_per_sec": (
                        round(mb_per_sec, 1) if mb_per_sec is not None else None
                    ),
                    "speedup": round(speedup, 1) if speedup is not None else None,
                    "wall_clock_sec": round(wall_clock_sec, 1),
                    "sync_time_stamp": result_snap["sync_time_stamp"],
                    "snaps_synced": result_snap["snaps_synced"],
                }
                results.append(result_entry)

                dur_str = (
                    f"{daemon_sync_duration:.1f}s ({daemon_sync_duration/60:.1f}min)"
                    if daemon_sync_duration is not None
                    else "None"
                )
                fps_str = (
                    f"{files_per_sec:.1f}" if files_per_sec is not None else "None"
                )
                mbs_str = f"{mb_per_sec:.1f}" if mb_per_sec is not None else "None"
                spd_str = f"{speedup:.1f}x" if speedup is not None else "None"
                log.info(
                    "RESULT | threads=%d | mount=%s | duration=%s | "
                    "%s files/sec | %s MB/sec | %s speedup",
                    thread_count,
                    mtype,
                    dur_str,
                    fps_str,
                    mbs_str,
                    spd_str,
                )

                _append_csv_row(csv_file, result_entry)

            # ---- 7. Cleanup this iteration (snapshots + data only) ----
            log.info("Cleaning up iteration for thread_count=%d", thread_count)
            for mtype in MOUNT_TYPES:
                fs_util_ceph1.remove_snapshot(
                    client=source_clients[0],
                    vol_name=source_fs,
                    subvol_name=created_subvols[mtype],
                    snap_name=iter_snapshots[mtype],
                    validate=True,
                    group_name=subvol_group,
                    force=True,
                )

            for mtype in MOUNT_TYPES:
                try:
                    log.info("Deleting generated files under %s", io_dir_paths[mtype])
                    source_clients[0].exec_command(
                        sudo=True,
                        cmd=f"rm -rf {io_dir_paths[mtype]}",
                        long_running=True,
                        timeout=600,
                    )
                except Exception as e:
                    log.warning("Failed to delete data in %s: %s", mtype, e)
                    if mtype == "nfs":
                        log.info("NFS data deletion failed, remounting NFS")
                        _remount_nfs(
                            source_clients[0],
                            mount_dirs["nfs"],
                            nfs_server,
                            nfs_export_name,
                            fs_util_ceph1,
                        )

        # ---- Final summary ----
        log.info("=" * 70)
        log.info("PERFORMANCE SUMMARY")
        log.info("=" * 70)
        log.info(
            "%-10s %-8s %-14s %-12s %-12s %-10s",
            "Threads",
            "Mount",
            "Duration(s)",
            "Files/sec",
            "MB/sec",
            "Speedup",
        )
        for r in results:
            log.info(
                "%-10d %-8s %-14s %-12s %-12s %-10s",
                r["thread_count"],
                r["mount_type"],
                _fmt(r["sync_duration_sec"]),
                _fmt(r["files_per_sec"]),
                _fmt(r["mb_per_sec"]),
                _fmt(r["speedup"], "x"),
            )
        log.info("CSV results written to %s", csv_file)

        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1

    finally:
        if (config or {}).get("cleanup", True) and source_clients:
            log.info("Cleanup: resetting cephfs_mirror_max_datasync_threads")
            try:
                source_clients[0].exec_command(
                    sudo=True,
                    cmd=(
                        "ceph config rm client.cephfs-mirror "
                        "cephfs_mirror_max_datasync_threads"
                    ),
                )
            except Exception as e:
                log.warning("Failed to reset thread config: %s", e)

            log.info("Cleanup: restarting cephfs-mirror daemon")
            try:
                source_clients[0].exec_command(
                    sudo=True, cmd="ceph orch restart cephfs-mirror"
                )
                time.sleep(30)
            except Exception as e:
                log.warning("Failed to restart mirror daemon: %s", e)

            log.info("Cleanup: unmounting")
            for mtype, mdir in mount_dirs.items():
                try:
                    source_clients[0].exec_command(
                        sudo=True, cmd=f"umount -l {mdir} 2>/dev/null || true"
                    )
                    source_clients[0].exec_command(sudo=True, cmd=f"rm -rf {mdir}")
                except Exception:
                    pass

            if nfs_export_name:
                log.info("Cleanup: removing NFS export %s", nfs_export_name)
                try:
                    fs_util_ceph1.remove_nfs_export(
                        source_clients[0], nfs_name, nfs_export_name, validate=True
                    )
                except Exception as e:
                    log.warning("Failed to remove NFS export: %s", e)

            if nfs_created:
                log.info("Cleanup: removing NFS cluster")
                try:
                    fs_util_ceph1.remove_nfs_cluster(
                        source_clients[0], nfs_name, validate=True
                    )
                except Exception as e:
                    log.warning("Failed to remove NFS cluster: %s", e)

            if fs_mirroring_utils and mirroring_paths and source_fs:
                log.info("Cleanup: removing mirroring paths")
                for mpath in mirroring_paths:
                    try:
                        fs_mirroring_utils.remove_path_from_mirroring(
                            source_clients[0], source_fs, mpath
                        )
                    except Exception as e:
                        log.warning("Failed to remove mirroring path %s: %s", mpath, e)

            if peer_uuid and fs_mirroring_utils:
                log.info("Cleanup: destroying CephFS Mirroring setup")
                try:
                    peer_uuid = fs_mirroring_utils.get_peer_uuid_by_name(
                        source_clients[0], source_fs
                    )
                    fs_mirroring_utils.destroy_cephfs_mirroring(
                        source_fs,
                        source_clients[0],
                        target_fs,
                        target_clients[0],
                        target_user,
                        peer_uuid,
                    )
                except Exception as e:
                    log.warning("Failed to destroy mirroring: %s", e)

            if fs_util_ceph1 and created_subvols:
                log.info("Cleanup: removing subvolumes")
                for sv in created_subvols.values():
                    try:
                        fs_util_ceph1.remove_subvolume(
                            source_clients[0],
                            vol_name=source_fs,
                            subvol_name=sv,
                            group_name=subvol_group,
                        )
                    except Exception as e:
                        log.warning("Failed to remove subvolume %s: %s", sv, e)

            if fs_util_ceph1 and subvolumegroup_list:
                log.info("Cleanup: removing subvolume group")
                for svg in subvolumegroup_list:
                    try:
                        fs_util_ceph1.remove_subvolumegroup(source_clients[0], **svg)
                    except Exception as e:
                        log.warning("Failed to remove subvolume group: %s", e)


def _fmt(val, suffix=""):
    """Format a float value for summary display, returning 'None' for missing values."""
    return f"{val:.1f}{suffix}" if val is not None else "None"


def _is_nfs_mount_alive(client, mount_dir):
    """Quick liveness check — stat the mountpoint root with a short timeout."""
    try:
        client.exec_command(
            sudo=True,
            cmd=f"timeout 10 stat {mount_dir}",
            check_ec=True,
        )
        return True
    except Exception:
        return False


def _remount_nfs(client, mount_dir, nfs_server, nfs_export, fs_util):
    """Force-unmount a stale NFS mount and remount it."""
    try:
        client.exec_command(sudo=True, cmd=f"umount -l {mount_dir} 2>/dev/null || true")
        time.sleep(5)
        rc = fs_util.cephfs_nfs_mount(
            client,
            nfs_server,
            nfs_export,
            mount_dir,
        )
        if rc:
            log.info("NFS remount successful at %s", mount_dir)
        else:
            log.error("NFS remount failed at %s", mount_dir)
    except Exception as e:
        log.error("NFS remount error: %s", e)


def _generate_large_files_on_client(client, io_dir, num_files, file_size_mb, timeout):
    """Generate large files using dd on the remote client, failing on any error."""
    # set -e aborts the loop on any dd failure so errors are not silently swallowed.
    cmd = (
        f"set -e; "
        f"for i in $(seq 1 {num_files}); do "
        f"dd if=/dev/urandom of='{io_dir}/large_file_$i' "
        f"bs=1M count={file_size_mb}; "
        f"done"
    )
    client.exec_command(sudo=True, timeout=timeout, cmd=cmd, long_running=True)


def _poll_snapshot_sync(
    fs_mirroring_utils,
    cephfs_mirror_node,
    source_fs,
    snapshot_name,
    fsid,
    asok_file,
    filesystem_id,
    peer_uuid,
    max_tries=240,
    delay=15,
):
    """
    Poll the mirror daemon ASOK until the snapshot appears as synced.

    Returns the result immediately once synced. If the daemon reports
    sync_duration as None, the result is returned as-is and the caller
    uses wall-clock time instead.
    """
    for attempt in range(1, max_tries + 1):
        try:
            result = fs_mirroring_utils.validate_snapshot_sync_status(
                cephfs_mirror_node,
                source_fs,
                snapshot_name,
                fsid,
                asok_file,
                filesystem_id,
                peer_uuid,
            )
            if result:
                return result
        except (CommandFailed, Exception) as e:
            if attempt % 10 == 0:
                log.info(
                    "Poll attempt %d/%d for %s: %s",
                    attempt,
                    max_tries,
                    snapshot_name,
                    e,
                )
        time.sleep(delay)

    log.error(
        "Snapshot %s not synced after %d attempts (%ds total)",
        snapshot_name,
        max_tries,
        max_tries * delay,
    )
    return None


def _initialize_csv(csv_file, ceph_version, mirror_node, config):
    """Create the CSV result file with header rows."""
    with open(csv_file, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([f"Ceph Version: {ceph_version}"])
        writer.writerow([f"Mirror Daemon Node: {mirror_node}"])
        writer.writerow(["Config Tuned: cephfs_mirror_max_datasync_threads"])
        writer.writerow(
            [
                f"Files per subvolume: {config.get('num_files', DEFAULT_NUM_FILES)}, "
                f"File size: {config.get('file_size_mb', DEFAULT_FILE_SIZE_MB)} MB"
            ]
        )
        writer.writerow([])
        writer.writerow(
            [
                "Threads",
                "Mount Type",
                "Num Files",
                "Data Size (MB)",
                "Sync Duration (sec)",
                "Sync Duration (min)",
                "Files/sec",
                "MB/sec",
                "Speedup",
                "Wall Clock (sec)",
                "Sync Timestamp",
                "Snaps Synced",
            ]
        )


def _append_csv_row(csv_file, result):
    """Append one result row to the CSV file."""
    with open(csv_file, mode="a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                result["thread_count"],
                result["mount_type"],
                result["num_files"],
                result["data_size_mb"],
                result["sync_duration_sec"],
                result["sync_duration_min"],
                result["files_per_sec"],
                result["mb_per_sec"],
                result["speedup"],
                result["wall_clock_sec"],
                result["sync_time_stamp"],
                result["snaps_synced"],
            ]
        )
