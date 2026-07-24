import json
import random
import string
import time
import traceback

from ceph.parallel import parallel
from tests.cephfs.cephfs_mirroring.cephfs_mirroring_utils import CephfsMirroringUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)

SYNC_TIMEOUT = 600
POLL_INTERVAL = 15


def run(ceph_cluster, **kw):
    """
    Multi-threaded mirroring: crawler thread hangs when syncing directory-only paths.

    When a mirrored directory contains only subdirectories (no regular files), the
    crawler thread can hang indefinitely in wait_for_sync(). The crawler pushes no
    file entries to the data queue, calls finish_crawl() then wait_for_sync(). If
    all data sync threads are busy in inner loops processing other syncm objects,
    has_pending_work() returns false (empty queue + crawl finished) and the syncm
    is permanently orphaned — nobody sets m_sync_done, and the crawler blocks
    forever.

    Reproduction strategy:
        All 4 directories are added for mirroring in parallel (using ceph.parallel)
        and all 4 snapshots are created in parallel. This matches manual CLI
        behaviour where all 'ceph fs snapshot mirror add' commands complete almost
        simultaneously, so all 4 directories are present in m_directories before
        crawlers scan. With default max_concurrent_directory_syncs=3, three
        crawlers pick 3 directories (dirs-only has ~75 percent chance of being in
        the first batch). The 3 data directories generate enough I/O (20 x 100MB
        each) to keep all 6 data sync threads in inner loops, and the dirs-only
        crawl finishes in milliseconds before any thread returns.

    Scenario:
        1. Deploy CephFS mirroring between source and target clusters
        2. Create 3 data directories with 20 x 100MB files each
        3. Create 1 directory-only path (subdirectories, no files)
        4. Add all 4 directories for mirroring in parallel
        5. Take snapshots on all 4 directories in parallel
        6. Wait for data directories to complete syncing
        7. Verify whether the directory-only path also syncs or remains stuck

    Expected:
        All directories including the directory-only path should sync successfully.
        If the directory-only path remains stuck while all data directories have
        completed, the empty dataq hang is present.

    Returns:
        0 if all directories sync successfully (bug not present / fixed)
        1 if the directory-only path hangs (bug is present)
    """

    all_paths = []
    snap_name = "dironly_hang_snap1"
    kernel_mounting_dir = None
    source_clients = None
    target_clients = None
    source_fs = None
    target_fs = None
    target_user = "mirror_remote"
    target_site_name = "remote_site"
    fs_mirroring_utils = None

    try:
        config = kw.get("config")
        ceph_cluster_dict = kw.get("ceph_cluster_dict")
        test_data = kw.get("test_data")
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

        log.info("Checking pre-requisites")
        if not source_clients or not target_clients:
            log.error(
                "This test requires a minimum of 1 client node on both ceph1 and ceph2."
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

        log.info("Deploy CephFS Mirroring Configuration")
        fs_mirroring_utils.deploy_cephfs_mirroring(
            source_fs,
            source_clients[0],
            target_fs,
            target_clients[0],
            target_user,
            target_site_name,
        )

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        kernel_mounting_dir = f"/mnt/cephfs_kernel{mounting_dir}_dironly/"
        mon_node_ips = fs_util_ceph1.get_mon_node_ips()
        log.info(f"Mount source filesystem at {kernel_mounting_dir}")
        fs_util_ceph1.kernel_mount(
            [source_clients[0]],
            kernel_mounting_dir,
            ",".join(mon_node_ips),
            extra_params=f",fs={source_fs}",
        )

        test_id = "".join(random.choice(string.ascii_lowercase) for _ in range(6))
        data_paths = [f"/mirror_data_{test_id}_{i}" for i in range(3)]
        dirs_only_path = f"/mirror_dironly_{test_id}"
        all_paths = data_paths + [dirs_only_path]

        # --- Create 3 data-heavy directories (20 x 100MB each = 2GB per dir) ---
        num_files = config.get("num_files", 20)
        file_size_mb = config.get("file_size_mb", 100)
        for data_path in data_paths:
            log.info(
                f"Creating data directory: {data_path} "
                f"({num_files} files x {file_size_mb}MB)"
            )
            abs_data_path = f"{kernel_mounting_dir}{data_path.lstrip('/')}"
            source_clients[0].exec_command(sudo=True, cmd=f"mkdir -p {abs_data_path}")
            for i in range(1, num_files + 1):
                source_clients[0].exec_command(
                    sudo=True,
                    cmd=(
                        f"dd if=/dev/urandom of={abs_data_path}/file_{i} "
                        f"bs=1M count={file_size_mb} 2>/dev/null"
                    ),
                    long_running=True,
                )

        # --- Create directory-only path (subdirectories, no files) ---
        log.info(
            f"Creating dirs-only directory: {dirs_only_path} "
            "(subdirectories only, no files)"
        )
        source_clients[0].exec_command(
            sudo=True,
            cmd=(
                f"mkdir -p {kernel_mounting_dir}{dirs_only_path.lstrip('/')}/subdir_a/nested && "
                f"mkdir -p {kernel_mounting_dir}{dirs_only_path.lstrip('/')}/subdir_b && "
                f"mkdir -p {kernel_mounting_dir}{dirs_only_path.lstrip('/')}/subdir_c/deep/deeper"
            ),
        )

        # --- Add ALL 4 directories for mirroring in parallel ---
        # Running in parallel ensures all 4 paths land in m_directories before
        # any crawler picks them, matching manual CLI behaviour.
        log.info("Adding all 4 directories for mirroring in parallel")
        with parallel() as p:
            for path in all_paths:
                p.spawn(
                    fs_mirroring_utils.add_path_for_mirroring,
                    source_clients[0],
                    source_fs,
                    path,
                )

        # --- Take snapshots on ALL directories in parallel ---
        log.info(f"Creating snapshot '{snap_name}' on all directories in parallel")
        with parallel() as p:
            for path in all_paths:
                p.spawn(
                    source_clients[0].exec_command,
                    sudo=True,
                    cmd=f"mkdir {kernel_mounting_dir}{path.lstrip('/')}/.snap/{snap_name}",
                )

        # --- Poll peer status to detect the hang ---
        log.info(f"Waiting up to {SYNC_TIMEOUT}s for directories to sync...")
        start_time = time.time()
        dirs_only_synced = False
        data_dirs_synced = False

        while time.time() - start_time < SYNC_TIMEOUT:
            time.sleep(POLL_INTERVAL)
            elapsed = int(time.time() - start_time)

            peer_status = fs_mirroring_utils.get_fs_mirror_peer_status_using_asok(
                cephfs_mirror_node[0],
                source_clients[0],
                source_fs,
            )
            log.info(f"Peer status at {elapsed}s: {json.dumps(peer_status, indent=2)}")

            data_states = []
            for data_path in data_paths:
                entry = peer_status.get(data_path, {})
                state = entry.get("state", "unknown")
                data_states.append(state)

            dirs_only_entry = peer_status.get(dirs_only_path, {})
            dirs_only_state = dirs_only_entry.get("state", "unknown")

            log.info(
                f"  dirs_only ({dirs_only_path}): {dirs_only_state}, "
                f"data dirs: {data_states}"
            )

            if dirs_only_state == "idle" and dirs_only_entry.get("snaps_synced", 0) > 0:
                dirs_only_synced = True

            if all(s == "idle" for s in data_states):
                all_data_synced_count = sum(
                    1
                    for dp in data_paths
                    if peer_status.get(dp, {}).get("snaps_synced", 0) > 0
                )
                if all_data_synced_count == len(data_paths):
                    data_dirs_synced = True

            if dirs_only_synced:
                log.info(
                    f"Directory-only path synced successfully at {elapsed}s. "
                    "Empty dataq hang is NOT present (or has been fixed)."
                )
                return 0

            if data_dirs_synced and not dirs_only_synced:
                log.warning(
                    f"All data directories synced but directory-only path "
                    f"is still in state '{dirs_only_state}' at {elapsed}s. "
                    "Waiting additional time to confirm hang..."
                )
                extra_wait = 120
                time.sleep(extra_wait)
                peer_status = fs_mirroring_utils.get_fs_mirror_peer_status_using_asok(
                    cephfs_mirror_node[0],
                    source_clients[0],
                    source_fs,
                )
                dirs_only_entry = peer_status.get(dirs_only_path, {})
                dirs_only_state = dirs_only_entry.get("state", "unknown")

                if (
                    dirs_only_state == "idle"
                    and dirs_only_entry.get("snaps_synced", 0) > 0
                ):
                    log.info(
                        "Directory-only path eventually synced. "
                        "Empty dataq hang is NOT present."
                    )
                    return 0
                else:
                    log.error(
                        f"EMPTY DATAQ HANG CONFIRMED: directory-only path "
                        f"({dirs_only_path}) is stuck in state '{dirs_only_state}' "
                        f"while all data directories have completed syncing. "
                        f"The crawler thread is hung in wait_for_sync() because "
                        f"no data sync thread picked up the empty syncm object."
                    )
                    log.error(
                        f"Final peer status for {dirs_only_path}: "
                        f"{json.dumps(dirs_only_entry, indent=2)}"
                    )
                    return 1

        log.error(
            f"Timeout after {SYNC_TIMEOUT}s waiting for sync to complete. "
            f"dirs_only_synced={dirs_only_synced}, "
            f"data_dirs_synced={data_dirs_synced}"
        )
        return 1

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        if config.get("cleanup", True):
            log.info("Cleanup: removing snapshots")
            for path in all_paths:
                source_clients[0].exec_command(
                    sudo=True,
                    cmd=(
                        f"rmdir {kernel_mounting_dir}{path.lstrip('/')}/"
                        f".snap/{snap_name} 2>/dev/null || true"
                    ),
                )

            log.info("Cleanup: removing paths from mirroring")
            for path in all_paths:
                fs_mirroring_utils.remove_path_from_mirroring(
                    source_clients[0], source_fs, path
                )

            log.info("Cleanup: restart mirror daemon to clear any stuck state")
            source_clients[0].exec_command(
                sudo=True, cmd="ceph orch restart cephfs-mirror"
            )
            time.sleep(30)

            log.info("Cleanup: removing test directories")
            for path in all_paths:
                source_clients[0].exec_command(
                    sudo=True,
                    cmd=f"rm -rf {kernel_mounting_dir}{path.lstrip('/')}",
                )

            if kernel_mounting_dir:
                log.info("Cleanup: unmounting")
                source_clients[0].exec_command(
                    sudo=True, cmd=f"umount -l {kernel_mounting_dir}"
                )
                source_clients[0].exec_command(
                    sudo=True, cmd=f"rm -rf {kernel_mounting_dir}"
                )

            log.info("Cleanup: destroying mirroring setup")
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
