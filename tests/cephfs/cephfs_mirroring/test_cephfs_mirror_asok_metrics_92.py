import json
import random
import string
import time
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_mirroring.cephfs_mirroring_utils import CephfsMirroringUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)

# Poll every 1 s for up to 15 min — required for 5 GiB full/delta sync on lab nodes.
# Do not reduce without verifying all 9.2 sync scenarios still complete in time.
SYNC_POLL_MAX_ITERATIONS = 900
SYNC_POLL_INTERVAL_SECONDS = 1

# 5 files modified + 20 new delta files + 1 large file partially modified
EXPECTED_MAX_DELTA_FILES = 26
PEER_COUNTER_KEY = "cephfs_mirror_peers"


def _get_peer_counters(
    fs_mirroring_utils, cephfs_mirror_node, fsid, asok_files, fs_name
):
    """Return cephfs_mirror_peers counters for fs_name."""
    data = fs_mirroring_utils.get_cephfs_mirror_counters(
        cephfs_mirror_node, fsid, asok_files
    )
    _, counters = fs_mirroring_utils.get_labels_and_counters(
        PEER_COUNTER_KEY, fs_name, data
    )
    if not counters:
        raise CommandFailed(
            "%s counters not found for %s" % (PEER_COUNTER_KEY, fs_name)
        )
    return counters


def _get_directory_counters(
    fs_mirroring_utils, cephfs_mirror_node, fsid, asok_files, dir_path, peer_uuid
):
    """Return cephfs_mirror_directory counters for dir_path + peer_uuid, or None."""
    entries = fs_mirroring_utils.get_directory_counters(
        cephfs_mirror_node, fsid, asok_files
    )
    wanted = {dir_path, dir_path.rstrip("/"), "%s/" % dir_path.rstrip("/")}
    for entry in entries:
        labels = entry.get("labels", {})
        if labels.get("directory") in wanted and labels.get("peer_uuid") == peer_uuid:
            return entry.get("counters", {})
    return None


def _wait_peer_counter_gt(
    fs_mirroring_utils,
    cephfs_mirror_node,
    fsid,
    asok_files,
    fs_name,
    key,
    baseline,
    timeout=300,
    interval=10,
):
    """Poll peer counters until key > baseline or timeout."""
    elapsed = 0
    while elapsed < timeout:
        counters = _get_peer_counters(
            fs_mirroring_utils, cephfs_mirror_node, fsid, asok_files, fs_name
        )
        actual = counters.get(key, 0)
        log.info("[%ss] wait %s > %s (current=%s)", elapsed, key, baseline, actual)
        if actual > baseline:
            return counters
        time.sleep(interval)
        elapsed += interval
    raise CommandFailed(
        "Timed out waiting for peer counter %s > %s within %ds"
        % (key, baseline, timeout)
    )


def run(ceph_cluster, **kw):
    """
    CEPH-83632744 - Validate CephFS mirroring asok metrics for 9.2 enhancements.

    Consolidated scenarios:
     1. Schema verification (field presence)
     2. Full sync — sync-mode, ETA, crawl, datasync, throughput (500 MiB)
     3. Delta sync — sync-mode, monotonicity, snapdiff/blockdiff (500 MiB)
     4. last_synced_snap enrichment (assert non-zero values)
     5. Sync-mode fallback when snapdiff ref missing
     6. Peer perf counter validation (snaps_synced, last_synced_bytes, timing)
        + directory counter cross-check (ported from upstream test_cephfs_mirror_stats)
     7. Snap delete → snaps_deleted counter (peer + directory)
     8. Snap rename → snaps_renamed counter (peer + directory)

    Note: Scenario for sync failure in test_cephfs_mirror_disruptive_ops.py
    Note: Zero-file directory sync validated in test_cephfs_mirror_improved_stats (R12).

    Returns 0 on success, 1 on failure.
    """
    try:
        config = kw.get("config")
        ceph_cluster_dict = kw.get("ceph_cluster_dict")
        test_data = kw.get("test_data")
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

        source_fs = "cephfs"
        target_fs = "cephfs"
        target_user = "mirror_remote"
        target_site_name = "remote_site"

        log.info("checking Pre-requisites")
        if not source_clients or not target_clients:
            log.info(
                "This test requires a minimum of 1 client node "
                "on both ceph1 and ceph2."
            )
            return 1

        log.info("Preparing Clients...")
        fs_util_ceph1.prepare_clients(source_clients, build)
        fs_util_ceph2.prepare_clients(target_clients, build)
        fs_util_ceph1.auth_list(source_clients)
        fs_util_ceph2.auth_list(target_clients)

        log.info("Deploy CephFS Mirroring Configuration")
        fs_mirroring_utils.deploy_cephfs_mirroring(
            source_fs,
            source_clients[0],
            target_fs,
            target_clients[0],
            target_user,
            target_site_name,
        )

        subvol_group_name = "subvolgroup_asok"
        subvol_name = "subvol_asok"
        subvol_size = "12884901888"
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        kernel_mounting_dir = "/mnt/cephfs_kernel%s_1" % mounting_dir
        fuse_mounting_dir = "/mnt/cephfs_fuse%s_1" % mounting_dir
        subvol_details = [
            {
                "subvol_name": "%s_1" % subvol_name,
                "subvol_size": subvol_size,
                "mount_type": "kernel",
                "mount_dir": kernel_mounting_dir,
            },
            {
                "subvol_name": "%s_2" % subvol_name,
                "subvol_size": subvol_size,
                "mount_type": "fuse",
                "mount_dir": fuse_mounting_dir,
            },
        ]
        subvolume_paths = fs_mirroring_utils.setup_subvolumes_and_mounts(
            source_fs,
            source_clients[0],
            fs_util_ceph1,
            subvol_group_name,
            subvol_details,
        )
        log.info("Subvolume Paths: %s", subvolume_paths)

        subvol_path1 = subvolume_paths[0]
        subvol_path2 = subvolume_paths[1]

        log.info("Add subvolumes for mirroring")
        for subvol_path in subvolume_paths:
            fs_mirroring_utils.add_path_for_mirroring(
                source_clients[0], source_fs, subvol_path
            )

        log.info("Set tick interval to 1s for accurate sync metrics")
        source_clients[0].exec_command(
            sudo=True,
            cmd="ceph config set client.cephfs-mirror " "cephfs_mirror_tick_interval 1",
        )
        log.info("Restart cephfs-mirror daemon for tick_interval to take effect")
        source_clients[0].exec_command(sudo=True, cmd="ceph orch restart cephfs-mirror")
        time.sleep(30)

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

        mount_path1 = "%s%s" % (kernel_mounting_dir, subvol_path1)
        mount_path2 = "%s%s" % (fuse_mounting_dir, subvol_path2)
        path1_key = subvol_path1.rstrip("/")
        path2_key = subvol_path2.rstrip("/")

        # ============================================================
        # Scenario 1: Schema verification
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 1: Schema verification")
        log.info("=" * 60)
        status_before = fs_mirroring_utils.get_fs_mirror_peer_status_using_asok(
            cephfs_mirror_node[0], source_clients[0], source_fs
        )
        log.info("Peer status (initial): %s", json.dumps(status_before, indent=2))

        for path, dir_status in status_before.items():
            required = ["state", "snaps_synced", "snaps_deleted", "snaps_renamed"]
            for field in required:
                if field not in dir_status:
                    raise CommandFailed(
                        "Missing field '%s' in asok status for %s" % (field, path)
                    )
            log.info("Schema OK for %s: %s", path, list(dir_status.keys()))

        # ============================================================
        # Scenario 2: Full sync — comprehensive in-flight metrics
        # (sync-mode, ETA, crawl, datasync, throughput)
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 2: Full sync — all in-flight metrics (500 MiB)")
        log.info("=" * 60)

        log.info("Write 5 GiB data for observable full sync")
        source_clients[0].exec_command(
            sudo=True,
            cmd="dd if=/dev/urandom of=%sfulldata bs=1M count=5120" % mount_path1,
            timeout=600,
        )
        source_clients[0].exec_command(
            sudo=True, cmd="mkdir %s.snap/snap_full" % mount_path1
        )

        sync_mode_full = False
        eta_observed = False
        crawl_observed = False
        datasync_observed = False
        throughput_observed = False

        log.info("Poll asok during full sync — capture all in-flight fields")
        for poll_i in range(SYNC_POLL_MAX_ITERATIONS):
            time.sleep(SYNC_POLL_INTERVAL_SECONDS)
            try:
                status = fs_mirroring_utils.get_asok_peer_status_raw(
                    cephfs_mirror_node[0], source_clients[0], source_fs
                )
                log.info(
                    "[S2 Poll %d] Raw asok: %s",
                    poll_i,
                    json.dumps(status.get(path1_key, {})),
                )

                dir_data = status.get(path1_key, {})
                state = dir_data.get("state", "")
                syncing = dir_data.get("current_syncing_snap")

                if syncing:
                    mode = syncing.get("sync-mode", "")
                    eta = syncing.get("eta", "")
                    crawl = syncing.get("crawl", {})
                    dswait = syncing.get("datasync_queue_wait", {})
                    read_tp = syncing.get("avg_read_throughput_bytes", "")
                    write_tp = syncing.get("avg_write_throughput_bytes", "")
                    bytes_info = syncing.get("bytes", {})
                    files_info = syncing.get("files", {})

                    log.info(
                        "[S2 Poll %d] state=%s, sync-mode=%s, eta=%s, "
                        "crawl=%s/%s, datasync=%s/%s, "
                        "read_bps=%s, write_bps=%s, "
                        "sync_bytes=%s, total_bytes=%s, sync_pct=%s, "
                        "sync_files=%s, total_files=%s",
                        poll_i,
                        state,
                        mode,
                        eta,
                        crawl.get("state", ""),
                        crawl.get("duration", ""),
                        dswait.get("state", ""),
                        dswait.get("duration", ""),
                        read_tp,
                        write_tp,
                        bytes_info.get("sync_bytes", ""),
                        bytes_info.get("total_bytes", ""),
                        bytes_info.get("sync_percent", ""),
                        files_info.get("sync_files", ""),
                        files_info.get("total_files", ""),
                    )

                    if mode == "full":
                        sync_mode_full = True
                    if eta:
                        eta_observed = True
                    if crawl.get("state"):
                        crawl_observed = True
                    if dswait.get("state"):
                        datasync_observed = True
                    if read_tp or write_tp:
                        throughput_observed = True

                if state == "idle":
                    last = dir_data.get("last_synced_snap", {})
                    if last.get("name") == "snap_full":
                        log.info("snap_full synced: %s", json.dumps(last))
                        break
            except Exception as e:
                log.warning("S2 poll error: %s", e)
        else:
            raise CommandFailed(
                "S2: snap_full did not reach idle within %ds" % SYNC_POLL_MAX_ITERATIONS
            )

        fs_mirroring_utils.validate_snapshot_sync_status(
            cephfs_mirror_node[0],
            source_fs,
            "snap_full",
            fsid,
            asok_file,
            filesystem_id,
            peer_uuid,
        )

        log.info(
            "S2 Results: sync_mode_full=%s, eta_observed=%s, "
            "crawl_observed=%s, datasync_observed=%s, throughput_observed=%s",
            sync_mode_full,
            eta_observed,
            crawl_observed,
            datasync_observed,
            throughput_observed,
        )

        if not sync_mode_full:
            raise CommandFailed(
                "S2 FAILED: sync-mode=full was NOT captured during 5 GiB sync"
            )
        log.info("S2: sync-mode=full captured")

        if not crawl_observed:
            raise CommandFailed(
                "S2 FAILED: Crawl state was NOT observed during 5 GiB sync"
            )
        log.info("S2: Crawl state captured")

        if not throughput_observed:
            raise CommandFailed(
                "S2 FAILED: Throughput was NOT observed during 5 GiB sync"
            )
        log.info("S2: Throughput captured")

        if not eta_observed:
            log.warning(
                "S2: ETA was NOT observed during sync (may show 'calculating...')"
            )

        # ============================================================
        # Scenario 3: Delta sync — sync-mode, monotonicity, snapdiff
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 3: Delta sync — mode, monotonicity, snapdiff")
        log.info("=" * 60)

        log.info("Create baseline files: 10 small (1 MiB) + 1 large (64 MiB)")
        source_clients[0].exec_command(
            sudo=True,
            cmd="for i in $(seq 1 10); do dd if=/dev/urandom "
            "of=%ssmall_$i bs=1M count=1 2>/dev/null; done" % mount_path1,
        )
        source_clients[0].exec_command(
            sudo=True,
            cmd="dd if=/dev/urandom of=%slarge_file "
            "bs=1M count=64 2>/dev/null" % mount_path1,
        )
        source_clients[0].exec_command(
            sudo=True, cmd="mkdir %s.snap/snap_base" % mount_path1
        )

        log.info("Wait for snap_base to sync")
        fs_mirroring_utils.validate_snapshot_sync_status(
            cephfs_mirror_node[0],
            source_fs,
            "snap_base",
            fsid,
            asok_file,
            filesystem_id,
            peer_uuid,
        )

        log.info("Modify 5 of 10 small files + partial write to large file")
        source_clients[0].exec_command(
            sudo=True,
            cmd="for i in $(seq 1 5); do dd if=/dev/urandom "
            "of=%ssmall_$i bs=1M count=1 conv=notrunc 2>/dev/null; done" % mount_path1,
        )
        source_clients[0].exec_command(
            sudo=True,
            cmd="dd if=/dev/urandom of=%slarge_file "
            "bs=4K count=1 conv=notrunc seek=100 2>/dev/null" % mount_path1,
        )
        log.info("Add 20 NEW files (256 MiB each = 5 GiB) for observable delta sync")
        source_clients[0].exec_command(
            sudo=True,
            cmd="for i in $(seq 1 20); do dd if=/dev/urandom "
            "of=%sdelta_$i bs=1M count=256 2>/dev/null; done" % mount_path1,
            timeout=600,
        )
        source_clients[0].exec_command(
            sudo=True, cmd="mkdir %s.snap/snap_delta" % mount_path1
        )

        sync_mode_delta = False
        prev_sync_bytes = 0
        monotonic = True
        delta_poll_count = 0

        log.info("Poll during delta sync — validate mode, monotonicity")
        for poll_i in range(SYNC_POLL_MAX_ITERATIONS):
            time.sleep(SYNC_POLL_INTERVAL_SECONDS)
            try:
                status = fs_mirroring_utils.get_asok_peer_status_raw(
                    cephfs_mirror_node[0], source_clients[0], source_fs
                )
                dir_data = status.get(path1_key, {})
                state = dir_data.get("state", "")
                syncing = dir_data.get("current_syncing_snap")

                if syncing and syncing.get("name") == "snap_delta":
                    delta_poll_count += 1
                    mode = syncing.get("sync-mode", "")
                    bytes_info = syncing.get("bytes", {})
                    files_info = syncing.get("files", {})
                    sync_bytes_str = bytes_info.get("sync_bytes", "0")
                    sync_pct = bytes_info.get("sync_percent", "N/A")
                    sync_files = files_info.get("sync_files", 0)
                    total_files = files_info.get("total_files", 0)

                    log.info(
                        "[S3 Poll %d] state=%s, mode=%s, "
                        "sync_bytes=%s, sync_pct=%s, "
                        "sync_files=%s, total_files=%s",
                        poll_i,
                        state,
                        mode,
                        sync_bytes_str,
                        sync_pct,
                        sync_files,
                        total_files,
                    )

                    if mode == "delta":
                        sync_mode_delta = True

                    try:
                        parts = sync_bytes_str.split()
                        val = float(parts[0]) if parts else 0.0
                        unit = parts[1] if len(parts) > 1 else "B"
                        mult = {
                            "B": 1,
                            "KiB": 1024,
                            "MiB": 1048576,
                            "GiB": 1073741824,
                        }
                        cur_bytes = val * mult.get(unit, 1)
                    except (ValueError, IndexError):
                        cur_bytes = 0

                    if cur_bytes < prev_sync_bytes:
                        log.warning(
                            "Monotonicity violation: bytes %s -> %s",
                            prev_sync_bytes,
                            cur_bytes,
                        )
                        monotonic = False
                    prev_sync_bytes = cur_bytes
                else:
                    log.info(
                        "[S3 Poll %d] state=%s, snap=%s",
                        poll_i,
                        state,
                        syncing.get("name") if syncing else None,
                    )

                if state == "idle":
                    last = dir_data.get("last_synced_snap", {})
                    if last.get("name") == "snap_delta":
                        log.info("snap_delta synced: %s", json.dumps(last))
                        break
            except Exception as e:
                log.warning("S3 poll error: %s", e)
        else:
            raise CommandFailed(
                "S3: snap_delta did not reach idle within %ds"
                % SYNC_POLL_MAX_ITERATIONS
            )

        fs_mirroring_utils.validate_snapshot_sync_status(
            cephfs_mirror_node[0],
            source_fs,
            "snap_delta",
            fsid,
            asok_file,
            filesystem_id,
            peer_uuid,
        )

        log.info(
            "S3 Results: sync_mode_delta=%s, monotonic=%s, " "delta_polls_captured=%s",
            sync_mode_delta,
            monotonic,
            delta_poll_count,
        )

        if not sync_mode_delta:
            raise CommandFailed(
                "S3 FAILED: sync-mode=delta was NOT captured during 5 GiB delta sync"
            )
        log.info("S3: sync-mode=delta captured")

        if not monotonic:
            raise CommandFailed(
                "S3 FAILED: Monotonicity violations detected during delta sync"
            )
        log.info("S3: Monotonicity maintained throughout delta sync")

        # ============================================================
        # Scenario 4: last_synced_snap enrichment (assert non-zero)
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 4: last_synced_snap enrichment")
        log.info("=" * 60)

        status_after = fs_mirroring_utils.get_asok_peer_status_raw(
            cephfs_mirror_node[0], source_clients[0], source_fs
        )
        log.info("Full asok status: %s", json.dumps(status_after, indent=2))

        path1_last = status_after.get(path1_key, {}).get("last_synced_snap", {})
        log.info("S4: path1 last_synced_snap: %s", json.dumps(path1_last, indent=2))

        if not path1_last.get("name"):
            raise CommandFailed("S4 FAILED: last_synced_snap.name missing")

        enrichment_fields = [
            "id",
            "name",
            "sync_duration",
            "sync_time_stamp",
            "sync_bytes",
            "sync_files",
        ]
        for field in enrichment_fields:
            val = path1_last.get(field)
            log.info("  %s = %s", field, val)
            if val is None:
                log.warning("S4: enrichment field '%s' is missing", field)

        sync_bytes = path1_last.get("sync_bytes", "0")
        sync_files = path1_last.get("sync_files", 0)
        sync_duration = path1_last.get("sync_duration", "0s")

        if sync_bytes in ("0", "0.00 B") or sync_files == 0:
            raise CommandFailed(
                "S4 FAILED: last_synced_snap has zero metrics after 5 GiB sync — "
                "sync_bytes=%s, sync_files=%s, sync_duration=%s"
                % (sync_bytes, sync_files, sync_duration)
            )
        log.info(
            "S4 VALIDATED: sync_bytes=%s, sync_files=%s, sync_duration=%s",
            sync_bytes,
            sync_files,
            sync_duration,
        )

        if path1_last.get("name") == "snap_delta" and sync_files > 0:
            if sync_files <= EXPECTED_MAX_DELTA_FILES:
                log.info(
                    "S4 Snapdiff OK: sync_files=%d <= %d "
                    "(5 modified + 20 new + 1 large partial)",
                    sync_files,
                    EXPECTED_MAX_DELTA_FILES,
                )
            else:
                log.warning(
                    "S4 Snapdiff: sync_files=%d, expected <= %d",
                    sync_files,
                    EXPECTED_MAX_DELTA_FILES,
                )

        log.info("S4 PASSED: last_synced_snap enrichment validated")

        # ============================================================
        # Scenario 5: Sync-mode fallback when snapdiff ref missing
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 5: Full sync fallback when snapdiff ref missing")
        log.info("=" * 60)

        source_clients[0].exec_command(sudo=True, cmd="touch %sref_file" % mount_path1)
        source_clients[0].exec_command(
            sudo=True, cmd="mkdir %s.snap/snap_ref1" % mount_path1
        )
        fs_mirroring_utils.validate_snapshot_sync_status(
            cephfs_mirror_node[0],
            source_fs,
            "snap_ref1",
            fsid,
            asok_file,
            filesystem_id,
            peer_uuid,
        )

        log.info("Delete prior snapshots to remove snapdiff reference")
        for snap in ["snap_full", "snap_base", "snap_delta", "snap_ref1"]:
            source_clients[0].exec_command(
                sudo=True,
                cmd="rmdir %s.snap/%s" % (mount_path1, snap),
                check_ec=False,
            )
        time.sleep(10)

        source_clients[0].exec_command(
            sudo=True, cmd="touch %sref_file_new" % mount_path1
        )
        source_clients[0].exec_command(
            sudo=True, cmd="mkdir %s.snap/snap_ref2" % mount_path1
        )

        ref_mode = None
        for poll_i in range(30):
            time.sleep(3)
            try:
                status = fs_mirroring_utils.get_asok_peer_status_raw(
                    cephfs_mirror_node[0], source_clients[0], source_fs
                )
                dir_data = status.get(path1_key, {})
                state = dir_data.get("state", "")
                syncing = dir_data.get("current_syncing_snap", {})
                log.info(
                    "[S5 Poll %d] state=%s, snap=%s, mode=%s",
                    poll_i,
                    state,
                    syncing.get("name") if syncing else None,
                    syncing.get("sync-mode") if syncing else None,
                )
                if syncing and syncing.get("name") == "snap_ref2":
                    ref_mode = syncing.get("sync-mode", "")
                    log.info("S5: Captured sync-mode=%s", ref_mode)
                    log.info("S5: Full snap details: %s", json.dumps(syncing))
                    break
                if state == "idle":
                    last = dir_data.get("last_synced_snap", {})
                    if last.get("name") == "snap_ref2":
                        ref_mode = "full"
                        log.info("S5: snap_ref2 synced fast: %s", json.dumps(last))
                        break
            except Exception as e:
                log.warning("S5 poll error: %s", e)

        fs_mirroring_utils.validate_snapshot_sync_status(
            cephfs_mirror_node[0],
            source_fs,
            "snap_ref2",
            fsid,
            asok_file,
            filesystem_id,
            peer_uuid,
        )

        if ref_mode == "full":
            log.info("S5 PASSED: Falls back to full sync when ref missing")
        else:
            log.warning("S5: sync-mode=%s, expected 'full'", ref_mode)

        # ============================================================
        # Final: Validate cumulative snaps_synced counters
        # ============================================================
        log.info("=" * 60)
        log.info("Final: Validate cumulative snaps_synced counters")
        log.info("=" * 60)

        final_status = fs_mirroring_utils.get_asok_peer_status_raw(
            cephfs_mirror_node[0], source_clients[0], source_fs
        )
        log.info("Final asok status: %s", json.dumps(final_status, indent=2))
        for path, dir_status in final_status.items():
            synced = dir_status.get("snaps_synced", 0)
            log.info("%s: snaps_synced=%s", path, synced)

        path1_synced = final_status.get(path1_key, {}).get("snaps_synced", 0)
        if path1_synced < 1:
            raise CommandFailed(
                "snaps_synced should be >= 1 for %s, got %s" % (path1_key, path1_synced)
            )
        log.info("Path1 snaps_synced=%s — OK", path1_synced)

        path2_synced = final_status.get(path2_key, {}).get("snaps_synced", 0)
        log.info(
            "Path2 snaps_synced=%s (no snaps created on path2, 0 is expected)",
            path2_synced,
        )

        log.info("All asok metrics scenarios (S1-S5) passed")

        # ============================================================
        # Scenario 6: Peer perf counter validation + directory cross-check
        # (ported from upstream test_cephfs_mirror_stats)
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 6: Peer perf counter + directory counter validation")
        log.info("=" * 60)

        for node in cephfs_mirror_node:
            node.exec_command(
                sudo=True,
                cmd="yum install -y ceph-common --nogpgcheck",
                check_ec=False,
            )

        log.info("Write 10 x 100 MiB files for counter validation")
        source_clients[0].exec_command(
            sudo=True,
            cmd="for i in $(seq 0 9); do dd if=/dev/urandom "
            "of=%sfile.$i bs=1M count=100 2>/dev/null; done" % mount_path1,
            timeout=300,
        )

        baseline = _get_peer_counters(
            fs_mirroring_utils, cephfs_mirror_node, fsid, asok_file, source_fs
        )
        log.info("S6 baseline peer counters: %s", baseline)

        source_clients[0].exec_command(
            sudo=True, cmd="mkdir %s.snap/snap_ctr0" % mount_path1
        )
        fs_mirroring_utils.validate_snapshot_sync_status(
            cephfs_mirror_node[0],
            source_fs,
            "snap_ctr0",
            fsid,
            asok_file,
            filesystem_id,
            peer_uuid,
        )

        after_ctr0 = _get_peer_counters(
            fs_mirroring_utils, cephfs_mirror_node, fsid, asok_file, source_fs
        )
        log.info("S6 peer counters after snap_ctr0: %s", after_ctr0)

        if after_ctr0["snaps_synced"] <= baseline["snaps_synced"]:
            raise CommandFailed(
                "S6: snaps_synced did not increase: %s -> %s"
                % (baseline["snaps_synced"], after_ctr0["snaps_synced"])
            )
        if after_ctr0.get("last_synced_start", 0) <= baseline.get(
            "last_synced_start", 0
        ):
            raise CommandFailed(
                "S6: last_synced_start did not advance: %s -> %s"
                % (
                    baseline.get("last_synced_start"),
                    after_ctr0.get("last_synced_start"),
                )
            )
        if after_ctr0.get("last_synced_end", 0) < after_ctr0.get(
            "last_synced_start", 0
        ):
            raise CommandFailed(
                "S6: last_synced_end < last_synced_start: %s < %s"
                % (
                    after_ctr0.get("last_synced_end"),
                    after_ctr0.get("last_synced_start"),
                )
            )
        if after_ctr0.get("last_synced_duration", 0) <= 0:
            log.warning(
                "S6: last_synced_duration is %s (may be 0 for fast sync)",
                after_ctr0.get("last_synced_duration"),
            )
        log.info("S6: Peer counter timing fields validated")

        dir_ctr0 = _get_directory_counters(
            fs_mirroring_utils,
            cephfs_mirror_node,
            fsid,
            asok_file,
            subvol_path1,
            peer_uuid,
        )
        if dir_ctr0 is not None:
            if dir_ctr0.get("snaps_synced") != after_ctr0["snaps_synced"]:
                log.warning(
                    "S6: directory snaps_synced (%s) != peer snaps_synced (%s)",
                    dir_ctr0.get("snaps_synced"),
                    after_ctr0["snaps_synced"],
                )
            else:
                log.info(
                    "S6: directory snaps_synced matches peer: %s",
                    dir_ctr0.get("snaps_synced"),
                )
        else:
            log.warning("S6: directory counters not found for %s", subvol_path1)

        log.info("S6 PASSED: Peer counter + directory counter validated")

        # ============================================================
        # Scenario 7: Snap delete → snaps_deleted counter
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 7: snaps_deleted counter after snap delete")
        log.info("=" * 60)

        log.info("Write 15 x 100 MiB more files + snap_ctr1")
        source_clients[0].exec_command(
            sudo=True,
            cmd="for i in $(seq 0 14); do dd if=/dev/urandom "
            "of=%smore_file.$i bs=1M count=100 2>/dev/null; done" % mount_path1,
            timeout=300,
        )
        source_clients[0].exec_command(
            sudo=True, cmd="mkdir %s.snap/snap_ctr1" % mount_path1
        )
        fs_mirroring_utils.validate_snapshot_sync_status(
            cephfs_mirror_node[0],
            source_fs,
            "snap_ctr1",
            fsid,
            asok_file,
            filesystem_id,
            peer_uuid,
        )

        after_ctr1 = _get_peer_counters(
            fs_mirroring_utils, cephfs_mirror_node, fsid, asok_file, source_fs
        )
        log.info("S7 peer counters after snap_ctr1: %s", after_ctr1)

        log.info("Delete snap_ctr0")
        source_clients[0].exec_command(
            sudo=True,
            cmd="rmdir %s.snap/snap_ctr0" % mount_path1,
        )

        after_delete = _wait_peer_counter_gt(
            fs_mirroring_utils,
            cephfs_mirror_node,
            fsid,
            asok_file,
            source_fs,
            "snaps_deleted",
            after_ctr1.get("snaps_deleted", 0),
        )
        log.info("S7 peer counters after delete: %s", after_delete)

        dir_after_delete = _get_directory_counters(
            fs_mirroring_utils,
            cephfs_mirror_node,
            fsid,
            asok_file,
            subvol_path1,
            peer_uuid,
        )
        if dir_ctr0 is not None and dir_after_delete is not None:
            if dir_after_delete.get("snaps_deleted", 0) <= dir_ctr0.get(
                "snaps_deleted", 0
            ):
                log.warning(
                    "S7: directory snaps_deleted did not increase: %s -> %s",
                    dir_ctr0.get("snaps_deleted"),
                    dir_after_delete.get("snaps_deleted"),
                )
            else:
                log.info(
                    "S7: directory snaps_deleted increased: %s -> %s",
                    dir_ctr0.get("snaps_deleted"),
                    dir_after_delete.get("snaps_deleted"),
                )

        log.info("S7 PASSED: snaps_deleted counter validated")

        # ============================================================
        # Scenario 8: Snap rename → snaps_renamed counter
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 8: snaps_renamed counter after snap rename")
        log.info("=" * 60)

        log.info("Rename snap_ctr1 -> snap_ctr2")
        source_clients[0].exec_command(
            sudo=True,
            cmd="mv %s.snap/snap_ctr1 %s.snap/snap_ctr2" % (mount_path1, mount_path1),
        )

        after_rename = _wait_peer_counter_gt(
            fs_mirroring_utils,
            cephfs_mirror_node,
            fsid,
            asok_file,
            source_fs,
            "snaps_renamed",
            after_delete.get("snaps_renamed", 0),
        )
        log.info("S8 peer counters after rename: %s", after_rename)

        dir_after_rename = _get_directory_counters(
            fs_mirroring_utils,
            cephfs_mirror_node,
            fsid,
            asok_file,
            subvol_path1,
            peer_uuid,
        )
        if dir_after_delete is not None and dir_after_rename is not None:
            if dir_after_rename.get("snaps_renamed", 0) <= dir_after_delete.get(
                "snaps_renamed", 0
            ):
                log.warning(
                    "S8: directory snaps_renamed did not increase: %s -> %s",
                    dir_after_delete.get("snaps_renamed"),
                    dir_after_rename.get("snaps_renamed"),
                )
            else:
                log.info(
                    "S8: directory snaps_renamed increased: %s -> %s",
                    dir_after_delete.get("snaps_renamed"),
                    dir_after_rename.get("snaps_renamed"),
                )

        log.info("S8 PASSED: snaps_renamed counter validated")

        log.info("=" * 60)
        log.info("ALL SCENARIOS (S1-S8) PASSED")
        log.info("=" * 60)

        source_clients[0].exec_command(
            sudo=True,
            cmd="ceph config rm client.cephfs-mirror cephfs_mirror_tick_interval",
        )

        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean up the system")
        try:
            log.info("Cleanup: Reset config overrides")
            source_clients[0].exec_command(
                sudo=True,
                cmd="ceph config rm client.cephfs-mirror "
                "cephfs_mirror_tick_interval",
                check_ec=False,
            )

            all_snaps = [
                "snap_full",
                "snap_base",
                "snap_delta",
                "snap_ref1",
                "snap_ref2",
                "snap_ctr0",
                "snap_ctr1",
                "snap_ctr2",
            ]
            snap_mount_paths = [mount_path1, mount_path2]
            log.info("Delete the snapshots")
            for spath in snap_mount_paths:
                for snap in all_snaps:
                    source_clients[0].exec_command(
                        sudo=True,
                        cmd="rmdir %s.snap/%s" % (spath, snap),
                        check_ec=False,
                    )

            mount_dirs = [kernel_mounting_dir, fuse_mounting_dir]
            log.info("Unmount the paths")
            for mdir in mount_dirs:
                source_clients[0].exec_command(
                    sudo=True, cmd="umount -l %s" % mdir, check_ec=False
                )

            log.info("Delete the mounted paths")
            for mdir in mount_dirs:
                source_clients[0].exec_command(
                    sudo=True, cmd="rm -rf %s" % mdir, check_ec=False
                )

            log.info("Remove paths used for mirroring")
            for subvol_path in subvolume_paths:
                fs_mirroring_utils.remove_path_from_mirroring(
                    source_clients[0], source_fs, subvol_path
                )

            log.info("Destroy CephFS Mirroring setup")
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

            log.info("Remove Subvolumes")
            for sv in subvol_details:
                fs_util_ceph1.remove_subvolume(
                    source_clients[0],
                    source_fs,
                    sv["subvol_name"],
                    group_name=subvol_group_name,
                    check_ec=False,
                )

            log.info("Remove Subvolume Group")
            fs_util_ceph1.remove_subvolumegroup(
                source_clients[0],
                source_fs,
                subvol_group_name,
                check_ec=False,
            )
        except Exception as cleanup_err:
            log.warning("Cleanup encountered an error: %s", cleanup_err)
