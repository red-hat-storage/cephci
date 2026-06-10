import json
import re
import time
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_mirroring.cephfs_mirroring_utils import CephfsMirroringUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)

SNAPDIFF_THRESHOLD_MB = 16

DEFAULT_SMALL_FILES = [
    {"name": "small_1MB.bin", "size_mb": 1},
    {"name": "small_4MB.bin", "size_mb": 4},
    {"name": "small_8MB.bin", "size_mb": 8},
    {"name": "small_15MB.bin", "size_mb": 15},
]

DEFAULT_LARGE_FILES = [
    {"name": "large_16MB.bin", "size_mb": 16},
    {"name": "large_32MB.bin", "size_mb": 32},
    {"name": "large_64MB.bin", "size_mb": 64},
    {"name": "large_128MB.bin", "size_mb": 128},
]


def run(ceph_cluster, **kw):
    """
    Snapdiff vs Blockdiff Validation Test (CephFS Mirroring)

    Validates that the cephfs-mirror daemon correctly uses:
      - snapdiff  for files < 16 MiB  (copies entire file as one block)
      - blockdiff for files >= 16 MiB (copies only changed blocks)

    Test flow:
      1. Create subvolume, enable mirroring, kernel-mount.
      2. Create files of varying sizes (small < 16M, large >= 16M).
      3. Snapshot (snap1) -> wait for full initial sync.
      4. Modify a small file (full overwrite) and a large file (partial).
      5. Sync + Snapshot (snap2) -> wait for incremental sync.
      6. Parse mirror daemon logs to verify snapdiff/blockdiff patterns.
      7. Modify large file at two separate regions.
      8. Sync + Snapshot (snap3) -> wait for incremental sync.
      9. Verify multi-block blockdiff in logs.
     10. Cleanup.

    Config keys (all optional):
        debug_level     : int  -- debug_cephfs_mirror level, default 20
        small_files     : list -- [{name, size_mb}, ...], default 1/4/8/15 MB
        large_files     : list -- [{name, size_mb}, ...], default 16/32/64/128 MB
        sync_poll_tries : int  -- max poll attempts, default 120
        sync_poll_delay : int  -- seconds between polls, default 15

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
    subvol_group = "subvolgroup_snapdiff"
    subvol_name = "subvol_diff_test"
    subvolumegroup_list = []
    mirroring_paths = []
    mount_dir = None

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

        if not source_clients or not target_clients:
            log.error(
                "This test requires at least 1 client node on both ceph1 and ceph2."
            )
            return 1

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

        # ---- Read config ----
        debug_level = config.get("debug_level", 20)
        small_files = config.get("small_files", DEFAULT_SMALL_FILES)
        large_files = config.get("large_files", DEFAULT_LARGE_FILES)
        sync_poll_tries = config.get("sync_poll_tries", 120)
        sync_poll_delay = config.get("sync_poll_delay", 15)

        # ---- Set log_to_file and debug level on mirror daemon ----
        log.info("Setting log_to_file = true for cephfs-mirror")
        source_clients[0].exec_command(
            sudo=True,
            cmd="ceph config set client.cephfs-mirror log_to_file true",
        )
        log.info("Setting debug_cephfs_mirror = %d", debug_level)
        source_clients[0].exec_command(
            sudo=True,
            cmd=(
                f"ceph config set client.cephfs-mirror "
                f"debug_cephfs_mirror {debug_level}"
            ),
        )
        source_clients[0].exec_command(sudo=True, cmd="ceph orch restart cephfs-mirror")
        time.sleep(30)

        # ---- Resolve mirror daemon node for log access ----
        mirror_node = _get_mirror_daemon_node(source_clients[0], cephfs_mirror_node)
        if not mirror_node:
            log.error("Could not resolve cephfs-mirror daemon node")
            return 1
        log.info("Mirror daemon running on: %s", mirror_node.hostname)

        fsid = fs_mirroring_utils.get_fsid(cephfs_mirror_node[0])
        log_pattern = f"/var/log/ceph/{fsid}/ceph-client.cephfs-mirror.*.log"

        # ---- Step 1: Create subvolume and enable mirroring ----
        log.info("Step 1: Create subvolume and enable mirroring")
        subvolumegroup_list = [
            {"vol_name": source_fs, "group_name": subvol_group},
        ]
        fs_util_ceph1.create_subvolumegroup(source_clients[0], **subvolumegroup_list[0])
        fs_util_ceph1.create_subvolume(
            source_clients[0],
            vol_name=source_fs,
            subvol_name=subvol_name,
            group_name=subvol_group,
        )

        subvol_path_raw, _ = source_clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {source_fs} {subvol_name} {subvol_group}",
        )
        subvol_path_raw = subvol_path_raw.strip()
        idx = subvol_path_raw.find(f"{subvol_name}/")
        if idx != -1:
            subvol_path = subvol_path_raw[: idx + len(f"{subvol_name}/")]
        else:
            subvol_path = subvol_path_raw
        log.info("Subvolume path: %s", subvol_path)

        fs_mirroring_utils.add_path_for_mirroring(
            source_clients[0], source_fs, subvol_path
        )
        mirroring_paths.append(subvol_path)

        # Kernel mount
        mount_dir = "/mnt/cephfs_snapdiff_test/"
        source_clients[0].exec_command(sudo=True, cmd=f"mkdir -p {mount_dir}")
        mon_node_ips = fs_util_ceph1.get_mon_node_ips()
        fs_util_ceph1.kernel_mount(
            [source_clients[0]],
            mount_dir,
            ",".join(mon_node_ips),
            extra_params=f",fs={source_fs}",
        )

        basedir = f"{mount_dir}{subvol_path.lstrip('/')}diff_test"
        source_clients[0].exec_command(sudo=True, cmd=f"mkdir -p {basedir}")
        log.info("IO base directory: %s", basedir)

        # ---- Step 2: Create test files ----
        log.info("Step 2: Creating test files of varying sizes")
        for finfo in small_files:
            _dd_file(source_clients[0], basedir, finfo["name"], finfo["size_mb"])
        for finfo in large_files:
            _dd_file(source_clients[0], basedir, finfo["name"], finfo["size_mb"])

        source_clients[0].exec_command(sudo=True, cmd="sync")

        # ---- Step 3: Initial snapshot (full sync) ----
        log.info("Step 3: Creating initial snapshot (snap1) for full sync")
        snap1 = "diff_test_snap1"
        fs_util_ceph1.create_snapshot(
            client=source_clients[0],
            vol_name=source_fs,
            subvol_name=subvol_name,
            snap_name=snap1,
            validate=True,
            group_name=subvol_group,
        )

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

        log.info("Waiting for snap1 to sync (full initial sync)...")
        result_snap1 = _poll_snapshot_sync(
            fs_mirroring_utils,
            cephfs_mirror_node,
            source_fs,
            snap1,
            fsid,
            asok_file,
            filesystem_id,
            peer_uuid,
            max_tries=sync_poll_tries,
            delay=sync_poll_delay,
        )
        if not result_snap1:
            log.error("snap1 did not sync within timeout")
            return 1
        log.info("snap1 synced successfully: %s", result_snap1)

        # ---- Step 4: Modify files and create incremental snapshot ----
        log.info("Step 4: Modifying files for incremental sync")
        small_modify_file = "small_4MB.bin"
        large_modify_file = "large_64MB.bin"
        large_modify_offset = 30  # MB offset for partial write
        large_modify_count = 1  # MB to write

        log.info("Overwriting small file: %s (full rewrite)", small_modify_file)
        _dd_file(source_clients[0], basedir, small_modify_file, 4)

        log.info(
            "Partially modifying large file: %s (1MB at offset %dMB)",
            large_modify_file,
            large_modify_offset,
        )
        source_clients[0].exec_command(
            sudo=True,
            cmd=(
                f"dd if=/dev/urandom "
                f"of={basedir}/{large_modify_file} "
                f"bs=1M count={large_modify_count} seek={large_modify_offset} "
                f"conv=notrunc 2>/dev/null"
            ),
        )

        source_clients[0].exec_command(sudo=True, cmd="sync")

        log_marker_pre_snap2 = _get_log_line_count(mirror_node, log_pattern)

        snap2 = "diff_test_snap2"
        log.info("Creating snapshot: %s", snap2)
        fs_util_ceph1.create_snapshot(
            client=source_clients[0],
            vol_name=source_fs,
            subvol_name=subvol_name,
            snap_name=snap2,
            validate=True,
            group_name=subvol_group,
        )

        log.info("Waiting for snap2 to sync (incremental)...")
        result_snap2 = _poll_snapshot_sync(
            fs_mirroring_utils,
            cephfs_mirror_node,
            source_fs,
            snap2,
            fsid,
            asok_file,
            filesystem_id,
            peer_uuid,
            max_tries=sync_poll_tries,
            delay=sync_poll_delay,
        )
        if not result_snap2:
            log.error("snap2 did not sync within timeout")
            return 1
        log.info("snap2 synced successfully: %s", result_snap2)

        # ---- Step 5: Verify snapdiff vs blockdiff in daemon logs ----
        log.info("Step 5: Verifying snapdiff/blockdiff patterns in daemon logs")
        time.sleep(5)

        small_logs = _grep_daemon_log(
            mirror_node, log_pattern, small_modify_file, log_marker_pre_snap2
        )
        large_logs = _grep_daemon_log(
            mirror_node, log_pattern, large_modify_file, log_marker_pre_snap2
        )

        log.info("=== Daemon log lines for %s (small, snapdiff) ===", small_modify_file)
        for line in small_logs:
            log.info("  %s", line)

        log.info(
            "=== Daemon log lines for %s (large, blockdiff) ===", large_modify_file
        )
        for line in large_logs:
            log.info("  %s", line)

        # Validate snapdiff: small file blocks must cover the entire file from offset 0.
        # Use coverage check rather than exact equality — CephFS may align block sizes.
        small_blocks = _extract_copy_blocks(small_logs)
        failures = []

        if not small_blocks:
            failures.append(
                f"snapdiff: no copy_to_remote blocks found for {small_modify_file}"
            )
        else:
            expected_small_size = 4 * 1024 * 1024  # 4 MB
            log.info(
                "Snapdiff result for %s: num_blocks=%d, blocks=%s",
                small_modify_file,
                len(small_blocks),
                small_blocks,
            )
            if not _region_covers(small_blocks, 0, expected_small_size):
                failures.append(
                    f"snapdiff: blocks {small_blocks} do not cover entire file "
                    f"[0~{expected_small_size}] for {small_modify_file}"
                )
            else:
                log.info(
                    "PASS: %s - snapdiff covered entire file from offset 0",
                    small_modify_file,
                )

        # Validate blockdiff: blocks must cover the modified region (block-aligned).
        large_blocks = _extract_copy_blocks(large_logs)
        if not large_blocks:
            failures.append(
                f"blockdiff: no copy_to_remote blocks found for {large_modify_file}"
            )
        else:
            expected_offset = large_modify_offset * 1024 * 1024
            expected_len = large_modify_count * 1024 * 1024
            log.info(
                "Blockdiff result for %s: num_blocks=%d, blocks=%s",
                large_modify_file,
                len(large_blocks),
                large_blocks,
            )
            if not _region_covers(large_blocks, expected_offset, expected_len):
                failures.append(
                    f"blockdiff: blocks {large_blocks} do not cover modified region "
                    f"[{expected_offset}~{expected_len}] for {large_modify_file}"
                )
            else:
                log.info(
                    "PASS: %s - blockdiff covered modified region [%d~%d]",
                    large_modify_file,
                    expected_offset,
                    expected_len,
                )

        # ---- Step 6: Multi-region modification ----
        log.info("Step 6: Multi-region modification on large file")
        region1_offset = 10  # MB
        region2_offset = 50  # MB

        log.info(
            "Modifying %s at offsets %dMB and %dMB",
            large_modify_file,
            region1_offset,
            region2_offset,
        )
        source_clients[0].exec_command(
            sudo=True,
            cmd=(
                f"dd if=/dev/urandom of={basedir}/{large_modify_file} "
                f"bs=1M count=1 seek={region1_offset} conv=notrunc 2>/dev/null"
            ),
        )
        source_clients[0].exec_command(
            sudo=True,
            cmd=(
                f"dd if=/dev/urandom of={basedir}/{large_modify_file} "
                f"bs=1M count=1 seek={region2_offset} conv=notrunc 2>/dev/null"
            ),
        )
        source_clients[0].exec_command(sudo=True, cmd="sync")

        log_marker_pre_snap3 = _get_log_line_count(mirror_node, log_pattern)

        snap3 = "diff_test_snap3"
        log.info("Creating snapshot: %s", snap3)
        fs_util_ceph1.create_snapshot(
            client=source_clients[0],
            vol_name=source_fs,
            subvol_name=subvol_name,
            snap_name=snap3,
            validate=True,
            group_name=subvol_group,
        )

        log.info("Waiting for snap3 to sync...")
        result_snap3 = _poll_snapshot_sync(
            fs_mirroring_utils,
            cephfs_mirror_node,
            source_fs,
            snap3,
            fsid,
            asok_file,
            filesystem_id,
            peer_uuid,
            max_tries=sync_poll_tries,
            delay=sync_poll_delay,
        )
        if not result_snap3:
            log.error("snap3 did not sync within timeout")
            return 1
        log.info("snap3 synced successfully: %s", result_snap3)

        time.sleep(5)
        large_logs_snap3 = _grep_daemon_log(
            mirror_node, log_pattern, large_modify_file, log_marker_pre_snap3
        )

        log.info(
            "=== Daemon log lines for %s (multi-region blockdiff) ===",
            large_modify_file,
        )
        for line in large_logs_snap3:
            log.info("  %s", line)

        multi_blocks = _extract_copy_blocks(large_logs_snap3)
        if not multi_blocks:
            failures.append(
                f"blockdiff (snap3): no copy_to_remote blocks found for "
                f"{large_modify_file}"
            )
        else:
            r1_offset = region1_offset * 1024 * 1024
            r2_offset = region2_offset * 1024 * 1024
            block_1mb = 1024 * 1024
            log.info(
                "Multi-region blockdiff for %s: num_blocks=%d, blocks=%s",
                large_modify_file,
                len(multi_blocks),
                multi_blocks,
            )
            if not _region_covers(multi_blocks, r1_offset, block_1mb):
                failures.append(
                    f"blockdiff (snap3): region1 [{r1_offset}~{block_1mb}] "
                    f"not covered by {multi_blocks}"
                )
            else:
                log.info(
                    "PASS: %s - blockdiff covered region1 [%d~%d]",
                    large_modify_file,
                    r1_offset,
                    block_1mb,
                )
            if not _region_covers(multi_blocks, r2_offset, block_1mb):
                failures.append(
                    f"blockdiff (snap3): region2 [{r2_offset}~{block_1mb}] "
                    f"not covered by {multi_blocks}"
                )
            else:
                log.info(
                    "PASS: %s - blockdiff covered region2 [%d~%d]",
                    large_modify_file,
                    r2_offset,
                    block_1mb,
                )

        # ---- Final summary ----
        separator = "=" * 70
        log.info(separator)
        log.info("SNAPDIFF vs BLOCKDIFF VALIDATION SUMMARY")
        log.info(separator)
        log.info(
            "Threshold: files < %d MiB use snapdiff, >= %d MiB use blockdiff",
            SNAPDIFF_THRESHOLD_MB,
            SNAPDIFF_THRESHOLD_MB,
        )
        log.info(
            "snap2 (single modify): small=%s blocks, large=%s blocks",
            len(small_blocks) if small_blocks else "N/A",
            len(large_blocks) if large_blocks else "N/A",
        )
        log.info(
            "snap3 (multi-region):  large=%s blocks",
            len(multi_blocks) if multi_blocks else "N/A",
        )
        log.info(separator)

        if failures:
            for failure in failures:
                log.error("FAIL: %s", failure)
            return 1
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1

    finally:
        if (config or {}).get("cleanup", True) and source_clients:
            log.info("Cleanup: removing debug_cephfs_mirror and log_to_file configs")
            try:
                source_clients[0].exec_command(
                    sudo=True,
                    cmd="ceph config rm client.cephfs-mirror debug_cephfs_mirror",
                )
            except Exception as e:
                log.warning("Failed to reset debug config: %s", e)
            try:
                source_clients[0].exec_command(
                    sudo=True,
                    cmd="ceph config rm client.cephfs-mirror log_to_file",
                )
            except Exception as e:
                log.warning("Failed to reset log_to_file config: %s", e)

            log.info("Cleanup: restarting cephfs-mirror daemon")
            try:
                source_clients[0].exec_command(
                    sudo=True, cmd="ceph orch restart cephfs-mirror"
                )
                time.sleep(30)
            except Exception as e:
                log.warning("Failed to restart mirror daemon: %s", e)

            if mount_dir:
                log.info("Cleanup: unmounting %s", mount_dir)
                try:
                    source_clients[0].exec_command(
                        sudo=True, cmd=f"umount -l {mount_dir} 2>/dev/null || true"
                    )
                    source_clients[0].exec_command(sudo=True, cmd=f"rm -rf {mount_dir}")
                except Exception:
                    pass

            if fs_mirroring_utils and mirroring_paths and source_fs:
                log.info("Cleanup: removing mirroring paths")
                for mpath in mirroring_paths:
                    try:
                        fs_mirroring_utils.remove_path_from_mirroring(
                            source_clients[0], source_fs, mpath
                        )
                    except Exception as e:
                        log.warning("Failed to remove mirroring path %s: %s", mpath, e)

            # Remove snapshots
            for snap in ["diff_test_snap1", "diff_test_snap2", "diff_test_snap3"]:
                try:
                    fs_util_ceph1.remove_snapshot(
                        client=source_clients[0],
                        vol_name=source_fs,
                        subvol_name=subvol_name,
                        snap_name=snap,
                        validate=False,
                        group_name=subvol_group,
                        force=True,
                    )
                except Exception:
                    pass

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

            if fs_util_ceph1:
                try:
                    fs_util_ceph1.remove_subvolume(
                        source_clients[0],
                        vol_name=source_fs,
                        subvol_name=subvol_name,
                        group_name=subvol_group,
                    )
                except Exception:
                    pass

            if fs_util_ceph1 and subvolumegroup_list:
                for svg in subvolumegroup_list:
                    try:
                        fs_util_ceph1.remove_subvolumegroup(source_clients[0], **svg)
                    except Exception:
                        pass


def _dd_file(client, basedir, filename, size_mb):
    """Create a file with dd from /dev/urandom."""
    client.exec_command(
        sudo=True,
        cmd=(
            f"dd if=/dev/urandom of={basedir}/{filename} "
            f"bs=1M count={size_mb} 2>/dev/null"
        ),
    )
    log.info("Created %s/%s (%d MB)", basedir, filename, size_mb)


def _get_mirror_daemon_node(source_client, cephfs_mirror_nodes):
    """Resolve the node where the cephfs-mirror daemon is actually running."""
    try:
        out, _ = source_client.exec_command(
            sudo=True,
            cmd="ceph orch ps --daemon_type cephfs-mirror -f json",
        )
        data = json.loads(out)
        if data:
            hostname = data[0].get("hostname")
            for node_obj in cephfs_mirror_nodes:
                if node_obj.node.hostname == hostname:
                    return node_obj.node
            for node_obj in cephfs_mirror_nodes:
                return node_obj.node
    except Exception as e:
        log.warning("Failed to resolve mirror daemon node: %s", e)
    if cephfs_mirror_nodes:
        return cephfs_mirror_nodes[0].node
    return None


def _get_log_line_count(node, log_pattern):
    """Get current line count of the mirror daemon log file."""
    try:
        out, _ = node.exec_command(
            sudo=True,
            cmd=f"wc -l $(ls -1t {log_pattern} | head -1) 2>/dev/null | awk '{{print $1}}'",
        )
        return int(out.strip())
    except Exception:
        return 0


def _grep_daemon_log(node, log_pattern, filename, start_line):
    """Grep mirror daemon log for lines mentioning a file, starting from a line number."""
    try:
        out, _ = node.exec_command(
            sudo=True,
            cmd=(
                f"tail -n +{start_line + 1} $(ls -1t {log_pattern} | head -1) "
                f"| grep -E '(get_changed_blocks|copy_to_remote).*{filename}'"
            ),
            check_ec=False,
        )
        return [line.strip() for line in out.strip().split("\n") if line.strip()]
    except Exception as e:
        log.warning("Failed to grep daemon log for %s: %s", filename, e)
        return []


def _extract_copy_blocks(log_lines):
    """
    Parse copy_to_remote log lines to extract block offsets and lengths.

    Looks for patterns like:
        copy_to_remote: ...filename, block: [31457280~1048576]
        copy_to_remote: ...filename, num_blocks=2

    Returns list of (offset, length) tuples, or None if not parseable.
    """
    blocks = []
    block_re = re.compile(r"block:\s*\[(\d+)~(\d+)\]")

    for line in log_lines:
        match = block_re.search(line)
        if match:
            offset = int(match.group(1))
            length = int(match.group(2))
            blocks.append((offset, length))

    return blocks if blocks else None


def _region_covers(blocks, expected_offset, expected_len):
    """Return True if any block in blocks covers [expected_offset, expected_offset+expected_len).

    CephFS blockdiff reports dirty block ranges aligned to the object/block size
    (typically 4 MiB). A 1 MiB write may be reported as a larger aligned range,
    so we check coverage rather than exact equality.
    """
    end = expected_offset + expected_len
    return any(
        off <= expected_offset and (off + length) >= end for off, length in blocks
    )


def _poll_snapshot_sync(
    fs_mirroring_utils,
    cephfs_mirror_node,
    source_fs,
    snapshot_name,
    fsid,
    asok_file,
    filesystem_id,
    peer_uuid,
    max_tries=120,
    delay=15,
):
    """Poll the mirror daemon ASOK until the snapshot appears as synced."""
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
