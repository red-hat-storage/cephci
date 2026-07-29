import random
import string
import time
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_mirroring.cephfs_mirroring_utils import CephfsMirroringUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)

PEER_COUNTER_KEY = "cephfs_mirror_peers"
# Upstream write_n_mb(..., 100) — 10 × 100 MiB and 15 × 100 MiB
SNAP0_SYNCED_BYTES = 1048576000  # 10 * 100 * 1024 * 1024
SNAP1_SYNCED_BYTES = 1572864000  # 15 * 100 * 1024 * 1024
FILE_SIZE_MB = 100


def _get_peer_counters(
    fs_mirroring_utils, cephfs_mirror_node, fsid, asok_files, source_fs
):
    """Return cephfs_mirror_peers counters for source_fs."""
    data = fs_mirroring_utils.get_cephfs_mirror_counters(
        cephfs_mirror_node, fsid, asok_files
    )
    _, counters = fs_mirroring_utils.get_labels_and_counters(
        PEER_COUNTER_KEY, source_fs, data
    )
    if not counters:
        raise CommandFailed(f"{PEER_COUNTER_KEY} counters not found for {source_fs}")
    return counters


def _get_directory_counters(
    fs_mirroring_utils, cephfs_mirror_node, fsid, asok_files, dir_path, peer_uuid
):
    """Return cephfs_mirror_directory counters for dir_path + peer_uuid, or None."""
    entries = fs_mirroring_utils.get_directory_counters(
        cephfs_mirror_node, fsid, asok_files
    )
    wanted = {dir_path, dir_path.rstrip("/"), f"{dir_path.rstrip('/')}/"}
    for entry in entries:
        labels = entry.get("labels", {})
        if labels.get("directory") in wanted and labels.get("peer_uuid") == peer_uuid:
            return entry.get("counters", {})
    return None


def _write_n_mb_files(client, base_dir, prefix, count, size_mb):
    """Create count files of size_mb each under base_dir (dd bs=1M)."""
    for i in range(count):
        path = f"{base_dir}/{prefix}.{i}"
        log.info("Writing %s MiB file: %s", size_mb, path)
        client.exec_command(
            sudo=True,
            cmd=f"dd if=/dev/urandom of={path} bs=1M count={size_mb} status=none",
        )


def _wait_peer_counter_gt(
    fs_mirroring_utils,
    cephfs_mirror_node,
    fsid,
    asok_files,
    source_fs,
    key,
    baseline,
    timeout=300,
    interval=10,
):
    """Poll peer counters until key > baseline or timeout."""
    elapsed = 0
    while elapsed < timeout:
        counters = _get_peer_counters(
            fs_mirroring_utils, cephfs_mirror_node, fsid, asok_files, source_fs
        )
        actual = counters.get(key, 0)
        log.info("[%ss] wait %s > %s (current=%s)", elapsed, key, baseline, actual)
        if actual > baseline:
            return counters
        time.sleep(interval)
        elapsed += interval
    raise CommandFailed(
        f"Timed out waiting for peer counter {key} > {baseline} within {timeout}s"
    )


def _wait_last_synced_bytes(
    fs_mirroring_utils,
    cephfs_mirror_node,
    fsid,
    asok_files,
    source_fs,
    expected_bytes,
    timeout=120,
    interval=5,
):
    """Poll until peer last_synced_bytes equals expected (last sync was our snap)."""
    elapsed = 0
    last = None
    while elapsed < timeout:
        counters = _get_peer_counters(
            fs_mirroring_utils, cephfs_mirror_node, fsid, asok_files, source_fs
        )
        last = counters
        actual = counters.get("last_synced_bytes")
        log.info(
            "[%ss] wait last_synced_bytes == %s (current=%s)",
            elapsed,
            expected_bytes,
            actual,
        )
        if actual == expected_bytes:
            return counters
        time.sleep(interval)
        elapsed += interval
    raise CommandFailed(
        f"Timed out waiting for last_synced_bytes == {expected_bytes}; "
        f"last counters={last}"
    )


def _snap_dir_checksum(client, snap_abs_path):
    """
    Checksum a snapshot directory using relative paths (upstream-style).

    Absolute mount prefixes must not be part of the digest — otherwise source
    and target always differ even when file contents match.
    """
    cmd = (
        f"cd {snap_abs_path} && "
        f"find -L . -type f -exec md5sum {{}} + | sort -k2 | md5sum"
    )
    out, _ = client.exec_command(sudo=True, cmd=cmd)
    digest = out.strip().split()[0]
    log.info("Snapshot checksum for %s: %s", snap_abs_path, digest)
    return digest


def _verify_snapshot_checksum(
    source_client, source_snap_path, target_client, target_snap_path
):
    """Assert source and target snapshot contents match via relative checksums."""
    src = _snap_dir_checksum(source_client, source_snap_path)
    tgt = _snap_dir_checksum(target_client, target_snap_path)
    if src != tgt:
        raise CommandFailed(
            f"Snapshot checksum mismatch: source={src} target={tgt} "
            f"(paths: {source_snap_path} vs {target_snap_path})"
        )
    log.info("Snapshot checksums match: %s", src)


def _mount_target(target_client, target_client_user, target_fs):
    """Mount target FS and return mount path."""
    mounting_dir = "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(10)
    )
    target_mount_path = f"/mnt/{mounting_dir}"
    target_client.exec_command(sudo=True, cmd=f"mkdir -p {target_mount_path}")
    target_client.exec_command(
        sudo=True,
        cmd=(
            f"ceph-fuse -n {target_client_user} {target_mount_path} "
            f"--client_fs {target_fs}"
        ),
    )
    log.info("Mounted target FS %s at %s", target_fs, target_mount_path)
    return target_mount_path


def _list_remote_snaps(target_client, target_mount_path, mirror_path):
    """Return snapshot names under mirror_path on an already-mounted target."""
    # mirror_path is like /d0/ — ensure .snap path is correct
    snap_dir = f"{target_mount_path.rstrip('/')}{mirror_path.rstrip('/')}/.snap"
    out, _ = target_client.exec_command(sudo=True, cmd=f"ls {snap_dir}", check_ec=False)
    snaps = out.strip().split() if out and out.strip() else []
    log.info("Remote snapshots under %s: %s", snap_dir, snaps)
    return snaps


def _wait_remote_snaps(
    target_client,
    target_mount_path,
    mirror_path,
    must_include=None,
    must_exclude=None,
    timeout=300,
    interval=10,
):
    """Poll remote .snap until include/exclude conditions are met."""
    must_include = must_include or []
    must_exclude = must_exclude or []
    elapsed = 0
    while elapsed < timeout:
        snaps = _list_remote_snaps(target_client, target_mount_path, mirror_path)
        missing = [s for s in must_include if s not in snaps]
        present = [s for s in must_exclude if s in snaps]
        log.info(
            "[%ss] remote snaps=%s missing=%s still_present=%s",
            elapsed,
            snaps,
            missing,
            present,
        )
        if not missing and not present:
            return snaps
        time.sleep(interval)
        elapsed += interval
    raise CommandFailed(
        f"Timed out waiting for remote snaps include={must_include} "
        f"exclude={must_exclude} under {mirror_path}"
    )


def run(ceph_cluster, **kw):
    """
    Ported from upstream: test_cephfs_mirror_stats in
    qa/tasks/cephfs/test_mirroring.py

    Scenario (exact upstream data sizes and steps):
        1. Create /d0 with 10 × 100 MiB files (file.0 .. file.9)
        2. Enable mirroring, add /d0, add peer
        3. Dump peer perf counters (baseline)
        4. Create snap0 — wait sync, verify relative checksums
        5. Assert peer counters: snaps_synced↑, last_synced_* valid,
           last_synced_bytes == 1048576000; directory snaps_synced matches
        6. Write 15 × 100 MiB files (more_file.0 .. more_file.14)
        7. Create snap1 — wait sync, verify relative checksums
        8. Assert peer counters: snaps_synced↑, timing advanced,
           last_synced_bytes == 1572864000
        9. Delete snap0 — remote gone; snaps_deleted↑ (peer + directory)
       10. Rename snap1 → snap2 — remote shows snap2; snaps_renamed↑
       11. Cleanup

    Returns 0 on success, 1 on failure.
    """
    mirror_path = "/d0/"
    dir_path = "/d0"
    kernel_mounting_dir = None
    target_mount_path = None
    source_clients = None
    target_clients = None
    cephfs_mirror_node = None
    source_fs = target_fs = None
    target_user = "mirror_remote"
    target_site_name = "remote_site"
    fs_mirroring_utils = None
    peer_uuid = None
    snap_names = []

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
            log.error("Requires at least 1 client on both ceph1 and ceph2.")
            return 1

        fs_util_ceph1.prepare_clients(source_clients, build)
        fs_util_ceph2.prepare_clients(target_clients, build)
        fs_util_ceph1.auth_list(source_clients)
        fs_util_ceph2.auth_list(target_clients)

        source_fs = "cephfs" if not erasure else "cephfs-ec"
        target_fs = "cephfs" if not erasure else "cephfs-ec"
        if not fs_util_ceph1.get_fs_info(source_clients[0], source_fs):
            fs_util_ceph1.create_fs(source_clients[0], source_fs)
            fs_util_ceph1.wait_for_mds_process(source_clients[0], source_fs)
        if not fs_util_ceph2.get_fs_info(target_clients[0], target_fs):
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
            random.choice(string.ascii_lowercase + string.digits) for _ in range(10)
        )
        kernel_mounting_dir = f"/mnt/cephfs_kernel{mounting_dir}_stats/"
        mon_node_ips = fs_util_ceph1.get_mon_node_ips()
        fs_util_ceph1.kernel_mount(
            [source_clients[0]],
            kernel_mounting_dir,
            ",".join(mon_node_ips),
            extra_params=f",fs={source_fs}",
        )

        target_client_user = f"client.{target_clients[0].node.hostname}"
        target_mount_path = _mount_target(
            target_clients[0], target_client_user, target_fs
        )

        abs_d0 = f"{kernel_mounting_dir}d0"
        tgt_d0 = f"{target_mount_path.rstrip('/')}/d0"
        log.info("Create /d0 and write 10 × %s MiB files", FILE_SIZE_MB)
        source_clients[0].exec_command(sudo=True, cmd=f"mkdir -p {abs_d0}")
        _write_n_mb_files(source_clients[0], abs_d0, "file", 10, FILE_SIZE_MB)

        log.info("Add directory for mirroring: %s", mirror_path)
        fs_mirroring_utils.add_path_for_mirroring(
            source_clients[0], source_fs, mirror_path
        )

        fsid = fs_mirroring_utils.get_fsid(cephfs_mirror_node[0])
        daemon_names = fs_mirroring_utils.get_daemon_name(source_clients[0])
        asok_files = fs_mirroring_utils.get_asok_file_with_connectivity_check(
            cephfs_mirror_node, fsid, daemon_names
        )
        filesystem_id = fs_mirroring_utils.get_filesystem_id_by_name(
            source_clients[0], source_fs
        )
        peer_uuid = fs_mirroring_utils.get_peer_uuid_by_name(
            source_clients[0], source_fs
        )
        for node in cephfs_mirror_node:
            node.exec_command(
                sudo=True, cmd="yum install -y ceph-common --nogpgcheck", check_ec=False
            )

        # --- Baseline peer counters ---
        first = _get_peer_counters(
            fs_mirroring_utils, cephfs_mirror_node, fsid, asok_files, source_fs
        )
        log.info("Baseline peer counters: %s", first)

        # ==================== SNAP0 ====================
        snap0 = "snap0"
        snap_names.append(snap0)
        log.info("Creating snapshot '%s'", snap0)
        source_clients[0].exec_command(sudo=True, cmd=f"mkdir {abs_d0}/.snap/{snap0}")

        result0 = fs_mirroring_utils.validate_snapshot_sync_status(
            cephfs_mirror_node,
            source_fs,
            snap0,
            fsid,
            asok_files,
            filesystem_id,
            peer_uuid,
        )
        log.info(
            "snap0 synced: duration=%s snaps_synced=%s",
            result0["sync_duration"],
            result0["snaps_synced"],
        )
        if int(result0["snaps_synced"]) < 1:
            raise CommandFailed(
                f"Expected snaps_synced >= 1 after snap0, got {result0['snaps_synced']}"
            )

        _wait_remote_snaps(
            target_clients[0],
            target_mount_path,
            mirror_path,
            must_include=[snap0],
        )
        log.info("Verifying snap0 data checksums (relative paths)")
        _verify_snapshot_checksum(
            source_clients[0],
            f"{abs_d0}/.snap/{snap0}",
            target_clients[0],
            f"{tgt_d0}/.snap/{snap0}",
        )

        second = _wait_last_synced_bytes(
            fs_mirroring_utils,
            cephfs_mirror_node,
            fsid,
            asok_files,
            source_fs,
            SNAP0_SYNCED_BYTES,
        )
        log.info("Peer counters after snap0: %s", second)

        if second["snaps_synced"] <= first["snaps_synced"]:
            raise CommandFailed(
                f"snaps_synced did not increase after snap0: "
                f"{first['snaps_synced']} -> {second['snaps_synced']}"
            )
        if second["last_synced_start"] <= first["last_synced_start"]:
            raise CommandFailed(
                f"last_synced_start did not increase after snap0: "
                f"{first['last_synced_start']} -> {second['last_synced_start']}"
            )
        if second["last_synced_end"] < second["last_synced_start"]:
            raise CommandFailed(
                f"last_synced_end < last_synced_start after snap0: "
                f"{second['last_synced_end']} < {second['last_synced_start']}"
            )
        if second["last_synced_duration"] <= 0:
            raise CommandFailed(
                f"last_synced_duration should be > 0, got {second['last_synced_duration']}"
            )
        log.info(
            "snap0 peer counters OK (last_synced_bytes=%s)",
            second["last_synced_bytes"],
        )

        # With only /d0 mirrored, directory snaps_synced should match peer counter
        dir_second = _get_directory_counters(
            fs_mirroring_utils,
            cephfs_mirror_node,
            fsid,
            asok_files,
            dir_path,
            peer_uuid,
        )
        if dir_second is None:
            log.warning(
                "cephfs_mirror_directory counters not found for %s; "
                "skipping directory-level snaps_synced check",
                dir_path,
            )
        else:
            if dir_second.get("snaps_synced") != second["snaps_synced"]:
                raise CommandFailed(
                    f"directory snaps_synced ({dir_second.get('snaps_synced')}) "
                    f"!= peer snaps_synced ({second['snaps_synced']})"
                )
            log.info(
                "Directory snaps_synced matches peer: %s",
                dir_second.get("snaps_synced"),
            )

        # ==================== SNAP1 ====================
        log.info("Writing 15 × %s MiB more_file.* files", FILE_SIZE_MB)
        _write_n_mb_files(source_clients[0], abs_d0, "more_file", 15, FILE_SIZE_MB)

        snap1 = "snap1"
        snap_names.append(snap1)
        log.info("Creating snapshot '%s'", snap1)
        source_clients[0].exec_command(sudo=True, cmd=f"mkdir {abs_d0}/.snap/{snap1}")

        result1 = fs_mirroring_utils.validate_snapshot_sync_status(
            cephfs_mirror_node,
            source_fs,
            snap1,
            fsid,
            asok_files,
            filesystem_id,
            peer_uuid,
        )
        log.info(
            "snap1 synced: duration=%s snaps_synced=%s",
            result1["sync_duration"],
            result1["snaps_synced"],
        )
        if int(result1["snaps_synced"]) < 2:
            raise CommandFailed(
                f"Expected snaps_synced >= 2 after snap1, got {result1['snaps_synced']}"
            )

        _wait_remote_snaps(
            target_clients[0],
            target_mount_path,
            mirror_path,
            must_include=[snap0, snap1],
        )
        log.info("Verifying snap1 data checksums (relative paths)")
        _verify_snapshot_checksum(
            source_clients[0],
            f"{abs_d0}/.snap/{snap1}",
            target_clients[0],
            f"{tgt_d0}/.snap/{snap1}",
        )

        third = _wait_last_synced_bytes(
            fs_mirroring_utils,
            cephfs_mirror_node,
            fsid,
            asok_files,
            source_fs,
            SNAP1_SYNCED_BYTES,
        )
        log.info("Peer counters after snap1: %s", third)

        if third["snaps_synced"] <= second["snaps_synced"]:
            raise CommandFailed(
                f"snaps_synced did not increase after snap1: "
                f"{second['snaps_synced']} -> {third['snaps_synced']}"
            )
        if third["last_synced_start"] <= second["last_synced_end"]:
            raise CommandFailed(
                f"last_synced_start after snap1 not > previous last_synced_end: "
                f"{third['last_synced_start']} <= {second['last_synced_end']}"
            )
        if third["last_synced_end"] < third["last_synced_start"]:
            raise CommandFailed(
                f"last_synced_end < last_synced_start after snap1: "
                f"{third['last_synced_end']} < {third['last_synced_start']}"
            )
        if third["last_synced_duration"] <= 0:
            raise CommandFailed(
                f"last_synced_duration should be > 0, got {third['last_synced_duration']}"
            )
        log.info(
            "snap1 peer counters OK (last_synced_bytes=%s)",
            third["last_synced_bytes"],
        )

        # ==================== DELETE snap0 ====================
        log.info("Deleting snapshot snap0")
        source_clients[0].exec_command(sudo=True, cmd=f"rmdir {abs_d0}/.snap/{snap0}")
        snap_names.remove(snap0)

        fourth = _wait_peer_counter_gt(
            fs_mirroring_utils,
            cephfs_mirror_node,
            fsid,
            asok_files,
            source_fs,
            "snaps_deleted",
            third.get("snaps_deleted", 0),
        )
        log.info("Peer counters after snap0 delete: %s", fourth)

        _wait_remote_snaps(
            target_clients[0],
            target_mount_path,
            mirror_path,
            must_include=[snap1],
            must_exclude=[snap0],
        )
        log.info("Remote snap0 removed — OK")

        dir_fourth = _get_directory_counters(
            fs_mirroring_utils,
            cephfs_mirror_node,
            fsid,
            asok_files,
            dir_path,
            peer_uuid,
        )
        if dir_second is not None and dir_fourth is not None:
            if dir_fourth.get("snaps_deleted", 0) <= dir_second.get("snaps_deleted", 0):
                raise CommandFailed(
                    f"directory snaps_deleted did not increase: "
                    f"{dir_second.get('snaps_deleted')} -> "
                    f"{dir_fourth.get('snaps_deleted')}"
                )
            log.info(
                "Directory snaps_deleted increased: %s -> %s",
                dir_second.get("snaps_deleted"),
                dir_fourth.get("snaps_deleted"),
            )

        # ==================== RENAME snap1 → snap2 ====================
        snap2 = "snap2"
        log.info("Renaming snapshot snap1 -> snap2")
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"mv {abs_d0}/.snap/{snap1} {abs_d0}/.snap/{snap2}",
        )
        snap_names.remove(snap1)
        snap_names.append(snap2)

        fifth = _wait_peer_counter_gt(
            fs_mirroring_utils,
            cephfs_mirror_node,
            fsid,
            asok_files,
            source_fs,
            "snaps_renamed",
            fourth.get("snaps_renamed", 0),
        )
        log.info("Peer counters after snap rename: %s", fifth)

        _wait_remote_snaps(
            target_clients[0],
            target_mount_path,
            mirror_path,
            must_include=[snap2],
            must_exclude=[snap1],
        )
        log.info("Remote rename snap1 -> snap2 — OK")

        dir_fifth = _get_directory_counters(
            fs_mirroring_utils,
            cephfs_mirror_node,
            fsid,
            asok_files,
            dir_path,
            peer_uuid,
        )
        if dir_fourth is not None and dir_fifth is not None:
            if dir_fifth.get("snaps_renamed", 0) <= dir_fourth.get("snaps_renamed", 0):
                raise CommandFailed(
                    f"directory snaps_renamed did not increase: "
                    f"{dir_fourth.get('snaps_renamed')} -> "
                    f"{dir_fifth.get('snaps_renamed')}"
                )
            log.info(
                "Directory snaps_renamed increased: %s -> %s",
                dir_fourth.get("snaps_renamed"),
                dir_fifth.get("snaps_renamed"),
            )

        log.info("=" * 70)
        log.info("MIRROR STATS TEST PASSED")
        log.info(
            "  snap0 last_synced_bytes=%s (expected %s)",
            SNAP0_SYNCED_BYTES,
            SNAP0_SYNCED_BYTES,
        )
        log.info(
            "  snap1 last_synced_bytes=%s (expected %s)",
            SNAP1_SYNCED_BYTES,
            SNAP1_SYNCED_BYTES,
        )
        log.info("  snaps_deleted and snaps_renamed counters validated")
        log.info("=" * 70)
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean up the system")

        if source_clients and kernel_mounting_dir:
            for snap in list(snap_names):
                source_clients[0].exec_command(
                    sudo=True,
                    cmd=f"rmdir {kernel_mounting_dir}d0/.snap/{snap}",
                    check_ec=False,
                )

        if mirror_path and fs_mirroring_utils and source_clients and source_fs:
            try:
                fs_mirroring_utils.remove_path_from_mirroring(
                    source_clients[0], source_fs, mirror_path
                )
            except CommandFailed:
                log.debug(
                    "Mirror path %s was not tracked, skipping removal", mirror_path
                )

        if (
            fs_mirroring_utils
            and source_clients
            and target_clients
            and source_fs
            and target_fs
            and peer_uuid
        ):
            log.info("Destroy CephFS Mirroring setup.")
            try:
                fs_mirroring_utils.destroy_cephfs_mirroring(
                    source_fs,
                    source_clients[0],
                    target_fs,
                    target_clients[0],
                    target_user,
                    peer_uuid,
                )
            except Exception as cleanup_err:
                log.warning("destroy_cephfs_mirroring failed: %s", cleanup_err)

        if target_clients and target_mount_path:
            target_clients[0].exec_command(
                sudo=True, cmd=f"umount -l {target_mount_path}", check_ec=False
            )
            target_clients[0].exec_command(
                sudo=True, cmd=f"rm -rf {target_mount_path}", check_ec=False
            )

        if source_clients and kernel_mounting_dir:
            source_clients[0].exec_command(
                sudo=True, cmd=f"umount -l {kernel_mounting_dir}", check_ec=False
            )
            source_clients[0].exec_command(
                sudo=True, cmd=f"rm -rf {kernel_mounting_dir}", check_ec=False
            )
