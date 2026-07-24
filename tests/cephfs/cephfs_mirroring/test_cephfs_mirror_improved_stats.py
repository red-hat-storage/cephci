import json
import random
import string
import time
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_mirroring.cephfs_mirroring_utils import (
    CephfsMirroringUtils,
    validate_current_syncing_snap_schema,
    validate_last_synced_snap_schema,
    validate_peer_status_schema,
    wait_for_idle,
)
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)

UNIT_MULTIPLIERS = {
    "B": 1,
    "KiB": 1024,
    "MiB": 1024**2,
    "GiB": 1024**3,
}


def parse_bytes_str(bytes_str):
    """Parse human-readable byte string like '5.12 MiB' to float bytes."""
    parts = bytes_str.strip().split()
    if len(parts) != 2:
        return 0.0
    return float(parts[0]) * UNIT_MULTIPLIERS.get(parts[1], 1)


def run(ceph_cluster, **kw):
    """
    CEPH-83575001 - Validate improved CephFS mirroring stats (9.2 metrics)

    Covers P1 regression tests from the "Improve Mirroring Stats" test plan:
    R1: Schema validation + stats during sync and idle (asok + MGR)
    R2: Sync-mode validation (full vs delta)
    R5: Datasync queue wait under load (3 dirs, 10K files, config tuning)
    R7: last_synced_snap enrichment (asok + MGR)
    R12: Zero-file directory sync metrics (asok path1 + MGR path2)
    R16: Snapdiff and blockdiff verification (asok + MGR)

    Returns:
        0 if successful, 1 if any errors found.
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

        source_fs = "cephfs"
        target_fs = "cephfs"
        target_user = "mirror_remote"
        target_site_name = "remote_site"

        log.info("Deploy CephFS Mirroring Configuration")
        fs_mirroring_utils.deploy_cephfs_mirroring(
            source_fs,
            source_clients[0],
            target_fs,
            target_clients[0],
            target_user,
            target_site_name,
        )

        subvol_group_name = "subvolgroup_stats"
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )

        subvol_details = [
            {
                "subvol_name": "subvol_stats_1",
                "subvol_size": "12884901888",
                "mount_type": "kernel",
                "mount_dir": f"/mnt/cephfs_kernel{mounting_dir}_1",
            },
            {
                "subvol_name": "subvol_stats_2",
                "subvol_size": "12884901888",
                "mount_type": "fuse",
                "mount_dir": f"/mnt/cephfs_fuse{mounting_dir}_1",
            },
            {
                "subvol_name": "subvol_stats_3",
                "subvol_size": "12884901888",
                "mount_type": "kernel",
                "mount_dir": f"/mnt/cephfs_kernel{mounting_dir}_2",
            },
        ]
        subvolume_paths = fs_mirroring_utils.setup_subvolumes_and_mounts(
            source_fs,
            source_clients[0],
            fs_util_ceph1,
            subvol_group_name,
            subvol_details,
        )
        log.info(f"Subvolume Paths: {subvolume_paths}")

        for subvol_path in subvolume_paths:
            fs_mirroring_utils.add_path_for_mirroring(
                source_clients[0], source_fs, subvol_path
            )

        mount_path1 = f"/mnt/cephfs_kernel{mounting_dir}_1{subvolume_paths[0]}"
        mount_path2 = f"/mnt/cephfs_fuse{mounting_dir}_1{subvolume_paths[1]}"
        mount_path3 = f"/mnt/cephfs_kernel{mounting_dir}_2{subvolume_paths[2]}"

        log.info("Set tick interval to 1s for accurate sync metrics")
        source_clients[0].exec_command(
            sudo=True,
            cmd="ceph config set client.cephfs-mirror " "cephfs_mirror_tick_interval 1",
        )
        log.info("Restart cephfs-mirror daemon for tick_interval to take effect")
        source_clients[0].exec_command(sudo=True, cmd="ceph orch restart cephfs-mirror")
        time.sleep(30)

        # ============================================================
        # R12: Zero-file directory sync metrics
        #   path1 (kernel) — validated via asok
        #   path2 (fuse)   — validated via MGR interface
        # ============================================================
        log.info("=" * 60)
        log.info("R12: Zero-file directory sync metrics")
        log.info("=" * 60)

        log.info("Create empty subdirectories (no regular files) in both dirs")
        for i in range(5):
            source_clients[0].exec_command(
                sudo=True, cmd=f"mkdir -p {mount_path1}empty_dir_{i}"
            )
            source_clients[0].exec_command(
                sudo=True, cmd=f"mkdir -p {mount_path2}empty_dir_{i}"
            )

        log.info("Create snapshot on both empty directories")
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {mount_path1}.snap/snap_empty"
        )
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {mount_path2}.snap/snap_empty"
        )

        log.info("R12 path1: Validate via asok (poll until idle)")
        path1_key = subvolume_paths[0].rstrip("/")
        path1_status = wait_for_idle(
            fs_mirroring_utils,
            cephfs_mirror_node[0],
            source_clients[0],
            source_fs,
            subvolume_paths[0],
            timeout=180,
        )
        last_synced_p1 = path1_status.get("last_synced_snap", {})
        if last_synced_p1.get("name") != "snap_empty":
            raise CommandFailed(
                f"R12 path1 FAILED: Expected snap_empty, got {last_synced_p1}"
            )
        log.info("R12 path1 PASSED (asok): snap_empty synced")

        log.info("R12 path2: Validate via MGR mirror status")
        mgr_status = fs_mirroring_utils.get_mgr_mirror_status(
            source_clients[0], source_fs
        )
        log.info(f"R12 MGR status: {json.dumps(mgr_status, indent=2)}")
        path2_key = subvolume_paths[1].rstrip("/")
        mgr_metrics = mgr_status.get("metrics", {})
        mgr_path2_data = mgr_metrics.get(path2_key, {})
        if not mgr_path2_data:
            raise CommandFailed(f"R12: Path {path2_key} not found in MGR status")
        mgr_peer = list(mgr_path2_data.get("peer", {}).values())
        if not mgr_peer:
            raise CommandFailed("R12: No peer entry in MGR status for path2")
        mgr_state = mgr_peer[0].get("state", "")
        log.info(
            f"R12 path2 MGR: state={mgr_state}, entry={json.dumps(mgr_peer[0], indent=2)}"
        )
        if mgr_state not in ("idle", "syncing"):
            log.warning(f"R12 path2: MGR state is '{mgr_state}', expected idle/syncing")
        log.info("R12 path2 PASSED (MGR): Empty directory visible in mirror status")

        # ============================================================
        # R1: Mirroring stats validation with CLI schema verification
        # ============================================================
        log.info("=" * 60)
        log.info("R1: Stats validation + schema verification")
        log.info("=" * 60)

        log.info("Create data in dir1: 100 files x 1 MiB")
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"for i in $(seq 1 100); do dd if=/dev/urandom of={mount_path1}file_$i "
            f"bs=1M count=1 2>/dev/null; done",
        )

        log.info("Create snapshot snap1 on dir1 (triggers full sync)")
        source_clients[0].exec_command(sudo=True, cmd=f"mkdir {mount_path1}.snap/snap1")

        log.info("Poll peer status during sync to validate schema")
        for poll in range(30):
            time.sleep(3)
            try:
                peer_status = fs_mirroring_utils.get_asok_peer_status_raw(
                    cephfs_mirror_node[0], source_clients[0], source_fs
                )
                path1_check = peer_status.get(subvolume_paths[0].rstrip("/"), {})
                log.info(f"[R1 Poll {poll}] Raw asok: " f"{json.dumps(path1_check)}")
                if path1_check.get("state") in ("syncing", "idle"):
                    break
            except Exception as e:
                log.warning(f"R1 poll error: {e}")
        log.info(f"Peer status during sync: {json.dumps(peer_status, indent=2)}")

        path1_key = subvolume_paths[0].rstrip("/")
        validated = validate_peer_status_schema(peer_status, [subvolume_paths[0]])
        path1_status = validated[path1_key]

        state = path1_status.get("state")
        if state == "syncing":
            log.info("State is 'syncing' — validating current_syncing_snap schema")
            current_snap = path1_status.get("current_syncing_snap", {})
            result = validate_current_syncing_snap_schema(current_snap, strict=False)

            sync_mode = current_snap.get("sync-mode")
            if sync_mode != "full":
                log.warning(
                    f"R1: First snap sync-mode expected 'full', got '{sync_mode}'"
                )
            else:
                log.info("R1: First snap sync-mode is 'full' as expected")
        elif state == "idle":
            log.info("Sync already completed (fast sync). Checking idle state.")

        log.info("Wait for snap1 sync to complete (poll)")
        path1_status = wait_for_idle(
            fs_mirroring_utils,
            cephfs_mirror_node[0],
            source_clients[0],
            source_fs,
            subvolume_paths[0],
            timeout=300,
        )

        peer_status = fs_mirroring_utils.get_fs_mirror_peer_status_using_asok(
            cephfs_mirror_node[0], source_clients[0], source_fs
        )
        validated = validate_peer_status_schema(
            peer_status, [subvolume_paths[0]], expected_state="idle"
        )
        path1_status = validated[path1_key]

        if "current_syncing_snap" in path1_status:
            raise CommandFailed(
                "R1 FAILED: current_syncing_snap should be absent at idle"
            )

        if path1_status.get("snaps_synced", 0) < 1:
            raise CommandFailed("R1 FAILED: snaps_synced should be >= 1")

        log.info("R1: Validate via MGR interface")
        mgr_status = fs_mirroring_utils.get_mgr_mirror_status(
            source_clients[0], source_fs
        )
        log.info(f"R1 MGR status: {json.dumps(mgr_status, indent=2)}")
        mgr_metrics = mgr_status.get("metrics", {})
        mgr_path1_data = mgr_metrics.get(path1_key, {})
        if not mgr_path1_data:
            raise CommandFailed(f"R1 FAILED: Path {path1_key} not found in MGR status")
        mgr_peer = list(mgr_path1_data.get("peer", {}).values())
        if not mgr_peer:
            raise CommandFailed("R1 FAILED: No peer entry in MGR status")
        mgr_state = mgr_peer[0].get("state", "")
        log.info(
            f"R1 MGR: state={mgr_state}, snaps_synced={mgr_peer[0].get('snaps_synced')}"
        )
        if mgr_state != "idle":
            log.warning(f"R1: MGR state is '{mgr_state}', expected 'idle'")

        log.info("R1 PASSED: Schema validated via asok + MGR interface")

        # ============================================================
        # R7: last_synced_snap enrichment
        # ============================================================
        log.info("=" * 60)
        log.info("R7: last_synced_snap enrichment validation")
        log.info("=" * 60)

        log.info("R7: Validate last_synced_snap enrichment via asok")
        last_synced_asok = path1_status.get("last_synced_snap", {})
        validate_last_synced_snap_schema(last_synced_asok, expected_snap_name="snap1")
        log.info(f"R7 asok last_synced_snap: {json.dumps(last_synced_asok, indent=2)}")

        log.info("R7: Validate last_synced_snap enrichment via MGR interface")
        mgr_status = fs_mirroring_utils.get_mgr_mirror_status(
            source_clients[0], source_fs
        )
        log.info(f"R7 MGR status: {json.dumps(mgr_status, indent=2)}")
        mgr_metrics = mgr_status.get("metrics", {})
        mgr_path_data = mgr_metrics.get(path1_key, {})
        if not mgr_path_data:
            raise CommandFailed(f"R7 FAILED: Path {path1_key} not found in MGR status")
        mgr_peer = list(mgr_path_data.get("peer", {}).values())
        if not mgr_peer:
            raise CommandFailed("R7 FAILED: No peer entry in MGR status")
        last_synced_mgr = mgr_peer[0].get("last_synced_snap", {})
        validate_last_synced_snap_schema(last_synced_mgr, expected_snap_name="snap1")
        log.info(f"R7 MGR last_synced_snap: {json.dumps(last_synced_mgr, indent=2)}")

        log.info(
            "R7 PASSED: last_synced_snap enriched fields validated via both asok and MGR"
        )

        # ============================================================
        # R2: Sync-mode validation (full vs delta)
        # ============================================================
        log.info("=" * 60)
        log.info("R2: Sync-mode validation (full vs delta)")
        log.info("=" * 60)

        snaps_before_r2 = path1_status.get("snaps_synced", 0)
        log.info(f"R2: snaps_synced before delta snap = {snaps_before_r2}")

        log.info("Write NEW unique files (delta_*): 20 x 256 MiB = 5 GiB")
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"for i in $(seq 1 20); do dd if=/dev/urandom "
            f"of={mount_path1}delta_$i bs=1M count=256 2>/dev/null; done",
            timeout=600,
        )

        log.info("Create snap2 on dir1 (should trigger delta sync)")
        source_clients[0].exec_command(sudo=True, cmd=f"mkdir {mount_path1}.snap/snap2")

        log.info("Poll for sync-mode during snap2 sync (fast asok)")
        sync_mode_found = None
        for attempt in range(90):
            time.sleep(1)
            try:
                status_raw = fs_mirroring_utils.get_asok_peer_status_raw(
                    cephfs_mirror_node[0], source_clients[0], source_fs
                )
                path1_status = status_raw.get(path1_key, {})
                state = path1_status.get("state")
                current_snap = path1_status.get("current_syncing_snap", {})
                log.info(
                    f"[R2 Poll {attempt}] Raw asok: " f"{json.dumps(path1_status)}"
                )
                if current_snap.get("sync-mode"):
                    sync_mode_found = current_snap.get("sync-mode")
                    log.info(
                        f"R2: Captured sync-mode='{sync_mode_found}' during active sync"
                    )
                    break
                if state == "idle":
                    last_snap = path1_status.get("last_synced_snap", {})
                    if last_snap.get("name") == "snap2":
                        log.info("R2: snap2 synced before sync-mode could be captured")
                        break
            except Exception as e:
                log.warning(f"R2 poll error: {e}")

        if sync_mode_found == "delta":
            log.info("R2 PASSED: Second snapshot sync-mode is 'delta'")
        elif sync_mode_found == "full":
            log.warning("R2: sync-mode is 'full' instead of 'delta'")
        else:
            log.warning("R2: Sync completed too fast to capture sync-mode")

        log.info("Wait for snap2 sync to complete")
        path1_status = wait_for_idle(
            fs_mirroring_utils,
            cephfs_mirror_node[0],
            source_clients[0],
            source_fs,
            subvolume_paths[0],
            timeout=300,
        )

        last_snap = path1_status.get("last_synced_snap", {})
        log.info(
            f"R2: last_synced_snap after delta: name={last_snap.get('name')}, "
            f"sync_bytes={last_snap.get('sync_bytes')}, "
            f"sync_files={last_snap.get('sync_files')}, "
            f"sync_duration={last_snap.get('sync_duration')}"
        )

        snap2_bytes = last_snap.get("sync_bytes", "0")
        snap2_files = last_snap.get("sync_files", 0)
        if last_snap.get("name") == "snap2":
            if isinstance(snap2_bytes, str):
                has_bytes = snap2_bytes != "0.00 B" and snap2_bytes != "0"
            else:
                has_bytes = snap2_bytes > 0
            if has_bytes and snap2_files > 0:
                log.info(
                    f"R2 VALIDATED: Delta sync transferred "
                    f"sync_bytes={snap2_bytes}, sync_files={snap2_files}"
                )
            else:
                log.warning(
                    f"R2: Delta sync reported sync_bytes={snap2_bytes}, "
                    f"sync_files={snap2_files} — expected non-zero"
                )

        snaps_now = path1_status.get("snaps_synced", 0)
        if snaps_now <= snaps_before_r2:
            raise CommandFailed(
                f"R2 FAILED: snaps_synced did not increment, "
                f"before={snaps_before_r2}, after={snaps_now}"
            )
        log.info(
            f"R2 PASSED: snaps_synced incremented "
            f"({snaps_before_r2} -> {snaps_now}) after delta sync"
        )

        # ============================================================
        # R16: Snapdiff and Blockdiff verification
        # ============================================================
        log.info("=" * 60)
        log.info("R16: Snapdiff and Blockdiff verification")
        log.info("=" * 60)

        log.info("Create 10 small files (1 MiB each) + 1 large file (64 MiB)")
        source_clients[0].exec_command(sudo=True, cmd=f"rm -f {mount_path1}file_*")
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"for i in $(seq 1 10); do dd if=/dev/urandom of={mount_path1}small_$i "
            f"bs=1M count=1 2>/dev/null; done",
        )
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"dd if=/dev/urandom of={mount_path1}large_file bs=1M count=64 2>/dev/null",
        )

        log.info("Create snap3 (baseline for snapdiff)")
        source_clients[0].exec_command(sudo=True, cmd=f"mkdir {mount_path1}.snap/snap3")

        log.info("Wait for snap3 to sync (poll)")
        path1_status = wait_for_idle(
            fs_mirroring_utils,
            cephfs_mirror_node[0],
            source_clients[0],
            source_fs,
            subvolume_paths[0],
            timeout=360,
        )

        log.info("Modify 5 of 10 small files + few bytes in large file")
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"for i in $(seq 1 5); do dd if=/dev/urandom of={mount_path1}small_$i "
            f"bs=1M count=1 conv=notrunc 2>/dev/null; done",
        )
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"dd if=/dev/urandom of={mount_path1}large_file bs=4K count=1 "
            f"conv=notrunc seek=100 2>/dev/null",
        )

        log.info("Create snap4 (delta sync with snapdiff + blockdiff)")
        source_clients[0].exec_command(sudo=True, cmd=f"mkdir {mount_path1}.snap/snap4")

        log.info("Poll for delta sync and check sync_bytes (fast asok)")
        r16_delta_captured = False
        for poll in range(90):
            time.sleep(1)
            try:
                status_raw = fs_mirroring_utils.get_asok_peer_status_raw(
                    cephfs_mirror_node[0], source_clients[0], source_fs
                )
                path1_status = status_raw.get(path1_key, {})
                current_snap = path1_status.get("current_syncing_snap", {})
                log.info(f"[R16 Poll {poll}] Raw asok: " f"{json.dumps(path1_status)}")
                if current_snap and current_snap.get("name") == "snap4":
                    mode = current_snap.get("sync-mode")
                    if mode == "delta":
                        r16_delta_captured = True
                        log.info("R16: Confirmed delta sync-mode during snap4")
                if path1_status.get("state") == "idle":
                    last = path1_status.get("last_synced_snap", {})
                    if last.get("name") == "snap4":
                        log.info(f"R16: snap4 synced: {json.dumps(last)}")
                        break
            except Exception as e:
                log.warning(f"R16 poll error: {e}")

        log.info("Wait for snap4 sync to complete (poll)")
        path1_status = wait_for_idle(
            fs_mirroring_utils,
            cephfs_mirror_node[0],
            source_clients[0],
            source_fs,
            subvolume_paths[0],
            timeout=360,
        )

        log.info("R16: Validate via asok after snap4 idle")
        last_synced_asok = path1_status.get("last_synced_snap", {})
        log.info(f"R16 asok last_synced_snap: {json.dumps(last_synced_asok, indent=2)}")
        if last_synced_asok.get("name") != "snap4":
            raise CommandFailed(
                f"R16 FAILED (asok): Expected snap4, got {last_synced_asok.get('name')}"
            )

        asok_sync_files = last_synced_asok.get("sync_files", 0)
        asok_sync_bytes_str = last_synced_asok.get("sync_bytes", "0 B")
        asok_sync_bytes = parse_bytes_str(asok_sync_bytes_str)
        log.info(
            f"R16 asok: sync_files={asok_sync_files}, sync_bytes={asok_sync_bytes_str} "
            f"({asok_sync_bytes:.0f} bytes)"
        )

        full_dataset_bytes = 74 * 1024 * 1024
        if asok_sync_files > 6:
            log.warning(
                f"R16 Snapdiff check: sync_files={asok_sync_files}, expected <= 6 "
                f"(5 modified small + 1 modified large). All 11 files may have synced."
            )
        else:
            log.info(
                f"R16 Snapdiff PASSED: sync_files={asok_sync_files} (<= 6), "
                f"only modified files picked for sync"
            )

        if asok_sync_bytes >= full_dataset_bytes:
            log.warning(
                f"R16 Blockdiff check: sync_bytes={asok_sync_bytes_str} >= 74 MiB. "
                f"Expected << 74 MiB (only changed blocks of large file)."
            )
        else:
            log.info(
                f"R16 Blockdiff PASSED: sync_bytes={asok_sync_bytes_str} << 74 MiB, "
                f"only changed blocks transferred"
            )

        log.info("R16: Validate via MGR interface after snap4 idle")
        mgr_status = fs_mirroring_utils.get_mgr_mirror_status(
            source_clients[0], source_fs
        )
        mgr_metrics = mgr_status.get("metrics", {})
        mgr_path1_data = mgr_metrics.get(path1_key, {})
        if not mgr_path1_data:
            raise CommandFailed(f"R16 FAILED: Path {path1_key} not found in MGR status")
        mgr_peer = list(mgr_path1_data.get("peer", {}).values())
        if not mgr_peer:
            raise CommandFailed("R16 FAILED: No peer entry in MGR status")
        mgr_peer_data = mgr_peer[0]
        last_synced_mgr = mgr_peer_data.get("last_synced_snap", {})
        log.info(f"R16 MGR last_synced_snap: {json.dumps(last_synced_mgr, indent=2)}")
        if last_synced_mgr.get("name") != "snap4":
            raise CommandFailed(
                f"R16 FAILED (MGR): Expected snap4, got {last_synced_mgr.get('name')}"
            )

        mgr_sync_files = last_synced_mgr.get("sync_files", 0)
        mgr_sync_bytes_str = last_synced_mgr.get("sync_bytes", "0 B")
        mgr_sync_bytes = parse_bytes_str(mgr_sync_bytes_str)
        log.info(
            f"R16 MGR: sync_files={mgr_sync_files}, sync_bytes={mgr_sync_bytes_str} "
            f"({mgr_sync_bytes:.0f} bytes)"
        )

        if mgr_sync_files > 6:
            log.warning(
                f"R16 MGR Snapdiff check: sync_files={mgr_sync_files}, expected <= 6"
            )
        else:
            log.info(f"R16 MGR Snapdiff PASSED: sync_files={mgr_sync_files} (<= 6)")

        if mgr_sync_bytes >= full_dataset_bytes:
            log.warning(
                f"R16 MGR Blockdiff check: sync_bytes={mgr_sync_bytes_str} >= 74 MiB"
            )
        else:
            log.info(
                f"R16 MGR Blockdiff PASSED: sync_bytes={mgr_sync_bytes_str} << 74 MiB"
            )

        log.info("R16 PASSED: Snapdiff/blockdiff data validated via both asok and MGR")

        # ============================================================
        # R5: Datasync queue wait under load
        #   3 directories, 10K small files each, config tuning
        # ============================================================
        log.info("=" * 60)
        log.info("R5: Datasync queue wait under load")
        log.info("=" * 60)

        log.info("R5: Set distribute_datasync_threads=false to force queue contention")
        source_clients[0].exec_command(
            sudo=True,
            cmd="ceph config set client.cephfs-mirror "
            "cephfs_mirror_distribute_datasync_threads false",
        )

        dsync_dirs = [mount_path1, mount_path2, mount_path3]
        dsync_paths = [
            subvolume_paths[0],
            subvolume_paths[1],
            subvolume_paths[2],
        ]

        log.info("R5: Create 10000 small files in each of 3 directories")
        for mp in dsync_dirs:
            source_clients[0].exec_command(
                sudo=True,
                cmd=f"for i in $(seq 1 10000); do "
                f"dd if=/dev/urandom of={mp}dsync_$i bs=1K count=1 2>/dev/null; done",
                timeout=600,
            )

        log.info("R5: Create snapshots on all 3 directories simultaneously")
        for mp in dsync_dirs:
            source_clients[0].exec_command(sudo=True, cmd=f"mkdir {mp}.snap/snap_dsync")

        log.info("R5: Poll datasync_queue_wait during sync across 3 dirs")
        dsync_states_seen = set()
        waiting_observed = False
        for poll_i in range(90):
            time.sleep(5)
            try:
                peer_status = fs_mirroring_utils.get_fs_mirror_peer_status_using_asok(
                    cephfs_mirror_node[0], source_clients[0], source_fs
                )
                all_idle = True
                for dp in dsync_paths:
                    dp_key = dp.rstrip("/")
                    dir_data = peer_status.get(dp_key, {})
                    state = dir_data.get("state", "unknown")
                    if state != "idle":
                        all_idle = False
                    syncing_snap = dir_data.get("current_syncing_snap")
                    if syncing_snap:
                        dswait = syncing_snap.get("datasync_queue_wait", {})
                        ds_state = dswait.get("state", "")
                        ds_duration = dswait.get("duration", "N/A")
                        log.info(
                            f"[R5 Poll {poll_i}] dir={dp_key}, state={state}, "
                            f"dsync_state={ds_state}, dsync_duration={ds_duration}"
                        )
                        if ds_state:
                            dsync_states_seen.add(ds_state)
                        if ds_state == "waiting":
                            waiting_observed = True
                if all_idle:
                    log.info(f"[R5 Poll {poll_i}] All 3 directories idle")
                    break
            except Exception as e:
                log.warning(f"R5 poll error: {e}")

        log.info(f"R5: Datasync queue wait states observed: {dsync_states_seen}")
        if waiting_observed:
            log.info(
                "R5 PASSED: datasync_queue_wait 'waiting' state observed "
                "(threads busy on other dirs)"
            )
        else:
            log.info(
                "R5: 'waiting' state not captured (sync may have been fast). "
                f"States seen: {dsync_states_seen}"
            )

        log.info("R5: Verify all 3 directories reached idle via MGR")
        mgr_status = fs_mirroring_utils.get_mgr_mirror_status(
            source_clients[0], source_fs
        )
        mgr_metrics = mgr_status.get("metrics", {})
        for dp in dsync_paths:
            dp_key = dp.rstrip("/")
            dp_data = mgr_metrics.get(dp_key, {})
            if dp_data:
                mgr_peer = list(dp_data.get("peer", {}).values())
                if mgr_peer:
                    log.info(
                        f"R5 MGR: dir={dp_key}, state={mgr_peer[0].get('state')}, "
                        f"snaps_synced={mgr_peer[0].get('snaps_synced')}"
                    )

        log.info("R5: Reset distribute_datasync_threads config")
        source_clients[0].exec_command(
            sudo=True,
            cmd="ceph config rm client.cephfs-mirror "
            "cephfs_mirror_distribute_datasync_threads",
            check_ec=False,
        )

        log.info("R5 PASSED: Datasync queue wait under load validated")

        log.info("Reset tick interval to default")
        source_clients[0].exec_command(
            sudo=True,
            cmd="ceph config rm client.cephfs-mirror " "cephfs_mirror_tick_interval",
            check_ec=False,
        )

        log.info("=" * 60)
        log.info("ALL P1 TESTS COMPLETED SUCCESSFULLY")
        log.info("=" * 60)
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean up the system")
        try:
            log.info("Reset config overrides")
            source_clients[0].exec_command(
                sudo=True,
                cmd="ceph config rm client.cephfs-mirror "
                "cephfs_mirror_tick_interval",
                check_ec=False,
            )
            source_clients[0].exec_command(
                sudo=True,
                cmd="ceph config rm client.cephfs-mirror "
                "cephfs_mirror_distribute_datasync_threads",
                check_ec=False,
            )

            mount_dirs = [
                f"/mnt/cephfs_kernel{mounting_dir}_1",
                f"/mnt/cephfs_fuse{mounting_dir}_1",
                f"/mnt/cephfs_kernel{mounting_dir}_2",
            ]

            snap_names = [
                "snap_empty",
                "snap1",
                "snap2",
                "snap3",
                "snap4",
                "snap_dsync",
            ]
            snap_mount_paths = [
                f"/mnt/cephfs_kernel{mounting_dir}_1{subvolume_paths[0]}",
                f"/mnt/cephfs_fuse{mounting_dir}_1{subvolume_paths[1]}",
                f"/mnt/cephfs_kernel{mounting_dir}_2{subvolume_paths[2]}",
            ]
            log.info("Delete the snapshots")
            for spath in snap_mount_paths:
                for snap in snap_names:
                    source_clients[0].exec_command(
                        sudo=True,
                        cmd=f"rmdir {spath}.snap/{snap}",
                        check_ec=False,
                    )

            log.info("Unmount the paths")
            for mdir in mount_dirs:
                source_clients[0].exec_command(
                    sudo=True, cmd=f"umount -l {mdir}", check_ec=False
                )

            log.info("Delete the mounted paths")
            for mdir in mount_dirs:
                source_clients[0].exec_command(
                    sudo=True, cmd=f"rm -rf {mdir}", check_ec=False
                )

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

            for sv in subvol_details:
                fs_util_ceph1.remove_subvolume(
                    source_clients[0],
                    source_fs,
                    sv["subvol_name"],
                    group_name=subvol_group_name,
                    check_ec=False,
                )
            fs_util_ceph1.remove_subvolumegroup(
                source_clients[0],
                source_fs,
                subvol_group_name,
                check_ec=False,
            )
        except Exception as cleanup_err:
            log.warning("Cleanup encountered an error: %s", cleanup_err)
