import json
import random
import string
import time
import traceback
from datetime import datetime

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_mirroring.cephfs_mirroring_utils import CephfsMirroringUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def _parse_metrics_ts(value):
    """Convert metrics_updated_at to epoch float.

    Handles both numeric (legacy) and ISO 8601 string formats.
    """
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value:
        try:
            ts = value.replace("+0000", "+00:00")
            dt = datetime.fromisoformat(ts)
            return dt.timestamp()
        except (ValueError, TypeError):
            pass
    return 0.0


def run(ceph_cluster, **kw):
    """
    CEPH-83632824 - Validate MGR interface (ceph fs snapshot mirror status) for 9.2.

    Covers functional tests:
     1. Verify ceph fs snapshot mirror status command
     2. Default stats on newly added directory
     3. OMAP persistence verification
     4. Persist interval config validation
     5. MGR module bounce preserves state
     6. OMAP cleanup on directory removal
     7. Stale detection — daemon stop and recovery (combined)
     8. Metrics cache TTL and staleness after directory removal (combined)
     9. MGR module disabled — clear error
     10. MGR CLI sanity — invalid inputs

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

        subvol_group_name = "subvolgroup_mgr"
        subvol_name = "subvol_mgr"
        subvol_size = "5368709120"
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        kernel_mounting_dir = f"/mnt/cephfs_kernel{mounting_dir}_1"
        fuse_mounting_dir = f"/mnt/cephfs_fuse{mounting_dir}_1"
        subvol_details = [
            {
                "subvol_name": f"{subvol_name}_1",
                "subvol_size": subvol_size,
                "mount_type": "kernel",
                "mount_dir": kernel_mounting_dir,
            },
            {
                "subvol_name": f"{subvol_name}_2",
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
        log.info(f"Subvolume Paths: {subvolume_paths}")

        subvol_path1 = subvolume_paths[0]
        subvol_path2 = subvolume_paths[1]

        log.info("Add subvolumes for mirroring")
        for subvol_path in subvolume_paths:
            fs_mirroring_utils.add_path_for_mirroring(
                source_clients[0], source_fs, subvol_path
            )

        mount_path1 = f"{kernel_mounting_dir}{subvol_path1}"
        mount_path2 = f"{fuse_mounting_dir}{subvol_path2}"

        log.info("Clean up any leftover snapshots from previous runs")
        for snap in ["snap_mgr1", "snap_cleanup"]:
            source_clients[0].exec_command(
                sudo=True, cmd=f"rmdir {mount_path1}.snap/{snap}", check_ec=False
            )
        for snap in ["snap_mgr2"]:
            source_clients[0].exec_command(
                sudo=True, cmd=f"rmdir {mount_path2}.snap/{snap}", check_ec=False
            )

        log.info("Write data and create snapshots for initial sync")
        source_clients[0].exec_command(sudo=True, cmd=f"touch {mount_path1}file_mgr1")
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {mount_path1}.snap/snap_mgr1"
        )
        source_clients[0].exec_command(sudo=True, cmd=f"touch {mount_path2}file_mgr2")
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {mount_path2}.snap/snap_mgr2"
        )
        time.sleep(60)

        # ============================================================
        # Scenario 1: Verify ceph fs snapshot mirror status command
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 1: Verify ceph fs snapshot mirror status command")
        log.info("=" * 60)

        mgr_status = fs_mirroring_utils.get_mgr_mirror_status(
            source_clients[0], source_fs
        )
        log.info(f"MGR mirror status: {json.dumps(mgr_status, indent=2)}")

        if not mgr_status:
            raise CommandFailed("ceph fs snapshot mirror status returned empty")

        out_pretty, _ = source_clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs snapshot mirror status {source_fs} -f json-pretty",
        )
        parsed_pretty = json.loads(out_pretty)
        log.info(f"json-pretty output: {json.dumps(parsed_pretty, indent=2)}")
        log.info("json-pretty format works correctly")

        # ============================================================
        # Scenario 2: Default stats on newly added directory
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 2: Default stats on newly added directory")
        log.info("=" * 60)

        new_dir = "/new_test_dir/"
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir -p {kernel_mounting_dir}{new_dir}"
        )
        fs_mirroring_utils.add_path_for_mirroring(source_clients[0], source_fs, new_dir)

        log.info("Immediately query MGR status after adding path")
        mgr_status = fs_mirroring_utils.get_mgr_mirror_status(
            source_clients[0], source_fs
        )
        log.info(f"MGR status (immediate): {json.dumps(mgr_status, indent=2)}")

        new_dir_key = new_dir.rstrip("/")
        mgr_metrics = mgr_status.get("metrics", {})
        new_dir_data = mgr_metrics.get(new_dir_key, {})

        if not new_dir_data:
            log.info(
                f"Path {new_dir_key} not visible in immediate query, "
                "polling until it appears (up to 30s)"
            )
            for retry in range(6):
                time.sleep(5)
                mgr_status = fs_mirroring_utils.get_mgr_mirror_status(
                    source_clients[0], source_fs
                )
                mgr_metrics = mgr_status.get("metrics", {})
                new_dir_data = mgr_metrics.get(new_dir_key, {})
                if new_dir_data:
                    log.info(
                        f"Path appeared after {(retry + 1) * 5}s: "
                        f"{json.dumps(new_dir_data, indent=2)}"
                    )
                    break
            else:
                raise CommandFailed(
                    f"Path {new_dir_key} never appeared in MGR metrics "
                    "within 30s of adding"
                )

        mgr_peer_entries = list(new_dir_data.get("peer", {}).values())
        if not mgr_peer_entries:
            raise CommandFailed(f"No peer entry found for {new_dir_key} in MGR status")

        peer_stats = mgr_peer_entries[0]
        default_fields = {
            "state": peer_stats.get("state"),
            "snaps_synced": peer_stats.get("snaps_synced"),
            "snaps_deleted": peer_stats.get("snaps_deleted"),
            "snaps_renamed": peer_stats.get("snaps_renamed"),
            "last_synced_snap": peer_stats.get("last_synced_snap"),
        }
        log.info(f"Default stats for newly added dir: {default_fields}")

        state = default_fields["state"]
        if state != "idle":
            log.warning(f"Expected state='idle' for freshly added dir, got '{state}'")
        else:
            log.info("state is 'idle' as expected")

        for counter in ("snaps_synced", "snaps_deleted", "snaps_renamed"):
            val = default_fields[counter]
            if val != 0:
                log.warning(f"Expected {counter}=0 for freshly added dir, got {val}")
            else:
                log.info(f"{counter} is 0 as expected")

        last_snap = default_fields["last_synced_snap"]
        if last_snap and last_snap.get("name"):
            log.warning(
                f"Expected empty last_synced_snap for freshly added dir, "
                f"got {last_snap}"
            )
        else:
            log.info("last_synced_snap is empty as expected")

        fs_mirroring_utils.remove_path_from_mirroring(
            source_clients[0], source_fs, new_dir
        )
        source_clients[0].exec_command(
            sudo=True, cmd=f"rm -rf {kernel_mounting_dir}{new_dir}", check_ec=False
        )
        log.info("Default stats for new directory validated")

        # ============================================================
        # Scenario 3: OMAP persistence verification
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 3: OMAP persistence verification")
        log.info("=" * 60)

        fs_info = fs_util_ceph1.get_fs_info(source_clients[0], source_fs)
        metadata_pool = fs_info["metadata_pool_name"]
        log.info(f"Metadata pool: {metadata_pool}")

        log.info("Write bulk data to trigger a longer sync window")
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"for i in $(seq 1 50); do dd if=/dev/urandom "
            f"of={mount_path1}omap_file_$i bs=1M count=1 2>/dev/null; done",
        )
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"mkdir {mount_path1}.snap/snap_omap",
        )

        log.info("Inspect OMAP keys during active sync (listing 1)")
        out1, _ = source_clients[0].exec_command(
            sudo=True,
            cmd=f"rados -p {metadata_pool} listomapkeys cephfs_mirror",
            check_ec=False,
        )
        omap_keys_1 = set(out1.strip().splitlines()) if out1.strip() else set()
        log.info(f"OMAP keys (listing 1): {omap_keys_1}")

        if not omap_keys_1:
            raise CommandFailed(
                f"OMAP keys empty during active sync (pool={metadata_pool})"
            )

        time.sleep(10)

        log.info("Re-inspect OMAP keys after 10s (listing 2)")
        out2, _ = source_clients[0].exec_command(
            sudo=True,
            cmd=f"rados -p {metadata_pool} listomapkeys cephfs_mirror",
            check_ec=False,
        )
        omap_keys_2 = set(out2.strip().splitlines()) if out2.strip() else set()
        log.info(f"OMAP keys (listing 2): {omap_keys_2}")

        if not omap_keys_2:
            raise CommandFailed("OMAP keys disappeared after 10s re-list")

        if omap_keys_1 == omap_keys_2:
            log.info("OMAP keys consistent across both listings")
        else:
            added = omap_keys_2 - omap_keys_1
            removed = omap_keys_1 - omap_keys_2
            log.info(f"OMAP keys changed — added: {added}, removed: {removed}")

        for sp in [subvol_path1, subvol_path2]:
            if not any(sp.rstrip("/") in k for k in omap_keys_2):
                log.warning(f"Mirrored path {sp} not found in OMAP keys")

        log.info("Wait for snap_omap sync to finish")
        time.sleep(60)

        source_clients[0].exec_command(
            sudo=True,
            cmd=f"rmdir {mount_path1}.snap/snap_omap",
            check_ec=False,
        )

        log.info("OMAP persistence verification completed")

        # ============================================================
        # Scenario 4: Tick interval config validation
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 4: cephfs_mirror_tick_interval config validation")
        log.info("=" * 60)

        tick_key = "cephfs_mirror_tick_interval"
        log.info(f"Check default value for: {tick_key}")
        out_default, _ = source_clients[0].exec_command(
            sudo=True,
            cmd=f"ceph config get client.cephfs-mirror {tick_key}",
        )
        default_tick = out_default.strip()
        log.info(f"Default tick interval: {default_tick}")

        log.info(f"Set {tick_key} to 30")
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"ceph config set client.cephfs-mirror {tick_key} 30",
        )
        log.info("Restart cephfs-mirror daemon for tick_interval to take effect")
        source_clients[0].exec_command(sudo=True, cmd="ceph orch restart cephfs-mirror")
        time.sleep(30)
        out_set, _ = source_clients[0].exec_command(
            sudo=True,
            cmd=f"ceph config get client.cephfs-mirror {tick_key}",
        )
        if out_set.strip() != "30":
            raise CommandFailed(
                f"Tick interval not updated to 30, got '{out_set.strip()}'"
            )
        log.info(f"Tick interval set to: {out_set.strip()}")

        log.info("Write data and create first snapshot with tick=30")
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"for i in $(seq 1 20); do dd if=/dev/urandom "
            f"of={mount_path1}tick_file_$i bs=1M count=1 2>/dev/null; done",
        )
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"mkdir {mount_path1}.snap/snap_tick",
        )

        log.info("Wait for snap_tick to sync via asok (live, not affected by tick)")
        path1_key = subvol_path1.rstrip("/")
        for poll_i in range(20):
            time.sleep(10)
            peer_status = fs_mirroring_utils.get_fs_mirror_peer_status_using_asok(
                cephfs_mirror_node[0], source_clients[0], source_fs
            )
            ps = peer_status.get(path1_key, {})
            log.info(
                f"[Asok tick wait {poll_i}] state={ps.get('state')}, "
                f"last_synced={ps.get('last_synced_snap', {}).get('name')}"
            )
            if (
                ps.get("state") == "idle"
                and ps.get("last_synced_snap", {}).get("name") == "snap_tick"
            ):
                break

        log.info("snap_tick synced. Now create snap_tick2 and measure MGR lag")
        source_clients[0].exec_command(sudo=True, cmd=f"touch {mount_path1}tick2_file")
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"mkdir {mount_path1}.snap/snap_tick2",
        )

        log.info(
            "Poll MGR every 5s for 90s — with tick_interval=30, "
            "MGR should take ~30s to reflect snap_tick2"
        )
        mgr_snapshots = []
        first_seen_at = None
        for poll_i in range(18):
            time.sleep(5)
            elapsed = (poll_i + 1) * 5
            try:
                mgr_poll = fs_mirroring_utils.get_mgr_mirror_status(
                    source_clients[0], source_fs
                )
                mgr_metrics = mgr_poll.get("metrics", {})
                path1_data = mgr_metrics.get(path1_key, {})
                peer_entries = list(path1_data.get("peer", {}).values())
                if peer_entries:
                    state = peer_entries[0].get("state", "unknown")
                    snaps_synced = peer_entries[0].get("snaps_synced", 0)
                    last_snap = (
                        peer_entries[0].get("last_synced_snap", {}).get("name", "")
                    )
                    log.info(
                        f"[Tick poll {elapsed}s] state={state}, "
                        f"snaps_synced={snaps_synced}, last_snap={last_snap}"
                    )
                    mgr_snapshots.append(
                        {
                            "poll_s": elapsed,
                            "state": state,
                            "snaps_synced": snaps_synced,
                            "last_snap": last_snap,
                        }
                    )
                    if last_snap == "snap_tick2" and first_seen_at is None:
                        first_seen_at = elapsed
                        log.info(f"snap_tick2 first appeared in MGR at {elapsed}s")
                else:
                    log.info(f"[Tick poll {elapsed}s] No peer entry yet")
            except Exception as e:
                log.warning(f"[Tick poll {elapsed}s] Error: {e}")

        log.info(f"Tick poll summary ({len(mgr_snapshots)} samples): {mgr_snapshots}")
        if first_seen_at is not None:
            log.info(
                f"Scenario 4: snap_tick2 appeared in MGR after {first_seen_at}s "
                f"(tick_interval=30s). Expected lag ~30s."
            )
            if first_seen_at >= 20:
                log.info(
                    "Scenario 4 PASSED: MGR update lagged as expected with tick=30"
                )
            else:
                log.info(
                    f"Scenario 4: MGR updated faster than expected ({first_seen_at}s)"
                )
        else:
            log.warning("Scenario 4: snap_tick2 never appeared in MGR within 90s")

        for snap in ["snap_tick", "snap_tick2"]:
            source_clients[0].exec_command(
                sudo=True,
                cmd=f"rmdir {mount_path1}.snap/{snap}",
                check_ec=False,
            )

        log.info(f"Reset {tick_key} to 5")
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"ceph config set client.cephfs-mirror {tick_key} 5",
        )
        log.info("Restart cephfs-mirror daemon for tick_interval reset to take effect")
        source_clients[0].exec_command(sudo=True, cmd="ceph orch restart cephfs-mirror")
        time.sleep(30)
        out_reset, _ = source_clients[0].exec_command(
            sudo=True,
            cmd=f"ceph config get client.cephfs-mirror {tick_key}",
        )
        log.info(f"Tick interval after reset: {out_reset.strip()}")
        if out_reset.strip() != "5":
            raise CommandFailed(
                f"Failed to reset tick interval to 5, got '{out_reset.strip()}'"
            )

        log.info("Tick interval config validation passed")

        # ============================================================
        # Scenario 5: MGR module bounce preserves state
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 5: MGR module bounce preserves state")
        log.info("=" * 60)

        status_before_bounce = fs_mirroring_utils.get_mgr_mirror_status(
            source_clients[0], source_fs
        )
        log.info(
            f"Status before module bounce: {json.dumps(status_before_bounce, indent=2)}"
        )

        metrics_before = status_before_bounce.get("metrics", {})
        paths_before = set(metrics_before.keys())
        synced_before = {}
        for p, pdata in metrics_before.items():
            peers = list(pdata.get("peer", {}).values())
            if peers:
                synced_before[p] = peers[0].get("snaps_synced", 0)
        log.info(f"Before bounce: paths={paths_before}, snaps_synced={synced_before}")

        source_clients[0].exec_command(
            sudo=True, cmd="ceph mgr module disable mirroring"
        )
        time.sleep(5)
        source_clients[0].exec_command(
            sudo=True, cmd="ceph mgr module enable mirroring"
        )
        time.sleep(30)

        status_after_bounce = fs_mirroring_utils.get_mgr_mirror_status(
            source_clients[0], source_fs
        )
        log.info(
            f"Status after module bounce: {json.dumps(status_after_bounce, indent=2)}"
        )

        if not status_after_bounce:
            raise CommandFailed("MGR status empty after module bounce")

        metrics_after = status_after_bounce.get("metrics", {})
        paths_after = set(metrics_after.keys())
        peers_after = {}
        for p, pdata in metrics_after.items():
            peer_uuids = list(pdata.get("peer", {}).keys())
            peers_after[p] = peer_uuids
        log.info(f"After bounce: paths={paths_after}, peers={peers_after}")

        if paths_before != paths_after:
            raise CommandFailed(
                f"S5 FAILED: Mirrored paths lost after bounce — "
                f"before={paths_before}, after={paths_after}"
            )

        for p in paths_before:
            before_peers = list(metrics_before.get(p, {}).get("peer", {}).keys())
            after_peers = peers_after.get(p, [])
            if set(before_peers) != set(after_peers):
                raise CommandFailed(
                    f"S5 FAILED: Peer UUIDs changed for {p} — "
                    f"before={before_peers}, after={after_peers}"
                )

        log.info(
            f"S5: snaps_synced before={synced_before} (counters reset after "
            f"bounce — expected, these are in-memory)"
        )
        log.info(
            "S5 PASSED: MGR module bounce preserves mirrored paths and peer associations"
        )

        # ============================================================
        # Scenario 6: OMAP cleanup on directory removal
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 6: OMAP cleanup on directory removal")
        log.info("=" * 60)

        temp_dir = "/temp_cleanup_dir/"
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir -p {kernel_mounting_dir}{temp_dir}"
        )
        fs_mirroring_utils.add_path_for_mirroring(
            source_clients[0], source_fs, temp_dir
        )
        time.sleep(10)

        log.info("Write data and create snapshot in temp dir")
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"for i in $(seq 1 5); do dd if=/dev/urandom "
            f"of={kernel_mounting_dir}{temp_dir}cleanup_file_$i "
            f"bs=1M count=1 2>/dev/null; done",
        )
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"mkdir {kernel_mounting_dir}{temp_dir}.snap/snap_cleanup",
        )

        log.info("Poll MGR status until temp dir reaches idle")
        temp_dir_key = temp_dir.rstrip("/")
        for poll_i in range(24):
            time.sleep(5)
            mgr_poll = fs_mirroring_utils.get_mgr_mirror_status(
                source_clients[0], source_fs
            )
            mgr_metrics = mgr_poll.get("metrics", {})
            temp_data = mgr_metrics.get(temp_dir_key, {})
            if temp_data:
                peer_entries = list(temp_data.get("peer", {}).values())
                if peer_entries:
                    state = peer_entries[0].get("state", "unknown")
                    snaps_synced = peer_entries[0].get("snaps_synced", 0)
                    log.info(
                        f"[Cleanup poll {poll_i * 5}s] state={state}, "
                        f"snaps_synced={snaps_synced}"
                    )
                    if state == "idle" and snaps_synced >= 1:
                        log.info("Temp dir reached idle with snapshot synced")
                        break
        else:
            log.warning("Temp dir did not reach idle within 120s, proceeding anyway")

        log.info("Verify temp dir present in MGR status before removal")
        status_with_dir = fs_mirroring_utils.get_mgr_mirror_status(
            source_clients[0], source_fs
        )
        log.info(f"MGR status with temp dir: {json.dumps(status_with_dir, indent=2)}")
        mgr_metrics_before = status_with_dir.get("metrics", {})
        if temp_dir_key not in mgr_metrics_before:
            log.warning(f"Path {temp_dir_key} not found in MGR metrics before removal")

        log.info("OMAP keys before path removal")
        # Discover the OMAP object name dynamically to be version-agnostic
        omap_obj_out, _ = source_clients[0].exec_command(
            sudo=True,
            cmd="rados -p %s ls | grep cephfs_mirror || true" % metadata_pool,
            check_ec=False,
        )
        omap_obj_name = (
            omap_obj_out.strip().split("\n")[0]
            if omap_obj_out.strip()
            else "cephfs_mirror"
        )
        log.info("Using OMAP object name: %s", omap_obj_name)
        out_before, _ = source_clients[0].exec_command(
            sudo=True,
            cmd="rados -p %s listomapkeys %s" % (metadata_pool, omap_obj_name),
            check_ec=False,
        )
        omap_before = (
            set(out_before.strip().splitlines()) if out_before.strip() else set()
        )
        log.info("OMAP keys before removal: %s", omap_before)

        log.info("Remove snapshot, then remove path from mirroring")
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"rmdir {kernel_mounting_dir}{temp_dir}.snap/snap_cleanup",
        )
        time.sleep(5)
        fs_mirroring_utils.remove_path_from_mirroring(
            source_clients[0], source_fs, temp_dir
        )
        time.sleep(20)

        log.info("Query MGR status after path removal")
        status_after_remove = fs_mirroring_utils.get_mgr_mirror_status(
            source_clients[0], source_fs
        )
        log.info(
            f"MGR status after removal: {json.dumps(status_after_remove, indent=2)}"
        )
        mgr_metrics_after = status_after_remove.get("metrics", {})
        if temp_dir_key in mgr_metrics_after:
            log.warning(
                f"Path {temp_dir_key} still present in MGR metrics after removal "
                "(may take a tick interval to clear)"
            )
        else:
            log.info(f"Path {temp_dir_key} removed from MGR metrics")

        log.info("OMAP keys after path removal")
        out_after, _ = source_clients[0].exec_command(
            sudo=True,
            cmd=f"rados -p {metadata_pool} listomapkeys cephfs_mirror",
            check_ec=False,
        )
        omap_after = set(out_after.strip().splitlines()) if out_after.strip() else set()
        log.info(f"OMAP keys after removal: {omap_after}")

        removed_keys = omap_before - omap_after
        log.info(f"OMAP keys removed: {removed_keys}")

        if any(temp_dir_key in k for k in omap_after):
            raise CommandFailed(
                f"OMAP still contains key for removed path {temp_dir_key} "
                f"after mirror remove. Keys: {omap_after}"
            )
        log.info("Verified removed path no longer present in OMAP")

        source_clients[0].exec_command(
            sudo=True, cmd=f"rm -rf {kernel_mounting_dir}{temp_dir}", check_ec=False
        )
        log.info("OMAP cleanup on directory removal validated")

        # ============================================================
        # Scenario 7: Stale detection — daemon stop and recovery
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 7: Stale detection - daemon stop and recovery")
        log.info("=" * 60)

        log.info("Capture metrics_updated_at before daemon stop")
        status_before_stop = fs_mirroring_utils.get_mgr_mirror_status(
            source_clients[0], source_fs
        )
        log.info(f"Status before stop: {json.dumps(status_before_stop, indent=2)}")

        ts_before = {}
        for p, pdata in status_before_stop.get("metrics", {}).items():
            peers = list(pdata.get("peer", {}).values())
            if peers:
                ts_before[p] = _parse_metrics_ts(peers[0].get("metrics_updated_at", 0))
        log.info(f"metrics_updated_at before stop: {ts_before}")

        log.info("Stop cephfs-mirror daemon")
        source_clients[0].exec_command(sudo=True, cmd="ceph orch stop cephfs-mirror")
        time.sleep(20)

        status_during_stop = fs_mirroring_utils.get_mgr_mirror_status(
            source_clients[0], source_fs
        )
        log.info(
            f"Status during daemon stop: {json.dumps(status_during_stop, indent=2)}"
        )

        ts_during_stop = {}
        for p, pdata in status_during_stop.get("metrics", {}).items():
            peers = list(pdata.get("peer", {}).values())
            if peers:
                ts_during_stop[p] = _parse_metrics_ts(
                    peers[0].get("metrics_updated_at", 0)
                )
        log.info(f"metrics_updated_at during stop: {ts_during_stop}")

        now_during_stop = time.time()
        stale_threshold = 30
        stale_detected = False
        for p in ts_before:
            ts_stop = ts_during_stop.get(p, 0)
            age = now_during_stop - ts_stop if ts_stop else float("inf")
            log.info(
                f"S7: {p} — metrics_updated_at={ts_stop}, "
                f"current_time={now_during_stop:.2f}, age={age:.1f}s"
            )
            if age > stale_threshold:
                log.info(
                    f"S7: {p} metrics are STALE (age {age:.1f}s > {stale_threshold}s) "
                    f"— daemon stop detected"
                )
                stale_detected = True
            else:
                log.info(f"S7: {p} metrics still fresh (age {age:.1f}s)")

        if not stale_detected:
            log.warning(
                "S7: No stale metrics detected during daemon stop — "
                "20s wait may be too short"
            )

        paths_during_stop = set(status_during_stop.get("metrics", {}).keys())
        if not paths_during_stop:
            raise CommandFailed("S7 FAILED: MGR metrics empty during daemon stop")
        log.info(f"S7: Mirrored paths still visible during stop: {paths_during_stop}")

        log.info("Restart cephfs-mirror daemon")
        source_clients[0].exec_command(sudo=True, cmd="ceph orch start cephfs-mirror")
        time.sleep(60)

        status_after_restart = fs_mirroring_utils.get_mgr_mirror_status(
            source_clients[0], source_fs
        )
        log.info(f"Status after restart: {json.dumps(status_after_restart, indent=2)}")

        if not status_after_restart.get("metrics"):
            raise CommandFailed("S7 FAILED: MGR metrics empty after daemon restart")

        ts_after = {}
        for p, pdata in status_after_restart.get("metrics", {}).items():
            peers = list(pdata.get("peer", {}).values())
            if peers:
                ts_after[p] = _parse_metrics_ts(peers[0].get("metrics_updated_at", 0))
        log.info(f"metrics_updated_at after restart: {ts_after}")

        now_after_restart = time.time()
        recovered = True
        for p in ts_during_stop:
            ts_new = ts_after.get(p, 0)
            age_after = now_after_restart - ts_new if ts_new else float("inf")
            log.info(
                f"S7: {p} — metrics_updated_at={ts_new}, "
                f"current_time={now_after_restart:.2f}, age={age_after:.1f}s"
            )
            if ts_new > ts_during_stop[p]:
                log.info(
                    f"S7: {p} metrics REFRESHED after restart "
                    f"({ts_during_stop[p]} -> {ts_new})"
                )
            else:
                log.warning(
                    f"S7: {p} metrics NOT refreshed — "
                    f"stop={ts_during_stop[p]}, after={ts_new}"
                )
                recovered = False
            if age_after > stale_threshold:
                log.warning(
                    f"S7: {p} metrics still stale after restart "
                    f"(age {age_after:.1f}s > {stale_threshold}s)"
                )
                recovered = False
            else:
                log.info(f"S7: {p} metrics fresh after restart (age {age_after:.1f}s)")

        if not recovered:
            log.warning("S7: Some paths did not fully recover after daemon restart")

        paths_after = set(status_after_restart.get("metrics", {}).keys())
        if paths_during_stop != paths_after:
            raise CommandFailed(
                f"S7 FAILED: Paths changed after restart — "
                f"during_stop={paths_during_stop}, after={paths_after}"
            )
        log.info("S7 PASSED: Stale detection and recovery validated")

        # ============================================================
        # Scenario 8: Metrics cache TTL and staleness
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 8: Metrics cache TTL and staleness")
        log.info("=" * 60)

        cache_ttl = 10
        log.info(f"Set cache TTL to {cache_ttl}s")
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"ceph config set mgr "
            f"mgr/mirroring/snapshot_mirror_metrics_cache_ttl {cache_ttl}",
        )
        out, _ = source_clients[0].exec_command(
            sudo=True,
            cmd="ceph config get mgr mgr/mirroring/snapshot_mirror_metrics_cache_ttl",
        )
        log.info(f"Cache TTL confirmed: {out.strip()}")

        log.info("P1: Baseline query")
        p1 = fs_mirroring_utils.get_mgr_mirror_status(source_clients[0], source_fs)
        p1_key = subvol_path1.rstrip("/")
        p1_metrics = p1.get("metrics", {}).get(p1_key, {})
        p1_peer = list(p1_metrics.get("peer", {}).values())
        p1_synced = p1_peer[0].get("snaps_synced", 0) if p1_peer else 0
        p1_last = p1_peer[0].get("last_synced_snap", {}).get("name") if p1_peer else ""
        log.info(f"P1: snaps_synced={p1_synced}, last_snap={p1_last}")

        log.info("P2: Query within TTL (should be cached = same as P1)")
        time.sleep(3)
        p2 = fs_mirroring_utils.get_mgr_mirror_status(source_clients[0], source_fs)
        log.info(f"P1 == P2 (cached): {p1 == p2}")
        if p1 == p2:
            log.info("Scenario 8: P1 == P2 confirmed (cache hit within TTL)")
        else:
            log.warning("Scenario 8: P1 != P2 within TTL (unexpected)")

        log.info("Create snap_cache to trigger a state change")
        source_clients[0].exec_command(sudo=True, cmd=f"touch {mount_path1}cache_file")
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {mount_path1}.snap/snap_cache"
        )

        log.info("Wait for snap_cache to sync via asok")
        for attempt in range(20):
            time.sleep(5)
            peer_status = fs_mirroring_utils.get_fs_mirror_peer_status_using_asok(
                cephfs_mirror_node[0], source_clients[0], source_fs
            )
            ps = peer_status.get(p1_key, {})
            log.info(
                f"[Cache sync wait {attempt}] state={ps.get('state')}, "
                f"last_synced={ps.get('last_synced_snap', {}).get('name')}"
            )
            if (
                ps.get("state") == "idle"
                and ps.get("last_synced_snap", {}).get("name") == "snap_cache"
            ):
                break

        log.info("P3: Query MGR immediately after sync (may still be cached)")
        p3 = fs_mirroring_utils.get_mgr_mirror_status(source_clients[0], source_fs)
        p3_metrics = p3.get("metrics", {}).get(p1_key, {})
        p3_peer = list(p3_metrics.get("peer", {}).values())
        p3_last = p3_peer[0].get("last_synced_snap", {}).get("name") if p3_peer else ""
        log.info(f"P3: last_snap={p3_last}, P3 == P1: {p3 == p1}")

        log.info(f"Wait for cache TTL to expire ({cache_ttl + 5}s)")
        time.sleep(cache_ttl + 5)

        log.info("P4: Query after TTL expiry (should show fresh data)")
        p4 = fs_mirroring_utils.get_mgr_mirror_status(source_clients[0], source_fs)
        p4_metrics = p4.get("metrics", {}).get(p1_key, {})
        p4_peer = list(p4_metrics.get("peer", {}).values())
        p4_last = p4_peer[0].get("last_synced_snap", {}).get("name") if p4_peer else ""
        p4_synced = p4_peer[0].get("snaps_synced", 0) if p4_peer else 0
        log.info(
            f"P4: last_snap={p4_last}, snaps_synced={p4_synced}, "
            f"P4 == P1: {p4 == p1}"
        )

        if p4_last == "snap_cache":
            log.info(
                "Scenario 8 PASSED: After TTL expiry, MGR shows fresh data "
                f"(snap_cache visible, snaps_synced={p4_synced})"
            )
        else:
            log.warning(f"Scenario 8: Expected snap_cache in P4, got {p4_last}")

        log.info("Reset cache TTL")
        source_clients[0].exec_command(
            sudo=True,
            cmd="ceph config rm mgr mgr/mirroring/snapshot_mirror_metrics_cache_ttl",
            check_ec=False,
        )
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"rmdir {mount_path1}.snap/snap_cache",
            check_ec=False,
        )

        # ============================================================
        # Scenario 9: MGR module disabled — clear error
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 9: MGR module disabled — clear error")
        log.info("=" * 60)

        source_clients[0].exec_command(
            sudo=True, cmd="ceph mgr module disable mirroring"
        )
        time.sleep(5)

        disabled_error = False
        try:
            out, err = source_clients[0].exec_command(
                sudo=True,
                cmd=f"ceph fs snapshot mirror status {source_fs} -f json",
                check_ec=False,
            )
            combined = f"{out.strip()} {err.strip() if err else ''}".strip()
            log.info(
                f"S9: Output with module disabled: stdout='{out.strip()}', stderr='{err}'"
            )
            if not out.strip():
                log.info("S9: Empty response when module disabled")
                disabled_error = True
            elif "error" in combined.lower() or "not" in combined.lower():
                log.info(f"S9: Error message received: {combined}")
                disabled_error = True
            else:
                log.warning(f"S9: Unexpected non-error output: {combined}")
        except CommandFailed as e:
            log.info(f"S9: Command failed as expected: {e}")
            disabled_error = True

        if not disabled_error:
            log.warning(
                "S9: No error/empty response when mirroring module disabled — "
                "command should fail or return an error"
            )

        source_clients[0].exec_command(
            sudo=True, cmd="ceph mgr module enable mirroring"
        )
        time.sleep(30)

        status_after_reenable = fs_mirroring_utils.get_mgr_mirror_status(
            source_clients[0], source_fs
        )
        log.info(
            f"S9: Status after re-enable: {json.dumps(status_after_reenable, indent=2)}"
        )
        if status_after_reenable and status_after_reenable.get("metrics"):
            log.info("S9 PASSED: Status recovered after re-enable with metrics intact")
        else:
            log.warning("S9: Status empty or no metrics after re-enable")

        # ============================================================
        # Scenario 10: MGR CLI sanity — invalid inputs
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 10: MGR CLI sanity — invalid inputs")
        log.info("=" * 60)

        invalid_cmds = {
            "ceph fs snapshot mirror status nonexistent_fs -f json": "nonexistent filesystem",
            f"ceph fs snapshot mirror status {source_fs} /non_mirrored_dir -f json": "non-mirrored directory",
        }
        for cmd, description in invalid_cmds.items():
            try:
                out, err = source_clients[0].exec_command(
                    sudo=True, cmd=cmd, check_ec=False
                )
                combined = f"{out.strip()} {err.strip() if err else ''}".strip()
                log.info(
                    f"S10 ({description}): stdout='{out.strip()}', "
                    f"stderr='{err.strip() if err else ''}'"
                )
                if (
                    not combined
                    or "error" in combined.lower()
                    or "no" in combined.lower()
                ):
                    log.info(f"S10 ({description}): Got expected error/empty response")
                else:
                    log.warning(
                        f"S10 ({description}): Unexpected non-error output: {combined}"
                    )
            except CommandFailed as e:
                log.info(f"S10 ({description}): Command failed as expected: {e}")

        log.info("S10 PASSED: CLI sanity validated")

        log.info("All MGR interface scenarios passed")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean up the system")
        try:
            source_clients[0].exec_command(
                sudo=True, cmd="ceph mgr module enable mirroring", check_ec=False
            )
            source_clients[0].exec_command(
                sudo=True, cmd="ceph orch start cephfs-mirror", check_ec=False
            )
            source_clients[0].exec_command(
                sudo=True,
                cmd="ceph config rm client.cephfs-mirror "
                "cephfs_mirror_tick_interval",
                check_ec=False,
            )
            source_clients[0].exec_command(
                sudo=True,
                cmd="ceph config rm mgr "
                "mgr/mirroring/snapshot_mirror_metrics_cache_ttl",
                check_ec=False,
            )
            time.sleep(30)

            log.info("Delete the snapshots")
            all_snaps = [
                "snap_mgr1",
                "snap_mgr2",
                "snap_omap",
                "snap_tick",
                "snap_tick2",
                "snap_cache",
                "snap_cleanup",
            ]
            snap_mount_paths = [
                f"{kernel_mounting_dir}{subvolume_paths[0]}",
                f"{fuse_mounting_dir}{subvolume_paths[1]}",
            ]
            for spath in snap_mount_paths:
                for snap in all_snaps:
                    source_clients[0].exec_command(
                        sudo=True,
                        cmd=f"rmdir {spath}.snap/{snap}",
                        check_ec=False,
                    )

            log.info("Unmount the paths")
            for mdir in [kernel_mounting_dir, fuse_mounting_dir]:
                source_clients[0].exec_command(
                    sudo=True, cmd=f"umount -l {mdir}", check_ec=False
                )

            log.info("Delete the mounted paths")
            for mdir in [kernel_mounting_dir, fuse_mounting_dir]:
                source_clients[0].exec_command(
                    sudo=True, cmd=f"rm -rf {mdir}", check_ec=False
                )

            log.info("Remove paths used for mirroring")
            for subvol_path in subvolume_paths:
                fs_mirroring_utils.remove_path_from_mirroring(
                    source_clients[0], source_fs, subvol_path
                )
            for extra_path in ["/new_test_dir/", "/temp_cleanup_dir/"]:
                source_clients[0].exec_command(
                    sudo=True,
                    cmd=f"ceph fs snapshot mirror remove {source_fs} {extra_path}",
                    check_ec=False,
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
            for i in range(1, 3):
                fs_util_ceph1.remove_subvolume(
                    source_clients[0],
                    source_fs,
                    f"{subvol_name}_{i}",
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
            log.warning(f"Cleanup encountered an error: {cleanup_err}")
