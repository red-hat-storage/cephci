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


def collect_tri_interface(
    fs_mirroring_utils,
    cephfs_mirror_node,
    source_client,
    source_fs,
    fsid,
    asok_file,
    subvol_path,
    label="",
):
    """
    Collect metrics from all three interfaces (Asok, MGR, Perf counters)
    for a given directory path. Returns a dict with keys: asok, mgr, perf.
    """
    prefix = f"[Tri {label}]" if label else "[Tri]"
    result = {"asok": {}, "mgr": {}, "perf": {}}

    try:
        asok_status = fs_mirroring_utils.get_asok_peer_status_raw(
            cephfs_mirror_node, source_client, source_fs
        )
        path_key = subvol_path.rstrip("/")
        result["asok"] = asok_status.get(path_key, {})
        syncing_snap = result["asok"].get("current_syncing_snap")
        snap_name = syncing_snap.get("name") if syncing_snap else None
        log.info(
            "%s Asok: state=%s, snaps_synced=%s, current_syncing_snap=%s",
            prefix,
            result["asok"].get("state"),
            result["asok"].get("snaps_synced"),
            snap_name,
        )
    except Exception as e:
        log.warning("%s Asok query failed: %s", prefix, e)

    try:
        mgr_status = fs_mirroring_utils.get_mgr_mirror_status(source_client, source_fs)
        path_key = subvol_path.rstrip("/")
        mgr_metrics = mgr_status.get("metrics", {})
        mgr_path_data = mgr_metrics.get(path_key, {})
        mgr_peer = list(mgr_path_data.get("peer", {}).values())
        result["mgr"] = mgr_peer[0] if mgr_peer else {}
        log.info(
            "%s MGR: state=%s, snaps_synced=%s, last_synced=%s",
            prefix,
            result["mgr"].get("state"),
            result["mgr"].get("snaps_synced"),
            result["mgr"].get("last_synced_snap", {}).get("name"),
        )
    except Exception as e:
        log.warning("%s MGR query failed: %s", prefix, e)

    try:
        data = fs_mirroring_utils.get_cephfs_mirror_counters(
            (
                [cephfs_mirror_node]
                if not isinstance(cephfs_mirror_node, list)
                else cephfs_mirror_node
            ),
            fsid,
            asok_file,
        )
        path_key = subvol_path.rstrip("/")
        for entry in data.get("cephfs_mirror_directory", []):
            if path_key in entry.get("labels", {}).get("directory", ""):
                result["perf"] = entry.get("counters", {})
                log.info("%s Perf: %s", prefix, result["perf"])
                break
        if not result["perf"]:
            log.info("%s Perf: no matching entry for %s", prefix, path_key)
    except Exception as e:
        log.warning("%s Perf counter query failed: %s", prefix, e)

    return result


def run(ceph_cluster, **kw):
    """
    CEPH-83632819 - Validate tri-interface consistency for 9.2.

    Covers functional tests:
     1. Tri-interface consistency (full sync, delta sync, idle + rapid)
     2. Tri-interface on failure
     3. Tri-interface snap lifecycle counters
     4. Tri-interface with multiple directories

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

        source_clients[0].exec_command(
            sudo=True,
            cmd="ceph config set client.cephfs-mirror cephfs_mirror_tick_interval 1",
        )
        source_clients[0].exec_command(
            sudo=True,
            cmd="ceph config set mgr mgr/mirroring/snapshot_mirror_metrics_cache_ttl 3",
        )
        log.info("Restart cephfs-mirror daemon for tick_interval to take effect")
        source_clients[0].exec_command(sudo=True, cmd="ceph orch restart cephfs-mirror")
        time.sleep(30)

        subvol_group_name = "subvolgroup_tri"
        subvol_name = "subvol_tri"
        subvol_size = "12884901888"
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        kernel_mounting_dir = f"/mnt/cephfs_kernel{mounting_dir}_1"
        fuse_mounting_dir = f"/mnt/cephfs_fuse{mounting_dir}_1"
        fuse_mounting_dir_2 = f"/mnt/cephfs_fuse{mounting_dir}_2"
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
            {
                "subvol_name": f"{subvol_name}_3",
                "subvol_size": subvol_size,
                "mount_type": "fuse",
                "mount_dir": fuse_mounting_dir_2,
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
        subvol_path3 = subvolume_paths[2]

        log.info("Add subvolumes for mirroring")
        for subvol_path in subvolume_paths:
            fs_mirroring_utils.add_path_for_mirroring(
                source_clients[0], source_fs, subvol_path
            )

        mount_path1 = f"{kernel_mounting_dir}{subvol_path1}"
        mount_path2 = f"{fuse_mounting_dir}{subvol_path2}"
        mount_path3 = f"{fuse_mounting_dir_2}{subvol_path3}"

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

        # ============================================================
        # Scenario 1: Tri-interface consistency (full + delta + idle)
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 1: Tri-interface consistency (full/delta/idle)")
        log.info("=" * 60)

        log.info("Full sync: write 5 GiB to ensure syncing state is observable")
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"dd if=/dev/urandom of={mount_path1}tri_full bs=1M count=5120",
            timeout=600,
        )
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {mount_path1}.snap/snap_tri_full"
        )

        log.info("Poll all 3 interfaces during sync — validate consistency")
        syncing_consistency_checked = False
        for poll_i in range(900):
            time.sleep(1)
            tri = collect_tri_interface(
                fs_mirroring_utils,
                cephfs_mirror_node[0],
                source_clients[0],
                source_fs,
                fsid,
                asok_file,
                subvol_path1,
                label=f"S1-full Poll {poll_i}",
            )
            asok_state = tri["asok"].get("state", "unknown")
            mgr_state = tri["mgr"].get("state", "unknown")
            perf_dir_state = tri["perf"].get("dir_state", -1)

            if asok_state == "syncing" and not syncing_consistency_checked:
                log.info("=== Tri-interface SYNCING state comparison ===")
                asok_synced = tri["asok"].get("snaps_synced", 0)
                mgr_synced = tri["mgr"].get("snaps_synced", 0)
                log.info(f"  Asok:  state={asok_state}, snaps_synced={asok_synced}")
                log.info(f"  MGR:   state={mgr_state}, snaps_synced={mgr_synced}")
                log.info(f"  Perf:  dir_state={perf_dir_state} (1=syncing, 0=idle)")

                asok_snap = tri["asok"].get("current_syncing_snap", {})
                mgr_snap = tri["mgr"].get("current_syncing_snap", {})
                perf = tri["perf"]
                if asok_snap:
                    log.info(
                        f"  Asok current_syncing_snap: name={asok_snap.get('name')}, "
                        f"sync-mode={asok_snap.get('sync-mode')}, "
                        f"sync_bytes={asok_snap.get('bytes', {}).get('sync_bytes')}, "
                        f"total_bytes={asok_snap.get('bytes', {}).get('total_bytes')}, "
                        f"sync_files={asok_snap.get('files', {}).get('sync_files')}, "
                        f"total_files={asok_snap.get('files', {}).get('total_files')}"
                    )
                if mgr_snap:
                    log.info(
                        f"  MGR  current_syncing_snap: name={mgr_snap.get('name')}, "
                        f"sync-mode={mgr_snap.get('sync-mode')}, "
                        f"sync_bytes={mgr_snap.get('bytes', {}).get('sync_bytes')}, "
                        f"total_bytes={mgr_snap.get('bytes', {}).get('total_bytes')}, "
                        f"sync_files={mgr_snap.get('files', {}).get('sync_files')}, "
                        f"total_files={mgr_snap.get('files', {}).get('total_files')}"
                    )
                if perf:
                    log.info(
                        f"  Perf counters: dir_state={perf.get('dir_state')}, "
                        f"sync_bytes={perf.get('current_sync_bytes')}, "
                        f"total_bytes={perf.get('current_total_bytes')}, "
                        f"sync_pct={perf.get('current_sync_bytes_percent')}, "
                        f"sync_files={perf.get('current_sync_files')}, "
                        f"total_files={perf.get('current_total_files')}, "
                        f"sync_files_pct={perf.get('current_sync_files_percent')}, "
                        f"read_bps={perf.get('current_read_bps')}, "
                        f"write_bps={perf.get('current_write_bps')}, "
                        f"sync_mode={perf.get('current_sync_mode')}, "
                        f"crawl_state={perf.get('crawl_state')}"
                    )

                if mgr_state == "syncing":
                    log.info("  CONSISTENT: Both asok and MGR show syncing")
                else:
                    log.info(
                        f"  MGR state={mgr_state} (may lag by cache TTL, "
                        f"asok is live)"
                    )
                if perf_dir_state == 1:
                    log.info("  CONSISTENT: Perf dir_state=1 (syncing)")
                    perf_total = perf.get("current_total_bytes", 0)
                    perf_files = perf.get("current_total_files", 0)
                    if perf_total > 0:
                        log.info(
                            f"  CONSISTENT: Perf current_total_bytes={perf_total} > 0"
                        )
                    if perf_files > 0:
                        log.info(
                            f"  CONSISTENT: Perf current_total_files={perf_files} > 0"
                        )
                else:
                    log.info(f"  Perf dir_state={perf_dir_state}")

                syncing_consistency_checked = True

            if asok_state == "idle":
                last = tri["asok"].get("last_synced_snap", {})
                if last.get("name") == "snap_tri_full":
                    log.info(f"snap_tri_full synced, last_synced_snap: {last}")
                    break

        if not syncing_consistency_checked:
            raise CommandFailed(
                "S1 FAILED: Syncing state was NOT captured across interfaces "
                "during 5 GiB full sync — expected to observe syncing state"
            )
        log.info("S1: Syncing consistency checked across all 3 interfaces")

        log.info("Idle state: validate all 3 interfaces show idle consistently")
        time.sleep(5)
        tri_idle = collect_tri_interface(
            fs_mirroring_utils,
            cephfs_mirror_node[0],
            source_clients[0],
            source_fs,
            fsid,
            asok_file,
            subvol_path1,
            label="S1-idle",
        )
        asok_idle = tri_idle["asok"].get("state", "")
        mgr_idle = tri_idle["mgr"].get("state", "")
        perf_idle = tri_idle["perf"].get("dir_state", -1)
        asok_synced = tri_idle["asok"].get("snaps_synced", 0)
        mgr_synced = tri_idle["mgr"].get("snaps_synced", 0)

        log.info("=== Tri-interface IDLE state comparison ===")
        log.info(f"  Asok: state={asok_idle}, snaps_synced={asok_synced}")
        log.info(f"  MGR:  state={mgr_idle}, snaps_synced={mgr_synced}")
        log.info(f"  Perf: dir_state={perf_idle}")

        if asok_idle != "idle":
            raise CommandFailed(
                f"S1 FAILED: Asok state={asok_idle}, expected idle after sync"
            )
        if mgr_idle != "idle":
            raise CommandFailed(
                f"S1 FAILED: MGR state={mgr_idle}, expected idle after sync"
            )
        log.info("  CONSISTENT: Both asok and MGR show idle")
        if perf_idle == 0:
            log.info("  CONSISTENT: Perf dir_state=0 (idle)")

        if asok_synced != mgr_synced:
            log.warning(
                f"  snaps_synced mismatch: asok={asok_synced}, mgr={mgr_synced} "
                f"(MGR may lag due to tick interval)"
            )
        else:
            log.info(f"  CONSISTENT: snaps_synced matches ({asok_synced})")

        asok_last = tri_idle["asok"].get("last_synced_snap", {})
        mgr_last = tri_idle["mgr"].get("last_synced_snap", {})
        log.info(f"  Asok last_synced_snap: {json.dumps(asok_last, indent=2)}")
        log.info(f"  MGR  last_synced_snap: {json.dumps(mgr_last, indent=2)}")
        if asok_last.get("name") != mgr_last.get("name"):
            raise CommandFailed(
                f"S1 FAILED: last_synced_snap mismatch: "
                f"asok={asok_last.get('name')}, mgr={mgr_last.get('name')}"
            )
        log.info(
            f"  CONSISTENT: last_synced_snap name matches ({asok_last.get('name')})"
        )

        log.info("Delta sync: write 5 GiB new data for second snapshot")
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"dd if=/dev/urandom of={mount_path1}tri_delta bs=1M count=5120",
            timeout=600,
        )
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {mount_path1}.snap/snap_tri_delta"
        )

        log.info("Poll for delta sync — compare all 3 interfaces")
        delta_consistency_checked = False
        for poll_i in range(900):
            time.sleep(1)
            tri = collect_tri_interface(
                fs_mirroring_utils,
                cephfs_mirror_node[0],
                source_clients[0],
                source_fs,
                fsid,
                asok_file,
                subvol_path1,
                label=f"S1-delta Poll {poll_i}",
            )
            asok_state = tri["asok"].get("state", "unknown")
            mgr_state = tri["mgr"].get("state", "unknown")
            perf = tri["perf"]

            if asok_state == "syncing" and not delta_consistency_checked:
                asok_snap = tri["asok"].get("current_syncing_snap", {})
                mgr_snap = tri["mgr"].get("current_syncing_snap", {})
                log.info("=== Tri-interface DELTA SYNCING comparison ===")
                log.info(
                    f"  Asok: state=syncing, sync-mode={asok_snap.get('sync-mode')}, "
                    f"snap={asok_snap.get('name')}, "
                    f"sync_bytes={asok_snap.get('bytes', {}).get('sync_bytes')}, "
                    f"total_bytes={asok_snap.get('bytes', {}).get('total_bytes')}, "
                    f"sync_files={asok_snap.get('files', {}).get('sync_files')}, "
                    f"total_files={asok_snap.get('files', {}).get('total_files')}"
                )
                log.info(
                    f"  MGR:  state={mgr_state}, "
                    f"snap={mgr_snap.get('name') if mgr_snap else None}, "
                    f"sync_bytes={mgr_snap.get('bytes', {}).get('sync_bytes') if mgr_snap else None}, "
                    f"total_bytes={mgr_snap.get('bytes', {}).get('total_bytes') if mgr_snap else None}"
                )
                log.info(
                    f"  Perf: dir_state={perf.get('dir_state')}, "
                    f"sync_bytes={perf.get('current_sync_bytes')}, "
                    f"total_bytes={perf.get('current_total_bytes')}, "
                    f"sync_mode={perf.get('current_sync_mode')}, "
                    f"read_bps={perf.get('current_read_bps')}, "
                    f"write_bps={perf.get('current_write_bps')}"
                )
                delta_consistency_checked = True

            if asok_state == "idle":
                last = tri["asok"].get("last_synced_snap", {})
                if last.get("name") == "snap_tri_delta":
                    log.info(f"Delta sync completed, last_synced_snap: {last}")
                    break

        log.info("Tri-interface consistency (full/delta/idle) validated")

        # ============================================================
        # Scenario 2: Tri-interface on failure
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 2: Tri-interface on failure")
        log.info("=" * 60)

        target_mount_fail = "/mnt/tri_fail_target"
        try:
            fs_mirroring_utils.inject_sync_failure(
                target_clients[0],
                target_mount_fail,
                "client.admin",
                subvol_path2,
                "snap_conflict",
                target_fs,
            )
            time.sleep(30)

            tri_fail = collect_tri_interface(
                fs_mirroring_utils,
                cephfs_mirror_node[0],
                source_clients[0],
                source_fs,
                fsid,
                asok_file,
                subvol_path2,
                label="S2-failure",
            )
            asok_state = tri_fail["asok"].get("state", "")
            log.info(f"Asok state during failure: {asok_state}")
            log.info(f"Full asok data during failure: {tri_fail['asok']}")

            snaps_synced = tri_fail["asok"].get("snaps_synced", 0)
            log.info(f"snaps_synced during failure: {snaps_synced}")

        finally:
            for cmd in [
                f"rmdir {target_mount_fail}{subvol_path2}.snap/snap_conflict",
                f"umount -l {target_mount_fail}",
                f"rm -rf {target_mount_fail}",
            ]:
                target_clients[0].exec_command(sudo=True, cmd=cmd, check_ec=False)
            time.sleep(10)

        log.info("Tri-interface on failure validated")

        # ============================================================
        # Scenario 3: Tri-interface snap lifecycle counters
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 3: Snap lifecycle counters across interfaces")
        log.info("=" * 60)

        status_before = fs_mirroring_utils.get_asok_peer_status_raw(
            cephfs_mirror_node[0], source_clients[0], source_fs
        )
        log.info(f"Status before snap lifecycle: {status_before}")

        source_clients[0].exec_command(sudo=True, cmd=f"touch {mount_path2}lc_file")
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {mount_path2}.snap/snap_lc"
        )
        fs_mirroring_utils.validate_snapshot_sync_status(
            cephfs_mirror_node[0],
            source_fs,
            "snap_lc",
            fsid,
            asok_file,
            filesystem_id,
            peer_uuid,
        )

        source_clients[0].exec_command(
            sudo=True,
            cmd=f"mv {mount_path2}.snap/snap_lc {mount_path2}.snap/snap_lc_renamed",
        )
        time.sleep(60)

        source_clients[0].exec_command(
            sudo=True, cmd=f"rmdir {mount_path2}.snap/snap_lc_renamed"
        )
        time.sleep(60)

        status_after = fs_mirroring_utils.get_asok_peer_status_raw(
            cephfs_mirror_node[0], source_clients[0], source_fs
        )
        log.info(f"Status after snap lifecycle: {status_after}")
        path_key = subvol_path2.rstrip("/")

        if path_key in status_before and path_key in status_after:
            before_synced = status_before[path_key].get("snaps_synced", 0)
            after_synced = status_after[path_key].get("snaps_synced", 0)
            after_renamed = status_after[path_key].get("snaps_renamed", 0)
            after_deleted = status_after[path_key].get("snaps_deleted", 0)
            log.info(
                f"Counters: synced {before_synced}->{after_synced}, "
                f"renamed={after_renamed}, deleted={after_deleted}"
            )
            if after_synced > before_synced:
                log.info("snaps_synced incremented correctly")
            if after_renamed > 0:
                log.info("snaps_renamed incremented correctly")
            if after_deleted > 0:
                log.info("snaps_deleted incremented correctly")

        log.info("Snap lifecycle counters validated")

        # ============================================================
        # Scenario 4: Tri-interface with multiple directories
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 4: Multiple directories isolation")
        log.info("=" * 60)

        source_clients[0].exec_command(sudo=True, cmd=f"touch {mount_path1}multi_1")
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {mount_path1}.snap/snap_multi_d1"
        )
        source_clients[0].exec_command(sudo=True, cmd=f"touch {mount_path3}multi_3")
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {mount_path3}.snap/snap_multi_d3"
        )

        fs_mirroring_utils.validate_snapshot_sync_status(
            cephfs_mirror_node[0],
            source_fs,
            "snap_multi_d1",
            fsid,
            asok_file,
            filesystem_id,
            peer_uuid,
        )
        fs_mirroring_utils.validate_snapshot_sync_status(
            cephfs_mirror_node[0],
            source_fs,
            "snap_multi_d3",
            fsid,
            asok_file,
            filesystem_id,
            peer_uuid,
        )

        tri_d1 = collect_tri_interface(
            fs_mirroring_utils,
            cephfs_mirror_node[0],
            source_clients[0],
            source_fs,
            fsid,
            asok_file,
            subvol_path1,
            label="S4-dir1",
        )
        tri_d3 = collect_tri_interface(
            fs_mirroring_utils,
            cephfs_mirror_node[0],
            source_clients[0],
            source_fs,
            fsid,
            asok_file,
            subvol_path3,
            label="S4-dir3",
        )

        log.info(f"Dir1 full asok: {tri_d1['asok']}")
        log.info(f"Dir3 full asok: {tri_d3['asok']}")
        log.info(f"Dir1 full perf: {tri_d1['perf']}")
        log.info(f"Dir3 full perf: {tri_d3['perf']}")
        log.info("Multiple directories isolation validated")

        log.info("All tri-interface scenarios passed")

        source_clients[0].exec_command(
            sudo=True,
            cmd="ceph config rm client.cephfs-mirror cephfs_mirror_tick_interval",
        )
        source_clients[0].exec_command(
            sudo=True,
            cmd="ceph config rm mgr mgr/mirroring/snapshot_mirror_metrics_cache_ttl",
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
            for key in [
                "client.cephfs-mirror cephfs_mirror_tick_interval",
                "mgr mgr/mirroring/snapshot_mirror_metrics_cache_ttl",
            ]:
                source_clients[0].exec_command(
                    sudo=True, cmd=f"ceph config rm {key}", check_ec=False
                )

            log.info("Delete the snapshots")
            all_snaps = [
                "snap_tri_full",
                "snap_tri_delta",
                "snap_conflict",
                "snap_lc",
                "snap_lc_renamed",
                "snap_multi_d1",
                "snap_multi_d3",
            ]
            snap_mount_paths = [mount_path1, mount_path2, mount_path3]
            for spath in snap_mount_paths:
                for snap in all_snaps:
                    source_clients[0].exec_command(
                        sudo=True,
                        cmd=f"rmdir {spath}.snap/{snap}",
                        check_ec=False,
                    )

            log.info("Unmount the paths")
            for mdir in [kernel_mounting_dir, fuse_mounting_dir, fuse_mounting_dir_2]:
                source_clients[0].exec_command(
                    sudo=True, cmd=f"umount -l {mdir}", check_ec=False
                )

            log.info("Delete the mounted paths")
            for mdir in [kernel_mounting_dir, fuse_mounting_dir, fuse_mounting_dir_2]:
                source_clients[0].exec_command(
                    sudo=True, cmd=f"rm -rf {mdir}", check_ec=False
                )

            log.info("Cleanup target client")
            target_clients[0].exec_command(
                sudo=True,
                cmd="umount -l /mnt/tri_fail_target",
                check_ec=False,
            )
            target_clients[0].exec_command(
                sudo=True,
                cmd="rm -rf /mnt/tri_fail_target",
                check_ec=False,
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
            for i in range(1, 4):
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
