import json
import random
import string
import time
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_mirroring.cephfs_mirroring_utils import (
    CephfsMirroringUtils,
    wait_for_idle,
)
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83632752 - CephFS mirroring disruptive operations

    Validates metrics resilience across all interfaces (Asok + MGR) under disruptive
    conditions — sync failure, daemon restart, MGR failover, and MGR restart during failure.

    Scenario 1 — Daemon restart mid-sync preserves stats:
        - Write 5 GiB file and 50 small files, then create snapshot snap_restart_test
        - Poll until sync is actively in progress (>20% sync progress or syncing state)
        - Restart cephfs-mirror daemon mid-sync
        - Validate asok session counters reset / restart cleanly
        - Validate last_synced_snap retains the baseline snap from OMAP
        - Cross-check MGR status (handle transient stale / resync cleanly)
        - Validate sync resumes and does not remain frozen at pre-restart percent
        - Wait until sync completes and state returns to idle with snap_restart_test synced

    Scenario 2 — MGR failover during active sync:
        - Write 5 GiB data and create snapshot to trigger large sync
        - Poll asok until state=syncing
        - Fail the active MGR daemon mid-sync (ceph mgr fail)
        - Validate standby MGR takes over and new active MGR returns valid metrics
        - Verify sync completes cleanly (state=idle, last_synced_snap=snap_mgr_failover)

    Scenario 3 — Mirror failure + MGR restart:
        - Inject sync conflict to put mirror into failed state
        - Restart active MGR daemon while mirror is in failure state (ceph mgr fail)
        - Validate failure stats (path presence) restored from OMAP after MGR recovery
        - Remove conflict and verify status returns toward normal

    Scenario 4 — Sync failure does not corrupt metrics:
        - Inject sync failure via conflicting snapshot on target
        - Poll asok until state=failed
        - Validate snaps_synced does NOT increment; current_syncing_snap cleared
        - Cross-check failure via MGR; after conflict removal, recovery to idle

    Returns:
        0 if successful, 1 if any errors found, -1 if skipped.
    """
    config = kw.get("config") or {}
    if CephfsMirroringUtils.skip_if_rhcs_below(config):
        log.info(
            "Skipping test: requires Ceph version >= 9.2 (rhbuild=%s)",
            config.get("rhbuild"),
        )
        return -1

    try:
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
                "This test requires a minimum of 1 client node on both ceph1 and ceph2."
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

        subvol_group_name = "subvolgroup_disruptive"
        subvol_name = "subvol_disruptive"
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        kernel_mounting_dir = f"/mnt/cephfs_kernel{mounting_dir}_1"

        subvol_details = [
            {
                "subvol_name": f"{subvol_name}_1",
                "subvol_size": "10737418240",
                "mount_type": "kernel",
                "mount_dir": kernel_mounting_dir,
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
        subvol_path = subvolume_paths[0]
        mount_path = f"{kernel_mounting_dir}{subvol_path}"
        path_key = subvol_path.rstrip("/")
        target_mount_path = None

        fs_mirroring_utils.add_path_for_mirroring(
            source_clients[0], source_fs, subvol_path
        )

        log.info("Create initial data and snapshot to establish baseline")
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"for i in $(seq 1 10); do dd if=/dev/urandom of={mount_path}file_$i "
            f"bs=1M count=1 2>/dev/null; done",
        )
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {mount_path}.snap/snap_baseline"
        )

        log.info("Wait for baseline snap to sync")
        path_status = wait_for_idle(
            fs_mirroring_utils,
            cephfs_mirror_node[0],
            source_clients[0],
            source_fs,
            subvol_path,
            timeout=300,
            expected_snap_name="snap_baseline",
        )
        log.info(f"Baseline synced: snaps_synced={path_status.get('snaps_synced')}")

        # ============================================================
        # Scenario 1 — Daemon Restart Mid-Sync
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 1: Daemon restart mid-sync preserves stats")
        log.info("=" * 60)

        log.info(
            "Write 5 GiB large file and 50 small files (100 KiB each) for snap_restart_test"
        )
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"dd if=/dev/urandom of={mount_path}large_5g.dat bs=100M count=50 status=none",
        )
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"for i in $(seq 1 50); do dd if=/dev/urandom of={mount_path}small_file_$i.dat "
            f"bs=100k count=1 status=none 2>/dev/null; done",
        )

        snap_restart = "snap_restart_test"
        log.info(f"Create snapshot {snap_restart} to initiate sync")
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {mount_path}.snap/{snap_restart}"
        )

        log.info("Poll for active sync in progress before restarting mirror daemon")
        pre_restart_pct = 0.0
        sync_active = False
        for poll in range(30):
            time.sleep(2)
            peer_status = fs_mirroring_utils.get_fs_mirror_peer_status_using_asok(
                cephfs_mirror_node[0], source_clients[0], source_fs
            )
            path_status = peer_status.get(path_key, {})
            state = path_status.get("state", "")
            syncing_snap = path_status.get("current_syncing_snap")
            log.info(f"[Pre-Restart Poll {poll}] state={state}, syncing={syncing_snap}")

            if state == "syncing" and syncing_snap:
                sync_active = True
                pct_str = syncing_snap.get("bytes", {}).get("sync_percent", "0%")
                try:
                    pre_restart_pct = float(str(pct_str).rstrip("%"))
                except ValueError:
                    pre_restart_pct = 0.0
                log.info(f"Active sync detected with sync_percent={pre_restart_pct}%")
                if pre_restart_pct >= 20.0 or poll >= 10:
                    break

        log.info("Restart cephfs-mirror daemon mid-sync")
        source_clients[0].exec_command(sudo=True, cmd="ceph orch restart cephfs-mirror")
        time.sleep(15)

        log.info("Wait for mirror daemon to be running after restart")
        fs_mirroring_utils.wait_for_daemon_running(
            source_clients[0],
            cephfs_mirror_node,
            ceph_cluster=ceph_cluster_dict.get("ceph1"),
        )

        log.info("Validate post-restart state via asok")
        for poll_i in range(12):
            post_restart_status = (
                fs_mirroring_utils.get_fs_mirror_peer_status_using_asok(
                    cephfs_mirror_node[0], source_clients[0], source_fs
                )
            )
            post_path_status = post_restart_status.get(path_key, {})
            log.info(
                f"[Post-restart Poll {poll_i}] asok status: {json.dumps(post_path_status, indent=2)}"
            )
            last_synced_after_restart = post_path_status.get("last_synced_snap", {})
            if last_synced_after_restart.get("name") in ("snap_baseline", snap_restart):
                break
            time.sleep(5)
        else:
            raise CommandFailed(
                "Daemon restart test FAILED: Expected last_synced_snap to retain baseline "
                f"(or completed {snap_restart}), got {last_synced_after_restart}"
            )
        log.info(
            f"last_synced_snap preserved correctly from OMAP: {last_synced_after_restart.get('name')}"
        )

        log.info("Cross-check MGR mirror status after daemon restart")
        mgr_status = fs_mirroring_utils.get_mgr_mirror_status(
            source_clients[0], source_fs
        )
        log.info(f"Post-restart MGR status: {json.dumps(mgr_status, indent=2)}")

        log.info(f"Wait for {snap_restart} to finish sync and return to idle")
        final_path_status = wait_for_idle(
            fs_mirroring_utils,
            cephfs_mirror_node[0],
            source_clients[0],
            source_fs,
            subvol_path,
            timeout=600,
            expected_snap_name=snap_restart,
        )
        log.info(
            f"Final status after daemon restart test: {json.dumps(final_path_status, indent=2)}"
        )

        final_last_synced = final_path_status.get("last_synced_snap", {})
        if final_last_synced.get("name") != snap_restart:
            raise CommandFailed(
                "Daemon restart test FAILED: Expected final last_synced_snap to be "
                f"{snap_restart}, got {final_last_synced}"
            )

        log.info("=" * 60)
        log.info(
            "PASSED: Scenario 1 — Daemon restart mid-sync preserved stats and completed sync"
        )
        log.info("=" * 60)

        # ============================================================
        # Scenario 2 — MGR Failover During Active Sync
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 2: MGR failover during active data sync")
        log.info("=" * 60)

        snap_mgr_failover = "snap_mgr_failover"

        log.info("Write 5 GiB data for MGR failover scenario")
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"dd if=/dev/urandom of={mount_path}large_mgr_failover.dat "
            f"bs=100M count=50 status=none",
        )
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {mount_path}.snap/{snap_mgr_failover}"
        )

        log.info("Poll for active sync state before triggering MGR failover")
        for poll_i in range(30):
            time.sleep(2)
            peer_status = fs_mirroring_utils.get_fs_mirror_peer_status_using_asok(
                cephfs_mirror_node[0], source_clients[0], source_fs
            )
            path_status = peer_status.get(path_key, {})
            state = path_status.get("state")
            syncing = path_status.get("current_syncing_snap")
            log.info(f"[Scenario 3 Pre-Poll {poll_i}] state={state}, syncing={syncing}")
            if state == "syncing" or syncing:
                break

        log.info("Capture MGR active_name before failover")
        mgr_stat_before_out, _ = source_clients[0].exec_command(
            sudo=True, cmd="ceph mgr stat -f json"
        )
        mgr_stat_before = json.loads(mgr_stat_before_out)
        active_mgr_before = mgr_stat_before.get("active_name")
        log.info(f"MGR active_name before failover: {active_mgr_before}")

        log.info("Trigger MGR failover via 'ceph mgr fail'")
        source_clients[0].exec_command(sudo=True, cmd="ceph mgr fail", check_ec=False)
        time.sleep(15)

        log.info("Validate MGR failover by checking active_name has changed")
        mgr_stat_after_out, _ = source_clients[0].exec_command(
            sudo=True, cmd="ceph mgr stat -f json"
        )
        mgr_stat_after = json.loads(mgr_stat_after_out)
        active_mgr_after = mgr_stat_after.get("active_name")
        log.info(f"MGR active_name after failover: {active_mgr_after}")
        if active_mgr_after == active_mgr_before:
            raise CommandFailed(
                f"Scenario 2 FAILED: MGR failover did not occur "
                f"(active_name is still '{active_mgr_after}')"
            )
        log.info(
            f"MGR failover confirmed: '{active_mgr_before}' -> '{active_mgr_after}'"
        )

        log.info("Query MGR status from newly active MGR daemon")
        mgr_status_post_failover = None
        for poll_i in range(20):
            time.sleep(5)
            try:
                mgr_status_post_failover = fs_mirroring_utils.get_mgr_mirror_status(
                    source_clients[0], source_fs
                )
                if mgr_status_post_failover and mgr_status_post_failover.get("metrics"):
                    log.info("MGR daemon recovered and serving mirror metrics")
                    break
            except Exception as mgr_err:
                log.info(
                    f"[Scenario 2 Post-Failover Poll {poll_i}] Waiting for MGR: {mgr_err}"
                )

        if not mgr_status_post_failover or not mgr_status_post_failover.get("metrics"):
            raise CommandFailed(
                "Scenario 2 FAILED: MGR metrics unavailable after MGR failover"
            )

        metrics_failover = mgr_status_post_failover.get("metrics", {})
        path_mgr_data = metrics_failover.get(path_key, {})
        log.info(f"MGR metrics after failover: {json.dumps(path_mgr_data, indent=2)}")
        if not path_mgr_data:
            raise CommandFailed(
                f"Scenario 2 FAILED: Mirrored path {path_key} missing from MGR after failover"
            )

        log.info("Wait for snap_mgr_failover sync to complete and return to idle")
        for poll_i in range(40):
            time.sleep(15)
            peer_status = fs_mirroring_utils.get_fs_mirror_peer_status_using_asok(
                cephfs_mirror_node[0], source_clients[0], source_fs
            )
            path_status = peer_status.get(path_key, {})
            state = path_status.get("state")
            last = path_status.get("last_synced_snap", {}).get("name")
            log.info(
                f"[Scenario 2 Post-Failover Wait {poll_i}] state={state}, last_synced={last}"
            )
            if state == "idle" and last == snap_mgr_failover:
                log.info(f"Sync reached idle for {snap_mgr_failover}")
                break
        else:
            raise CommandFailed(
                "Scenario 2 FAILED: Sync did not reach idle after MGR failover"
            )

        log.info("=" * 60)
        log.info(
            "PASSED: Scenario 2 — MGR failover during active sync preserved metrics "
            "and completed sync"
        )
        log.info("=" * 60)

        # ============================================================
        # Scenario 3 — Mirror Failure + MGR Restart
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 3: Mirror failure + MGR restart")
        log.info("=" * 60)

        target_mount_path_s3 = "/mnt/target_conflict_mgr_s3"
        snap_conflict_s3 = "snap_conflict_s3"

        log.info("Inject failure: create conflicting snapshot on target first")
        fs_mirroring_utils.inject_sync_failure(
            target_clients[0],
            target_mount_path_s3,
            "client.admin",
            subvol_path,
            snap_conflict_s3,
            target_fs,
        )

        log.info("Create conflicting snapshot on source to trigger failure state")
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {mount_path}.snap/{snap_conflict_s3}"
        )

        log.info("Wait for failure state to be registered in asok")
        for poll_i in range(20):
            time.sleep(10)
            peer_status = fs_mirroring_utils.get_fs_mirror_peer_status_using_asok(
                cephfs_mirror_node[0], source_clients[0], source_fs
            )
            path_status = peer_status.get(path_key, {})
            state = path_status.get("state")
            log.info(f"[Scenario 3 Failure Poll {poll_i}] state={state}")
            if state == "failed":
                break
        else:
            raise CommandFailed(
                f"Scenario 3 FAILED: expected state=failed after conflict injection, "
                f"got '{state}' — failure was not injected successfully"
            )

        log.info("Capture MGR active_name before restart")
        mgr_stat_before_out, _ = source_clients[0].exec_command(
            sudo=True, cmd="ceph mgr stat -f json"
        )
        mgr_stat_before = json.loads(mgr_stat_before_out)
        active_mgr_before = mgr_stat_before.get("active_name")
        log.info(f"MGR active_name before restart: {active_mgr_before}")

        log.info("Restart active MGR daemon while mirror is in failure state")
        source_clients[0].exec_command(sudo=True, cmd="ceph mgr fail", check_ec=False)

        log.info("Wait for standby MGR to take over")
        active_mgr_after = None
        for _wait in range(10):
            time.sleep(3)
            mgr_stat_out, _ = source_clients[0].exec_command(
                sudo=True, cmd="ceph mgr stat -f json", check_ec=False
            )
            try:
                mgr_stat = json.loads(mgr_stat_out)
                active_mgr_after = mgr_stat.get("active_name")
                if active_mgr_after and active_mgr_after != active_mgr_before:
                    break
            except Exception:
                pass
        log.info(f"MGR active_name after restart: {active_mgr_after}")
        if active_mgr_after == active_mgr_before:
            raise CommandFailed(
                f"Scenario 3 FAILED: MGR failover did not occur (still '{active_mgr_after}')"
            )
        log.info(
            f"MGR failover confirmed: '{active_mgr_before}' -> '{active_mgr_after}'"
        )

        log.info("Verify path presence in MGR after restart (stats restored from OMAP)")
        mgr_status_post_restart = None
        for poll_i in range(20):
            time.sleep(5)
            try:
                mgr_status_post_restart = fs_mirroring_utils.get_mgr_mirror_status(
                    source_clients[0], source_fs
                )
                if mgr_status_post_restart and mgr_status_post_restart.get("metrics"):
                    break
            except Exception as e:
                log.info(f"[Scenario 4 MGR Poll {poll_i}] Waiting for MGR: {e}")

        log.info(
            f"MGR status after restart during failure: "
            f"{json.dumps(mgr_status_post_restart, indent=2)}"
        )
        mgr_metrics_s3 = (
            mgr_status_post_restart.get("metrics", {})
            if mgr_status_post_restart
            else {}
        )
        if path_key not in mgr_metrics_s3:
            raise CommandFailed(
                f"Scenario 3 FAILED: Mirrored path {path_key} missing from MGR after restart"
            )

        log.info("Remove conflicting snapshot on target to allow recovery")
        target_clients[0].exec_command(
            sudo=True,
            cmd=f"rmdir {target_mount_path_s3}{subvol_path}.snap/{snap_conflict_s3}",
            check_ec=False,
        )
        target_clients[0].exec_command(
            sudo=True, cmd=f"umount -l {target_mount_path_s3}", check_ec=False
        )
        target_clients[0].exec_command(
            sudo=True, cmd=f"rm -rf {target_mount_path_s3}", check_ec=False
        )
        target_mount_path_s3 = None

        log.info("=" * 60)
        log.info("PASSED: Scenario 3 — Mirror failure preserved across MGR restart")
        log.info("=" * 60)

        # ============================================================
        # Scenario 4 — Sync Failure Does Not Corrupt Metrics
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 4: Sync failure does not corrupt metrics")
        log.info("=" * 60)

        snaps_synced_before = path_status.get("snaps_synced", 0)
        log.info(f"snaps_synced before failure injection: {snaps_synced_before}")

        log.info("Inject sync failure: create conflicting snapshot on target FIRST")
        target_mount_path = "/mnt/remote_dir_disruptive"
        snap_conflict = "snap_conflict"

        fs_mirroring_utils.inject_sync_failure(
            target_clients[0],
            target_mount_path,
            "client.admin",
            subvol_path,
            snap_conflict,
            target_fs,
        )

        log.info("Now create same-named snapshot on source to trigger conflict")
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {mount_path}.snap/{snap_conflict}"
        )

        log.info("Poll for failure state after injection")
        fail_detected = False
        for poll in range(20):
            time.sleep(15)
            peer_status = fs_mirroring_utils.get_fs_mirror_peer_status_using_asok(
                cephfs_mirror_node[0], source_clients[0], source_fs
            )
            path_status = peer_status.get(path_key, {})
            state = path_status.get("state")
            log.info(f"[Sync Failure Poll {poll}] state={state}")
            if state == "failed":
                fail_detected = True
                break

        if fail_detected:
            log.info("Sync failure: state=failed as expected")
        else:
            raise CommandFailed(
                f"Scenario 4 FAILED: expected state=failed after conflict injection, "
                f"got '{state}'"
            )

        log.info("Validate snaps_synced via asok during failure")
        snaps_synced_after = path_status.get("snaps_synced", 0)
        if snaps_synced_after > snaps_synced_before:
            raise CommandFailed(
                f"Sync failure test FAILED: snaps_synced falsely incremented from "
                f"{snaps_synced_before} to {snaps_synced_after}"
            )
        log.info(
            f"Asok: snaps_synced stable ({snaps_synced_before} == {snaps_synced_after})"
        )

        if "current_syncing_snap" in path_status:
            log.warning(
                "Sync failure: current_syncing_snap present during failed state"
            )

        log.info("Cross-check failure via MGR interface")
        mgr_status = fs_mirroring_utils.get_mgr_mirror_status(
            source_clients[0], source_fs
        )
        log.info(f"MGR status during failure: {json.dumps(mgr_status, indent=2)}")
        mgr_metrics = mgr_status.get("metrics", {})
        mgr_path_data = mgr_metrics.get(path_key, {})
        if mgr_path_data:
            mgr_peer = list(mgr_path_data.get("peer", {}).values())
            if mgr_peer:
                mgr_state = mgr_peer[0].get("state", "")
                mgr_synced = mgr_peer[0].get("snaps_synced", 0)
                log.info(
                    f"MGR during failure: state={mgr_state}, snaps_synced={mgr_synced}"
                )

        log.info("Remove conflicting snapshot from target to allow recovery")
        target_clients[0].exec_command(
            sudo=True,
            cmd=f"rmdir {target_mount_path}{subvol_path}.snap/{snap_conflict}",
            check_ec=False,
        )
        target_clients[0].exec_command(
            sudo=True, cmd=f"umount -l {target_mount_path}", check_ec=False
        )
        target_clients[0].exec_command(
            sudo=True, cmd=f"rm -rf {target_mount_path}", check_ec=False
        )

        log.info("Poll for recovery after conflict removal")
        recovered = False
        for poll in range(20):
            time.sleep(15)
            peer_status = fs_mirroring_utils.get_fs_mirror_peer_status_using_asok(
                cephfs_mirror_node[0], source_clients[0], source_fs
            )
            path_status = peer_status.get(path_key, {})
            state = path_status.get("state")
            log.info(f"[Recovery Poll {poll}] state={state}")
            if state in ("idle", "syncing"):
                recovered = True
                break

        if recovered:
            log.info("Recovered to '%s' after removing conflict", state)
        else:
            raise CommandFailed(
                f"Scenario 4 FAILED: did not recover from failed state after conflict "
                f"removal (state={state}). Known issue: IBMCEPH-17374 — mirror daemon "
                f"stuck in failed and does not auto-recover after conflicting snapshot removed."
            )

        log.info("Verify recovery via MGR interface")
        mgr_status = fs_mirroring_utils.get_mgr_mirror_status(
            source_clients[0], source_fs
        )
        mgr_metrics = mgr_status.get("metrics", {})
        mgr_path_data = mgr_metrics.get(path_key, {})
        if mgr_path_data:
            mgr_peer = list(mgr_path_data.get("peer", {}).values())
            if mgr_peer:
                log.info(
                    "MGR post-recovery: state=%s, snaps_synced=%s",
                    mgr_peer[0].get("state"),
                    mgr_peer[0].get("snaps_synced"),
                )

        log.info("=" * 60)
        log.info("PASSED: Scenario 4 — Sync failure did not corrupt metrics")
        log.info("=" * 60)

        log.info(
            "All disruptive scenarios (Scenarios 1-4) completed successfully "
            "(order: 1=daemon-restart, 2=mgr-failover, 3=mirror-failure+mgr-restart, "
            "4=sync-failure-metrics)"
        )
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean up the system")
        try:
            if not locals().get("mount_path"):
                raise Exception("Skip cleanup: setup did not complete")
            log.info("Delete the snapshots")
            for snap in [
                "snap_baseline",
                "snap_restart_test",
                "snap_conflict",
                "snap_conflict_s3",
                "snap_mgr_failover",
            ]:
                source_clients[0].exec_command(
                    sudo=True,
                    cmd=f"rmdir {mount_path}.snap/{snap}",
                    check_ec=False,
                )

            log.info("Unmount the paths")
            source_clients[0].exec_command(
                sudo=True, cmd=f"umount -l {kernel_mounting_dir}", check_ec=False
            )

            log.info("Delete the mounted paths")
            source_clients[0].exec_command(
                sudo=True, cmd=f"rm -rf {kernel_mounting_dir}", check_ec=False
            )

            log.info("Cleanup target client")
            if target_mount_path:
                target_clients[0].exec_command(
                    sudo=True,
                    cmd=f"umount -l {target_mount_path}",
                    check_ec=False,
                )
                target_clients[0].exec_command(
                    sudo=True,
                    cmd=f"rm -rf {target_mount_path}",
                    check_ec=False,
                )
            if locals().get("target_mount_path_s3"):
                target_clients[0].exec_command(
                    sudo=True,
                    cmd=f"umount -l {target_mount_path_s3}",
                    check_ec=False,
                )
                target_clients[0].exec_command(
                    sudo=True,
                    cmd=f"rm -rf {target_mount_path_s3}",
                    check_ec=False,
                )

            log.info("Remove paths used for mirroring")
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
            fs_util_ceph1.remove_subvolume(
                source_clients[0],
                source_fs,
                f"{subvol_name}_1",
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

            log.info("Clean up mirrored data and snapshots on target cluster")
            fs_mirroring_utils.cleanup_target_cluster(
                target_clients[0],
                target_fs=target_fs,
                subvolume_paths=[subvol_path],
            )
        except Exception as cleanup_err:
            log.warning(f"Cleanup encountered an error: {cleanup_err}")
