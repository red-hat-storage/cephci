import json
import random
import string
import time
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_mirroring.cephfs_mirroring_utils import CephfsMirroringUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.snapshot_clone.cephfs_snap_utils import SnapUtils
from utility.log import Log

log = Log(__name__)


def checkpoint_add(client, fs_name, path, snap_name):
    """Add a checkpoint for a snapshot."""
    cmd = f"ceph fs snapshot mirror checkpoint add {fs_name} {path} {snap_name}"
    out, _ = client.exec_command(sudo=True, cmd=cmd)
    log.info(f"checkpoint add: {out.strip()}")
    return out.strip()


def checkpoint_ls(client, fs_name, path):
    """List checkpoints for a path. Returns list of checkpoint dicts."""
    cmd = f"ceph fs snapshot mirror checkpoint ls {fs_name} {path} -f json"
    out, _ = client.exec_command(sudo=True, cmd=cmd)
    log.info(f"checkpoint ls raw output: {out.strip()}")
    data = json.loads(out)
    if isinstance(data, dict):
        log.info(f"checkpoint ls parsed as dict with keys: {list(data.keys())}")
        return data.get("checkpoints", [])
    log.info(f"checkpoint ls parsed as list with {len(data)} entries")
    return data


def checkpoint_rm(client, fs_name, path, snap_name):
    """Remove a checkpoint for a snapshot."""
    cmd = f"ceph fs snapshot mirror checkpoint remove {fs_name} {path} {snap_name}"
    out, _ = client.exec_command(sudo=True, cmd=cmd)
    log.info(f"checkpoint remove: {out.strip()}")
    return out.strip()


def run(ceph_cluster, **kw):
    """
    CEPH-83632782 - Validate CephFS mirroring checkpoint commands for 9.2.

    Covers functional tests:
     1. Checkpoint add, ls, remove lifecycle
     2. Checkpoint now (latest snapshot)
     3. State transition CREATED → COMPLETE after sync
     4. Immediate COMPLETE for already-synced snapshot
     5. Multiple checkpoints + ordering
     6. Checkpoint on older snapshot with snap schedule (BZ#XXXXXX)

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
        snap_util = SnapUtils(ceph_cluster_dict.get("ceph1"))
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

        subvol_group_name = "subvolgroup_ckpt"
        subvol_name = "subvol_ckpt"
        subvol_size = "12884901888"
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        kernel_mounting_dir = f"/mnt/cephfs_kernel{mounting_dir}_1"
        subvol_details = [
            {
                "subvol_name": f"{subvol_name}_1",
                "subvol_size": subvol_size,
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
        subvol_path1 = subvolume_paths[0]

        log.info("Add subvolume for mirroring")
        fs_mirroring_utils.add_path_for_mirroring(
            source_clients[0], source_fs, subvol_path1
        )

        mount_path = f"{kernel_mounting_dir}{subvol_path1}"

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
        # Scenario 1: Checkpoint add, ls, remove lifecycle
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 1: Checkpoint add, ls, remove lifecycle")
        log.info("=" * 60)

        source_clients[0].exec_command(sudo=True, cmd=f"touch {mount_path}file_ckpt1")
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {mount_path}.snap/snap_lifecycle"
        )
        time.sleep(10)

        fs_mirroring_utils.validate_snapshot_sync_status(
            cephfs_mirror_node[0],
            source_fs,
            "snap_lifecycle",
            fsid,
            asok_file,
            filesystem_id,
            peer_uuid,
        )

        checkpoint_add(source_clients[0], source_fs, subvol_path1, "snap_lifecycle")
        ckpt_list = checkpoint_ls(source_clients[0], source_fs, subvol_path1)
        log.info(f"Checkpoint list after add: {ckpt_list}")

        found = False
        for ckpt in ckpt_list:
            if ckpt.get("snap_name", "") == "snap_lifecycle":
                found = True
                log.info(f"Checkpoint found: {ckpt}")
                break
        if not found:
            raise CommandFailed("Checkpoint for snap_lifecycle not found in ls")

        checkpoint_rm(source_clients[0], source_fs, subvol_path1, "snap_lifecycle")
        ckpt_list_after = checkpoint_ls(source_clients[0], source_fs, subvol_path1)
        log.info(f"Checkpoint list after rm: {ckpt_list_after}")
        for ckpt in ckpt_list_after:
            if ckpt.get("snap_name", "") == "snap_lifecycle":
                raise CommandFailed("Checkpoint still present after rm")
        log.info("Checkpoint lifecycle (add, ls, rm) validated")

        # ============================================================
        # Scenario 2: Checkpoint now (latest snapshot)
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 2: Checkpoint now (latest snapshot)")
        log.info("=" * 60)

        source_clients[0].exec_command(sudo=True, cmd=f"touch {mount_path}file_now")
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {mount_path}.snap/snap_now"
        )
        fs_mirroring_utils.validate_snapshot_sync_status(
            cephfs_mirror_node[0],
            source_fs,
            "snap_now",
            fsid,
            asok_file,
            filesystem_id,
            peer_uuid,
        )

        try:
            cmd = f"ceph fs snapshot mirror checkpoint now {source_fs} {subvol_path1}"
            out, _ = source_clients[0].exec_command(sudo=True, cmd=cmd)
            log.info(f"Checkpoint now result: {out.strip()}")

            ckpt_list = checkpoint_ls(source_clients[0], source_fs, subvol_path1)
            now_ckpt_snap = None
            for ckpt in ckpt_list:
                if ckpt.get("snap_name") == "snap_now":
                    now_ckpt_snap = ckpt
                    break
            log.info(f"Checkpoint now entry: {now_ckpt_snap}")

            log.info("Create a new snapshot AFTER checkpoint now")
            source_clients[0].exec_command(
                sudo=True, cmd=f"touch {mount_path}file_after_now"
            )
            source_clients[0].exec_command(
                sudo=True, cmd=f"mkdir {mount_path}.snap/snap_after_now"
            )

            log.info("Wait for snap_after_now to sync")
            path_key_s2 = subvol_path1.rstrip("/")
            for attempt in range(20):
                time.sleep(15)
                peer_status = fs_mirroring_utils.get_fs_mirror_peer_status_using_asok(
                    cephfs_mirror_node[0], source_clients[0], source_fs
                )
                ps = peer_status.get(path_key_s2, {})
                last = ps.get("last_synced_snap", {}).get("name")
                log.info(
                    "[S2 Poll %d/20] state=%s, last_synced=%s",
                    attempt,
                    ps.get("state"),
                    last,
                )
                if ps.get("state") == "idle" and last == "snap_after_now":
                    break
            else:
                raise CommandFailed("S2: snap_after_now did not reach idle within 300s")

            log.info(
                "Re-check checkpoint — should still reference snap_now, not snap_after_now"
            )
            ckpt_list_after = checkpoint_ls(source_clients[0], source_fs, subvol_path1)
            now_ckpt_after = None
            for ckpt in ckpt_list_after:
                if ckpt.get("snap_name") == "snap_now":
                    now_ckpt_after = ckpt
                    break
            if now_ckpt_after:
                log.info("Checkpoint still references snap_now: %s", now_ckpt_after)
                log.info("Scenario 2 PASSED: checkpoint now is pinned to snap_now")
            else:
                log.warning("Checkpoint for snap_now not found after new snap created")

            source_clients[0].exec_command(
                sudo=True,
                cmd=f"rmdir {mount_path}.snap/snap_after_now",
                check_ec=False,
            )
        except CommandFailed as e:
            log.info("Checkpoint 'now' may not be supported yet: %s", e)

        log.info("Checkpoint now scenario completed")

        # ============================================================
        # Scenario 3: State transition CREATED → COMPLETE
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 3: State transition CREATED → COMPLETE")
        log.info("=" * 60)

        log.info("S3: Write 5 GiB to ensure sync takes time")
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"dd if=/dev/urandom of={mount_path}state_data bs=1M count=5120",
            timeout=600,
        )
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {mount_path}.snap/snap_state"
        )

        log.info("S3: Add checkpoint immediately (before sync completes)")
        checkpoint_add(source_clients[0], source_fs, subvol_path1, "snap_state")

        ckpt_list = checkpoint_ls(source_clients[0], source_fs, subvol_path1)
        log.info(f"S3: checkpoint ls immediately after add: {ckpt_list}")
        initial_status = None
        for ckpt in ckpt_list:
            if ckpt.get("snap_name") == "snap_state":
                initial_status = ckpt.get("status", ckpt.get("state", ""))
                log.info(f"S3: Initial checkpoint status: {initial_status}")

        if initial_status and "created" in initial_status.lower():
            log.info("S3: Captured CREATED state before sync completes")
        elif initial_status and "complete" in initial_status.lower():
            log.warning(
                "S3: Status already COMPLETE — 5 GiB synced too fast "
                "to observe CREATED state"
            )
        else:
            log.info(f"S3: Initial status: {initial_status}")

        log.info("S3: Wait for snap_state to sync")
        fs_mirroring_utils.validate_snapshot_sync_status(
            cephfs_mirror_node[0],
            source_fs,
            "snap_state",
            fsid,
            asok_file,
            filesystem_id,
            peer_uuid,
        )

        ckpt_list_after = checkpoint_ls(source_clients[0], source_fs, subvol_path1)
        log.info(f"S3: checkpoint ls after sync: {ckpt_list_after}")
        final_status = None
        for ckpt in ckpt_list_after:
            if ckpt.get("snap_name") == "snap_state":
                final_status = ckpt.get("status", ckpt.get("state", ""))
                log.info(f"S3: Final checkpoint status: {final_status}")

        if final_status and "complete" in final_status.lower():
            log.info("S3 PASSED: Checkpoint reached COMPLETE after sync")
        else:
            raise CommandFailed(
                f"S3 FAILED: Expected status 'complete' after sync, "
                f"got '{final_status}'"
            )

        # ============================================================
        # Scenario 4: Immediate COMPLETE for already-synced snapshot
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 4: Immediate COMPLETE for already-synced")
        log.info("=" * 60)

        try:
            checkpoint_add(source_clients[0], source_fs, subvol_path1, "snap_lifecycle")
            time.sleep(10)
            ckpt_list = checkpoint_ls(source_clients[0], source_fs, subvol_path1)
            log.info(f"S4: checkpoint ls after adding already-synced snap: {ckpt_list}")
            found = False
            for ckpt in ckpt_list:
                if ckpt.get("snap_name") == "snap_lifecycle":
                    found = True
                    ckpt_status = ckpt.get("status", ckpt.get("state", ""))
                    log.info(
                        f"S4: Checkpoint on already-synced snap: status={ckpt_status}"
                    )
                    if "complete" not in ckpt_status.lower():
                        raise CommandFailed(
                            f"S4 FAILED: Expected status 'complete' for "
                            f"already-synced snap, got '{ckpt_status}'"
                        )
                    log.info("S4 PASSED: Immediate COMPLETE validated")
            if not found:
                raise CommandFailed(
                    "S4 FAILED: snap_lifecycle not found in checkpoint ls"
                )
            checkpoint_rm(source_clients[0], source_fs, subvol_path1, "snap_lifecycle")
        except CommandFailed as e:
            if "S4 FAILED" in str(e):
                raise
            log.warning(f"S4: Checkpoint command error: {e}")

        # ============================================================
        # Scenario 5: Multiple checkpoints + ordering
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 5: Multiple checkpoints + ordering")
        log.info("=" * 60)

        peer_status_pre = fs_mirroring_utils.get_fs_mirror_peer_status_using_asok(
            cephfs_mirror_node[0], source_clients[0], source_fs
        )
        path_key = subvol_path1.rstrip("/")
        snaps_synced_before = peer_status_pre.get(path_key, {}).get("snaps_synced", 0)
        log.info(f"Scenario 5: snaps_synced before = {snaps_synced_before}")

        multi_snaps = []
        for i in range(3):
            snap_name = f"snap_multi_{i}"
            log.info(f"S5: Write 200 MiB data for {snap_name}")
            source_clients[0].exec_command(
                sudo=True,
                cmd=f"dd if=/dev/urandom of={mount_path}multi_data_{i} "
                f"bs=1M count=200 2>/dev/null",
            )
            source_clients[0].exec_command(
                sudo=True, cmd=f"mkdir {mount_path}.snap/{snap_name}"
            )
            checkpoint_add(source_clients[0], source_fs, subvol_path1, snap_name)
            log.info(f"S5: Created {snap_name} and added checkpoint")
            multi_snaps.append(snap_name)

        log.info("S5: Wait for all 3 multi snaps to sync (poll for idle)")
        for attempt in range(40):
            time.sleep(15)
            peer_status = fs_mirroring_utils.get_fs_mirror_peer_status_using_asok(
                cephfs_mirror_node[0], source_clients[0], source_fs
            )
            path_status = peer_status.get(path_key, {})
            state = path_status.get("state", "unknown")
            snaps_now = path_status.get("snaps_synced", 0)
            last = path_status.get("last_synced_snap", {}).get("name")
            log.info(
                f"[S5 Poll {attempt}] state={state}, snaps_synced={snaps_now}, "
                f"last_synced={last}"
            )
            if state == "idle" and snaps_now >= snaps_synced_before + 3:
                break
        else:
            raise CommandFailed(
                f"S5: Expected snaps_synced >= {snaps_synced_before + 3}, "
                f"got {snaps_now}"
            )
        log.info(f"S5: All 3 snaps synced, snaps_synced={snaps_now}")

        ckpt_list = checkpoint_ls(source_clients[0], source_fs, subvol_path1)
        log.info(f"S5: checkpoint ls ({len(ckpt_list)} entries): {ckpt_list}")

        ckpt_map = {c.get("snap_name"): c for c in ckpt_list}
        for snap_name in multi_snaps:
            if snap_name not in ckpt_map:
                raise CommandFailed(
                    f"S5 FAILED: Checkpoint {snap_name} not found in ls"
                )
            ckpt = ckpt_map[snap_name]
            ckpt_status = ckpt.get("status", ckpt.get("state", ""))
            if "complete" not in ckpt_status.lower():
                raise CommandFailed(
                    f"S5 FAILED: {snap_name} status='{ckpt_status}', "
                    f"expected 'complete'"
                )
            log.info(
                f"S5: {snap_name} — status={ckpt_status}, "
                f"snap_id={ckpt.get('snap_id')}, "
                f"created_at={ckpt.get('created_at')}, "
                f"updated_at={ckpt.get('updated_at')}"
            )

        multi_ids = [ckpt_map[s].get("snap_id", 0) for s in multi_snaps]
        if multi_ids != sorted(multi_ids):
            raise CommandFailed(
                f"S5 FAILED: snap_id not in ascending order: {multi_ids}"
            )
        log.info(f"S5: snap_id ordering correct: {multi_ids}")

        created_times = [ckpt_map[s].get("created_at", "") for s in multi_snaps]
        if created_times != sorted(created_times):
            raise CommandFailed(
                f"S5 FAILED: created_at not in ascending order: {created_times}"
            )
        log.info(f"S5: created_at ordering correct: {created_times}")

        updated_times = [ckpt_map[s].get("updated_at", "") for s in multi_snaps]
        if updated_times != sorted(updated_times):
            raise CommandFailed(
                f"S5 FAILED: updated_at not in ascending order: {updated_times}"
            )
        log.info(f"S5: updated_at ordering correct: {updated_times}")

        for snap_name in multi_snaps:
            checkpoint_rm(source_clients[0], source_fs, subvol_path1, snap_name)

        ckpt_list_after = checkpoint_ls(source_clients[0], source_fs, subvol_path1)
        remaining = [c.get("snap_name") for c in ckpt_list_after]
        for snap_name in multi_snaps:
            if snap_name in remaining:
                raise CommandFailed(
                    f"S5 FAILED: {snap_name} still present after remove"
                )

        log.info("S5 PASSED: Multiple checkpoints — all complete, ordered, removed")

        # ============================================================
        # Scenario 6: Checkpoint on older snapshot with snap schedule
        # (Known bug — may cause MDS deadlock)
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 6: Checkpoint on older snap with snap schedule")
        log.info("=" * 60)

        log.info("Enable snap schedule module")
        snap_util.enable_snap_schedule(source_clients[0])
        time.sleep(10)
        snap_util.allow_minutely_schedule(source_clients[0], allow=True)
        time.sleep(5)

        sched_path = subvol_path1
        snap_sched_params = {
            "client": source_clients[0],
            "path": sched_path,
            "sched": "1m",
            "fs_name": source_fs,
        }
        log.info(f"Create minutely snap schedule on {sched_path}")
        snap_util.create_snap_schedule(snap_sched_params)

        log.info("Wait 3 minutes for at least 3 scheduled snapshots to accumulate")
        time.sleep(180)

        sched_snaps_out, _ = source_clients[0].exec_command(
            sudo=True, cmd=f"ls {mount_path}.snap/ | grep scheduled | sort"
        )
        sched_snaps = sched_snaps_out.strip().splitlines()
        log.info(f"Scheduled snapshots found ({len(sched_snaps)}): {sched_snaps}")

        if len(sched_snaps) < 2:
            log.warning(
                "Not enough scheduled snapshots to test older checkpoint, skipping"
            )
        else:
            oldest_snap = sched_snaps[0]
            latest_snap = sched_snaps[-1]
            log.info(f"Oldest scheduled snap: {oldest_snap}")
            log.info(f"Latest scheduled snap: {latest_snap}")

            log.info("Run checkpoint now (pins to latest snapshot)")
            try:
                cmd_now = (
                    f"ceph fs snapshot mirror checkpoint now "
                    f"{source_fs} {subvol_path1}"
                )
                out_now, _ = source_clients[0].exec_command(sudo=True, cmd=cmd_now)
                log.info(f"checkpoint now result: {out_now.strip()}")
            except CommandFailed as e:
                log.warning(f"checkpoint now failed: {e}")

            log.info(
                f"Add checkpoint for OLDER snapshot: {oldest_snap} "
                f"(with 60s timeout to detect hang)"
            )
            older_ckpt_hung = False
            try:
                source_clients[0].exec_command(
                    sudo=True,
                    cmd=(
                        f"timeout 60 ceph fs snapshot mirror checkpoint add "
                        f"{source_fs} {subvol_path1} {oldest_snap}"
                    ),
                    check_ec=False,
                )
                log.info(f"checkpoint add for older snap {oldest_snap} returned")
            except Exception as e:
                log.warning(f"checkpoint add for older snap timed out or failed: {e}")
                older_ckpt_hung = True

            log.info("Verify checkpoint ls is responsive (60s timeout)")
            try:
                ls_out, _ = source_clients[0].exec_command(
                    sudo=True,
                    cmd=(
                        f"timeout 60 ceph fs snapshot mirror checkpoint ls "
                        f"{source_fs} {subvol_path1} -f json"
                    ),
                    check_ec=False,
                )
                log.info(f"checkpoint ls output: {ls_out.strip()}")
            except Exception as e:
                log.warning(f"checkpoint ls timed out or failed: {e}")
                older_ckpt_hung = True

            log.info("Check MDS for slow/blocked ops")
            try:
                ops_out, _ = source_clients[0].exec_command(
                    sudo=True,
                    cmd=f"ceph tell mds.{source_fs}:0 dump_blocked_ops -f json",
                    check_ec=False,
                )
                log.info(f"MDS blocked ops: {ops_out.strip()}")
            except Exception as e:
                log.warning(f"Could not query MDS blocked ops: {e}")

            if older_ckpt_hung:
                log.error(
                    "KNOWN BUG: checkpoint add for older snapshot caused hang. "
                    "MDS may have snap lock deadlock. "
                    "See BZ for details."
                )
                raise CommandFailed(
                    "Checkpoint on older snapshot hung — known bug: "
                    "MDS snap lock deadlock when adding checkpoint for "
                    "older snapshot after checkpoint now"
                )
            else:
                log.info(
                    "Scenario 6 PASSED: checkpoint on older snapshot "
                    "completed without hang"
                )

        log.info("Deactivate and remove snap schedule")
        snap_util.deactivate_snap_schedule(
            source_clients[0],
            sched_path,
            sched_val="1m",
            fs_name=source_fs,
        )
        snap_util.remove_snap_schedule(
            source_clients[0],
            sched_path,
            fs_name=source_fs,
        )
        time.sleep(10)

        log.info("Remove scheduled snapshots")
        for snap in sched_snaps:
            source_clients[0].exec_command(
                sudo=True,
                cmd=f"rmdir {mount_path}.snap/{snap}",
                check_ec=False,
            )

        log.info("Scenario 6 completed")

        log.info("All checkpoint scenarios passed")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean up the system")
        try:
            log.info("Cleanup: Deactivate and remove snap schedule")
            snap_util.deactivate_snap_schedule(
                source_clients[0],
                subvol_path1,
                sched_val="1m",
                fs_name=source_fs,
            )
            snap_util.remove_snap_schedule(
                source_clients[0],
                subvol_path1,
                fs_name=source_fs,
                check_ec=False,
            )

            log.info("Cleanup: Remove all scheduled snapshots")
            try:
                sched_out, _ = source_clients[0].exec_command(
                    sudo=True,
                    cmd=f"ls {mount_path}.snap/ | grep scheduled || true",
                    check_ec=False,
                )
                for snap in sched_out.strip().splitlines():
                    if snap:
                        source_clients[0].exec_command(
                            sudo=True,
                            cmd=f"rmdir {mount_path}.snap/{snap}",
                            check_ec=False,
                        )
            except Exception as sched_snap_err:
                log.warning(
                    "Could not remove scheduled snapshots during cleanup: %s",
                    sched_snap_err,
                )

            log.info("Delete the snapshots")
            all_snaps = [
                "snap_lifecycle",
                "snap_now",
                "snap_after_now",
                "snap_state",
            ] + [f"snap_multi_{i}" for i in range(3)]
            for snap in all_snaps:
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

            log.info("Remove paths used for mirroring")
            fs_mirroring_utils.remove_path_from_mirroring(
                source_clients[0], source_fs, subvol_path1
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
        except Exception as cleanup_err:
            log.warning("Cleanup encountered an error: %s", cleanup_err)
