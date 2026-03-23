import random
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_mirroring.cephfs_mirroring_utils import CephfsMirroringUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83580027 - Test incremental snapshot synchronization (based on mtime differences).

    Ported from upstream: test_cephfs_mirror_incremental_sync in
    qa/tasks/cephfs/test_mirroring.py

    Scenario:
        1. Deploy mirroring between source and target clusters
        2. Clone a git repo (ceph-qa-suite, branch 'giant') into the mirrored path
        3. Create snap_a (full sync) — wait for sync, verify checksums on target
        4. Record full sync duration from perf counters
        5. Reset git repo to HEAD~N (random diff) to modify a subset of files
        6. Create snap_b (incremental sync) — wait for sync, verify checksums
        7. Assert incremental sync duration <= full sync duration
        8. Pull git repo back to HEAD (another diff)
        9. Create snap_c (incremental sync) — wait for sync, verify checksums
       10. Assert incremental sync duration <= full sync duration

    Returns 0 on success, 1 on failure.
    """
    mirror_path = None
    kernel_mounting_dir = None
    source_clients = None
    target_clients = None
    cephfs_mirror_node = None
    source_fs = target_fs = None
    target_user = "mirror_remote"
    target_site_name = "remote_site"
    fs_mirroring_utils = None
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
            log.info("Requires at least 1 client on both ceph1 and ceph2.")
            return 1

        fs_util_ceph1.prepare_clients(source_clients, build)
        fs_util_ceph2.prepare_clients(target_clients, build)
        fs_util_ceph1.auth_list(source_clients)
        fs_util_ceph2.auth_list(target_clients)

        source_fs = "cephfs" if not erasure else "cephfs-ec"
        target_fs = "cephfs" if not erasure else "cephfs-ec"
        if not fs_util_ceph1.get_fs_info(source_clients[0], source_fs):
            fs_util_ceph1.create_fs(source_clients[0], source_fs)
        if not fs_util_ceph2.get_fs_info(target_clients[0], target_fs):
            fs_util_ceph2.create_fs(target_clients[0], target_fs)

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
        kernel_mounting_dir = f"/mnt/cephfs_kernel{mounting_dir}_incsync/"
        mon_node_ips = fs_util_ceph1.get_mon_node_ips()
        fs_util_ceph1.kernel_mount(
            [source_clients[0]],
            kernel_mounting_dir,
            ",".join(mon_node_ips),
            extra_params=f",fs={source_fs}",
        )

        # --- Clone a git repo as the dataset ---
        repo = "ceph-qa-suite"
        repo_dir = "ceph_repo"
        repo_path = f"{repo_dir}/{repo}"
        mirror_path = f"/{repo_path}/"
        abs_repo_dir = f"{kernel_mounting_dir}{repo_dir}"
        abs_repo_path = f"{kernel_mounting_dir}{repo_path}"

        log.info(f"Cloning {repo} (branch giant) into {abs_repo_path}")
        source_clients[0].exec_command(sudo=True, cmd=f"mkdir -p {abs_repo_dir}")
        source_clients[0].exec_command(
            sudo=True,
            cmd=(
                f"git clone --branch giant "
                f"http://github.com/ceph/{repo} {abs_repo_path}"
            ),
            long_running=True,
        )

        log.info("Add directory for mirroring")
        fs_mirroring_utils.add_path_for_mirroring(
            source_clients[0], source_fs, mirror_path
        )

        # Upload md5sum helper to both clusters
        md5sum_script = "md5sum_script.sh"
        for client in [source_clients[0], target_clients[0]]:
            client.upload_file(
                sudo=True,
                src="tests/cephfs/cephfs_mirror_upgrade/md5sum_script.sh",
                dst=f"/root/{md5sum_script}",
            )

        # --- Retrieve mirror daemon identifiers for status queries ---
        fsid = fs_mirroring_utils.get_fsid(cephfs_mirror_node[0])
        daemon_names = fs_mirroring_utils.get_daemon_name(source_clients[0])
        asok_files = fs_mirroring_utils.get_asok_file(
            cephfs_mirror_node, fsid, daemon_names
        )
        filesystem_id = fs_mirroring_utils.get_filesystem_id_by_name(
            source_clients[0], source_fs
        )
        peer_uuid = fs_mirroring_utils.get_peer_uuid_by_name(
            source_clients[0], source_fs
        )
        for node in cephfs_mirror_node:
            node.exec_command(sudo=True, cmd="yum install -y ceph-common --nogpgcheck")

        # ==================== SNAP_A: Full sync ====================
        snap_a = "snap_a"
        snap_names.append(snap_a)
        log.info(f"Creating snapshot '{snap_a}' (full sync)")
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"mkdir {abs_repo_path}/.snap/{snap_a}",
        )

        result_a = fs_mirroring_utils.validate_snapshot_sync_status(
            cephfs_mirror_node,
            source_fs,
            snap_a,
            fsid,
            asok_files,
            filesystem_id,
            peer_uuid,
        )
        log.info(
            f"snap_a synced: duration={result_a['sync_duration']} "
            f"snaps_synced={result_a['snaps_synced']}"
        )
        full_sync_duration = float(result_a["sync_duration"])

        log.info("Verifying snap_a data on target cluster")
        out, rc = fs_mirroring_utils.list_and_verify_remote_snapshots_and_data_checksum(
            target_clients[0],
            f"client.{target_clients[0].node.hostname}",
            mirror_path,
            source_clients[0],
            kernel_mounting_dir,
            target_fs,
        )
        if not out:
            raise CommandFailed(f"snap_a checksum verification failed: {rc}")
        snap_a_checksum, _ = source_clients[0].exec_command(
            sudo=True,
            cmd=f"bash /root/{md5sum_script} {abs_repo_path}/.snap/{snap_a}",
        )
        log.info(f"snap_a checksum:\n{snap_a_checksum}")
        log.info("snap_a: full sync completed and verified")

        # ==================== SNAP_B: Incremental sync (git reset) ====================
        num = random.randint(5, 10)
        log.info(f"Creating diff: git reset --hard HEAD~{num}")
        source_clients[0].exec_command(
            sudo=True,
            cmd=(
                f"git --git-dir {abs_repo_path}/.git "
                f"--work-tree {abs_repo_path} "
                f"reset --hard HEAD~{num}"
            ),
        )

        snap_b = "snap_b"
        snap_names.append(snap_b)
        log.info(f"Creating snapshot '{snap_b}' (incremental sync)")
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"mkdir {abs_repo_path}/.snap/{snap_b}",
        )

        result_b = fs_mirroring_utils.validate_snapshot_sync_status(
            cephfs_mirror_node,
            source_fs,
            snap_b,
            fsid,
            asok_files,
            filesystem_id,
            peer_uuid,
        )
        log.info(
            f"snap_b synced: duration={result_b['sync_duration']} "
            f"snaps_synced={result_b['snaps_synced']}"
        )
        inc_sync_duration_1 = float(result_b["sync_duration"])

        log.info("Verifying snap_b data on target cluster")
        out, rc = fs_mirroring_utils.list_and_verify_remote_snapshots_and_data_checksum(
            target_clients[0],
            f"client.{target_clients[0].node.hostname}",
            mirror_path,
            source_clients[0],
            kernel_mounting_dir,
            target_fs,
        )
        if not out:
            raise CommandFailed(f"snap_b checksum verification failed: {rc}")
        snap_b_checksum, _ = source_clients[0].exec_command(
            sudo=True,
            cmd=f"bash /root/{md5sum_script} {abs_repo_path}/.snap/{snap_b}",
        )
        log.info(f"snap_b checksum:\n{snap_b_checksum}")
        if snap_a_checksum == snap_b_checksum:
            log.error(
                "snap_b checksum must differ from snap_a after git reset, "
                "but they are identical"
            )
            return 1
        log.info("Cross-snapshot check: snap_a != snap_b — PASSED")

        log.info(
            f"snap_b: incremental sync duration ({inc_sync_duration_1:.3f}s) vs "
            f"full sync duration ({full_sync_duration:.3f}s)"
        )
        if full_sync_duration < inc_sync_duration_1:
            log.error(
                f"Incremental sync (snap_b) took longer than full sync: "
                f"{inc_sync_duration_1:.3f}s > {full_sync_duration:.3f}s"
            )
            return 1

        # ==================== SNAP_C: Incremental sync (git pull) ====================
        log.info("Creating diff: git pull (back to HEAD)")
        source_clients[0].exec_command(
            sudo=True,
            cmd=(
                f"git --git-dir {abs_repo_path}/.git "
                f"--work-tree {abs_repo_path} "
                f"pull"
            ),
            long_running=True,
        )

        snap_c = "snap_c"
        snap_names.append(snap_c)
        log.info(f"Creating snapshot '{snap_c}' (incremental sync)")
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"mkdir {abs_repo_path}/.snap/{snap_c}",
        )

        result_c = fs_mirroring_utils.validate_snapshot_sync_status(
            cephfs_mirror_node,
            source_fs,
            snap_c,
            fsid,
            asok_files,
            filesystem_id,
            peer_uuid,
        )
        log.info(
            f"snap_c synced: duration={result_c['sync_duration']} "
            f"snaps_synced={result_c['snaps_synced']}"
        )
        inc_sync_duration_2 = float(result_c["sync_duration"])

        log.info("Verifying snap_c data on target cluster")
        out, rc = fs_mirroring_utils.list_and_verify_remote_snapshots_and_data_checksum(
            target_clients[0],
            f"client.{target_clients[0].node.hostname}",
            mirror_path,
            source_clients[0],
            kernel_mounting_dir,
            target_fs,
        )
        if not out:
            raise CommandFailed(f"snap_c checksum verification failed: {rc}")
        snap_c_checksum, _ = source_clients[0].exec_command(
            sudo=True,
            cmd=f"bash /root/{md5sum_script} {abs_repo_path}/.snap/{snap_c}",
        )
        log.info(f"snap_c checksum:\n{snap_c_checksum}")
        if snap_b_checksum == snap_c_checksum:
            log.error(
                "snap_c checksum must differ from snap_b after git pull, "
                "but they are identical"
            )
            return 1
        log.info("Cross-snapshot check: snap_b != snap_c — PASSED")
        if snap_a_checksum != snap_c_checksum:
            log.error(
                "snap_c checksum must match snap_a (both at HEAD), " "but they differ"
            )
            return 1
        log.info(
            "Cross-snapshot check: snap_a == snap_c — PASSED (round-trip integrity)"
        )

        log.info(
            f"snap_c: incremental sync duration ({inc_sync_duration_2:.3f}s) vs "
            f"full sync duration ({full_sync_duration:.3f}s)"
        )
        if full_sync_duration < inc_sync_duration_2:
            log.error(
                f"Incremental sync (snap_c) took longer than full sync: "
                f"{inc_sync_duration_2:.3f}s > {full_sync_duration:.3f}s"
            )
            return 1

        # --- Summary ---
        log.info("=" * 70)
        log.info("INCREMENTAL SYNC TEST RESULTS")
        log.info("=" * 70)
        log.info(f"  snap_a (full sync):        {full_sync_duration:.3f}s")
        log.info(f"  snap_b (incremental, reset): {inc_sync_duration_1:.3f}s")
        log.info(f"  snap_c (incremental, pull):  {inc_sync_duration_2:.3f}s")
        log.info(
            "All incremental syncs completed faster than full sync "
            "and data verified on target"
        )
        log.info("=" * 70)
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean up the system")

        log.info("Delete the snapshots")
        for snap in snap_names:
            source_clients[0].exec_command(
                sudo=True,
                cmd=f"rmdir {abs_repo_path}/.snap/{snap}",
                check_ec=False,
            )

        log.info("Remove path used for mirroring")
        if mirror_path:
            fs_mirroring_utils.remove_path_from_mirroring(
                source_clients[0], source_fs, mirror_path
            )

        log.info("Destroy CephFS Mirroring setup.")
        fs_mirroring_utils.destroy_cephfs_mirroring(
            source_fs,
            source_clients[0],
            target_fs,
            target_clients[0],
            target_user,
            peer_uuid,
        )

        log.info("Unmount the paths")
        if kernel_mounting_dir:
            source_clients[0].exec_command(
                sudo=True, cmd=f"umount -l {kernel_mounting_dir}"
            )

        log.info("Delete the mounted paths")
        if kernel_mounting_dir:
            source_clients[0].exec_command(
                sudo=True, cmd=f"rm -rf {kernel_mounting_dir}"
            )
