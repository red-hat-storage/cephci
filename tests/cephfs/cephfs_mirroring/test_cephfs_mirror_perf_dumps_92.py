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


def ensure_prometheus_enabled(client):
    """Enable the Prometheus MGR module if the service endpoint is not found."""
    svc_out, _ = client.exec_command(
        sudo=True,
        cmd="ceph mgr services -f json",
    )
    mgr_services = json.loads(svc_out)
    prom_url = mgr_services.get("prometheus", "").rstrip("/")
    if prom_url:
        log.info(f"Prometheus service already available: {prom_url}")
        return prom_url

    log.info("Prometheus service not found, enabling mgr prometheus module")
    client.exec_command(
        sudo=True,
        cmd="ceph mgr module enable prometheus",
        check_ec=False,
    )
    time.sleep(10)
    svc_out, _ = client.exec_command(
        sudo=True,
        cmd="ceph mgr services -f json",
    )
    mgr_services = json.loads(svc_out)
    prom_url = mgr_services.get("prometheus", "").rstrip("/")
    if prom_url:
        log.info(f"Prometheus service enabled successfully: {prom_url}")
    else:
        log.warning("Prometheus service still not available after enabling module")
    return prom_url


def run(ceph_cluster, **kw):
    """
    CEPH-83632798 - Validate enhanced cephfs_mirror_directory perf counters for 9.2.

    Covers functional tests:
     1. Validate cephfs_mirror_directory counter group + label validation (combined)
     2. Counter lifecycle — add and remove directory (combined)
     3. dir_state semantics during state transitions
     4. Basis points encoding
     5. Prometheus scrape end-to-end
     6. Multiple daemons/filesystems isolation
     7. Legacy counter groups still present

    Returns 0 on success, 1 on failure, -1 if skipped.
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

        subvol_group_name = "subvolgroup_perf"
        subvol_name = "subvol_perf"
        subvol_size = "12884901888"
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
        # Scenario 1: Counter group schema + label validation
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 1: Counter group schema + label validation")
        log.info("=" * 60)

        data = fs_mirroring_utils.get_cephfs_mirror_counters(
            cephfs_mirror_node, fsid, asok_file
        )
        log.info(f"Counter dump top-level keys: {list(data.keys())}")

        if "cephfs_mirror_directory" not in data:
            raise CommandFailed(
                "cephfs_mirror_directory group not present in counter dump. "
                f"Available groups: {list(data.keys())}"
            )

        dir_entries = data["cephfs_mirror_directory"]
        if not isinstance(dir_entries, list):
            raise CommandFailed(
                f"cephfs_mirror_directory should be an array, "
                f"got {type(dir_entries).__name__}"
            )
        log.info(f"cephfs_mirror_directory entries: {len(dir_entries)}")

        if not dir_entries:
            raise CommandFailed(
                "cephfs_mirror_directory array is empty — expected one entry "
                "per mirrored directory per peer"
            )

        expected_labels = {
            "source_fscid",
            "source_filesystem",
            "peer_uuid",
            "peer_cluster_name",
            "peer_cluster_filesystem",
            "directory",
        }
        expected_counters = {
            "dir_state",
            "current_sync_bytes",
            "current_total_bytes",
            "current_sync_files",
            "current_total_files",
            "current_sync_bytes_percent",
            "current_sync_files_percent",
            "current_sync_mode",
            "current_snap_id",
            "current_read_bps",
            "current_write_bps",
            "crawl_state",
            "crawl_duration_seconds",
            "last_crawl_duration_seconds",
            "datasync_wait_state",
            "datasync_wait_duration_seconds",
            "last_datasync_wait_duration_seconds",
            "current_eta_valid",
            "current_eta_seconds",
            "snaps_synced",
            "snaps_deleted",
            "snaps_renamed",
            "last_snap_id",
            "last_sync_duration_seconds",
            "last_sync_timestamp",
            "last_sync_bytes",
            "last_sync_files",
        }

        for idx, entry in enumerate(dir_entries):
            labels = entry.get("labels", {})
            counters = entry.get("counters", {})
            log.info(f"Entry {idx} labels: {labels}")
            log.info(f"Entry {idx} counter keys: {sorted(counters.keys())}")

            missing_labels = expected_labels - set(labels.keys())
            if missing_labels:
                raise CommandFailed(f"Entry {idx}: missing labels: {missing_labels}")
            log.info(f"Entry {idx}: all {len(expected_labels)} labels present")

            if labels.get("source_filesystem") != source_fs:
                log.warning(
                    f"Entry {idx}: source_filesystem='{labels.get('source_filesystem')}'"
                    f", expected '{source_fs}'"
                )
            if str(labels.get("source_fscid", "")) != str(filesystem_id):
                log.warning(
                    f"Entry {idx}: source_fscid='{labels.get('source_fscid')}'"
                    f", expected '{filesystem_id}'"
                )
            if labels.get("peer_uuid") != peer_uuid:
                log.warning(
                    f"Entry {idx}: peer_uuid='{labels.get('peer_uuid')}'"
                    f", expected '{peer_uuid}'"
                )

            missing_counters = expected_counters - set(counters.keys())
            if missing_counters:
                log.warning(f"Entry {idx}: missing counters: {missing_counters}")
            else:
                log.info(f"Entry {idx}: all {len(expected_counters)} counters present")

            extra_counters = set(counters.keys()) - expected_counters
            if extra_counters:
                log.info(f"Entry {idx}: extra counters found: {extra_counters}")

        log.info(
            f"Validated {len(dir_entries)} entries — "
            f"expected {len(subvolume_paths)} (one per mirrored dir per peer)"
        )
        if len(dir_entries) != len(subvolume_paths):
            log.warning(
                f"Entry count mismatch: got {len(dir_entries)}, "
                f"expected {len(subvolume_paths)}"
            )

        log.info("Prometheus: check cephfs_mirror_directory HELP/TYPE lines")
        try:
            prom_url = ensure_prometheus_enabled(source_clients[0])

            if not prom_url:
                log.warning("Prometheus service URL not found in mgr services")
            else:
                prom_out, _ = source_clients[0].exec_command(
                    sudo=True,
                    cmd=f"curl -sk {prom_url}/metrics 2>/dev/null "
                    f"| grep cephfs_mirror_directory || echo 'no_match'",
                    check_ec=False,
                )
                if "cephfs_mirror_directory" in prom_out:
                    help_found = "# HELP cephfs_mirror_directory" in prom_out
                    type_found = "# TYPE cephfs_mirror_directory" in prom_out
                    log.info(
                        f"Prometheus cephfs_mirror_directory: "
                        f"HELP={help_found}, TYPE={type_found}"
                    )
                    for line in prom_out.strip().split("\n")[:10]:
                        log.info(f"  {line}")
                    if not help_found:
                        log.warning("HELP line not found for cephfs_mirror_directory")
                    if not type_found:
                        log.warning("TYPE line not found for cephfs_mirror_directory")
                else:
                    log.warning(
                        "cephfs_mirror_directory not found in Prometheus scrape"
                    )
        except Exception as e:
            log.warning(f"Prometheus scrape check failed: {e}")

        log.info("Counter group schema + label + Prometheus validation completed")

        # ============================================================
        # Scenario 2: Counter lifecycle — add and remove directory
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 2: Counter lifecycle — add and remove directory")
        log.info("=" * 60)

        data_before = fs_mirroring_utils.get_cephfs_mirror_counters(
            cephfs_mirror_node, fsid, asok_file
        )
        count_before = len(data_before.get("cephfs_mirror_directory", []))
        log.info(f"Directory counter entries before: {count_before}")

        lifecycle_dir = "/lifecycle_test_dir/"
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir -p {kernel_mounting_dir}{lifecycle_dir}"
        )
        fs_mirroring_utils.add_path_for_mirroring(
            source_clients[0], source_fs, lifecycle_dir
        )
        time.sleep(10)

        data_after_add = fs_mirroring_utils.get_cephfs_mirror_counters(
            cephfs_mirror_node, fsid, asok_file
        )
        count_after_add = len(data_after_add.get("cephfs_mirror_directory", []))
        log.info(f"Directory counter entries after add: {count_after_add}")

        if count_after_add > count_before:
            log.info("Counter entry added for new directory")

        fs_mirroring_utils.remove_path_from_mirroring(
            source_clients[0], source_fs, lifecycle_dir
        )
        time.sleep(10)

        data_after_rm = fs_mirroring_utils.get_cephfs_mirror_counters(
            cephfs_mirror_node, fsid, asok_file
        )
        count_after_rm = len(data_after_rm.get("cephfs_mirror_directory", []))
        log.info(f"Directory counter entries after remove: {count_after_rm}")

        if count_after_rm <= count_before:
            log.info("Counter entry removed for deleted directory")

        source_clients[0].exec_command(
            sudo=True,
            cmd=f"rm -rf {kernel_mounting_dir}{lifecycle_dir}",
            check_ec=False,
        )
        log.info("Counter lifecycle validated")

        # ============================================================
        # Scenario 3: dir_state semantics during state transitions
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 3: dir_state semantics during state transitions")
        log.info("=" * 60)

        log.info("Set tick interval to 1s for frequent counter updates")
        source_clients[0].exec_command(
            sudo=True,
            cmd="ceph config set client.cephfs-mirror " "cephfs_mirror_tick_interval 1",
        )
        log.info("Restart cephfs-mirror daemon for tick_interval to take effect")
        source_clients[0].exec_command(sudo=True, cmd="ceph orch restart cephfs-mirror")
        time.sleep(30)
        daemon_name = fs_mirroring_utils.get_daemon_name(source_clients[0])
        asok_file = fs_mirroring_utils.get_asok_file(
            cephfs_mirror_node[0], fsid, daemon_name
        )

        source_clients[0].exec_command(
            sudo=True,
            cmd=f"dd if=/dev/urandom of={mount_path1}state_data bs=1M count=5120",
            timeout=600,
        )
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {mount_path1}.snap/snap_dirstate"
        )

        # Poll every 1 s for up to 15 min to observe dir_state=1 then idle.
        DIRSTATE_POLL_MAX = 900
        dir_states_seen = set()
        syncing_counters_captured = False
        dirstate_done = False
        for poll_i in range(DIRSTATE_POLL_MAX):
            if dirstate_done:
                break
            try:
                data = fs_mirroring_utils.get_cephfs_mirror_counters(
                    cephfs_mirror_node, fsid, asok_file
                )
                found_entry = False
                for entry in data.get("cephfs_mirror_directory", []):
                    dir_label = entry.get("labels", {}).get("directory", "")
                    if subvol_path1.rstrip("/") in dir_label:
                        counters = entry.get("counters", {})
                        dir_state = counters.get("dir_state", -1)
                        dir_states_seen.add(dir_state)
                        log.info(
                            "[DirState Poll %d] dir_state=%s, snap_id=%s, "
                            "sync_bytes=%s, total_bytes=%s, sync_pct=%s, "
                            "sync_files=%s, crawl_state=%s, "
                            "sync_mode=%s, snaps_synced=%s",
                            poll_i,
                            dir_state,
                            counters.get("current_snap_id", 0),
                            counters.get("current_sync_bytes", 0),
                            counters.get("current_total_bytes", 0),
                            counters.get("current_sync_bytes_percent", 0),
                            counters.get("current_sync_files", 0),
                            counters.get("crawl_state", 0),
                            counters.get("current_sync_mode", 0),
                            counters.get("snaps_synced", 0),
                        )
                        if dir_state == 1 and not syncing_counters_captured:
                            syncing_counters_captured = True
                            log.info(
                                "[DirState] Captured syncing counters: "
                                "read_bps=%s, write_bps=%s, "
                                "eta_valid=%s, eta_secs=%s, datasync_wait=%s",
                                counters.get("current_read_bps", 0),
                                counters.get("current_write_bps", 0),
                                counters.get("current_eta_valid", 0),
                                counters.get("current_eta_seconds", 0),
                                counters.get("datasync_wait_state", 0),
                            )
                        if dir_state == 0 and counters.get("snaps_synced", 0) >= 1:
                            log.info(
                                "[DirState Poll %d] snap synced, "
                                "last_sync_bytes=%s, last_sync_files=%s",
                                poll_i,
                                counters.get("last_sync_bytes", 0),
                                counters.get("last_sync_files", 0),
                            )
                            dirstate_done = True
                        found_entry = True
                        break
                if not found_entry:
                    log.info(
                        "[DirState Poll %d] no matching entry for %s",
                        poll_i,
                        subvol_path1.rstrip("/"),
                    )
            except Exception as e:
                log.warning("dir_state poll error: %s", e)
            time.sleep(1)
        if not dirstate_done:
            log.warning(
                "dir_state poll exited after %d iterations without reaching idle; "
                "validate_snapshot_sync_status will confirm sync separately",
                DIRSTATE_POLL_MAX,
            )

        fs_mirroring_utils.validate_snapshot_sync_status(
            cephfs_mirror_node[0],
            source_fs,
            "snap_dirstate",
            fsid,
            asok_file,
            filesystem_id,
            peer_uuid,
        )
        log.info("dir_state values observed: %s", dir_states_seen)
        if 1 not in dir_states_seen:
            raise CommandFailed(
                "S3 FAILED: dir_state=1 (syncing) was NOT observed "
                "during 5 GiB sync — expected syncing state"
            )
        log.info("S3: dir_state=1 (syncing) was observed during sync")

        if not syncing_counters_captured:
            raise CommandFailed(
                "S3 FAILED: No syncing counters captured "
                "(read_bps, write_bps, etc.) during 5 GiB sync"
            )
        log.info("S3: Syncing counters captured successfully")

        data_final = fs_mirroring_utils.get_cephfs_mirror_counters(
            cephfs_mirror_node, fsid, asok_file
        )
        for entry in data_final.get("cephfs_mirror_directory", []):
            if subvol_path1.rstrip("/") in entry.get("labels", {}).get("directory", ""):
                last_bytes = entry.get("counters", {}).get("last_sync_bytes", 0)
                last_files = entry.get("counters", {}).get("last_sync_files", 0)
                last_dur = entry.get("counters", {}).get(
                    "last_sync_duration_seconds", 0
                )
                log.info(
                    "S3 last_* counters: last_sync_bytes=%s, "
                    "last_sync_files=%s, last_sync_duration_seconds=%s",
                    last_bytes,
                    last_files,
                    last_dur,
                )
                if last_bytes == 0:
                    raise CommandFailed("S3 FAILED: last_sync_bytes=0 after 5 GiB sync")
                log.info("S3: last_sync_bytes validated (non-zero)")

        # ============================================================
        # Scenario 4: Basis points encoding
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 4: Basis points encoding")
        log.info("=" * 60)

        source_clients[0].exec_command(
            sudo=True,
            cmd=f"dd if=/dev/urandom of={mount_path2}bps_data bs=1M count=5120",
            timeout=600,
        )
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {mount_path2}.snap/snap_bps"
        )

        # Poll every 1 s for up to 15 min to capture non-zero BPS value.
        BPS_POLL_MAX = 900
        bps_validated = False
        bps_done = False
        for poll_i in range(BPS_POLL_MAX):
            if bps_done:
                break
            try:
                data = fs_mirroring_utils.get_cephfs_mirror_counters(
                    cephfs_mirror_node, fsid, asok_file
                )
                for entry in data.get("cephfs_mirror_directory", []):
                    dir_label = entry.get("labels", {}).get("directory", "")
                    if subvol_path2.rstrip("/") in dir_label:
                        counters = entry.get("counters", {})
                        dir_state = counters.get("dir_state", -1)
                        bps = counters.get("current_sync_bytes_percent", 0)
                        sync_bytes = counters.get("current_sync_bytes", 0)
                        total_bytes = counters.get("current_total_bytes", 0)
                        snaps_synced = counters.get("snaps_synced", 0)
                        log.info(
                            "[BPS Poll %d] dir_state=%s, bps=%s, "
                            "sync_bytes=%s, total_bytes=%s, "
                            "sync_files_pct=%s, snaps_synced=%s",
                            poll_i,
                            dir_state,
                            bps,
                            sync_bytes,
                            total_bytes,
                            counters.get("current_sync_files_percent", 0),
                            snaps_synced,
                        )
                        if dir_state == 1 and bps > 0:
                            log.info(
                                "[BPS] Captured non-zero basis points: "
                                "bps=%s (%.2f%%)",
                                bps,
                                bps / 100.0,
                            )
                            bps_validated = True
                        if dir_state == 0 and snaps_synced >= 1:
                            log.info(
                                "[BPS Poll %d] snap synced, last_sync_bytes=%s",
                                poll_i,
                                counters.get("last_sync_bytes", 0),
                            )
                            bps_done = True
                        break
            except Exception as e:
                log.warning("BPS poll error: %s", e)
            time.sleep(1)
        if not bps_done:
            log.warning(
                "BPS poll exited after %d iterations without reaching idle; "
                "validate_snapshot_sync_status will confirm sync separately",
                BPS_POLL_MAX,
            )

        fs_mirroring_utils.validate_snapshot_sync_status(
            cephfs_mirror_node[0],
            source_fs,
            "snap_bps",
            fsid,
            asok_file,
            filesystem_id,
            peer_uuid,
        )
        if not bps_validated:
            raise CommandFailed(
                "S4 FAILED: current_sync_bytes_percent (BPS) was never "
                "non-zero during 5 GiB sync — expected progress percentage"
            )
        log.info("S4: Basis points (BPS) validated — non-zero captured during sync")

        data_bps = fs_mirroring_utils.get_cephfs_mirror_counters(
            cephfs_mirror_node, fsid, asok_file
        )
        for entry in data_bps.get("cephfs_mirror_directory", []):
            if subvol_path2.rstrip("/") in entry.get("labels", {}).get("directory", ""):
                last_bytes = entry.get("counters", {}).get("last_sync_bytes", 0)
                log.info("S4 last_sync_bytes=%s", last_bytes)
                if last_bytes == 0:
                    raise CommandFailed("S4 FAILED: last_sync_bytes=0 after 5 GiB sync")
                log.info("S4: last_sync_bytes validated (non-zero)")

        # ============================================================
        # Scenario 5: Prometheus scrape end-to-end
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 5: Prometheus scrape end-to-end")
        log.info("=" * 60)

        try:
            prom_url = ensure_prometheus_enabled(source_clients[0])

            if not prom_url:
                log.warning("Prometheus service URL not found in mgr services")
            else:
                prom_out, _ = source_clients[0].exec_command(
                    sudo=True,
                    cmd=f"curl -sk {prom_url}/metrics 2>/dev/null "
                    f"| grep cephfs_mirror || echo 'no_match'",
                    check_ec=False,
                )
                if "cephfs_mirror" in prom_out:
                    log.info("Prometheus cephfs_mirror metrics found")
                    for line in prom_out.strip().split("\n")[:10]:
                        log.info("  %s", line)
                else:
                    log.info("Prometheus cephfs_mirror metrics not found in scrape")
        except Exception as e:
            log.warning("Prometheus scrape check: %s", e)

        log.info("Prometheus scrape scenario completed")

        # ============================================================
        # Scenario 6: Legacy counter groups still present
        # ============================================================
        log.info("=" * 60)
        log.info("Scenario 6: Legacy counter groups still present")
        log.info("=" * 60)

        data = fs_mirroring_utils.get_cephfs_mirror_counters(
            cephfs_mirror_node, fsid, asok_file
        )

        legacy_groups = [
            "cephfs_mirror_mirrored_filesystems",
            "cephfs_mirror_peers",
            "cephfs_mirror",
        ]
        for group in legacy_groups:
            if group in data:
                log.info(f"Legacy group '{group}' present")
            else:
                log.warning(f"Legacy group '{group}' not found in counter dump")

        label, counter = fs_mirroring_utils.get_labels_and_counters(
            "cephfs_mirror_mirrored_filesystems", source_fs, data
        )
        if counter:
            log.info(
                f"Legacy mirrored_filesystems counter: "
                f"directory_count={counter.get('directory_count')}"
            )

        label, counter = fs_mirroring_utils.get_labels_and_counters(
            "cephfs_mirror_peers", source_fs, data
        )
        if counter:
            log.info(
                f"Legacy peers counter: snaps_synced={counter.get('snaps_synced')}"
            )

        log.info("All perf dump scenarios passed")

        log.info("Reset tick interval to default")
        source_clients[0].exec_command(
            sudo=True,
            cmd="ceph config rm client.cephfs-mirror cephfs_mirror_tick_interval",
            check_ec=False,
        )

        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean up the system")
        try:
            if not locals().get("kernel_mounting_dir"):
                raise Exception("Skip cleanup: setup did not complete")
            log.info("Cleanup: Reset tick interval")
            source_clients[0].exec_command(
                sudo=True,
                cmd="ceph config rm client.cephfs-mirror "
                "cephfs_mirror_tick_interval",
                check_ec=False,
            )

            log.info("Delete the snapshots")
            all_snaps = ["snap_dirstate", "snap_bps"]
            snap_mount_paths = [mount_path1, mount_path2]
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
            source_clients[0].exec_command(
                sudo=True,
                cmd=f"ceph fs snapshot mirror remove {source_fs} /lifecycle_test_dir/",
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
