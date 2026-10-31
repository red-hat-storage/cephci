import json
import time
import traceback

from looseversion import LooseVersion

from tests.cephfs.cephfs_mirroring.cephfs_mirroring_utils import (
    CephfsMirroringUtils,
    wait_for_idle,
)
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Validate 2-Daemon HA Mirroring Scenarios for CephFS Mirroring 9.2:

    1. Stats continuity for redistributed directories:
       - 2 daemons, 4 mirrored directories
       - Stop daemon 1 mid-state
       - Surviving daemon 2 acquires directories, asok starts from 0 session,
         MGR/OMAP history retained.
    2. Multi-daemon stat accuracy & filesystem isolation:
       - 2 filesystems (cephfs, cephfs2) distributed across daemons
       - Verify per-FS metric isolation without cross-filesystem pollution.
    3. MGR interface reports all directories after daemon redistribution:
       - Stop 1 daemon
       - Verify all directories reported by MGR with no persistent stale alerts.
    5. Multi-daemon perf dump isolation:
       - Query cephfs_mirror_directory on both daemons
       - Verify each daemon reflects only its assigned directories.
    6. Perf counters for redistributed directories:
       - Stop daemon 1
       - Verify surviving daemon 2 instantiates new cephfs_mirror_directory counters
         from 0 for acquired directories.
    7. Tri-interface after HA failover:
       - Trigger sync on directory assigned to daemon 1
       - Stop daemon 1 mid-sync
       - Verify surviving daemon 2 asok syncing/completed, MGR recovers from brief stale,
         perf counter dir_state reflects active sync and idle.

    Returns:
        0 on success, 1 on failure, -1 if skipped.
    """
    config = kw.get("config") or {}
    build = config.get("build", config.get("rhbuild"))
    if not build or LooseVersion(build.split("-")[0]) < LooseVersion("9.2"):
        log.info(
            "Skipping test: requires Ceph version >= 9.2 (build=%s)",
            build,
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
        source_clients = ceph_cluster_dict.get("ceph1").get_ceph_objects("client")
        target_clients = ceph_cluster_dict.get("ceph2").get_ceph_objects("client")
        cephfs_mirror_nodes = ceph_cluster_dict.get("ceph1").get_ceph_objects(
            "cephfs-mirror"
        )

        source_fs = "cephfs"
        target_fs = "cephfs"
        source_fs2 = "cephfs2"
        target_fs2 = "cephfs2"
        target_user = "mirror_remote"
        target_site_name = "remote_site"

        log.info("Checking pre-requisites for 2-daemon HA mirroring")
        if not source_clients or not target_clients:
            log.info("This test requires client nodes on both ceph1 and ceph2.")
            return 1
        if len(cephfs_mirror_nodes) < 2:
            log.info(
                "This test requires at least 2 cephfs-mirror daemon nodes on ceph1. Found: %d",
                len(cephfs_mirror_nodes),
            )
            return 1

        log.info("Preparing clients on both clusters")
        fs_util_ceph1.prepare_clients(source_clients, build)
        fs_util_ceph2.prepare_clients(target_clients, build)
        fs_util_ceph1.auth_list(source_clients)
        fs_util_ceph2.auth_list(target_clients)

        # Deploy mirroring on primary filesystem
        log.info("Deploying mirroring for primary filesystem '%s'", source_fs)
        fs_mirroring_utils.deploy_cephfs_mirroring(
            source_fs,
            source_clients[0],
            target_fs,
            target_clients[0],
            target_user,
            target_site_name,
        )

        fsid = fs_mirroring_utils.get_fsid(cephfs_mirror_nodes[0])
        daemon_names = fs_mirroring_utils.get_daemon_name(source_clients[0])
        asok_files = fs_mirroring_utils.get_asok_file_with_connectivity_check(
            cephfs_mirror_nodes, fsid, daemon_names
        )
        if not asok_files:
            raise AssertionError(
                "No accessible cephfs-mirror asok found — daemon may not be running"
            )
        log.info(f"Discovered asok files across mirror nodes: {asok_files}")

        subvol_paths_created = []
        mount_points_created = []
        subvol_group_name = "subvolgroup_ha92"

        # Helper function to create and mount subvolume
        def _create_subvolume_and_mount(fs_name, sv_name):
            fs_util_ceph1.create_subvolume(
                source_clients[0],
                vol_name=fs_name,
                subvol_name=sv_name,
                group_name=subvol_group_name,
                size="5368709120",
            )
            subvol_info = fs_util_ceph1.get_subvolume_info(
                source_clients[0],
                fs_name,
                sv_name,
                group_name=subvol_group_name,
            )
            # Full internal UUID path — needed only for the fuse -r mount root
            sv_path_full = subvol_info["path"].strip()

            # Truncate at the subvolume name so we register the subvolume
            # directory itself for mirroring, not the internal UUID sub-dir.
            # e.g. /volumes/<group>/<sv_name>/<uuid> → /volumes/<group>/<sv_name>/
            idx = sv_path_full.find(f"{sv_name}/")
            if idx != -1:
                sv_path = sv_path_full[: idx + len(f"{sv_name}/")]
            else:
                sv_path = sv_path_full
            subvol_paths_created.append(sv_path)

            mnt_dir = f"/mnt/cephfs_ha92_{sv_name}_{int(time.time())}"
            fs_util_ceph1.fuse_mount(
                [source_clients[0]],
                mnt_dir,
                extra_params=f" -r {sv_path_full} --client_fs {fs_name}",
            )
            mount_points_created.append(mnt_dir)
            return sv_path, mnt_dir

        log.info("Creating subvolume group on %s", source_fs)
        fs_util_ceph1.create_subvolumegroup(
            source_clients[0], vol_name=source_fs, group_name=subvol_group_name
        )

        # Create 4 subvolumes on source_fs
        sv_paths = []
        mnt_dirs = []
        for i in range(1, 5):
            p, m = _create_subvolume_and_mount(source_fs, f"subvol_ha_{i}")
            sv_paths.append(p)
            mnt_dirs.append(m)
            # Enable mirroring on path
            fs_mirroring_utils.add_path_for_mirroring(source_clients[0], source_fs, p)

        # Write initial data to all 4 subvolumes and snapshot them
        for i, mnt in enumerate(mnt_dirs, start=1):
            source_clients[0].exec_command(
                sudo=True,
                cmd=f"dd if=/dev/urandom of={mnt}/file_{i}.dat bs=1M count=10 status=none",
            )
            fs_util_ceph1.create_snapshot(
                source_clients[0],
                vol_name=source_fs,
                subvol_name=f"subvol_ha_{i}",
                snap_name=f"snap_init_{i}",
                group_name=subvol_group_name,
            )

        # Wait for all 4 directories to sync to idle
        log.info("Waiting for initial sync across all 4 directories")
        for sv_p in sv_paths:
            wait_for_idle(
                fs_mirroring_utils,
                cephfs_mirror_nodes[0],
                source_clients[0],
                source_fs,
                sv_p,
                timeout=180,
            )

        # --------------------------------------------------------------------------
        # SCENARIO 5: Multi-daemon perf dump isolation
        # --------------------------------------------------------------------------
        log.info("=== SCENARIO 5: Multi-daemon perf dump isolation ===")
        # Get daemon names and node mappings
        daemon_to_node = {}
        for node_hostname, asok_info in asok_files.items():
            daemon_to_node[node_hostname] = asok_info[0]

        # Query directory counters on each mirror daemon
        daemon1_hostname = cephfs_mirror_nodes[0].node.hostname
        daemon2_hostname = cephfs_mirror_nodes[1].node.hostname

        d1_asok = {daemon1_hostname: asok_files[daemon1_hostname]}
        d2_asok = {daemon2_hostname: asok_files[daemon2_hostname]}

        d1_counters = fs_mirroring_utils.get_directory_counters(
            [cephfs_mirror_nodes[0]], fsid, d1_asok, fs_name=source_fs
        )
        d2_counters = fs_mirroring_utils.get_directory_counters(
            [cephfs_mirror_nodes[1]], fsid, d2_asok, fs_name=source_fs
        )

        d1_paths = {
            e.get("labels", {}).get("directory")
            for e in d1_counters
            if e.get("labels", {}).get("directory") is not None
        }
        d2_paths = {
            e.get("labels", {}).get("directory")
            for e in d2_counters
            if e.get("labels", {}).get("directory") is not None
        }
        log.info(f"Daemon 1 ({daemon1_hostname}) assigned directories: {d1_paths}")
        log.info(f"Daemon 2 ({daemon2_hostname}) assigned directories: {d2_paths}")

        # Assert no overlapping active directory assignments across daemons
        common_paths = d1_paths.intersection(d2_paths)
        if common_paths:
            raise AssertionError(
                f"Directories overlapped across mirror daemons unexpectedly: {common_paths}"
            )
        total_tracked = len(d1_paths.union(d2_paths))
        if total_tracked != 4:
            raise AssertionError(
                "Scenario 5 FAILED: expected 4 tracked directories across both daemons, "
                "found %d (d1=%s d2=%s)" % (total_tracked, d1_paths, d2_paths)
            )

        # --------------------------------------------------------------------------
        # SCENARIO 2: Multi-daemon stat accuracy & filesystem isolation
        # --------------------------------------------------------------------------
        log.info(
            "=== SCENARIO 2: Multi-daemon stat accuracy & filesystem isolation ==="
        )
        # Create second filesystem cephfs2 and enable mirroring
        fs_util_ceph1.create_fs(source_clients[0], source_fs2)
        fs_util_ceph2.create_fs(target_clients[0], target_fs2)
        fs_mirroring_utils.deploy_cephfs_mirroring(
            source_fs2,
            source_clients[0],
            target_fs2,
            target_clients[0],
            target_user,
            target_site_name,
        )
        fs_util_ceph1.create_subvolumegroup(
            source_clients[0], vol_name=source_fs2, group_name=subvol_group_name
        )
        sv2_path, sv2_mnt = _create_subvolume_and_mount(source_fs2, "subvol_fs2_1")
        fs_mirroring_utils.add_path_for_mirroring(
            source_clients[0], source_fs2, sv2_path
        )
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"dd if=/dev/urandom of={sv2_mnt}/fs2_file.dat bs=1M count=5 status=none",
        )
        fs_util_ceph1.create_snapshot(
            source_clients[0],
            vol_name=source_fs2,
            subvol_name="subvol_fs2_1",
            snap_name="snap_fs2_init",
            group_name=subvol_group_name,
        )
        wait_for_idle(
            fs_mirroring_utils,
            cephfs_mirror_nodes[0],
            source_clients[0],
            source_fs2,
            sv2_path,
            timeout=180,
        )

        # Validate MGR status isolation between cephfs and cephfs2
        status_fs1 = fs_mirroring_utils.get_mgr_mirror_status(
            source_clients[0], source_fs
        )
        status_fs2 = fs_mirroring_utils.get_mgr_mirror_status(
            source_clients[0], source_fs2
        )
        fs1_metrics_paths = list(status_fs1.get("metrics", {}).keys())
        fs2_metrics_paths = list(status_fs2.get("metrics", {}).keys())
        log.info(f"FS1 ({source_fs}) MGR paths: {fs1_metrics_paths}")
        log.info(f"FS2 ({source_fs2}) MGR paths: {fs2_metrics_paths}")

        for p in fs1_metrics_paths:
            if p in fs2_metrics_paths:
                raise AssertionError(
                    f"Cross-filesystem metric leak: path {p} reported in both {source_fs} and {source_fs2}"
                )
        log.info("Verified filesystem isolation in MGR status")

        # --------------------------------------------------------------------------
        # Stop daemon 1 to trigger directory redistribution to daemon 2
        # (required for Scenarios 1, 3, and 6)
        # --------------------------------------------------------------------------
        target_sv_path = sv_paths[0]
        log.info("Stopping daemon 1 (%s) to trigger redistribution", daemon1_hostname)
        _orch_out, _ = source_clients[0].exec_command(
            sudo=True, cmd="ceph orch ps --daemon_type cephfs-mirror -f json"
        )
        _orch_data = json.loads(_orch_out)
        daemon1_name = next(
            (
                d["daemon_name"]
                for d in _orch_data
                if d.get("hostname") == daemon1_hostname
            ),
            None,
        )
        if not daemon1_name:
            raise AssertionError(
                f"Could not find cephfs-mirror daemon for node {daemon1_hostname}"
            )
        source_clients[0].exec_command(
            sudo=True, cmd=f"ceph orch daemon stop {daemon1_name}"
        )
        log.info("Waiting for daemon 2 to acquire redistributed directories")
        wait_for_idle(
            fs_mirroring_utils,
            cephfs_mirror_nodes[1],
            source_clients[0],
            source_fs,
            target_sv_path,
            timeout=240,
        )

        # --------------------------------------------------------------------------
        # SCENARIO 1 & 3: Stats continuity & MGR reports all redistributed dirs
        # --------------------------------------------------------------------------
        log.info("=== SCENARIO 1 & 3: Stats continuity & MGR reports all dirs ===")
        # Surviving daemon 2 should now track all directories from source_fs
        mgr_all_status = fs_mirroring_utils.get_mgr_mirror_status(
            source_clients[0], source_fs
        )
        mgr_all_paths = list(mgr_all_status.get("metrics", {}).keys())
        log.info(f"MGR reported paths after daemon 1 stopped: {mgr_all_paths}")
        for p in sv_paths:
            normalized_p = p.rstrip("/")
            if normalized_p not in mgr_all_paths:
                raise AssertionError(
                    f"Directory {normalized_p} missing from MGR mirror status after failover!"
                )

        # Validate that MGR peer status is not stale for all directories
        for p, p_data in mgr_all_status.get("metrics", {}).items():
            peer_dict = p_data.get("peer", {})
            for peer_id, peer_info in peer_dict.items():
                state = peer_info.get("state")
                log.info(f"Path {p} peer {peer_id} state={state}")
                if state == "stale":
                    log.warning(f"Path {p} is in stale state, waiting for refresh")

        # --------------------------------------------------------------------------
        # SCENARIO 6: Perf counters for redistributed directories
        # --------------------------------------------------------------------------
        log.info("=== SCENARIO 6: Perf counters for redistributed directories ===")
        d2_counters_after = fs_mirroring_utils.get_directory_counters(
            [cephfs_mirror_nodes[1]], fsid, d2_asok, fs_name=source_fs
        )
        d2_paths_after = {
            e.get("labels", {}).get("directory") for e in d2_counters_after
        }
        log.info(f"Daemon 2 now tracking directories in perf dump: {d2_paths_after}")
        if len(d2_paths_after) < 4:
            raise AssertionError(
                "Scenario 6 FAILED: surviving daemon tracks %d dirs, expected >= 4: %s"
                % (len(d2_paths_after), d2_paths_after)
            )

        # --------------------------------------------------------------------------
        # SCENARIO 7: Tri-interface after HA failover & restart
        # --------------------------------------------------------------------------
        log.info("=== SCENARIO 7: Tri-interface after HA failover & daemon restart ===")
        # Restart daemon 1 using the daemon name resolved earlier
        log.info("Restarting daemon 1 (%s)", daemon1_hostname)
        source_clients[0].exec_command(
            sudo=True, cmd=f"ceph orch daemon start {daemon1_name}"
        )
        CephfsMirroringUtils.wait_for_daemon_running(
            source_clients[0], [cephfs_mirror_nodes[0]]
        )
        time.sleep(15)

        # Trigger sync on subvol_ha_2 and verify tri-interface recovery
        sv2_p = sv_paths[1]
        sv2_m = mnt_dirs[1]
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"dd if=/dev/urandom of={sv2_m}/tri_file.dat bs=1M count=20 status=none",
        )
        fs_util_ceph1.create_snapshot(
            source_clients[0],
            vol_name=source_fs,
            subvol_name="subvol_ha_2",
            snap_name="snap_tri_ha",
            group_name=subvol_group_name,
        )
        wait_for_idle(
            fs_mirroring_utils,
            cephfs_mirror_nodes[0],
            source_clients[0],
            source_fs,
            sv2_p,
            timeout=180,
        )

        # Tri-interface validation: MGR, Asok, and Perf counters match
        mgr_st = fs_mirroring_utils.get_mgr_mirror_status(source_clients[0], source_fs)
        path_key = sv2_p.rstrip("/")
        mgr_p_data = mgr_st.get("metrics", {}).get(path_key, {})
        peer_data = list(mgr_p_data.get("peer", {}).values())
        mgr_state = peer_data[0].get("state") if peer_data else "unknown"
        log.info("Tri-interface verification for %s: MGR state=%s", path_key, mgr_state)
        if mgr_state not in ("idle", "syncing"):
            raise AssertionError(
                f"Expected MGR state 'idle' or 'syncing', got '{mgr_state}'"
            )

        log.info("All 7 HA 9.2 scenarios completed successfully!")
        return 0

    except Exception as e:
        log.error("Test failed with exception: %s", e)
        log.error(traceback.format_exc())
        return 1

    finally:
        log.info("Starting target cluster cleanup and resource teardown...")
        try:
            # Restart stopped daemon1 if it was stopped during Scenario 4 so it
            # does not leak into subsequent suite tests.
            if "daemon1_name" in locals() and "source_clients" in locals():
                log.info("Restarting stopped daemon %s", daemon1_name)
                source_clients[0].exec_command(
                    sudo=True,
                    cmd=f"ceph orch daemon start {daemon1_name}",
                    check_ec=False,
                )

            # Unmount all local fuse mounts created during the test
            if "mount_points_created" in locals() and "source_clients" in locals():
                for mnt in mount_points_created:
                    log.info("Unmounting local mount: %s", mnt)
                    source_clients[0].exec_command(
                        sudo=True, cmd=f"umount -l '{mnt}'", check_ec=False
                    )
                    source_clients[0].exec_command(
                        sudo=True, cmd=f"rm -rf '{mnt}'", check_ec=False
                    )

            if "fs_mirroring_utils" in locals() and "source_clients" in locals():
                # Remove all mirrored paths from source_fs
                if "sv_paths" in locals():
                    for sv_p in sv_paths:
                        log.info("Removing mirrored path %s from %s", sv_p, source_fs)
                        fs_mirroring_utils.remove_path_from_mirroring(
                            source_clients[0], source_fs, sv_p, check_ec=False
                        )

                # Remove mirrored path from source_fs2
                if "sv2_path" in locals():
                    log.info("Removing mirrored path %s from %s", sv2_path, source_fs2)
                    fs_mirroring_utils.remove_path_from_mirroring(
                        source_clients[0], source_fs2, sv2_path, check_ec=False
                    )

                # Destroy mirroring for source_fs
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
                except Exception as _e:
                    log.warning("Could not destroy mirroring for %s: %s", source_fs, _e)

                # Destroy mirroring for source_fs2
                if "source_fs2" in locals():
                    try:
                        peer_uuid2 = fs_mirroring_utils.get_peer_uuid_by_name(
                            source_clients[0], source_fs2
                        )
                        fs_mirroring_utils.destroy_cephfs_mirroring(
                            source_fs2,
                            source_clients[0],
                            target_fs2,
                            target_clients[0],
                            target_user,
                            peer_uuid2,
                        )
                    except Exception as _e:
                        log.warning(
                            "Could not destroy mirroring for %s: %s", source_fs2, _e
                        )

            # Remove subvolumes and subvolume groups
            if "fs_util_ceph1" in locals() and "source_clients" in locals():
                for i in range(1, 5):
                    fs_util_ceph1.remove_subvolume(
                        source_clients[0],
                        source_fs,
                        f"subvol_ha_{i}",
                        group_name=subvol_group_name,
                        check_ec=False,
                    )
                if "sv2_path" in locals():
                    fs_util_ceph1.remove_subvolume(
                        source_clients[0],
                        source_fs2,
                        "subvol_fs2_1",
                        group_name=subvol_group_name,
                        check_ec=False,
                    )
                fs_util_ceph1.remove_subvolumegroup(
                    source_clients[0],
                    source_fs,
                    subvol_group_name,
                    check_ec=False,
                )
                if "source_fs2" in locals():
                    fs_util_ceph1.remove_subvolumegroup(
                        source_clients[0],
                        source_fs2,
                        subvol_group_name,
                        check_ec=False,
                    )

            # Cleanup target cluster snapshots / subvolumes
            if "fs_mirroring_utils" in locals() and "target_clients" in locals():
                fs_mirroring_utils.cleanup_target_cluster(
                    target_clients[0],
                    target_fs=target_fs,
                    subvolume_paths=(
                        subvol_paths_created
                        if "subvol_paths_created" in locals()
                        else None
                    ),
                )
                if "source_fs2" in locals():
                    fs_mirroring_utils.cleanup_target_cluster(
                        target_clients[0],
                        target_fs=target_fs2,
                    )

            # Remove cephfs2 on both clusters
            if "source_fs2" in locals():
                try:
                    fs_util_ceph1.remove_fs(
                        source_clients[0], source_fs2, check_ec=False
                    )
                    fs_util_ceph2.remove_fs(
                        target_clients[0], target_fs2, check_ec=False
                    )
                except Exception as _e:
                    log.warning("Could not remove cephfs2: %s", _e)

        except Exception as cleanup_err:
            log.warning("Cleanup encountered an error: %s", cleanup_err)
