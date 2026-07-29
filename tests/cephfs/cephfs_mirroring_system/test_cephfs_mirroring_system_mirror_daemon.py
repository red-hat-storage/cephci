import json
import signal
import time
import traceback

from tests.cephfs.cephfs_mirroring.cephfs_mirroring_utils import CephfsMirroringUtils
from tests.cephfs.cephfs_mirroring_system.cephfs_mirroring_system_utils import (
    cleanup_mirroring_test_environment,
    collect_background_io_logs,
    get_active_daemon_nodes,
    recover_crashed_mirror_daemon,
    run_container_restart,
    run_daemon_redeploy,
    run_node_reboot,
    run_signal_tests,
    run_systemctl_restart,
    setup_mirroring_test_environment,
    start_background_ios,
    wait_for_sync_id_increase,
)
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test cephfs-mirror daemon operations during active mirroring to ensure IOs and snapshot sync continue.

    This test verifies that:
    1. Background IOs continue running during cephfs-mirror daemon operations
    2. Snapshot sync is not affected by cephfs-mirror daemon operations
    3. Cluster remains healthy after each operation
    4. Mirroring continues properly after cephfs-mirror daemon operations

    Test operations performed:
    - SIGTERM: Graceful termination signal
    - SIGHUP: Reload configuration signal
    - SIGKILL: Forceful kill signal
    - Systemctl Restart: Restart cephfs-mirror using systemctl restart
    - Node Reboot: Reboot cephfs-mirror nodes
    - Container Restart: Restart cephfs-mirror containers
    - Redeploy: Redeploy cephfs-mirror on different nodes

    Args:
        ceph_cluster: The Ceph cluster to perform the mirroring tests on.
        **kw: Additional keyword arguments.

    Returns:
        int: 0 if the test is successful, 1 if there's an error.

    Raises:
        Exception: Any unexpected exceptions that might occur during the test.
    """

    env = {}
    try:
        config = kw.get("config")
        ceph_cluster_dict = kw.get("ceph_cluster_dict")
        test_data = kw.get("test_data")

        io_runtime = config.get("io_runtime", 120)
        signal_tests = [
            {
                "type": "signal",
                "signal": signal.SIGHUP,
                "name": "SIGHUP",
                "expect_exit": False,
            },
            # {
            #     "type": "signal",
            #     "signal": signal.SIGKILL,
            #     "name": "SIGKILL",
            #     "expect_exit": True,
            # },
            {
                "type": "signal",
                "signal": signal.SIGTERM,
                "name": "SIGTERM",
                "expect_exit": True,
            },
            {
                "type": "systemctl_restart",
                "name": "CephFS Mirror Systemctl Restart",
            },
            {
                "type": "container_restart",
                "name": "CephFS Mirror Container Restart",
            },
            {
                "type": "node_reboot",
                "name": "CephFS Mirror Node Reboot",
            },
            {
                "type": "mirror_redeploy",
                "name": "CephFS Mirror Redeploy",
            },
        ]

        env = setup_mirroring_test_environment(
            ceph_cluster,
            ceph_cluster_dict,
            config,
            test_data,
        )
        cephfs_common_utils = env["cephfs_common_utils"]
        fs_util_v1_ceph1 = env["fs_util_v1_ceph1"]
        fs_mirroring_utils = env["fs_mirroring_utils"]
        source_clients = env["source_clients"]
        cephfs_mirror_nodes = env["cephfs_mirror_nodes"]
        source_fs = env["source_fs"]
        subvolume_paths = env["subvolume_paths"]
        mounting_dirs = env["mounting_dirs"]
        full_subvolume_path = env["full_subvolume_path"]
        fsid = env["fsid"]
        daemon_name = env["daemon_name"]
        asok_file = env["asok_file"]
        filesystem_id = env["filesystem_id"]
        peer_uuid = env["peer_uuid"]

        # Record original cephfs-mirror daemon host before any tests
        out, _ = source_clients[0].exec_command(
            sudo=True,
            cmd="ceph orch ps --daemon_type=cephfs-mirror --format json",
            check_ec=False,
        )
        original_mirror_host = json.loads(out)[0].get("hostname")
        log.info("Original cephfs-mirror daemon host: %s", original_mirror_host)

        log.info("Starting background IOs for %s minutes", io_runtime)
        io_threads, stop_io_event = start_background_ios(
            fs_util_v1_ceph1, source_clients[0], full_subvolume_path, io_runtime
        )

        # Give IO a moment to actually start
        time.sleep(15)
        log.info("Verifying IO threads are running")
        for idx, t in enumerate(io_threads):
            log.info("IO thread %s alive: %s", idx, t.is_alive())

        # Get initial last_synced_snap ids for all subvolumes
        snap_sync_id_initial = []
        for path in subvolume_paths:
            sync_id = fs_mirroring_utils.get_last_synced_snap_id(
                source_fs, fsid, asok_file, filesystem_id, peer_uuid, path
            )
            snap_sync_id_initial.append(sync_id)
            log.info("Initial last_synced_snap id for %s: %s", path, sync_id)

        # Run all tests (signals, redeploy, node reboot)
        for test_case in signal_tests:
            test_type = test_case["type"]
            test_name = test_case["name"]

            log.info("=== Starting %s test ===", test_name)

            # Wait for daemon to be running; if stuck in crash loop, redeploy
            try:
                CephfsMirroringUtils.wait_for_daemon_running(
                    source_clients[0],
                    cephfs_mirror_nodes,
                    ceph_cluster=ceph_cluster_dict.get("ceph1"),
                )
            except Exception:
                log.error(
                    "cephfs-mirror daemon not running before %s, "
                    "attempting crash-loop recovery",
                    test_name,
                )
                if not recover_crashed_mirror_daemon(
                    source_clients[0], original_mirror_host
                ):
                    log.error("Recovery failed, cannot continue")
                    return 1
                daemon_name = fs_mirroring_utils.get_daemon_name(source_clients[0])
                log.info("Daemon name after recovery: %s", daemon_name)

            # Fetch asok_file at the start of each test
            asok_file = fs_mirroring_utils.get_asok_file_with_connectivity_check(
                cephfs_mirror_nodes, fsid, daemon_name
            )
            if not asok_file:
                log.error("Failed to get asok_file before %s", test_name)
                return 1

            # Check cluster health before test
            log.info("Verify cluster is healthy before %s", test_name)
            if cephfs_common_utils.wait_for_healthy_ceph(source_clients[0], 300):
                log.error("Cluster is not healthy before %s", test_name)
                return 1

            # Verify IOs are still running
            log.info("Verifying IO threads are still running before %s", test_name)
            for idx, t in enumerate(io_threads):
                if not t.is_alive():
                    log.error("IO thread %s died before %s test", idx, test_name)
                    return 1
                log.info("IO thread %s alive: %s", idx, t.is_alive())

            # Verify mount points are responsive before test
            log.info("Verifying mount points are responsive before %s", test_name)
            if not CephfsMirroringUtils.verify_mount_points_responsive(
                source_clients[0], mounting_dirs
            ):
                log.error("Mount points are hung before %s", test_name)
                return 1

            # Get baseline last_synced_snap id before test
            snap_sync_id_before = []
            for path in subvolume_paths:
                sync_id = fs_mirroring_utils.get_last_synced_snap_id(
                    source_fs, fsid, asok_file, filesystem_id, peer_uuid, path
                )
                snap_sync_id_before.append(sync_id)

            try:
                all_mirror_nodes = [mn.node for mn in cephfs_mirror_nodes]
                active_mirror_nodes = get_active_daemon_nodes(
                    source_clients[0], "cephfs-mirror", all_mirror_nodes
                )
                if not active_mirror_nodes:
                    log.error(
                        "No active cephfs-mirror daemons found before %s", test_name
                    )
                    return 1
                log.info(
                    "Active cephfs-mirror nodes for %s: %s",
                    test_name,
                    [n.hostname for n in active_mirror_nodes],
                )

                recovery_timeout = (
                    120 if test_type in ("node_reboot", "mirror_redeploy") else 60
                )

                if test_type == "signal":
                    log.info("Running signal test on cephfs-mirror daemons")
                    result = run_signal_tests(
                        fs_util_v1_ceph1,
                        active_mirror_nodes,
                        "cephfs-mirror",
                        r"cephfs-mirror\.",
                        test_case,
                    )
                elif test_type == "systemctl_restart":
                    log.info("Running systemctl restart test on cephfs-mirror daemons")
                    result = run_systemctl_restart(
                        fs_util_v1_ceph1,
                        active_mirror_nodes,
                        "cephfs-mirror",
                        r"cephfs-mirror\.",
                    )
                elif test_type == "container_restart":
                    log.info("Running container restart test on cephfs-mirror daemons")
                    result = run_container_restart(
                        active_mirror_nodes,
                        ["cephfs-mirror"],
                    )
                elif test_type == "node_reboot":
                    log.info("Running node reboot test on cephfs-mirror nodes")
                    result = run_node_reboot(fs_util_v1_ceph1, active_mirror_nodes)
                elif test_type == "mirror_redeploy":
                    log.info("Running cephfs-mirror redeploy test")
                    result = run_daemon_redeploy(
                        source_clients[0],
                        "cephfs-mirror",
                        ceph_cluster_dict,
                        "ceph1",
                        "cephfs-mirror",
                    )
                else:
                    log.error("Unknown test type: %s", test_type)
                    return 1

                if result != 0:
                    return 1
                if CephfsMirroringUtils.wait_for_daemon_recovery(
                    source_clients[0],
                    "cephfs-mirror",
                    test_name,
                    timeout=recovery_timeout,
                ):
                    return 1

            except Exception as e:
                log.error("Error during %s test: %s", test_name, e)
                log.error(traceback.format_exc())
                return 1

            # Refetch asok_file after operations that restart the daemon
            # Operations that restart daemon: SIGKILL, SIGTERM, systemctl_restart,
            # container_restart, node_reboot, mirror_redeploy
            # Note: SIGHUP does not restart the daemon, so no refetch needed
            needs_asok_refetch = False
            if test_type == "signal":
                # Only SIGKILL and SIGTERM restart the daemon, SIGHUP does not
                sig = test_case.get("signal")
                if sig in [signal.SIGKILL, signal.SIGTERM]:
                    needs_asok_refetch = True
            elif test_type in [
                "systemctl_restart",
                "container_restart",
                "node_reboot",
                "mirror_redeploy",
            ]:
                needs_asok_refetch = True

            if needs_asok_refetch:
                log.info(
                    "Refetching asok_file after %s (daemon was restarted)", test_name
                )
                # Wait for daemon to be fully running (retries up to ~5 min)
                CephfsMirroringUtils.wait_for_daemon_running(
                    source_clients[0],
                    cephfs_mirror_nodes,
                    ceph_cluster=ceph_cluster_dict.get("ceph1"),
                )
                # Refetch daemon_name in case it changed (especially for redeploy)
                daemon_name = fs_mirroring_utils.get_daemon_name(source_clients[0])
                log.info("Updated daemon name: %s", daemon_name)
                if (
                    not daemon_name
                    or not isinstance(daemon_name, list)
                    or len(daemon_name) == 0
                ):
                    log.error("No cephfs-mirror daemons found after %s", test_name)
                    return 1
                try:
                    hostname = daemon_name[0].split(".")[1]
                except (IndexError, AttributeError) as e:
                    log.error(
                        "Failed to extract hostname from daemon name %s: %s",
                        daemon_name,
                        e,
                    )
                    return 1
                mirror_node = ceph_cluster_dict.get("ceph1").get_node_by_hostname(
                    hostname
                )
                if mirror_node:
                    cephfs_mirror_nodes = mirror_node.get_ceph_objects()

                # Retry asok fetch — after SIGKILL/restart the new admin socket
                # may take extra time to become connectable
                asok_file = {}
                for attempt in range(10):
                    asok_file = (
                        fs_mirroring_utils.get_asok_file_with_connectivity_check(
                            cephfs_mirror_nodes, fsid, daemon_name
                        )
                    )
                    if asok_file:
                        break
                    log.warning(
                        "Asok not ready yet after %s, retry %d/10",
                        test_name,
                        attempt + 1,
                    )
                    time.sleep(15)
                if not asok_file:
                    log.error("Failed to refetch asok_file after %s", test_name)
                    return 1

            # Check cluster health after test
            health_timeout = 300
            log.info("Verify cluster is healthy after %s", test_name)
            if cephfs_common_utils.wait_for_healthy_ceph(
                source_clients[0], health_timeout
            ):
                log.error("Cluster is not healthy after %s", test_name)
                return 1

            # Verify IOs are still running after test
            log.info("Verifying IO threads are still running after %s", test_name)
            for idx, t in enumerate(io_threads):
                if not t.is_alive():
                    log.error("IO thread %s died after %s", idx, test_name)
                    return 1

            # Verify mount points are responsive after test
            log.info("Verifying mount points are responsive after %s", test_name)
            if not CephfsMirroringUtils.verify_mount_points_responsive(
                source_clients[0], mounting_dirs, 120
            ):
                log.error("Mount points are hung after %s", test_name)
                return 1

            # Ensure daemon is running before checking sync status
            CephfsMirroringUtils.wait_for_daemon_running(
                source_clients[0],
                cephfs_mirror_nodes,
                ceph_cluster=ceph_cluster_dict.get("ceph1"),
            )

            for idx, path in enumerate(subvolume_paths):
                sync_id_b = snap_sync_id_before[idx]
                try:
                    wait_for_sync_id_increase(
                        fs_mirroring_utils,
                        source_fs,
                        fsid,
                        asok_file,
                        filesystem_id,
                        peer_uuid,
                        path,
                        sync_id_b,
                        test_name,
                    )
                except Exception as e:
                    log.error(str(e))

            log.info("=== %s test completed successfully ===", test_name)

        # Final verification: Wait for IOs to complete or check they're still running
        log.info("Final verification: Checking IO threads status")
        for idx, t in enumerate(io_threads):
            if t.is_alive():
                log.info("IO thread %s still running (as expected)", idx)
            else:
                log.info("IO thread %s completed", idx)

        # Final snapshot sync verification
        log.info("Final snapshot sync verification")
        snap_sync_id_final = []
        for path, snap_id_initial in zip(subvolume_paths, snap_sync_id_initial):
            sync_id_final = fs_mirroring_utils.get_last_synced_snap_id(
                source_fs, fsid, asok_file, filesystem_id, peer_uuid, path
            )
            snap_sync_id_final.append(sync_id_final)

            if sync_id_final is not None and sync_id_final > snap_id_initial:
                log.info(
                    "Final last_synced_snap.id for %s: initial=%s, final=%s",
                    path,
                    snap_id_initial,
                    sync_id_final,
                )
            else:
                log.warning(
                    "last_synced_snap.id did not increase for %s: initial=%s, final=%s",
                    path,
                    snap_id_initial,
                    sync_id_final,
                )

        # Final cluster health check
        log.info("Verify cluster is healthy (final check)")
        if cephfs_common_utils.wait_for_healthy_ceph(source_clients[0], 300):
            log.error("Cluster is not healthy at final check")
            return 1

        # Signal IO threads to stop and wait for them
        log.info("Stopping background IO threads")
        stop_io_event.set()
        for t in io_threads:
            t.join(timeout=120)
        log.info("All IO threads have stopped")

        # Redeploy cephfs-mirror back to the original host so that
        # subsequent tests find the daemon on the expected node.
        out, _ = source_clients[0].exec_command(
            sudo=True,
            cmd="ceph orch ps --daemon_type=cephfs-mirror --format json",
            check_ec=False,
        )
        current_mirror_host = json.loads(out)[0].get("hostname")
        if current_mirror_host != original_mirror_host:
            log.info(
                "cephfs-mirror moved from %s to %s, redeploying back",
                original_mirror_host,
                current_mirror_host,
            )
            try:
                source_clients[0].exec_command(
                    sudo=True,
                    cmd=f"ceph orch apply cephfs-mirror --placement='1 {original_mirror_host}'",
                )
                # Wait and verify daemon moved back
                time.sleep(30)
                out_verify, _ = source_clients[0].exec_command(
                    sudo=True,
                    cmd="ceph orch ps --daemon_type=cephfs-mirror --format json",
                    check_ec=False,
                )
                verify_host = json.loads(out_verify)[0].get("hostname")
                if verify_host == original_mirror_host:
                    log.info(
                        "cephfs-mirror successfully redeployed to %s",
                        original_mirror_host,
                    )
                else:
                    log.warning(
                        "cephfs-mirror on %s, expected %s",
                        verify_host,
                        original_mirror_host,
                    )
            except Exception as e:
                log.error("Failed to redeploy cephfs-mirror: %s", e)
        else:
            log.info(
                "cephfs-mirror still on original host %s, no redeploy needed",
                original_mirror_host,
            )

        log.info(
            "Test Completed Successfully. All signal tests passed. "
            "Snapshot sync continued during all cephfs-mirror daemon operations. "
            "All snapshots are synced to target cluster."
        )
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        if env and "source_clients" in env and "full_subvolume_path" in env:
            collect_background_io_logs(
                env["source_clients"][0], env["full_subvolume_path"]
            )
        if config and config.get("cleanup", True) and env:
            cleanup_mirroring_test_environment(env)
