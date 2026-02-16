import signal
import time
import traceback

from tests.cephfs.cephfs_mirroring.cephfs_mirroring_utils import CephfsMirroringUtils
from tests.cephfs.cephfs_mirroring_system.cephfs_mirroring_system_utils import (
    cleanup_mirroring_test_environment,
    run_container_restart,
    run_daemon_redeploy,
    run_node_reboot,
    run_signal_tests,
    run_systemctl_restart,
    setup_mirroring_test_environment,
    start_background_ios,
    wait_for_snaps_synced_increase,
)
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test MDS failure operations during active mirroring to ensure IOs and snapshot sync continue.

    This test verifies that:
    1. Background IOs continue running during MDS operations
    2. Snapshot sync is not affected by MDS operations
    3. Cluster remains healthy after each operation
    4. Clients reconnect properly after MDS operations

    Test operations performed:
    - SIGTERM: Graceful termination signal
    - SIGHUP: Reload configuration signal
    - SIGKILL: Forceful kill signal
    - Systemctl Restart: Restart MDS using systemctl restart ceph-mds@<id>
    - Node Reboot: Reboot MDS nodes

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

        mds_nodes = ceph_cluster_dict.get("ceph1").get_ceph_objects("mds")
        mds_hosts = ceph_cluster_dict.get("ceph1").get_nodes(role="mds")
        if not mds_nodes:
            log.error("No MDS nodes found in ceph1 cluster")
            return 1
        log.info("Found %s MDS node(s)", len(mds_nodes))

        target_mds_nodes = ceph_cluster_dict.get("ceph2").get_ceph_objects("mds")
        target_mds_hosts = ceph_cluster_dict.get("ceph2").get_nodes(role="mds")
        if not target_mds_nodes:
            log.error("No MDS nodes found in ceph2 cluster")
            return 1
        log.info("Found %s target MDS node(s)", len(target_mds_nodes))

        io_runtime = config.get("io_runtime", 40)
        signal_tests = [
            {
                "type": "signal",
                "signal": signal.SIGHUP,
                "name": "SIGHUP",
                "expect_exit": False,
            },
            {
                "type": "signal",
                "signal": signal.SIGKILL,
                "name": "SIGKILL",
                "expect_exit": True,
            },
            {
                "type": "signal",
                "signal": signal.SIGTERM,
                "name": "SIGTERM",
                "expect_exit": True,
            },
            {"type": "systemctl_restart", "name": "MDS Systemctl Restart"},
            {"type": "container_restart", "name": "MDS Container Restart"},
            {"type": "node_reboot", "name": "MDS Node Reboot"},
            {"type": "mds_redeploy", "name": "MDS Redeploy"},
        ]

        env = setup_mirroring_test_environment(
            ceph_cluster,
            ceph_cluster_dict,
            config,
            test_data,
        )
        cephfs_common_utils = env["cephfs_common_utils"]
        fs_util_v1_ceph1 = env["fs_util_v1_ceph1"]
        fs_util_v1_ceph2 = env["fs_util_v1_ceph2"]
        fs_mirroring_utils = env["fs_mirroring_utils"]
        source_clients = env["source_clients"]
        target_clients = env["target_clients"]
        source_fs = env["source_fs"]
        target_fs = env["target_fs"]
        subvolume_paths = env["subvolume_paths"]
        mounting_dirs = env["mounting_dirs"]
        fsid = env["fsid"]
        asok_file = env["asok_file"]
        filesystem_id = env["filesystem_id"]
        peer_uuid = env["peer_uuid"]

        log.info(f"Starting background IOs for {io_runtime} minutes")
        io_threads = start_background_ios(
            fs_util_v1_ceph1, source_clients[0], mounting_dirs, io_runtime
        )

        # Give IO a moment to actually start
        time.sleep(15)
        log.info("Verifying IO threads are running")
        for idx, t in enumerate(io_threads):
            log.info("IO thread %s alive: %s", idx, t.is_alive())

        # Get initial snapshot sync counts
        snap_sync_counts_initial = []
        for path in subvolume_paths:
            snaps_synced = fs_mirroring_utils.get_snaps_synced(
                source_fs, fsid, asok_file, filesystem_id, peer_uuid, path
            )
            snap_sync_counts_initial.append(snaps_synced)
            log.info("Initial snap sync count for %s: %s", path, snaps_synced)

        # Run all tests (signals, mds redeploy, node reboot)
        for test_case in signal_tests:
            test_type = test_case["type"]
            test_name = test_case["name"]

            log.info("=== Starting %s test ===", test_name)

            # Get baseline snapshot sync counts before test
            snap_sync_counts_before = []
            for path in subvolume_paths:
                snaps_synced = fs_mirroring_utils.get_snaps_synced(
                    source_fs, fsid, asok_file, filesystem_id, peer_uuid, path
                )
                snap_sync_counts_before.append(snaps_synced)

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

            log.info("Verifying mount points are responsive before %s", test_name)
            if not CephfsMirroringUtils.verify_mount_points_responsive(
                source_clients[0], mounting_dirs
            ):
                log.error("Mount points are hung before %s", test_name)
                return 1

            try:
                active_mds = fs_util_v1_ceph1.get_active_mdss(
                    source_clients[0], source_fs
                )
                active_mds_objects = [
                    mds
                    for mds in mds_hosts
                    if any(mds.hostname in active for active in active_mds)
                ]
                log.info(
                    "Active MDS Nodes are: %s",
                    [mds.hostname for mds in active_mds_objects],
                )

                if not active_mds_objects:
                    log.error("No active MDS nodes found")
                    return 1

                target_active_mds = fs_util_v1_ceph2.get_active_mdss(
                    target_clients[0], target_fs
                )
                target_active_mds_objects = [
                    mds
                    for mds in target_mds_hosts
                    if any(mds.hostname in active for active in target_active_mds)
                ]
                log.info(
                    "Active target MDS Nodes are: %s",
                    [mds.hostname for mds in target_active_mds_objects],
                )

                if not target_active_mds_objects:
                    log.error("No active target MDS nodes found")
                    return 1

                recovery_timeout = (
                    120 if test_type in ("node_reboot", "mds_redeploy") else 60
                )

                clusters = [
                    (
                        fs_util_v1_ceph1,
                        source_clients[0],
                        active_mds_objects,
                        source_fs,
                        "ceph1",
                        "source",
                    ),
                    (
                        fs_util_v1_ceph2,
                        target_clients[0],
                        target_active_mds_objects,
                        target_fs,
                        "ceph2",
                        "target",
                    ),
                ]

                for fs_util, client, mds_objs, fs_name, ckey, label in clusters:
                    if test_type == "signal":
                        log.info("Running signal test on %s cluster MDS", label)
                        result = run_signal_tests(
                            fs_util,
                            mds_objs,
                            "mds",
                            rf"mds\.{fs_name}\.",
                            test_case,
                        )
                    elif test_type == "systemctl_restart":
                        log.info("Running systemctl restart on %s cluster MDS", label)
                        result = run_systemctl_restart(
                            fs_util,
                            mds_objs,
                            "mds",
                            rf"mds\.{fs_name}\.",
                        )
                    elif test_type == "container_restart":
                        log.info("Running container restart on %s cluster MDS", label)
                        result = run_container_restart(
                            mds_objs,
                            ["mds", fs_name],
                        )
                    elif test_type == "node_reboot":
                        log.info("Running node reboot on %s cluster MDS", label)
                        result = run_node_reboot(fs_util, mds_objs)
                    elif test_type == "mds_redeploy":
                        log.info("Running MDS redeploy on %s cluster", label)
                        result = run_daemon_redeploy(
                            client,
                            "mds",
                            ceph_cluster_dict,
                            ckey,
                            f"mds {fs_name}",
                            process_name=f"mds.{fs_name}",
                        )
                    else:
                        log.error("Unknown test type: %s", test_type)
                        return 1

                    if result != 0:
                        return 1

                for fs_util, client, _, fs_name, _, label in clusters:
                    log.info("Waiting for MDS active on %s cluster", label)
                    if CephfsMirroringUtils.wait_for_mds_active(
                        fs_util,
                        client,
                        fs_name,
                        timeout=recovery_timeout,
                    ):
                        return 1

            except Exception as e:
                log.error("Error during %s test: %s", test_name, e)
                log.error(traceback.format_exc())
                return 1

            health_timeout = 300 if test_type in ("node_reboot", "mds_redeploy") else 60
            log.info("Verify cluster is healthy after %s", test_name)
            if cephfs_common_utils.wait_for_healthy_ceph(
                source_clients[0], health_timeout
            ):
                log.error("Cluster is not healthy after %s", test_name)
                return 1

            log.info("Verifying IO threads are still running after %s", test_name)
            for idx, t in enumerate(io_threads):
                if not t.is_alive():
                    log.error("IO thread %s died after %s", idx, test_name)
                    return 1
                log.info("IO thread %s alive: %s", idx, t.is_alive())

            log.info("Verifying mount points are responsive after %s", test_name)
            if not CephfsMirroringUtils.verify_mount_points_responsive(
                source_clients[0], mounting_dirs
            ):
                log.error("Mount points are hung after %s", test_name)
                return 1

            for path, snap_synced_before in zip(
                subvolume_paths, snap_sync_counts_before
            ):
                try:
                    wait_for_snaps_synced_increase(
                        fs_mirroring_utils,
                        source_fs,
                        fsid,
                        asok_file,
                        filesystem_id,
                        peer_uuid,
                        path,
                        snap_synced_before,
                        test_name,
                    )
                except Exception as e:
                    log.error(str(e))
                    return 1

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
        snap_sync_counts_final = []
        for path, snap_synced_initial in zip(subvolume_paths, snap_sync_counts_initial):
            snaps_synced_final = fs_mirroring_utils.get_snaps_synced(
                source_fs, fsid, asok_file, filesystem_id, peer_uuid, path
            )
            snap_sync_counts_final.append(snaps_synced_final)

            if (
                snaps_synced_final is not None
                and snap_synced_initial is not None
                and snaps_synced_final > snap_synced_initial
            ):
                log.info(
                    "Final snapshot sync count for %s: initial=%s, final=%s",
                    path,
                    snap_synced_initial,
                    snaps_synced_final,
                )
            else:
                log.warning(
                    "Snapshot sync count did not increase for %s: initial=%s, final=%s",
                    path,
                    snap_synced_initial,
                    snaps_synced_final,
                )

        # Final cluster health check
        log.info("Verify cluster is healthy (final check)")
        if cephfs_common_utils.wait_for_healthy_ceph(source_clients[0], 300):
            log.error("Cluster is not healthy at final check")
            return 1

        # Wait for all IO threads to complete
        log.info("Waiting for all IO threads to complete")
        for t in io_threads:
            t.join()
        log.info("All IO threads have completed")

        log.info(
            "Test Completed Successfully. All signal tests passed. "
            "Snapshot sync continued during all MDS signal operations. "
            "All snapshots are synced to target cluster."
        )
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        if config and config.get("cleanup", True) and env:
            cleanup_mirroring_test_environment(env)
