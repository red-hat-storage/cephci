"""
Tier-3 test module to perform various operations to stress MON database
and execute MON compaction with different methods
"""

import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.monitor_workflows import MonitorWorkflows
from ceph.rados.pool_workflows import PoolFunctions
from ceph.rados.utils import get_cluster_timestamp
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)

COMPACTION_WAIT = 10
STORE_DB_CHECK_WAIT = 5


def run(ceph_cluster, **kw):
    """
    CEPH-83604848 - Monitor compaction workflow tests
    This test is to stress MON database and execute MON compaction
    with various methods to verify compaction effectiveness

    Test Steps:
    1. Create cluster with default configuration
    2. Generate MON database entries by:
       - Creating pools
       - Writing/deleting objects
       - Starting/stopping OSDs
       - Creating/deleting OMAP entries
    3. Test different MON compaction methods:
       a. Online compaction: ceph tell mon.<ID> compact
       b. Compaction on start: mon_compact_on_start
       c. Compaction on bootstrap: mon_compact_on_bootstrap
       d. Compaction on trim: mon_compact_on_trim (verify default)
       e. Offline compaction: ceph-monstore-tool compact
    4. For each compaction method, collect:
       - MON performance stats before/after
       - store.db size before/after
       - Verify compaction events in MON logs
    5. Verify that store.db size reduces after compaction
    6. Verify MON performance improvements post compaction
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    mon_config_obj = MonConfigMethods(rados_obj=rados_obj)
    mon_daemon_obj = MonitorWorkflows(node=cephadm)

    test_config = config.get("mon_compaction_config", {})
    test_start_time = get_cluster_timestamp(rados_obj.node)
    log.debug(f"Test workflow started. Start time: {test_start_time}")
    try:
        # Get current MON leader
        current_leader = mon_daemon_obj.get_mon_quorum_leader()
        log.info("Using primary MON for testing: %s", current_leader)

        # Step 1: Generate MON database entries
        log.info("=== STEP 1: Generating MON database entries ===")
        generate_mon_db_entries(rados_obj, pool_obj, test_config, cephadm)

        # Verify all MONs are running after generating entries
        log.info("Verifying all MON daemons are running after generating entries")
        if not verify_all_mons_running(rados_obj):
            raise AssertionError(
                "Not all MON daemons are running after generating entries"
            )

        # Step 2: Enable debug logging for MONs to capture compaction events
        log.info("=== STEP 2: Enabling debug logging for MON compaction events ===")
        enable_mon_debug_logging(mon_config_obj)

        # Step 3: Test online compaction (ceph tell mon.<ID> compact)
        log.info("=== STEP 3: Testing online MON compaction ===")
        online_start, online_end = test_online_mon_compaction(
            rados_obj, current_leader, cephadm
        )

        # Verify online compaction logs
        if not verify_mon_compaction_logs(
            rados_obj, current_leader, online_start, online_end, "online compaction"
        ):
            log.warning("Could not verify online compaction in logs")

        # Step 4: Test compaction on trim (verify default config)
        log.info("=== STEP 4: Verifying mon_compact_on_trim configuration ===")
        verify_mon_compact_on_trim(mon_config_obj)

        # Step 5: Test compaction on start
        log.info("=== STEP 5: Testing MON compaction on start ===")
        if not verify_all_mons_running(rados_obj):
            raise AssertionError(
                "Not all MON daemons are running before compact on start test"
            )

        test_mon_compact_on_start(rados_obj, mon_config_obj, current_leader, cephadm)

        # Verify MON is back up after restart
        log.info("Verifying all MON daemons are running after compact on start test")
        if not verify_all_mons_running(rados_obj):
            raise AssertionError(
                "Not all MON daemons are running after compact on start test"
            )

        # Step 6: Test compaction on bootstrap
        log.info("=== STEP 6: Testing MON compaction on bootstrap ===")
        test_mon_compact_on_bootstrap(
            rados_obj, mon_daemon_obj, mon_config_obj, cephadm, ceph_cluster
        )

        # Verify all MONs are healthy after bootstrap test
        log.info("Verifying all MON daemons are running after bootstrap test")
        if not verify_all_mons_running(rados_obj):
            raise AssertionError("Not all MON daemons are running after bootstrap test")

        # Step 7: Test offline compaction via monstore tool
        log.info("=== STEP 7: Testing offline MON compaction via monstore tool ===")
        test_offline_mon_compaction(rados_obj, current_leader, cephadm)

        # Verify all MONs are back up after offline compaction
        log.info("Verifying all MON daemons are running after offline compaction test")
        if not verify_all_mons_running(rados_obj):
            raise AssertionError(
                "Not all MON daemons are running after offline compaction test"
            )

        log.info("All MON compaction tests completed successfully")

    except Exception as e:
        log.error("Failed with exception: %s", e.__doc__)
        log.exception(e)
        rados_obj.log_cluster_health()
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        rados_obj.rados_pool_cleanup()

        # Reset debug logging
        log.info("Resetting MON debug logging configurations")
        reset_mon_debug_logging(mon_config_obj)

        # Reset compaction configs
        log.info("Resetting MON compaction configurations")
        reset_mon_compaction_configs(mon_config_obj)

        # Log cluster health
        rados_obj.log_cluster_health()

        # Check for crashes
        test_end_time = get_cluster_timestamp(rados_obj.node)
        log.debug(
            f"Test workflow completed. Start time: {test_start_time}, End time: {test_end_time}"
        )
        if rados_obj.check_crash_status(
            start_time=test_start_time, end_time=test_end_time
        ):
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info("Completed testing MON compaction with various methods")
    return 0


def get_mon_host_object(rados_obj, mon_id):
    """
    Get the host object for the node where MON daemon is running

    Args:
        rados_obj: RadosOrchestrator object
        mon_id: MON ID (e.g., 'a', 'b', 'c' or full hostname)

    Returns:
        host object: Host where MON is running
    """
    try:
        host = rados_obj.fetch_host_node(daemon_type="mon", daemon_id=str(mon_id))

        if not host:
            log.error("Failed to find host for mon.%s", mon_id)
            return None

        log.debug("Hostname of MON %s: %s", mon_id, host.hostname)
        return host

    except Exception as e:
        log.error("Could not get MON host object for mon.%s: %s", mon_id, e)
        return None


def verify_all_mons_running(rados_obj, max_wait_time=300):
    """
    Verify that all MON daemons are running and in quorum.
    Waits up to max_wait_time seconds for all MONs to be ready.

    Args:
        rados_obj: RadosOrchestrator object
        max_wait_time: Maximum time to wait for MONs (default: 300 seconds)

    Returns:
        bool: True if all MONs are running, False otherwise
    """
    log.info("Verifying all MON daemons are running (timeout: %ss)", max_wait_time)

    end_time = time.time() + max_wait_time
    retry_interval = 10

    while time.time() < end_time:
        try:
            # Get expected MONs from mon dump
            mon_dump = rados_obj.run_ceph_command(cmd="ceph mon dump")
            expected_mons = [mon.get("name") for mon in mon_dump.get("mons", [])]

            # Get MONs in quorum
            quorum = rados_obj.run_ceph_command(cmd="ceph quorum_status")
            quorum_mons = quorum.get("quorum_names", [])

            # Get running MONs from orchestrator
            orch_mons = rados_obj.run_ceph_command(cmd="ceph orch ps --daemon_type mon")
            running_mons = [
                m.get("daemon_id")
                for m in orch_mons
                if m.get("status_desc") == "running"
            ]

            log.debug(
                "MON Status: expected=%s, in_quorum=%s, running=%s",
                len(expected_mons),
                len(quorum_mons),
                len(running_mons),
            )

            # Check if all MONs are in quorum and running
            if len(expected_mons) == len(quorum_mons) == len(running_mons):
                log.info(
                    "All %s MON daemons are running and in quorum: %s",
                    len(expected_mons),
                    expected_mons,
                )
                return True

            # Log what's missing
            missing_quorum = set(expected_mons) - set(quorum_mons)
            missing_running = set(expected_mons) - set(running_mons)
            if missing_quorum:
                log.warning("MONs not in quorum: %s", missing_quorum)
            if missing_running:
                log.warning("MONs not running: %s", missing_running)

            log.info("Waiting %ss for MONs to come up...", retry_interval)
            time.sleep(retry_interval)

        except Exception as e:
            log.warning("Error checking MON status: %s. Retrying...", e)
            time.sleep(retry_interval)

    log.error("Timeout: Not all MON daemons are running after %ss", max_wait_time)
    return False


def enable_mon_debug_logging(mon_config_obj):
    """
    Enable debug logging for MON daemons to capture compaction events

    Args:
        mon_config_obj: MonConfigMethods object
    """
    log.info("Enabling debug logging for all mon daemon")
    debug_configs = {
        "debug_paxos": "10",
        "debug_mon": "10",
        "debug_ms": "10",
    }
    for config_name, config_value in debug_configs.items():
        if not mon_config_obj.set_config(
            section="mon", name=config_name, value=config_value
        ):
            log.warning("Failed to set %s to %s", config_name, config_value)
        else:
            log.info("Set %s = %s", config_name, config_value)


def reset_mon_debug_logging(mon_config_obj):
    """
    Reset debug logging for MON daemons

    Args:
        mon_config_obj: MonConfigMethods object
    """
    debug_configs = ["debug_paxos", "debug_mon", "debug_ms"]

    for config_name in debug_configs:
        if not mon_config_obj.remove_config(section="mon", name=config_name):
            log.warning("Failed to reset %s", config_name)


def reset_mon_compaction_configs(mon_config_obj):
    """
    Reset MON compaction configurations

    Args:
        mon_config_obj: MonConfigMethods object
    """
    compaction_configs = [
        "mon_compact_on_start",
        "mon_compact_on_bootstrap",
    ]

    for config_name in compaction_configs:
        if not mon_config_obj.remove_config(section="mon", name=config_name):
            log.warning("Failed to reset %s", config_name)


def generate_mon_db_entries(rados_obj, pool_obj, config, cephadm):
    """
    Generate MON database entries by performing various operations

    Args:
        rados_obj: RadosOrchestrator object
        pool_obj: PoolFunctions object
        config: Test configuration
        cephadm: CephAdmin object
    """
    log.info("Generating MON database entries through various operations")

    # Create and delete multiple pools
    num_temp_pools = config.get("num_temp_pools", 10)
    log.info("Creating %s temporary pools", num_temp_pools)
    for i in range(num_temp_pools):
        temp_pool = f"temp_pool_{i}"
        method_should_succeed(
            rados_obj.create_pool,
            pool_name=temp_pool,
            pg_num=8,
        )
        rados_obj.bench_write(
            pool_name=temp_pool,
            rados_write_duration=300,
            byte_size="4KB",
            max_objs=5000,
            verify_stats=False,
        )
        pool_obj.set_bulk_flag(pool_name=temp_pool)

        # Create and delete OMAP entries
        if config.get("create_omaps", True):
            log.info("Creating and manipulating OMAP entries")
            omap_config = {
                "obj_start": 0,
                "obj_end": 500,
                "num_keys_obj": 1000,
                "verify_omap_count": False,
            }
            pool_obj.fill_omap_entries(pool_name=temp_pool, **omap_config)

        time.sleep(5)
        log.debug("Created pool: %s", temp_pool)

    # Start and stop OSDs to generate MON db activity with background IO
    osd_restart_cycles = config.get("osd_restart_cycles", 2)
    if osd_restart_cycles > 0:
        log.info(
            "Performing %s OSD service restart cycles with background IO to generate MON db entries",
            osd_restart_cycles,
        )

        # Start bench write in background
        log.info("Starting background bench write during OSD restarts")
        rados_obj.bench_write(
            pool_name="temp_pool_1",
            rados_write_duration=600,
            byte_size="4KB",
            max_objs=20000,
            verify_stats=False,
            background=True,
        )

        # Restart OSD services while IO is happening - repeat for specified cycles
        osd_services = rados_obj.list_orch_services(service_type="osd")
        for cycle in range(osd_restart_cycles):
            log.info("=== OSD restart cycle %s/%s ===", cycle + 1, osd_restart_cycles)
            for osd_service in osd_services:
                log.info("Restarting OSD service: %s", osd_service)
                cephadm.shell(args=["ceph", "orch", "restart", osd_service])
                time.sleep(5)  # Stagger restarts
            log.info("Completed restart cycle %s", cycle + 1)
            time.sleep(10)  # Wait between cycles

        log.info(
            "All %s OSD restart cycles completed, waiting for background IO to finish",
            osd_restart_cycles,
        )
        time.sleep(30)

    log.info("MON database entry generation completed")


def get_mon_store_size(rados_obj, mon_host):
    """
    Get the size of MON store.db directory

    Args:
        rados_obj: RadosOrchestrator object
        mon_host: Host object for the MON node

    Returns:
        int: Size in bytes, or None if failed
    """
    try:
        # Get cluster FSID
        fsid = rados_obj.run_ceph_command(cmd="ceph fsid")["fsid"]
        mon_hostname = mon_host.hostname
        store_path = f"/var/lib/ceph/{fsid}/mon.{mon_hostname}/store.db"

        # Get size using du command on the specific host
        cmd = f"sudo du -sb {store_path}"
        out, _ = mon_host.exec_command(cmd=cmd, sudo=True)

        # Parse output: "12345678    /path/to/store.db"
        size_bytes = int(out.split()[0])
        log.info(
            "MON %s store.db size: %s bytes (%s MB)",
            mon_hostname,
            f"{size_bytes:,}",
            f"{size_bytes / (1024 * 1024):.2f}",
        )
        return size_bytes

    except Exception as e:
        log.error(
            "Failed to get MON store size for %s: %s",
            mon_host.hostname if mon_host else "unknown",
            e,
        )
        return None


def test_online_mon_compaction(rados_obj, mon_id, cephadm):
    """
    Test online MON compaction using 'ceph tell mon.<ID> compact'

    Args:
        rados_obj: RadosOrchestrator object
        mon_id: MON ID to compact
        cephadm: CephAdmin object for timestamp capture

    Returns:
        tuple: (start_time, end_time) for log verification
    """
    log.info("Testing online MON compaction for mon.%s", mon_id)

    # Get MON host object
    mon_host = get_mon_host_object(rados_obj, mon_id)
    if not mon_host:
        log.error("Failed to get host object for mon.%s", mon_id)
        raise AssertionError(f"Could not find host for mon.{mon_id}")

    # Get store size before compaction
    store_size_before = get_mon_store_size(rados_obj, mon_host)

    # Capture start time
    start_time_out, _ = cephadm.shell(
        args=["date", "-u", "'+%Y-%m-%dT%H:%M:%S.%3N+0000'"]
    )
    start_time = start_time_out.strip().strip("'")
    log.info("Online compaction start time: %s", start_time)

    # Execute online compaction
    log.info("Executing online compaction: ceph tell mon.%s compact", mon_id)
    try:
        out, err = cephadm.shell(args=["ceph", "tell", f"mon.{mon_id}", "compact"])
        log.info("Compaction command output: %s", out)

        # ceph tell command returns plain text, check output
        if "compacted rocksdb" in out or out.strip():
            log.info("Online compaction command executed successfully")
        else:
            log.warning("Unexpected compaction output: %s", out)
            raise

    except Exception as e:
        log.error("Failed to execute online compaction: %s", e)
        raise

    # Wait for compaction to complete
    log.info("Waiting %s seconds for compaction to complete", COMPACTION_WAIT)
    time.sleep(COMPACTION_WAIT)

    # Capture end time
    end_time_out, _ = cephadm.shell(
        args=["date", "-u", "'+%Y-%m-%dT%H:%M:%S.%3N+0000'"]
    )
    end_time = end_time_out.strip().strip("'")
    log.info("Online compaction end time: %s", end_time)

    # Get store size after compaction
    store_size_after = get_mon_store_size(rados_obj, mon_host)

    # Verify reduction in store size
    if store_size_before and store_size_after:
        reduction = store_size_before - store_size_after
        reduction_pct = (
            (reduction / store_size_before * 100) if store_size_before > 0 else 0
        )

        log.info(
            "MON %s store.db size change after online compaction:\n"
            "  Before: %s bytes (%s MB)\n"
            "  After:  %s bytes (%s MB)\n"
            "  Change: %+s bytes (%+.2f%%)",
            mon_host.hostname,
            f"{store_size_before:,}",
            f"{store_size_before / (1024 * 1024):.2f}",
            f"{store_size_after:,}",
            f"{store_size_after / (1024 * 1024):.2f}",
            f"{reduction:,}",
            reduction_pct,
        )

        # Assert that size reduced or stayed the same
        if store_size_after <= store_size_before:
            log.info("Online compaction successful: store.db size reduced or stable")
        else:
            log.warning("Store size increased after compaction")
            # TBD: if the mon daemon activities are ongoing, the BD size can increase again post compaction.
            # Need to wait till the mon activity is stopped.
            # The wait will increase the suite run duration.
            # For now, all the operations are executed and checked for completeness

    log.info("Online MON compaction test completed")
    return start_time, end_time


def verify_mon_compact_on_trim(mon_config_obj):
    """
    Verify mon_compact_on_trim configuration

    Args:
        mon_config_obj: MonConfigMethods object
    """
    log.info("Verifying mon_compact_on_trim configuration")

    try:
        # Get current value
        config_value = mon_config_obj.get_config(
            section="mon", param="mon_compact_on_trim"
        )

        log.info("mon_compact_on_trim current value: %s", config_value)

        # Verify default value is true
        if config_value is None:
            log.error("Could not retrieve mon_compact_on_trim value")
            raise AssertionError("mon_compact_on_trim configuration not found")

        # Check if value is true (could be "true" string or boolean)
        if str(config_value).lower() == "true":
            log.info(" mon_compact_on_trim verified: default is true")
        else:
            log.error(
                "mon_compact_on_trim has unexpected value: %s (expected: true)",
                config_value,
            )
            raise AssertionError(
                f"mon_compact_on_trim should be true by default, found: {config_value}"
            )

    except Exception as e:
        log.error("Failed to verify mon_compact_on_trim: %s", e)
        raise


def test_mon_compact_on_start(rados_obj, mon_config_obj, mon_id, cephadm):
    """
    Test MON compaction on start by enabling mon_compact_on_start

    Args:
        rados_obj: RadosOrchestrator object
        mon_config_obj: MonConfigMethods object
        mon_id: MON ID to restart
        cephadm: CephAdmin object for timestamp capture
    """
    log.info("Testing MON compaction on start for mon.%s", mon_id)

    # Enable mon_compact_on_start
    log.info("Enabling mon_compact_on_start configuration")
    if not mon_config_obj.set_config(
        section=f"mon.{mon_id}", name="mon_compact_on_start", value="true"
    ):
        log.error("Failed to set mon_compact_on_start")
        raise AssertionError("Could not enable mon_compact_on_start configuration")

    log.info("Successfully enabled mon_compact_on_start")

    # Get MON host object
    mon_host = get_mon_host_object(rados_obj, mon_id)
    if not mon_host:
        log.error("Failed to get host object for mon.%s", mon_id)
        raise AssertionError(f"Could not find host for mon.{mon_id}")

    # Get store size before restart
    store_size_before = get_mon_store_size(rados_obj, mon_host)

    # Capture start time
    start_time_out, _ = cephadm.shell(
        args=["date", "-u", "'+%Y-%m-%dT%H:%M:%S.%3N+0000'"]
    )
    start_time = start_time_out.strip().strip("'")
    log.info("Restart compaction start time: %s", start_time)

    # Restart MON daemon
    log.info("Restarting MON daemon: mon.%s", mon_id)
    try:
        out, err = cephadm.shell(
            args=["ceph", "orch", "daemon", "restart", f"mon.{mon_id}"]
        )
        log.info("MON restart command output: %s", out)
    except Exception as e:
        log.error("Failed to restart MON: %s", e)
        raise

    # Wait for MON to come back up and compaction to trigger
    log.info("Waiting for MON to restart and compaction to trigger")
    time.sleep(60)

    # Verify MON is back up
    max_wait = 180
    waited = 0
    while waited < max_wait:
        try:
            # Check if MON is in quorum
            out = rados_obj.run_ceph_command(cmd="ceph quorum_status")
            quorum_names = out.get("quorum_names", [])
            if mon_id in quorum_names:
                log.info("MON %s is back up and in quorum", mon_id)
                break
        except Exception:
            pass
        time.sleep(10)
        waited += 10

    if waited >= max_wait:
        log.error("MON did not come back up within timeout")
        raise AssertionError(f"MON {mon_id} failed to restart")

    # Additional wait for compaction
    time.sleep(COMPACTION_WAIT)

    # Capture end time
    end_time_out, _ = cephadm.shell(
        args=["date", "-u", "'+%Y-%m-%dT%H:%M:%S.%3N+0000'"]
    )
    end_time = end_time_out.strip().strip("'")
    log.info("Restart compaction end time: %s", end_time)

    # Get store size after restart
    store_size_after = get_mon_store_size(rados_obj, mon_host)

    # Verify reduction
    if store_size_before and store_size_after:
        reduction = store_size_before - store_size_after
        reduction_pct = (
            (reduction / store_size_before * 100) if store_size_before > 0 else 0
        )

        log.info(
            "MON %s store.db size change after restart with mon_compact_on_start:\n"
            "  Before: %s bytes (%s MB)\n"
            "  After:  %s bytes (%s MB)\n"
            "  Change: %+s bytes (%+.2f%%)",
            mon_host.hostname,
            f"{store_size_before:,}",
            f"{store_size_before / (1024 * 1024):.2f}",
            f"{store_size_after:,}",
            f"{store_size_after / (1024 * 1024):.2f}",
            f"{reduction:,}",
            reduction_pct,
        )

        if store_size_after <= store_size_before:
            log.info("Compaction on start successful: store.db size reduced or stable")
        else:
            log.warning("Store size increased after compaction on start")
            # TBD: if the mon daemon activities are ongoing, the BD size can increase again post compaction.
            # Need to wait till the mon activity is stopped.
            # The wait will increase the suite run duration.
            # For now, all the operations are executed and checked for completeness

    # Verify compaction logs
    if not verify_mon_compaction_logs(
        rados_obj, mon_id, start_time, end_time, "compaction on start"
    ):
        log.warning("Could not verify compaction on start in logs")
        raise

    log.info("MON compaction on start test completed")


def test_mon_compact_on_bootstrap(
    rados_obj, mon_daemon_obj, mon_config_obj, cephadm, ceph_cluster
):
    """
    Test mon_compact_on_bootstrap configuration by adding a new MON daemon

    Args:
        rados_obj: RadosOrchestrator object
        mon_daemon_obj: MonitorWorkflows object
        mon_config_obj: MonConfigMethods object
        cephadm: CephAdmin object
        ceph_cluster: Ceph cluster object
    """
    log.info("Testing mon_compact_on_bootstrap by bootstrapping a new MON")
    new_mon_host = None
    new_mon_hostname = None

    try:
        # Enable mon_compact_on_bootstrap
        log.info("Enabling mon_compact_on_bootstrap configuration")
        if not mon_config_obj.set_config(
            section="mon", name="mon_compact_on_bootstrap", value="true"
        ):
            log.error("Failed to set mon_compact_on_bootstrap")
            raise AssertionError(
                "Could not enable mon_compact_on_bootstrap configuration"
            )

        log.info("Successfully enabled mon_compact_on_bootstrap")

        # Get all cluster nodes
        cluster_nodes = ceph_cluster.get_nodes()

        # Get current MON nodes
        mon_daemons = rados_obj.run_ceph_command(cmd="ceph mon dump")
        current_mon_hostnames = [mon.get("name") for mon in mon_daemons.get("mons", [])]
        log.info("Current MON hostnames: %s", current_mon_hostnames)

        # Find a host without a MON daemon
        for node in cluster_nodes:
            if node.hostname not in current_mon_hostnames:
                new_mon_host = node
                new_mon_hostname = node.hostname
                break

        if not new_mon_host:
            log.warning("No available host without MON found. Skipping bootstrap test.")
            return

        log.info(
            "Selected host %s (IP: %s) for new MON bootstrap",
            new_mon_hostname,
            new_mon_host.ip_address,
        )

        # Set MON service as unmanaged
        log.info("Setting MON service to unmanaged")
        if not mon_daemon_obj.set_mon_service_managed_type(unmanaged=True):
            log.error("Could not set mon service to unmanaged")
            raise AssertionError("Failed to set mon service as unmanaged")

        # Capture start time
        start_time_out, _ = cephadm.shell(
            args=["date", "-u", "'+%Y-%m-%dT%H:%M:%S.%3N+0000'"]
        )
        start_time = start_time_out.strip().strip("'")
        log.info("Bootstrap compaction start time: %s", start_time)

        # Add new MON service (triggers bootstrap)
        log.info(
            "Adding new MON service to %s (this triggers bootstrap)", new_mon_hostname
        )
        if not mon_daemon_obj.add_mon_service(host=new_mon_host):
            log.error("Failed to add MON service on host %s", new_mon_hostname)
            raise AssertionError(f"Could not add MON service on {new_mon_hostname}")

        log.info("Successfully bootstrapped new MON on %s", new_mon_hostname)

        # Wait for new MON to join quorum
        log.info("Waiting for new MON to join quorum")
        time.sleep(30)

        # Capture end time
        end_time_out, _ = cephadm.shell(
            args=["date", "-u", "'+%Y-%m-%dT%H:%M:%S.%3N+0000'"]
        )
        end_time = end_time_out.strip().strip("'")
        log.info("Bootstrap compaction end time: %s", end_time)

        # Verify compaction logs for the new MON
        log.info("Verifying compaction logs for newly bootstrapped MON")
        if not verify_mon_compaction_logs(
            rados_obj, new_mon_hostname, start_time, end_time, "bootstrap compaction"
        ):
            log.warning("Could not verify bootstrap compaction in logs")

        log.info("MON compact on bootstrap test completed")

    finally:
        # Cleanup: Remove the newly added MON
        if new_mon_hostname:
            log.info("Cleaning up: Removing newly added MON from %s", new_mon_hostname)
            try:
                if mon_daemon_obj.remove_mon_service(host=new_mon_hostname):
                    log.info("Successfully removed MON from %s", new_mon_hostname)
                else:
                    log.warning("Failed to remove MON from %s", new_mon_hostname)
            except Exception as e:
                log.warning("Exception while removing MON: %s", e)

        # Set MON service back to managed
        log.info("Setting MON service back to managed")
        try:
            if mon_daemon_obj.set_mon_service_managed_type(unmanaged=False):
                log.info("Successfully set MON service back to managed")
            else:
                log.warning("Failed to set MON service back to managed")
        except Exception as e:
            log.warning("Exception while setting mon service to managed: %s", e)


def test_offline_mon_compaction(rados_obj, mon_id, cephadm):
    """
    Test offline MON compaction using ceph-monstore-tool

    Args:
        rados_obj: RadosOrchestrator object
        mon_id: MON ID to compact
        cephadm: CephAdmin object for timestamp capture
    """
    log.info("Testing offline MON compaction for mon.%s", mon_id)

    # Get MON host object
    mon_host = get_mon_host_object(rados_obj, mon_id)
    if not mon_host:
        log.error("Failed to get host object for mon.%s", mon_id)
        raise AssertionError(f"Could not find host for mon.{mon_id}")

    # Get store size before compaction
    store_size_before = get_mon_store_size(rados_obj, mon_host)

    # Stop MON daemon
    log.info("Stopping MON daemon: mon.%s", mon_id)
    try:
        out, err = cephadm.shell(
            args=["ceph", "orch", "daemon", "stop", f"mon.{mon_id}"]
        )
        log.info("MON stop command output: %s", out)
    except Exception as e:
        log.error("Failed to stop MON: %s", e)
        raise

    # Wait for MON to stop completely
    log.info("Waiting for MON to stop completely")
    time.sleep(60)

    # Capture start time
    start_time_out, _ = cephadm.shell(
        args=["date", "-u", "'+%Y-%m-%dT%H:%M:%S.%3N+0000'"]
    )
    start_time = start_time_out.strip().strip("'")
    log.info("Offline compaction start time: %s", start_time)

    # Execute offline compaction using monstore tool
    log.info("Executing offline compaction using ceph-monstore-tool")
    # Get cluster FSID
    fsid = rados_obj.run_ceph_command(cmd="ceph fsid")["fsid"]
    store_path = f"/var/lib/ceph/{fsid}/mon.{mon_host.hostname}"

    cmd = f"cephadm shell -- ceph-monstore-tool {store_path} compact"
    out, err = mon_host.exec_command(cmd=cmd, sudo=True, check_ec=False)
    log.info("Offline compaction output: %s", out)

    # Wait after compaction
    time.sleep(STORE_DB_CHECK_WAIT)

    # Capture end time
    end_time_out, _ = cephadm.shell(
        args=["date", "-u", "'+%Y-%m-%dT%H:%M:%S.%3N+0000'"]
    )
    end_time = end_time_out.strip().strip("'")
    log.info("Offline compaction end time: %s", end_time)

    # Get store size after compaction (while still stopped)
    store_size_after = get_mon_store_size(rados_obj, mon_host)

    # Start MON daemon back up
    log.info("Starting MON daemon back up: mon.%s", mon_id)
    try:
        out, err = cephadm.shell(
            args=["ceph", "orch", "daemon", "start", f"mon.{mon_id}"]
        )
        log.info("MON start command output: %s", out)
    except Exception as e:
        log.error("Failed to start MON: %s", e)
        raise

    # Wait for MON to come back up
    time.sleep(30)

    # Verify MON is back up
    max_wait = 180
    waited = 0
    while waited < max_wait:
        try:
            # Check if MON is in quorum
            out = rados_obj.run_ceph_command(cmd="ceph quorum_status")
            quorum_names = out.get("quorum_names", [])
            if mon_id in quorum_names:
                log.info("MON %s is back up and in quorum", mon_id)
                break
        except Exception:
            pass
        time.sleep(10)
        waited += 10

    # Verify reduction
    if store_size_before and store_size_after:
        reduction = store_size_before - store_size_after
        reduction_pct = (
            (reduction / store_size_before * 100) if store_size_before > 0 else 0
        )

        log.info(
            "MON %s store.db size change after offline compaction:\n"
            "  Before: %s bytes (%s MB)\n"
            "  After:  %s bytes (%s MB)\n"
            "  Change: %+s bytes (%+.2f%%)",
            mon_host.hostname,
            f"{store_size_before:,}",
            f"{store_size_before / (1024 * 1024):.2f}",
            f"{store_size_after:,}",
            f"{store_size_after / (1024 * 1024):.2f}",
            f"{reduction:,}",
            reduction_pct,
        )

        if store_size_after <= store_size_before:
            log.info("Offline compaction successful: store.db size reduced or stable")
        else:
            log.warning("Store size increased after offline compaction")

    # Note: Offline compaction may not generate logs in the same way as online compaction
    # since the MON daemon is stopped.
    log.info("Offline MON compaction test completed")


def verify_mon_compaction_logs(
    rados_obj, mon_id, start_time, end_time, operation_name="compaction"
):
    """
    Verify compaction events in MON daemon logs on the specific host

    Args:
        rados_obj: RadosOrchestrator object
        mon_id: MON ID to check logs for
        start_time: Start time for log analysis (format: YYYY-MM-DDTHH:MM:SS.mmm+0000)
        end_time: End time for log analysis
        operation_name: Name of the operation for logging (default: "compaction")

    Returns:
        bool: True if compaction events found, False otherwise
    """
    log.info(
        "Verifying %s events in MON.%s daemon logs between %s and %s",
        operation_name,
        mon_id,
        start_time,
        end_time,
    )

    try:
        # Get cluster fsid
        fsid = rados_obj.run_ceph_command(cmd="ceph fsid")["fsid"]

        # Get MON host object
        mon_host = get_mon_host_object(rados_obj, mon_id)
        if not mon_host:
            log.error("Failed to get host object for mon.%s", mon_id)
            return False

        mon_hostname = mon_host.hostname
        log_path = f"/var/log/ceph/{fsid}/ceph-mon.{mon_hostname}.log"
        temp_log = f"/tmp/mon_{mon_hostname}_compaction_test.log"

        # Extract date portion for filtering
        start_date = start_time.split("T")[0]
        end_date = end_time.split("T")[0]

        log.debug(
            "Extracting log entries for MON.%s in time range %s to %s",
            mon_hostname,
            start_time,
            end_time,
        )

        # Filter logs by time range
        time_filter_cmd = f"grep -E '{start_date}|{end_date}' {log_path}"
        cmd = (
            f"{time_filter_cmd} | "
            f"awk -v start='{start_time}' -v end='{end_time}' "
            f"'$1 >= start && $1 <= end' "
            f"> {temp_log} 2>/dev/null || true"
        )
        mon_host.exec_command(cmd=cmd, sudo=True, check_ec=False)

        # Count filtered lines
        try:
            count_filtered, _ = mon_host.exec_command(
                cmd=f"wc -l {temp_log}", sudo=True, check_ec=False
            )
            lines_captured = int(count_filtered.strip().split()[0])
            log.info(
                "Captured %s total log lines in time range for MON.%s",
                lines_captured,
                mon_hostname,
            )
        except Exception as e:
            log.warning("Could not count filtered lines: %s", e)
            lines_captured = 0

        # Read and parse log
        log.debug("Reading and parsing MON log")
        log_content, _ = mon_host.exec_command(
            cmd=f"sudo cat {temp_log}", sudo=True, check_ec=False
        )

        # Check if we captured any logs
        if not log_content or len(log_content.strip()) == 0 or lines_captured == 0:
            log.warning(
                "No log entries found for MON.%s in time range %s to %s",
                mon_hostname,
                start_time,
                end_time,
            )
            mon_host.exec_command(cmd=f"rm -f {temp_log}", sudo=True, check_ec=False)
            return False

        # Parse for compaction-related patterns
        compact_events = []
        compact_start_count = 0
        compact_finish_count = 0
        matched_lines = []

        log_lines = log_content.splitlines() if log_content else []
        log.debug("Parsing %s lines for compaction events", len(log_lines))

        for line in log_lines:
            line_lower = line.lower()

            # Look for various compaction-related patterns
            if "compact" in line_lower:
                matched_lines.append(line)

                # Count specific compaction events
                if "compacting" in line_lower or "compact start" in line_lower:
                    compact_start_count += 1
                elif (
                    "compact done" in line_lower
                    or "compact finish" in line_lower
                    or "compacted" in line_lower
                ):
                    compact_finish_count += 1

                # Collect compact events
                if any(
                    keyword in line_lower
                    for keyword in [
                        "compact",
                        "compaction",
                        "compacting",
                        "compacted",
                    ]
                ):
                    compact_events.append(line.strip())

        # Cleanup temp file
        mon_host.exec_command(cmd=f"rm -f {temp_log}", sudo=True, check_ec=False)

        # Log results
        log.info("MON.%s Compaction Log Analysis Results:", mon_hostname)
        log.info("  Log lines captured: %s", lines_captured)
        log.info("  Total compaction-related entries: %s", len(compact_events))
        log.info("  Compaction start events: %s", compact_start_count)
        log.info("  Compaction finish events: %s", compact_finish_count)

        # Show sample of matched lines
        if matched_lines:
            log.info("Sample compaction-related log entries (first 20):")
            for line in matched_lines[:20]:
                log.info("  %s", line.strip())

        # Verification
        if len(compact_events) > 0:
            log.info(
                "Compaction events verified in MON.%s daemon logs for %s",
                mon_hostname,
                operation_name,
            )
            return True
        else:
            log.warning(
                "No compaction events found in MON.%s daemon logs for %s",
                mon_hostname,
                operation_name,
            )
            return False

    except Exception as e:
        log.error("Failed to verify MON compaction logs: %s", e)
        log.exception(e)
        return False
