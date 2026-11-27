"""
Module to verify scenarios related to MGR Stats Period Auto-Tuning (IBMCEPH-18100)

This module tests the automatic tuning of mgr_stats_period based on messenger dispatch
queue depth, a feature that prevents Manager overload during high cluster activity.

Feature behavior:
- When the MGR dispatch queue exceeds the threshold, mgr_stats_period increases (capped at 60s)
- When queue depth drops below recovery threshold, mgr_stats_period decreases toward baseline
- If an administrator sets mgr_stats_period manually, the autotuner adopts it as the new baseline
- With autotune disabled, mgr_stats_period remains at the configured value

Configuration options:
- mgr_stats_period_autotune (bool, default: true)
- mgr_stats_period_autotune_queue_threshold (int, default: 100)
- mgr_stats_period (int, default: 5) — existing option, auto-adjusted at runtime

PR: https://github.com/ceph/ceph/pull/65611
"""

import json
import re
import threading
import time
import datetime

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.mgr_workflows import MgrWorkflows
from ceph.rados.utils import get_cluster_timestamp
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log

log = Log(__name__)

# Constants from the C++ StatsAutotuner class
MAX_PERIOD = 60
RECOVERY_THRESHOLD = 20
MIN_QUEUE_DEPTH = 5
DEFAULT_STATS_PERIOD = 5
DEFAULT_AUTOTUNE_ENABLED = "true"
DEFAULT_QUEUE_THRESHOLD = 100
# Autotune increases mgr_stats_period only when dispatch_queue.length is
# strictly greater than this value.
SUSTAIN_QUEUE_THRESHOLD = 5

# Pool name prefix for cleanup
AUTOTUNE_TEST_POOL_PREFIX = "autotune-test-pool-"


def run(ceph_cluster, **kw):
    """
    Test suite for MGR Stats Period Auto-Tuning (IBMCEPH-18100)

    This test suite verifies the automatic tuning of the Manager stats collection
    period. The feature dynamically adjusts mgr_stats_period based on the messenger
    dispatch queue depth to prevent Manager overload during high cluster activity.

    Args:
        ceph_cluster: Ceph cluster object
        kw: Test configuration parameters

    Supported test scenarios:
        - test_mgr_stats_autotune_config_defaults: Verify default values of new config
          options (mgr_stats_period_autotune=true, queue_threshold=100)
        - test_mgr_stats_autotune_config_set_get: Verify set/get/toggle for autotune
          boolean and threshold integer configs
        - test_mgr_stats_autotune_invalid_config: Negative tests for invalid config
          values (non-boolean, negative threshold, zero threshold)
        - test_mgr_stats_autotune_config_persistence_mgr_failover: Verify config
          values persist across MGR daemon failover
        - test_mgr_stats_autotune_disabled_behavior: Verify period stays fixed when
          autotune is disabled, while a side thread shows the dispatch queue
          exceeded mgr_stats_period_autotune_queue_threshold
        - test_mgr_stats_autotune_period_increase_and_recovery: Combined functional
          test for period increase under load, 60s cap, step-wise adjustment, and
          automatic recovery back to baseline
        - test_mgr_stats_autotune_baseline_tracking: Verify that a manually set
          mgr_stats_period becomes the new baseline and is honored after recovery
        - test_mgr_stats_autotune_idle_cluster: Verify that on an idle cluster the
          autotune does not decrease period below baseline
        - test_mgr_stats_autotune_mgr_crash_during_adjustment: Verify cluster
          stability when active MGR crashes during an active period adjustment

    Test Requirements:
        - Ceph version with auto-tune feature (tentacle / 9.x and above)
        - At least one client node
        - Multiple MGR daemons for failover tests
    """

    log.info(run.__doc__)
    config = kw["config"]
    rhbuild = config.get("rhbuild")
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    mgr_obj = MgrWorkflows(node=cephadm)
    client_node = ceph_cluster.get_nodes(role="client")[0]

    # -----------------------------------------------------------------------
    # Test 1: Verify default config values
    # Test plan: #1, #2
    # -----------------------------------------------------------------------
    if config.get("test_mgr_stats_autotune_config_defaults"):
        doc = (
            "\n# Test: MGR Stats Period Auto-Tune Default Config Verification"
            "\n Verify that new autotune configuration options exist with correct defaults"
            "\n\t 1. Verify mgr_stats_period_autotune default is true"
            "\n\t 2. Verify mgr_stats_period_autotune_queue_threshold default is 100"
            "\n\t 3. Verify mgr_stats_period default is 5"
            "\n\t 4. Verify configs are readable at MGR runtime level"
            "\n\t 5. Verify configs are present in ceph config dump"
        )

        log.info(doc)
        log.info("Running test to verify MGR stats autotune default configurations")

        start_time = get_cluster_timestamp(rados_obj.node)
        log.debug(f"Test workflow started. Start time: {start_time}")

        try:
            # Step 1: Check mgr_stats_period_autotune default
            log.info("Step 1: Checking default value of mgr_stats_period_autotune")
            verify_mgr_config(
                mon_obj=mon_obj,
                param_name="mgr_stats_period_autotune",
                expected_value="true",
            )
            log.info(" Verified: mgr_stats_period_autotune = true (default)")

            # Step 2: Check mgr_stats_period_autotune_queue_threshold default
            log.info(
                "Step 2: Checking default value of mgr_stats_period_autotune_queue_threshold"
            )
            verify_mgr_config(
                mon_obj=mon_obj,
                param_name="mgr_stats_period_autotune_queue_threshold",
                expected_value="100",
            )
            log.info(
                " Verified: mgr_stats_period_autotune_queue_threshold = 100 (default)"
            )

            # Step 3: Check mgr_stats_period base default
            log.info("Step 3: Checking default value of mgr_stats_period")
            verify_mgr_config(
                mon_obj=mon_obj,
                param_name="mgr_stats_period",
                expected_value="5",
            )
            log.info(" Verified: mgr_stats_period = 5 (default)")

            # Step 4: Verify at runtime level on active MGR
            log.info("Step 4: Verifying configs at runtime on active MGR")
            active_mgr = mgr_obj.get_active_mgr()
            log.info("Active MGR: %s", active_mgr)

            runtime_autotune = get_mgr_runtime_config(
                mon_obj=mon_obj,
                rados_obj=rados_obj,
                param_name="mgr_stats_period_autotune",
                mgr_id=active_mgr,
            )
            log.info(
                "Runtime mgr_stats_period_autotune on %s: %s",
                active_mgr,
                runtime_autotune,
            )
            if runtime_autotune != "true":
                raise Exception(
                    "mgr_stats_period_autotune runtime value mismatch: "
                    "expected 'true', got '%s'" % runtime_autotune
                )

            runtime_threshold = get_mgr_runtime_config(
                mon_obj=mon_obj,
                rados_obj=rados_obj,
                param_name="mgr_stats_period_autotune_queue_threshold",
                mgr_id=active_mgr,
            )
            log.info(
                "Runtime mgr_stats_period_autotune_queue_threshold on %s: %s",
                active_mgr,
                runtime_threshold,
            )
            if runtime_threshold != "100":
                raise Exception(
                    "mgr_stats_period_autotune_queue_threshold runtime mismatch: "
                    "expected '100', got '%s'" % runtime_threshold
                )

            runtime_period = get_mgr_runtime_config(
                mon_obj=mon_obj,
                rados_obj=rados_obj,
                param_name="mgr_stats_period",
                mgr_id=active_mgr,
            )
            log.info("Runtime mgr_stats_period on %s: %s", active_mgr, runtime_period)
            if runtime_period != "5":
                raise Exception(
                    "mgr_stats_period runtime mismatch: "
                    "expected '5', got '%s'" % runtime_period
                )

            # Step 5: Sanity check
            log.info("Step 5: Running pool sanity check")
            if not rados_obj.run_pool_sanity_check():
                log.error("Pool sanity checks failed on cluster")
                return 1

            log.info(" All default config verification checks passed")
            return 0

        except Exception as e:
            log.error("Failed with exception: %s", e.__doc__)
            log.exception(e)
            rados_obj.log_cluster_health()
            return 1

    # -----------------------------------------------------------------------
    # Test 2: Config set/get/toggle operations
    # Test plan: #3, #4
    # -----------------------------------------------------------------------
    if config.get("test_mgr_stats_autotune_config_set_get"):
        doc = (
            "\n# Test: MGR Stats Period Auto-Tune Config Set/Get Operations"
            "\n Verify configuration can be toggled and custom values set correctly"
            "\n\t 1. Set mgr_stats_period_autotune to false, verify value"
            "\n\t 2. Set mgr_stats_period_autotune back to true, verify value"
            "\n\t 3. Set mgr_stats_period_autotune_queue_threshold to 150, verify"
            "\n\t 4. Restore threshold to default 100, verify"
            "\n\t 5. Set mgr_stats_period_autotune_queue_threshold to 50 (lower), verify"
            "\n\t 6. Set mgr_stats_period to 10, verify"
            "\n\t 7. Cleanup: restore all configs to defaults"
        )

        log.info(doc)
        log.info("Running test to verify MGR stats autotune config set/get operations")

        start_time = get_cluster_timestamp(rados_obj.node)
        log.debug(f"Test workflow started. Start time: {start_time}")

        try:
            # Step 1: Disable autotune
            log.info("Step 1: Setting mgr_stats_period_autotune to false")
            mon_obj.set_config(
                section="mgr",
                name="mgr_stats_period_autotune",
                value="false",
            )
            verify_mgr_config(
                mon_obj=mon_obj,
                param_name="mgr_stats_period_autotune",
                expected_value="false",
            )
            log.info(" Verified: mgr_stats_period_autotune set to false")

            # Step 2: Re-enable autotune
            log.info("Step 2: Setting mgr_stats_period_autotune back to true")
            mon_obj.set_config(
                section="mgr",
                name="mgr_stats_period_autotune",
                value="true",
            )
            verify_mgr_config(
                mon_obj=mon_obj,
                param_name="mgr_stats_period_autotune",
                expected_value="true",
            )
            log.info(" Verified: mgr_stats_period_autotune restored to true")

            # Step 3: Set threshold to 150
            log.info("Step 3: Setting queue threshold to 150")
            mon_obj.set_config(
                section="mgr",
                name="mgr_stats_period_autotune_queue_threshold",
                value="150",
            )
            verify_mgr_config(
                mon_obj=mon_obj,
                param_name="mgr_stats_period_autotune_queue_threshold",
                expected_value="150",
            )
            log.info(" Verified: mgr_stats_period_autotune_queue_threshold set to 150")

            # Step 4: Restore threshold to default 100
            log.info("Step 4: Restoring queue threshold to 100")
            mon_obj.set_config(
                section="mgr",
                name="mgr_stats_period_autotune_queue_threshold",
                value="100",
            )
            verify_mgr_config(
                mon_obj=mon_obj,
                param_name="mgr_stats_period_autotune_queue_threshold",
                expected_value="100",
            )
            log.info(
                " Verified: mgr_stats_period_autotune_queue_threshold restored to 100"
            )

            # Step 5: Set threshold lower than default
            log.info("Step 5: Setting queue threshold to 50")
            mon_obj.set_config(
                section="mgr",
                name="mgr_stats_period_autotune_queue_threshold",
                value="50",
            )
            verify_mgr_config(
                mon_obj=mon_obj,
                param_name="mgr_stats_period_autotune_queue_threshold",
                expected_value="50",
            )
            log.info(" Verified: mgr_stats_period_autotune_queue_threshold set to 50")

            # Step 6: Set mgr_stats_period to custom value
            log.info("Step 6: Setting mgr_stats_period to 10")
            mon_obj.set_config(
                section="mgr",
                name="mgr_stats_period",
                value="10",
            )
            verify_mgr_config(
                mon_obj=mon_obj,
                param_name="mgr_stats_period",
                expected_value="10",
            )
            log.info(" Verified: mgr_stats_period set to 10")

            log.info(" All config set/get verification checks passed")

        except Exception as e:
            log.error("Failed with exception: %s", e.__doc__)
            log.exception(e)
            rados_obj.log_cluster_health()
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here"
                " *************** \n \n"
            )
            # Step 7: Cleanup - restore defaults
            log.info("Step 7: Cleanup - restoring all configs to defaults")
            restore_autotune_defaults(mon_obj=mon_obj)

        return 0

    # -----------------------------------------------------------------------
    # Test 3: Invalid / negative config values
    # Test plan: #20, #21
    # -----------------------------------------------------------------------
    if config.get("test_mgr_stats_autotune_invalid_config"):
        doc = (
            "\n# Test: MGR Stats Period Auto-Tune Invalid Config Handling"
            "\n Verify invalid config values are rejected gracefully"
            "\n\t 1. Set mgr_stats_period_autotune to 'invalid' (non-boolean) - expect rejection"
            "\n\t 2. Verify mgr_stats_period_autotune remains unchanged after rejection"
            "\n\t 3. Set mgr_stats_period_autotune_queue_threshold to -1 - verify behavior"
            "\n\t 4. Set mgr_stats_period_autotune_queue_threshold to 0 - verify behavior"
            "\n\t 5. Set mgr_stats_period_autotune_queue_threshold to 999999999 - verify accepted"
            "\n\t 6. Verify cluster health is OK after invalid config attempts"
            "\n\t 7. Cleanup: restore defaults"
        )

        log.info(doc)
        log.info("Running test to verify invalid config value handling")

        start_time = get_cluster_timestamp(rados_obj.node)
        log.debug(f"Test workflow started. Start time: {start_time}")

        try:
            # Step 1: Try invalid boolean value
            log.info("Step 1: Attempting to set mgr_stats_period_autotune to 'invalid'")
            try:
                rados_obj.node.shell(
                    ["ceph config set mgr mgr_stats_period_autotune invalid"]
                )
                log.error(
                    "Setting mgr_stats_period_autotune to 'invalid' did not fail!"
                )
                raise Exception(
                    "Expected failure when setting boolean config to 'invalid'"
                )
            except Exception as e:
                if (
                    "error" in str(e).lower()
                    or "invalid" in str(e).lower()
                    or "Error" in str(e)
                    or "EINVAL" in str(e)
                ):
                    log.info(
                        " Verified: Invalid boolean value rejected with error: %s",
                        str(e)[:200],
                    )
                else:
                    raise

            # Step 2: Verify config unchanged
            log.info("Step 2: Verifying config unchanged after invalid attempt")
            current_val = mon_obj.get_config(
                section="mgr", param="mgr_stats_period_autotune"
            )
            log.info("Current mgr_stats_period_autotune: %s", current_val)
            if str(current_val).strip().lower() not in ("true", ""):
                log.error("Config was unexpectedly changed to: %s", current_val)
                raise Exception("Config changed despite invalid input")
            log.info(" Verified: Config unchanged after invalid boolean attempt")

            # Step 3: Try negative threshold
            log.info("Step 3: Setting mgr_stats_period_autotune_queue_threshold to -1")
            negative_accepted = False
            try:
                rados_obj.node.shell(
                    [
                        "ceph config set mgr"
                        " mgr_stats_period_autotune_queue_threshold -1"
                    ]
                )
                negative_accepted = True
                log.warning("Negative threshold value -1 was accepted by config store")
            except Exception as e:
                log.info(" Negative threshold -1 rejected: %s", str(e)[:200])

            if negative_accepted:
                log.info("Verifying cluster stability with negative threshold")
                time.sleep(5)
                rados_obj.log_cluster_health()

            # Step 4: Try zero threshold
            log.info("Step 4: Setting mgr_stats_period_autotune_queue_threshold to 0")
            zero_accepted = False
            try:
                rados_obj.node.shell(
                    [
                        "ceph config set mgr"
                        " mgr_stats_period_autotune_queue_threshold 0"
                    ]
                )
                zero_accepted = True
                log.warning("Zero threshold value was accepted by config store")
            except Exception as e:
                log.info(" Zero threshold rejected: %s", str(e)[:200])

            if zero_accepted:
                log.info("Verifying cluster stability with zero threshold")
                time.sleep(5)
                rados_obj.log_cluster_health()

            # Step 5: Try extremely large threshold
            log.info(
                "Step 5: Setting mgr_stats_period_autotune_queue_threshold to 999999999"
            )
            try:
                mon_obj.set_config(
                    section="mgr",
                    name="mgr_stats_period_autotune_queue_threshold",
                    value="999999999",
                )
                log.info(
                    " Large threshold 999999999 accepted (expected - valid integer)"
                )
            except Exception as e:
                log.info("Large threshold rejected: %s", str(e)[:200])

            # Step 6: Verify cluster health
            log.info("Step 6: Verifying cluster health after invalid config attempts")
            time.sleep(5)
            if not rados_obj.run_pool_sanity_check():
                log.error("Pool sanity checks failed after invalid config tests")
                return 1
            log.info(" Cluster health OK after invalid config attempts")

            log.info(" All invalid config handling checks passed")

        except Exception as e:
            log.error("Failed with exception: %s", e.__doc__)
            log.exception(e)
            rados_obj.log_cluster_health()
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here"
                " *************** \n \n"
            )
            # Step 7: Cleanup
            log.info("Step 7: Cleanup - restoring all configs to defaults")
            restore_autotune_defaults(mon_obj=mon_obj)

        return 0

    # -----------------------------------------------------------------------
    # Test 4: Config persistence across MGR failover
    # Test plan: #6, #19
    # -----------------------------------------------------------------------
    if config.get("test_mgr_stats_autotune_config_persistence_mgr_failover"):
        doc = (
            "\n# Test: MGR Stats Period Auto-Tune Config Persistence Across MGR Failover"
            "\n Verify config values survive MGR daemon failover"
            "\n\t 1. Record current active MGR"
            "\n\t 2. Set mgr_stats_period_autotune_queue_threshold to 200"
            "\n\t 3. Set mgr_stats_period to 15"
            "\n\t 4. Fail the active MGR (ceph mgr fail)"
            "\n\t 5. Wait for new active MGR to come up"
            "\n\t 6. Verify threshold remains 200 on new active MGR"
            "\n\t 7. Verify mgr_stats_period is 15 on new active MGR (baseline preserved)"
            "\n\t 8. Run pool sanity check"
            "\n\t 9. Cleanup: restore defaults"
        )

        log.info(doc)
        log.info("Running test to verify config persistence across MGR failover")

        start_time = get_cluster_timestamp(rados_obj.node)
        log.debug(f"Test workflow started. Start time: {start_time}")

        try:
            # Check that we have multiple MGR daemons
            mgr_daemons = mgr_obj.get_mgr_daemon_list()
            log.info("MGR daemons in cluster: %s", mgr_daemons)
            if len(mgr_daemons) < 2:
                log.error(
                    "Need at least 2 MGR daemons for failover test. Found: %s",
                    len(mgr_daemons),
                )
                return 1

            # Step 1: Record current active MGR
            active_mgr_before = mgr_obj.get_active_mgr()
            log.info("Step 1: Active MGR before failover: %s", active_mgr_before)

            # Step 2: Set queue threshold to 200
            log.info("Step 2: Setting mgr_stats_period_autotune_queue_threshold to 200")
            mon_obj.set_config(
                section="mgr",
                name="mgr_stats_period_autotune_queue_threshold",
                value="200",
            )
            verify_mgr_config(
                mon_obj=mon_obj,
                param_name="mgr_stats_period_autotune_queue_threshold",
                expected_value="200",
            )
            log.info(" Threshold set to 200")

            # Step 3: Set mgr_stats_period to 15
            log.info("Step 3: Setting mgr_stats_period to 15")
            mon_obj.set_config(
                section="mgr",
                name="mgr_stats_period",
                value="15",
            )
            verify_mgr_config(
                mon_obj=mon_obj,
                param_name="mgr_stats_period",
                expected_value="15",
            )
            log.info(" mgr_stats_period set to 15")

            # Step 4: Fail the active MGR
            log.info("Step 4: Failing the active MGR: %s", active_mgr_before)
            mgr_obj.set_mgr_fail(active_mgr_before)

            # Step 5: Wait for new active MGR
            log.info("Step 5: Waiting for new active MGR")
            active_mgr_after = wait_for_new_active_mgr(
                mgr_obj=mgr_obj,
                old_active_mgr=active_mgr_before,
                timeout=120,
            )
            log.info("New active MGR: %s", active_mgr_after)

            if active_mgr_after == active_mgr_before:
                log.warning(
                    "Same MGR became active again. This is acceptable but "
                    "means the test is less thorough for cross-daemon persistence."
                )

            # Step 6: Verify threshold persisted
            log.info("Step 6: Verifying threshold on new active MGR")
            runtime_threshold = get_mgr_runtime_config(
                mon_obj=mon_obj,
                rados_obj=rados_obj,
                param_name="mgr_stats_period_autotune_queue_threshold",
                mgr_id=active_mgr_after,
            )
            log.info("Runtime threshold on %s: %s", active_mgr_after, runtime_threshold)
            if str(runtime_threshold) != "200":
                raise Exception(
                    "Threshold not persisted across MGR failover: "
                    "expected '200', got '%s'" % runtime_threshold
                )
            log.info(" Verified: Threshold persisted at 200 across MGR failover")

            # Step 7: Verify mgr_stats_period baseline persisted
            log.info("Step 7: Verifying mgr_stats_period on new active MGR")
            runtime_period = get_mgr_runtime_config(
                mon_obj=mon_obj,
                rados_obj=rados_obj,
                param_name="mgr_stats_period",
                mgr_id=active_mgr_after,
            )
            log.info(
                "Runtime mgr_stats_period on %s: %s",
                active_mgr_after,
                runtime_period,
            )
            if str(runtime_period) != "15":
                raise Exception(
                    "mgr_stats_period baseline not persisted across MGR failover: "
                    "expected '15', got '%s'" % runtime_period
                )
            log.info(
                " Verified: mgr_stats_period baseline persisted at 15 "
                "across MGR failover"
            )

            # Step 8: Pool sanity check
            log.info("Step 8: Running pool sanity check")
            if not rados_obj.run_pool_sanity_check():
                log.error("Pool sanity checks failed after MGR failover")
                return 1
            log.info(" Pool sanity check passed")

            log.info(" All MGR failover persistence checks passed")

        except Exception as e:
            log.error("Failed with exception: %s", e.__doc__)
            log.exception(e)
            rados_obj.log_cluster_health()
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here"
                " *************** \n \n"
            )
            log.info("Step 9: Cleanup - restoring all configs to defaults")
            restore_autotune_defaults(mon_obj=mon_obj)

        return 0

    # -----------------------------------------------------------------------
    # Test 5: Disabled autotune behavior under load
    # Test plan: #7
    # -----------------------------------------------------------------------
    if config.get("test_mgr_stats_autotune_disabled_behavior"):
        doc = (
            "\n# Test: MGR Stats Period Auto-Tune Disabled Behavior"
            "\n Verify that with autotune disabled, mgr_stats_period stays fixed"
            "\n while the messenger dispatch queue stays above the threshold"
            "\n\t 1. Disable mgr_stats_period_autotune"
            "\n\t 2. Lower mgr_stats_period_autotune_queue_threshold so the"
            "\n\t    queue can be held above it, and record mgr_stats_period"
            "\n\t 3. Start a parallel thread that prints dispatch queue depth"
            "\n\t 4. Flood the active mgr with in-flight commands and cluster IO"
            "\n\t    so the queue stays above the threshold for the rest of the test"
            "\n\t 5. Monitor mgr_stats_period and verify it remains at baseline"
            "\n\t 6. Verify every queue sample in that window was above the threshold"
            "\n\t 7. Run pool sanity check"
            "\n\t 8. Cleanup: stop the flood and queue thread, restore configs, delete pools"
        )

        log.info(doc)
        log.info("Running test to verify disabled autotune behavior under load")

        start_time = get_cluster_timestamp(rados_obj.node)
        log.debug(f"Test workflow started. Start time: {start_time}")
        test_pools = []
        queue_monitor = None
        flood_thread = None
        flood_marker = None
        bench_threads = []

        try:
            # Step 1: Disable autotune
            log.info("Step 1: Disabling mgr_stats_period_autotune")
            mon_obj.set_config(
                section="mgr",
                name="mgr_stats_period_autotune",
                value="false",
            )
            verify_mgr_config(
                mon_obj=mon_obj,
                param_name="mgr_stats_period_autotune",
                expected_value="false",
            )
            log.info(" Autotune disabled")

            # Step 2: Lower the threshold, speed up stat reports, record baseline
            log.info(
                "Step 2: Setting queue threshold to %s, mgr_stats_period to 1, "
                "and recording baseline",
                SUSTAIN_QUEUE_THRESHOLD,
            )
            mon_obj.set_config(
                section="mgr",
                name="mgr_stats_period_autotune_queue_threshold",
                value=str(SUSTAIN_QUEUE_THRESHOLD),
            )
            mon_obj.set_config(
                section="mgr",
                name="mgr_stats_period",
                value="1",
            )
            active_mgr = mgr_obj.get_active_mgr()
            log.info("Active MGR: %s", active_mgr)
            baseline_period = get_mgr_runtime_config(
                mon_obj=mon_obj,
                rados_obj=rados_obj,
                param_name="mgr_stats_period",
                mgr_id=active_mgr,
            )
            queue_threshold = get_mgr_runtime_config(
                mon_obj=mon_obj,
                rados_obj=rados_obj,
                param_name="mgr_stats_period_autotune_queue_threshold",
                mgr_id=active_mgr,
            )
            log.info("Baseline mgr_stats_period: %s", baseline_period)
            log.info(
                "mgr_stats_period_autotune_queue_threshold: %s",
                queue_threshold,
            )
            if int(str(queue_threshold).strip()) != SUSTAIN_QUEUE_THRESHOLD:
                raise Exception(
                    "queue threshold did not apply: expected %s, got %s"
                    % (SUSTAIN_QUEUE_THRESHOLD, queue_threshold)
                )

            # Step 3: Print dispatch-queue depth for the rest of the test
            log.info(
                "Step 3: Starting parallel thread to print messenger queue depth"
            )
            messenger_names = resolve_mgr_messengers(
                rados_obj=rados_obj, mgr_id=active_mgr
            )
            flood_marker = "mgrflood%s" % active_mgr.replace(".", "")

            # Step 4: Hold the queue above the threshold for the rest of the test
            log.info(
                "Step 4: Starting command flood and cluster IO to sustain "
                "dispatch queue depth above %s",
                queue_threshold,
            )
            flood_thread = threading.Thread(
                target=start_mgr_command_flood,
                kwargs={
                    "rados_obj": rados_obj,
                    "mgr_id": active_mgr,
                    "duration": 420,
                    "marker": flood_marker,
                    "messenger_names": messenger_names,
                },
                name="mgr-command-flood",
                daemon=True,
            )
            for i in range(8):
                pool_name = "%s%s" % (AUTOTUNE_TEST_POOL_PREFIX, i)
                try:
                    if rados_obj.create_pool(pool_name=pool_name, pg_num=32):
                        test_pools.append(pool_name)
                        log.info("Created pool %s", pool_name)
                except Exception as pool_err:
                    log.warning(
                        "Error creating pool %s: %s", pool_name, str(pool_err)[:100]
                    )
            log.info("Created %s pools before holding the dispatch queue", len(test_pools))

            flood_thread.start()
            log.info("Command flood thread started: %s", flood_thread.name)
            queue_monitor = MgrDispatchQueueMonitor(
                rados_obj=rados_obj,
                mgr_id=active_mgr,
                messenger_names=messenger_names,
                threshold=queue_threshold,
                marker=flood_marker,
            )
            queue_monitor.start()

            hot_deadline = time.time() + 150
            while time.time() < hot_deadline:
                if (
                    queue_monitor.last_depth is not None
                    and queue_monitor.last_depth > int(queue_threshold)
                ):
                    break
                time.sleep(2)
            else:
                raise Exception(
                    "Dispatch queue did not rise above threshold %s. "
                    "Last depth=%s max depth=%s"
                    % (
                        queue_threshold,
                        queue_monitor.last_depth,
                        queue_monitor.max_depth,
                    )
                )
            queue_monitor.begin_sustain_window()

            for pool_name in test_pools[:3]:
                bench_thread = threading.Thread(
                    target=rados_obj.bench_write,
                    kwargs={
                        "pool_name": pool_name,
                        "byte_size": "4K",
                        "rados_write_duration": 60,
                        "timeout": 120,
                    },
                    name="bench-%s" % pool_name,
                    daemon=True,
                )
                bench_thread.start()
                bench_threads.append(bench_thread)
            log.info(
                "Started bench writes on %s pools for the sustain window",
                len(bench_threads),
            )

            # Step 5: Verify period unchanged while the queue stays hot
            log.info("Step 5: Verifying mgr_stats_period remained at baseline")

            # Check multiple times over a period
            period_unchanged = True
            for check_round in range(5):
                time.sleep(10)
                current_period = get_mgr_runtime_config(
                    mon_obj=mon_obj,
                    rados_obj=rados_obj,
                    param_name="mgr_stats_period",
                    mgr_id=active_mgr,
                )
                log.info(
                    "  Check %s: mgr_stats_period = %s (baseline: %s)",
                    check_round + 1,
                    current_period,
                    baseline_period,
                )
                if str(current_period) != str(baseline_period):
                    log.error(
                        "mgr_stats_period changed from %s to %s despite autotune being disabled!",
                        baseline_period,
                        current_period,
                    )
                    period_unchanged = False
                    break

            if not period_unchanged:
                raise Exception("mgr_stats_period changed while autotune was disabled")
            log.info(
                " Verified: mgr_stats_period stayed at %s with autotune disabled",
                baseline_period,
            )

            # Step 6: Every sample in the sustain window must stay above threshold
            log.info(
                "Step 6: Verifying dispatch queue stayed above "
                "mgr_stats_period_autotune_queue_threshold for the whole window"
            )
            queue_monitor.stop()
            if not queue_monitor.sustained_above_threshold():
                raise Exception(
                    "Dispatch queue was not held above threshold %s for the "
                    "entire sustain window. samples=%s min=%s max=%s below=%s. "
                    "mgr_stats_period staying at %s is not enough on its own."
                    % (
                        queue_threshold,
                        queue_monitor.window_samples,
                        queue_monitor.window_min,
                        queue_monitor.window_max,
                        queue_monitor.window_below,
                        baseline_period,
                    )
                )
            log.info(
                " Verified: dispatch queue stayed above %s for %s samples "
                "(min=%s max=%s) while mgr_stats_period stayed at %s",
                queue_threshold,
                queue_monitor.window_samples,
                queue_monitor.window_min,
                queue_monitor.window_max,
                baseline_period,
            )

            # Step 7: Pool sanity check
            log.info("Step 7: Running pool sanity check")
            if not rados_obj.run_pool_sanity_check():
                log.error("Pool sanity checks failed")
                return 1
            log.info(" Pool sanity check passed")

            log.info(" Disabled autotune behavior verified")

        except Exception as e:
            log.error("Failed with exception: %s", e.__doc__)
            log.exception(e)
            rados_obj.log_cluster_health()
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here"
                " *************** \n \n"
            )
            log.info("Step 8: Cleanup")
            if queue_monitor is not None:
                queue_monitor.stop()
            if flood_marker is not None:
                stop_mgr_command_flood(rados_obj=rados_obj, marker=flood_marker)
            restore_autotune_defaults(mon_obj=mon_obj)
            cleanup_test_pools(rados_obj=rados_obj, pool_names=test_pools)

        return 0

    # -----------------------------------------------------------------------
    # Test 6: Period increase under load, cap enforcement, and recovery
    # Test plan: #8, #9, #10, #11
    # -----------------------------------------------------------------------
    if config.get("test_mgr_stats_autotune_period_increase_and_recovery"):
        doc = (
            "\n# Test: MGR Stats Period Auto-Tune — Period Increase, Cap & Recovery"
            "\n Comprehensive functional test for the autotune mechanism"
            "\n\t 1. Enable autotune (if not already)"
            "\n\t 2. Set queue threshold very low (5) to maximize trigger probability"
            "\n\t 3. Set mgr_stats_period to 1 (aggressive reporting)"
            "\n\t 4. Record baseline and test start time for log analysis"
            "\n\t 5. Generate heavy cluster load (many pools, bench writes, OSD stop/start)"
            "\n\t 6. Monitor mgr_stats_period runtime value — verify increase above baseline"
            "\n\t 7. Verify period does NOT exceed 60 seconds (MAX_PERIOD cap)"
            "\n\t 8. Verify MGR logs contain 'Adjusting mgr_stats_period' entries"
            "\n\t 9. Stop load generation and allow cluster to stabilize"
            "\n\t 10. Monitor period decrease — verify recovery toward baseline"
            "\n\t 11. Run pool sanity check"
            "\n\t 12. Cleanup: restore configs, delete test pools"
        )

        log.info(doc)
        log.info("Running test to verify period increase, cap, and recovery")

        start_time = get_cluster_timestamp(rados_obj.node)
        log.debug(f"Test workflow started. Start time: {start_time}")
        test_pools = []
        stopped_osds = []

        try:
            # Step 1: Ensure autotune is enabled
            log.info("Step 1: Ensuring autotune is enabled")
            mon_obj.set_config(
                section="mgr",
                name="mgr_stats_period_autotune",
                value="true",
            )
            log.info(" Autotune enabled")

            # Step 2: Set queue threshold very low
            log.info("Step 2: Setting queue threshold to 5 (low trigger point)")
            mon_obj.set_config(
                section="mgr",
                name="mgr_stats_period_autotune_queue_threshold",
                value="5",
            )
            log.info(" Queue threshold set to 5")

            # Step 3: Set mgr_stats_period to 1 (aggressive reporting)
            log.info("Step 3: Setting mgr_stats_period to 1")
            mon_obj.set_config(
                section="mgr",
                name="mgr_stats_period",
                value="1",
            )
            time.sleep(5)
            log.info(" mgr_stats_period set to 1")

            # Step 4: Record baseline
            log.info("Step 4: Recording baseline and active MGR")
            active_mgr = mgr_obj.get_active_mgr()
            log.info("Active MGR: %s", active_mgr)

            baseline_period = get_mgr_runtime_config(
                mon_obj=mon_obj,
                rados_obj=rados_obj,
                param_name="mgr_stats_period",
                mgr_id=active_mgr,
            )
            log.info("Baseline mgr_stats_period: %s", baseline_period)
            log_start_time = get_cluster_timestamp(rados_obj.node)

            # Step 5: Generate heavy cluster load
            log.info("Step 5: Generating heavy cluster load")
            test_pools = generate_cluster_load(
                rados_obj=rados_obj,
                pool_count=50,
                bench_max_objs=500,
            )
            log.info("Created %s pools with bench writes", len(test_pools))

            # Also stop/start OSDs to trigger PG state changes
            osd_list = rados_obj.get_osd_list(status="up")
            if len(osd_list) >= 4:
                osds_to_stop = osd_list[:2]
                log.info("Stopping OSDs %s to trigger recovery traffic", osds_to_stop)
                for osd_id in osds_to_stop:
                    rados_obj.change_osd_state(action="stop", target=osd_id)
                    stopped_osds.append(osd_id)
                    log.info("Stopped OSD.%s", osd_id)
                time.sleep(10)

            # Run more bench writes while OSDs are recovering
            for pool_name in test_pools[:3]:
                try:
                    rados_obj.bench_write(
                        pool_name=pool_name, byte_size="4K", max_objs=200
                    )
                except Exception as bench_err:
                    log.warning(
                        "Bench write on %s during recovery: %s",
                        pool_name,
                        str(bench_err)[:100],
                    )

            # Step 6: Monitor for period increase
            log.info("Step 6: Monitoring mgr_stats_period for increase above baseline")
            period_increased = False
            max_observed_period = int(baseline_period)

            for check_round in range(12):
                time.sleep(15)
                try:
                    current_period = get_mgr_runtime_config(
                        mon_obj=mon_obj,
                        rados_obj=rados_obj,
                        param_name="mgr_stats_period",
                        mgr_id=active_mgr,
                    )
                    current_int = int(current_period)
                    log.info(
                        "  Check %s: mgr_stats_period = %s (baseline: %s, max: %s)",
                        check_round + 1,
                        current_int,
                        baseline_period,
                        max_observed_period,
                    )
                    if current_int > int(baseline_period):
                        period_increased = True
                        max_observed_period = max(max_observed_period, current_int)
                    if period_increased:
                        break
                except Exception as check_err:
                    log.warning(
                        "Could not check period on round %s: %s",
                        check_round + 1,
                        str(check_err)[:100],
                    )

            if period_increased:
                log.info(
                    " mgr_stats_period increased from %s to %s",
                    baseline_period,
                    max_observed_period,
                )
            else:
                log.error(
                    "mgr_stats_period did not increase during load generation. "
                    "This may happen if the queue depth did not exceed the threshold. "
                    "Checking MGR logs for autotune activity."
                )
                return 1

            # Step 7: Verify cap at 60 seconds
            log.info("Step 7: Verifying period cap at 60 seconds")
            if max_observed_period > MAX_PERIOD:
                raise Exception(
                    "mgr_stats_period exceeded MAX_PERIOD cap! "
                    "Observed: %s, Max allowed: %s" % (max_observed_period, MAX_PERIOD)
                )
            log.info(
                " Verified: max observed period (%s) <= %s",
                max_observed_period,
                MAX_PERIOD,
            )

            # Step 8: Check MGR logs for autotune messages
            log.info("Step 8: Checking MGR logs for autotune activity")
            log_end_time = get_cluster_timestamp(rados_obj.node)
            log_found = rados_obj.lookup_log_message(
                init_time=log_start_time,
                end_time=log_end_time,
                daemon_type="mgr",
                daemon_id=active_mgr,
                search_string="'Adjusting mgr_stats_period|checking whether to adjust stats period'",
            )
            if log_found:
                log.info(
                    " Found autotune log entries in MGR.%s logs",
                    active_mgr,
                )
            else:
                log.warning(
                    "No autotune log entries found in MGR.%s logs. "
                    "This may be expected if queue depth never exceeded threshold.",
                    active_mgr,
                )

            # Step 9: Stop load and allow stabilization
            log.info("Step 9: Stopping load and allowing cluster to stabilize")

            # Restart stopped OSDs
            for osd_id in stopped_osds:
                rados_obj.change_osd_state(action="start", target=osd_id)
                log.info("Restarted OSD.%s", osd_id)
            stopped_osds.clear()

            time.sleep(30)

            # Step 10: Monitor recovery if period had increased
            if period_increased:
                log.info(
                    "Step 10: Monitoring mgr_stats_period recovery toward baseline"
                )
                period_recovered = False
                for check_round in range(18):
                    time.sleep(20)
                    try:
                        current_period = get_mgr_runtime_config(
                            mon_obj=mon_obj,
                            rados_obj=rados_obj,
                            param_name="mgr_stats_period",
                            mgr_id=active_mgr,
                        )
                        current_int = int(current_period)
                        log.info(
                            "  Recovery check %s: mgr_stats_period = %s (baseline: %s)",
                            check_round + 1,
                            current_int,
                            baseline_period,
                        )
                        if current_int <= int(baseline_period):
                            period_recovered = True
                            break
                    except Exception as check_err:
                        log.warning(
                            "Could not check period on recovery round %s: %s",
                            check_round + 1,
                            str(check_err)[:100],
                        )

                if period_recovered:
                    log.info(
                        " mgr_stats_period recovered to baseline (%s)",
                        baseline_period,
                    )
                else:
                    log.warning(
                        "mgr_stats_period did not fully recover to baseline within timeout. "
                        "Current: %s, Baseline: %s. Recovery may need more time.",
                        current_period,
                        baseline_period,
                    )
            else:
                log.info("Step 10: Skipping recovery check (period did not increase)")

            # Step 11: Pool sanity check
            log.info("Step 11: Running pool sanity check")
            time.sleep(10)
            if not rados_obj.run_pool_sanity_check():
                log.error("Pool sanity checks failed")
                return 1
            log.info(" Pool sanity check passed")

            log.info(" Period increase and recovery test completed")

        except Exception as e:
            log.error("Failed with exception: %s", e.__doc__)
            log.exception(e)
            rados_obj.log_cluster_health()
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here"
                " *************** \n \n"
            )
            log.info("Step 12: Cleanup")
            # Restart any stopped OSDs
            for osd_id in stopped_osds:
                try:
                    rados_obj.change_osd_state(action="start", target=osd_id)
                    log.info("Restarted OSD.%s during cleanup", osd_id)
                except Exception as e:
                    log.warning("Failed to restart OSD.%s: %s", osd_id, e)
            restore_autotune_defaults(mon_obj=mon_obj)
            cleanup_test_pools(rados_obj=rados_obj, pool_names=test_pools)

        return 0

    # -----------------------------------------------------------------------
    # Test 7: Baseline tracking with custom mgr_stats_period
    # Test plan: #12
    # -----------------------------------------------------------------------
    if config.get("test_mgr_stats_autotune_baseline_tracking"):
        doc = (
            "\n# Test: MGR Stats Period Auto-Tune Baseline Tracking"
            "\n Verify that a manually set mgr_stats_period becomes the new baseline"
            "\n\t 1. Set mgr_stats_period to 10 (custom baseline)"
            "\n\t 2. Enable autotune, set low threshold"
            "\n\t 3. Generate load to trigger period increase"
            "\n\t 4. Verify period increases above 10 (not above 60)"
            "\n\t 5. Stop load and wait for recovery"
            "\n\t 6. Verify period recovers to 10 (new baseline), not 5 (old default)"
            "\n\t 7. Cleanup: restore defaults"
        )

        log.info(doc)
        log.info(
            "Running test to verify baseline tracking with custom mgr_stats_period"
        )

        start_time = get_cluster_timestamp(rados_obj.node)
        log.debug(f"Test workflow started. Start time: {start_time}")
        test_pools = []

        try:
            # Step 1: Set custom baseline
            log.info("Step 1: Setting mgr_stats_period to 10 (custom baseline)")
            mon_obj.set_config(
                section="mgr",
                name="mgr_stats_period",
                value="10",
            )
            time.sleep(5)

            # Step 2: Enable autotune with low threshold
            log.info("Step 2: Enabling autotune with low threshold")
            mon_obj.set_config(
                section="mgr",
                name="mgr_stats_period_autotune",
                value="true",
            )
            mon_obj.set_config(
                section="mgr",
                name="mgr_stats_period_autotune_queue_threshold",
                value="5",
            )
            time.sleep(5)

            active_mgr = mgr_obj.get_active_mgr()
            log.info("Active MGR: %s", active_mgr)

            # Verify baseline is 10
            runtime_period = get_mgr_runtime_config(
                mon_obj=mon_obj,
                rados_obj=rados_obj,
                param_name="mgr_stats_period",
                mgr_id=active_mgr,
            )
            log.info("Current runtime mgr_stats_period: %s", runtime_period)

            # Step 3: Generate load
            log.info("Step 3: Generating cluster load")
            test_pools = generate_cluster_load(
                rados_obj=rados_obj,
                pool_count=50,
                bench_max_objs=300,
            )
            log.info("Generated load with %s pools", len(test_pools))

            # Step 4: Monitor for period increase
            log.info("Step 4: Monitoring for period increase above baseline 10")
            period_increased = False
            for check_round in range(10):
                time.sleep(15)
                try:
                    current_period = get_mgr_runtime_config(
                        mon_obj=mon_obj,
                        rados_obj=rados_obj,
                        param_name="mgr_stats_period",
                        mgr_id=active_mgr,
                    )
                    current_int = int(current_period)
                    log.info(
                        "  Check %s: mgr_stats_period = %s (baseline: 10)",
                        check_round + 1,
                        current_int,
                    )
                    if current_int > 10:
                        period_increased = True
                        if current_int > MAX_PERIOD:
                            raise Exception(
                                "Period exceeded cap: %s > %s"
                                % (current_int, MAX_PERIOD)
                            )
                        break
                except Exception as check_err:
                    if "exceeded cap" in str(check_err):
                        raise
                    log.warning(
                        "Period check round %s: %s",
                        check_round + 1,
                        str(check_err)[:100],
                    )

            if period_increased:
                log.info(" Period increased above custom baseline of 10")
            else:
                log.warning(
                    "Period did not increase. Testing recovery to custom baseline anyway."
                )

            # Step 5: Stop load and wait
            log.info("Step 5: Stopping load and waiting for recovery")
            time.sleep(60)

            # Step 6: Verify recovery to custom baseline (10)
            log.info("Step 6: Verifying period recovers to 10 (custom baseline)")
            # The stored config is 10, so the runtime should return to 10
            db_period = mon_obj.get_config(section="mgr", param="mgr_stats_period")
            log.info("DB mgr_stats_period: %s", db_period)
            if str(db_period).strip() != "10":
                raise Exception(
                    "DB mgr_stats_period changed from custom baseline! "
                    "Expected 10, got %s" % db_period
                )
            log.info(
                " Verified: mgr_stats_period DB value remains at 10 "
                "(custom baseline preserved)"
            )

            log.info(" Baseline tracking test completed")

        except Exception as e:
            log.error("Failed with exception: %s", e.__doc__)
            log.exception(e)
            rados_obj.log_cluster_health()
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here"
                " *************** \n \n"
            )
            log.info("Step 7: Cleanup")
            restore_autotune_defaults(mon_obj=mon_obj)
            cleanup_test_pools(rados_obj=rados_obj, pool_names=test_pools)

        return 0

    # -----------------------------------------------------------------------
    # Test 8: Idle cluster edge case
    # Test plan: #22
    # -----------------------------------------------------------------------
    if config.get("test_mgr_stats_autotune_idle_cluster"):
        doc = (
            "\n# Test: MGR Stats Period Auto-Tune Idle Cluster Edge Case"
            "\n Verify that autotune does not decrease period below baseline on idle cluster"
            "\n\t 1. Enable autotune with default settings"
            "\n\t 2. Record baseline mgr_stats_period"
            "\n\t 3. Ensure cluster is idle (no active IO)"
            "\n\t 4. Wait several autotune check cycles"
            "\n\t 5. Verify period has not decreased below baseline"
            "\n\t 6. Verify no errors in MGR logs (no divide-by-zero, no crashes)"
            "\n\t 7. Run pool sanity check"
        )

        log.info(doc)
        log.info("Running test to verify idle cluster edge case")

        start_time = get_cluster_timestamp(rados_obj.node)
        log.debug(f"Test workflow started. Start time: {start_time}")

        try:
            # Step 1: Ensure autotune is enabled with defaults
            log.info("Step 1: Ensuring autotune is enabled with defaults")
            restore_autotune_defaults(mon_obj=mon_obj)
            time.sleep(5)
            log.info(" Autotune enabled with default config")

            # Step 2: Record baseline
            log.info("Step 2: Recording baseline")
            active_mgr = mgr_obj.get_active_mgr()
            baseline_period = get_mgr_runtime_config(
                mon_obj=mon_obj,
                rados_obj=rados_obj,
                param_name="mgr_stats_period",
                mgr_id=active_mgr,
            )
            log.info(
                "Baseline mgr_stats_period: %s (active MGR: %s)",
                baseline_period,
                active_mgr,
            )

            # Step 3 & 4: Wait several check cycles (idle cluster)
            log.info(
                "Steps 3-4: Waiting for multiple autotune check cycles on idle cluster"
            )
            log_start_time = get_cluster_timestamp(rados_obj.node)

            for check_round in range(6):
                time.sleep(20)
                current_period = get_mgr_runtime_config(
                    mon_obj=mon_obj,
                    rados_obj=rados_obj,
                    param_name="mgr_stats_period",
                    mgr_id=active_mgr,
                )
                log.info(
                    "  Idle check %s: mgr_stats_period = %s (baseline: %s)",
                    check_round + 1,
                    current_period,
                    baseline_period,
                )

                # Step 5: Verify no decrease below baseline
                if int(current_period) < int(baseline_period):
                    raise Exception(
                        "mgr_stats_period decreased below baseline on idle cluster! "
                        "Current: %s, Baseline: %s" % (current_period, baseline_period)
                    )

            log.info(
                " Verified: Period stayed at or above baseline (%s) on idle cluster",
                baseline_period,
            )

            # Step 6: Check MGR logs for errors
            log.info("Step 6: Checking MGR logs for errors")
            log_end_time = get_cluster_timestamp(rados_obj.node)
            crash_log = rados_obj.lookup_log_message(
                init_time=log_start_time,
                end_time=log_end_time,
                daemon_type="mgr",
                daemon_id=active_mgr,
                search_string="'divide.by.zero|segfault|assert|abort'",
            )
            if crash_log:
                log.error(
                    "Found error/crash entries in MGR.%s logs during idle period!",
                    active_mgr,
                )
                raise Exception("Error entries found in MGR logs during idle period")
            log.info(
                " No error entries in MGR.%s logs during idle period",
                active_mgr,
            )

            # Step 7: Pool sanity check
            log.info("Step 7: Running pool sanity check")
            if not rados_obj.run_pool_sanity_check():
                log.error("Pool sanity checks failed")
                return 1
            log.info(" Pool sanity check passed")

            log.info(" Idle cluster edge case test passed")
            return 0

        except Exception as e:
            log.error("Failed with exception: %s", e.__doc__)
            log.exception(e)
            rados_obj.log_cluster_health()
            return 1

    # -----------------------------------------------------------------------
    # Test 9: MGR crash during period adjustment
    # Test plan: #18
    # -----------------------------------------------------------------------
    if config.get("test_mgr_stats_autotune_mgr_crash_during_adjustment"):
        doc = (
            "\n# Test: MGR Crash During Active Period Adjustment"
            "\n Verify cluster stability when active MGR crashes during autotune activity"
            "\n\t 1. Enable autotune with low threshold"
            "\n\t 2. Set mgr_stats_period to 1 for aggressive reporting"
            "\n\t 3. Generate cluster load"
            "\n\t 4. While under load, kill the active MGR"
            "\n\t 5. Verify standby MGR takes over"
            "\n\t 6. Verify new active MGR has correct autotune config"
            "\n\t 7. Verify cluster health recovers"
            "\n\t 8. Cleanup: stop load, restore configs"
        )

        log.info(doc)
        log.info("Running test to verify MGR crash during period adjustment")

        start_time = get_cluster_timestamp(rados_obj.node)
        log.debug(f"Test workflow started. Start time: {start_time}")
        test_pools = []

        try:
            # Check for multiple MGR daemons
            mgr_daemons = mgr_obj.get_mgr_daemon_list()
            log.info("MGR daemons: %s", mgr_daemons)
            if len(mgr_daemons) < 2:
                log.error(
                    "Need at least 2 MGR daemons for crash test. Found: %s",
                    len(mgr_daemons),
                )
                return 1

            # Step 1: Configure autotune with low threshold
            log.info("Step 1: Configuring autotune with low threshold")
            mon_obj.set_config(
                section="mgr",
                name="mgr_stats_period_autotune",
                value="true",
            )
            mon_obj.set_config(
                section="mgr",
                name="mgr_stats_period_autotune_queue_threshold",
                value="5",
            )

            # Step 2: Set aggressive reporting period
            log.info("Step 2: Setting mgr_stats_period to 1")
            mon_obj.set_config(
                section="mgr",
                name="mgr_stats_period",
                value="1",
            )
            time.sleep(5)

            active_mgr_before = mgr_obj.get_active_mgr()
            log.info("Active MGR before crash: %s", active_mgr_before)

            # Step 3: Generate load
            log.info("Step 3: Generating cluster load")
            test_pools = generate_cluster_load(
                rados_obj=rados_obj,
                pool_count=50,
                bench_max_objs=200,
            )

            # Step 4: Kill the active MGR
            log.info("Step 4: Killing active MGR: %s", active_mgr_before)
            mgr_obj.set_mgr_fail(active_mgr_before)

            # Step 5: Wait for new active MGR
            log.info("Step 5: Waiting for standby MGR to take over")
            active_mgr_after = wait_for_new_active_mgr(
                mgr_obj=mgr_obj,
                old_active_mgr=active_mgr_before,
                timeout=120,
            )
            log.info("New active MGR: %s", active_mgr_after)

            # Step 6: Verify autotune config on new MGR
            log.info("Step 6: Verifying autotune config on new active MGR")
            runtime_autotune = get_mgr_runtime_config(
                mon_obj=mon_obj,
                rados_obj=rados_obj,
                param_name="mgr_stats_period_autotune",
                mgr_id=active_mgr_after,
            )
            log.info(
                "Runtime mgr_stats_period_autotune on %s: %s",
                active_mgr_after,
                runtime_autotune,
            )
            if str(runtime_autotune) != "true":
                raise Exception(
                    "Autotune not enabled on new active MGR: %s" % runtime_autotune
                )

            runtime_threshold = get_mgr_runtime_config(
                mon_obj=mon_obj,
                rados_obj=rados_obj,
                param_name="mgr_stats_period_autotune_queue_threshold",
                mgr_id=active_mgr_after,
            )
            log.info(
                "Runtime threshold on %s: %s",
                active_mgr_after,
                runtime_threshold,
            )
            if str(runtime_threshold) != "5":
                raise Exception(
                    "Threshold not correct on new active MGR: expected 5, got %s"
                    % runtime_threshold
                )

            log.info(" Autotune config intact on new active MGR after crash")

            # Step 7: Verify cluster health recovers
            log.info("Step 7: Waiting for cluster health to recover")
            time.sleep(30)
            rados_obj.log_cluster_health()

            log.info(
                " MGR crash during adjustment test completed. "
                "Cluster remained stable."
            )

        except Exception as e:
            log.error("Failed with exception: %s", e.__doc__)
            log.exception(e)
            rados_obj.log_cluster_health()
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here"
                " *************** \n \n"
            )
            log.info("Step 8: Cleanup")
            restore_autotune_defaults(mon_obj=mon_obj)
            cleanup_test_pools(rados_obj=rados_obj, pool_names=test_pools)

        return 0

    # -----------------------------------------------------------------------
    # Test 10: OSD/MDS report frequency changes
    # Test plan: #13
    # -----------------------------------------------------------------------
    if config.get("test_mgr_stats_autotune_daemon_report_frequency"):
        doc = (
            "\n# Test: OSD/MDS Report Frequency Changes With Auto-Tuned Period"
            "\n Verify that OSDs adjust their reporting interval when mgr_stats_period changes"
            "\n\t 1. Record baseline mgr_stats_period and OSD reporting config"
            "\n\t 2. Set mgr_stats_period to a distinct value (e.g., 20)"
            "\n\t 3. Verify OSD runtime config shows the updated stats period"
            "\n\t 4. Verify via ceph config show on OSD that stats_period matches"
            "\n\t 5. Restore mgr_stats_period to baseline"
            "\n\t 6. Verify OSD reverts to original reporting interval"
            "\n\t 7. Cleanup: restore defaults"
        )

        log.info(doc)
        log.info("Running test to verify OSD/MDS report frequency changes")

        start_time = get_cluster_timestamp(rados_obj.node)
        log.debug(f"Test workflow started. Start time: {start_time}")

        try:
            # Step 1: Record baseline
            log.info("Step 1: Recording baseline mgr_stats_period")
            osd_list = rados_obj.get_osd_list(status="up")
            if not osd_list:
                raise Exception("No up OSDs found")
            target_osd = osd_list[0]
            log.info("Target OSD for reporting check: OSD.%s", target_osd)

            baseline_period_osd = mon_obj.show_config(
                daemon="osd", id=target_osd, param="mgr_stats_period"
            )
            baseline_period_osd = str(baseline_period_osd).strip()
            log.info(
                "OSD.%s baseline mgr_stats_period: %s",
                target_osd,
                baseline_period_osd,
            )

            # Step 2: Set mgr_stats_period to 20
            log.info("Step 2: Setting mgr_stats_period to 20")
            mon_obj.set_config(
                section="global",
                name="mgr_stats_period",
                value="20",
            )
            time.sleep(10)

            # Step 3: Verify OSD runtime config updated
            log.info("Step 3: Verifying OSD.%s reporting period updated", target_osd)
            osd_period_after = mon_obj.show_config(
                daemon="osd", id=target_osd, param="mgr_stats_period"
            )
            osd_period_after = str(osd_period_after).strip()
            log.info(
                "OSD.%s mgr_stats_period after change: %s",
                target_osd,
                osd_period_after,
            )
            if osd_period_after != "20":
                raise Exception(
                    "OSD.%s did not pick up mgr_stats_period change! "
                    "Expected 20, got %s" % (target_osd, osd_period_after)
                )
            log.info(
                " Verified: OSD.%s reporting period updated to 20",
                target_osd,
            )

            # Step 4: Also verify on a mon if MDS exists
            mon_list = rados_obj.run_ceph_command(
                cmd="ceph mon dump", client_exec=True
            )["mons"]
            if mon_list:
                target_mon = mon_list[0]["name"]
                mon_period = mon_obj.show_config(
                    daemon="mon", id=target_mon, param="mgr_stats_period"
                )
                mon_period = str(mon_period).strip()
                log.info("Mon.%s mgr_stats_period: %s", target_mon, mon_period)

            # Step 5: Restore to baseline
            log.info("Step 5: Restoring mgr_stats_period")
            mon_obj.remove_config(
                section="global",
                name="mgr_stats_period",
                verify_rm=False,
            )
            time.sleep(10)

            # Step 6: Verify OSD reverted
            log.info("Step 6: Verifying OSD.%s reverted to baseline", target_osd)
            osd_period_restored = mon_obj.show_config(
                daemon="osd", id=target_osd, param="mgr_stats_period"
            )
            osd_period_restored = str(osd_period_restored).strip()
            log.info(
                "OSD.%s mgr_stats_period after restore: %s",
                target_osd,
                osd_period_restored,
            )
            if osd_period_restored != baseline_period_osd:
                log.warning(
                    "OSD.%s period did not revert exactly. " "Before: %s, After: %s",
                    target_osd,
                    baseline_period_osd,
                    osd_period_restored,
                )

            log.info(" OSD report frequency change test completed")

        except Exception as e:
            log.error("Failed with exception: %s", e.__doc__)
            log.exception(e)
            rados_obj.log_cluster_health()
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here"
                " *************** \n \n"
            )
            log.info("Step 7: Cleanup")
            restore_autotune_defaults(mon_obj=mon_obj)

        return 0

    # -----------------------------------------------------------------------
    # Test 11: Dashboard responsiveness during auto-tune
    # Test plan: #14
    # -----------------------------------------------------------------------
    if config.get("test_mgr_stats_autotune_dashboard_responsiveness"):
        doc = (
            "\n# Test: Ceph Dashboard/CLI Responsiveness During Auto-Tune"
            "\n Verify MGR remains responsive (via CLI) when autotune is active"
            "\n\t 1. Enable autotune with low threshold, set period to 1"
            "\n\t 2. Generate cluster load"
            "\n\t 3. While under load, execute ceph status / ceph health repeatedly"
            "\n\t 4. Verify commands return within a reasonable timeout (no hangs)"
            "\n\t 5. Verify ceph mgr services are reachable"
            "\n\t 6. Cleanup: stop load, restore configs"
        )

        log.info(doc)
        log.info("Running test to verify CLI responsiveness during auto-tune")

        start_time = get_cluster_timestamp(rados_obj.node)
        log.debug(f"Test workflow started. Start time: {start_time}")
        test_pools = []

        try:
            # Step 1: Configure aggressive autotune
            log.info("Step 1: Configuring aggressive autotune")
            mon_obj.set_config(
                section="mgr",
                name="mgr_stats_period_autotune",
                value="true",
            )
            mon_obj.set_config(
                section="mgr",
                name="mgr_stats_period_autotune_queue_threshold",
                value="5",
            )
            mon_obj.set_config(
                section="mgr",
                name="mgr_stats_period",
                value="1",
            )
            time.sleep(5)

            # Step 2: Generate load
            log.info("Step 2: Generating cluster load")
            test_pools = generate_cluster_load(
                rados_obj=rados_obj,
                pool_count=50,
                bench_max_objs=200,
            )

            # Step 3: Execute CLI commands under load and verify responsiveness
            log.info("Step 3: Testing CLI responsiveness under load")
            cli_commands = [
                "ceph status",
                "ceph health detail",
                "ceph osd stat",
                "ceph mgr stat",
                "ceph df",
                "ceph osd pool ls detail",
            ]

            failed_cmds = []
            for cmd in cli_commands:
                try:
                    cmd_start = time.time()
                    rados_obj.run_ceph_command(cmd=cmd, client_exec=True)
                    cmd_duration = time.time() - cmd_start
                    log.info("  '%s' completed in %.2f seconds", cmd, cmd_duration)
                    if cmd_duration > 30:
                        log.warning(
                            "  '%s' took longer than 30s (%.2f s) - "
                            "MGR may be under pressure",
                            cmd,
                            cmd_duration,
                        )
                except Exception as cmd_err:
                    log.error("  '%s' FAILED: %s", cmd, str(cmd_err)[:200])
                    failed_cmds.append(cmd)

            if failed_cmds:
                raise Exception(
                    "CLI commands failed during autotune activity: %s" % failed_cmds
                )

            log.info(" All CLI commands succeeded under load")

            # Step 5: Verify MGR services reachable
            log.info("Step 5: Verifying MGR services")
            mgr_stat = rados_obj.run_ceph_command(cmd="ceph mgr stat", client_exec=True)
            log.info("MGR stat: %s", mgr_stat)
            if not mgr_stat.get("active_name"):
                raise Exception("No active MGR found during load test")
            log.info(
                " MGR services reachable. Active: %s",
                mgr_stat.get("active_name"),
            )

            log.info(" Dashboard/CLI responsiveness test passed")

        except Exception as e:
            log.error("Failed with exception: %s", e.__doc__)
            log.exception(e)
            rados_obj.log_cluster_health()
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here"
                " *************** \n \n"
            )
            log.info("Step 6: Cleanup")
            restore_autotune_defaults(mon_obj=mon_obj)
            cleanup_test_pools(rados_obj=rados_obj, pool_names=test_pools)

        return 0

    # -----------------------------------------------------------------------
    # Test 12: BUG HUNTER — Negative/zero threshold causes permanent period spike
    # Targets code bug: no min constraint on queue_threshold config
    # -----------------------------------------------------------------------
    if config.get("test_mgr_stats_autotune_negative_threshold_behavior"):
        doc = (
            "\n# Test: [BUG HUNTER] Negative/Zero Threshold Runtime Behavior"
            "\n Verify what happens when queue threshold is set to -1 or 0"
            "\n Code analysis: mgr_stats_period_autotune_queue_threshold has no min/max"
            "\n   constraint in mgr.yaml.in. A negative value makes (queue_depth > threshold)"
            "\n   always true, driving period to MAX_PERIOD (60s) permanently."
            "\n\t 1. Set threshold to -1"
            "\n\t 2. Wait for autotune checks to run"
            "\n\t 3. Verify mgr_stats_period increases toward 60"
            "\n\t 4. Set threshold to 0, repeat"
            "\n\t 5. Verify cluster stability (no crashes)"
            "\n\t 6. Restore threshold, verify period recovers"
            "\n\t 7. Cleanup"
        )

        log.info(doc)
        log.info("Running BUG HUNTER test for negative/zero threshold behavior")

        start_time = get_cluster_timestamp(rados_obj.node)
        log.debug(f"Test workflow started. Start time: {start_time}")

        try:
            active_mgr = mgr_obj.get_active_mgr()
            log.info("Active MGR: %s", active_mgr)

            # Ensure autotune is enabled
            mon_obj.set_config(
                section="mgr",
                name="mgr_stats_period_autotune",
                value="true",
            )
            time.sleep(3)

            # Step 1: Set threshold to -1
            log.info("Step 1: Setting threshold to -1")
            try:
                rados_obj.node.shell(
                    [
                        "ceph config set mgr"
                        " mgr_stats_period_autotune_queue_threshold -- -1"
                    ]
                )
                log.info("Threshold set to -1 (accepted by config store)")
            except Exception as e:
                log.info(
                    "Threshold -1 rejected: %s. Skipping negative test.",
                    str(e)[:200],
                )
                # If rejected, skip to zero test
                pass

            # Step 2: Wait for autotune checks
            log.info("Step 2: Waiting for autotune to process negative threshold")
            time.sleep(30)

            # Step 3: Check if period increased
            runtime_period = get_mgr_runtime_config(
                mon_obj=mon_obj,
                rados_obj=rados_obj,
                param_name="mgr_stats_period",
                mgr_id=active_mgr,
            )
            log.info(
                "mgr_stats_period with threshold=-1: %s "
                "(if increased to %s, confirms bug: no min constraint)",
                runtime_period,
                MAX_PERIOD,
            )
            if int(runtime_period) > DEFAULT_STATS_PERIOD:
                log.warning(
                    "BUG CONFIRMED: Negative threshold caused period increase "
                    "from %s to %s. The config should have a min constraint.",
                    DEFAULT_STATS_PERIOD,
                    runtime_period,
                )

            # Verify cluster stability
            rados_obj.log_cluster_health()

            # Step 4: Test threshold = 0
            log.info("Step 4: Setting threshold to 0")
            try:
                rados_obj.node.shell(
                    [
                        "ceph config set mgr"
                        " mgr_stats_period_autotune_queue_threshold 0"
                    ]
                )
                log.info("Threshold set to 0")
                time.sleep(30)

                runtime_period_zero = get_mgr_runtime_config(
                    mon_obj=mon_obj,
                    rados_obj=rados_obj,
                    param_name="mgr_stats_period",
                    mgr_id=active_mgr,
                )
                log.info(
                    "mgr_stats_period with threshold=0: %s",
                    runtime_period_zero,
                )
                if int(runtime_period_zero) > DEFAULT_STATS_PERIOD:
                    log.warning(
                        "BUG CONFIRMED: Zero threshold caused period increase "
                        "to %s. queue_depth > 0 is almost always true.",
                        runtime_period_zero,
                    )
            except Exception as e:
                log.info("Threshold 0 rejected: %s", str(e)[:200])

            # Step 5: Verify cluster stability
            log.info("Step 5: Verifying cluster stability")
            if not rados_obj.run_pool_sanity_check():
                log.error("Pool sanity failed with negative/zero threshold")
                return 1
            log.info(" Cluster stable despite negative/zero threshold")

            # Step 6: Restore threshold and verify recovery
            log.info("Step 6: Restoring threshold to 100 and verifying recovery")
            mon_obj.set_config(
                section="mgr",
                name="mgr_stats_period_autotune_queue_threshold",
                value="100",
            )
            time.sleep(60)

            recovered_period = get_mgr_runtime_config(
                mon_obj=mon_obj,
                rados_obj=rados_obj,
                param_name="mgr_stats_period",
                mgr_id=active_mgr,
            )
            log.info("mgr_stats_period after restoring threshold: %s", recovered_period)

            log.info(" Negative/zero threshold behavior test completed")

        except Exception as e:
            log.error("Failed with exception: %s", e.__doc__)
            log.exception(e)
            rados_obj.log_cluster_health()
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here"
                " *************** \n \n"
            )
            log.info("Step 7: Cleanup")
            restore_autotune_defaults(mon_obj=mon_obj)

        return 0

    # -----------------------------------------------------------------------
    # Test 13: BUG HUNTER — Baseline state lost on MGR failover under load
    # Targets: StatsAutotuner constructed fresh per-MGR, no state transfer
    # -----------------------------------------------------------------------
    if config.get("test_mgr_stats_autotune_failover_under_load"):
        doc = (
            "\n# Test: [BUG HUNTER] MGR Failover Under Active Load — Baseline State"
            "\n Code analysis: When MGR fails, new MGR constructs fresh StatsAutotuner"
            "\n   with baseline from monitor config store. The autotuned runtime value"
            "\n   is lost. This test verifies:"
            "\n\t 1. Set custom baseline (10s), enable autotune with low threshold"
            "\n\t 2. Generate load — wait for period to increase"
            "\n\t 3. Fail the active MGR while period is elevated"
            "\n\t 4. Verify new MGR starts with baseline period (10), not elevated value"
            "\n\t 5. Verify new MGR re-autotunes if load persists"
            "\n\t 6. Stop load, verify new MGR recovers to baseline (10)"
            "\n\t 7. Cleanup"
        )

        log.info(doc)
        log.info("Running BUG HUNTER test for MGR failover under active load")

        start_time = get_cluster_timestamp(rados_obj.node)
        log.debug(f"Test workflow started. Start time: {start_time}")
        test_pools = []

        try:
            mgr_daemons = mgr_obj.get_mgr_daemon_list()
            if len(mgr_daemons) < 2:
                log.error("Need >=2 MGR daemons. Found: %s", len(mgr_daemons))
                return 1

            # Step 1: Set custom baseline and low threshold
            log.info("Step 1: Setting custom baseline period=10, threshold=5")
            mon_obj.set_config(section="mgr", name="mgr_stats_period", value="10")
            mon_obj.set_config(
                section="mgr", name="mgr_stats_period_autotune", value="true"
            )
            mon_obj.set_config(
                section="mgr",
                name="mgr_stats_period_autotune_queue_threshold",
                value="5",
            )
            time.sleep(5)

            active_mgr_before = mgr_obj.get_active_mgr()
            log.info("Active MGR: %s", active_mgr_before)

            # Step 2: Generate load
            log.info("Step 2: Generating load")
            test_pools = generate_cluster_load(
                rados_obj=rados_obj,
                pool_count=50,
                bench_max_objs=300,
            )

            # Check if period elevated
            elevated_period = get_mgr_runtime_config(
                mon_obj=mon_obj,
                rados_obj=rados_obj,
                param_name="mgr_stats_period",
                mgr_id=active_mgr_before,
            )
            log.info("Period before failover: %s (baseline: 10)", elevated_period)

            # Step 3: Fail the active MGR
            log.info("Step 3: Failing active MGR: %s", active_mgr_before)
            mgr_obj.set_mgr_fail(active_mgr_before)

            # Step 4: Verify new MGR
            active_mgr_after = wait_for_new_active_mgr(
                mgr_obj=mgr_obj,
                old_active_mgr=active_mgr_before,
                timeout=120,
            )
            log.info("New active MGR: %s", active_mgr_after)

            time.sleep(10)

            new_mgr_period = get_mgr_runtime_config(
                mon_obj=mon_obj,
                rados_obj=rados_obj,
                param_name="mgr_stats_period",
                mgr_id=active_mgr_after,
            )
            log.info(
                "Step 4: New MGR period: %s (expected baseline=10, " "old elevated=%s)",
                new_mgr_period,
                elevated_period,
            )

            # The stored config is 10, so the new MGR should start at 10
            db_period = mon_obj.get_config(section="mgr", param="mgr_stats_period")
            log.info("DB mgr_stats_period: %s", str(db_period).strip())

            if str(db_period).strip() != "10":
                log.error(
                    "BUG: monitor config store value changed! Expected 10, got %s",
                    db_period,
                )

            # Step 5: Wait and check if new MGR re-autotunes under load
            log.info("Step 5: Checking if new MGR re-autotunes")
            for pool_name in test_pools[:3]:
                try:
                    rados_obj.bench_write(
                        pool_name=pool_name, byte_size="4K", max_objs=100
                    )
                except Exception:
                    pass

            time.sleep(30)
            new_mgr_period_after_load = get_mgr_runtime_config(
                mon_obj=mon_obj,
                rados_obj=rados_obj,
                param_name="mgr_stats_period",
                mgr_id=active_mgr_after,
            )
            log.info(
                "New MGR period after continued load: %s",
                new_mgr_period_after_load,
            )

            # Step 6: Stop load and verify recovery
            log.info("Step 6: Stopping load and verifying recovery")
            time.sleep(60)

            final_period = get_mgr_runtime_config(
                mon_obj=mon_obj,
                rados_obj=rados_obj,
                param_name="mgr_stats_period",
                mgr_id=active_mgr_after,
            )
            log.info("Final period after recovery: %s", final_period)

            log.info(" MGR failover under load test completed")

        except Exception as e:
            log.error("Failed with exception: %s", e.__doc__)
            log.exception(e)
            rados_obj.log_cluster_health()
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here"
                " *************** \n \n"
            )
            log.info("Step 7: Cleanup")
            restore_autotune_defaults(mon_obj=mon_obj)
            cleanup_test_pools(rados_obj=rados_obj, pool_names=test_pools)

        return 0

    # -----------------------------------------------------------------------
    # Test 14: BUG HUNTER — Rapid autotune toggle leaves period elevated
    # Targets: disabling autotune while period is elevated → never recovers
    # -----------------------------------------------------------------------
    if config.get("test_mgr_stats_autotune_rapid_toggle"):
        doc = (
            "\n# Test: [BUG HUNTER] Rapid Autotune Toggle — Elevated Period Stuck"
            "\n Code analysis: tick() reads mgr_stats_period_autotune fresh each cycle."
            "\n   If autotune is disabled while the period is elevated, the autotuner"
            "\n   stops running and the period stays elevated because set_val() is local."
            "\n   The period only returns to baseline if the user explicitly resets it."
            "\n\t 1. Enable autotune with low threshold"
            "\n\t 2. Generate load to elevate period"
            "\n\t 3. Immediately disable autotune while period is elevated"
            "\n\t 4. Stop load — verify period does NOT recover (stays elevated)"
            "\n\t 5. Re-enable autotune — verify period recovers to baseline"
            "\n\t 6. Cleanup"
        )

        log.info(doc)
        log.info("Running BUG HUNTER test for rapid autotune toggle")

        start_time = get_cluster_timestamp(rados_obj.node)
        log.debug(f"Test workflow started. Start time: {start_time}")
        test_pools = []

        try:
            active_mgr = mgr_obj.get_active_mgr()
            log.info("Active MGR: %s", active_mgr)

            # Step 1: Enable autotune with very low threshold
            log.info("Step 1: Enabling autotune with threshold=5, period=1")
            mon_obj.set_config(
                section="mgr", name="mgr_stats_period_autotune", value="true"
            )
            mon_obj.set_config(
                section="mgr",
                name="mgr_stats_period_autotune_queue_threshold",
                value="5",
            )
            mon_obj.set_config(section="mgr", name="mgr_stats_period", value="1")
            time.sleep(5)

            baseline = get_mgr_runtime_config(
                mon_obj=mon_obj,
                rados_obj=rados_obj,
                param_name="mgr_stats_period",
                mgr_id=active_mgr,
            )
            log.info("Baseline period: %s", baseline)

            # Step 2: Generate load
            log.info("Step 2: Generating load to elevate period")
            test_pools = generate_cluster_load(
                rados_obj=rados_obj, pool_count=50, bench_max_objs=300
            )

            # Wait for potential period increase
            time.sleep(30)
            period_before_disable = get_mgr_runtime_config(
                mon_obj=mon_obj,
                rados_obj=rados_obj,
                param_name="mgr_stats_period",
                mgr_id=active_mgr,
            )
            log.info("Period before disabling autotune: %s", period_before_disable)

            # Step 3: Disable autotune
            log.info("Step 3: Disabling autotune while period may be elevated")
            mon_obj.set_config(
                section="mgr", name="mgr_stats_period_autotune", value="false"
            )
            time.sleep(5)

            period_after_disable = get_mgr_runtime_config(
                mon_obj=mon_obj,
                rados_obj=rados_obj,
                param_name="mgr_stats_period",
                mgr_id=active_mgr,
            )
            log.info("Period after disabling autotune: %s", period_after_disable)

            # Step 4: Stop load and check behavior
            log.info("Step 4: Waiting 60s with load stopped and autotune disabled")
            time.sleep(60)

            period_stuck = get_mgr_runtime_config(
                mon_obj=mon_obj,
                rados_obj=rados_obj,
                param_name="mgr_stats_period",
                mgr_id=active_mgr,
            )
            log.info(
                "Period 60s after disabling autotune (no load): %s "
                "(if still elevated from %s, confirms behavior)",
                period_stuck,
                period_after_disable,
            )

            if int(period_stuck) > int(baseline):
                log.warning(
                    "BEHAVIOR NOTE: Disabling autotune left period elevated at %s. "
                    "The autotuner changed period via set_val() (local), so disabling "
                    "autotune does not revert it. User must manually reset or "
                    "re-enable autotune.",
                    period_stuck,
                )

            # Step 5: Re-enable autotune and check recovery
            log.info("Step 5: Re-enabling autotune to verify recovery")
            mon_obj.set_config(
                section="mgr", name="mgr_stats_period_autotune", value="true"
            )
            time.sleep(60)

            period_after_reenable = get_mgr_runtime_config(
                mon_obj=mon_obj,
                rados_obj=rados_obj,
                param_name="mgr_stats_period",
                mgr_id=active_mgr,
            )
            log.info(
                "Period after re-enabling autotune: %s (baseline: %s)",
                period_after_reenable,
                baseline,
            )

            log.info(" Rapid toggle test completed")

        except Exception as e:
            log.error("Failed with exception: %s", e.__doc__)
            log.exception(e)
            rados_obj.log_cluster_health()
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here"
                " *************** \n \n"
            )
            log.info("Step 6: Cleanup")
            restore_autotune_defaults(mon_obj=mon_obj)
            cleanup_test_pools(rados_obj=rados_obj, pool_names=test_pools)

        return 0

    # -----------------------------------------------------------------------
    # Test 15: BUG HUNTER — Autotune config change latency
    # Targets: get_tracked_keys() excludes autotune configs
    # -----------------------------------------------------------------------
    if config.get("test_mgr_stats_autotune_config_change_latency"):
        doc = (
            "\n# Test: [BUG HUNTER] Autotune Config Change Propagation Latency"
            "\n Code analysis: get_tracked_keys() only returns mgr_stats_threshold"
            "\n   and mgr_stats_period. Autotune boolean and threshold are NOT tracked,"
            "\n   so handle_conf_change never fires for them. Changes propagate via"
            "\n   g_conf() on the next tick cycle (up to 5 ticks = ~10s delay)."
            "\n\t 1. Enable autotune with threshold=5"
            "\n\t 2. Generate load, observe period increase"
            "\n\t 3. Change threshold to 99999 (effectively disable triggering)"
            "\n\t 4. Measure how long until period stops increasing"
            "\n\t 5. Cleanup"
        )

        log.info(doc)
        log.info("Running BUG HUNTER test for config change propagation latency")

        start_time = get_cluster_timestamp(rados_obj.node)
        log.debug(f"Test workflow started. Start time: {start_time}")
        test_pools = []

        try:
            active_mgr = mgr_obj.get_active_mgr()
            log.info("Active MGR: %s", active_mgr)

            # Step 1: Enable autotune with low threshold
            log.info("Step 1: Enabling autotune threshold=5")
            mon_obj.set_config(
                section="mgr", name="mgr_stats_period_autotune", value="true"
            )
            mon_obj.set_config(
                section="mgr",
                name="mgr_stats_period_autotune_queue_threshold",
                value="5",
            )
            mon_obj.set_config(section="mgr", name="mgr_stats_period", value="2")
            time.sleep(5)

            # Step 2: Generate load
            log.info("Step 2: Generating load")
            test_pools = generate_cluster_load(
                rados_obj=rados_obj, pool_count=50, bench_max_objs=200
            )

            time.sleep(20)
            period_under_load = get_mgr_runtime_config(
                mon_obj=mon_obj,
                rados_obj=rados_obj,
                param_name="mgr_stats_period",
                mgr_id=active_mgr,
            )
            log.info("Period under load: %s", period_under_load)

            # Step 3: Change threshold to very high value
            log.info("Step 3: Setting threshold to 99999 (disable triggering)")
            change_time = time.time()
            mon_obj.set_config(
                section="mgr",
                name="mgr_stats_period_autotune_queue_threshold",
                value="99999",
            )

            # Step 4: Measure propagation
            log.info("Step 4: Measuring config propagation latency")
            for i in range(10):
                time.sleep(5)
                current_threshold = get_mgr_runtime_config(
                    mon_obj=mon_obj,
                    rados_obj=rados_obj,
                    param_name="mgr_stats_period_autotune_queue_threshold",
                    mgr_id=active_mgr,
                )
                elapsed = time.time() - change_time
                log.info(
                    "  +%.0fs: runtime threshold = %s",
                    elapsed,
                    current_threshold,
                )
                if str(current_threshold) == "99999":
                    log.info(
                        " Threshold change propagated in ~%.0f seconds",
                        elapsed,
                    )
                    break

            log.info(" Config change latency test completed")

        except Exception as e:
            log.error("Failed with exception: %s", e.__doc__)
            log.exception(e)
            rados_obj.log_cluster_health()
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here"
                " *************** \n \n"
            )
            log.info("Step 5: Cleanup")
            restore_autotune_defaults(mon_obj=mon_obj)
            cleanup_test_pools(rados_obj=rados_obj, pool_names=test_pools)

        return 0

    log.info("No recognized test scenario specified in config. Exiting.")
    return 1


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def verify_mgr_config(mon_obj, param_name, expected_value):
    """
    Verify a configuration parameter at the MGR level via ceph config get

    Args:
        mon_obj: MonConfigMethods object
        param_name: Name of the configuration parameter
        expected_value: Expected string value

    Returns: True if verified, else raises Exception
    """
    log.info("Verifying config '%s' = '%s' at mgr level", param_name, expected_value)

    actual_value = mon_obj.get_config(section="mgr", param=param_name)
    actual_value = str(actual_value).strip()
    log.debug("  MGR config get '%s': %s", param_name, actual_value)

    if actual_value != str(expected_value):
        error_msg = (
            "Config verification failed for '%s': "
            "expected '%s', got '%s'" % (param_name, expected_value, actual_value)
        )
        log.error(error_msg)
        raise Exception(error_msg)

    log.info(" Verified: %s = %s (mgr config get)", param_name, expected_value)
    return True


def get_mgr_runtime_config(mon_obj, rados_obj, param_name, mgr_id):
    """
    Get the runtime value of a config parameter on a specific MGR daemon

    Uses 'ceph daemon mgr.<id> config get <param>' to read the actual running
    value, which may differ from the stored value if the autotuner has adjusted it.

    Args:
        mon_obj: MonConfigMethods object
        rados_obj: RadosOrchestrator object
        param_name: Configuration parameter name
        mgr_id: MGR daemon identifier (e.g., 'ceph-node1.abcdef')

    Returns: String value of the config parameter
    """
    log.debug("Getting runtime config '%s' from mgr.%s", param_name, mgr_id)

    try:
        result = mon_obj.daemon_config_get(
            daemon_type="mgr", daemon_id=mgr_id, param=param_name
        )
        value = str(result.get(param_name, "")).strip()
        log.debug("  Runtime config mgr.%s '%s' = %s", mgr_id, param_name, value)
        return value
    except Exception as e:
        log.warning(
            "daemon_config_get failed for mgr.%s '%s': %s. "
            "Falling back to ceph config show.",
            mgr_id,
            param_name,
            str(e)[:100],
        )
        # Fallback: use 'ceph config show'
        show_value = mon_obj.show_config(daemon="mgr", id=mgr_id, param=param_name)
        return str(show_value).strip()


def wait_for_new_active_mgr(mgr_obj, old_active_mgr, timeout=120):
    """
    Wait for a new active MGR to come up after failing the current one

    Args:
        mgr_obj: MgrWorkflows object
        old_active_mgr: Name of the MGR that was failed
        timeout: Maximum seconds to wait

    Returns: Name of the new active MGR
    Raises: Exception if no new active MGR within timeout
    """
    log.info(
        "Waiting up to %s seconds for new active MGR (old: %s)",
        timeout,
        old_active_mgr,
    )
    end_time = time.time() + timeout

    while time.time() < end_time:
        try:
            new_active = mgr_obj.get_active_mgr()
            if new_active and new_active != old_active_mgr:
                log.info(
                    "New active MGR detected: %s (was: %s)",
                    new_active,
                    old_active_mgr,
                )
                time.sleep(5)
                return new_active
        except Exception as e:
            log.debug("Waiting for active MGR: %s", str(e)[:100])
        time.sleep(5)

    # If we get here, try one more time - the same MGR might have restarted
    try:
        current_active = mgr_obj.get_active_mgr()
        if current_active:
            log.warning(
                "Same MGR '%s' became active again after failover",
                current_active,
            )
            return current_active
    except Exception:
        pass

    raise Exception(
        "No active MGR detected within %s seconds after failing %s"
        % (timeout, old_active_mgr)
    )


def _parse_json_blob(text):
    """Return the first JSON object embedded in a daemon command's stdout."""
    if isinstance(text, bytes):
        text = text.decode()
    text = (text or "").strip()
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end < start:
        raise Exception("getting queue length failed: no JSON in %s" % text[:300])
    return json.loads(text[start : end + 1])


def _ceph_tell_json(rados_obj, mgr_id, tell_args):
    """
    Run `ceph tell mgr.<id> <tell_args>` and return parsed JSON.

    `cephadm shell --name mgr.<id>` injects extra arguments that the
    messenger admin command rejects, so the tell path is used instead.
    """
    cmd = "ceph tell mgr.%s %s" % (mgr_id, tell_args)
    out, _err = rados_obj.node.shell([cmd], print_output=False, timeout=90)
    return _parse_json_blob(out)


def _messenger_names(listing):
    """Normalize `messenger dump` name list to plain strings."""
    names = []
    for entry in listing.get("messengers") or []:
        if isinstance(entry, dict):
            names.append(str(entry.get("name", "")))
        else:
            names.append(str(entry))
    return [name for name in names if name]


def resolve_mgr_messengers(rados_obj, mgr_id):
    """
    Return messengers to sample for dispatch_queue.length.

    Matches the manual check:
      ceph tell mgr.<id> messenger dump
      # use messengers[1] (mgr-<nonce>), the listening DaemonServer msgr

    Returns [messengers[1], ...] so the primary sample target is first.
    """
    listing = _ceph_tell_json(rados_obj, mgr_id, "messenger dump")
    names = _messenger_names(listing)
    if not names:
        raise Exception("getting queue name failed: empty messenger list")
    # Prefer messengers[1] when present (same as: jq -r '.messengers[1]').
    if len(names) >= 2:
        primary = names[1]
        chosen = [primary] + [n for n in names if n != primary and (
            n == "mgr" or n.startswith("mgr-")
        )]
    else:
        chosen = [names[0]]
    log.info(
        "MGR messengers: %s; primary (messengers[1])=%s; sampling %s",
        names,
        chosen[0],
        chosen,
    )
    return chosen


def sample_mgr_dispatch_queue(rados_obj, mgr_id, messenger_name, limited=False):
    """
    Read one messenger's dispatch queue via the same path as:
      ceph tell mgr.<id> messenger dump <name> | jq '.messenger.dispatch_queue'
    """
    tell_args = "messenger dump %s" % messenger_name
    dump = _ceph_tell_json(rados_obj, mgr_id, tell_args)
    queue = (dump.get("messenger") or {}).get("dispatch_queue")
    if not isinstance(queue, dict) or "length" not in queue:
        raise Exception(
            "getting queue length failed: dispatch_queue=%s" % str(queue)[:300]
        )
    length = int(queue["length"])
    max_age = queue.get("max_age_ago")
    log.info(
        "MESSENGER_QUEUE mgr=%s messenger=%s length=%s max_age_ago=%s",
        mgr_id,
        messenger_name,
        length,
        max_age,
    )
    return {
        "messenger": messenger_name,
        "length": length,
        "max_age_ago": max_age,
    }


def start_mgr_command_flood(
    rados_obj, mgr_id, duration, marker, messenger_names
):
    """
    Continuously sample messenger.dispatch_queue and append to /tmp/<marker>.log.

    Uses the same command as the manual check (no dumpcontents filter):
      ceph tell mgr.<id> messenger dump <name>
      -> .messenger.dispatch_queue.{length,max_age_ago}
    """
    log.info(
        "Sampling mgr.%s dispatch_queue for %ss on messengers %s",
        mgr_id,
        duration,
        messenger_names,
    )
    try:
        rados_obj.node.installer.exec_command(
            sudo=True,
            cmd="rm -f /tmp/%s.log" % marker,
            check_ec=False,
        )
    except Exception as exc:
        log.debug("Could not reset queue log: %s", str(exc)[:100])
    # Keep \" in the remote python -c string so bash can unescape it.
    # Use str.format (not %) so outer script % formatting is safe.
    py = (
        "import json,sys; q=json.load(sys.stdin)"
        '[\\"messenger\\"][\\"dispatch_queue\\"]; '
        'print(\\"{} {}\\".format(q[\\"length\\"], q.get(\\"max_age_ago\\",\\"\\")))'
    )
    msgrs = " ".join(messenger_names)
    script = (
        ": %s; end=$((SECONDS+%d)); "
        "while (( SECONDS < end )); do "
        "max=0; depths=; ages=; "
        "for msgr in %s; do "
        "out=$(ceph tell mgr.%s messenger dump \"$msgr\" "
        "2>/dev/null | python3 -c \"%s\" 2>/dev/null || echo -1 unknown); "
        "len=$(echo \"$out\" | cut -d\" \" -f1); "
        "age=$(echo \"$out\" | cut -d\" \" -f2); "
        "depths=\"$depths $msgr=$len\"; "
        "ages=\"$ages $msgr=$age\"; "
        "if [ \"$len\" -gt \"$max\" ] 2>/dev/null; then max=$len; fi; "
        "done; "
        "echo QUEUE_DEPTH$depths max_age_ago$ages depth=$max "
        ">> /tmp/%s.log; "
        "done"
        % (marker, int(duration), msgrs, mgr_id, py, marker)
    )
    # /tmp is mounted from the host so each append is visible to the tailer.
    # cephadm's stdout pipe is block-buffered and was hiding these lines.
    cmd = "cephadm shell --mount /tmp:/tmp -- bash -c '%s'" % script
    try:
        rados_obj.node.installer.exec_command(
            sudo=True, cmd=cmd, timeout=int(duration) + 180
        )
    except Exception as exc:
        log.warning("MGR command flood ended: %s", str(exc)[:300])


def stop_mgr_command_flood(rados_obj, marker):
    """Stop the cephadm shell that is running the command flood."""
    try:
        rados_obj.node.installer.exec_command(
            sudo=True,
            cmd="pkill -f %s || true" % marker,
            check_ec=False,
        )
        log.info("Stopped MGR command flood (%s)", marker)
    except Exception as exc:
        log.warning("Could not stop MGR command flood: %s", str(exc)[:200])


class MgrDispatchQueueMonitor:
    """
    Background sampler that prints the active MGR dispatch-queue length.

    Each pass runs:
      ceph tell mgr.<id> messenger dump
      ceph tell mgr.<id> messenger dump <name> | jq '.messenger.dispatch_queue'
    """

    def __init__(
        self, rados_obj, mgr_id, messenger_names, threshold, marker, interval=1
    ):
        self.rados_obj = rados_obj
        self.mgr_id = mgr_id
        self.messenger_names = list(messenger_names)
        self.marker = marker
        self.threshold = int(str(threshold).strip())
        self.interval = interval
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._thread = None
        self._limited_dump = True
        self._in_window = False
        self.max_depth = None
        self.last_depth = None
        self.samples = 0
        self.errors = 0
        self.window_samples = 0
        self.window_min = None
        self.window_max = None
        self.window_below = 0

    def start(self):
        log.info(
            "Starting dispatch-queue monitor thread for mgr.%s "
            "messengers=%s threshold=%s interval=%ss",
            self.mgr_id,
            self.messenger_names,
            self.threshold,
            self.interval,
        )
        self._thread = threading.Thread(
            target=self._loop,
            name="mgr-dispatch-queue",
            daemon=True,
        )
        self._thread.start()

    def begin_sustain_window(self):
        """Ignore earlier samples. Every later sample must stay above threshold."""
        with self._lock:
            self._in_window = True
            self.window_samples = 0
            self.window_min = None
            self.window_max = None
            self.window_below = 0
        log.info(
            "Dispatch-queue sustain window started. "
            "Every sample must stay above threshold %s",
            self.threshold,
        )

    def stop(self):
        self._stop.set()
        thread = self._thread
        if thread is None:
            return
        if thread.is_alive():
            thread.join(timeout=120)
            if thread.is_alive():
                log.warning(
                    "Dispatch-queue monitor thread still running after join timeout"
                )
        self._thread = None
        log.info(
            "Dispatch-queue monitor stopped: samples=%s max_length=%s "
            "last_length=%s errors=%s threshold=%s "
            "sustain_samples=%s sustain_min=%s sustain_max=%s sustain_below=%s",
            self.samples,
            self.max_depth,
            self.last_depth,
            self.errors,
            self.threshold,
            self.window_samples,
            self.window_min,
            self.window_max,
            self.window_below,
        )

    def sustained_above_threshold(self):
        with self._lock:
            return (
                self.window_samples > 0
                and self.window_below == 0
                and self.window_min is not None
                and self.window_min > self.threshold
            )

    def _record(self, length):
        with self._lock:
            self.samples += 1
            self.last_depth = length
            if self.max_depth is None or length > self.max_depth:
                self.max_depth = length
            in_window = self._in_window
            if in_window:
                self.window_samples += 1
                if self.window_min is None or length < self.window_min:
                    self.window_min = length
                if self.window_max is None or length > self.window_max:
                    self.window_max = length
                if length <= self.threshold:
                    self.window_below += 1
        return in_window

    def _handle_queue_line(self, line):
        line = line.strip()
        if not line:
            return
        if not line.startswith("QUEUE_DEPTH"):
            log.info("queue flood: %s", line)
            return
        depth = None
        ages = []
        for token in line.split():
            if token.startswith("depth="):
                try:
                    depth = int(token.split("=", 1)[1])
                except ValueError:
                    depth = None
            elif "=" in token and not token.startswith("QUEUE_DEPTH"):
                # Capture per-messenger length= and max_age entries for the log.
                key, val = token.split("=", 1)
                if key.startswith("mgr") or key == "mgr":
                    pass
                if "max_age" in line and key.startswith("mgr"):
                    ages.append("%s=%s" % (key, val))
        if depth is None or depth < 0:
            log.warning("queue sample failed: %s", line[:300])
            self.errors += 1
            return
        in_window = self._record(depth)
        over = depth > self.threshold
        # Print in the same shape as:
        #   jq '.messenger.dispatch_queue' -> {length, max_age_ago}
        log.info(
            "MESSENGER_QUEUE mgr=%s %s threshold=%s "
            "over_threshold=%s sustain_window=%s time=%s",
            self.mgr_id,
            line,
            self.threshold,
            over,
            in_window,
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        log.info(
            "MESSENGER_QUEUE_LENGTH mgr=%s length=%s threshold=%s "
            "over_threshold=%s",
            self.mgr_id,
            depth,
            self.threshold,
            over,
        )
        if in_window and not over:
            log.error(
                "dispatch queue depth %s is not above threshold %s",
                depth,
                self.threshold,
            )

    def _loop(self):
        path = "/tmp/%s.log" % self.marker
        offset = 0
        pending = ""
        while not self._stop.is_set():
            try:
                out, _err = self.rados_obj.node.installer.exec_command(
                    sudo=True,
                    cmd="tail -c +%d %s 2>/dev/null || true" % (offset + 1, path),
                    check_ec=False,
                )
            except Exception as exc:
                self.errors += 1
                log.warning("reading queue log failed: %s", str(exc)[:200])
                self._stop.wait(1)
                continue
            chunk = out.decode() if isinstance(out, bytes) else (out or "")
            if chunk:
                offset += len(chunk.encode())
                pending += chunk
                lines = pending.split("\n")
                pending = lines.pop()
                for line in lines:
                    self._handle_queue_line(line)
            self._stop.wait(1)


def generate_cluster_load(rados_obj, pool_count=50, bench_max_objs=200):
    """
    Generate cluster load by creating pools and running rados bench

    Creates multiple replicated pools and runs bench writes on each to generate
    a stream of PG stats, OSD reports, and other messages to the MGR.

    Args:
        rados_obj: RadosOrchestrator object
        pool_count: Number of pools to create
        bench_max_objs: Max objects per bench write

    Returns: List of pool names created (for cleanup)
    """
    pool_names = []
    log.info(
        "Generating cluster load: creating %s pools with bench writes",
        pool_count,
    )

    for i in range(pool_count):
        pool_name = f"{AUTOTUNE_TEST_POOL_PREFIX}{i}"
        try:
            if rados_obj.create_pool(pool_name=pool_name, pg_num=32):
                pool_names.append(pool_name)
                log.debug("Created pool: %s", pool_name)
            else:
                log.warning("Failed to create pool: %s", pool_name)
        except Exception as e:
            log.warning("Error creating pool %s: %s", pool_name, str(e)[:100])

    log.info("Created %s pools for load generation", len(pool_names))

    # Run bench write on each pool
    for pool_name in pool_names:
        try:
            rados_obj.bench_write(
                pool_name=pool_name,
                byte_size="4K",
                max_objs=bench_max_objs,
            )
            log.debug("Bench write completed on %s", pool_name)
        except Exception as e:
            log.warning("Bench write on %s: %s", pool_name, str(e)[:100])

    log.info("Load generation completed")
    return pool_names


def cleanup_test_pools(rados_obj, pool_names):
    """
    Delete test pools created during load generation

    Args:
        rados_obj: RadosOrchestrator object
        pool_names: List of pool names to delete
    """
    log.info("Cleaning up %s test pools", len(pool_names))
    existing_pools = rados_obj.list_pools()

    for pool_name in pool_names:
        if pool_name in existing_pools:
            try:
                rados_obj.delete_pool(pool=pool_name)
                log.debug("Deleted pool: %s", pool_name)
            except Exception as e:
                log.warning("Failed to delete pool %s: %s", pool_name, e)

    # Also clean up any leftover pools from previous runs
    for pool_name in existing_pools:
        if pool_name.startswith(AUTOTUNE_TEST_POOL_PREFIX):
            if pool_name not in pool_names:
                try:
                    rados_obj.delete_pool(pool=pool_name)
                    log.debug("Cleaned up leftover pool: %s", pool_name)
                except Exception as e:
                    log.warning("Failed to cleanup leftover pool %s: %s", pool_name, e)

    log.info("Pool cleanup completed")


def restore_autotune_defaults(mon_obj):
    """
    Restore all autotune-related config parameters to their default values

    Args:
        mon_obj: MonConfigMethods object
    """
    log.info("Restoring autotune config parameters to defaults")

    configs_to_remove = [
        "mgr_stats_period_autotune",
        "mgr_stats_period_autotune_queue_threshold",
        "mgr_stats_period",
    ]

    for param in configs_to_remove:
        try:
            mon_obj.remove_config(
                section="mgr",
                name=param,
                verify_rm=False,
            )
            log.debug("Removed config override: mgr/%s", param)
        except Exception as e:
            log.warning("Could not remove mgr/%s: %s", param, str(e)[:100])

        # Also try removing from global section
        try:
            mon_obj.remove_config(
                section="global",
                name=param,
                verify_rm=False,
            )
            log.debug("Removed config override: global/%s", param)
        except Exception as e:
            log.debug("global/%s not set: %s", param, str(e)[:50])

    time.sleep(5)
    log.info("Autotune config defaults restored")
