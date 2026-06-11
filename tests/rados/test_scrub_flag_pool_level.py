"""
Polarion ID: CEPH-83621444 - Validation of pool-level noscrub and nodeep-scrub settings

This module verifies pool-level noscrub and nodeep-scrub flag behavior and confirms
that user-initiated scrub operations take precedence over these flags.

The following test scenarios are verified:

Case 1: Scheduled scrub with pool-level scrub flags
- Creates two pools (active_pool and inactive_pool) and writes test data
- Sets noscrub and nodeep-scrub flags on inactive_pool only
- Verifies POOL_SCRUB_FLAGS health warning is reported
- Configures scheduled scrub parameters (scrub window, intervals, and chunk limits)
- Verifies scheduled scrub and deep-scrub complete on active_pool
- Verifies scheduled scrub and deep-scrub do not run on inactive_pool
- Restores scheduled scrub parameters to default values

Case 2: User-initiated scrub priority over pool-level flags
- Creates two pools (active_pool and inactive_pool) and writes test data
- Sets noscrub and nodeep-scrub flags on both pools
- Initiates user-requested scrub and deep-scrub on both pools
- Verifies scrub and deep-scrub complete on active_pool despite the flags
- Verifies scrub and deep-scrub complete on inactive_pool despite the flags

Initial Setup:
- Creates two replicated pools configured via config['pool_names']
- Writes 500 benchmark objects into each pool

Expected Behavior:
- Pool-level noscrub and nodeep-scrub flags block scheduled scrub operations
- POOL_SCRUB_FLAGS warning appears when scrub flags are set on any pool
- User-initiated scrub and deep-scrub operations proceed regardless of pool-level flags

"""

import time
import traceback
from datetime import datetime, timedelta

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.rados_scrub import RadosScrubber
from ceph.rados.utils import get_cluster_timestamp
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test execution summary for pool-level noscrub and nodeep-scrub flag verification.

    This test validates that pool-level scrub flags control scheduled scrub behavior
    while user-initiated scrubs take precedence over those flags.

    Test Execution Flow:
    1. Initial Setup:
       - Creates active_pool and inactive_pool
       - Writes benchmark data (500 objects) into each pool

    2. Test Cases (executed based on config['case_to_run']):

       Case 1: Scheduled scrub with pool-level scrub flags
       - Sets noscrub and nodeep-scrub on inactive_pool
       - Verifies POOL_SCRUB_FLAGS health warning
       - Configures scheduled scrub parameters
       - Confirms scrub runs on active_pool and not on inactive_pool
       - Restores scheduled scrub parameters

       Case 2: User-initiated scrub priority over pool-level flags
       - Sets noscrub and nodeep-scrub on both pools
       - Runs user-initiated scrub and deep-scrub on both pools
       - Confirms scrub and deep-scrub complete on both pools

    3. Cleanup:
       - Restores OSD scrub configuration to defaults
       - Deletes test pools
       - Verifies cluster health and checks for daemon crashes

    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_object = RadosOrchestrator(node=cephadm)
    scrub_object = RadosScrubber(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_object)
    active_test_pool = ""
    inactive_test_pool = ""
    start_time = get_cluster_timestamp(rados_object.node)
    log.debug("Test workflow started. Start time: %s", start_time)
    case_to_run = config.get("case_to_run")
    if not case_to_run:
        log.error("case_to_run is not specified or is empty in the config")
        return 1
    log.info("Executing test case(s): %s", case_to_run)

    try:
        pool_names = config.get("pool_names")
        if not pool_names:
            log.error("config['pool_names'] is required but not provided")
            return 1
        if len(pool_names) != 2:
            log.error("config['pool_names'] must contain exactly two pool names")
            return 1
        else:
            active_test_pool, inactive_test_pool = setup_test_pools(
                rados_object, pool_names
            )
            log.info(
                "Test pools created: active=%s, inactive=%s",
                active_test_pool,
                inactive_test_pool,
            )

        if "case1" in case_to_run:
            if case1_scheduled_scrub_with_pool_flags(
                rados_object,
                scrub_object,
                active_test_pool,
                inactive_test_pool,
            ):
                return 1

        if "case2" in case_to_run:
            if case2_user_initiated_scrub_priority(
                rados_object, active_test_pool, inactive_test_pool
            ):
                return 1

    except (KeyError, ValueError, AssertionError) as e:
        log.error("Test failed with error: %s", str(e))
        log.error("Traceback:\n%s", traceback.format_exc())
        return 1
    except Exception as e:
        log.error("Unexpected error occurred: %s", str(e))
        log.error("Traceback:\n%s", traceback.format_exc())
        return 1
    finally:
        log.info(
            "=====Execution of finally block - restoring cluster state and cleaning up============"
        )
        if mon_obj is not None:
            unset_scheduled_scrub_parameters(mon_obj)
        if active_test_pool:
            log.info("Deleting active test pool %s", active_test_pool)
            method_should_succeed(rados_object.delete_pool, active_test_pool)
        if inactive_test_pool:
            log.info("Deleting inactive test pool %s", inactive_test_pool)
            method_should_succeed(rados_object.delete_pool, inactive_test_pool)
        time.sleep(5)
        rados_object.log_cluster_health()
        test_end_time = get_cluster_timestamp(rados_object.node)
        log.debug(
            "Test workflow completed. Start time: %s, End time: %s",
            start_time,
            test_end_time,
        )
        if rados_object.check_crash_status(
            start_time=start_time, end_time=test_end_time
        ):
            log.error("Test failed due to daemon crash during test execution")
            return 1

        log.info("Validation completed successfully")
    return 0


def setup_test_pools(rados_object, pool_names):
    """
    Create test pools and populate them with benchmark data.

    Args:
        rados_object: RadosOrchestrator instance
        pool_names: List of two pool names; index 0 is active, index 1 is inactive

    Returns:
        Tuple of (active_test_pool, inactive_test_pool)
    """
    for pool_name in pool_names:
        if not rados_object.create_pool(pool_name=pool_name):
            raise RuntimeError(f"Failed to create pool '{pool_name}'")
        log.info("Created pool %s", pool_name)
        if not rados_object.bench_write(pool_name=pool_name, max_objs=500):
            raise RuntimeError(f"Failed to write benchmark data to pool '{pool_name}'")
        log.info("Wrote benchmark data to pool %s", pool_name)

    active_test_pool = pool_names[0]
    inactive_test_pool = pool_names[1]
    log.info(
        "Active pool: %s, Inactive pool: %s",
        active_test_pool,
        inactive_test_pool,
    )
    if not wait_for_clean_pg_sets(rados_object, timeout=300, sleep_interval=10):
        log.warning("PGs did not reach clean state within timeout, continuing anyway")
    return active_test_pool, inactive_test_pool


def set_pool_scrub_flags(rados_object, pool_name):
    """
    Enable noscrub and nodeep-scrub pool properties to block scheduled scrubs.

    Args:
        rados_object: RadosOrchestrator instance
        pool_name: Name of the pool on which to set scrub flags
    """
    log.info(
        "Setting noscrub and nodeep-scrub flags on pool %s",
        pool_name,
    )
    rados_object.set_pool_property(pool=pool_name, props="noscrub", value="true")
    rados_object.set_pool_property(pool=pool_name, props="nodeep-scrub", value="true")


def case1_scheduled_scrub_with_pool_flags(
    rados_object, scrub_object, active_test_pool, inactive_test_pool
):
    """
    Case 1: Verify scheduled scrub runs on active pool but not on pool with scrub flags set.

    Scenario:
    1. Capture scrub timestamps on both pools before testing
    2. Set noscrub and nodeep-scrub flags on inactive_pool only
    3. Verify POOL_SCRUB_FLAGS health warning is present
    4. Configure scheduled scrub parameters to trigger scrub within the test window
    5. Wait for scrub completion on active_pool (expected to succeed)
    6. Confirm scrub does not start on inactive_pool within the wait period
    7. Restore scheduled scrub parameters to default values

    Args:
        rados_object: RadosOrchestrator instance
        scrub_object: RadosScrubber instance
        active_test_pool: Pool without scrub flags; scheduled scrub should run
        inactive_test_pool: Pool with scrub flags; scheduled scrub should be blocked

    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(case1_scheduled_scrub_with_pool_flags.__doc__)
    log.info(
        "Case 1: START - Verifying scheduled scrub behavior when scrub flags are set on one pool"
    )
    active_pool_before_scrub_stamps = rados_object.get_scrub_stamps(active_test_pool)
    log.debug(
        "Case 1: Scrub timestamps before testing on active pool %s: %s",
        active_test_pool,
        active_pool_before_scrub_stamps,
    )

    inactive_pool_before_scrub_stamps = rados_object.get_scrub_stamps(
        inactive_test_pool
    )
    log.debug(
        "Case 1: Scrub timestamps before testing on inactive pool %s: %s",
        inactive_test_pool,
        inactive_pool_before_scrub_stamps,
    )

    set_pool_scrub_flags(rados_object, inactive_test_pool)

    log.info("Case 1: Verifying POOL_SCRUB_FLAGS health warning")
    if not rados_object.check_health_warning(warning="POOL_SCRUB_FLAGS"):
        log.error("Case 1: POOL_SCRUB_FLAGS warning did not appear in cluster health")
        return 1
    log.info(
        "Case 1: POOL_SCRUB_FLAGS warning reported - pool has noscrub or nodeep-scrub set"
    )

    log.info("Case 1: Configuring scheduled scrub parameters")

    set_scheduled_scrub_parameters(scrub_object)
    log.info(
        "Case 1: Expected result - scheduled scrub should run on pool %s",
        active_test_pool,
    )
    if not wait_for_scrub_completion(
        rados_object,
        active_test_pool,
        active_pool_before_scrub_stamps,
        wait_time_in_seconds=1200,
    ):
        log.error(
            "Case 1: Scheduled scrub did not complete on active pool %s",
            active_test_pool,
        )
        return 1
    log.info(
        "Case 1: Scheduled scrub completed on active pool %s",
        active_test_pool,
    )

    log.info(
        "Case 1: Expected result - scheduled scrub should not run on pool %s",
        inactive_test_pool,
    )
    if wait_for_scrub_completion(
        rados_object,
        inactive_test_pool,
        inactive_pool_before_scrub_stamps,
        wait_time_in_seconds=300,
    ):
        log.error(
            "Case 1: Scheduled scrub started on pool %s despite noscrub and nodeep-scrub flags",
            inactive_test_pool,
        )
        return 1
    log.info(
        "Case 1: Scheduled scrub did not run on inactive pool %s as expected",
        inactive_test_pool,
    )

    log.info("Case 1: Restoring scheduled scrub parameters to default values")

    log.info(
        "Case 1: END - Verified scheduled scrub behavior with pool-level scrub flags"
    )
    return 0


def case2_user_initiated_scrub_priority(
    rados_object, active_test_pool, inactive_test_pool
):
    """
    Case 2: Verify user-initiated scrub and deep-scrub take precedence over pool-level flags.

    Scenario:
    1. Set noscrub and nodeep-scrub flags on both active_pool and inactive_pool
    2. Capture scrub timestamps on both pools before testing
    3. Initiate user-requested scrub on both pools
    4. Initiate user-requested deep-scrub on both pools
    5. Wait for scrub completion on active_pool (expected to succeed)
    6. Wait for scrub completion on inactive_pool (expected to succeed)

    Args:
        rados_object: RadosOrchestrator instance
        active_test_pool: Pool with scrub flags; user-initiated scrub should still run
        inactive_test_pool: Pool with scrub flags; user-initiated scrub should still run

    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(case2_user_initiated_scrub_priority.__doc__)
    wait_time_in_seconds = 180
    log.info(
        "Case 2: START - Verifying user-initiated scrub priority over pool-level scrub flags"
    )

    set_pool_scrub_flags(rados_object, active_test_pool)
    set_pool_scrub_flags(rados_object, inactive_test_pool)

    wait_for_clean_pg_sets(rados_object, timeout=300, sleep_interval=10)

    active_pool_before_scrub_stamps = rados_object.get_scrub_stamps(active_test_pool)

    inactive_pool_before_scrub_stamps = rados_object.get_scrub_stamps(
        inactive_test_pool
    )

    log.debug(
        "Case 2: Scrub timestamps before testing on active pool %s: %s",
        active_test_pool,
        active_pool_before_scrub_stamps,
    )

    log.debug(
        "Case 2: Scrub timestamps before testing on inactive pool %s: %s",
        inactive_test_pool,
        inactive_pool_before_scrub_stamps,
    )

    log.info(
        "Case 2: Initiating user-requested scrub and deep-scrub on pools %s and %s",
        active_test_pool,
        inactive_test_pool,
    )
    for pool_name, before_scrub_stamps in [
        (active_test_pool, active_pool_before_scrub_stamps),
        (inactive_test_pool, inactive_pool_before_scrub_stamps),
    ]:
        log.info(
            "Case 2: Expected result - user-initiated scrub should complete on pool %s",
            pool_name,
        )
        scrub_completed = False
        for attempt in range(5):
            log.info(
                "Attempt-%s-Performing the scrub operation on %s pool",
                attempt,
                pool_name,
            )
            rados_object.run_scrub(pool=pool_name)
            log.info("Performing the deep-scrub operation on %s pool", pool_name)
            rados_object.run_deep_scrub(pool=pool_name)
            if wait_for_scrub_completion(
                rados_object,
                pool_name,
                before_scrub_stamps,
                wait_time_in_seconds=wait_time_in_seconds,
            ):
                scrub_completed = True
                break
            if attempt == 0:
                log.warning(
                    "Case 2: User-initiated scrub did not complete on pool %s. "
                    "Retrying scrub and deep-scrub.",
                    pool_name,
                )
        if not scrub_completed:
            log.error(
                "Case 2: User-initiated scrub did not complete on pool %s "
                "after five attempts.",
                pool_name,
            )
            return 1
        log.info(
            "Case 2: User-initiated scrub completed on pool %s",
            pool_name,
        )
    log.info(
        "Case 2: END - Verified user-initiated scrub priority over pool-level scrub flags"
    )
    return 0


def set_scheduled_scrub_parameters(scrub_object):
    """
    Configure OSD scrub parameters to trigger scheduled scrub within the test window.

    Sets scrub begin/end hours, min/max/deep-scrub intervals, max concurrent scrubs,
    and chunk limits to accelerate scheduled scrub activity during Case 1.

    Args:
        scrub_object: RadosScrubber instance
    """
    osd_scrub_min_interval = 30
    osd_scrub_max_interval = 150
    osd_deep_scrub_interval = 150
    (
        scrub_begin_hour,
        scrub_begin_weekday,
        scrub_end_hour,
        scrub_end_weekday,
    ) = scrub_object.add_begin_end_hours(0, 2)

    log.info("Configuring scheduled scrub parameters for Case 1")
    scrub_object.set_osd_configuration("osd_scrub_begin_hour", scrub_begin_hour)
    scrub_object.set_osd_configuration("osd_scrub_begin_week_day", scrub_begin_weekday)
    scrub_object.set_osd_configuration("osd_scrub_end_hour", scrub_end_hour)
    scrub_object.set_osd_configuration("osd_scrub_end_week_day", scrub_end_weekday)
    scrub_object.set_osd_configuration("osd_scrub_min_interval", osd_scrub_min_interval)
    scrub_object.set_osd_configuration("osd_scrub_max_interval", osd_scrub_max_interval)
    scrub_object.set_osd_configuration(
        "osd_deep_scrub_interval", osd_deep_scrub_interval
    )
    scrub_object.set_osd_configuration("osd_max_scrubs", 8)
    scrub_object.set_osd_configuration("osd_scrub_chunk_max", 50)
    scrub_object.set_osd_configuration("osd_shallow_scrub_chunk_max", 100)
    scrub_object.set_osd_configuration("osd_scrub_priority", 10)


def unset_scheduled_scrub_parameters(mon_obj):
    """
    Remove OSD scrub configuration overrides and restore cluster defaults.

    Args:
        mon_obj: MonConfigMethods instance
    """
    mon_obj.remove_config(section="osd", name="osd_scrub_begin_hour")
    mon_obj.remove_config(section="osd", name="osd_scrub_begin_week_day")
    mon_obj.remove_config(section="osd", name="osd_scrub_end_hour")
    mon_obj.remove_config(section="osd", name="osd_scrub_end_week_day")
    mon_obj.remove_config(section="osd", name="osd_scrub_min_interval")
    mon_obj.remove_config(section="osd", name="osd_scrub_max_interval")
    mon_obj.remove_config(section="osd", name="osd_deep_scrub_interval")
    mon_obj.remove_config(section="osd", name="osd_max_scrubs")
    mon_obj.remove_config(section="osd", name="osd_scrub_chunk_max")
    mon_obj.remove_config(section="osd", name="osd_shallow_scrub_chunk_max")
    mon_obj.remove_config(section="osd", name="osd_scrub_priority")


def log_scrub_stamps_table(pool_name, label, scrub_stamps):
    """
    Log PG scrub timestamps in a readable table format.

    Args:
        pool_name: Name of the pool
        label: Table label (e.g. before_scrub_stamps, after_scrub_stamps)
        scrub_stamps: Dictionary with PG_ID as key and scrub stamp values
    """
    if not scrub_stamps:
        log.info("%s on pool %s: no scrub stamp data available", label, pool_name)
        return

    pg_id_col = "PG_id"
    scrub_col = "last_scrub_stamp"
    deep_scrub_col = "last_deep_scrub_stamp"

    max_pg_id_len = max(len(pg_id_col), max(len(str(pgid)) for pgid in scrub_stamps))
    max_scrub_len = max(
        len(scrub_col),
        max(len(vals["last_scrub_stamp"]) for vals in scrub_stamps.values()),
    )
    max_deep_scrub_len = max(
        len(deep_scrub_col),
        max(len(vals["last_deep_scrub_stamp"]) for vals in scrub_stamps.values()),
    )

    separator = "=" * (max_pg_id_len + max_scrub_len + max_deep_scrub_len + 6)
    log.info("Scrub timestamps on pool %s - %s", pool_name, label)
    log.info(separator)
    log.info(
        "%-*s  %-*s  %s",
        max_pg_id_len,
        pg_id_col,
        max_scrub_len,
        scrub_col,
        deep_scrub_col,
    )
    log.info(separator)

    for pgid, vals in sorted(scrub_stamps.items()):
        log.info(
            "%-*s  %-*s  %s",
            max_pg_id_len,
            str(pgid),
            max_scrub_len,
            vals["last_scrub_stamp"],
            vals["last_deep_scrub_stamp"],
        )

    log.info(separator)


def compare_pg_scrub(before_dict, after_dict):
    """
    Compare PG scrub timestamps before and after a scrub operation.

    Args:
        before_dict: PG scrub stamps collected before the scrub started
        after_dict: PG scrub stamps collected after the scrub started

    Returns:
        True if both last_scrub_stamp and last_deep_scrub_stamp changed for all PGs
        False if any PG scrub stamp is unchanged (scrub still in progress or not started)
    """
    for pgid, before_vals in before_dict.items():
        if pgid not in after_dict:
            log.info(
                "PG ID %s not present in scrub stamp data collected after testing",
                pgid,
            )
            continue

        after_vals = after_dict[pgid]
        before_scrub = datetime.strptime(
            before_vals["last_scrub_stamp"], "%Y-%m-%dT%H:%M:%S.%f+0000"
        )
        after_scrub = datetime.strptime(
            after_vals["last_scrub_stamp"], "%Y-%m-%dT%H:%M:%S.%f+0000"
        )
        log.debug(
            "PG %s - before scrub stamp: %s, after scrub stamp: %s",
            pgid,
            before_scrub,
            after_scrub,
        )
        if after_scrub == before_scrub:
            log.info("Scrub still in progress on PG %s", pgid)
            return False

        before_deep_scrub = datetime.strptime(
            before_vals["last_deep_scrub_stamp"], "%Y-%m-%dT%H:%M:%S.%f+0000"
        )
        after_deep_scrub = datetime.strptime(
            after_vals["last_deep_scrub_stamp"], "%Y-%m-%dT%H:%M:%S.%f+0000"
        )
        log.debug(
            "PG %s - before deep-scrub stamp: %s, after deep-scrub stamp: %s",
            pgid,
            before_deep_scrub,
            after_deep_scrub,
        )
        if before_deep_scrub == after_deep_scrub:
            log.info("Deep-scrub still in progress on PG %s", pgid)
            return False
    return True


def wait_for_scrub_completion(
    rados_object, pool_name, before_scrub_stamps, wait_time_in_seconds
):
    """
    Poll PG scrub timestamps until scrub completes or the timeout is reached.

    Args:
        rados_object: RadosOrchestrator instance
        pool_name: Name of the pool to monitor
        before_scrub_stamps: Scrub timestamps captured before scrub was triggered
        wait_time_in_seconds: Maximum time to wait for scrub completion

    Returns:
        True if scrub completed within the timeout, False otherwise
    """
    end_time = datetime.now() + timedelta(seconds=wait_time_in_seconds)
    while datetime.now() <= end_time:
        after_scrub_stamps = rados_object.get_scrub_stamps(pool_name)
        log_scrub_stamps_table(pool_name, "before_scrub_stamps", before_scrub_stamps)
        log_scrub_stamps_table(pool_name, "after_scrub_stamps", after_scrub_stamps)

        scrub_result = compare_pg_scrub(before_scrub_stamps, after_scrub_stamps)
        if scrub_result:
            log.info("Scrub completed successfully on pool %s", pool_name)
            return True

        log.info("Scrub still in progress on pool %s", pool_name)
        log.info(
            "Waiting 15 seconds before retrying scrub status check on pool %s",
            pool_name,
        )
        time.sleep(15)

    log.warning(
        "Scrub did not complete on pool %s within %s seconds",
        pool_name,
        wait_time_in_seconds,
    )
    return False
