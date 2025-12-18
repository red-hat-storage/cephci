"""
This module verifies the noscrub and nodeep-scrub settings at the pool level and ensures that user-initiated
scrub operations take precedence over these flags.
"""

import time
import traceback
from datetime import datetime, timedelta

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.rados_scrub import RadosScrubber
from ceph.rados.utils import get_cluster_timestamp
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    1. Create two pools, namely active_pool and inactive_pool.
    2. Insert test data into both pools.
    3. Apply the noscrub and nodeep-scrub flags to the inactive_pool.
    4. Verify the cluster health details to confirm that the noscrub and nodeep-scrub flags are correctly reflected.
    5. Configure scheduled scrub and deep-scrub operations for the cluster.
    6. Validate that scheduled scrubbing occurs only on active_pool and does not run on the inactive_pool.
    7. Apply noscrub and nodeep-scrub flags to all pools in the cluster.
    8. Initiate a user-triggered scrub and verify that the scrub operation proceeds successfully despite the flags,
        confirming that user-initiated scrubs take precedence.
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
    log.debug(f"Test workflow started. Start time: {start_time}")
    try:

        for pool_name in config["pool_names"]:
            assert rados_object.create_pool(pool_name=pool_name)
            log.info("The %s pool name is created", pool_name)
            assert rados_object.bench_write(pool_name=pool_name, max_objs=500)
            log.info("The data is pushed in the %s pool", pool_name)

        active_test_pool = config["pool_names"][0]
        log.info("The flags are setting on %s pool", active_test_pool)
        inactive_test_pool = config["pool_names"][1]
        log.info(
            "START:Case1-Verifying the scrub status after configuring the scheduled parameters and enabling "
            "scrub flags on the selected pool only."
        )
        active_pool_before_scrub_stamps = rados_object.get_scrub_stamps(
            active_test_pool
        )
        log.debug(
            "Case1-The time stamps before testing on the active pool - %s is %s",
            active_test_pool,
            active_pool_before_scrub_stamps,
        )

        inactive_pool_before_scrub_stamps = rados_object.get_scrub_stamps(
            inactive_test_pool
        )
        log.debug(
            "Case1-The time stamps before testing on the inactive pool - %s is %s",
            inactive_test_pool,
            inactive_pool_before_scrub_stamps,
        )
        log.info(
            "Case1- Setting the noscrub and nodeep-scrub flags on the %s pool",
            inactive_test_pool,
        )
        # Set the noscrub and nodeep-scrub flags to the pool name
        rados_object.set_pool_property(
            pool=inactive_test_pool, props="noscrub", value="true"
        )
        rados_object.set_pool_property(
            pool=inactive_test_pool, props="nodeep-scrub", value="true"
        )
        log.info("Case1-Checking the health warning ")
        if not rados_object.check_health_warning(warning="POOL_SCRUB_FLAGS"):
            log.error(
                "The POOL_SCRUB_FLAGS warning not appeared in the cluster warnings"
            )
            return 1
        log.info(
            "POOL_SCRUB_FLAGS: Some pool(s) have the noscrub, nodeep-scrub flag(s) set message appeared"
        )

        log.info("Case1-Setting the scheduled scrub parameters")
        # Get the scrub and dep-scrub timings
        set_scheduled_scrub_parameters(scrub_object)
        log.info(
            "Expected Result: Case1-Scrub operations are expected to be executed on the %s pool",
            active_test_pool,
        )
        if not wait_for_scrub_completion(
            rados_object,
            active_test_pool,
            active_pool_before_scrub_stamps,
            wait_time_in_seconds=1200,
        ):
            log.error("Case 1:The scrub operations are not completed on active pool")
            return 1
        log.info(
            "Expected Result: Case1-Scrub operations are executed on the %s pool",
            active_test_pool,
        )

        log.info(
            "Expected Result:  Case1--Scrub operations are not expected to be executed on the %s pool",
            inactive_test_pool,
        )
        if wait_for_scrub_completion(
            rados_object,
            inactive_test_pool,
            inactive_pool_before_scrub_stamps,
            wait_time_in_seconds=300,
        ):
            log.error(
                "Case1:The scrub operations are started on the %s pool which the noscrub and nodeep-scrub "
                "flags are set",
                inactive_test_pool,
            )
            return 1
        log.info(
            "Expected Result:  Case1--Scrub operations are not executed on the %s pool",
            inactive_test_pool,
        )

        log.info("Case1:UnSetting the scheduled scrub parameters")
        unset_scheduled_scrub_parameters(mon_obj)
        log.info(
            "END:Case1-Verified the scrub status after configuring the scheduled parameters and enabling "
            "scrub flags on the selected pool only."
        )

        log.info(
            "START-Case2-Verifying that user-initiated scrub and deep-scrub operations have higher priority "
            "than the 'noscrub' and 'nodeep-scrub' flags at pool level."
        )
        log.info(
            "Case2- Setting the noscrub and nodeep-scrub flags on the %s pool",
            active_test_pool,
        )
        # Set the noscrub and nodeep-scrub flags to the pool name
        rados_object.set_pool_property(
            pool=active_test_pool, props="noscrub", value="true"
        )
        rados_object.set_pool_property(
            pool=active_test_pool, props="nodeep-scrub", value="true"
        )

        active_pool_before_scrub_stamps = rados_object.get_scrub_stamps(
            active_test_pool
        )
        log.debug(
            "Case2-The time stamps before testing on the active pool - %s is %s",
            active_test_pool,
            active_pool_before_scrub_stamps,
        )

        inactive_pool_before_scrub_stamps = rados_object.get_scrub_stamps(
            inactive_test_pool
        )
        log.debug(
            "Case2-The time stamps before testing on the inactive pool - %s is %s",
            inactive_test_pool,
            inactive_pool_before_scrub_stamps,
        )

        log.info(
            "Case2:Performing the user initiated scrub on the %s and %s pools",
            active_test_pool,
            inactive_test_pool,
        )
        # Perform the user initiated scrub
        rados_object.run_scrub(pool=active_test_pool)
        rados_object.run_scrub(pool=inactive_test_pool)
        rados_object.run_deep_scrub(pool=active_test_pool)
        rados_object.run_deep_scrub(pool=inactive_test_pool)

        log.info(
            "Expected Result: Case2-Scrub operations are expected to be executed on the %s pool",
            active_test_pool,
        )
        if not wait_for_scrub_completion(
            rados_object,
            active_test_pool,
            active_pool_before_scrub_stamps,
            wait_time_in_seconds=1200,
        ):
            log.error(
                "Case2:The scrub operations are not completed on the %s pool",
                active_test_pool,
            )
            return 1
        log.info(
            "Expected Result: Case2-Scrub operations are executed on the %s pool",
            active_test_pool,
        )

        log.info(
            "Expected Result: Case2-Scrub operations are expected to be executed on the %s pool",
            inactive_test_pool,
        )
        if not wait_for_scrub_completion(
            rados_object,
            inactive_test_pool,
            inactive_pool_before_scrub_stamps,
            wait_time_in_seconds=1200,
        ):
            log.error(
                "Case2:The scrub operations are not completed on %s pool",
                inactive_test_pool,
            )
            return 1

        log.info(
            "Expected Result: Case2-Scrub operations are executed on the %s pool",
            inactive_test_pool,
        )
        log.info(
            "END:Case2-Verified that user-initiated scrub and deep-scrub operations have higher priority "
            "than the 'noscrub' and 'nodeep-scrub' flags at pool level."
        )

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )

        unset_scheduled_scrub_parameters(mon_obj)
        method_should_succeed(rados_object.delete_pool, active_test_pool)
        method_should_succeed(rados_object.delete_pool, inactive_test_pool)
        time.sleep(5)
        # log cluster health
        rados_object.log_cluster_health()
        # check for crashes after test execution
        test_end_time = get_cluster_timestamp(rados_object.node)
        log.debug(
            f"Test workflow completed. Start time: {start_time}, End time: {test_end_time}"
        )
        if rados_object.check_crash_status(
            start_time=start_time, end_time=test_end_time
        ):
            log.error("Test failed due to crash at the end of test")
            return 1
    log.info(
        "========== Validation of the noscrub and nodeep-scrub settings at the pool level completed  ============="
    )
    return 0


def set_scheduled_scrub_parameters(scrub_object):
    """
    Method to set the scheduled parameters
    Args:
        scrub_object: Scrub object
    Returns: None

    """
    osd_scrub_min_interval = 30
    osd_scrub_max_interval = 300
    osd_deep_scrub_interval = 300
    (
        scrub_begin_hour,
        scrub_begin_weekday,
        scrub_end_hour,
        scrub_end_weekday,
    ) = scrub_object.add_begin_end_hours(0, 1)

    log.info("Setting the scheduled scrub parameters")
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


def unset_scheduled_scrub_parameters(mon_obj):
    """
    Used to set the default osd scrub parameter value
    Args:
        mon_obj: monitor object
    Returns : None
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


def compare_pg_scrub(before_dict, after_dict):
    """
    Method is used to compare the scrub_stamps in the two dictionaries
    Args:
        before_dict: Dictionary of the old time stamps
        after_dict: Dictionary of the new time stamps
    Return:
        True : If Time stamps are not same. Scrub operations are success.
        False: If Time stamps are same. Scrub operations are failure.
    """
    for pgid, before_vals in before_dict.items():
        if pgid not in after_dict:
            log.info(
                "The PG ID %s is not present in the data collected after the tests",
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
            "%s PG ID-Before scrub stamp : %s  After scrub stamp :  %s",
            pgid,
            before_scrub,
            after_scrub,
        )
        if after_scrub == before_scrub:
            log.info("The scrubbing is in progress on -%s pgid", pgid)
            return False

        before_deep_scrub = datetime.strptime(
            before_vals["last_deep_scrub_stamp"], "%Y-%m-%dT%H:%M:%S.%f+0000"
        )
        after_deep_scrub = datetime.strptime(
            after_vals["last_deep_scrub_stamp"], "%Y-%m-%dT%H:%M:%S.%f+0000"
        )
        log.debug(
            "%s PG ID-Before deep-scrub stamp : %s  After deep-scrub stamp :  %s",
            pgid,
            before_deep_scrub,
            after_deep_scrub,
        )
        if before_deep_scrub == after_deep_scrub:
            log.info("The scrubbing is in progress on -%s pgid", pgid)
            return False
    return True


def wait_for_scrub_completion(
    rados_object, pool_name, before_scrub_stamps, wait_time_in_seconds
):
    """
    Method to Wait for the scrub operation to complete and return the status.
    Args:
        rados_object: Rados object
        pool_name: Name of the pool to monitor
        before_scrub_stamps: Scrub timestamps before starting scrub
        wait_time_in_seconds: Wait the time  to complete scrub operation
    return: True if scrub completed, False if timeout
    """
    end_time = datetime.now() + timedelta(seconds=wait_time_in_seconds)
    while datetime.now() <= end_time:
        after_scrub_stamps = rados_object.get_scrub_stamps(pool_name)
        log.debug(
            "The time stamps after testing on the pool '%s' are %s",
            pool_name,
            after_scrub_stamps,
        )

        scrub_result = compare_pg_scrub(before_scrub_stamps, after_scrub_stamps)
        if scrub_result:
            log.info("Scrub completed successfully on pool: %s", pool_name)
            return True

        log.info("Scrub operations are still in progress on pool: %s", pool_name)
        log.info("Waiting 15 seconds before retrying...")
        time.sleep(15)

    log.warning("Scrub did not complete on pool: %s before timeout", pool_name)
    return False
