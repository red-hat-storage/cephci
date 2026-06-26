"""
This file contains the methods to verify inconsistent object functionality during
scrub/deep-scrub in an EC pool.

As part of verification the script performs the following tasks:
   1. Create objects in an EC pool
   2. Convert the objects into inconsistent objects
   3. Use osd_scrub_auto_repair and osd_scrub_auto_repair_num_errors to verify PG repair behavior
       osd_scrub_auto_repair - Setting this to true enables automatic PG repair when errors are
                               found by scrubs or deep-scrubs.
       osd_scrub_auto_repair_num_errors - Auto repair will not occur if more than this many errors
                                          are found. Default value is 5.
   4. Verify the functionality by executing the following cases (config['case_to_run']):
      case1 - Scrub: inconsistent objects > osd_scrub_auto_repair_num_errors and auto_repair is true
              Set osd_scrub_auto_repair_num_errors to n-1 and osd_scrub_auto_repair to true.
              Expectation: No repairs to be made.
      case2 - Scrub: inconsistent objects < osd_scrub_auto_repair_num_errors and auto_repair is false
              Set osd_scrub_auto_repair_num_errors to n+1 and osd_scrub_auto_repair to false.
              Expectation: No repairs to be made.
      case3 - Scrub: inconsistent objects < osd_scrub_auto_repair_num_errors and auto_repair is true
              Set osd_scrub_auto_repair_num_errors to n+1 and osd_scrub_auto_repair to true.
              Expectation: All inconsistent objects are auto-repaired.
      case4 - Deep-scrub: inconsistent objects > osd_scrub_auto_repair_num_errors and auto_repair is true
              Set osd_scrub_auto_repair_num_errors to n-1 and osd_scrub_auto_repair to true.
              Expectation: No repairs to be made.
      case5 - Deep-scrub: inconsistent objects < osd_scrub_auto_repair_num_errors and auto_repair is false
              Set osd_scrub_auto_repair_num_errors to n+1 and osd_scrub_auto_repair to false.
              Expectation: No repairs to be made.
      case6 - Deep-scrub: inconsistent objects < osd_scrub_auto_repair_num_errors and auto_repair is true
              Set osd_scrub_auto_repair_num_errors to n+1 and osd_scrub_auto_repair to true.
              Expectation: All inconsistent objects are auto-repaired and PG repair state is cleared.

      If case_to_run is not specified, all six cases are executed sequentially.
"""

import time
import traceback
from datetime import datetime, timedelta

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.objectstoretool_workflows import objectstoreToolWorkflows
from ceph.rados.rados_scrub import RadosScrubber
from ceph.rados.utils import get_cluster_timestamp
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)

CASE_LOG_PREFIX = {
    "case1": "CASE1",
    "case2": "CASE2",
    "case3": "CASE3",
    "case4": "CASE4",
    "case5": "CASE5",
    "case6": "CASE6",
}


def run(ceph_cluster, **kw):
    """
    Verify inconsistent object behavior during scrub/deep-scrub in an EC pool.

    Test cases are selected via config['case_to_run']:
      case1 - Scrub with inconsistent count > auto_repair_num_errors and auto_repair enabled.
              Expectation: No repairs.
      case2 - Scrub with inconsistent count < auto_repair_num_errors and auto_repair disabled.
              Expectation: No repairs.
      case3 - Scrub with inconsistent count < auto_repair_num_errors and auto_repair enabled.
              Expectation: All inconsistent objects are auto-repaired.
      case4 - Deep-scrub with inconsistent count > auto_repair_num_errors and auto_repair enabled.
              Expectation: No repairs.
      case5 - Deep-scrub with inconsistent count < auto_repair_num_errors and auto_repair disabled.
              Expectation: No repairs.
      case6 - Deep-scrub with inconsistent count < auto_repair_num_errors and auto_repair enabled.
              Expectation: All inconsistent objects are auto-repaired and PG repair state is cleared.

    If case_to_run is not specified, all six cases run sequentially.

    Returns:
        1 -> Fail, 0 -> Pass
    """

    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    scrub_object = RadosScrubber(node=cephadm)
    objectstore_obj = objectstoreToolWorkflows(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    client_node = ceph_cluster.get_nodes(role="client")[0]
    wait_time = 45
    start_time = get_cluster_timestamp(rados_obj.node)
    log.info(f"[SETUP] Test workflow started at cluster time: {start_time}")
    try:
        log.info("[SETUP] Disabling PG autoscaler for the test duration")
        rados_obj.configure_pg_autoscaler(**{"default_mode": "off"})
        ec_config = config.get("ec_pool")
        pool_name = ec_config["pool_name"]
        req_no_of_objects = config.get("inconsistent_obj_count")
        if (
            not req_no_of_objects
            or not isinstance(req_no_of_objects, int)
            or req_no_of_objects <= 0
        ):
            log.error("[SETUP] inconsistent_obj_count must be a positive integer")
            return 1

        log.info(
            f"[SETUP] EC pool configuration - pool: {pool_name}, "
            f"requested inconsistent objects: {req_no_of_objects}"
        )

        log.info(
            "[SETUP] Resetting scrub interval settings to defaults to prevent "
            "immediate scrub from prior suite tests"
        )
        mon_obj.remove_config(section="osd", name="osd_scrub_min_interval")
        mon_obj.remove_config(section="osd", name="osd_scrub_max_interval")
        mon_obj.remove_config(section="osd", name="osd_deep_scrub_interval")

        log.info("[SETUP] Setting noscrub and nodeep-scrub OSD flags")
        scrub_object.set_osd_flags("set", "nodeep-scrub")
        scrub_object.set_osd_flags("set", "noscrub")

        if not rados_obj.create_erasure_pool(name=pool_name, **ec_config):
            log.error(f"[SETUP] Failed to create EC pool '{pool_name}'")
            return 1
        log.info(f"[SETUP] EC pool '{pool_name}' created successfully")

        acting_pg_set = rados_obj.get_pg_acting_set(pool_name=pool_name)
        log.info(f"[SETUP] Acting PG set for pool '{pool_name}': {acting_pg_set}")

        if config.get("debug_enable"):
            log.info("[SETUP] Enabling debug logging for osd and mgr")
            mon_obj.set_config(section="osd", name="debug_osd", value="20/20")
            mon_obj.set_config(section="mgr", name="debug_mgr", value="20/20")

        log.info("[SETUP] Waiting for all PGs to reach clean state")
        method_should_succeed(wait_for_clean_pg_sets, rados_obj)
        log.info("[SETUP] All PGs are in clean state")

        try:
            log.info(
                f"[SETUP] Creating {req_no_of_objects} inconsistent objects in "
                f"pool '{pool_name}'"
            )
            pg_info = rados_obj.create_ecpool_inconsistent_obj(
                objectstore_obj, client_node, pool_name, req_no_of_objects
            )
            pg_id, no_of_inconsistent_objects = pg_info
            log.info(
                f"[SETUP] Created inconsistent objects in PG {pg_id}. "
                f"Inconsistent object count: {no_of_inconsistent_objects}"
            )
        except Exception as e:
            log.error(f"[SETUP] Failed to create inconsistent objects: {e}")
            log.error(
                "[SETUP] inconsistent_obj_count must be greater than 0. "
                "Cannot proceed when auto_repair_param_value would be invalid."
            )
            return 1

        case_to_run = config.get("case_to_run")
        if not case_to_run:
            case_to_run = ["case1", "case2", "case3", "case4", "case5", "case6"]
            log.info(
                "[SETUP] case_to_run not specified; executing all cases sequentially: "
                f"{case_to_run}"
            )
        else:
            log.info(f"[SETUP] Cases selected for execution: {case_to_run}")

        if "case1" in case_to_run:
            log_case_start(
                "case1",
                "Inconsistent objects > osd_scrub_auto_repair_num_errors, auto_repair enabled",
                "scrub",
                "No repairs to be made",
            )
            log.info(
                "[CASE1] Verifying default values of osd_scrub_auto_repair_num_errors "
                "and osd_scrub_auto_repair"
            )
            auto_repair_num_value = mon_obj.get_config(
                section="osd", param="osd_scrub_auto_repair_num_errors"
            )
            log.info(
                f"[CASE1] Default osd_scrub_auto_repair_num_errors: {auto_repair_num_value}"
            )
            if int(auto_repair_num_value) != 5:
                log_case_failure(
                    "case1",
                    "Default osd_scrub_auto_repair_num_errors is not equal to 5",
                )
                return 1

            auto_repair_value = mon_obj.get_config(
                section="osd", param="osd_scrub_auto_repair"
            )
            log.info(f"[CASE1] Default osd_scrub_auto_repair: {auto_repair_value}")
            if auto_repair_value == "true":
                log_case_failure(
                    "case1", "Default osd_scrub_auto_repair should be false"
                )
                return 1

            if not check_for_pg_scrub_state(rados_obj, pg_id, wait_time):
                log_case_failure(
                    "case1",
                    "Scrub operation still in progress; cannot proceed with test",
                )
                return 1
            no_of_inconsistent_objects = get_pg_inconsistent_object_count(
                rados_obj, pg_id
            )
            auto_repair_param_value = no_of_inconsistent_objects - 1

            mon_obj.set_config(
                section="osd",
                name="osd_scrub_auto_repair_num_errors",
                value=auto_repair_param_value,
            )
            log.info(
                f"[CASE1] Set osd_scrub_auto_repair_num_errors to {auto_repair_param_value}"
            )
            mon_obj.set_config(
                section="osd", name="osd_scrub_auto_repair", value="true"
            )
            log.info("[CASE1] Set osd_scrub_auto_repair to true")
            scrub_object.set_osd_flags("unset", "noscrub")
            log.info("[CASE1] Unset noscrub OSD flag to allow scheduled scrub")
            log_case_parameters(
                "case1",
                no_of_inconsistent_objects,
                auto_repair_param_value,
                "True",
                "scrub",
            )
            obj_count = run_scrub_operation(
                scrub_object,
                mon_obj,
                pg_id,
                rados_obj,
                "scrub",
                acting_pg_set,
                "case1",
            )
            if obj_count == -1:
                return 1
            if obj_count != no_of_inconsistent_objects:
                log_case_failure(
                    "case1",
                    f"Scrub repaired {no_of_inconsistent_objects - obj_count} "
                    f"inconsistent objects unexpectedly",
                )
                rados_obj.log_cluster_health()
                return 1
            log_case_complete(
                "case1",
                "scrub",
                "No repairs to be made",
                no_of_inconsistent_objects,
                obj_count,
            )

        if "case2" in case_to_run:
            log_case_start(
                "case2",
                "Inconsistent objects < osd_scrub_auto_repair_num_errors, auto_repair disabled",
                "scrub",
                "No repairs to be made",
            )
            if not check_for_pg_scrub_state(rados_obj, pg_id, wait_time):
                log_case_failure(
                    "case2",
                    "Scrub operation still in progress; cannot proceed with test",
                )
                return 1

            mon_obj.set_config(
                section="osd", name="osd_scrub_auto_repair", value="false"
            )
            log.info("[CASE2] Set osd_scrub_auto_repair to false")
            no_of_inconsistent_objects = get_pg_inconsistent_object_count(
                rados_obj, pg_id
            )
            auto_repair_param_value = no_of_inconsistent_objects + 1
            mon_obj.set_config(
                section="osd",
                name="osd_scrub_auto_repair_num_errors",
                value=auto_repair_param_value,
            )
            log.info(
                f"[CASE2] Set osd_scrub_auto_repair_num_errors to {auto_repair_param_value}"
            )
            scrub_object.set_osd_flags("unset", "noscrub")
            log.info("[CASE2] Unset noscrub OSD flag to allow scheduled scrub")
            log_case_parameters(
                "case2",
                no_of_inconsistent_objects,
                auto_repair_param_value,
                "False",
                "scrub",
            )
            obj_count = run_scrub_operation(
                scrub_object,
                mon_obj,
                pg_id,
                rados_obj,
                "scrub",
                acting_pg_set,
                "case2",
            )
            if obj_count == -1:
                return 1
            if obj_count != no_of_inconsistent_objects:
                log_case_failure(
                    "case2",
                    f"Scrub repaired {no_of_inconsistent_objects - obj_count} "
                    f"inconsistent objects unexpectedly",
                )
                rados_obj.log_cluster_health()
                return 1
            log_case_complete(
                "case2",
                "scrub",
                "No repairs to be made",
                no_of_inconsistent_objects,
                obj_count,
            )

        if "case3" in case_to_run:
            log_case_start(
                "case3",
                "Inconsistent objects < osd_scrub_auto_repair_num_errors, auto_repair enabled",
                "scrub",
                "All inconsistent objects are auto-repaired",
            )
            if not check_for_pg_scrub_state(rados_obj, pg_id, wait_time):
                log_case_failure(
                    "case3",
                    "Scrub operation still in progress; cannot proceed with test",
                )
                return 1
            no_of_inconsistent_objects = get_pg_inconsistent_object_count(
                rados_obj, pg_id
            )
            auto_repair_param_value = no_of_inconsistent_objects + 1
            mon_obj.set_config(
                section="osd",
                name="osd_scrub_auto_repair_num_errors",
                value=auto_repair_param_value,
            )
            log.info(
                f"[CASE3] Set osd_scrub_auto_repair_num_errors to {auto_repair_param_value}"
            )
            mon_obj.set_config(
                section="osd", name="osd_scrub_auto_repair", value="true"
            )
            log.info("[CASE3] Set osd_scrub_auto_repair to true")
            scrub_object.set_osd_flags("unset", "noscrub")
            log.info("[CASE3] Unset noscrub OSD flag to allow scheduled scrub")
            log_case_parameters(
                "case3",
                no_of_inconsistent_objects,
                auto_repair_param_value,
                "True",
                "scrub",
            )
            obj_count = run_scrub_operation(
                scrub_object,
                mon_obj,
                pg_id,
                rados_obj,
                "scrub",
                acting_pg_set,
                "case3",
            )
            if obj_count == -1:
                return 1

            if obj_count != 0:
                log_case_failure(
                    "case3",
                    f"failed to repair {no_of_inconsistent_objects - obj_count} objects",
                )
                rados_obj.log_cluster_health()
                return 1
            log_case_complete(
                "case3",
                "scrub",
                "All inconsistent objects are auto-repaired",
                no_of_inconsistent_objects,
                obj_count,
            )
            if any(case in case_to_run for case in ("case4", "case5", "case6")):
                scrub_object.set_osd_flags("set", "noscrub")
                log.info(
                    "[CASE3] Set noscrub OSD flag before upcoming deep-scrub cases"
                )

        if "case4" in case_to_run:
            log_case_start(
                "case4",
                "Inconsistent objects > osd_scrub_auto_repair_num_errors, auto_repair enabled",
                "deep-scrub",
                "No repairs to be made",
            )
            if not check_for_pg_scrub_state(rados_obj, pg_id, wait_time):
                log_case_failure(
                    "case4",
                    "Scrub operation still in progress; cannot proceed with test",
                )
                return 1
            no_of_inconsistent_objects = get_pg_inconsistent_object_count(
                rados_obj, pg_id
            )
            auto_repair_param_value = no_of_inconsistent_objects - 1
            mon_obj.set_config(
                section="osd",
                name="osd_scrub_auto_repair_num_errors",
                value=auto_repair_param_value,
            )
            log.info(
                f"[CASE4] Set osd_scrub_auto_repair_num_errors to {auto_repair_param_value}"
            )
            mon_obj.set_config(
                section="osd", name="osd_scrub_auto_repair", value="true"
            )
            log.info("[CASE4] Set osd_scrub_auto_repair to true")
            scrub_object.set_osd_flags("unset", "nodeep-scrub")
            log.info(
                "[CASE4] Unset nodeep-scrub OSD flag to allow scheduled deep-scrub"
            )
            log_case_parameters(
                "case4",
                no_of_inconsistent_objects,
                auto_repair_param_value,
                "True",
                "deep-scrub",
            )
            obj_count = run_scrub_operation(
                scrub_object,
                mon_obj,
                pg_id,
                rados_obj,
                "deep-scrub",
                acting_pg_set,
                "case4",
            )
            if obj_count == -1:
                return 1
            if obj_count != no_of_inconsistent_objects:
                log_case_failure(
                    "case4",
                    f"Deep-scrub repaired {no_of_inconsistent_objects - obj_count} "
                    f"inconsistent objects unexpectedly",
                )
                rados_obj.log_cluster_health()
                return 1
            log_case_complete(
                "case4",
                "deep-scrub",
                "No repairs to be made",
                no_of_inconsistent_objects,
                obj_count,
            )

        if "case5" in case_to_run:
            log_case_start(
                "case5",
                "Inconsistent objects < osd_scrub_auto_repair_num_errors, auto_repair disabled",
                "deep-scrub",
                "No repairs to be made",
            )
            if not check_for_pg_scrub_state(rados_obj, pg_id, wait_time):
                log_case_failure(
                    "case5",
                    "Scrub operation still in progress; cannot proceed with test",
                )
                return 1
            mon_obj.set_config(
                section="osd", name="osd_scrub_auto_repair", value="false"
            )
            log.info("[CASE5] Set osd_scrub_auto_repair to false")
            no_of_inconsistent_objects = get_pg_inconsistent_object_count(
                rados_obj, pg_id
            )
            auto_repair_param_value = no_of_inconsistent_objects + 1
            mon_obj.set_config(
                section="osd",
                name="osd_scrub_auto_repair_num_errors",
                value=auto_repair_param_value,
            )
            log.info(
                f"[CASE5] Set osd_scrub_auto_repair_num_errors to {auto_repair_param_value}"
            )
            scrub_object.set_osd_flags("unset", "nodeep-scrub")
            log.info(
                "[CASE5] Unset nodeep-scrub OSD flag to allow scheduled deep-scrub"
            )

            log_case_parameters(
                "case5",
                no_of_inconsistent_objects,
                auto_repair_param_value,
                "False",
                "deep-scrub",
            )
            obj_count = run_scrub_operation(
                scrub_object,
                mon_obj,
                pg_id,
                rados_obj,
                "deep-scrub",
                acting_pg_set,
                "case5",
            )
            if obj_count == -1:
                return 1
            if obj_count != no_of_inconsistent_objects:
                log_case_failure(
                    "case5",
                    f"Deep-scrub repaired {no_of_inconsistent_objects - obj_count} "
                    f"inconsistent objects unexpectedly",
                )
                rados_obj.log_cluster_health()
                return 1
            log_case_complete(
                "case5",
                "deep-scrub",
                "No repairs to be made",
                no_of_inconsistent_objects,
                obj_count,
            )

        if "case6" in case_to_run:
            log_case_start(
                "case6",
                "Inconsistent objects < osd_scrub_auto_repair_num_errors, auto_repair enabled",
                "deep-scrub",
                "All inconsistent objects are auto-repaired and PG repair state is cleared",
            )
            if not check_for_pg_scrub_state(rados_obj, pg_id, wait_time):
                log_case_failure(
                    "case6",
                    "Scrub operation still in progress; cannot proceed with test",
                )
                return 1
            no_of_inconsistent_objects = get_pg_inconsistent_object_count(
                rados_obj, pg_id
            )
            auto_repair_param_value = no_of_inconsistent_objects + 1
            mon_obj.set_config(
                section="osd",
                name="osd_scrub_auto_repair_num_errors",
                value=auto_repair_param_value,
            )
            log.info(
                f"[CASE6] Set osd_scrub_auto_repair_num_errors to {auto_repair_param_value}"
            )
            mon_obj.set_config(
                section="osd", name="osd_scrub_auto_repair", value="true"
            )
            log.info("[CASE6] Set osd_scrub_auto_repair to true")
            scrub_object.set_osd_flags("unset", "nodeep-scrub")
            log.info(
                "[CASE6] Unset nodeep-scrub OSD flag to allow scheduled deep-scrub"
            )

            log_case_parameters(
                "case6",
                no_of_inconsistent_objects,
                auto_repair_param_value,
                "True",
                "deep-scrub",
            )
            obj_count = run_scrub_operation(
                scrub_object,
                mon_obj,
                pg_id,
                rados_obj,
                "deep-scrub",
                acting_pg_set,
                "case6",
            )
            if obj_count == -1:
                return 1
            if obj_count != 0:
                log_case_failure(
                    "case6",
                    f"Deep-scrub repaired only {no_of_inconsistent_objects - obj_count} "
                    f"of {no_of_inconsistent_objects} inconsistent objects; "
                    f"expected all objects to be repaired",
                )
                rados_obj.log_cluster_health()
                return 1
            log.info(
                f"[CASE6] All {no_of_inconsistent_objects} inconsistent objects "
                "were repaired as expected"
            )
            result = verify_pg_state(rados_obj, pg_id)
            if not result:
                log_case_failure(
                    "case6", "PG state still contains repair state after deep-scrub"
                )
                rados_obj.log_cluster_health()
                return 1
            log.info("[CASE6] PG state verified: repair state cleared after deep-scrub")
            log_case_complete(
                "case6",
                "deep-scrub",
                "All inconsistent objects are auto-repaired and PG repair state is cleared",
                no_of_inconsistent_objects,
                obj_count,
            )

        log.info(f"[SETUP] All selected cases completed successfully: {case_to_run}")
    except Exception as e:
        log.error(f"[SETUP] Test failed with unexpected exception: {e}")
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("\n\n ********* Executing finally block ******** \n\n")
        log.info("\n[TEARDOWN] Starting test cleanup\n")
        scrub_object.set_osd_flags("unset", "nodeep-scrub")
        scrub_object.set_osd_flags("unset", "noscrub")
        log.info("[TEARDOWN] Unset noscrub and nodeep-scrub OSD flags")
        rados_obj.configure_pg_autoscaler(**{"default_mode": "on"})
        log.info("[TEARDOWN] Re-enabled PG autoscaler")
        if config.get("delete_pool"):
            method_should_succeed(rados_obj.delete_pool, pool_name)
            log.info(f"[TEARDOWN] Deleted EC pool '{pool_name}' successfully")
        set_ecpool_inconsistent_default_param_value(mon_obj, scrub_object)
        time.sleep(30)

        test_end_time = get_cluster_timestamp(rados_obj.node)
        log.info(
            f"[TEARDOWN] Test workflow completed. Start time: {start_time}, "
            f"End time: {test_end_time}"
        )
        if rados_obj.check_crash_status(start_time=start_time, end_time=test_end_time):
            log.error("[TEARDOWN] Test failed due to OSD crash during test execution")
            return 1
        rados_obj.log_cluster_health()
        log.info("[TEARDOWN] Cleanup completed successfully")
    return 0


def get_pg_inconsistent_object_count(rados_obj, pg_id):
    """Fetch inconsistent object count with stability check."""
    max_attempts = 10
    stable_threshold = 2

    obj_count = 0
    prev_count = -1
    stable_count = 0

    log.info(f"Fetching inconsistent object count for PG {pg_id}")

    for attempt in range(max_attempts + 1):
        inconsistent_details = rados_obj.get_inconsistent_object_details(pg_id)
        log.debug(f"Inconsistent object details for PG {pg_id}: {inconsistent_details}")
        obj_count = len(inconsistent_details["inconsistents"])
        log.info(
            f"PG {pg_id} inconsistent object count (attempt {attempt + 1}/{max_attempts + 1}): "
            f"{obj_count}"
        )

        # Check if count has stabilized
        if obj_count == prev_count:
            stable_count += 1
            if stable_count >= stable_threshold:
                log.info(
                    f"Count stabilized at {obj_count} after {attempt + 1} attempts"
                )
                break
        else:
            stable_count = 0

        prev_count = obj_count

        # Don't sleep on last iteration
        if attempt < max_attempts:
            time.sleep(30)

    return obj_count


def get_inconsistent_count(
    scrub_object, mon_object, pg_id, rados_obj, operation, acting_pg_set
):
    """
    Perform scrub and get the inconsistent object count
    Args:
        pg_id: pg id
        rados_obj: Rados object
        operation: scrub/deep-scrub operation

    Returns: No of inconsistent object count or negative value(-1) if scrub/deep-scrub not performed on PG

    """

    operation_chk_flag = False
    osd_scrub_min_interval = 10
    osd_scrub_max_interval = 450
    osd_deep_scrub_interval = 450
    obj_count = -1
    (
        scrub_begin_hour,
        scrub_begin_weekday,
        scrub_end_hour,
        scrub_end_weekday,
    ) = scrub_object.add_begin_end_hours(0, 2)
    for osd_id in acting_pg_set:
        scrub_object.set_osd_configuration(
            "osd_scrub_begin_hour", scrub_begin_hour, osd_id
        )
        scrub_object.set_osd_configuration(
            "osd_scrub_begin_week_day", scrub_begin_weekday, osd_id
        )
        scrub_object.set_osd_configuration("osd_scrub_end_hour", scrub_end_hour, osd_id)
        scrub_object.set_osd_configuration(
            "osd_scrub_end_week_day", scrub_end_weekday, osd_id
        )
        scrub_object.set_osd_configuration(
            "osd_scrub_min_interval", osd_scrub_min_interval, osd_id
        )
        scrub_object.set_osd_configuration(
            "osd_scrub_max_interval", osd_scrub_max_interval, osd_id
        )
        scrub_object.set_osd_configuration(
            "osd_deep_scrub_interval", osd_deep_scrub_interval, osd_id
        )
    endtime = datetime.now() + timedelta(minutes=40)

    while datetime.now() <= endtime:
        if operation == "scrub":
            log.debug(f"Running scrub on pg : {pg_id}")
            if rados_obj.start_check_scrub_complete(
                pg_id=pg_id, user_initiated=False, wait_time=1500
            ):
                log.info(f"Scrub completed on pg : {pg_id}")
                operation_chk_flag = True
                break
        else:
            log.debug(f"Running deep-scrub on pg : {pg_id}")
            if rados_obj.start_check_deep_scrub_complete(
                pg_id=pg_id, user_initiated=False, wait_time=3600, time_interval=5
            ):
                log.info(f"Deep scrub completed on pg : {pg_id}")
                time.sleep(30)
                operation_chk_flag = True
                break
        log.info(f"Waiting for the {operation} to complete")
        time.sleep(60)
    if not operation_chk_flag:
        log.error(f"{operation} not initiated on pg-{pg_id}")
        return -1
    for osd_id in acting_pg_set:
        osd = f"osd.{osd_id}"
        mon_object.remove_config(section=osd, name="osd_scrub_min_interval")
        mon_object.remove_config(section=osd, name="osd_scrub_max_interval")
        mon_object.remove_config(section=osd, name="osd_deep_scrub_interval")

    obj_count = get_pg_inconsistent_object_count(rados_obj, pg_id)

    return obj_count


def verify_pg_state(rados_obj, pg_id):
    """
    Returns True if the PG state does not contain repair state.
      Args:
          rados_obj: Rados object
          pg_id: pgid

      Returns:True if repair is not in Pg state or else False

    """
    pool_pg_dump = rados_obj.get_ceph_pg_dump(pg_id=pg_id)
    pg_state = pool_pg_dump["state"]
    log.info(f"The pg status is -{pg_state}")
    if "repair" not in pg_state:
        return True
    return False


def set_ecpool_inconsistent_default_param_value(mon_obj, scrub_obj):
    """
    Method set the default parameter value
    Args:
        mon_obj: Monitor object
    Returns: None

    """
    mon_obj.remove_config(section="osd", name="osd_scrub_auto_repair_num_errors")
    mon_obj.remove_config(section="osd", name="osd_scrub_auto_repair")
    mon_obj.remove_config(section="osd", name="osd_scrub_begin_hour")
    mon_obj.remove_config(section="osd", name="osd_scrub_begin_week_day")
    mon_obj.remove_config(section="osd", name="osd_scrub_end_hour")
    mon_obj.remove_config(section="osd", name="osd_scrub_end_week_day")
    mon_obj.remove_config(section="osd", name="osd_scrub_min_interval")
    mon_obj.remove_config(section="osd", name="osd_scrub_max_interval")
    mon_obj.remove_config(section="osd", name="osd_deep_scrub_interval")
    mon_obj.remove_config(section="osd", name="debug_osd")
    scrub_obj.set_osd_flags("unset", "noscrub")
    scrub_obj.set_osd_flags("unset", "nodeep-scrub")
    mon_obj.remove_config(section="global", name="osd_pool_default_pg_autoscale_mode")
    mon_obj.remove_config(section="mgr", name="debug_mgr")
    time.sleep(10)


def check_for_pg_scrub_state(rados_obj, pg_id, wait_time):
    """
    Method is to wait for a PG  scrub operation to finish
    Args:
        rados_obj: Rados object
        pg_id: pg id
        wait_time : wait time in minutes
    Returns: bool: True if scrubbing is not in progress at wait_time, False otherwise.

    """
    end_time = datetime.now() + timedelta(minutes=wait_time)
    while end_time > datetime.now():
        try:
            pg_state = rados_obj.get_pg_state(pg_id=pg_id)
            if "scrubbing" in pg_state:
                log.info("Scrubbing in progress, waiting 5 seconds...")
                time.sleep(5)
            else:
                log.info("No scrub operations running.")
                return True
        except Exception as err:
            log.error(f"PGID : {pg_id} was not found, err: {err}")
            return False
    log.info("Timeout reached, scrubbing still in progress.")
    return False


def log_case_start(case_id, description, operation, expectation):
    """Log the start of a test case with operation details."""
    prefix = CASE_LOG_PREFIX[case_id]
    log.info(
        f"\n{'=' * 70}\n"
        f"[{prefix}] Starting: {description}\n"
        f"[{prefix}] Operation: {operation}\n"
        f"[{prefix}] Expectation: {expectation}\n"
        f"{'=' * 70}"
    )


def log_case_parameters(
    case_id, inconsistent_count, auto_repair_num_errors, auto_repair, operation
):
    """Log test parameters configured before running scrub/deep-scrub."""
    prefix = CASE_LOG_PREFIX[case_id]
    log.info(
        f"[{prefix}] Parameters - inconsistent object count: {inconsistent_count}, "
        f"osd_scrub_auto_repair_num_errors: {auto_repair_num_errors}, "
        f"osd_scrub_auto_repair: {auto_repair}, operation: {operation}"
    )


def log_case_complete(
    case_id, operation, expectation, inconsistent_count_before, inconsistent_count_after
):
    """Log successful completion of a test case."""
    prefix = CASE_LOG_PREFIX[case_id]
    log.info(
        f"[{prefix}] Completed successfully - operation: {operation}, "
        f"expectation: {expectation}, inconsistent count before: "
        f"{inconsistent_count_before}, after: {inconsistent_count_after}"
    )


def log_case_failure(case_id, message):
    """Log a test case failure."""
    log.error(f"[{CASE_LOG_PREFIX[case_id]}] Failed - {message}")


def run_scrub_operation(
    scrub_object, mon_obj, pg_id, rados_obj, operation, acting_pg_set, case_id
):
    """
    Run scrub/deep-scrub and log the outcome.

    Returns:
        int: inconsistent object count after the operation, or -1 on failure.
    """
    prefix = CASE_LOG_PREFIX[case_id]
    log.info(f"[{prefix}] Initiating scheduled {operation} on PG {pg_id}")
    try:
        obj_count = get_inconsistent_count(
            scrub_object, mon_obj, pg_id, rados_obj, operation, acting_pg_set
        )
    except Exception:
        log.error(f"[{prefix}] {operation} failed")
        raise

    if obj_count == -1:
        log.error(f"[{prefix}] {operation} was not initiated on PG {pg_id}")
        rados_obj.log_cluster_health()
        return -1

    log.info(
        f"[{prefix}] {operation} completed on PG {pg_id}. "
        f"Inconsistent object count: {obj_count}"
    )
    return obj_count
