"""
This file contains the  methods to verify the  inconsistent object functionality during scrub/deep-scrub in EC pool.
AS part of verification the script  perform the following tasks-
   1. Creating omaps
   2. Convert the object in to inconsistent object and generate the scrub errors
   3. Use the osd_scrub_auto_repair and osd_scrub_auto_repair_num_errors to perform the PG repair
       osd_scrub_auto_repair_num_errors - Setting this to true will enable automatic PG repair when errors are
                                          found by scrubs or deep-scrubs.
       osd_scrub_auto_repair_num_errors - Auto repair will not occur if more than this many errors are found
                                          Default value is - 5
   3. Verifying the functionality by executing the following scenarios-
      3.1- Scrub errors  greater than the osd_scrub_auto_repair_num_errors count
           Creating the n  scrub errors
           Setting the osd_scrub_auto_repair_num_errors to n-1
           Setting the osd_scrub_auto_repair to true
           Performing the scrub and deep-scrub on the PG
           NOTE: Currently this scenario fail while performing the deep-scrub.This is  due to the BZ#2316244
      3.2  The scrub error count is less than osd_scrub_auto_repair_num_errors count
           Setting the osd_scrub_auto_repair_num_errors to n+1
           Performing the scrub and deep-scrub on the PG
"""

import time
import traceback

from test_osd_ecpool_inconsistency_scenario import (
    check_for_pg_scrub_state,
    get_inconsistent_count,
    get_pg_inconsistent_object_count,
    print_parameter_messages,
    set_ecpool_inconsistent_default_param_value,
    verify_pg_state,
)

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.objectstoretool_workflows import objectstoreToolWorkflows
from ceph.rados.rados_scrub import RadosScrubber
from ceph.rados.utils import get_cluster_timestamp
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test to create an inconsistent object functionality during scrub/deep-scrub in replicated pool.
    Returns:
        1 -> Fail, 0 -> Pass
    """

    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    scrub_object = RadosScrubber(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    scrub_obj = RadosScrubber(node=cephadm)
    objectstore_obj = objectstoreToolWorkflows(node=cephadm, nostart=True)
    wait_time = 15
    start_time = get_cluster_timestamp(rados_obj.node)
    log.debug(f"Test workflow started. Start time: {start_time}")
    try:

        pool_name = config["pool_name"]
        no_of_objects = config.get("inconsistent_obj_count")

        # Set the default values
        log.info(
            "Reset the scrub minimum and maximum interval settings to their default values before initiating "
            "the tests. This ensures that any custom values set by previous tests in the suite are cleared, "
            "preventing the scrub operation from starting immediately."
        )
        log.info("Before starting the tests ")
        mon_obj.remove_config(section="osd", name="osd_scrub_min_interval")
        mon_obj.remove_config(section="osd", name="osd_scrub_max_interval")
        mon_obj.remove_config(section="osd", name="osd_deep_scrub_interval")

        scrub_obj.set_osd_flags("set", "nodeep-scrub")
        scrub_obj.set_osd_flags("set", "noscrub")

        try:
            if not rados_obj.create_pool(**config):
                log.error("Failed to create the replicated Pool")
                return 1
            log.info("Generating the inconsistent object")
            obj_pg_map = rados_obj.create_inconsistent_object(
                objectstore_obj, pool_name, no_of_objects
            )
            if obj_pg_map is None:
                log.error(
                    "Inconsistent objects are not created.Not executing the further test"
                )
                return 1

            pg_id = list(set(obj_pg_map.values()))[0]
            msg_pgid = f"The inconsistent object created in{pg_id} pg and the pool is {pool_name}"
            log.info(msg_pgid)
            msg_obj = (
                f"The objects in the {pool_name} pool is - {list(obj_pg_map.keys())}"
            )
            log.info(msg_obj)
        except Exception as e:
            log.error(e)
            log.error(
                "The inconsistent_obj_count is 0.To proceed further tests the auto_repair_param_value "
                "should be less than Inconsistent objects count which is -1.The -1 is not acceptable to "
                "run the tests"
            )
            return 1

        # getting the acting set for the created pool
        acting_pg_set = rados_obj.get_pg_acting_set(pool_name=pool_name)

        if config.get("debug_enable"):
            mon_obj.set_config(section="osd", name="debug_osd", value="20/20")
            mon_obj.set_config(section="mgr", name="debug_mgr", value="20/20")

        if "case1" in config.get("case_to_run"):
            # case 1: inconsistent objects are greater than the osd_scrub_auto_repair_num_errors count
            log.info(
                "Test scenario1:Inconsistent objects greater than the osd_scrub_auto_repair_num_errors count\n"
                + "operation : scrub\n"
                + "Expectation : No repairs to be made"
            )
            log.info(
                "Verification of the  default values of the osd_scrub_auto_repair_num_errors and "
                "osd_scrub_auto_repair "
            )

            auto_repair_num_value = mon_obj.get_config(
                section="osd", param="osd_scrub_auto_repair_num_errors"
            )
            msg_auto_repair = f"Original value of osd_scrub_auto_repair_num_errors is {auto_repair_num_value}"
            log.info(msg_auto_repair)

            if int(auto_repair_num_value) != 5:
                log.error(
                    "The default value of the osd_scrub_auto_repair_num_errors is not equal to 5"
                )
                return 1

            auto_repair_value = mon_obj.get_config(
                section="osd", param="osd_scrub_auto_repair"
            )
            msg_auto_repair = (
                f"Original value of osd_scrub_auto_repair is {auto_repair_value}"
            )
            log.info(msg_auto_repair)
            if auto_repair_value is True:
                log.error(
                    "The default value of the osd_scrub_auto_repair should be false"
                )
                return 1

            if not check_for_pg_scrub_state(rados_obj, pg_id, wait_time):
                log.error(
                    "Test_scenario1: The scrub operations are still in progress.Not executing the further tests"
                )
                return 1
            scrub_error_count = get_pg_inconsistent_object_count(rados_obj, pg_id)
            auto_repair_param_value = scrub_error_count - 1

            log.info(
                "Test scenario1: scrub error count greater than the osd_scrub_auto_repair_num_errors count"
            )
            mon_obj.set_config(
                section="osd",
                name="osd_scrub_auto_repair_num_errors",
                value=auto_repair_param_value,
            )
            msg_auto_repair = f"The osd_scrub_auto_repair_num_errors value is set to {auto_repair_param_value}"
            log.info(msg_auto_repair)
            mon_obj.set_config(
                section="osd", name="osd_scrub_auto_repair", value="true"
            )
            log.info("The osd_scrub_auto_repair value is set to true")
            scrub_obj.set_osd_flags("unset", "noscrub")
            print_parameter_messages(
                1,
                scrub_error_count,
                auto_repair_param_value,
                auto_repair_value="True",
                operation="scrub",
            )

            try:
                get_inconsistent_count(
                    scrub_object, mon_obj, pg_id, rados_obj, "scrub", acting_pg_set
                )
            except Exception as e:
                log.info(e)
            new_scrub_error_count = get_pg_inconsistent_object_count(rados_obj, pg_id)
            if scrub_error_count != new_scrub_error_count:
                msg_err_inconsistent = (
                    f"Scrub repaired the {scrub_error_count - new_scrub_error_count} "
                    f"inconsistent objects"
                )
                log.error(msg_err_inconsistent)
                rados_obj.log_cluster_health()
                return 1
            msg_scenario_end = (
                "Test scenario2 completed:Inconsistent objects greater than the "
                "osd_scrub_auto_repair_num_errors count and osd_scrub_auto_repair is true\n"
                + "operation : scrub\n"
                + "Expectation : No repairs to be made\n"
                + f"Inconsistent count before test:{scrub_error_count}\n"
                + f"Inconsistent count after test:{new_scrub_error_count}"
            )
            log.info(msg_scenario_end)
        if "case2" in config.get("case_to_run"):
            log.info(
                "Test scenario2:Inconsistent objects less than the osd_scrub_auto_repair_num_errors count and "
                "osd_scrub_auto_repair is false\n"
                + "operation : scrub\n"
                + "Expectation : No repairs to be made"
            )
            if not check_for_pg_scrub_state(rados_obj, pg_id, wait_time):
                log.error(
                    "Test_scenario2: The scrub operations are still in progress.Not executing the further tests"
                )
                return 1
            mon_obj.set_config(
                section="osd", name="osd_scrub_auto_repair", value="false"
            )
            log.info("The osd_scrub_auto_repair value is set to false")
            scrub_error_count = get_pg_inconsistent_object_count(rados_obj, pg_id)
            auto_repair_param_value = scrub_error_count + 1
            mon_obj.set_config(
                section="osd",
                name="osd_scrub_auto_repair_num_errors",
                value=auto_repair_param_value,
            )
            log.info(
                f"The osd_scrub_auto_repair_num_errors value is set to {auto_repair_param_value}"
            )
            scrub_obj.set_osd_flags("unset", "noscrub")
            print_parameter_messages(
                2,
                scrub_error_count,
                auto_repair_param_value,
                auto_repair_value="False",
                operation="scrub",
            )
            try:
                get_inconsistent_count(
                    scrub_object, mon_obj, pg_id, rados_obj, "scrub", acting_pg_set
                )
            except Exception as e:
                log.info(e)
            new_scrub_error_count = get_pg_inconsistent_object_count(rados_obj, pg_id)
            if scrub_error_count != new_scrub_error_count:
                log.error(
                    f"Scrub repaired the {scrub_error_count - new_scrub_error_count} inconsistent objects"
                )
                rados_obj.log_cluster_health()
                return 1
            msg_scenario_end = (
                "Test scenario2 completed:Inconsistent objects less than the "
                "osd_scrub_auto_repair_num_errors count and osd_scrub_auto_repair is false\n"
                + "operation : scrub\n"
                + "Expectation : No repairs to be made\n"
                + f"Inconsistent count before test:{scrub_error_count}\n"
                + f"Inconsistent count after test:{new_scrub_error_count}"
            )
            log.info(msg_scenario_end)
        if "case3" in config.get("case_to_run"):
            log.info(
                "Test scenario3:Inconsistent objects less than the osd_scrub_auto_repair_num_errors count and "
                "osd_scrub_auto_repair is true\n"
                + "operation : scrub\n"
                + "Expectation : No repairs to be made"
            )
            if not check_for_pg_scrub_state(rados_obj, pg_id, wait_time):
                log.error(
                    "Test_scenario3: The scrub operations are still in progress.Not executing the further tests"
                )
                return 1
            scrub_error_count = get_pg_inconsistent_object_count(rados_obj, pg_id)
            auto_repair_param_value = scrub_error_count + 1
            mon_obj.set_config(
                section="osd",
                name="osd_scrub_auto_repair_num_errors",
                value=auto_repair_param_value,
            )
            mon_obj.set_config(
                section="osd", name="osd_scrub_auto_repair", value="true"
            )
            scrub_obj.set_osd_flags("unset", "noscrub")
            print_parameter_messages(
                3,
                scrub_error_count,
                auto_repair_param_value,
                auto_repair_value="True",
                operation="scrub",
            )
            try:
                get_inconsistent_count(
                    scrub_object, mon_obj, pg_id, rados_obj, "scrub", acting_pg_set
                )
            except Exception as e:
                log.info(e)
            new_scrub_error_count = get_pg_inconsistent_object_count(rados_obj, pg_id)
            if scrub_error_count != new_scrub_error_count:
                msg_err = f"Scrub repaired the {scrub_error_count - new_scrub_error_count} inconsistent objects"
                log.error(msg_err)
                rados_obj.log_cluster_health()
                return 1
            msg_scenario = (
                "Test scenario3 completed:SCRUB-Inconsistent objects less than the "
                "osd_scrub_auto_repair_num_errors count and osd_scrub_auto_repair is true\n"
                + "operation : scrub\n"
                + "Expectation : No repairs to be made\n"
                + f"Inconsistent count before test:{scrub_error_count}\n"
                + f"Inconsistent count after test:{new_scrub_error_count}"
            )
            log.info(msg_scenario)
        if "case4" in config.get("case_to_run"):

            log.info(
                "Test scenario4:Inconsistent objects greater than the osd_scrub_auto_repair_num_errors count\n"
                + "operation : Deep-Scrub\n"
                + "Expectation : No repairs to be made"
            )
            if not check_for_pg_scrub_state(rados_obj, pg_id, wait_time):
                log.error(
                    "Test_scenario4: The scrub operations are still in progress.Not executing the further tests"
                )
                return 1
            scrub_error_count = get_pg_inconsistent_object_count(rados_obj, pg_id)
            auto_repair_param_value = scrub_error_count - 1

            mon_obj.set_config(
                section="osd",
                name="osd_scrub_auto_repair_num_errors",
                value=auto_repair_param_value,
            )
            msg_repair_info = f"The osd_scrub_auto_repair_num_errors value is set to {auto_repair_param_value}"
            mon_obj.set_config(
                section="osd", name="osd_scrub_auto_repair", value="true"
            )
            log.info(msg_repair_info)
            scrub_obj.set_osd_flags("unset", "nodeep-scrub")
            time.sleep(5)
            print_parameter_messages(
                4,
                scrub_error_count,
                auto_repair_param_value,
                auto_repair_value="True",
                operation="deep-scrub",
            )
            try:
                get_inconsistent_count(
                    scrub_object, mon_obj, pg_id, rados_obj, "deep-scrub", acting_pg_set
                )
            except Exception as e:
                log.info(e)
            new_scrub_error_count = get_pg_inconsistent_object_count(rados_obj, pg_id)
            if scrub_error_count != new_scrub_error_count:
                log.error(
                    f"Deep-Scrub repaired the {scrub_error_count - new_scrub_error_count} inconsistent objects"
                )
                rados_obj.log_cluster_health()
                return 1
            msg_scenario = (
                "Test scenario4 completed:Inconsistent objects greater than the "
                "osd_scrub_auto_repair_num_errors count operation : Deep-scrub\n"
                + "Expectation : No repairs to be made\n"
                + f"Inconsistent count before test:{scrub_error_count}\n"
                + f"Inconsistent count after test:{new_scrub_error_count}"
            )
            log.info(msg_scenario)
        if "case5" in config.get("case_to_run"):
            log.info(
                "Test scenario5:Inconsistent objects less than the osd_scrub_auto_repair_num_errors count and "
                "osd_scrub_auto_repair is false\n"
                + "operation : Deep-scrub\n"
                + "Expectation : No repairs to be made"
            )
            if not check_for_pg_scrub_state(rados_obj, pg_id, wait_time):
                log.error(
                    "Test_scenario5: The scrub operations are still in progress.Not executing the further tests"
                )
                return 1
            mon_obj.set_config(
                section="osd", name="osd_scrub_auto_repair", value="false"
            )
            log.info("The osd_scrub_auto_repair value is set to false")
            scrub_error_count = get_pg_inconsistent_object_count(rados_obj, pg_id)
            auto_repair_param_value = scrub_error_count + 1
            mon_obj.set_config(
                section="osd",
                name="osd_scrub_auto_repair_num_errors",
                value=auto_repair_param_value,
            )
            msg_auto_repair = f"The osd_scrub_auto_repair_num_errors value is set to {auto_repair_param_value}"
            log.info(msg_auto_repair)
            scrub_obj.set_osd_flags("unset", "nodeep-scrub")
            time.sleep(5)
            print_parameter_messages(
                5,
                scrub_error_count,
                auto_repair_param_value,
                auto_repair_value="False",
                operation="deep-scrub",
            )
            try:
                get_inconsistent_count(
                    scrub_object, mon_obj, pg_id, rados_obj, "deep-scrub", acting_pg_set
                )
            except Exception as e:
                log.info(e)
            new_scrub_error_count = get_pg_inconsistent_object_count(rados_obj, pg_id)
            if scrub_error_count != new_scrub_error_count:
                msg_repair_info = (
                    f"Deep-Scrub repaired the "
                    f"{scrub_error_count - new_scrub_error_count} inconsistent objects"
                )
                log.error(msg_repair_info)
                rados_obj.log_cluster_health()
                return 1
            msg_scenario = (
                "Test scenario5 completed:Inconsistent objects less than the "
                "osd_scrub_auto_repair_num_errors count and osd_scrub_auto_repair is false\n"
                + "operation : Deep-scrub\n"
                + "Expectation : No repairs to be made\n"
                + f"Inconsistent count before test:{scrub_error_count}\n"
                + f"Inconsistent count after test:{new_scrub_error_count}"
            )
            log.info(msg_scenario)
        if "case6" in config.get("case_to_run"):
            log.info(
                "Test scenario6:Inconsistent objects less than the osd_scrub_auto_repair_num_errors count and "
                "osd_scrub_auto_repair is true\n"
                + "operation : Deep-scrub\n"
                + "Expectation : Repairs to be made"
            )
            wait_time = 30
            if not check_for_pg_scrub_state(rados_obj, pg_id, wait_time):
                log.error(
                    "Test_scenario6: The scrub operations are still in progress.Not executing the further tests"
                )
                return 1
            scrub_error_count = get_pg_inconsistent_object_count(rados_obj, pg_id)

            auto_repair_param_value = scrub_error_count + 1
            mon_obj.set_config(
                section="osd",
                name="osd_scrub_auto_repair_num_errors",
                value=auto_repair_param_value,
            )
            mon_obj.set_config(
                section="osd", name="osd_scrub_auto_repair", value="true"
            )
            scrub_obj.set_osd_flags("unset", "nodeep-scrub")
            time.sleep(5)
            print_parameter_messages(
                6,
                scrub_error_count,
                auto_repair_param_value,
                auto_repair_value="True",
                operation="deep-scrub",
            )
            try:
                get_inconsistent_count(
                    scrub_object, mon_obj, pg_id, rados_obj, "deep-scrub", acting_pg_set
                )
            except Exception as e:
                log.info(e)
            new_scrub_error_count = get_pg_inconsistent_object_count(rados_obj, pg_id)
            # new_scrub_error_count = get_inconsistent_object_count(rados_obj, pg_id)
            if new_scrub_error_count != 0:
                msg_repair_info = (
                    f"Deep-Scrub repaired the {scrub_error_count - new_scrub_error_count} "
                    f"inconsistent objects.The actual inconsistent objects are - {scrub_error_count}"
                )
                log.error(msg_repair_info)
                rados_obj.log_cluster_health()
                return 1
            result = verify_pg_state(rados_obj, pg_id)
            if not result:
                log.error("The  pg state output contain repair state after scrub")
                return 1
            msg_scenario = (
                "Test scenario6 completed:SCRUB-Inconsistent objects less than the "
                "osd_scrub_auto_repair_num_errors count and osd_scrub_auto_repair is true\n"
                + "operation : Deep-scrub\n"
                + "Expectation : Repairs to be made\n"
                + f"Inconsistent count before test:{scrub_error_count}\n"
                + f"Inconsistent count after test:{new_scrub_error_count}"
            )
            log.info(msg_scenario)

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info("===============Execution of finally block=======================")
        scrub_obj.set_osd_flags("unset", "nodeep-scrub")
        scrub_obj.set_osd_flags("unset", "noscrub")
        mon_obj.remove_config(section="osd", name="debug_osd")
        mon_obj.remove_config(section="mgr", name="debug_mgr")
        method_should_succeed(rados_obj.delete_pool, pool_name)
        log.info("deleted the pool successfully")
        # The values used for the  replicated and ec pool are same.Using the same method to set the default values
        set_ecpool_inconsistent_default_param_value(mon_obj, scrub_object)
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        test_end_time = get_cluster_timestamp(rados_obj.node)
        log.debug(
            f"Test workflow completed. Start time: {start_time}, End time: {test_end_time}"
        )
        if rados_obj.check_crash_status(start_time=start_time, end_time=test_end_time):
            log.error("Test failed due to crash at the end of test")
            return 1
    return 0
