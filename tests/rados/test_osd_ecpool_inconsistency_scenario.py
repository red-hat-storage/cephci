"""
This file contains the  methods to verify the  inconsistent object functionality during scrub/deep-scrub in EC pool.
AS part of verification the script  perform the following tasks-
   1. Create objects
   2. Convert the object in to inconsistent object
   3. Use the osd_scrub_auto_repair and osd_scrub_auto_repair_num_errors to perform the PG repair
       osd_scrub_auto_repair_num_errors - Setting this to true will enable automatic PG repair when errors are
                                          found by scrubs or deep-scrubs.
       osd_scrub_auto_repair_num_errors - Auto repair will not occur if more than this many errors are found
                                          Default value is - 5
   3. Verifying the functionality by executing the following scenarios-
      3.1- Inconsistent objects greater than the osd_scrub_auto_repair_num_errors count
           Creating the n inconsistent
           Setting the osd_scrub_auto_repair_num_errors to n-1
           Setting the osd_scrub_auto_repair to true
           Performing the scrub and deep-scrub on the PG
      3.2  The inconsistent object count is less than osd_scrub_auto_repair_num_errors count
           Setting the osd_scrub_auto_repair_num_errors to n+1
           Performing the scrub and deep-scrub on the PG
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


def run(ceph_cluster, **kw):
    """
    Test to create an inconsistent object functionality during scrub/deep-scrub in EC pool.
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
    log.debug(f"Test workflow started. Start time: {start_time}")
    try:
        # set global autoscaler to off
        rados_obj.configure_pg_autoscaler(**{"default_mode": "off"})
        # Creating ec pool
        ec_config = config.get("ec_pool")
        pool_name = ec_config["pool_name"]
        req_no_of_objects = config.get("inconsistent_obj_count")

        # Set the default values
        log.info(
            "Reset the scrub minimum and maximum interval settings to their default values before initiating "
            "the tests. This ensures that any custom values set by previous tests in the suite are cleared, "
            "preventing the scrub operation from starting immediately."
        )
        mon_obj.remove_config(section="osd", name="osd_scrub_min_interval")
        mon_obj.remove_config(section="osd", name="osd_scrub_max_interval")
        mon_obj.remove_config(section="osd", name="osd_deep_scrub_interval")

        scrub_object.set_osd_flags("set", "nodeep-scrub")
        scrub_object.set_osd_flags("set", "noscrub")

        if not rados_obj.create_erasure_pool(name=pool_name, **ec_config):
            log.error("Failed to create the EC Pool")
            return 1
        # Get the acting PG set
        acting_pg_set = rados_obj.get_pg_acting_set(pool_name=pool_name)

        if config.get("debug_enable"):
            mon_obj.set_config(section="osd", name="debug_osd", value="20/20")
            mon_obj.set_config(section="mgr", name="debug_mgr", value="20/20")
        method_should_succeed(wait_for_clean_pg_sets, rados_obj)

        try:
            pg_info = rados_obj.create_ecpool_inconsistent_obj(
                objectstore_obj, client_node, pool_name, req_no_of_objects
            )

            pg_id, no_of_inconsistent_objects = pg_info
        except Exception as e:
            log.error(e)
            log.error(
                "The inconsistent_obj_count is 0.To proceed further tests the auto_repair_param_value "
                "should be less than Inconsistent objects count which is -1.The -1 is not acceptable to "
                "run the tests"
            )
            return 1

        # Check the default values of the osd_scrub_auto_repair_num_errors
        auto_repair_num_value = mon_obj.get_config(
            section="osd", param="osd_scrub_auto_repair_num_errors"
        )
        log.info(
            f"Original value of osd_scrub_auto_repair_num_errors is {auto_repair_num_value}"
        )
        if int(auto_repair_num_value) != 5:
            log.error(
                "The default value of the osd_scrub_auto_repair_num_errors is not equal to 5"
            )
            return 1

        # Check the default values of the osd_scrub_auto_repair
        auto_repair_value = mon_obj.get_config(
            section="osd", param="osd_scrub_auto_repair"
        )
        log.info(f"Original value of osd_scrub_auto_repair is {auto_repair_value}")
        if auto_repair_value is True:
            log.error("The default value of the osd_scrub_auto_repair should be false")
            return 1

        log.info("====VERIFICATION OF THE TESTS BY PERFORMING SCRUB OPERATIONS====")

        if not check_for_pg_scrub_state(rados_obj, pg_id, wait_time):
            log.error(
                "Test_scenario1: The scrub operations are still in progress.Not executing the further tests"
            )
            return 1
        no_of_inconsistent_objects = get_pg_inconsistent_object_count(rados_obj, pg_id)
        auto_repair_param_value = no_of_inconsistent_objects - 1

        # case 1: inconsistent objects are greater than the osd_scrub_auto_repair_num_errors count

        mon_obj.set_config(section="osd", name="osd_scrub_auto_repair", value="true")
        log.info("The osd_scrub_auto_repair value is set to true")

        log.info(
            "Test scenario1:Inconsistent objects greater than the osd_scrub_auto_repair_num_errors count\n"
            + "operation : scrub\n"
            + "Expectation : No repairs to be made"
        )
        mon_obj.set_config(
            section="osd",
            name="osd_scrub_auto_repair_num_errors",
            value=auto_repair_param_value,
        )
        log.info(
            f"The osd_scrub_auto_repair_num_errors value is set to {auto_repair_param_value}"
        )
        scrub_object.set_osd_flags("unset", "noscrub")
        print_parameter_messages(
            1,
            no_of_inconsistent_objects,
            auto_repair_param_value,
            auto_repair_value="True",
            operation="scrub",
        )
        try:
            obj_count = get_inconsistent_count(
                scrub_object, mon_obj, pg_id, rados_obj, "scrub", acting_pg_set
            )
        except Exception as e:
            log.info(e)
            return 1
        msg_obj_count = f"Scenario-1:The object count value is-{obj_count}"
        log.info(msg_obj_count)
        if obj_count == -1:
            log.error(f"The scrub not initiated on the pg-{pg_id}")
            rados_obj.log_cluster_health()
            return 1

        if obj_count != no_of_inconsistent_objects:
            log.error(
                f"Scrub repaired the {no_of_inconsistent_objects - obj_count} inconsistent objects"
            )
            return 1
        log.info(
            "Test scenario1 completed:Inconsistent objects greater than the osd_scrub_auto_repair_num_errors count\n"
            + "operation : scrub\n"
            + "Expectation : No repairs to be made\n"
            + f"Inconsistent count before test:{obj_count}\n"
            + f"Inconsistent count after test:{no_of_inconsistent_objects}"
        )

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

        mon_obj.set_config(section="osd", name="osd_scrub_auto_repair", value="false")
        no_of_inconsistent_objects = get_pg_inconsistent_object_count(rados_obj, pg_id)
        auto_repair_param_value = no_of_inconsistent_objects + 1
        mon_obj.set_config(
            section="osd",
            name="osd_scrub_auto_repair_num_errors",
            value=auto_repair_param_value,
        )

        log.info(
            f"The osd_scrub_auto_repair_num_errors value is set to {auto_repair_param_value}"
        )
        print_parameter_messages(
            2,
            no_of_inconsistent_objects,
            auto_repair_param_value,
            auto_repair_value="False",
            operation="scrub",
        )
        try:
            obj_count = get_inconsistent_count(
                scrub_object, mon_obj, pg_id, rados_obj, "scrub", acting_pg_set
            )
        except Exception as e:
            log.info(e)
            return 1
        # Change to high value
        msg_obj_count = f"Scenario-2:The object count value is-{obj_count}"
        log.info(msg_obj_count)
        if obj_count == -1:
            log.error(f"The scrub not initiated on the pg-{pg_id}")
            rados_obj.log_cluster_health()
            return 1
        if obj_count != no_of_inconsistent_objects:
            log.error(
                f"Scrub repaired the {no_of_inconsistent_objects - obj_count} inconsistent objects"
            )
            return 1

        log.info(
            "Test scenario2 completed:Inconsistent objects less than the osd_scrub_auto_repair_num_errors "
            "count and osd_scrub_auto_repair is false\n"
            + "operation : scrub\n"
            + "Expectation : No repairs to be made\n"
            + f"Inconsistent count before test:{obj_count}\n"
            + f"Inconsistent count after test:{no_of_inconsistent_objects}"
        )

        # Check the scrub/deep-scrub in sin running or not if not
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
        no_of_inconsistent_objects = get_pg_inconsistent_object_count(rados_obj, pg_id)
        auto_repair_param_value = no_of_inconsistent_objects + 1
        mon_obj.set_config(
            section="osd",
            name="osd_scrub_auto_repair_num_errors",
            value=auto_repair_param_value,
        )
        mon_obj.set_config(section="osd", name="osd_scrub_auto_repair", value="true")
        print_parameter_messages(
            3,
            no_of_inconsistent_objects,
            auto_repair_param_value,
            auto_repair_value="True",
            operation="scrub",
        )
        obj_count = get_inconsistent_count(
            scrub_object, mon_obj, pg_id, rados_obj, "scrub", acting_pg_set
        )
        msg_obj_count = f"Scenario-3:The object count value is-{obj_count}"
        log.info(msg_obj_count)

        if obj_count == -1:
            log.error(f"The scrub not initiated on the pg-{pg_id}")
            rados_obj.log_cluster_health()
            return 1
        if obj_count != no_of_inconsistent_objects:
            log.error(
                f"Scrub repaired the {no_of_inconsistent_objects - obj_count} inconsistent objects"
            )
            return 1

        log.info(
            "Test scenario3 completed:SCRUB-Inconsistent objects less than the osd_scrub_auto_repair_num_errors "
            "count and osd_scrub_auto_repair is true\n"
            + "operation : scrub\n"
            + "Expectation : No repairs to be made\n"
            + f"Inconsistent count before test:{obj_count}\n"
            + f"Inconsistent count after test:{no_of_inconsistent_objects}"
        )
        scrub_object.set_osd_flags("set", "noscrub")
        log.info("Scenario3: noscrub flag is set")
        log.info(
            "====VERIFICATION OF THE TESTS BY PERFORMING SCRUB OPERATIONS ARE COMPLETED===="
        )
        log.info(
            "====VERIFICATION OF THE TESTS BY PERFORMING DEEP-SCRUB OPERATIONS===="
        )

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
        no_of_inconsistent_objects = get_pg_inconsistent_object_count(rados_obj, pg_id)
        auto_repair_param_value = no_of_inconsistent_objects - 1
        mon_obj.set_config(
            section="osd",
            name="osd_scrub_auto_repair_num_errors",
            value=auto_repair_param_value,
        )

        log.info(
            f"The osd_scrub_auto_repair_num_errors value is set to {auto_repair_param_value}"
        )
        mon_obj.set_config(section="osd", name="osd_scrub_auto_repair", value="true")
        scrub_object.set_osd_flags("unset", "nodeep-scrub")
        time.sleep(5)
        print_parameter_messages(
            4,
            no_of_inconsistent_objects,
            auto_repair_param_value,
            auto_repair_value="True",
            operation="deep-scrub",
        )
        try:
            obj_count = get_inconsistent_count(
                scrub_object, mon_obj, pg_id, rados_obj, "deep-scrub", acting_pg_set
            )
        except Exception as e:
            log.info(e)
            return 1
        msg_obj_count = f"Scenario-4:The object count value is-{obj_count}"
        log.info(msg_obj_count)
        if obj_count == -1:
            log.error(f"The scrub not initiated on the pg-{pg_id}")
            rados_obj.log_cluster_health()
            return 1

        if obj_count != no_of_inconsistent_objects:
            log.error(
                f"Scrub repaired the {no_of_inconsistent_objects - obj_count} inconsistent objects"
            )
            return 1

        log.info(
            "Test scenario4 completed:Inconsistent objects greater than the osd_scrub_auto_repair_num_errors count\n"
            + "operation : Deep-scrub\n"
            + "Expectation : No repairs to be made\n"
            + f"Inconsistent count before test:{obj_count}\n"
            + f"Inconsistent count after test:{no_of_inconsistent_objects}"
        )

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
        no_of_inconsistent_objects = get_pg_inconsistent_object_count(rados_obj, pg_id)
        auto_repair_param_value = no_of_inconsistent_objects + 1
        mon_obj.set_config(section="osd", name="osd_scrub_auto_repair", value="false")
        mon_obj.set_config(
            section="osd",
            name="osd_scrub_auto_repair_num_errors",
            value=auto_repair_param_value,
        )
        print_parameter_messages(
            5,
            no_of_inconsistent_objects,
            auto_repair_param_value,
            auto_repair_value="False",
            operation="deep-scrub",
        )
        try:
            obj_count = get_inconsistent_count(
                scrub_object, mon_obj, pg_id, rados_obj, "deep-scrub", acting_pg_set
            )
        except Exception as e:
            log.info(e)
            return 1
        msg_obj_count = f"Scenario-5:The object count value is-{obj_count}"
        log.info(msg_obj_count)
        if obj_count == -1:
            log.error(f"The scrub not initiated on the pg-{pg_id}")
            rados_obj.log_cluster_health()
            return 1
        if obj_count != no_of_inconsistent_objects:
            log.error(
                f"Scrub repaired the {no_of_inconsistent_objects - obj_count} inconsistent objects"
            )
            return 1
        log.info(
            "Test scenario5 completed:Inconsistent objects less than the osd_scrub_auto_repair_num_errors "
            "count and osd_scrub_auto_repair is false\n"
            + "operation : Deep-scrub\n"
            + "Expectation : No repairs to be made\n"
            + f"Inconsistent count before test:{obj_count}\n"
            + f"Inconsistent count after test:{no_of_inconsistent_objects}"
        )
        log.info(
            "Test scenario6:Inconsistent objects less than the osd_scrub_auto_repair_num_errors count and "
            "osd_scrub_auto_repair is true\n"
            + "operation : Deep-scrub\n"
            + "Expectation : Repairs to be made"
        )
        if not check_for_pg_scrub_state(rados_obj, pg_id, wait_time):
            log.error(
                "Test_scenario6: The scrub operations are still in progress.Not executing the further tests"
            )
            return 1
        no_of_inconsistent_objects = get_pg_inconsistent_object_count(rados_obj, pg_id)
        auto_repair_param_value = no_of_inconsistent_objects + 1
        mon_obj.set_config(
            section="osd",
            name="osd_scrub_auto_repair_num_errors",
            value=auto_repair_param_value,
        )
        mon_obj.set_config(section="osd", name="osd_scrub_auto_repair", value="true")
        print_parameter_messages(
            6,
            no_of_inconsistent_objects,
            auto_repair_param_value,
            auto_repair_value="True",
            operation="deep-scrub",
        )
        try:
            obj_count = get_inconsistent_count(
                scrub_object, mon_obj, pg_id, rados_obj, "deep-scrub", acting_pg_set
            )
        except Exception as e:
            log.info(e)
            return 1
        msg_obj_count = f"Scenario-6:The object count value is-{obj_count}"
        log.info(msg_obj_count)

        msg_obj_count = f"The object count value is-{obj_count}"
        log.info(msg_obj_count)

        if obj_count == -1:
            log.error(f"The scrub not initiated on the pg-{pg_id}")
            rados_obj.log_cluster_health()
            return 1

        if obj_count != 0:
            log.error(
                f"Scrub repaired the {no_of_inconsistent_objects - obj_count} inconsistent objects"
            )
            rados_obj.log_cluster_health()
            return 1
        result = verify_pg_state(rados_obj, pg_id)

        if not result:
            log.error("The  pg state output contain repair state after deep-scrub")
            rados_obj.log_cluster_health()
            return 1
        scrub_object.set_osd_flags("unset", "noscrub")
        log.info(
            "Test scenario6 completed:SCRUB-Inconsistent objects less than the osd_scrub_auto_repair_num_errors "
            "count and osd_scrub_auto_repair is true\n"
            + "operation : Deep-scrub\n"
            + "Expectation : Repairs to be made\n"
            + f"Inconsistent count before test:{obj_count}\n"
            + f"Inconsistent count after test:{no_of_inconsistent_objects}"
        )

        log.info(
            "====VERIFICATION OF THE TESTS BY PERFORMING DEEP-SCRUB OPERATIONS ARE COMPLETED===="
        )
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info(
            "\n\n================ Execution of finally block =======================\n\n"
        )
        scrub_object.set_osd_flags("unset", "nodeep-scrub")
        scrub_object.set_osd_flags("unset", "noscrub")
        rados_obj.configure_pg_autoscaler(**{"default_mode": "on"})
        if config.get("delete_pool"):
            method_should_succeed(rados_obj.delete_pool, pool_name)
            log.info("deleted the pool successfully")
        set_ecpool_inconsistent_default_param_value(mon_obj, scrub_object)
        time.sleep(30)

        test_end_time = get_cluster_timestamp(rados_obj.node)
        log.debug(
            f"Test workflow completed. Start time: {start_time}, End time: {test_end_time}"
        )
        if rados_obj.check_crash_status(start_time=start_time, end_time=test_end_time):
            log.error("Test failed due to crash at the end of test")
            return 1
        # log cluster health
        rados_obj.log_cluster_health()
    return 0


def get_pg_inconsistent_object_count(rados_obj, pg_id):
    chk_count = 0
    obj_count = ""
    while chk_count <= 10:
        inconsistent_details = rados_obj.get_inconsistent_object_details(pg_id)
        log.debug(
            f"The inconsistent object details in the pg-{pg_id} is - {inconsistent_details}"
        )
        obj_count = len(inconsistent_details["inconsistents"])
        log.info(f" The inconsistent object count is --{obj_count}")
        chk_count = chk_count + 1
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
    ) = scrub_object.add_begin_end_hours(0, 1)
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
    endTime = datetime.now() + timedelta(minutes=40)

    while datetime.now() <= endTime:
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
                pg_id=pg_id, user_initiated=False, wait_time=2400
            ):
                log.info(f"Deep scrub completed on pg : {pg_id}")
                time.sleep(30)
                operation_chk_flag = True
                break
        log.info(f"Wating for the {operation} to complete")
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


def verify_pg_state(rads_obj, pg_id):
    """
    The method return if the pg state not contain repair state
    Args:
        rads_obj: Rados object
        pg_id: pgid

    Returns: True or False

    """
    pool_pg_dump = rads_obj.get_ceph_pg_dump(pg_id=pg_id)
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
    log.info("Timeout reached, scrubbing  still  in progress.")
    return False


def print_parameter_messages(
    scenario_value,
    scrub_error_count,
    auto_repair_param_value,
    auto_repair_value,
    operation,
):
    msg_scenario_tmp = (
        f"===================Test scenario{scenario_value} - "
        f"Parameter details before starting test  =========="
    )
    log.info(msg_scenario_tmp)
    msg_tmp = f"The scrub error count is - {scrub_error_count}"
    log.info(msg_tmp)
    msg_tmp = (
        f"The osd_scrub_auto_repair_num_errors value is - {auto_repair_param_value}"
    )
    log.info(msg_tmp)
    msg_tmp = f"The osd_scrub_auto_repair value is -{auto_repair_value}"
    log.info(msg_tmp)
    msg_tmp = f"Operation : {operation}"
    log.info(msg_tmp)
    log.info(msg_scenario_tmp)
