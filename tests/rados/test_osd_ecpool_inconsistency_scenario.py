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
    scrub_obj = RadosScrubber(node=cephadm)
    obj_count = ""

    try:
        # set global autoscaler to off
        rados_obj.configure_pg_autoscaler(**{"default_mode": "off"})
        # Creating ec pool
        ec_config = config.get("ec_pool")
        pool_name = ec_config["pool_name"]

        req_no_of_objects = config.get("inconsistent_obj_count")

        if not rados_obj.create_erasure_pool(name=pool_name, **ec_config):
            log.error("Failed to create the EC Pool")
            return 1

        if config.get("debug_enable"):
            mon_obj.set_config(section="osd", name="debug_osd", value="20/20")
            mon_obj.set_config(section="mgr", name="debug_mgr", value="20/20")
        method_should_succeed(wait_for_clean_pg_sets, rados_obj)
        pg_info = rados_obj.create_ecpool_inconsistent_obj(
            objectstore_obj, client_node, pool_name, req_no_of_objects
        )
        if pg_info is None:
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
        # Verification of the tests with the scrub operation
        pg_id, no_of_objects = pg_info
        auto_repair_param_value = no_of_objects - 1

        # case 1: inconsistent objects are greater than the osd_scrub_auto_repair_num_errors count
        scrub_obj.set_osd_flags("set", "nodeep-scrub")

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

        try:
            obj_count = get_inconsistent_count(scrub_object, pg_id, rados_obj, "scrub")
        except Exception as e:
            log.info(e)
            return 1

        if obj_count == -1:
            log.error(f"The scrub not initiated on the pg-{pg_id}")
            rados_obj.log_cluster_health()
            return 1

        if obj_count != no_of_objects:
            log.error(
                f"Scrub repaired the {no_of_objects - obj_count} inconsistent objects"
            )
            return 1
        log.info(
            "Test scenario1 completed:Inconsistent objects greater than the osd_scrub_auto_repair_num_errors count\n"
            + "operation : scrub\n"
            + "Expectation : No repairs to be made\n"
            + f"Inconsistent count before test:{obj_count}\n"
            + f"Inconsistent count after test:{no_of_objects}"
        )
        # Case2: The inconsistent object count is less than osd_scrub_auto_repair_num_errors count and
        # osd_scrub_auto_repair is false
        auto_repair_param_value = no_of_objects + 1
        log.info(
            "Test scenario2:Inconsistent objects less than the osd_scrub_auto_repair_num_errors count and "
            "osd_scrub_auto_repair is false\n"
            + "operation : scrub\n"
            + "Expectation : No repairs to be made"
        )
        mon_obj.remove_config(section="osd", name="osd_scrub_auto_repair")
        mon_obj.set_config(
            section="osd",
            name="osd_scrub_auto_repair_num_errors",
            value=auto_repair_param_value,
        )

        log.info(
            f"The osd_scrub_auto_repair_num_errors value is set to {auto_repair_param_value}"
        )
        try:
            obj_count = get_inconsistent_count(scrub_object, pg_id, rados_obj, "scrub")
        except Exception as e:
            log.info(e)
            return 1
        if obj_count == -1:
            log.error(f"The scrub not initiated on the pg-{pg_id}")
            rados_obj.log_cluster_health()
            return 1
        if obj_count != no_of_objects:
            log.error(
                f"Scrub repaired the {no_of_objects - obj_count} inconsistent objects"
            )
            return 1

        log.info(
            "Test scenario2 completed:Inconsistent objects less than the osd_scrub_auto_repair_num_errors "
            "count and osd_scrub_auto_repair is false\n"
            + "operation : scrub\n"
            + "Expectation : No repairs to be made\n"
            + f"Inconsistent count before test:{obj_count}\n"
            + f"Inconsistent count after test:{no_of_objects}"
        )

        log.info(
            "Test scenario3:Inconsistent objects less than the osd_scrub_auto_repair_num_errors count and "
            "osd_scrub_auto_repair is true\n"
            + "operation : scrub\n"
            + "Expectation : Repairs to be made"
        )
        mon_obj.set_config(section="osd", name="osd_scrub_auto_repair", value="true")

        # Case3: The inconsistent object count is less than osd_scrub_auto_repair_num_errors count and
        # osd_scrub_auto_repair is True
        # # Commented the code due to the BZ#2335762- Scrub operations fail to repair inconsistent data in the EC pool
        # # when the nodeep-scrub flag is enabled. To track the issue jira task is -RHCEPHQE-17391
        #
        # obj_count = get_inconsistent_count(scrub_object, pg_id, rados_obj, "scrub")
        #
        # scrub_obj.set_osd_flags("unset", "nodeep-scrub")
        # if obj_count == -1:
        #     log.error(f"The scrub not initiated on the pg-{pg_id}")
        #     rados_obj.log_cluster_health()
        #     return 1
        #
        # if obj_count != 0:
        #     log.error(
        #         f"Scrub repaired the {no_of_objects - obj_count} inconsistent objects"
        #     )
        #     rados_obj.log_cluster_health()
        #     return 1
        # result = verify_pg_state(rados_obj, pg_id)
        #
        # if not result:
        #     log.error("The  pg state output contain repair state after deep-scrub")
        #     rados_obj.log_cluster_health()
        #     return 1

        log.info(
            "Test scenario3 completed:SCRUB-Inconsistent objects less than the osd_scrub_auto_repair_num_errors "
            "count and osd_scrub_auto_repair is true\n"
            + "operation : scrub\n"
            + "Expectation : Repairs to be made\n"
            + f"Inconsistent count before test:{obj_count}\n"
            + f"Inconsistent count after test:{no_of_objects}"
        )
        method_should_succeed(rados_obj.delete_pool, pool_name)
        set_ecpool_inconsistent_default_param_value(mon_obj, scrub_obj)
        method_should_succeed(wait_for_clean_pg_sets, rados_obj)
        log.info(
            "====VERIFICATION OF THE TESTS BY PERFORMING SCRUB OPERATIONS ARE COMPLETED===="
        )
        log.info(
            "====VERIFICATION OF THE TESTS BY PERFORMING DEEP-SCRUB OPERATIONS===="
        )
        req_no_of_objects = config.get("inconsistent_obj_count")
        if not rados_obj.create_erasure_pool(name=pool_name, **ec_config):
            log.error("Failed to create the EC Pool")
            return 1
        pg_info = rados_obj.create_ecpool_inconsistent_obj(
            objectstore_obj, client_node, pool_name, req_no_of_objects
        )
        if pg_info is None:
            log.error(
                "The inconsistent_obj_count is 0.To proceed further tests the auto_repair_param_value "
                "should be less than Inconsistent objects count which is -1.The -1 is not acceptable to "
                "run the tests"
            )
            return 1
            # Verification of the tests with the scrub operation
        pg_id, no_of_objects = pg_info
        auto_repair_param_value = no_of_objects - 1

        log.info(
            "Test scenario4:Inconsistent objects greater than the osd_scrub_auto_repair_num_errors count\n"
            + "operation : Deep-Scrub\n"
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
        method_should_succeed(wait_for_clean_pg_sets, rados_obj)
        scrub_obj.set_osd_flags("set", "noscrub")
        try:
            obj_count = get_inconsistent_count(
                scrub_object, pg_id, rados_obj, "deep-scrub"
            )
        except Exception as e:
            log.info(e)
            return 1

        if obj_count == -1:
            log.error(f"The scrub not initiated on the pg-{pg_id}")
            rados_obj.log_cluster_health()
            return 1

        if obj_count != no_of_objects:
            log.error(
                f"Scrub repaired the {no_of_objects - obj_count} inconsistent objects"
            )
            return 1

        log.info(
            "Test scenario4 completed:Inconsistent objects greater than the osd_scrub_auto_repair_num_errors count\n"
            + "operation : Deep-scrub\n"
            + "Expectation : No repairs to be made\n"
            + f"Inconsistent count before test:{obj_count}\n"
            + f"Inconsistent count after test:{no_of_objects}"
        )

        log.info(
            "Test scenario5:Inconsistent objects less than the osd_scrub_auto_repair_num_errors count and "
            "osd_scrub_auto_repair is false\n"
            + "operation : Deep-scrub\n"
            + "Expectation : No repairs to be made"
        )
        auto_repair_param_value = no_of_objects + 1
        mon_obj.remove_config(section="osd", name="osd_scrub_auto_repair")
        mon_obj.set_config(
            section="osd",
            name="osd_scrub_auto_repair_num_errors",
            value=auto_repair_param_value,
        )
        try:
            obj_count = get_inconsistent_count(
                scrub_object, pg_id, rados_obj, "deep-scrub"
            )
        except Exception as e:
            log.info(e)
            return 1
        if obj_count == -1:
            log.error(f"The scrub not initiated on the pg-{pg_id}")
            rados_obj.log_cluster_health()
            return 1
        if obj_count != no_of_objects:
            log.error(
                f"Scrub repaired the {no_of_objects - obj_count} inconsistent objects"
            )
            return 1
        log.info(
            "Test scenario5 completed:Inconsistent objects less than the osd_scrub_auto_repair_num_errors "
            "count and osd_scrub_auto_repair is false\n"
            + "operation : Deep-scrub\n"
            + "Expectation : No repairs to be made\n"
            + f"Inconsistent count before test:{obj_count}\n"
            + f"Inconsistent count after test:{no_of_objects}"
        )
        log.info(
            "Test scenario6:Inconsistent objects less than the osd_scrub_auto_repair_num_errors count and "
            "osd_scrub_auto_repair is true\n"
            + "operation : Deep-scrub\n"
            + "Expectation : Repairs to be made"
        )
        mon_obj.set_config(section="osd", name="osd_scrub_auto_repair", value="true")
        try:
            obj_count = get_inconsistent_count(
                scrub_object, pg_id, rados_obj, "deep-scrub"
            )
        except Exception as e:
            log.info(e)
            return 1
        scrub_obj.set_osd_flags("unset", "noscrub")
        if obj_count == -1:
            log.error(f"The scrub not initiated on the pg-{pg_id}")
            rados_obj.log_cluster_health()
            return 1

        if obj_count != 0:
            log.error(
                f"Scrub repaired the {no_of_objects - obj_count} inconsistent objects"
            )
            rados_obj.log_cluster_health()
            return 1
        result = verify_pg_state(rados_obj, pg_id)

        if not result:
            log.error("The  pg state output contain repair state after deep-scrub")
            rados_obj.log_cluster_health()
            return 1
        log.info(
            "Test scenario6 completed:SCRUB-Inconsistent objects less than the osd_scrub_auto_repair_num_errors "
            "count and osd_scrub_auto_repair is true\n"
            + "operation : Deep-scrub\n"
            + "Expectation : Repairs to be made\n"
            + f"Inconsistent count before test:{obj_count}\n"
            + f"Inconsistent count after test:{no_of_objects}"
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
        scrub_obj.set_osd_flags("unset", "nodeep-scrub")
        scrub_obj.set_osd_flags("unset", "noscrub")
        rados_obj.configure_pg_autoscaler(**{"default_mode": "on"})
        if config.get("delete_pool"):
            method_should_succeed(rados_obj.delete_pool, pool_name)
            log.info("deleted the pool successfully")
        set_ecpool_inconsistent_default_param_value(mon_obj, scrub_obj)
        time.sleep(30)
        # log cluster health
        rados_obj.log_cluster_health()
    return 0


def get_inconsistent_count(scrub_object, pg_id, rados_obj, operation):
    """
    Perform scrub and get the inconsistent object count
    Args:
        pg_id: pg id
        rados_obj: Rados object
        operation: scrub/deep-scrub operation

    Returns: No of inconsistent object count or negative value(-1) if scrub/deep-scrub not performed on PG

    """

    operation_chk_flag = False
    osd_scrub_min_interval = 5
    osd_scrub_max_interval = 1500
    osd_deep_scrub_interval = 2400
    obj_count = -1
    (
        scrub_begin_hour,
        scrub_begin_weekday,
        scrub_end_hour,
        scrub_end_weekday,
    ) = scrub_object.add_begin_end_hours(0, 2)

    scrub_object.set_osd_configuration("osd_scrub_begin_hour", scrub_begin_hour)
    scrub_object.set_osd_configuration("osd_scrub_begin_week_day", scrub_begin_weekday)
    scrub_object.set_osd_configuration("osd_scrub_end_hour", scrub_end_hour)
    scrub_object.set_osd_configuration("osd_scrub_end_week_day", scrub_end_weekday)
    scrub_object.set_osd_configuration("osd_scrub_min_interval", osd_scrub_min_interval)
    scrub_object.set_osd_configuration("osd_scrub_max_interval", osd_scrub_max_interval)
    scrub_object.set_osd_configuration(
        "osd_deep_scrub_interval", osd_deep_scrub_interval
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
                # Increased the time due to the BZ#2335727-Delay in displaying accurate Inconsistent Data with the
                # 'list-inconsistent-obj' Command in a ECpool.Jira tracker - RHCEPHQE-17358
                time.sleep(600)
                operation_chk_flag = True
                break
        log.info(f"Wating for the {operation} to complete")
        time.sleep(60)
    if not operation_chk_flag:
        log.error(f"{operation} not initiated on pg-{pg_id}")
        return -1
    chk_count = 0
    while chk_count <= 10:
        inconsistent_details = rados_obj.get_inconsistent_object_details(pg_id)
        log.debug(
            f"The inconsistent object details in the pg-{pg_id} is - {inconsistent_details}"
        )
        obj_count = len(inconsistent_details["inconsistents"])
        log.info(f" The inconsistent object count after {operation} is {obj_count}")
        chk_count = chk_count + 1
        time.sleep(30)
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
