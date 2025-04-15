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
    get_inconsistent_count,
    set_ecpool_inconsistent_default_param_value,
    verify_pg_state,
)

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from ceph.rados.rados_scrub import RadosScrubber
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
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
    pool_obj = PoolFunctions(node=cephadm)
    scrub_obj = RadosScrubber(node=cephadm)

    try:
        replicated_config = config.get("replicated_pool")
        replicated_pool_name = replicated_config["pool_name"]
        no_of_objects = config.get("inconsistent_obj_count")
        pg_info = create_pool_inconsistent_object(
            rados_obj, no_of_objects, pool_obj, **replicated_config
        )
        if pg_info is None:
            log.error(
                "The inconsistent_obj_count is 0.To proceed further tests the auto_repair_param_value "
                "should be less than Inconsistent objects count which is -1.The -1 is not acceptable to "
                "run the tests"
            )
            return 1
        mon_obj.set_config(section="osd", name="debug_osd", value="20/20")
        mon_obj.set_config(section="mgr", name="debug_mgr", value="20/20")
        pg_id, inconsistent_obj_count = pg_info
        log.info(
            f"The inconsistent object count is- {inconsistent_obj_count} on pg -{pg_id}"
        )

        scrub_error_count = get_scrub_error_count(rados_obj)
        auto_repair_param_value = scrub_error_count - 1

        # Check the default values of the osd_scrub_auto_repair_num_errors and osd_scrub_auto_repair
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

        auto_repair_value = mon_obj.get_config(
            section="osd", param="osd_scrub_auto_repair"
        )
        log.info(f"Original value of osd_scrub_auto_repair is {auto_repair_value}")
        if auto_repair_value is True:
            log.error("The default value of the osd_scrub_auto_repair should be false")
            return 1

        # case 1: inconsistent objects are greater than the osd_scrub_auto_repair_num_errors count
        log.info(
            "Test scenario1: scrub error count greater than the osd_scrub_auto_repair_num_errors count"
        )
        mon_obj.set_config(
            section="osd",
            name="osd_scrub_auto_repair_num_errors",
            value=auto_repair_param_value,
        )
        log.info(
            f"The osd_scrub_auto_repair_num_errors value is set to {auto_repair_param_value}"
        )

        mon_obj.set_config(section="osd", name="osd_scrub_auto_repair", value="true")
        log.info("The osd_scrub_auto_repair value is set to true")
        method_should_succeed(wait_for_clean_pg_sets, rados_obj)

        scrub_obj.set_osd_flags("set", "nodeep-scrub")
        try:
            get_inconsistent_count(scrub_object, pg_id, rados_obj, "scrub")
        except Exception as e:
            log.info(e)
        scrub_obj.set_osd_flags("unset", "nodeep-scrub")
        new_scrub_error_count = get_scrub_error_count(rados_obj)
        if scrub_error_count != new_scrub_error_count:
            log.error(
                f"Scrub repaired the {scrub_error_count - new_scrub_error_count} inconsistent objects"
            )
            rados_obj.log_cluster_health()
            return 1
        log.info(
            "Test scenario1: scrub error count > osd_scrub_auto_repair_num_errors,errors are not repaired"
        )
        method_should_succeed(wait_for_clean_pg_sets, rados_obj)
        scrub_obj.set_osd_flags("set", "noscrub")
        try:
            # At the moment, this scenario fails.This is  due to the BZ#2316244
            get_inconsistent_count(scrub_object, pg_id, rados_obj, "deep-scrub")
        except Exception as e:
            log.info(e)
        scrub_obj.set_osd_flags("unset", "noscrub")
        new_scrub_error_count = get_scrub_error_count(rados_obj)
        if scrub_error_count != new_scrub_error_count:
            log.error(
                f"Deep scrub repaired the {scrub_error_count - new_scrub_error_count} inconsistent objects"
            )
            rados_obj.log_cluster_health()
            return 1
        log.info(
            "Test scenario1: scrub error count > osd_scrub_auto_repair_num_errors,errors are not repaired"
        )
        log.info(
            "Verification of Test scenario1- inconsistent objects greater than the "
            "osd_scrub_auto_repair_num_errors count completed "
        )
        # Case2: The inconsistent object count is less than osd_scrub_auto_repair_num_errors count
        log.info(
            "Test scenario2: inconsistent objects less than the osd_scrub_auto_repair_num_errors count "
        )
        auto_repair_param_value = scrub_error_count + 1
        mon_obj.set_config(
            section="osd",
            name="osd_scrub_auto_repair_num_errors",
            value=auto_repair_param_value,
        )

        log.info(
            f"The osd_scrub_auto_repair_num_errors value is set to {auto_repair_param_value}"
        )
        method_should_succeed(wait_for_clean_pg_sets, rados_obj)
        scrub_obj.set_osd_flags("set", "nodeep-scrub")
        try:
            get_inconsistent_count(scrub_object, pg_id, rados_obj, "scrub")
        except Exception as e:
            log.info(e)
        scrub_obj.set_osd_flags("unset", "nodeep-scrub")
        new_scrub_error_count = get_scrub_error_count(rados_obj)
        if new_scrub_error_count != 0:
            log.error(
                f"Scrub repaired the {scrub_error_count - new_scrub_error_count} inconsistent objects"
            )
            rados_obj.log_cluster_health()
            return 1
        result = verify_pg_state(rados_obj, pg_id)
        if not result:
            log.error("The  pg state output contain repair state after scrub")
            return 1

        method_should_succeed(rados_obj.delete_pool, replicated_pool_name)
        time.sleep(5)
        pg_info = create_pool_inconsistent_object(
            rados_obj, no_of_objects, pool_obj, **replicated_config
        )
        if pg_info is None:
            log.error(
                "The inconsistent_obj_count is 0.To proceed further tests the auto_repair_param_value "
                "should be less than Inconsistent objects count which is -1.The -1 is not acceptable to "
                "run the tests"
            )
            return 1
        pg_id, inconsistent_obj_count = pg_info
        scrub_error_count = get_scrub_error_count(rados_obj)
        auto_repair_param_value = scrub_error_count + 1
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
            get_inconsistent_count(scrub_object, pg_id, rados_obj, "deep-scrub")
        except Exception as e:
            log.info(e)
        scrub_obj.set_osd_flags("unset", "noscrub")
        new_scrub_error_count = get_scrub_error_count(rados_obj)
        if new_scrub_error_count != 0:
            log.error(
                f"Deep scrub repaired the {scrub_error_count - new_scrub_error_count} inconsistent objects"
            )
            rados_obj.log_cluster_health()
            return 1
        result = verify_pg_state(rados_obj, pg_id)
        if not result:
            log.error("The  pg state output contain repair state after deep-scrub")
            return 1
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info("Execution of finally block")
        scrub_obj.set_osd_flags("unset", "nodeep-scrub")
        scrub_obj.set_osd_flags("unset", "noscrub")
        mon_obj.remove_config(section="osd", name="debug_osd")
        mon_obj.remove_config(section="mgr", name="debug_mgr")
        if config.get("delete_pool"):
            method_should_succeed(rados_obj.delete_pool, replicated_pool_name)
            log.info("deleted the pool successfully")
        # The values used for the  replicated and ec pool are same.Using the same method to set the default values
        set_ecpool_inconsistent_default_param_value(mon_obj, scrub_object)
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
    return 0


def create_pool_inconsistent_object(
    rados_object,
    no_of_objects,
    pool_obj,
    **config,
):
    """
     The method create a replicated pool and generate the inconsistent objects
    Args:
        rados_object: Rados object
        no_of_objects: numer of objects to convert into inconsistent objects
        pool_obj: pool object
        config: pool configurations
    Returns: Returns  the pg id and no of inconsistent object count
             None if the inconsistent object count is 0
    """
    pg_id_list = []
    pool_name = config["pool_name"]
    if not rados_object.create_pool(**config):
        log.error("Failed to create the replicated Pool")
        return 1
    if not pool_obj.fill_omap_entries(
        pool_name=pool_name, obj_end=50, num_keys_obj=100
    ):
        log.error(f"Omap entries not generated on pool {pool_name}")
        raise Exception(f"Omap entries not generated on pool {pool_name}")
    rep_obj_list = rados_object.get_object_list(pool_name)
    rep_count = 0
    for obj in rep_obj_list:
        try:
            # Create inconsistency objects
            try:
                pg_id = rados_object.create_inconsistent_object(
                    pool_name, obj, num_keys=1
                )
                pg_id_list.append(pg_id)
            except Exception as err:
                log.info(
                    f"Cannot able to convert the object-{obj} into inconsistent object."
                    f"Picking another object to convert-{err}.Currently the {rep_count} objects converted into "
                    f"inconsistent objects"
                )
                continue
            rep_count = rep_count + 1
            log.info(f"The {rep_count} objects are converted into inconsistent objects")
            if rep_count == no_of_objects:
                log.info(
                    f"The inconsistent count on the replicated pool is - {rep_count}"
                )
                break
        except Exception as err:
            log.error(f"Unable to create inconsistent object. error {err}")
            raise Exception("inconsistent object not generated error")
    if rep_count == 0:
        log.error(
            f"The inconsistent object count is- {rep_count}.The inconsistent object should be greater than 1"
        )
        return None
    pg_id_set = set(pg_id_list)
    if pg_id_set is None:
        log.error("The inconsistent objects are not crated in any PG.The pg_id is none")
        return 1
    pg_id = pg_id_set.pop()
    return pg_id, rep_count


def get_scrub_error_count(rados_object):
    """
    Method is used to get the scrub error count in the cluster
    Args:
        rados_object: Rados object
    Return:
        Retuns the scrub error count.If not present returns 0

    """
    status_out_put = rados_object.run_ceph_command(cmd="ceph -s")
    if "OSD_SCRUB_ERRORS" not in status_out_put["health"]["checks"]:
        log.error("The inconsistent objects not created")
        return 0
    return status_out_put["health"]["checks"]["OSD_SCRUB_ERRORS"]["summary"]["count"]
