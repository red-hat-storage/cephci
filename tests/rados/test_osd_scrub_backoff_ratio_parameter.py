"""
Polarion ID: CEPH-XXXXX - Verification of the osd_scrub_backoff_ratio parameter functionality

The file contains the method to verify the osd_scrub_backoff_ratio parameter.
The following steps are verified-
1. Verification of the default value of the osd_scrub_backoff_ratio parameter.

2. Modification of the osd_scrub_backoff_ratio value and verification of scrub operation behavior.

3. Verification of scrub backoff behavior under different load conditions.

"""

import math
import pdb
import time
import traceback

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.rados_scrub import RadosScrubber
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.scheduled_scrub_scenarios import chk_scrub_or_deepScrub_status
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from tests.rados.test_osd_scrub_load_threshold_parameter import (
    configure_log_level,
    remove_parameter_configuration,
)
from tests.rados.test_track_availibility_status import create_data_pool
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_object = RadosOrchestrator(node=cephadm)
    scrub_object = RadosScrubber(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_object)
    installer = ceph_cluster.get_nodes(role="installer")[0]
    wait_time = 120
    expected_backoff_ratio = 0.66
    osd_nodes = ceph_cluster.get_nodes(role="osd")
    replicated_config = config.get("replicated_pool")
    pool_name = replicated_config["pool_name"]
    acting_pg_set = ""
    scrub_failure_msg = "'Lost on the dice. Regular scheduled scrubs are not permitted'"
    # scrub_success_message = "Won and scrub operation starts"

    try:

        assert create_data_pool(mon_obj, rados_object, replicated_config, pool_name)

        # Enable the file logging
        if not rados_object.enable_file_logging():
            log.error("Error while setting config to enable logging into file")
            return 1
        log.info("Logging to file configured")

        # Get the pool pg_id
        pg_id = rados_object.get_pgid(pool_name=pool_name)
        pg_id = pg_id[0]
        msg_pool_id = f"The {pool_name} pg id is - {pg_id}"
        log.info(msg_pool_id)

        if "case1" in config.get("case_to_run"):
            log.info(
                "====Case1 : The verification of the osd_scrub_backoff_ratio parameter by setting the value to 1 ==== "
            )
            operations = ["scrub", "deep_scrub"]
            # Get default value of osd_scrub_backoff_ratio
            actual_backoff_ratio = mon_obj.get_config(
                section="osd", param="osd_scrub_backoff_ratio"
            )
            actual_backoff_ratio = float(actual_backoff_ratio)
            log.info(
                f"The default value of the osd_scrub_backoff_ratio is {actual_backoff_ratio}"
            )

            if not math.isclose(expected_backoff_ratio, actual_backoff_ratio):
                log.error(
                    "The osd_scrub_backoff_ratio default value is not -%s the actual value is -%s",
                    expected_backoff_ratio,
                    actual_backoff_ratio,
                )
                return 1
            scrub_object.set_osd_configuration("osd_scrub_backoff_ratio", "1")

            log.info("Waiting for the PG into active+clean state")
            if not wait_for_clean_pg_sets(rados_object, timeout=300):
                log.error(
                    "Cluster could not reach active+clean state within 300 seconds"
                )
            acting_pg_set = rados_object.get_pg_acting_set(pool_name=pool_name)
            msg_acting_set = f"The {pool_name} pool acting set is -{acting_pg_set}"
            log.info(msg_acting_set)
            wait_time = 150
            for operation in operations:

                # Truncate the osd logs
                rados_object.remove_log_file_content(osd_nodes, daemon_type="osd")
                log.info("The content of osd log are removed")

                init_time, _ = installer.exec_command(
                    cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
                )
                init_time = init_time.strip()
                configure_log_level(mon_obj, acting_pg_set, set_to_default=False)
                set_scheduled_scrub_parameters(scrub_object, operation, acting_pg_set)
                status = chk_scrub_or_deepScrub_status(
                    rados_object, operation, pg_id, wait_time=wait_time
                )
                if status:
                    log.error(
                        "%s  operation started after setting the osd_scrub_backoff_ratio to 1",
                        operation,
                    )
                    return 1
                log.info(
                    "%s operation not started after setting the osd_scrub_backoff_ratio to 1",
                    operation,
                )
                configure_log_level(mon_obj, acting_pg_set, set_to_default=True)
                end_time, _ = installer.exec_command(
                    cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
                )
                end_time = end_time.strip()
                remove_parameter_configuration(mon_obj)
                if (
                    rados_object.lookup_log_message(
                        init_time=init_time,
                        end_time=end_time,
                        daemon_type="osd",
                        daemon_id=acting_pg_set[0],
                        search_string=scrub_failure_msg,
                    )
                    is False
                ):
                    log.error(
                        "Scenario1:After setting the backoff ratio to 1 the scrub operation is initiated"
                    )
                    return 1
                log.info(
                    "After setting the backoff ratio to 1 the %s operation is not initiated",
                    operation,
                )
                log.info(
                    "====Case1 : The verification of the osd_scrub_backoff_ratio parameter by setting "
                    "the value to 1  completed==== "
                )
        if "case2" in config.get("case_to_run"):
            log.info(
                "====Case2 : The verification of the osd_scrub_backoff_ratio parameter by setting the value to 0 ==== "
            )

            # scrub_object.set_osd_flags("set", "noscrub")

            scrub_wait_time = 1000
            acting_pg_set = rados_object.get_pg_acting_set(pool_name=pool_name)
            msg_acting_set = f"The {pool_name} pool acting set is -{acting_pg_set}"
            log.info(msg_acting_set)

            time_diff_dict = {}
            for backoff_ratio in ["0", ".5", ".75"]:
                scrub_object.set_osd_configuration("osd_scrub_backoff_ratio", "0")
                set_scheduled_scrub_parameters(
                    scrub_object, "deep_scrub", acting_pg_set
                )
                log.info("The parameters are set")
                method_start_time = time.time()
                status = chk_scrub_or_deepScrub_status(
                    rados_object, "deep_scrub", pg_id, wait_time=scrub_wait_time
                )
                method_end_time = time.time()
                remove_parameter_configuration(mon_obj)
                if not status:
                    log.error(
                        "Deep_scrub operation not started after setting the osd_scrub_backoff_ratio to 0"
                    )
                    return 1
                time_diff = method_end_time - method_start_time
                time_diff_dict[backoff_ratio] = time_diff
                log.info("Waiting for the PG into active+clean state")
                if not wait_for_clean_pg_sets(rados_object, timeout=600):
                    log.error(
                        "Cluster could not reach active+clean state within 600 seconds"
                    )
                    return 1
            pdb.set_trace()
            diff_0_05 = time_diff_dict[".5"] - time_diff_dict["0"]
            log.info("The time difference between the value .5 and 0 is- %s", diff_0_05)

            diff_05_075 = time_diff_dict[".75"] - time_diff_dict[".5"]
            log.info(
                "The time difference between the value .75 and .5 is- %s", diff_05_075
            )

            if time_diff_dict["0"] > time_diff_dict[".5"] > time_diff_dict[".75"]:
                log.error(
                    "The execution time of the backoff ratio is not working as expected"
                )
                return 1
            # scrub_object.set_osd_flags("unset", "noscrub")
            log.info(
                "====Case2 : The verification of the osd_scrub_backoff_ratio parameter by"
                " setting the value to 0 completed==== "
            )

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        method_should_succeed(rados_object.delete_pool, pool_name)
        remove_parameter_configuration(mon_obj)
    log.info(
        "========== Validation of the osd_scrub_backoff_ratio is success ============="
    )
    return 0


def set_scheduled_scrub_parameters(scrub_object, operation, acting_pg_set):
    """
    Method to set the scheduled parameters
    Args:
        scrub_object: Scrub object
        acting_pg_set: Acting PG set
        operation: scrub/deep scrub operation
    Returns: None
    """
    osd_scrub_min_interval = 60
    osd_scrub_max_interval = 600
    osd_deep_scrub_interval = 600
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
        if operation == "scrub":
            scrub_object.set_osd_configuration(
                "osd_scrub_max_interval", osd_scrub_max_interval, osd_id
            )
        if operation == "deep_scrub":
            scrub_object.set_osd_configuration(
                "osd_deep_scrub_interval", osd_deep_scrub_interval, osd_id
            )
