"""
This module contains the methods required to check the scheduled scrubbing scenarios.
Based on the test cases setting the scrub parameters and verifying the functionality.
"""

import os
import sys
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.rados_scrub import RadosScrubber
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    osd_scrub_min_interval = 10
    osd_scrub_max_interval = 300
    osd_deep_scrub_interval = 300

    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosScrubber(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)

    test_scenarios = ["scrub", "deep-scrub"]
    # test_scenarios = ["deep-scrub"]

    for test_input in test_scenarios:
        # Creating replicated pool
        rp_config = config.get("replicated_pool")
        pool_name = rp_config["pool_name"]
        log.info(f"===Start the  validation of the {test_input}=======")
        if config.get("debug_enable"):
            log.info("Set the OSD logs to 20")
            mon_obj.set_config(section="osd", name="debug_osd", value="20/20")

        if not rados_obj.create_pool(name=pool_name, **rp_config):
            log.error("Failed to create the  Pool")
            return 1
        rados_obj.bench_write(pool_name=pool_name, byte_size="5K", max_objs=200)
        # rados_obj.bench_write(pool_name=, rados_write_duration=5)
        pg_id = rados_obj.get_pgid(pool_name=pool_name)
        log.info(f"The pg id is -{pg_id}")
        flag = wait_for_clean_pg_sets(rados_obj)
        if not flag:
            log.error(
                "The cluster did not reach active + Clean state after add capacity"
            )
            return 1
        leader_mon_daemon = rados_obj.run_ceph_command(cmd="ceph mon stat")["leader"]
        mon_host = rados_obj.fetch_host_node(
            daemon_type="mon", daemon_id=leader_mon_daemon
        )
        init_time, _ = mon_host.exec_command(cmd="date '+%Y-%m-%d %H:%M:%S'", sudo=True)
        log.info(f"The cluster time before  testing is - {init_time}")
        try:
            (
                scrub_begin_hour,
                scrub_begin_weekday,
                scrub_end_hour,
                scrub_end_weekday,
            ) = rados_obj.add_begin_end_hours(0, 1)

            # Scenario to verify that scrub start and end hours are same
            # CEPH-9362
            if config.get("scenario") == "begin_end_time_equal":
                log.info(f'{"Setting scrub start and end hour same"}')
                scrub_end_hour = scrub_begin_hour

            # Begin time is greater than end hour  and current time less than end hour
            # CEPH-9363&CEPH-9364

            if config.get("scenario") == "beginTime gt endTime":
                log.info(
                    f'{"Setting scrub start time is greater than end hour and current time less than end hour"}'
                )
                scrub_end_hour, scrub_begin_hour = scrub_begin_hour, scrub_end_hour

            # set begin_hour > end_hour and current_time > end_hour
            # CEPH-9365&CEPH-9366
            if config.get("scenario") == "beginTime gt endTime lt currentTime":
                (
                    scrub_begin_hour,
                    scrub_begin_weekday,
                    scrub_end_hour,
                    scrub_end_weekday,
                ) = rados_obj.add_begin_end_hours(-1, -1)
                log.info(
                    f'{"Setting scrub start is greater then end_hour and current time is greater than end hour"}'
                )

            # set begin hour and end hour greater than current time
            # CEPH-9367 & CEPH 9368
            if config.get("scenario") == "beginTime and endTime gt currentTime":
                (
                    scrub_begin_hour,
                    scrub_begin_weekday,
                    scrub_end_hour,
                    scrub_end_weekday,
                ) = rados_obj.add_begin_end_hours(2, 3)
                log.info(
                    f'{"Setting scrub start is greater then end_hour and current time is greater than end hour"}'
                )
                osd_scrub_min_interval = 86400
                osd_scrub_max_interval = 604800
                osd_deep_scrub_interval = 604800

            # unset the scrub.CEPH-9374
            if config.get("scenario") == "unsetScrub":
                rados_obj.set_osd_flags("set", "noscrub")
                log.info("Set no scrub flag")
                rados_obj.set_osd_flags("set", "nodeep-scrub")
                log.info("Set no deep scrub flag")

            # Setting the scrub parameters
            rados_obj.set_osd_configuration(
                "osd_scrub_min_interval", osd_scrub_min_interval
            )
            rados_obj.set_osd_configuration(
                "osd_scrub_max_interval", osd_scrub_max_interval
            )
            rados_obj.set_osd_configuration(
                "osd_deep_scrub_interval", osd_deep_scrub_interval
            )
            rados_obj.set_osd_configuration(
                "osd_scrub_begin_week_day", scrub_begin_weekday
            )
            rados_obj.set_osd_configuration("osd_scrub_end_week_day", scrub_end_weekday)
            rados_obj.set_osd_configuration("osd_scrub_begin_hour", scrub_begin_hour)
            rados_obj.set_osd_configuration("osd_scrub_end_hour", scrub_end_hour)

            if config.get("scenario") == "decreaseTime":
                (
                    scrub_begin_hour,
                    scrub_begin_weekday,
                    scrub_end_hour,
                    scrub_end_weekday,
                ) = rados_obj.add_begin_end_hours(0, 1)

                rados_obj.set_osd_configuration(
                    "osd_scrub_begin_week_day", scrub_begin_weekday
                )
                rados_obj.set_osd_configuration(
                    "osd_scrub_end_week_day", scrub_end_weekday
                )
                rados_obj.set_osd_configuration(
                    "osd_scrub_begin_hour", scrub_begin_hour
                )
                rados_obj.set_osd_configuration("osd_scrub_end_hour", scrub_end_hour)

            # Printing the scrub parameter values
            get_parameter_configuration(mon_obj)
            status = chk_scrub_or_deepScrub_status(rados_obj, test_input, pg_id[-1])
            end_time, _ = mon_host.exec_command(
                cmd="date '+%Y-%m-%d %H:%M:%S'", sudo=True
            )
            log.info(f"The cluster time after testing is - {end_time}")

            # Checking the failure case
            if status is False and (
                config.get("scenario") == "default"
                or config.get("scenario") == "begin_end_time_equal"
                or config.get("scenario") == "beginTime gt endTime lt currentTime"
                or config.get("scenario") == "decreaseTime"
                or config.get("scenario") == "beginTime gt endTime"
            ):
                log.info(f"{test_input} validation failed")
                return 1

            if (
                config.get("scenario") == "beginTime and endTime gt currentTime"
                or config.get("scenario") == "unsetScrub"
            ) and status is True:
                log.info(f"{test_input} validation Failed")
                return 1
            log.info(f"Deletion of the pool after the {test_input} validation")
            method_should_succeed(rados_obj.delete_pool, pool_name)
            time.sleep(10)
            flag = wait_for_clean_pg_sets(rados_obj)
            if not flag:
                log.error(
                    "The cluster did not reach active + Clean state after add capacity"
                )
                return 1
            log.info(f"===End of the  validation of the {test_input}=======")
        except Exception as err:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            sys.stderr.write("ERRORED DESC\t::%s:\n" % str(err))
            sys.stderr.write("ERRORED MODULE\t::%s:\n" % str(exc_type))
            sys.stderr.write("ERRORED FILE\t::%s:\n" % str(fname))
            sys.stderr.write("ERRORED LINE\t::%s:\n" % str(exc_tb.tb_lineno))
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here *************** \n \n"
            )
            mon_obj.remove_config(section="osd", name="debug_osd")
            method_should_succeed(rados_obj.delete_pool, pool_name)
            remove_parameter_configuration(mon_obj)
            rados_obj.set_osd_flags("unset", "noscrub")
            rados_obj.set_osd_flags("unset", "nodeep-scrub")
            time.sleep(10)
            flag = wait_for_clean_pg_sets(rados_obj)
            if not flag:
                log.error(
                    "The cluster did not reach active + Clean state after add capacity"
                )
                return 1
            # log cluster health
            rados_obj.log_cluster_health()
    log.error(f'{"Schedule scrub & deep-scrub validation success"}')
    return 0


def remove_parameter_configuration(mon_obj):
    """
    Used to set the default osd scrub parameter value
    Args:
        mon_obj: monitor object
    Returns : None
    """
    mon_obj.remove_config(section="osd", name="osd_scrub_min_interval")
    mon_obj.remove_config(section="osd", name="osd_scrub_max_interval")
    mon_obj.remove_config(section="osd", name="osd_deep_scrub_interval")
    mon_obj.remove_config(section="osd", name="osd_scrub_begin_week_day")
    mon_obj.remove_config(section="osd", name="osd_scrub_end_week_day")
    mon_obj.remove_config(section="osd", name="osd_scrub_begin_hour")
    mon_obj.remove_config(section="osd", name="osd_scrub_end_hour")


def chk_scrub_or_deepScrub_status(rados_object, action, pg_id):
    """
    Method to check that the scrub or deep-scrub is completed
    Args:
        rados_object: Rados object
        action: To perform scrub or deep-scrub
        pg_id: pg id

    Returns: True if scrub or deep-scrub started
             False if scrub or deep-scrub not started
    """
    if action == "scrub":
        try:
            status = rados_object.start_check_scrub_complete(
                pg_id=pg_id, user_initiated=False, wait_time=900
            )
            log.info("Verification of the scrub is completed")
        except Exception as err:
            log.error(f"The error is-{err}")
            status = False
    else:
        try:
            status = rados_object.start_check_deep_scrub_complete(
                pg_id=pg_id, user_initiated=False, wait_time=900
            )
            log.info("Verification of the deep-scrub is completed")
        except Exception as err:
            log.error(f"The error is-{err}")
            status = False
    return status


def get_parameter_configuration(mon_obj):
    """
    Used to get the  osd scrub parameter change values
    Args:
        mon_obj: monitor object
    Returns : None
    """
    log.info("The scrub parameter values are:  ")
    out_put = mon_obj.get_config(section="osd", param="osd_scrub_min_interval")
    log.info(f"The osd_scrub_min_interval parameter value is - {out_put}")
    out_put = mon_obj.get_config(section="osd", param="osd_scrub_max_interval")
    log.info(f"The osd_scrub_max_interval parameter value is - {out_put}")
    out_put = mon_obj.get_config(section="osd", param="osd_deep_scrub_interval")
    log.info(f"The osd_deep_scrub_interval parameter value is - {out_put}")
    out_put = mon_obj.get_config(section="osd", param="osd_scrub_begin_week_day")
    log.info(f"The osd_scrub_begin_week_day parameter value is - {out_put}")
    out_put = mon_obj.get_config(section="osd", param="osd_scrub_end_week_day")
    log.info(f"The osd_scrub_end_week_day parameter value is - {out_put}")
    out_put = mon_obj.get_config(section="osd", param="osd_scrub_begin_hour")
    log.info(f"The osd_scrub_begin_hour parameter value is - {out_put}")
    out_put = mon_obj.get_config(section="osd", param="osd_scrub_end_hour")
    log.info(f"The osd_scrub_end_hour parameter value is - {out_put}")
