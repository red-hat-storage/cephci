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
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    osd_scrub_min_interval = 60
    osd_scrub_max_interval = 3600
    osd_deep_scrub_interval = 3600

    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosScrubber(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    # Creating ec pool
    ec_config = config.get("replicated_pool")
    pool_name = ec_config["pool_name"]

    if not rados_obj.create_pool(name=pool_name, **ec_config):
        log.error("Failed to create the  Pool")
        return 1
    rados_obj.bench_write(pool_name=pool_name, rados_write_duration=60)

    pg_id = rados_obj.get_pgid(pool_name=pool_name)
    log.info(f"The pg id is -{pg_id}")

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
        if (
            config.get("scenario") == "beginTime and endTime gt currentTime"
            or config.get("scenario") == "decreaseTime"
        ):
            (
                scrub_begin_hour,
                scrub_begin_weekday,
                scrub_end_hour,
                scrub_end_weekday,
            ) = rados_obj.add_begin_end_hours(2, 3)
            log.info(
                f'{"Setting scrub start is greater then end_hour and current time is greater than end hour"}'
            )
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
        rados_obj.set_osd_configuration("osd_scrub_begin_week_day", scrub_begin_weekday)
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
            rados_obj.set_osd_configuration("osd_scrub_end_week_day", scrub_end_weekday)
            rados_obj.set_osd_configuration("osd_scrub_begin_hour", scrub_begin_hour)
            rados_obj.set_osd_configuration("osd_scrub_end_hour", scrub_end_hour)

        try:
            scrub_status = rados_obj.start_check_scrub_complete(
                pg_id=pg_id[-1], user_initiated=False, wait_time=3600
            )
        except Exception as err:
            log.error(f"The error is-{err}")
            scrub_status = False

        if scrub_status is True and (
            config.get("scenario") == "default"
            or config.get("scenario") == "begin_end_time_equal"
            or config.get("scenario") == "beginTime gt endTime"
            or config.get("scenario") == "decreaseTime"
        ):
            log.info(f'{"Scrubbing validation is success"}')
            return 0

        try:
            deep_scrub_status = rados_obj.start_check_deep_scrub_complete(
                pg_id=pg_id[-1], user_initiated=False, wait_time=3600
            )
        except Exception as err:
            log.error(f"The error is-{err}")
            deep_scrub_status = False

        log.info(f" The deep-scrub execution status is -{deep_scrub_status}")

        if (
            config.get("scenario") == "beginTime gt endTime lt currentTime"
            or config.get("scenario") == "beginTime and endTime gt currentTime"
            or (
                config.get("scenario") == "unsetScrub"
                and (deep_scrub_status is False or scrub_status is False)
            )
        ):
            log.info(f'{"Scrubbing validation is success"}')
            return 0
        log.error(f'{"Scrubbing  validation failed"}')
        return 1
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
        method_should_succeed(rados_obj.delete_pool, pool_name)
        remove_parameter_configuration(mon_obj)
        rados_obj.set_osd_flags("unset", "noscrub")
        rados_obj.set_osd_flags("unset", "nodeep-scrub")
        time.sleep(10)
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1


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
