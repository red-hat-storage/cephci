"""
   This module contains the methods required for scrubbing.
   1.To set the parameters for scrubbing initially required the
     cluster time and day details.get_cluster_date method provides
     the details.
   2.set_osd_configuration method  used to set the configuration
     parameters on the cluster.
   3.get_osd_configuration  method is used to get the configured parameters
     on the cluster.
     NOTE: With set_osd_configuration & get_osd_configuration methods can
          use to set the get the any OSD configuration parameters.
   4. get_pg_dump  method is used to get the pg dump details from the cluster.
   5. verify_scrub  method used for the verification of scheduled scrub.
"""

import datetime
import logging
import os
import sys
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.rados_scrub import RadosScrubber

log = logging.getLogger(__name__)


def set_default_params(rados_obj):
    """
    Used to set the default osd scrub parameter value
    Args:
        rados_obj: Rados object

    Returns : None
    """
    rados_obj.set_osd_configuration("osd_scrub_min_interval", 86400)
    rados_obj.set_osd_configuration("osd_scrub_max_interval", 604800)
    rados_obj.set_osd_configuration("osd_deep_scrub_interval", 604800)
    rados_obj.set_osd_configuration("osd_scrub_begin_week_day", 0)
    rados_obj.set_osd_configuration("osd_scrub_end_week_day", 0)
    rados_obj.set_osd_configuration("osd_scrub_begin_hour", 0)
    rados_obj.set_osd_configuration("osd_scrub_end_hour", 0)
    time.sleep(10)


def run(ceph_cluster, **kw):

    osd_scrub_min_interval = 1800
    osd_scrub_max_interval = 3600
    osd_deep_scrub_interval = 3600

    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosScrubber(node=cephadm)

    # Storing the pg dump log before setting the scrub parameters
    before_scrub_log = rados_obj.get_pg_dump("pgid", "last_scrub_stamp")

    # Preparation of cofiguration parameter values from the current
    # cluster time
    try:

        (
            scrub_begin_hour,
            scrub_begin_weekday,
            scrub_end_hour,
            scrub_end_weekday,
        ) = rados_obj.add_begin_end_hours(1, 1)

        # Scenario to verify that scrub start and end hours are same
        # CEPH-9362
        if config.get("scenario") == "begin_end_time_equal":
            log.info(f'{"Setting scrub start and end hour same"}')
            scrub_end_hour = scrub_begin_hour

        # Begin time is greater than end hour  and current time less than end hour
        # CEPH-9363,CEPH-9364
        if config.get("scenario") == "beginTime gt endTime":
            log.info(
                f'{"Setting scrub start time is greater than end hour and current time less than end hour"}'
            )
            scrub_end_hour, scrub_begin_hour = scrub_begin_hour, scrub_end_hour

        # Time schedules scrubbing: set begin_hour > end_hour and current_time > begin_hour
        # Test case should fail,CEPH-9365
        if config.get("scenario") == "currentTime gt beginTime gt endTime":
            (
                scrub_begin_hour,
                scrub_begin_weekday,
                scrub_end_hour,
                scrub_end_weekday,
            ) = rados_obj.add_begin_end_hours(0, -1)
            time.sleep(60)
            log.info(
                f'{"Setting scrub start is greater then end_hour and current time is greater than begin hour"}'
            )

        # set begin_hour > end_hour and current_time > end_hour
        # Test case should fail,CEPH-9366
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

        # Scheduled scrub verification
        endTime = datetime.datetime.now() + datetime.timedelta(minutes=60)
        while datetime.datetime.now() <= endTime:
            after_scrub_log = rados_obj.get_pg_dump("pgid", "last_scrub_stamp")
            scrub_status = rados_obj.verify_scrub(before_scrub_log, after_scrub_log)

            if scrub_status == 0 and (
                config.get("scenario") == "default"
                or config.get("scenario") == "begin_end_time_equal"
                or config.get("scenario") == "beginTime gt endTime"
                or config.get("scenario") == "currentTime gt beginTime gt endTime"
            ):
                log.info(f'{"Scrubbing validation is success"}')
                return 0
            log.info(f'{"Scrubbing validation is in progress..."}')
            time.sleep(240)
        if config.get("scenario") == "beginTime gt endTime lt currentTime":
            log.info(f'{"Scrubbing validation is success"}')
            return 0
        log.info(f'{"Scrubbing failed"}')
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
        set_default_params(rados_obj)
