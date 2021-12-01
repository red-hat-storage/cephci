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

    # Get the cluster time
    current_time = rados_obj.get_cluster_date()

    # Preparation of cofiguration parameter values from the current
    # cluster time
    try:
        yy, mm, dd, hh, day = current_time.split(":")
        date = datetime.datetime(int(yy), int(mm), int(dd), int(hh))
        date += datetime.timedelta(hours=1)
        scrub_begin_hour = date.hour
        date += datetime.timedelta(hours=1)
        scrub_end_hour = date.hour
        date += datetime.timedelta(days=1)
        scrub_begin_weekday = date.weekday()
        if scrub_end_hour == 0:
            date += datetime.timedelta(days=1)
            scrub_end_weekday = date.weekday()
        else:
            scrub_end_weekday = date.weekday()

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

            if scrub_status == 0:
                log.info(f'{"Scrubbing validation is success"}')
                return 0
            log.info(f'{"Scrubbing validation is in progress..."}')
            time.sleep(240)
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
