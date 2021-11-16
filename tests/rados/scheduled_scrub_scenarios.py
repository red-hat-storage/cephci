"""
   This test module execute the scheduled scrub scenarios.
   This module cover the followingg scenario-
      
      Setting the time parameter for scrubbing and check whether 
           scrubbing will be scheduled according to the time: 
              Begin hour < current time<End hour 


   Entry Point:
         def run(ceph_cluster,**kw):
"""



import logging
import datetime

from ceph.ceph_admin import CephAdmin
from ceph.rados. rados_scrub import RadosScrubber 
import pdb
import time

log = logging.getLogger(__name__)

def run(ceph_cluster, **kw):
 
    OSD_SCRUB_MIN_INTERVAL = 1800
    OSD_SCRUB_MAX_INTERVAL = 3600
    OSD_DEEP_SCRUB_INTERVAL=3600

    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosScrubber(node=cephadm)
   
    # Storing the pg dump log before setting the scrub parameters
    before_scrub_log=rados_obj.get_pg_dump("pgid","last_scrub_stamp")
   
    #Get the cluster time
    current_time=rados_obj.get_cluster_date()

    #Preparation of cofiguration parameter values from the current 
    # cluster time
    yy,mm,dd,hh,day=current_time.split(':')
    date = datetime.datetime(int(yy),int(mm),int(dd),int(hh))
    date += datetime.timedelta(hours=1)
    scrub_begin_hour= date.hour
    date += datetime.timedelta(hours=1)
    scrub_end_hour= date.hour
    date += datetime.timedelta(days=1)
    scrub_begin_weekday= date.weekday()
    if scrub_end_hour == 0:
        date += datetime.timedelta(days=1)
        scrub_end_weekday = date.weekday()
    else:
        scrub_end_weekday = date.weekday()
   
    # Setting the scrub parameters
    rados_obj.set_osd_configuration("osd_scrub_min_interval",OSD_SCRUB_MIN_INTERVAL)
    rados_obj.set_osd_configuration("osd_scrub_max_interval",OSD_SCRUB_MAX_INTERVAL)
    rados_obj.set_osd_configuration("osd_deep_scrub_interval",OSD_DEEP_SCRUB_INTERVAL)
    rados_obj.set_osd_configuration("osd_scrub_begin_week_day",scrub_begin_weekday)
    rados_obj.set_osd_configuration("osd_scrub_end_week_day",scrub_end_weekday)
    rados_obj.set_osd_configuration("osd_scrub_begin_hour",scrub_begin_hour)
    rados_obj.set_osd_configuration("osd_scrub_end_hour",scrub_end_hour)

    #after_scrub_log=rados_obj.get_pg_dump("pgid","last_scrub_stamp")
    #print ("The scrub log before command execution is ::", after_scrub_log)  
    #rados_obj.verify_scrub(before_scrub_log,after_scrub_log)    

    # Scheduled scrub verification
    endTime = datetime.datetime.now() + datetime.timedelta(minutes=60)
    while True:
        if datetime.datetime.now() >= endTime:
            break
        else:
            after_scrub_log=rados_obj.get_pg_dump("pgid","last_scrub_stamp")
            scrub_status=rados_obj.verify_scrub(before_scrub_log,after_scrub_log)

            if scrub_status == 0:
                return 0
            log.info(f"Scrubbing validation is in progress...")
            time.sleep(240)
    log.info(f"Scrubbing failed")        
    return 1



