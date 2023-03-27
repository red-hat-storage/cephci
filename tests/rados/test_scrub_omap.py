"""
This file contains the  methods to verify the  OSD status during large omap entries.
AS part of verification the script  perform the following tasks-
   1. Creating omaps
   2. Perform scrub and deepscrub
   3. While creating objects check that osds status
"""


import datetime
import time
from threading import Thread

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from ceph.rados.rados_scrub import RadosScrubber
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test to create a large number of omap entries on the single PG pool and test osd resiliency
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]

    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    scrub_obj = RadosScrubber(node=cephadm)

    pool_target_configs = config["verify_osd_omap_entries"]["configurations"]
    omap_target_configs = config["verify_osd_omap_entries"]["omap_config"]

    # Creating pools and starting the test
    for entry in pool_target_configs.values():
        log.debug(
            f"Creating {entry['pool_type']} pool on the cluster with name {entry['pool_name']}"
        )
    method_should_succeed(
        rados_obj.create_pool,
        **entry,
    )

    log.debug(
        "Created the pool. beginning to create large number of omap entries on the pool"
    )

    # Creating omaps
    create_omap(pool_obj, pool_target_configs, omap_target_configs)

    omap_thread = Thread(
        target=create_omap, args=[pool_obj, pool_target_configs, omap_target_configs]
    )

    # Get the scrub and deep scrup time stamp before scrub and deep scrub execution
    before_scrub_log = scrub_obj.get_pg_dump("pgid", "last_scrub_stamp")
    before_deep_scrub_log = scrub_obj.get_pg_dump("pgid", "last_deep_scrub_stamp")

    # performing scrub and deep-scrub
    rados_obj.run_scrub()
    rados_obj.run_deep_scrub()
    scrub_status = 1
    deep_scrub_status = 1

    while scrub_status == 1 and deep_scrub_status == 1:
        # Scrub check for every 20 seconds
        time.sleep(20)
        after_scrub_log = scrub_obj.get_pg_dump("pgid", "last_scrub_stamp")
        after_deep_scrub_log = scrub_obj.get_pg_dump("pgid", "last_deep_scrub_stamp")
        scrub_status = scrub_obj.verify_scrub_deepscrub(
            before_scrub_log, after_scrub_log, "scrub"
        )
        deep_scrub_status = scrub_obj.verify_scrub_deepscrub(
            before_deep_scrub_log, after_deep_scrub_log, "deepscrub"
        )
        log.info("scrubbing and deep-scrubbing are in progress")
    log.info("scrubbing and deep-scrubbing completed")

    # Check for large omap massage
    ceph_status = rados_obj.run_ceph_command(cmd=" ceph health")
    if "LARGE_OMAP_OBJECTS" in ceph_status["checks"]:
        log.info(" Generated large omaps in the cluster")
    else:
        log.error(" Unable to generate the large omaps in the cluster")
        return 1

    # verification of OSDs for 30 minutes
    verification_osd_thread = Thread(target=verification_osd, args=[rados_obj, 30])
    omap_thread.daemon = True
    omap_thread.start()
    verification_osd_thread.start()
    verification_osd_thread.join()
    omap_thread._delete()
    if result == 0:
        return 0
    else:
        return 1


def create_omap(pool_fun, pool_configs, omap_configs):
    """
    Used to create a omap entries.
    Args:
        pool_fun : PoolFunctions object imported from  pool_workflows
        pool_configs:  Pool configuration details
        omap_target_configs: omap configurations

    Returns: 1 if the method failed to create omap.

    """

    for entry in pool_configs.values():
        if not pool_fun.fill_omap_entries(pool_name=entry["pool_name"], **omap_configs):
            log.error(f"Omap entries not generated on pool {entry['pool_name']}")
            return 1


def verification_osd(rados_object, test_time):
    """
    This method is used to verify the OSD status while performing the scrub,deep-scrub and creating omaps
    Args:
        rados_object : RadosOrchestrator object
        test_time: Used to set the test time

    """
    # The result parameter is used to return value in the thread
    global result
    result = 0

    osd_stats = rados_object.get_osd_stat()
    time_execution = datetime.datetime.now() + datetime.timedelta(minutes=test_time)

    while datetime.datetime.now() < time_execution:
        time.sleep(10)
        total_osds = osd_stats["num_osds"]
        log.info(f"The total number of osd are:{total_osds}")
        no_osd_up = osd_stats["num_up_osds"]
        log.info(f"The total number of osd up are:{no_osd_up}")
        if total_osds != no_osd_up:
            log.error("The total osd are not equal to up OSDs")
            result = 1
            break
        osd_stats = rados_object.get_osd_stat()
    log.info("The total osd are  equal to up OSDs")
