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

    try:
        # Creating pools and starting the test
        for entry in pool_target_configs.values():
            log.debug(
                f"Creating {entry['pool_type']} pool on the cluster with name {entry['pool_name']}"
            )
            method_should_succeed(
                rados_obj.create_pool,
                **entry,
            )

            log.info(
                f"Created the pool {entry['pool_name']}. beginning to create large number of omap entries on the pool"
            )

        # Creating omaps
        create_omap(pool_obj, pool_target_configs, omap_target_configs)

        # Check for large omap warning
        if not pool_obj.check_large_omap_warning(
            pool=entry["pool_name"],
            obj_num=omap_target_configs["obj_end"],
            obj_check=False,
        ):
            log.error("Failed to generate large omap warning on the cluster")
            return 1
        log.info("Generated Large OMAP warning on the cluster")

        scrub_thread = Thread(
            target=perform_scrubbing, args=[rados_obj, scrub_obj, pool_target_configs]
        )

        # verification of OSDs for 30 minutes
        verification_osd_thread = Thread(target=verification_osd, args=[rados_obj, 30])
        scrub_thread.daemon = True
        log.info("Starting scrubbing thread")
        scrub_thread.start()
        log.info("Starting OSD state verification thread")
        verification_osd_thread.start()
        verification_osd_thread.join()
        scrub_thread.join()

    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1

    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        # removal of rados pools
        rados_obj.rados_pool_cleanup()

        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
    if result == 0:
        return 0
    else:
        return 1


def perform_scrubbing(rados_obj, scrub_obj, pool_configs):
    for entry in pool_configs.values():
        # Get the scrub and deep scrub time stamp before scrub and deep scrub execution
        pool_pgid = rados_obj.get_pgid(pool_name=entry["pool_name"])[0]
        pool_pg_dump = rados_obj.get_ceph_pg_dump(pg_id=pool_pgid)
        before_scrub_log = pool_pg_dump["last_scrub_stamp"]
        before_deep_scrub_log = pool_pg_dump["last_deep_scrub_stamp"]

        log.info(f"last_scrub: {pool_pg_dump['last_scrub']}")
        log.info(f"last_scrub_stamp: {pool_pg_dump['last_scrub_stamp']}")
        log.info(f"last_deep_scrub: {pool_pg_dump['last_deep_scrub']}")
        log.info(f"last_deep_scrub_stamp: {pool_pg_dump['last_deep_scrub_stamp']}")

        # performing scrub and deep-scrub
        rados_obj.run_scrub(pool=entry["pool_name"])
        rados_obj.run_deep_scrub(pool=entry["pool_name"])
        scrub_status = 1
        deep_scrub_status = 1
        log.info(f"Scrubbing has been triggered for the pool {entry['pool_name']}")

        while scrub_status == 1 and deep_scrub_status == 1:
            # Scrub check for every 30 seconds
            time.sleep(30)
            pool_pg_dump = rados_obj.get_ceph_pg_dump(pg_id=pool_pgid)
            after_scrub_log = pool_pg_dump["last_scrub_stamp"]
            after_deep_scrub_log = pool_pg_dump["last_deep_scrub_stamp"]
            scrub_status = scrub_obj.verify_scrub_deepscrub(
                before_scrub_log, after_scrub_log, "scrub"
            )
            deep_scrub_status = scrub_obj.verify_scrub_deepscrub(
                before_deep_scrub_log, after_deep_scrub_log, "deepscrub"
            )
            log.info("scrubbing and deep-scrubbing are in progress")
        log.info("scrubbing and deep-scrubbing completed")


def create_omap(pool_fun, pool_configs, omap_configs):
    """
    Used to create omap entries.
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
        time.sleep(30)
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
