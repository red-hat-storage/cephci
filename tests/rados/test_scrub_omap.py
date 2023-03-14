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

    global rados_obj
    global pool_obj
    global omap_target_configs
    global pool_target_configs

    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)

    pool_target_configs = config["verify_osd_omap_entries"]["configurations"]
    omap_target_configs = config["verify_osd_omap_entries"]["omap_config"]

    # Creating pools and starting the test
    for entry in pool_target_configs.values():
        log.debug(
            f"Creating {entry['pool_type']} pool on the cluster with name {entry['pool_name']}"
        )
        if entry.get("pool_type", "replicated") == "erasure":
            method_should_succeed(
                rados_obj.create_erasure_pool, name=entry["pool_name"], **entry
            )
        else:
            method_should_succeed(
                rados_obj.create_pool,
                **entry,
            )

        log.debug(
            "Created the pool. beginning to create large number of omap entries on the pool"
        )
    omap_thread = Thread(target=create_omap)

    # verification of omap for 30 minutes
    verification_osd_thread = Thread(target=verification_osd, args=[30])
    omap_thread.daemon = True
    omap_thread.start()
    verification_osd_thread.start()
    verification_osd_thread.join()
    omap_thread._delete()
    if result == 0:
        return 0
    else:
        return 1


def create_omap():
    """
    Used to create a omap entries.
    Returns: 1 if the method failed to create omap.

    """
    for entry in pool_target_configs.values():
        if not pool_obj.fill_omap_entries(
            pool_name=entry["pool_name"], **omap_target_configs
        ):
            log.error(f"Omap entries not generated on pool {entry['pool_name']}")
            return 1


def verification_osd(test_time):
    """
    This method is used to verify the OSD status while performing the scrub,deep-scrub and creating omaps
    Args:
            test_time: Used to set the test time

    """
    # The result parameter is used to return value in the thread
    global result
    result = 0
    # performing scrub and deep-scrub
    rados_obj.run_scrub()
    rados_obj.run_deep_scrub()
    osd_stats = rados_obj.get_osd_stat()
    time_execution = datetime.datetime.now() + datetime.timedelta(minutes=test_time)

    while datetime.datetime.now() < time_execution:
        time.sleep(3)
        total_osds = osd_stats["num_osds"]
        log.info(f"The total number of osd are:{total_osds}")
        no_osd_up = osd_stats["num_up_osds"]
        log.info(f"The total number of osd up are:{no_osd_up}")
        if total_osds != no_osd_up:
            log.info("The total osd are not equal to up OSDs")
            result = 1
            break
    log.info("The total osd are  equal to up OSDs")
