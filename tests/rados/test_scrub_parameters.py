"""
This method is used to verify the dump scrub.
"""

import datetime
import time
import traceback

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
    1. Test to the forced flag during the scrubbing and deep-scrubbing
    2. Test the scheduled time <=  osd_scrub_min_interval+
        (osd_scrub_min_interval * osd_scrub_interval_randomized_ratio)
    Returns:
        1 -> Fail, 0 -> Pass
    """

    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_object = RadosOrchestrator(node=cephadm)
    scrub_object = RadosScrubber(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_object)
    pool_type = ["replicated_pool", "ec_pool"]
    try:

        flag = wait_for_clean_pg_sets(rados_object)
        if not flag:
            log.error(
                "The cluster did not reach active + Clean state after add capacity"
            )
            return 1

        for val in ("scrub", "deep-scrub"):
            for pool in pool_type:
                pool_detail = config[pool]
                pool_name = pool_detail["pool_name"]
                selected_osd = create_pool_get_osd(rados_object, pool, pool_detail)
                if selected_osd is None:
                    log.error(
                        "The test case is failed due to the pool creation or cannot able to create data into pool"
                        " or cluster is not in active+clean state.The exact cause you can get "
                        "from the previous error messages"
                    )
                    return 1
                dump_scrub_before_scrub = scrub_object.get_dump_scrubs(selected_osd)
                # Get the PG ID
                pool_id = pool_obj.get_pool_id(pool_name=pool_name)
                if pool == "replicated_pool":
                    pg_id = f"{pool_id}.0"
                else:
                    ec_pg_id = f"{pool_id}.0"
                    for pg_no in dump_scrub_before_scrub:
                        if pg_no["pgid"].startswith(str(ec_pg_id)):
                            pg_id = pg_no["pgid"]
                            break
                log.info(f"The PG ID  is - {pg_id}")

                if val == "scrub":
                    rados_object.run_scrub(osd=selected_osd)
                else:
                    rados_object.run_deep_scrub(osd=selected_osd)

                status = is_forced_flag_set_true(scrub_object, selected_osd, pg_id)

                if not status:

                    log.info(
                        f"The {val}bing not started so as workaround performing the {val}bing on pg.Executing "
                        f"scrub/deep-scrub on PG.The waiting time is 20 minutes."
                    )
                    if val == "scrub":
                        rados_object.run_scrub(pgid=pg_id)
                    else:
                        rados_object.run_deep_scrub(pgid=pg_id)
                status = is_forced_flag_set_true(scrub_object, selected_osd, pg_id)
                if not status:
                    log.error(
                        f"After the {val} none of the pgid is set as true and the waiting time is 40 minutes"
                    )
                    return 1
                forced_flag = is_forced_flag_set_false(
                    scrub_object, selected_osd, pg_id
                )
                if not forced_flag:
                    log.error(
                        "The forced flag is not set to  True and the waiting time is 40 minutes"
                    )
                    return 1
                log.info(f"===Scenario-1: Completed by {val}bing the osd===")
                pg_dump_scrub = scrub_object.get_pg_dump_scrub(selected_osd, pg_id)
                log.info(f"The {pg_id} dumb scrub is - {pg_dump_scrub}")
                pg_current_sched_time = pg_dump_scrub["sched_time"]
                # Get the dump_scrub of the PG id
                scheduled_time = verify_scheduled_time(
                    mon_object=mon_obj,
                    prev_dump_scrub=dump_scrub_before_scrub,
                    pg_id=pg_id,
                    pg_current_sched_time=pg_current_sched_time,
                )
                if not scheduled_time:
                    log.error("The scrub time is greater than upper bound time")
                    return 1
                log.info(f"===Scenario-2:Completed by {val}bing the osd")
                log.info(f"=====Verification completed by {val}bing the OSD====")
                # deleted pool.This is due to execution time is taking more time until waiting for the cluster to clean
                # state
                method_should_succeed(rados_object.delete_pool, pool_name)
                time.sleep(10)
                rados_object.log_cluster_health()
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info("Execution of finally block")
        rados_object.configure_pg_autoscaler(default_mode="on")
        method_should_succeed(rados_object.delete_pool, pool_name)
        time.sleep(10)
        rados_object.log_cluster_health()
        # check for crashes after test execution
        if rados_object.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
    return 0


def create_pool_get_osd(rados_object, pool_type, pool_config):
    """
    Method is used -
        1. Create replicated/Erasure pool
        2. Push the data into pool
        3. From the active OSD list return a random OSD
    Args:
        rados_object: Rados object
        pool_type: type of pool.
        pool_config: pool configurations
    Returns:
        Primary OSD ID from an active OSD list or None if any issues faced during the pool creation
    """
    rados_object.configure_pg_autoscaler(default_mode="off")
    time.sleep(10)
    pool_name = pool_config["pool_name"]
    if pool_type == "replicated_pool":
        if not rados_object.create_pool(**pool_config):
            log.error("Failed to create the replicated Pool")
            return None
    else:
        if not rados_object.create_erasure_pool(name=pool_name, **pool_config):
            log.error("Failed to create the EC Pool")
            return None

    if not rados_object.bench_write(pool_name=pool_name, byte_size="5M", max_objs=100):
        log.error("Failed to write objects into Pool-1, with compression enabled")
        return None
    res = wait_for_clean_pg_sets(rados_object, test_pool=pool_name)
    if not res:
        log.error("PG's in cluster are not active + Clean ")
        return None
    osd_list = rados_object.get_pg_acting_set(pool_name=pool_name)
    return osd_list[0]


def is_forced_flag_set_true(scrub_object, osd_id, pg_id):
    """
    Method is used to check the forced flag status
    Args:
        rados_object: Rados object
        osd_id: OSD ID number
        pg_id : pg id
    Returns:
        True -> If forced flag is true
        False -> If forced flag is false
    """
    time_execution = datetime.datetime.now() + datetime.timedelta(minutes=20)
    while datetime.datetime.now() < time_execution:
        pg_dump_scrub = scrub_object.get_pg_dump_scrub(osd_id, pg_id)
        log.info(f"The pg dump scrub is -{pg_dump_scrub}")
        if pg_dump_scrub is None:
            log.error(f"The dump scrub of the {pg_id} is empty")
            return False
        if pg_dump_scrub["forced"] is True:
            log.info("The forced flag is set to True")
            return True
        log.info(
            f"The forced flag for the {pg_id} is True and scrubbing is in progress"
        )
        time.sleep(30)
    log.error("The forced flag is not set to True")
    return False


def is_forced_flag_set_false(scrub_object, osd_id, pg_id):
    """
    Method is used to wait for the forced flag status
    Args:
        scrub_object: Scrub object
        osd_id: OSD ID number
        pg_id: pg id number
    Returns:
        True -> If forced flag set to  false
        False -> If forced flag is true
    """
    time_execution = datetime.datetime.now() + datetime.timedelta(minutes=20)
    while datetime.datetime.now() < time_execution:
        pg_dump_scrub = scrub_object.get_pg_dump_scrub(osd_id, pg_id)
        log.info(f"The pg dump scrub is -{pg_dump_scrub}")
        if pg_dump_scrub is None:
            log.error(f"The dump scrub of the {pg_id} is empty")
            return False
        log.info(f"The {pg_id} dumb scrub is - {pg_dump_scrub}")
        if pg_dump_scrub["forced"] is False:
            log.info("The forced flag is set to False")
            return True
        log.info(
            f"The forced flag for the {pg_id} is True and scrubbing is in progress"
        )
        time.sleep(30)
    log.error("The forced flag is not set to False")
    return False


def verify_scheduled_time(mon_object, prev_dump_scrub, pg_id, pg_current_sched_time):
    """
    Method is used to verify the scheduled time is less than the  osd_scrub_min_interval+
    (osd_scrub_min_interval * osd_scrub_interval_randomized_ratio)
    Args:
        mon_object: Monitor object
        prev_dump_scrub: dumb scrub before scrub/deep-scrub operation
        pg_id: pg id number
        pg_current_sched_time: current scheduled time
    Returns:
        True ->  if scheduled time <=  osd_scrub_min_interval+
    (osd_scrub_min_interval * osd_scrub_interval_randomized_ratio)
        False -> if scheduled time >  osd_scrub_min_interval+
    (osd_scrub_min_interval * osd_scrub_interval_randomized_ratio)

    """
    pg_prev_sched_time = ""
    for pg_no in prev_dump_scrub:
        if pg_no["pgid"] == pg_id:
            pg_prev_sched_time = pg_no["sched_time"]
            break
    if not pg_prev_sched_time:
        log.error(f"Not able to the the {pg_id} previous scheduled time")
        return 1
    if pg_current_sched_time == pg_prev_sched_time:
        log.error("The scheduled time is not changed after the scrub")
        return 1
    prev_datetime = datetime.datetime.strptime(
        pg_prev_sched_time, "%Y-%m-%dT%H:%M:%S.%f%z"
    )
    current_datetime = datetime.datetime.strptime(
        pg_current_sched_time, "%Y-%m-%dT%H:%M:%S.%f%z"
    )

    log.info(
        "Add a random delay to osd_scrub_min_interval when scheduling the next scrub job for a PG. "
        "The delay is a random value less than osd_scrub_min_interval * osd_scrub_interval_randomized_ratio."
        "The default setting spreads scrubs throughout the allowed time window of "
        "[1, 1.5] * osd_scrub_min_interval."
    )
    scrub_min_interval = mon_object.get_config(
        section="osd", param="osd_scrub_min_interval"
    )
    log.info(f"The scrub_min_interval of the cluster is - {scrub_min_interval}")
    scrub_interval_rand_ratio = mon_object.get_config(
        section="osd", param="osd_scrub_interval_randomize_ratio"
    )
    log.info(
        f"The scrub_interval_rand_ratio of the cluster is -{scrub_interval_rand_ratio}"
    )
    adding_time_interval = float(scrub_min_interval) * float(scrub_interval_rand_ratio)
    log.info(
        f"The maximum time interval to adding to the previous time is -{adding_time_interval} "
    )
    upper_datetime = prev_datetime + datetime.timedelta(seconds=adding_time_interval)
    log.info(
        f"The maximum schedule time for stating scrub/deep-scrub is -{upper_datetime}"
    )
    if current_datetime > upper_datetime:
        return False
    return True
