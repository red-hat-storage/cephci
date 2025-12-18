"""
BZ#2330755: Verification of  deep scrub taking too long under mclock I/O scheduler .
"""

import datetime
import time
import traceback

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from ceph.rados.utils import get_cluster_timestamp
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    # CEPH-83605026
    Bugzilla tracker:
    Reef - 2330755
    Squid - 2292517
    Bug Verfication steps:
    1. Create a replicate pool with single PG
    2. Push the data into the pool
    3. Identify the active OSDs of the pool
    4. Set the osd_mclock_max_capacity_iops_hdd with less values
    5. Start deep-scrub and check the time.The wait time in the script is 60 minutes
    6. Set the osd_mclock_force_run_benchmark_on_init to true
    7. Remove the osd_mclock_max_capacity_iops_hdd value and restart the OSDs
    8. Check the deep-scrub completed in less time
    Returns:
        1 -> Fail, 0 -> Pass
    """

    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_object = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_object)
    pool_obj = PoolFunctions(node=cephadm)
    # Customer faced the issue with the low "osd_mclock_max_capacity_iops_hdd" values.The same values
    # are picked for the testing.
    # mclock_max_capacity_values = ["2.985434", "0.198044", "0.198104"]
    start_time = get_cluster_timestamp(rados_object.node)
    log.debug(f"Test workflow started. Start time: {start_time}")
    try:
        pool_name = config["pool_name"]
        method_should_succeed(rados_object.create_pool, **config)
        rados_object.bench_write(pool_name=pool_name, max_objs=75000)
        # Get the OSD list
        osd_list = rados_object.get_osd_list(status="up")
        log.info(f"The OSDs in the cluster are-{osd_list}")

        # Get the osd_mclock_max_capacity_iops_hdd values for all OSDs
        for osd_id in osd_list:
            capacity_iops_value = mon_obj.show_config(
                daemon="osd", id=osd_id, param="osd_mclock_max_capacity_iops_hdd"
            )
            log.info(
                f"The osd.{osd_id} osd_mclock_max_capacity_iops_hdd value: {capacity_iops_value}"
            )
        pool_id = pool_obj.get_pool_id(pool_name=pool_name)
        log.info(f"The {pool_name} pool id is -{pool_id}")
        pg_id = f"{pool_id}.0"
        # Get the pg acting set
        acting_set = rados_object.get_pg_acting_set(pool_name=pool_name)
        log.info(f"The PG acting set is -{acting_set}")
        log.info(
            "Customer faced the deep-scrub issue when the osd_mclock_max_capacity_iops_hdd was low."
            "For verification setting the acting set OSDs with the low values "
        )

        # perform deep-scrub with default value of osd_mclock_max_capacity_iops_hdd
        init_pool_pg_dump = rados_object.get_ceph_pg_dump(pg_id=pg_id)

        init_deep_scrub_stamp = datetime.datetime.strptime(
            init_pool_pg_dump["last_deep_scrub_stamp"], "%Y-%m-%dT%H:%M:%S.%f%z"
        )
        log.info(f"The deep scrub time stamp at the beginning: {init_deep_scrub_stamp}")
        rados_object.run_deep_scrub(pgid=pg_id)
        scrub_wait_time = 30
        status, init_scrub_time = is_deep_scrub_complete(
            rados_object, pg_id, init_deep_scrub_stamp, scrub_wait_time
        )

        if not status:
            err_msg = (
                f"Unable to complete deep-scrub on PG {pg_id} within {scrub_wait_time}"
            )
            log.error(err_msg)
            raise Exception(err_msg)

        time.sleep(10)
        for osd_id in acting_set:
            section_id = f"osd.{osd_id}"
            init_capacity_iops = mon_obj.show_config(
                daemon="osd", id=osd_id, param="osd_mclock_max_capacity_iops_hdd"
            ).strip()
            mon_obj.set_config(
                section=section_id,
                name="osd_mclock_max_capacity_iops_hdd",
                value=float(init_capacity_iops) / 2,
            )
            time.sleep(5)
            rados_object.change_osd_state(action="restart", target=osd_id)
            capacity_iops_value = mon_obj.show_config(
                daemon="osd", id=osd_id, param="osd_mclock_max_capacity_iops_hdd"
            )
            log.info(
                f"The osd.{osd_id} osd_mclock_max_capacity_iops_hdd value is set to: {capacity_iops_value}"
            )
        init_pool_pg_dump = rados_object.get_ceph_pg_dump(pg_id=pg_id)

        init_deep_scrub_stamp = datetime.datetime.strptime(
            init_pool_pg_dump["last_deep_scrub_stamp"], "%Y-%m-%dT%H:%M:%S.%f%z"
        )
        log.info(
            f"The deep scrub time stamp before deep-scrub: {init_deep_scrub_stamp}"
        )
        rados_object.run_deep_scrub(pgid=pg_id)
        # wait for deep-scrub to complete in twice the original scrub duration + 10 minutes
        scrub_wait_time = (int(init_scrub_time) * 2) + 10
        status, upd_scrub_time = is_deep_scrub_complete(
            rados_object, pg_id, init_deep_scrub_stamp, scrub_wait_time
        )
        if not status:
            err_msg = (
                f"Unable to complete deep-scrub on PG {pg_id} within {scrub_wait_time}"
            )
            log.error(err_msg)
            raise Exception(err_msg)
        log.info(
            "Deep scrub time with default osd_mclock_max_capacity_iops_hdd: "
            + str(init_scrub_time)
        )
        log.info(
            "Deep scrub time with half of default osd_mclock_max_capacity_iops_hdd: "
            + str(upd_scrub_time)
        )

        mon_obj.set_config(
            section="osd",
            name="osd_mclock_force_run_benchmark_on_init",
            value="true",
        )
        for osd_id in acting_set:
            section_id = f"osd.{osd_id}"
            mon_obj.remove_config(
                section=section_id, name="osd_mclock_max_capacity_iops_hdd"
            )
            rados_object.change_osd_state(action="restart", target=osd_id)
            time.sleep(5)
            # Verification of the bug
            capacity_iops_value = mon_obj.show_config(
                daemon="osd", id=osd_id, param="osd_mclock_max_capacity_iops_hdd"
            )
            log.info(
                f"The osd.{osd_id} value after removing the parameter: {capacity_iops_value}"
            )
            # Verification of the osd_mclock_max_capacity_iops_hdd values
            if float(capacity_iops_value) < 50 or float(capacity_iops_value) > 500:
                log.error(
                    f"The osd_mclock_max_capacity_iops_hdd parameter value for the osd.{osd_id} is "
                    f"{capacity_iops_value}. Should lie between 50 and 500"
                )
                return 1
            time.sleep(60)

        init_pool_pg_dump = rados_object.get_ceph_pg_dump(pg_id=pg_id)
        init_deep_scrub_stamp = datetime.datetime.strptime(
            init_pool_pg_dump["last_deep_scrub_stamp"], "%Y-%m-%dT%H:%M:%S.%f%z"
        )
        scrub_wait_time = int(init_scrub_time / 60) + 10
        rados_object.run_deep_scrub(pgid=pg_id)
        status, final_scrub_time = is_deep_scrub_complete(
            rados_object, pg_id, init_deep_scrub_stamp, scrub_wait_time
        )
        if not status:
            log.error("deep-scrub not completed within %s minutes" % scrub_wait_time)
            return 1
        log.info(
            "Deep scrub for PG %s took %s time to complete" % (pg_id, final_scrub_time)
        )
        log.info(
            "Deep scrub time with default osd_mclock_max_capacity_iops_hdd: "
            + str(init_scrub_time)
        )
        log.info(
            "Deep scrub time with half of default osd_mclock_max_capacity_iops_hdd: "
            + str(upd_scrub_time)
        )
        log.info(
            "Deep scrub time after resetting osd_mclock_max_capacity_iops_hdd: "
            + str(final_scrub_time)
        )

        # As after OSD restart, the osd_mclock_max_capacity_iops_hdd should get restored to
        # previous default value, it is expected that second round of deep scrub time
        # with restored values remains within 10% deviation
        if float(final_scrub_time) > int(init_scrub_time) * 1.1:
            err_msg = (
                f"Time taken to Deep scrub PG {pg_id} after resetting osd_mclock_max_capacity_iops_hdd"
                f" is more than 10% of initial deep scrub duration. "
                f"Initial: {init_scrub_time} | Final: {final_scrub_time}"
            )
            log.error(err_msg)
            raise Exception(err_msg)

        log.info(
            "The deep-scrub completed within %s and osd_mclock_max_capacity_iops_hdd value is "
            "in between 50 and 500. The verification of bug fix is completed"
            % final_scrub_time
        )
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info(
            "\n\n================ Execution of finally block =======================\n\n"
        )

        method_should_succeed(rados_object.delete_pool, pool_name)
        mon_obj.remove_config(
            section="osd", name="osd_mclock_force_run_benchmark_on_init"
        )
        for osd_id in acting_set:
            section_id = f"osd.{osd_id}"
            mon_obj.remove_config(
                section=section_id, name="osd_mclock_max_capacity_iops_hdd"
            )
            rados_object.change_osd_state(action="restart", target=osd_id)
        time.sleep(20)

        rados_object.log_cluster_health()
        test_end_time = get_cluster_timestamp(rados_object.node)
        log.debug(
            f"Test workflow completed. Start time: {start_time}, End time: {test_end_time}"
        )
        if rados_object.check_crash_status(
            start_time=start_time, end_time=test_end_time
        ):
            log.error("Test failed due to crash at the end of test")
            return 1
    return 0


def is_deep_scrub_complete(rados_object, pg_id, old_deep_scrub_stamp, wait_time):
    scrub_duration = 0
    end_time = datetime.datetime.now() + datetime.timedelta(minutes=wait_time)
    while end_time > datetime.datetime.now():
        init_pool_pg_dump = rados_object.get_ceph_pg_dump(pg_id=pg_id)
        current_deep_scrub_stamp = datetime.datetime.strptime(
            init_pool_pg_dump["last_deep_scrub_stamp"], "%Y-%m-%dT%H:%M:%S.%f%z"
        )
        if current_deep_scrub_stamp > old_deep_scrub_stamp:
            log.info(
                f"The current deep-scrub time stamp is: {current_deep_scrub_stamp}"
            )
            # deep_scrub_difference = current_deep_scrub_stamp - old_deep_scrub_stamp
            scrub_duration = init_pool_pg_dump["last_scrub_duration"]
            log.info(
                f"The deep-scrub operation took {scrub_duration} time to complete the operation"
            )
            return True, scrub_duration
        time.sleep(40)
        log.info("The deep-scrub is in progress")
    return False, scrub_duration
