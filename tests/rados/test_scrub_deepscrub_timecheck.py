"""
BZ#2330755: Verification of  deep scrub taking too long under mclock I/O scheduler .
"""

import datetime
import time
import traceback

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
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
    ceph_nodes = kw.get("ceph_nodes")
    osd_list = []
    # Customer faced the issue with the low "osd_mclock_max_capacity_iops_hdd" values.The same values
    # are picked for the testing.
    mclock_max_capacity_values = ["2.985434", "0.198044", "0.198104"]

    try:
        pool_name = config["pool_name"]
        method_should_succeed(rados_object.create_pool, **config)
        rados_object.bench_write(pool_name=pool_name, max_objs=75000)
        # Get the OSD list
        for node in ceph_nodes:
            if node.role == "osd":
                node_osds = rados_object.collect_osd_daemon_ids(node)
                osd_list = osd_list + node_osds
            log.info(f"The OSDs in the cluster are-{osd_list}")

            # Get the osd_mclock_max_capacity_iops_hdd values for all OSDs
        for osd_id in osd_list:
            capacity_iops_value = mon_obj.show_config(
                daemon="osd", id=osd_id, param="osd_mclock_max_capacity_iops_hdd"
            )
            log.info(
                f"The osd-{osd_id} osd_mclock_max_capacity_iops_hdd value - {capacity_iops_value}"
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
        for osd_id, value in zip(acting_set, mclock_max_capacity_values):
            section_id = f"osd.{osd_id}"
            mon_obj.set_config(
                section=section_id,
                name="osd_mclock_max_capacity_iops_hdd",
                value=value,
            )
            time.sleep(5)
            rados_object.change_osd_state(action="restart", target=osd_id)
            capacity_iops_value = mon_obj.show_config(
                daemon="osd", id=osd_id, param="osd_mclock_max_capacity_iops_hdd"
            )
            log.info(f"The osd.{osd_id} value is set to the -{capacity_iops_value}")
        init_pool_pg_dump = rados_object.get_ceph_pg_dump(pg_id=pg_id)

        init_deep_scrub_stamp = datetime.datetime.strptime(
            init_pool_pg_dump["last_deep_scrub_stamp"], "%Y-%m-%dT%H:%M:%S.%f%z"
        )
        log.info(
            f"The deep scrub time stamp before deep-scrub -{init_deep_scrub_stamp}"
        )
        rados_object.run_deep_scrub(pgid=pg_id)
        # wait for deep-scrub to complete in  40 minutes
        deep_scrub_time = 40
        if is_deep_scrub_complete(
            rados_object, pg_id, init_deep_scrub_stamp, deep_scrub_time
        ):
            log.error(
                f"Cannot able to reproduce the issue after waiting the {deep_scrub_time} time"
            )
            return 1
        log.info("The bug is reproduced and verifying the bug fix")

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
                f"The osd.{osd_id} value after removing the parameter -{capacity_iops_value}"
            )
            # Verification of the osd_mclock_max_capacity_iops_hdd values
            if float(capacity_iops_value) < 50 or float(capacity_iops_value) > 500:
                log.error(
                    f"The osd_mclock_max_capacity_iops_hdd parameter value for the osd.{osd_id} is "
                    f"{capacity_iops_value}. The value range is in between 50 and 500"
                )
                return 1
            time.sleep(60)

        init_pool_pg_dump = rados_object.get_ceph_pg_dump(pg_id=pg_id)
        init_deep_scrub_stamp = datetime.datetime.strptime(
            init_pool_pg_dump["last_deep_scrub_stamp"], "%Y-%m-%dT%H:%M:%S.%f%z"
        )
        deep_scrub_time = 20
        rados_object.run_deep_scrub(pgid=pg_id)
        deep_result = is_deep_scrub_complete(
            rados_object, pg_id, init_deep_scrub_stamp, deep_scrub_time
        )
        if not deep_result:
            log.error(f"The deep-scrub not completed within {deep_scrub_time} minutes")
            return 1
        log.info(
            f"The deep-scrub completed within {deep_scrub_time} and osd_mclock_max_capacity_iops_hdd value is "
            f"in between 50 and 500.The verification of bug fix is completed "
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
    return 0


def is_deep_scrub_complete(rados_object, pg_id, old_deep_scrub_stamp, wait_time):

    end_time = datetime.datetime.now() + datetime.timedelta(minutes=wait_time)
    while end_time > datetime.datetime.now():
        init_pool_pg_dump = rados_object.get_ceph_pg_dump(pg_id=pg_id)
        current_deep_scrub_stamp = datetime.datetime.strptime(
            init_pool_pg_dump["last_deep_scrub_stamp"], "%Y-%m-%dT%H:%M:%S.%f%z"
        )
        if current_deep_scrub_stamp > old_deep_scrub_stamp:
            log.info(f"The current deep-scrub time stamp is-{current_deep_scrub_stamp}")
            deep_scrub_difference = current_deep_scrub_stamp - old_deep_scrub_stamp
            log.info(
                f"The deep-scrub operation took -{deep_scrub_difference} time to complete the operation"
            )
            return True
        time.sleep(40)
        log.info("The deep-scrub is in progress")
    return False
