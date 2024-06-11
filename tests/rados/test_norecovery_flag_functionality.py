"""
This file contains the  methods to verify the  norecover flag functionality.
Bug- https://bugzilla.redhat.com/show_bug.cgi?id=2134786
1.Checking that norecover falg  should not perform any autoscale on PG
2.After setting the autorecover checking that the recovery is progress on the cluster
"""

import datetime
import time
import traceback

from ceph.ceph_admin import CephAdmin
from ceph.rados import utils
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
     Test to Verify the norecover flag functionality.
    Returns:
        1 -> Fail, 0 -> Pass
    """

    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    config = kw["config"]

    replicated_config = config.get("replicated_pool")
    pool_name = replicated_config["pool_name"]

    try:
        log.info(
            "Scenario1: Verify that the pg_num should not  increase after setting the norecover flag"
        )
        method_should_succeed(wait_for_clean_pg_sets, rados_obj)
        utils.configure_osd_flag(ceph_cluster, "set", "norecover")
        log.info("Scenario1:The norecover is set on the cluster")
        if not rados_obj.create_pool(pool_name=pool_name, pg_num=8):
            log.error("Failed to create the  Pool")
            return 1
        log.info(f"Scenario1:The pool-{pool_name} is created")
        old_pg_num = pool_obj.get_pg_autoscaler_value(pool_name, "pg_num_target")
        log.info(f"The current pg number is -{old_pg_num}")
        rados_obj.bench_write(
            pool_name=pool_name, byte_size=1024, rados_write_duration=10
        )
        status = check_status(pool_obj, old_pg_num, pool_name)
        pg_num = pool_obj.get_pg_autoscaler_value(pool_name, "pg_num_target")
        if not status:
            log.error(
                f"Scenario1:The pg number is increased to after setting the norecover flag -{pg_num}"
            )
            return 1
        log.info(
            f"Scenario1:The pg number is not increased after setting the norecover flag.The pg_num is - {pg_num}"
        )
        # unset the norecover flag
        utils.configure_osd_flag(ceph_cluster, "unset", "norecover")
        log.info("Scenario1:The norecover is unset on the cluster")
        status = check_status(pool_obj, old_pg_num, pool_name)
        pg_num = pool_obj.get_pg_autoscaler_value(pool_name, "pg_num_target")
        if status:
            log.error(
                f"Scenario1:The pg number is not increased to after unsetting the norecover flag."
                f"The pg number is -{pg_num}"
            )
            return 1
        log.info(
            f"Scenario1:The pg number is increased after unsetting the norecover flag."
            f"The pg number is -{pg_num}"
        )
        delete_and_healthCheck(rados_obj, pool_name)

        log.info(
            "Scenario2: Verify that the pg_num should not decrease after setting the norecover flag"
        )
        if not rados_obj.create_pool(pool_name=pool_name, bulk="bulk"):
            log.error("Failed to create the  Pool")
            return 1
        endtime = datetime.datetime.now() + datetime.timedelta(seconds=180)
        while datetime.datetime.now() < endtime:
            target_pg_num = pool_obj.get_pg_autoscaler_value(pool_name, "pg_num_target")
            final_pg_num = pool_obj.get_pg_autoscaler_value(pool_name, "pg_num_final")
            if final_pg_num == target_pg_num:
                log.info(
                    f"Scenario2:-The pool pg num is increased to the - {final_pg_num}"
                )
                break
        old_pg_num = pool_obj.get_pg_autoscaler_value(pool_name, "pg_num_target")
        log.info(f"Scenario2:The current pg number is -{old_pg_num}")
        rados_obj.bench_write(
            pool_name=pool_name, byte_size=1024, rados_write_duration=10
        )
        status = check_status(pool_obj, old_pg_num, pool_name)
        pg_num = pool_obj.get_pg_autoscaler_value(pool_name, "pg_num_target")
        if not status:
            log.error(
                f"Scenario2:The pg number is decreased to after setting the norecover flag "
                f"-{pg_num}"
            )
            return 1
        log.info(
            f"Scenario2:The pg number is not decreased after setting the norecover flag.The pg_num is-{pg_num}"
        )
        utils.configure_osd_flag(ceph_cluster, "unset", "norecover")
        log.info("The norecover is unset on the cluster")
        status = check_status(pool_obj, pg_num, pool_name)
        pg_num = pool_obj.get_pg_autoscaler_value(pool_name, "pg_num_target")
        if status:
            expected_pg_num = pool_obj.get_pg_autoscaler_value(
                pool_name, "pg_num_final"
            )
            log.info(f"The expected pg number is -{expected_pg_num}")
            if pg_num != expected_pg_num:
                log.error(
                    f"Scenario2:The pg number is not decreased to after unsetting the norecover flag."
                    f"The pg number is -{pg_num}"
                )
                return 1
        log.info(
            f"Scenario2:The pg number is decreased after unsetting the norecover flag."
            f"The pg number is -{pg_num}"
        )
        delete_and_healthCheck(rados_obj, pool_name)

        log.info(
            "Scenario3:Modify the pg number manually and verify that the pg_num should not increase "
            "after setting the norecover flag"
        )
        utils.configure_osd_flag(ceph_cluster, "set", "norecover")
        log.info("Scenario3:The norecover is set on the cluster")
        if not rados_obj.create_pool(pool_name=pool_name, pg_num=8):
            log.error("Scenario3:Failed to create the  Pool")
            return 1
        pg_num = pool_obj.get_pg_autoscaler_value(pool_name, "pg_num_target")
        status = check_status(pool_obj, pg_num, pool_name)
        pg_num = pool_obj.get_pg_autoscaler_value(pool_name, "pg_num_target")
        if not status:
            log.error(
                f"Scenario3:The pg_num increased after setting the norecover flag .The pg_num is-{pg_num} "
            )
            return 1
        log.info(
            f"Scenario3:The pg_num is not increased after setting the recovery flag.The pg_num is -{pg_num}"
        )
        utils.configure_osd_flag(ceph_cluster, "unset", "norecover")
        log.info("Scenario3:The norecover is unset on the cluster")
        status = check_status(pool_obj, pg_num, pool_name)
        pg_num = pool_obj.get_pg_autoscaler_value(pool_name, "pg_num_target")
        if status:
            log.error(
                f"Scenario3:The pg_num not increased after unsetting the norecover flag .The pg_num is-{pg_num} "
            )
            return 1
        log.info(
            f"Scenario3:The pg_num is increased after unsetting the recover flag.The pg_num is -{pg_num}"
        )
        method_should_succeed(wait_for_clean_pg_sets, rados_obj)
        log.info(
            "Scenario4:Modify the pg number manually and verify that the pg_num should not decrease "
            "after setting the norecover flag"
        )
        rados_obj.set_pool_property(pool=pool_name, props="pg_num", value=512)
        utils.configure_osd_flag(ceph_cluster, "set", "norecover")
        log.info("Scenario4:The norecover is set on the cluster")
        old_pg_num = pool_obj.get_pg_autoscaler_value(pool_name, "pg_num_target")
        log.info(f"Scenario4:The pg_num after setting the norecover flg is - {pg_num}")

        rados_obj.bench_write(
            pool_name=pool_name, byte_size=1024, rados_write_duration=10
        )
        status = check_status(pool_obj, old_pg_num, pool_name)

        if not status:
            pg_num = pool_obj.get_pg_autoscaler_value(pool_name, "pg_num_target")
            log.error(
                f"Scenario4:The pg number is decreased to after setting the norecover flag "
                f"-{pg_num}"
            )
            return 1
        log.info(
            "Scenario4:The pg number is not decreased after setting the norecover flag"
        )
        utils.configure_osd_flag(ceph_cluster, "unset", "norecover")
        log.info("Scenario4:The norecover is unset on the cluster")
        status = check_status(pool_obj, old_pg_num, pool_name)
        pg_num = pool_obj.get_pg_autoscaler_value(pool_name, "pg_num_target")
        if status:
            log.error(
                f"Scenario4:The pg number is not decreased to after unsetting the norecover flag."
                f"The pg number is -{pg_num}"
            )
            return 1
        log.info(
            f"Scenario4:The pg number is decreased after unsetting the norecover flag."
            f"The pg number is -{pg_num}"
        )
        delete_and_healthCheck(rados_obj, pool_name)

        log.info(
            "Scenario5:Checking the recovery of cluster after setting norecover flag"
        )
        utils.configure_osd_flag(ceph_cluster, "set", "norecover")
        log.info("Scenario5:The norecover is set on the cluster")
        if not rados_obj.create_pool(pool_name=pool_name, pg_num=16):
            log.error("Failed to create the  Pool")
            return 1
        log.info(f"Scenario5:The pool-{pool_name} is created")
        pg_num = pool_obj.get_pg_autoscaler_value(pool_name, "pg_num_target")
        log.info(
            f"Scenario5:The pg_num before creating data in the {pool_name} is - {pg_num}"
        )
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=3600)
        while end_time > datetime.datetime.now():
            rados_obj.bench_write(pool_name=pool_name)
            pool_stat = rados_obj.get_cephdf_stats(pool_name=pool_name)
            out_put = rados_obj.run_ceph_command(cmd="ceph -s")
            for state in out_put["pgmap"]["pgs_by_state"]:
                if "recovering" in state["state_name"]:
                    return 1
            memory_used = pool_stat["stats"]["percent_used"]
            log.info(
                f"The pool-{pool_name} occupied the {memory_used} percentage of memory"
            )
            if memory_used >= 14:
                break
            time.sleep(10)
        new_pg_num = pool_obj.get_pg_autoscaler_value(pool_name, "pg_num_target")
        log.info(
            f"Scenario5:The pg_num after creating data in the {pool_name} is - {pg_num}"
        )
        if pg_num != new_pg_num:
            log.error(
                f"Scenario5:During the tests the pg_num changed from {pg_num} to the {new_pg_num}"
            )
            return 1
        log.info(
            "Scenario5:Recovery not stated on the cluster after setting the recover flag"
        )
        delete_and_healthCheck(rados_obj, pool_name)
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info("Execution of finally block")
        if config.get("delete_pool"):
            method_should_succeed(rados_obj.delete_pool, pool_name)
            log.info(f"deleted the {pool_name} pool successfully")
        utils.configure_osd_flag(ceph_cluster, "unset", "norecover")
        log.info("The norecover is unset on the cluster")
    return 0


def check_status(pool_object, current_pg_num, pool_name):
    """
    Method to check that the pg_num will get change or not.
    Args:
        pool_object: pool object
        current_pg_num: Pg number
        pool_name: pool name

    Returns: True -> PG number is not changed
             False -> PG number is changed

    """
    endtime = datetime.datetime.now() + datetime.timedelta(seconds=180)
    while datetime.datetime.now() < endtime:
        time.sleep(10)
        new_pg_num = pool_object.get_pg_autoscaler_value(pool_name, "pg_num_target")
        log.info(f"The {pool_name} pg number is -{new_pg_num}")
        if new_pg_num != current_pg_num:
            return False
    return True


def delete_and_healthCheck(rados_obj, pool_name):
    """
    Method delete the pool and wait for PG to come active+clean state
    Args:
        rados_obj: Rados object
        pool_name: pool name to delete

    Returns: None

    """
    method_should_succeed(rados_obj.delete_pool, pool_name)
    log.info("Checking the pg are in active+clean state")
    method_should_succeed(wait_for_clean_pg_sets, rados_obj)
    return None
