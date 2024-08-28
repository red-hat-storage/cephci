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

        def check_pg_increase(pool_name):
            """
            Method to check if the PG count has reached desired counts
            """
            pg_num_final = pool_obj.get_pg_autoscaler_value(pool_name, "pg_num_final")
            endtime = datetime.datetime.now() + datetime.timedelta(seconds=1800)
            pg_increased = False
            while datetime.datetime.now() < endtime:
                current_pg_num = rados_obj.get_pool_property(
                    pool=pool_name, props="pg_num"
                )["pg_num"]
                if pg_num_final != current_pg_num:
                    log.debug(
                        f"PG count on the pool not reached desired levels,"
                        f"current count : {current_pg_num}, target count : {pg_num_final}"
                        f" sleeping for 10 seconds and checking again"
                    )
                    time.sleep(10)
                else:
                    log.info("PG count on the pool has reached desired levels")
                    pg_increased = True
                    break
            return pg_increased

        log.info(
            "Scenario1: Verify that the pg_num should not  increase after setting the norecover flag"
        )
        method_should_succeed(wait_for_clean_pg_sets, rados_obj)
        utils.configure_osd_flag(ceph_cluster, "set", "norecover")
        time.sleep(10)

        log.info("Scenario1:The norecover is set on the cluster")
        if not rados_obj.create_pool(pool_name=pool_name, pg_num=8):
            log.error("Failed to create the  Pool")
            return 1

        log.info(f"Scenario1:The pool-{pool_name} is created")
        old_pg_num = rados_obj.get_pool_property(pool=pool_name, props="pg_num")[
            "pg_num"
        ]

        log.debug(f"The current pg number is -{old_pg_num}")
        rados_obj.log_cluster_health()
        rados_obj.bench_write(
            pool_name=pool_name, byte_size=1024, rados_write_duration=10
        )
        rados_obj.log_cluster_health()
        pg_num_final = pool_obj.get_pg_autoscaler_value(pool_name, "pg_num_final")
        if not check_status(pool_obj, old_pg_num, pool_name):
            log.error(
                f"Scenario1: The pg number is increased to after setting the norecover flag post pool creation with "
                f"{old_pg_num} PGs"
            )
            return 1
        log.info(
            f"Scenario1:The pg number is not increased after setting the norecover flag post pool creation."
            f"The pg_num is - {old_pg_num} on the pool. Target : {pg_num_final}"
            f"Proceeding to unset the norecover flag on the cluster"
        )

        # unset the norecover flag
        utils.configure_osd_flag(ceph_cluster, "unset", "norecover")

        log.info(
            "Scenario1: The norecover is unset on the cluster, and verifying "
            "if the PG count on the pool automatically increases to desired value"
        )

        if check_status(pool_obj, old_pg_num, pool_name):
            log.error(
                "Scenario1:The pg number is not increased to after unsetting the norecover flag."
            )
            return 1

        if not check_pg_increase(pool_name=pool_name):
            log.error(
                "PG count not increased on the pool post removal of norecover flag."
                "Test Failed"
            )
            return 1

        log.info(
            f"Scenario1:The pg number is increased after unsetting the norecover flag."
            f"The pg number now is -{pg_num_final}"
        )
        delete_and_healthCheck(rados_obj, pool_name)
        time.sleep(20)
        log.info("=========Scenario1: Test completed===========")
        log.info(
            "Scenario2: Verify that the pg_num should not decrease after setting the norecover flag"
        )

        if not rados_obj.create_pool(pool_name=pool_name, bulk="bulk"):
            log.error("Failed to create the  Pool for scenario 2")
            return 1

        if not check_pg_increase(pool_name=pool_name):
            log.error(
                "PG count not increased on the pool upon creation of pool with bulk flag"
                "Test Failed"
            )
            return 1
        old_pg_num = rados_obj.get_pool_property(pool=pool_name, props="pg_num")[
            "pg_num"
        ]

        log.info(f" The current pg number is -{old_pg_num} with bulk flag enabled")
        utils.configure_osd_flag(ceph_cluster, "set", "norecover")
        time.sleep(5)
        log.info("Scenario2:The norecover is set on the cluster")
        # Removed the bulk flag
        pool_obj.rm_bulk_flag(pool_name=pool_name)
        rados_obj.log_cluster_health()
        rados_obj.bench_write(
            pool_name=pool_name, byte_size=1024, rados_write_duration=10
        )

        if not check_status(pool_obj, old_pg_num, pool_name):
            log.error(
                "Scenario2:The pg number is decreased to after "
                "setting the norecover flag on the pool with bulk flag removed"
            )
            return 1
        log.info(
            "Scenario2:The pg number is not decreased after setting the norecover flag."
            "Removing the bulk flag on the pool to check if the PG count would increase now"
        )

        utils.configure_osd_flag(ceph_cluster, "unset", "norecover")
        log.info("The norecover is unset on the cluster")

        if check_status(pool_obj, old_pg_num, pool_name):
            log.error(
                "Scenario2:The pg number is not decreased to after unsetting the norecover flag."
                "on the pool where bulk flag was removed"
            )
            return 1

        if not check_pg_increase(pool_name=pool_name):
            log.error(
                "PG count not increased on the pool upon creation of pool with bulk flag"
                "Test Failed"
            )
            return 1
        log.info(
            "Scenario2:The pg number is decreased after unsetting the norecover"
            " flag upon bulk flag addition/ removal complete"
        )
        delete_and_healthCheck(rados_obj, pool_name)
        log.info("=========Scenario2: Test completed===========")
        time.sleep(20)

        log.info(
            "Scenario3:Modify the pg number manually and verify that the pg_num should not increase "
            "after setting the norecover flag"
            "Checking if the below scenarios are valid via bugzilla "
            "comment here : https://bugzilla.redhat.com/show_bug.cgi?id=2134786#c48"
            "comment here : https://bugzilla.redhat.com/show_bug.cgi?id=2134786#c49"
            "As explained,  We don't want to restrict users from manually changing the PG count."
            "Removing scenarios 3 & 4"
        )
        log.info(
            "Scenario5:Checking the recovery of cluster after setting norecover flag"
        )
        utils.configure_osd_flag(ceph_cluster, "set", "norecover")
        log.info("Scenario5:The norecover is set on the cluster")
        time.sleep(10)
        if not rados_obj.create_pool(pool_name=pool_name, pg_num=8):
            log.error("Failed to create the  Pool")
            return 1
        log.info(f"Scenario5:The pool-{pool_name} is created")
        pg_num = rados_obj.get_pool_property(pool=pool_name, props="pg_num")["pg_num"]
        log.info(
            f"Scenario5:The pg_num before creating data in the {pool_name} is - {pg_num}"
        )
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=3600)
        while end_time > datetime.datetime.now():
            rados_obj.log_cluster_health()
            rados_obj.bench_write(pool_name=pool_name)
            rados_obj.log_cluster_health()
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
        new_pg_num = rados_obj.get_pool_property(pool=pool_name, props="pg_num")[
            "pg_num"
        ]
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
        log.info("=========Scenario5: Test completed===========")
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info("\n\n\n\nExecution of finally block\n\n\n\n")
        rados_obj.rados_pool_cleanup()
        utils.configure_osd_flag(ceph_cluster, "unset", "norecover")
        log.info("The norecover is unset on the cluster")
        time.sleep(20)
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
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
    log.debug(
        f"Checking for 300 seconds if the PG count on the pool: {pool_name} changes"
    )
    endtime = datetime.datetime.now() + datetime.timedelta(seconds=300)
    while datetime.datetime.now() < endtime:
        time.sleep(10)
        new_pg_num = pool_object.rados_obj.get_pool_property(
            pool=pool_name, props="pg_num"
        )["pg_num"]
        log.info(f"The pool : {pool_name} has pg number - {new_pg_num}")
        if new_pg_num != current_pg_num:
            log.error(f"PG count on the pool: {pool_name} has changed on the pool")
            return False
        time.sleep(30)
    log.debug(f"PG count on the pool: {pool_name} has not changed on the pool")
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
