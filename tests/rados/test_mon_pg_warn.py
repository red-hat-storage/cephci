"""
Module to verify MANY_OBJECTS_PER_PG health warning and
manual-increment of 'mon_pg_warn_max_object_skew' MGR parameter
"""

import datetime
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    # CEPH-83575440
    Test to verify occurrence of MANY_OBJECTS_PER_PG health warning and
    effect of change of 'mon_pg_warn_max_object_skew' MGR parameter
    1. Create a replicated pool with single pg and autoscaling disabled
    2. Ensure default value of MGR parameter 'mon_pg_warn_max_object_skew' is 10
    3. Write huge number of objects to the pool in order to trigger the MANY_OBJECTS_PER_PG
    warning
    4. Increase pool pgs manually and observe the health warning disappearing
    5. Repeat the above steps with a fresh pool and increase the value of
    MGR parameter 'mon_pg_warn_max_object_skew' to 600
    6. Observe the health warning disappearing, delete the pools
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    pool_obj = PoolFunctions(node=cephadm)
    pool_name = "test_mon_pg_warn"
    log.info(
        "Running test case to verify auto increment of 'mon_pg_warn_max_object_skew' parameter with PG auto-scaling"
    )

    def mon_pg_warn_health_warn(should_contain: bool):
        # check health warning for "MANY_OBJECTS_PER_PG"
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=180)
        while datetime.datetime.now() < end_time:
            try:
                health_detail, _ = cephadm.shell(args=["ceph health detail"])
                log.info(f"Health warning: \n {health_detail}")
                if should_contain:
                    assert "[WRN] MANY_OBJECTS_PER_PG:" in health_detail
                else:
                    assert "[WRN] MANY_OBJECTS_PER_PG:" not in health_detail
                break
            except AssertionError:
                time.sleep(30)
                if datetime.datetime.now() >= end_time:
                    raise

    for test_type in ["manual_increment", "pg_autoscale"]:
        try:
            # fetch the default value of MON parameter 'mon_pg_warn_max_object_skew'
            default_mon_pg_warn = float(
                mon_obj.get_config(section="mgr", param="mon_pg_warn_max_object_skew")
            )

            if not default_mon_pg_warn == 10.000000:
                err = (
                    "Default value of mon parameter 'mon_pg_warn_max_object_skew' has changed. "
                    f"Expected: 10.000000 | Actual: {default_mon_pg_warn}"
                )
                log.error(err)
                raise AssertionError(err)

            rados_obj.configure_pg_autoscaler(**{"default_mode": "warn"})
            # create a replicated pool with single pg
            assert rados_obj.create_pool(
                pool_name=pool_name, **{"pg_num": 1, "pgp_num": 1}
            )

            # write 10000 objects to the pool to trigger the warning
            bench_config = {
                "rados_write_duration": 300,
                "byte_size": "64KB",
                "max_objs": 10000,
            }
            assert rados_obj.bench_write(pool_name=pool_name, **bench_config)

            # check health warning for "MANY_OBJECTS_PER_PG"
            mon_pg_warn_health_warn(should_contain=True)

            if test_type == "pg_autoscale":
                # PG autoscale WARN mode cannot be removed as it
                # would also remove the health warning
                # Scale up PGs in the pool to 64 using pg_num pool property
                rados_obj.set_pool_property(pool=pool_name, props="pg_num", value=64)
                rados_obj.set_pool_property(pool=pool_name, props="pgp_num", value=64)
                rados_obj.set_pool_property(
                    pool=pool_name, props="pg_num_min", value=64
                )

                # wait for Pool to scale to 64
                timeout_time = datetime.datetime.now() + datetime.timedelta(seconds=160)
                while datetime.datetime.now() < timeout_time:
                    try:
                        pg_count = pool_obj.get_target_pg_num_bulk_flag(
                            pool_name=pool_name
                        )
                        assert pg_count == 64
                        log.info(
                            f"Number of PGs for {pool_name} has scaled up to {pg_count}"
                        )
                        break
                    except AssertionError:
                        log.info(f"PG count yet to reach 64, current value: {pg_count}")
                        time.sleep(30)
                        if datetime.datetime.now() >= timeout_time:
                            raise (
                                f"PG count for {pool_name} is still {pg_count} after setting "
                                f"pg_num to 64 and waiting for 160 secs"
                            )
            else:
                # with pg autoscaling restricted and pool having single PG,
                # increase the value of 'mon_pg_warn_max_object_skew' to 600
                mon_obj.set_config(
                    section="mgr", name="mon_pg_warn_max_object_skew", value=600
                )
            # ensure health warning for "MANY_OBJECTS_PER_PG" disappears
            mon_pg_warn_health_warn(should_contain=False)
        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here *************** \n \n"
            )
            mon_obj.remove_config(
                section="global", name="osd_pool_default_pg_autoscale_mode"
            )
            mon_obj.remove_config(section="mgr", name="mon_pg_warn_max_object_skew")
            rados_obj.delete_pool(pool=pool_name)
            # log cluster health
            rados_obj.log_cluster_health()
            # check for crashes after test execution
            if rados_obj.check_crash_status():
                log.error("Test failed due to crash at the end of test")
                return 1

    log.info(
        "Verification of heath warnings related to 'mon_pg_warn_max_object_skew' parameter completed"
    )
    return 0
