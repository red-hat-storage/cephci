"""
Module to verify MANY_OBJECTS_PER_PG health warning and
auto-increment of 'mon_pg_warn_max_object_skew' MON parameter
due to PG auto-scaling
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
    change in 'mon_pg_warn_max_object_skew' due to PG auto-scaling
    1. Create a replicated pool with single pg and autoscaling disabled
    2. Ensure default value of MON parameter 'mon_pg_warn_max_object_skew' is 10
    3. Write huge number of objects to the pool in order to trigger the MANY_OBJECTS_PER_PG
    warning
    4. Enable pg autoscaling, let the pool autoscale and health warning should disappear
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
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=125)
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
                mon_obj.get_config(section="mon", param="mon_pg_warn_max_object_skew")
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
                pool_name=pool_name, **{"pg_num": 1, "pgp_num": 1, "pg_num_max": 1}
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
                # enable PG autoscaling
                mon_obj.remove_config(
                    section="global", name="osd_pool_default_pg_autoscale_mode"
                )
                rados_obj.set_pool_property(
                    pool=pool_name, props="pg_autoscale_mode", value="on"
                )
                rados_obj.set_pool_property(
                    pool=pool_name, props="pg_num_max", value=256
                )

                # wait for Pool to autoscale
                timeout_time = datetime.datetime.now() + datetime.timedelta(seconds=155)
                while datetime.datetime.now() < timeout_time:
                    try:
                        pg_count = pool_obj.get_target_pg_num_bulk_flag(
                            pool_name=pool_name
                        )
                        assert pg_count > 1
                        log.info(
                            f"Number of PGs for {pool_name} has auto-scaled to {pg_count}"
                        )
                        break
                    except AssertionError:
                        time.sleep(30)
                        if datetime.datetime.now() >= timeout_time:
                            raise (
                                f"PG count for {pool_name} is still 1 after enabling"
                                f" auto-scaling and waiting for 155 secs"
                            )
                # ensure health warning for "MANY_OBJECTS_PER_PG" disappears
                mon_pg_warn_health_warn(should_contain=False)
            else:
                # with pg autoscaling restricted as pg_num_max is set to 1,
                # increase the value of 'mon_pg_warn_max_object_skew' to 100
                mon_obj.set_config(
                    section="mon", name="mon_pg_warn_max_object_skew", value=100
                )
                # ideally health warning should disappear after setting the above config, however,
                # it has been observed that health warn '[WRN] MANY_OBJECTS_PER_PG' persists even
                # if 'mon_pg_warn_max_object_skew' is set to 1000
        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            return 1
        finally:
            mon_obj.remove_config(
                section="global", name="osd_pool_default_pg_autoscale_mode"
            )
            mon_obj.remove_config(section="mon", name="mon_pg_warn_max_object_skew")
            rados_obj.detete_pool(pool=pool_name)

    log.info(
        "Verification of automatic change of 'mon_pg_warn_max_object_skew' parameter completed"
    )
    return 0
