"""
Module to test Reads balancer functionality on RHCS 8.0 and above clusters

"""

import re
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Module to test Reads balancer functionality on RHCS 8.0 and above clusters
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    variance = 0.10

    regex = r"\s*(\d.\d)-rhel-\d"
    build = (re.search(regex, config.get("build", config.get("rhbuild")))).groups()[0]
    is_within_variance = (
        lambda value, new_value, var: abs(value - new_value) / value <= variance
    )
    if not float(build) >= 8.0:
        log.info(
            "Test running on version less than 8.0, skipping verifying Reads Balancer functionality"
        )
        return 0

    try:
        log.info(
            "Starting the test to verify Online reads balancer with Balncer module "
        )
        rados_obj.configure_pg_autoscaler(**{"default_mode": "warn"})
        log.debug("Creating multiple pools for testing")
        time.sleep(10)
        pools = config["create_pools"]
        balancer_mode = config.get("balancer_mode", "upmap-read")

        for each_pool in pools:
            cr_pool = each_pool["create_pool"]
            if cr_pool.get("pool_type", "replicated") == "erasure":
                method_should_succeed(
                    rados_obj.create_erasure_pool, name=cr_pool["pool_name"], **cr_pool
                )
            else:
                method_should_succeed(rados_obj.create_pool, **cr_pool)
            method_should_succeed(rados_obj.bench_write, **cr_pool)
        log.info("Completed creating pools and writing test data into the pools")

        log.debug(
            "Checking the scores on each pool on the cluster, if the pool is replicated pool"
        )
        read_scores_pre = rados_obj.get_read_scores_on_cluster()
        log.info(
            f"Completed collection of balance scores for all the pools before enabling Online reads balancer"
            f"Scores are : {read_scores_pre}"
        )
        existing_pools = rados_obj.list_pools()
        for pool_name in existing_pools:
            pool_details = rados_obj.get_pool_details(pool=pool_name)
            log.debug(
                f"\nPool details before enabling bulk flag on pools: \n"
                f"Selected pool name : {pool_name} \n Details : {pool_details}"
            )

        # Tracker : https://tracker.ceph.com/issues/66274 .
        # -ve scenario . We should not be able to move to upmap-read or read profiles without setting min-compat-client
        min_client_version = rados_obj.run_ceph_command(cmd="ceph osd dump")[
            "require_min_compat_client"
        ]
        log.debug(
            f"require_min_compat_client before starting the tests is {min_client_version}"
        )
        failed = False
        try:
            config_cmd = "ceph osd set-require-min-compat-client quincy"
            rados_obj.run_ceph_command(cmd=config_cmd)
            rados_obj.enable_balancer(balancer_mode=balancer_mode)
        except Exception as err:
            log.info(
                f"Hit expected exception on the cluster. Error : {err}"
                f"Checking currently enabled mode on the cluster"
            )
            failed = True
            cmd = "ceph balancer status"
            balancer_status = rados_obj.run_ceph_command(cmd=cmd)
            if balancer_status["mode"] == "upmap-read":
                log.error(
                    f"Balancer mode updated without setting min-compat-client on the cluster."
                    f"balancer status : {balancer_status}"
                )
                raise Exception("Balancer upmap-read mode should not be enabled Error")
            log.info(
                "Verified that balancer mode Could not be enabled without require-min-compat-client. "
            )

        if not failed:
            log.error(
                "Balancer mode updated without setting min-compat-client on the cluster."
            )
            raise Exception("Balancer upmap-read mode should not be enabled Error")

        log.debug(
            "Setting the orig value of min_compat_client version on cluster"
            " to allow clients to perform read balancing"
        )
        config_cmd = "ceph osd set-require-min-compat-client reef"
        rados_obj.run_ceph_command(cmd=config_cmd)
        time.sleep(10)

        if not rados_obj.enable_balancer(balancer_mode="upmap-read"):
            log.error("Could not set the balancer mode to upmap-read. Test failed")
            raise Exception("Balancer upmap-read mode could not be enabled Error")

        time.sleep(120)

        log.info(
            "Checking the read balancer scores once the upmap-read balancing mode was enabled"
        )
        read_scores_post = rados_obj.get_read_scores_on_cluster()
        log.info(
            f"Completed collection of balance scores for all the pools before enabling Online reads balancer"
            f"Scores are : {read_scores_post}"
        )

        log.info(
            "Checking if the read scores are lower upon enabling reads balancing via Balancer module"
        )
        common_pools = set(read_scores_pre.keys()) & set(read_scores_post.keys())
        # Compare the values for each common key
        for pool in common_pools:
            if read_scores_pre[pool] < read_scores_post[pool]:
                if not is_within_variance(
                    read_scores_pre[pool], read_scores_post[pool], variance
                ):
                    log.error(
                        f"Read score on pool {pool} has increased post enabling Balancer based reads balancing,"
                        f"and has increased beyond the variance of 10%"
                        f"Before : {read_scores_pre[pool] } , After : {read_scores_post[pool]}\n"
                        f"Pool details : {rados_obj.get_pool_details(pool=pool)}"
                    )
                    raise Exception(
                        "Balancer score higher upon enabling reads balancing. Fail"
                    )
                log.error(
                    "The Read score on the pool is increased post enabling read balancer, "
                    " but it is within the variance of 10%. "
                    f"Before : {read_scores_pre[pool] } , After : {read_scores_post[pool]}\n"
                    f"Pool details : {rados_obj.get_pool_details(pool=pool)}"
                    "Not failing the test"
                )
        log.debug("Completed verifying reads balancer scores on all the pools")
        rados_obj.rados_pool_cleanup()
        rados_obj.configure_pg_autoscaler(**{"default_mode": "on"})
        time.sleep(20)

        log.debug(
            "Completed 1st stage of verification of Balancer scores on the pools"
            "Moving to checking effects of PG autoscaling on Balancer scores"
        )

        test_pool = "balancer_test_pool"
        if not rados_obj.create_pool(pool_name=test_pool, pg_num=16):
            log.error("test pool could not be created")
            raise Exception("pool could not be created error")
        rados_obj.bench_write(pool_name=test_pool, max_objs=500)
        time.sleep(10)
        test_read_score_init = rados_obj.get_read_scores_on_cluster()[test_pool]

        res, _ = pool_obj.run_autoscaler_bulk_test(
            pool=test_pool,
            overwrite_recovery_threads=True,
            test_pg_split=True,
            timeout=1800,
        )
        if not res:
            log.error("Failed to scale up the pool with bulk flag. Fail")
            raise Exception("Pool not scaled up error")

        test_read_score_scale_up = rados_obj.get_read_scores_on_cluster()[test_pool]
        if test_read_score_init < test_read_score_scale_up:
            if not is_within_variance(
                test_read_score_init, test_read_score_scale_up, variance
            ):
                log.error(
                    "The read balancer score is equal or higher than it was before PG splits."
                    "Greater than the allowed variance of 10%"
                    f"Before : {test_read_score_init } , After : {test_read_score_scale_up}\n"
                    f"Pool details : {rados_obj.get_pool_details(pool=test_pool)}"
                )
                raise Exception("Pool scores Higher or same error post scale up")
            log.error(
                "The Read score on the pool is increased post scale up, but it is within the variance of 10%."
                f"Before : {test_read_score_init } , After : {test_read_score_scale_up}\n"
                f"Pool details : {rados_obj.get_pool_details(pool=test_pool)}"
                "Not failing the test"
            )

        log.debug(
            "Starting with scale_down of PGs by removing bulk flag"
            "Bugzilla reported for scale down : https://bugzilla.redhat.com/show_bug.cgi?id=2302230 "
        )

        """
        res, _ = pool_obj.run_autoscaler_bulk_test(
            pool=test_pool, overwrite_recovery_threads=True, test_pg_merge=True, timeout=1800
        )
        if not res:
            log.error("Failed to scale down the pool with removal of bulk flag. Fail")
            raise Exception("Pool not scaled down error")

        test_read_score_scale_down = rados_obj.get_read_scores_on_cluster()[test_pool]
        if test_read_score_init < test_read_score_scale_down:
            if not is_within_variance(
                test_read_score_init, test_read_score_scale_down, variance
            ):
                log.error(
                    "The read balancer score is higher than it was before PG merges."
                    f"Before : {test_read_score_init} , After : {test_read_score_scale_down}\n"
                    f"Pool details : {rados_obj.get_pool_details(pool=test_pool)}"
                )
                raise Exception("Pool scores Higher error post scale down")
            log.error(
                "The Read score on the pool is increased post scale down, but it is within the variance of 10%. "
                f"Before : {test_read_score_init} , After : {test_read_score_scale_down}\n"
                f"Pool details : {rados_obj.get_pool_details(pool=test_pool)}"
                "Not failing the test"
            )
        """
        log.info("Completed reads balancing scenarios")
        return 0

    except Exception as err:
        log.error(
            f"hit Exception : {err} during the testing of reads balancer functionality"
        )
        return 1

    finally:
        log.info("\n\n\nIn the finally block of Online reads balancer tests\n\n\n")
        rados_obj.rados_pool_cleanup()
        rados_obj.enable_balancer(balancer_mode="upmap")
        time.sleep(60)
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
