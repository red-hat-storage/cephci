"""
This test module is used to test the -ve scenarios for 3 AZ stretch pool enable command

includes:

1. Try enabling stretch pool mode on EC pool. Should fail
2. Try providing incomplete command. should fail
3. Try providing non-existent pool name. should fail
4. Try providing wrong bucket name. should fail
5. Try enabling stretch pool mode by providing EC pool crush rule. Should fail
6. Try providing incorrect bucket counts. Should fail
"""

import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.utils import get_cluster_timestamp
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    This test module is used to test the -ve scenarios for 3 AZ stretch pool enable command

    includes:
    0. Try enabling stretch pool on replicated pool. should Pass
    1. Try enabling stretch pool mode on EC pool. Should fail
    2. Try providing incomplete command. should fail
    3. Try providing non-existent pool name. should fail
    4. Try providing wrong bucket name. should fail
    5. Try enabling stretch pool mode by providing EC pool crush rule. Should fail
    6. Try providing incorrect bucket counts. Should fail
    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster
    """

    log.info(run.__doc__)
    config = kw.get("config")
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_name = config.get("pool_name", "test_stretch_pools")
    stretch_bucket = config.get("stretch_bucket", "datacenter")
    start_time = get_cluster_timestamp(rados_obj.node)
    log.debug(f"Test workflow started. Start time: {start_time}")
    try:

        log.info(
            "Starting tests performing -ve scenarios in Stretch pool enable command"
        )

        # Creating test pool to check if regular pool creation works
        log.info(
            "Scenario 0: Try enabling stretch pool on replicated pool. should Pass"
        )
        rule_name_correct = "3az_rule_ok"
        try:
            if not rados_obj.create_pool(pool_name=pool_name):
                log.error(f"Failed to create pool : {pool_name}")
                raise Exception("Test execution failed")
            log.debug("Created pool, proceeding to enable stretch pools mode")
            if not rados_obj.create_n_az_stretch_pool(
                pool_name=pool_name,
                rule_name=rule_name_correct,
                rule_id=55,
                peer_bucket_barrier=stretch_bucket,
                num_sites=3,
                num_copies_per_site=2,
                total_buckets=3,
                req_peering_buckets=2,
            ):
                log.error(
                    f"Unable to Create/Enable stretch mode on the pool : {pool_name}"
                )
                raise Exception("Unable to enable stretch pool")
            log.info("Created pool, & Enabled stretch pools mode. Pass")
        except Exception as err:
            log.error(
                f"Unable to create regular pool on Stretch mode. Should be possible.Err: {err} "
            )
            raise Exception("Pool not created error")
        rados_obj.delete_pool(pool=pool_name)
        rados_obj.delete_crush_rule(rule_name=rule_name_correct)
        time.sleep(5)

        log.info("Scenario 1: Try enabling stretch pool mode on EC pool. Should fail")
        pool_name = "ec-stretch-pool"
        rule_name_correct = "3az_rule_ec"
        try:
            if not rados_obj.create_erasure_pool(
                name="stretch-ec", pool_name=pool_name
            ):
                log.error("Unable to create EC pool in 3az mode.")
                return 1
            log.info("Created EC pool, trying to enable stretch pools")
            if rados_obj.create_n_az_stretch_pool(
                pool_name=pool_name,
                rule_name=rule_name_correct,
                rule_id=56,
                peer_bucket_barrier=stretch_bucket,
                num_sites=3,
                num_copies_per_site=2,
                total_buckets=3,
                req_peering_buckets=2,
            ):
                log.error(f"Enabled stretch mode on a EC pool : {pool_name}, ")
                raise Exception("able to enable stretch pool")
            log.info("Created ECpool, & could not Enable stretch pools mode. Pass")
        except Exception as err:
            log.error(f" Scenario Fail Err hit: {err} ")
            raise Exception("Failed in scenario 1")
        log.info("Completed test for ec pool. Scenario 1")
        rados_obj.delete_pool(pool=pool_name)
        rados_obj.delete_crush_rule(rule_name=rule_name_correct)
        time.sleep(5)

        log.info(
            "Scenario 2: Try enabling stretch pool mode for non existent pool."
            " Command does not create new pools. Should fail"
        )
        pool_name = "non-existent-pool"
        rule_name = "3az_rule_non_existent"
        try:
            log.debug("trying to enable stretch pools on non existent pool")
            if rados_obj.create_n_az_stretch_pool(
                pool_name=pool_name,
                rule_name=rule_name,
                rule_id=57,
                peer_bucket_barrier=stretch_bucket,
                num_sites=3,
                num_copies_per_site=2,
                total_buckets=3,
                req_peering_buckets=2,
                create_pool=False,
            ):
                log.error("Enabled stretch mode on a non-existent pool")
                raise Exception("able to enable stretch pool")
            log.info("could not Enable stretch pools mode on non existent pool. Pass")
        except Exception as err:
            log.error(f" Scenario 2 Fail Err hit: {err} ")
            raise Exception("Failed in scenario 2")
        log.debug("Completed test for non-existent pool")
        rados_obj.delete_pool(pool=pool_name)
        rados_obj.delete_crush_rule(rule_name=rule_name)
        time.sleep(5)

        log.info(
            "Scenario 3: Try enabling stretch pool mode with a EC rule"
            " Command does not create new pools. Should fail"
        )
        pool_name = "test-pool"
        rule_name = "3az_rule_ec"
        profile_name = "ec-profile"
        cmd_profile = f"ceph osd erasure-code-profile set {profile_name} k=2 m=2 crush-failure-domain=host"
        rados_obj.run_ceph_command(cmd=cmd_profile)
        cmd_rule = f" ceph osd crush rule create-erasure {rule_name} {profile_name}"
        rados_obj.run_ceph_command(cmd=cmd_rule)
        if rule_name not in rados_obj.get_crush_rule_names():
            log.error(
                " Scenario 3 Fail. Could not create the new crush rule on cluster "
            )
            raise Exception("Failed in scenario 3")
        try:
            if not rados_obj.create_pool(pool_name=pool_name):
                log.error(f"Failed to create pool : {pool_name}")
                raise Exception("Test execution failed")

            log.debug(
                "Created pool, proceeding to enable stretch pools mode with wrong CRUSH rule"
            )
            if rados_obj.create_n_az_stretch_pool(
                pool_name=pool_name,
                rule_name=rule_name,
                rule_id=58,
                peer_bucket_barrier=stretch_bucket,
                num_sites=3,
                num_copies_per_site=2,
                total_buckets=3,
                req_peering_buckets=2,
            ):
                log.error(
                    f"Enabled stretch mode on pool with wrong crush rule : {pool_name}"
                )
                raise Exception("should not enable stretch pool with EC rule")
            log.info(
                "Created pool, & could not Enable stretch pools mode with EC rule. Pass"
            )
        except Exception as err:
            log.error(f" Scenario Fail Err hit: {err} ")
            raise Exception("Failed in scenario 3")
        log.debug("Completed test for Wrong CRUSH rule")
        rados_obj.delete_pool(pool=pool_name)
        rados_obj.delete_crush_rule(rule_name=rule_name)
        time.sleep(5)

        log.info("Scenario 4. Incorrect number of bucket numbers in cmd")
        rule_name_correct = "3az_rule_ok"
        try:
            if not rados_obj.create_pool(pool_name=pool_name):
                log.error(f"Failed to create pool : {pool_name}")
                raise Exception("Test execution failed")
            log.debug("Created pool, proceeding to enable stretch pools mode")
            # Incorrect bucket name
            if rados_obj.create_n_az_stretch_pool(
                pool_name=pool_name,
                rule_name=rule_name_correct,
                rule_id=55,
                peer_bucket_barrier="zone",
                num_sites=3,
                num_copies_per_site=2,
                total_buckets=3,
                req_peering_buckets=2,
            ):
                log.error(
                    f"able to Create/Enable stretch mode on the pool with wrong inputs: {pool_name}"
                )
                raise Exception("able to enable stretch pool with wrong inputs 1")

            # incorrect bucket count.
            if rados_obj.create_n_az_stretch_pool(
                pool_name=pool_name,
                rule_name=rule_name_correct,
                rule_id=55,
                peer_bucket_barrier="zone",
                num_sites=3,
                num_copies_per_site=2,
                total_buckets=3,
                req_peering_buckets=5,
            ):
                log.error(
                    f"able to Create/Enable stretch mode on the pool with wrong inputs: {pool_name}"
                )
                raise Exception("able to enable stretch pool with wrong inputs 2")
            # incorrect bucket count.
            if rados_obj.create_n_az_stretch_pool(
                pool_name=pool_name,
                rule_name=rule_name_correct,
                rule_id=55,
                peer_bucket_barrier="zone",
                num_sites=3,
                num_copies_per_site=2,
                total_buckets=3,
                req_peering_buckets=0,
            ):
                log.error(
                    f"able to Create/Enable stretch mode on the pool with wrong inputs: {pool_name}"
                )
                raise Exception("able to enable stretch pool with wrong inputs 3")
            # incorrect bucket count.
            if rados_obj.create_n_az_stretch_pool(
                pool_name=pool_name,
                rule_name=rule_name_correct,
                rule_id=55,
                peer_bucket_barrier="zone",
                num_sites=3,
                num_copies_per_site=2,
                total_buckets=3,
                req_peering_buckets=-2,
            ):
                log.error(
                    f"able to Create/Enable stretch mode on the pool with wrong inputs: {pool_name}"
                )
                raise Exception("able to enable stretch pool with wrong inputs 4")

            log.info("Created pool, & could not Enable stretch pools mode. Pass")
        except Exception as err:
            log.error(f"Exception hit while running scenario 4..Err: {err} ")
            raise Exception("Exception hit during scenario 4")
        rados_obj.delete_pool(pool=pool_name)
        rados_obj.delete_crush_rule(rule_name=rule_name_correct)
        time.sleep(5)

    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        # log cluster health
        rados_obj.log_cluster_health()
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
        test_end_time = get_cluster_timestamp(rados_obj.node)
        log.debug(
            f"Test workflow completed. Start time: {start_time}, End time: {test_end_time}"
        )
        if rados_obj.check_crash_status(start_time=start_time, end_time=test_end_time):
            log.error("Test failed due to crash at the end of test")
            return 1
    log.info("All the tests completed on the cluster, Pass!!!")
    return 0
