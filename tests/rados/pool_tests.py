"""
This file contains various tests/ validations related to pools.
Tests included:
1. Verification of EC pool recovery improvement
2. Effect on size of pools with and without compression
3. Test bulk flag with pools
4. Test target ratios on pools
5. Test Autoscaler warnings
"""

import datetime
import random
import re
import time

import yaml

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from tests.rados.test_data_migration_bw_pools import create_given_pool
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Performs various pool related validation tests
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    rhbuild = config.get("rhbuild")
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    pool_obj = PoolFunctions(node=cephadm)
    client_node = ceph_cluster.get_nodes(role="client")[0]

    if config.get("ec_pool_recovery_improvement"):
        try:
            ec_config = config.get("ec_pool_recovery_improvement")
            if not rados_obj.create_erasure_pool(name="recovery", **ec_config):
                log.error("Failed to create the EC Pool")
                return 1

            if not rados_obj.bench_write(**ec_config):
                log.error("Failed to write objects into the EC Pool")
                return 1
            rados_obj.bench_read(**ec_config)
            log.info("Created the EC Pool, Finished writing data into the pool")

            # getting the acting set for the created pool
            acting_pg_set = rados_obj.get_pg_acting_set(
                pool_name=ec_config["pool_name"]
            )
            if len(acting_pg_set) != ec_config["k"] + ec_config["m"]:
                log.error(
                    f"acting set consists of only these : {acting_pg_set} OSD's, less than k+m"
                )
                return 1
            log.info(f" Acting set of the pool consists of OSD's : {acting_pg_set}")
            log.info(
                f"Killing m, i.e {ec_config['m']} OSD's from acting set to verify recovery"
            )
            stop_osds = [acting_pg_set.pop() for _ in range(ec_config["m"])]
            for osd_id in stop_osds:
                if not rados_obj.change_osd_state(action="stop", target=osd_id):
                    log.error(f"Unable to stop the OSD : {osd_id}")
                    return 1

            log.info("Stopped 'm' number of OSD's from, starting to wait for recovery")
            rados_obj.change_recovery_threads(config=ec_config, action="set")

            # Sleeping for 25 seconds ( "osd_heartbeat_grace": "20" ) for osd's to be marked down
            time.sleep(25)

            # Waiting for up to 2.5 hours for the recovery to complete and PG's to enter active + Clean state
            end_time = datetime.datetime.now() + datetime.timedelta(seconds=15000)
            while end_time > datetime.datetime.now():
                flag = True
                status_report = rados_obj.run_ceph_command(cmd="ceph report")

                # Proceeding to check if all PG's are in active + clean
                for entry in status_report["num_pg_by_state"]:
                    rec = (
                        "backfilling",
                        "degraded",
                        "incomplete",
                        "recovering",
                        "recovery_wait",
                        "backfilling_wait",
                        "peered",
                        "undersized",
                    )
                    if any(key in rec for key in entry["state"].split("+")):
                        flag = False

                if flag:
                    log.info("The recovery and back-filling of the OSD is completed")
                    break
                log.info(
                    f"Waiting for active + clean. Active aletrs: {status_report['health']['checks'].keys()},"
                    f"PG States : {status_report['num_pg_by_state']}"
                    f" checking status again in 1 minute"
                )
                time.sleep(60)

            # getting the acting set for the created pool after recovery
            acting_pg_set = rados_obj.get_pg_acting_set(
                pool_name=ec_config["pool_name"]
            )
            if len(acting_pg_set) != ec_config["k"] + ec_config["m"]:
                log.error(
                    f"acting set consists of only these : {acting_pg_set} OSD's, less than k+m"
                )
                return 1
            log.info(f" Acting set of the pool consists of OSD's : {acting_pg_set}")
            # Changing recovery threads back to default
            rados_obj.change_recovery_threads(config=ec_config, action="rm")

            log.debug("Starting the stopped OSD's")
            for osd_id in stop_osds:
                if not rados_obj.change_osd_state(action="restart", target=osd_id):
                    log.error(f"Unable to restart the OSD : {osd_id}")
                    return 1

            # Sleep for 5 seconds for OSD's to join the cluster
            time.sleep(5)

            if not flag:
                log.error("The pool did not reach active + Clean state after recovery")
                return 1

            # Deleting the pool created
            if not rados_obj.delete_pool(pool=ec_config["pool_name"]):
                log.error(f"the pool {ec_config['pool_name']} could not be deleted")
                return 1
        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here *************** \n \n"
            )
            # removal of rados pools
            rados_obj.rados_pool_cleanup()
            # Changing recovery threads back to default
            rados_obj.change_recovery_threads(config=ec_config, action="rm")
            # log cluster health
            rados_obj.log_cluster_health()
            # check for crashes after test execution
            if rados_obj.check_crash_status():
                log.error("Test failed due to crash at the end of test")
                return 1

        log.info("Successfully tested EC pool recovery with K osd's surviving")
        return 0

    if config.get("Compression_tests"):
        """
        Create a 2 replicated pools:
        1. Pool_1 : enable any compression algorithm(def snappy) and compression mode(aggressive/force).
        2. Pool_2 : set compression mode to none
        Writing the same amount of data on 2 pools, size of pool with compression on would consume less space
        """
        pool_config = config["Compression_tests"]["pool_config"]
        compression_config = config["Compression_tests"]["compression_config"]
        pool_1 = pool_config["pool-1"]
        pool_2 = pool_config["pool-2"]

        try:
            if config["Compression_tests"]["pool_type"] == "replicated":
                if not rados_obj.create_pool(pool_name=pool_1, **pool_config):
                    log.error("could not create pool-1")
                    return 1
                if not rados_obj.create_pool(pool_name=pool_2, **pool_config):
                    log.error("could not create pool-2")
                    return 1
            elif config["Compression_tests"]["pool_type"] == "erasure":
                pool_config["pool_name"] = pool_1
                if not rados_obj.create_erasure_pool(name=pool_1, **pool_config):
                    log.error("could not create pool-1")
                    return 1
                pool_config["pool_name"] = pool_2
                if not rados_obj.create_erasure_pool(name=pool_2, **pool_config):
                    log.error("could not create pool-2")
                    return 1
                del pool_config["pool_name"]

            log.debug("Created two pools to test compression")

            # Enabling compression on pool-1
            if not rados_obj.pool_inline_compression(
                pool_name=pool_1, **compression_config
            ):
                log.error(
                    f"Error setting compression on pool : {pool_1} for config {compression_config}"
                )
                return 1

            # Writing the same amount of data into two pools
            if not rados_obj.bench_write(
                pool_name=pool_1, byte_size="5M", max_objs=200, verify_stats=True
            ):
                log.error(
                    "Failed to write objects into Pool-1, with compression enabled"
                )
                return 1

            if not rados_obj.bench_write(
                pool_name=pool_2, byte_size="5M", max_objs=200, verify_stats=True
            ):
                log.error(
                    "Failed to write objects into Pool-2, without compression enabled"
                )
                return 1
            # Sleeping for 5 seconds for status to be updated.
            time.sleep(5)

            log.debug("Finished writing data into the two pools. Checking pool stats")
            try:
                pool_stats = rados_obj.run_ceph_command(cmd="ceph df detail")["pools"]
                pool_1_stats = [
                    detail for detail in pool_stats if detail["name"] == pool_1
                ][0]["stats"]
                pool_2_stats = [
                    detail for detail in pool_stats if detail["name"] == pool_2
                ][0]["stats"]
            except KeyError:
                log.error("No stats about the pools requested found on the cluster")
                return 1

            log.debug(f"Pool-1 stats: {pool_1_stats}")
            log.debug(f"Pool-2 stats: {pool_2_stats}")
            if pool_1_stats["compress_bytes_used"] < 0:
                log.error("No data stored under pool-1 is compressed")
                return 1

            if pool_1_stats["kb_used"] >= pool_2_stats["kb_used"]:
                log.error("Compression has no effect on the pool size...")
                return 1

            if config["Compression_tests"].get("verify_compression_ratio_set"):
                # added verification for test: CEPH-83571672
                if not rados_obj.check_compression_size(
                    pool_name=pool_1, **compression_config
                ):
                    log.error("data not compressed in accordance to ratio set")
                    return 1

            log.info("Pool size is less when compression is enabled, Pass!")
            if config["Compression_tests"].get("delete_pools"):
                log.debug("Deleting pools created for compression tests")
                for pool in config["Compression_tests"]["delete_pools"]:
                    rados_obj.delete_pool(pool=pool)
        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
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
            if rados_obj.check_crash_status():
                log.error("Test failed due to crash at the end of test")
                return 1
        return 0

    if config.get("test_autoscaler_bulk_feature"):
        """
        Tests to verify the autoscaler bulk flag, which allows pools to make use of
        scale-down profile, making those pools start with full compliments of PG sets.
        Tests include
        1. creating new pools with bulk,
        2. enabling/disabling bulk flag on existing pools
        3. Verify the PG changes when the flag is set/unset
        Verifies bugs : https://bugzilla.redhat.com/show_bug.cgi?id=2049851
        """
        try:
            regex = r"\s*(\d.\d)-rhel-\d"
            build = (
                re.search(regex, config.get("build", config.get("rhbuild")))
            ).groups()[0]
            if not float(build) > 5.0:
                log.info(
                    "Test running on version less than 5.1, skipping verifying bulk flags"
                )
                return 0

            # Setting the global pg autoscale mode to on
            rados_obj.configure_pg_autoscaler(default_mode="on")

            pool_name = config.get("pool_name")
            if not rados_obj.create_pool(pool_name=pool_name):
                log.error("Failed to create pool on cluster")
                return 1
            rados_obj.bench_write(pool_name=pool_name, verify_stats=False)
            log.debug(f"Created pool : {pool_name} and wrote IO's on the pool")

            init_pg_count = rados_obj.get_pool_property(pool=pool_name, props="pg_num")[
                "pg_num"
            ]
            log.debug(f"Init PG count upon pool creation is {init_pg_count}")

            if not pool_obj.set_bulk_flag(pool_name=pool_name):
                log.error("Expected bulk flag should be set to True.")
                raise Exception("Expected bulk flag should be True.")

            log.debug("Set the bulk flag on the pool to true")
            # Sleeping for 120 seconds for bulk flag application and PG count to be increased.
            time.sleep(120)

            pg_count_bulk_true = rados_obj.get_pool_details(pool=pool_name)[
                "pg_num_target"
            ]
            log.debug(
                f"PG count on pool {pool_name} post addition of bulk flag : {pg_count_bulk_true}"
            )
            no_count_change = False
            if pg_count_bulk_true <= init_pg_count:
                log.info(
                    "Checking the ideal PG count on the pool and the threshold set. default is 3"
                )
                pool_data = rados_obj.get_pg_autoscale_status(pool_name=pool_name)
                final_pg_num = pool_data["pg_num_final"]
                log.debug(
                    f"Pool : {pool_name} details as fetched from autoscale-status : {pool_data}"
                )
                # for the threshold var, refer :
                # https://docs.ceph.com/en/latest/rados/operations/placement-groups/#viewing-pg-scaling-recommendations
                # cmd : ceph osd pool set threshold <val>
                if float(final_pg_num / init_pg_count) <= 3.0:
                    log.info(
                        "PG count not expected to increase as the final pg num is less than the threshold"
                    )
                    no_count_change = True
                else:
                    raise Exception(
                        f"Actual pg_num post bulk enablement {pg_count_bulk_true}"
                        f" is expected to be greater than {init_pg_count}"
                    )

            # Unsetting the bulk flag and checking the change in the PG counts
            if not pool_obj.rm_bulk_flag(pool_name=pool_name):
                log.error("Failed to create a pool with bulk features")
                return 1
            log.debug("Set the bulk flag on the pool to False")
            time.sleep(120)

            pg_count_bulk_false = rados_obj.get_pool_details(pool=pool_name)[
                "pg_num_target"
            ]
            log.debug(
                f"PG count on pool {pool_name} post addition of bulk flag : {pg_count_bulk_true}"
            )
            if not pg_count_bulk_false <= pg_count_bulk_true:
                if no_count_change:
                    log.info(
                        "PG count not expected to change upon bulk removal as it was less than the threshold set"
                    )
                else:
                    raise Exception(
                        f"Actual pg_num post bulk disable {pg_count_bulk_true} is "
                        f"expected to be less than {pg_count_bulk_true}"
                    )
        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            return 1
        finally:
            log.debug(f"Removing the pool created : {pool_name}")
            rados_obj.delete_pool(pool=pool_name)
            # log cluster health
            rados_obj.log_cluster_health()
            # check for crashes after test execution
            if rados_obj.check_crash_status():
                log.error("Test failed due to crash at the end of test")
                return 1
        log.info("Verified the workings of bulk flag")
        return 0

    if config.get("verify_pool_target_ratio"):
        try:
            log.debug("Verifying target size ratio on pools")
            target_configs = config["verify_pool_target_ratio"]["configurations"]
            # Creating pools and starting the test
            for entry in target_configs.values():
                log.debug(f"Creating {entry['pool_type']} pool on the cluster")
                if entry.get("pool_type", "replicated") == "erasure":
                    method_should_succeed(
                        rados_obj.create_erasure_pool, name=entry["pool_name"], **entry
                    )
                else:
                    method_should_succeed(
                        rados_obj.create_pool,
                        **entry,
                    )
                rados_obj.bench_write(**entry)
                pool_name = entry["pool_name"]
                init_pg_count = int(
                    pool_obj.get_pg_autoscaler_value(
                        pool_name=entry["pool_name"], item="pg_num_target"
                    )
                )
                log.debug(
                    f"PG count on pool : {pool_name} upon pool creation is : {init_pg_count}"
                )
                if not pool_obj.verify_target_ratio_set(
                    pool_name=pool_name, ratio=entry["target_size_ratio"]
                ):
                    log.error(
                        f"Could not change the target ratio on the pool: {entry['pool_name']}"
                    )
                    return 1
                log.debug("Set the ratio. getting the projected pg's")

                rados_obj.change_recovery_threads(config=config, action="set")
                log.debug(
                    "Waiting for the rebalancing to complete on the cluster after the change"
                )
                # Sleeping for 2 minutes for rebalancing to start & for new PG count to be updated.
                time.sleep(120)

                new_pg_count = int(
                    pool_obj.get_pg_autoscaler_value(
                        pool_name=entry["pool_name"], item="pg_num_target"
                    )
                )

                log.debug(
                    f"PG count post setting the target_size_ratio on pool {pool_name} is {new_pg_count}"
                )
                if new_pg_count < init_pg_count:
                    log.error(
                        f"Count of PG's not increased on the pool: {entry['pool_name']}"
                        f"Initial creation count : {entry['pg_num']}"
                        f"New count after setting num target : {new_pg_count}"
                    )
                    return 1

                res = wait_for_clean_pg_sets(rados_obj, test_pool=pool_name)
                if not res:
                    log.error(
                        "PG's in cluster are not active + Clean after the ratio change"
                    )
                    return 1
                if not pool_obj.verify_target_ratio_set(
                    pool_name=entry["pool_name"], ratio=0.0
                ):
                    log.error(
                        f"Could not remove the target ratio on the pool: {pool_name}"
                    )
                    return 1

                # Sleeping for 2 minutes for rebalancing to start & for new PG count to be updated.
                time.sleep(120)

                # Checking if after the reduction of ratio, the PG count has reduced
                end_pg_count = int(
                    pool_obj.get_pg_autoscaler_value(
                        pool_name=entry["pool_name"], item="pg_num_target"
                    )
                )
                log.debug("PG count post changing the ratio on the pool")
                if end_pg_count > new_pg_count:
                    log.error(
                        f"Count of PG's not changed/ reverted on the pool: {pool_name}"
                        f" after removing the target ratios"
                    )
                    return 1
                log.debug(
                    f"Deleting pool : {pool_name} & changing recovery options to default"
                )
                rados_obj.change_recovery_threads(config=config, action="rm")
                rados_obj.delete_pool(pool=entry["pool_name"])
                log.info(f"Completed the test of target ratio on pool: {pool_name} ")
        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here *************** \n \n"
            )
            # removal of rados pools
            rados_obj.rados_pool_cleanup()
            rados_obj.change_recovery_threads(config=config, action="rm")
            # log cluster health
            rados_obj.log_cluster_health()
            # check for crashes after test execution
            if rados_obj.check_crash_status():
                log.error("Test failed due to crash at the end of test")
                return 1

        log.info("Target ratio tests completed")
        return 0

    if config.get("verify_mon_target_pg_per_osd"):
        try:
            pg_conf = config.get("verify_mon_target_pg_per_osd")
            if not mon_obj.set_config(**pg_conf):
                log.error("Could not set the value for mon_target_pg_per_osd ")
                raise Exception("Could not set the value for mon_target_pg_per_osd ")
        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here *************** \n \n"
            )
            mon_obj.remove_config(**pg_conf)
            # log cluster health
            rados_obj.log_cluster_health()
            # check for crashes after test execution
            if rados_obj.check_crash_status():
                log.error("Test failed due to crash at the end of test")
                return 1
        log.info("Set and verified the value for mon_target_pg_per_osd ")
        return 0

    if config.get("verify_pg_num_min"):
        log.debug("Verifying pg_num_min on pools")
        target_configs = config["verify_pg_num_min"]["configurations"]
        try:
            # Creating pools and starting the test
            for entry in target_configs.values():
                log.debug(f"Creating {entry['pool_type']} pool on the cluster")
                if entry.get("pool_type", "replicated") == "erasure":
                    method_should_succeed(
                        rados_obj.create_erasure_pool, name=entry["pool_name"], **entry
                    )
                else:
                    method_should_succeed(
                        rados_obj.create_pool,
                        **entry,
                    )
                rados_obj.bench_write(**entry)

                if not rados_obj.set_pool_property(
                    pool=entry["pool_name"],
                    props="pg_num_min",
                    value=entry["pg_num_min"],
                ):
                    log.error("Could not set the pg_min_size on the pool")
                    raise Exception(
                        f"Could not set the pg_min_size on the pool {entry['pool_name']}"
                    )

                rados_obj.delete_pool(pool=entry["pool_name"])
                log.info(
                    f"Completed the test of pg_min_num on pool: {entry['pool_name']} "
                )
        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
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
            if rados_obj.check_crash_status():
                log.error("Test failed due to crash at the end of test")
                return 1
        log.info("pg_min_num tests completed")
        return 0

    if config.get("verify_pg_num_limit"):
        """
        This test is to verify the pg number limit for a pools.
        Script cover the following steps-
           1. Creating a pool with default pg number
           2. set the Logs
           3. Set noautoscale flag on
           4. Set the debug_mgr to 10
           5. Set the pg number more than the limit that is more than 128
           6. Check the logs for the proper message
        """
        # enable the file logging
        if not rados_obj.enable_file_logging():
            log.error("Error while setting config to enable logging into file")
            return 1
        log.info("Logging to file configured")
        pool_name = config.get("verify_pg_num_limit")["pool_name"]
        rados_obj.create_pool(pool_name)
        try:
            # Setting the no-autoscale flag
            cmd = f'{"ceph osd pool set noautoscale"}'
            rados_obj.run_ceph_command(cmd=cmd)

            # setting the debug_mgr to 10
            cmd = f'{"ceph tell mgr config set debug_mgr 10"}'
            rados_obj.run_ceph_command(cmd=cmd)

            # set the pg number more than 128
            cmd = f"ceph osd pool set {pool_name} pg_num 256"
            rados_obj.run_ceph_command(cmd=cmd)
            time.sleep(10)
            try:
                # checking the logs
                cmd = f'{"grep -rnw /var/log/ceph/ -e pg_num_actual"}'
                cephadm.shell([cmd])
            except Exception as err:
                log.error(f'{"Logs are not generated with pg_num_actual"}')
                log.error(err)
                raise
        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here *************** \n \n"
            )
            if config.get("delete_pool"):
                rados_obj.delete_pool(pool=pool_name)

            # Unsetting the no-autoscale flag
            cmd = f'{"ceph osd pool unset noautoscale"}'
            rados_obj.run_ceph_command(cmd=cmd)
            # unsetting the debug_mgr to default
            cmd = f'{"ceph config rm mgr debug_mgr"}'
            rados_obj.run_ceph_command(cmd=cmd)
            # log cluster health
            rados_obj.log_cluster_health()
            # check for crashes after test execution
            if rados_obj.check_crash_status():
                log.error("Test failed due to crash at the end of test")
                return 1

        log.info(" Verification of pg num checking completed")
        return 0

    # Blocked with bug : https://bugzilla.redhat.com/show_bug.cgi?id=2172795. Bug fixed.
    if config.get("verify_pool_warnings"):
        """
        Warnings Verified:
        1. POOL_APP_NOT_ENABLED
        2. POOL_TOO_FEW_PGS
        3. MANY_OBJECTS_PER_PG
        4. POOL_PG_NUM_NOT_POWER_OF_TWO
        5. POOL_TOO_MANY_PGS
        """
        log.debug(
            "Test to verify the warnings generated by autoscaler for PG count on pools"
        )
        pool_configs = config["verify_pool_warnings"]["pool_configs"]
        pool_configs_path = config["verify_pool_warnings"]["pool_configs_path"]

        try:
            with open(pool_configs_path, "r") as fd:
                pool_conf_file = yaml.safe_load(fd)

            pools = []
            for i in pool_configs:
                # Changing the default autoscaler value to warn for newly created pools
                rados_obj.configure_pg_autoscaler(default_mode="warn")

                # Checking if the new config is set for autoscaler module
                if not mon_obj.verify_set_config(
                    section="global",
                    name="osd_pool_default_pg_autoscale_mode",
                    value="warn",
                ):
                    log.error(
                        "unable to set the global autosclale mode to warn on the cluster"
                    )
                    raise Exception(
                        "Default PG autoscale mode not changed to warn on the cluster"
                    )

                pool = pool_conf_file[i["type"]][i["conf"]]
                pool["pg_num"] = 1
                if float(rhbuild.split("-")[0]) >= 7.1:
                    pool["app_name"] = None
                create_given_pool(rados_obj, pool)
                pools.append(pool["pool_name"])
                time.sleep(10)
                pool_autoscale_status = rados_obj.get_pg_autoscale_status(
                    pool_name=pool["pool_name"]
                )
                if pool_autoscale_status["pg_autoscale_mode"] != "warn":
                    log.error(
                        "Autoscale mode on the newly created pool is not 'warn' when global mode is set to warn"
                    )
                    raise Exception("PG autoscale mode not in warn mode on the pool")

                log.debug(
                    f"autoscale status on pools: \n{rados_obj.node.shell(['ceph osd pool autoscale-status'])}"
                )

                if float(rhbuild.split("-")[0]) >= 7.1:
                    log.debug("Bug fixed only in 7.1.")
                    # Scenario 1: Generate POOL_APP_NOT_ENABLED
                    log.debug(
                        "Checking if the warning for not enabling application on pool is generated"
                    )
                    POOL_APP_NOT_ENABLED = False
                    end_time = datetime.datetime.now() + datetime.timedelta(
                        seconds=1200
                    )
                    while end_time > datetime.datetime.now():
                        if rados_obj.check_health_warning(
                            warning="POOL_APP_NOT_ENABLED"
                        ):
                            POOL_APP_NOT_ENABLED = True
                            break
                        log.info(
                            "Health warning not generated on the cluster. Checking again in 20 seconds"
                        )
                        time.sleep(20)

                    if not POOL_APP_NOT_ENABLED:
                        log.error(
                            f"Warnings not generated on the cluster for app not enabled."
                            f"Ceph warnings : {rados_obj.log_cluster_health()}"
                        )
                        raise Exception(
                            "pool app not enabled warning not generated by cluster"
                        )

                    log.info(
                        f"Warnings generated on the cluster for app not enabled on pool : {pool['pool_name']}"
                    )
                    time.sleep(10)

                    # Scenario 2: Remove POOL_APP_NOT_ENABLED
                    # Enabling the application
                    log.debug("Enabling the application on the pool")
                    cmd = f"ceph osd pool application enable {pool['pool_name']} rados"
                    rados_obj.run_ceph_command(cmd=cmd, client_exec=True)
                    time.sleep(10)
                    POOL_APP_NOT_ENABLED = False
                    end_time = datetime.datetime.now() + datetime.timedelta(
                        seconds=1200
                    )
                    while end_time > datetime.datetime.now():
                        if not rados_obj.check_health_warning(
                            warning="POOL_APP_NOT_ENABLED"
                        ):
                            POOL_APP_NOT_ENABLED = True
                            break
                        log.debug(
                            "Health warning still present on the cluster. Checking again in 20 seconds"
                        )
                        time.sleep(20)

                    if not POOL_APP_NOT_ENABLED:
                        log.error(
                            "Warnings not removed on the cluster for application not enabled"
                            f"Ceph warnings : {rados_obj.log_cluster_health()}"
                        )
                        raise Exception("warning not removed by cluster")

                    log.info(
                        f"Warnings removed on the cluster for app not enabled on pool : {pool['pool_name']}"
                    )
                    time.sleep(10)
                else:
                    log.debug(
                        f"Skipped test for POOL_APP_NOT_ENABLED. build is : {rhbuild}"
                    )

                time.sleep(20)
                # Scenario 3: Generate POOL_TOO_FEW_PGS
                POOL_TOO_FEW_PGS = False
                end_time = datetime.datetime.now() + datetime.timedelta(seconds=2400)
                while end_time > datetime.datetime.now():
                    if rados_obj.check_health_warning(warning="POOL_TOO_FEW_PGS"):
                        POOL_TOO_FEW_PGS = True
                        break
                    log.debug(
                        "Health warning not generated on the cluster. Checking again in 20 seconds"
                    )
                    time.sleep(20)

                if not POOL_TOO_FEW_PGS:
                    log.error(
                        f"Warnings not generated on the cluster for too few PGs."
                        f"Ceph warnings : {rados_obj.log_cluster_health()}"
                    )
                    raise Exception("warning generated by cluster")

                log.info(
                    f"Warnings generated on the cluster for too few PGs on pool : {pool['pool_name']}"
                )
                time.sleep(10)

                # Scenario 4: Generate POOL_TOO_MANY_PGS
                POOL_TOO_MANY_PGS = False
                values = [256, 512, 1024]
                pool_val = rados_obj.get_pg_autoscale_status(
                    pool_name=pool["pool_name"]
                )
                value = 128
                for val in values:
                    if value <= pool_val["pg_num_final"]:
                        value = val
                log.debug(f"PG count selected to trigger too many pgs is : {value}")
                rados_obj.set_pool_property(
                    pool=pool["pool_name"], props="pg_num", value=value
                )
                rados_obj.set_pool_property(
                    pool=pool["pool_name"], props="pgp_num", value=value
                )
                end_time = datetime.datetime.now() + datetime.timedelta(seconds=2400)
                while end_time > datetime.datetime.now():
                    if rados_obj.check_health_warning(warning="POOL_TOO_MANY_PGS"):
                        POOL_TOO_MANY_PGS = True
                        break
                    log.debug(
                        "Health warning not generated on the cluster. Checking again in 20 seconds"
                    )
                    time.sleep(20)

                if not POOL_TOO_MANY_PGS:
                    log.error(
                        f"Warnings not generated on the cluster for too many PGs."
                        f"Ceph warnings : {rados_obj.log_cluster_health()}"
                    )
                    raise Exception("warning not generated by cluster")

                log.info(
                    f"Warnings generated on the cluster for too many PGs on pool :{pool['pool_name']}"
                )
                time.sleep(10)

                # Scenario 4: Generate POOL_PG_NUM_NOT_POWER_OF_TWO
                POOL_PG_NUM_NOT_POWER_OF_TWO = False
                rados_obj.set_pool_property(
                    pool=pool["pool_name"], props="pg_num", value=5
                )
                rados_obj.set_pool_property(
                    pool=pool["pool_name"], props="pgp_num", value=5
                )
                end_time = datetime.datetime.now() + datetime.timedelta(seconds=1200)
                while end_time > datetime.datetime.now():
                    if rados_obj.check_health_warning(
                        warning="POOL_PG_NUM_NOT_POWER_OF_TWO"
                    ):
                        POOL_PG_NUM_NOT_POWER_OF_TWO = True
                        break
                    log.debug(
                        "Health warning not generated on the cluster. Checking again in 20 seconds"
                    )
                    time.sleep(20)

                if not POOL_PG_NUM_NOT_POWER_OF_TWO:
                    log.error(
                        f"Warnings not generated on the cluster for non power of 2 no of PGs"
                        f"Ceph warnings : {rados_obj.log_cluster_health()}"
                    )
                    raise Exception("warning not generated by cluster")

                log.info(
                    f"Warnings generated on the cluster for PG count not power of 2 for pool :{pool['pool_name']}"
                )
                time.sleep(10)

                # Scenario 5: Generate MANY_OBJECTS_PER_PG
                MANY_OBJECTS_PER_PG = False
                end_time = datetime.datetime.now() + datetime.timedelta(seconds=1200)
                while end_time > datetime.datetime.now():
                    if rados_obj.check_health_warning(warning="MANY_OBJECTS_PER_PG"):
                        MANY_OBJECTS_PER_PG = True
                        break
                    log.debug(
                        "Health warning not generated on the cluster."
                        "Writing more objects and Checking again in 20 seconds"
                    )
                    rados_obj.bench_write(
                        pool_name=pool["pool_name"], byte_size="1Kb", max_objs=10000
                    )
                    time.sleep(20)

                if not MANY_OBJECTS_PER_PG:
                    log.error(
                        "Warnings not generated on the cluster for too many objects per pg."
                        f"Ceph warnings : {rados_obj.log_cluster_health()}"
                    )
                    raise Exception("warning generated by cluster")

                log.info(
                    f"Warnings generated on the cluster for too many objects on pool : {pool['pool_name']}"
                )

                # Scenario 6: Remove all warnings
                log.info(
                    f"Setting the PG autoscaler to 'ON' on pool {pool['pool_name']} "
                )
                rados_obj.set_pool_property(
                    pool=pool["pool_name"], props="pg_autoscale_mode", value="on"
                )
                MANY_OBJECTS_PER_PG = True
                end_time = datetime.datetime.now() + datetime.timedelta(seconds=1200)
                while end_time > datetime.datetime.now():
                    if not rados_obj.check_health_warning(
                        warning="MANY_OBJECTS_PER_PG"
                    ):
                        MANY_OBJECTS_PER_PG = False
                        break
                    log.debug(
                        "Health warning not removed on the cluster. Checking again in 20 seconds"
                    )
                    time.sleep(20)

                if MANY_OBJECTS_PER_PG:
                    log.error(
                        f"Warnings not removed on the cluster for too many objects per pg, post PG autoscaler ON"
                        f"Ceph warnings : {rados_obj.log_cluster_health()}"
                    )
                    raise Exception("warning generated by cluster")
                log.info(
                    "All warnings removed from the pool post enabling PG autoscaler to on"
                )
                log.debug("Deleting the pool created.")
                rados_obj.delete_pool(pool=pool["pool_name"])
                time.sleep(20)

            log.info("Completed all operations on the pool. Moving on to the next pool")
        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here *************** \n \n"
            )
            for pool in pools:
                rados_obj.delete_pool(pool=pool)
                log.debug(f"Completed deletion of pool {pool}")
            # reverting default pg autoscaler mode to ON
            rados_obj.configure_pg_autoscaler(default_mode="on")
            time.sleep(60)
            # log cluster health
            rados_obj.log_cluster_health()
            # check for crashes after test execution
            if rados_obj.check_crash_status():
                log.error("Test failed due to crash at the end of test")
                return 1
        return 0

    if config.get("Verify_pool_min_size_behaviour"):
        log.info("Test to verify that no Pools are able to serve clients at min_size")
        pool_name = config["Verify_pool_min_size_behaviour"]["pool_name"]

        try:
            rados_obj.create_pool(pool_name=pool_name)
            rados_obj.bench_write(pool_name=pool_name)

            # Getting a sample PG from the pool
            pool_id = pool_obj.get_pool_id(pool_name=pool_name)
            pgid = f"{pool_id}.0"
            pg_set = rados_obj.get_pg_acting_set(pg_num=pgid)

            # bringing down one OSD from the acting set at random
            osd_pick = random.choice(pg_set)
            stop_osd_check_warn(
                rados_obj=rados_obj, osd_pick=osd_pick, pool_name=pool_name, pgid=pgid
            )

            # Setting the size and min-size as 2. Even now the PGs should not go into inactive state.
            if not rados_obj.set_pool_property(pool=pool_name, props="size", value=2):
                log.error(f"failed to set size 2 on pool {pool_name}")
                raise Exception("Config set failed on pool")
            if not rados_obj.set_pool_property(
                pool=pool_name, props="min_size", value=2
            ):
                log.error(f"failed to set min_size 2 on pool {pool_name}")
                raise Exception("Config set failed on pool")

            pg_set = rados_obj.get_pg_acting_set(pg_num=pgid)
            # bringing down one OSD from the acting set at random
            osd_pick = random.choice(pg_set)
            stop_osd_check_warn(
                rados_obj=rados_obj, osd_pick=osd_pick, pool_name=pool_name, pgid=pgid
            )
            log.info("Tested the behaviour of pools upon same size and min-size")
        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here *************** \n \n"
            )
            # removal of rados pool
            rados_obj.delete_pool(pool=pool_name)
            time.sleep(60)
            # log cluster health
            rados_obj.log_cluster_health()
            # check for crashes after test execution
            if rados_obj.check_crash_status():
                log.error("Test failed due to crash at the end of test")
                return 1
        return 0

    if config.get("Verify_degraded_pool_min_size_behaviour"):
        log.info(
            "Test to verify that degraded Pools are not able to serve clients below min_size"
        )
        pool_target_configs = config["Verify_degraded_pool_min_size_behaviour"][
            "pool_config"
        ]
        pgid = ""
        osd_pick = ""
        second_osd_pick = ""
        rados_obj.configure_pg_autoscaler(default_mode="warn")
        for entry in pool_target_configs.values():
            pool_name = entry["pool_name"]
            try:
                method_should_succeed(rados_obj.create_pool, **entry)
                # Write IO to pool
                rados_obj.bench_write(
                    pool_name=pool_name,
                    rados_write_duration=50,
                    max_objs=50,
                    verify_stats=False,
                )

                log.info("Getting a sample PG from the pool")
                pool_id = pool_obj.get_pool_id(pool_name=pool_name)
                pgid = f"{pool_id}.0"
                pg_set = rados_obj.get_pg_acting_set(pg_num=pgid)
                log.info(
                    f"Pg set fetched from pool id {pool_id} for pg id {pgid} is {pg_set}"
                )
                osd_tree_op = rados_obj.run_ceph_command(cmd="ceph osd tree")
                log.info(
                    f"Before bringing down osds, output from ceph osd tree : {osd_tree_op}"
                    f"Setting norecovery, nonbackfill & no rebalance flags on the cluster so "
                    f"that the acting set does not change upon making OSD down"
                )

                rados_obj.change_recovery_flags(action="set", flags=["noout"])

                osd_pick = pg_set[0]
                second_osd_pick = pg_set[1]
                log.info(
                    f"Bringing down two OSDs from the acting set : {osd_pick} and {second_osd_pick}"
                )
                log.info(f"The OSD that will be remaining in the pg_set is : {pg_set}")
                if not rados_obj.change_osd_state(
                    action="stop", target=osd_pick, timeout=200
                ):
                    log.error(f"Failed to stop OSD : {osd_pick}")
                    raise Exception("test-bed set-up failure")

                log.info(
                    f"At size = min_size, i.e 1 OSD down writes would possible as well as reads."
                    f"Checking if read operations are allowed in pool {pool_name}"
                )

                if not pool_obj.do_rados_get(pool=pool_name, read_count=20):
                    log.error(
                        f"Could not perform read operations on the pool {pool_name} with 1 OSD down"
                    )
                    raise Exception("Reads failed on the cluster Error")

                if pool_obj.do_rados_put(
                    client=client_node, pool=pool_name, nobj=10, obj_name="test-obj-1"
                ):
                    log.error(
                        "UnAble to perform writes at min_size with 1 OSD down. Error"
                    )
                    raise Exception("Writes failed on the cluster Error")

                log.info(
                    "Able to perform read & writes operations on the cluster with 1 OSD down"
                    " Proceeding to bring down another OSD on the PG"
                )

                if not rados_obj.change_osd_state(
                    action="stop", target=second_osd_pick, timeout=200
                ):
                    log.error(f"Failed to stop OSD : {second_osd_pick}")
                    raise Exception("test-bed set-up failure")
                time.sleep(10)

                # Get the min_size property of the pool
                min_size_val = rados_obj.get_pool_property(
                    pool=pool_name, props="min_size"
                )
                log.info(f"Current min_size of the pool - {min_size_val}")
                post_pg_set = rados_obj.get_pg_acting_set(pg_num=pgid)
                log.info(
                    f"Acting set post 2 OSDs are down and before rados put op : {post_pg_set}"
                )
                if len(post_pg_set) != 1:
                    log.error("Acting set contains more than 1 OSD. Fail")
                    raise Exception("Acting set changed error")

                pool_stat_1 = rados_obj.get_cephdf_stats(pool_name=pool_name)
                total_objs_1 = pool_stat_1["stats"]["objects"]
                log.debug(
                    f"pool stats when 2 OSDs from AC is down before write is {pool_stat_1}. "
                    f"\n Total objects on the pool {total_objs_1}"
                )

                if pool_obj.do_rados_get(pool=pool_name, read_count=20):
                    log.error(
                        f"Could perform read operations on the pool {pool_name} with 2 OSDs down."
                        f"Should not be able to perform read operations"
                    )
                    raise Exception("Reads test failed on the cluster Error")

                if not pool_obj.do_rados_put(
                    client=client_node, pool=pool_name, nobj=5, obj_name="test-obj-2"
                ):
                    log.error(
                        f"Able to perform writes at min_size on the pool {pool_name}. with 2 OSDs down"
                        f"Should not be able to perform write operations"
                    )
                    raise Exception("Writes test failed on the cluster Error")

                log.info(
                    "Unable to perform read & write operations on the cluster as expected with 2 OSDs down"
                    " Proceeding to set min_size to 1 on the pool"
                )

                time.sleep(60)
                pool_stat_2 = rados_obj.get_cephdf_stats(pool_name=pool_name)
                total_objs_2 = pool_stat_2["stats"]["objects"]
                log.debug(
                    f"pool stats when 2 OSDs from AC is down post write is {pool_stat_2}\n"
                    f"No of objects post write with 2 OSD down : {total_objs_2}"
                )
                if total_objs_2 != total_objs_1:
                    log.error(
                        "Write ops should not have been possible, number of objects in the pool have changed"
                    )
                    raise Exception(
                        f"Pool {pool_name} has {pool_stat_2['stats']['objects']} objs |"
                        f" Expected {total_objs_1} objs, Actually present on pool : {total_objs_2}"
                    )

                ceph_health = rados_obj.run_ceph_command(cmd="ceph health detail")
                log.info(
                    f"ceph health details at the time of IO not being served - {ceph_health}"
                )
                log.info(
                    "RADOS PUT & GET op failed as expected since cluster is in degraded state!! "
                    "Modifying the min_size for the pool, so that read & write should be possible"
                )

                # Setting the min-size as 1. The IOs must be resumed and the PGs should become active+clean state.
                if not rados_obj.set_pool_property(
                    pool=pool_name, props="min_size", value=1
                ):
                    log.error(f"failed to set min_size 1 on pool {pool_name}")
                    raise Exception("Config set failed on pool")

                min_size_val = rados_obj.get_pool_property(
                    pool=pool_name, props="min_size"
                )
                log.info(f"min_size of the pool after modification - {min_size_val}")

                new_pg_set = rados_obj.get_pg_acting_set(pg_num=pgid)
                log.info(
                    f"Post modification, Pg set fetched from pool id {pool_id} for pg id {pgid} is {new_pg_set}"
                )
                if len(new_pg_set) == 1:
                    log.info(f"No change in acting set for pg {pgid}")
                else:
                    log.error("Acting set contains more than 1 OSD")
                    raise Exception("Aborting the test as acting set invalid")

                osd_tree_op = rados_obj.run_ceph_command(cmd="ceph osd tree")
                log.info(
                    f"After bringing down 2 osds, output from ceph osd tree : {osd_tree_op}"
                )
                time.sleep(10)

                # verify IOs are resumed after sometime post setting min_size to 1
                if not pool_obj.do_rados_get(pool=pool_name, read_count=20):
                    log.error(
                        f"Unable perform read operations on the pool {pool_name}"
                        f" with 2 OSD down and min_size set to 1"
                    )
                    raise Exception("Reads tests failed on the cluster Error")

                if pool_obj.do_rados_put(
                    client=client_node, pool=pool_name, nobj=5, obj_name="test-obj-3"
                ):
                    log.error(
                        f"Unable perform write operations on the pool {pool_name}"
                        f" with 2 OSD down and min_size set to 1"
                    )
                    raise Exception("Writes test failed on the cluster Error")

                log.info(
                    "With cluster in degraded state and OSDs in acting set equal to min_size, i.e 1 , IOs are served"
                )

                time.sleep(60)
                pool_stat_last = rados_obj.get_cephdf_stats(pool_name=pool_name)
                log.debug(pool_stat_last)
                if pool_stat_last["stats"]["objects"] <= total_objs_2:
                    log.error(
                        "Write ops should have been possible, number of objects in the pool have NOT changed"
                        "with min_size set to 1 with 1 OSD in AC"
                    )
                    raise Exception(
                        f"Pool {pool_name} has {pool_stat_last['stats']['objects']} objs"
                    )
                log.info(
                    "RADOS PUT op is successful as expected!! with min_size set to 1"
                )

                log.info(
                    "Tested the behaviour of cluster in degraded state and impact of reducing min-size"
                )
            except Exception as e:
                log.error(f"Exception hit in test flow: {e}")
                log.exception(e)
                return 1
            finally:
                log.info("\n\n\n\n ====== IN FINALLY BLOCK ====== \n\n\n\n")
                rados_obj.change_recovery_flags(action="unset", flags=["noout"])
                rados_obj.configure_pg_autoscaler(default_mode="on")
                # Starting the OSD back
                if not rados_obj.change_osd_state(action="restart", target=osd_pick):
                    log.error(f"Failed to start OSD : {osd_pick}")
                if not rados_obj.change_osd_state(
                    action="restart", target=second_osd_pick
                ):
                    log.error(f"Failed to start OSD : {second_osd_pick}")
                rados_obj.delete_pool(pool=pool_name)
                time.sleep(60)
                # log cluster health
                rados_obj.log_cluster_health()
            # check for crashes after test execution
            if rados_obj.check_crash_status():
                log.error("Test failed due to crash at the end of test")
                return 1
        return 0

    if config.get("Verify_osd_in_out_behaviour"):
        log.info(
            "Test to verify the behaviour of the OSD when it is marked out and back in to the cluster"
        )
        try:
            osd_tree = rados_obj.run_ceph_command(cmd="ceph osd tree")
            osd_list = []
            for entry in osd_tree["nodes"]:
                if entry["id"] > 0:
                    osd_list.append(entry["id"])
            selected_osd = random.choice(osd_list)

            log.info(
                f"OSDs on the cluster : {osd_list}\n Selected OSD for testing : {selected_osd}"
            )
            selected_osd_details_init = rados_obj.get_osd_details(osd_id=selected_osd)

            # Making the OSD out of the cluster
            if not rados_obj.update_osd_state_on_cluster(
                osd_id=selected_osd, state="out"
            ):
                log.error(f"OSD.{selected_osd} not marked out error")
                raise Exception("OSD not marked out error")
            log.info(f"OSD: {selected_osd} marked out successfully. ")

            selected_osd_details_post = rados_obj.get_osd_details(osd_id=selected_osd)

            if (
                selected_osd_details_post["crush_weight"]
                != selected_osd_details_init["crush_weight"]
            ):
                log.error(
                    f"Crush weight modified post marking the OSD.{selected_osd} out."
                    f"Before : {selected_osd_details_init['crush_weight']} "
                    f"After : {selected_osd_details_post['crush_weight']}"
                )
                raise Exception("OSD Crush weights modified error")

            if selected_osd_details_post["reweight"] != 0:
                log.error(
                    f"reweight not modified post marking the OSD.{selected_osd} out."
                    f"Before : {selected_osd_details_init['reweight']} "
                    f"After : {selected_osd_details_post['reweight']}"
                )
                raise Exception("OSD reweights not modified error")

            # Making the OSD in of the cluster
            if not rados_obj.update_osd_state_on_cluster(
                osd_id=selected_osd, state="in"
            ):
                log.error(f"OSD.{selected_osd} not marked in the cluster")
                raise Exception("OSD not marked in error")

            selected_osd_details_last = rados_obj.get_osd_details(osd_id=selected_osd)

            if (
                selected_osd_details_last["crush_weight"]
                != selected_osd_details_init["crush_weight"]
            ):
                log.error(
                    f"Crush weight modified post marking the OSD.{selected_osd} IN."
                    f"Before : {selected_osd_details_init['crush_weight']} "
                    f"After : {selected_osd_details_post['crush_weight']}"
                )
                raise Exception("OSD Crush weights modified error")

            if (
                selected_osd_details_last["reweight"]
                != selected_osd_details_init["reweight"]
            ):
                log.error(
                    f"reweight modified post marking the OSD.{selected_osd} IN."
                    f"Before : {selected_osd_details_init['reweight']} "
                    f"After : {selected_osd_details_last['reweight']}"
                )
                raise Exception("OSD reweights modified error")

            log.info(
                "Tested the behaviour of OSD upon marking it in and out of the cluster"
            )
        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here *************** \n \n"
            )
            if "selected_osd" in globals() or "selected_osd" in locals():
                rados_obj.update_osd_state_on_cluster(osd_id=selected_osd, state="in")

            time.sleep(30)
            # log cluster health
            rados_obj.log_cluster_health()
            # check for crashes after test execution
            if rados_obj.check_crash_status():
                log.error("Test failed due to crash at the end of test")
                return 1
        return 0

    if config.get("verify_pool_min_pg_count"):
        log.info(
            "test to verify if the PG autoscaler would scale up the PGs on the pool to pg_num_min"
            "Bugzilla verified : 2244985"
        )
        pools = []
        try:
            # Setting the global pg autoscale mode to on
            rados_obj.configure_pg_autoscaler(default_mode="on")
            pool_configs = config["verify_pool_min_pg_count"]["pool_configs"]
            pool_configs_path = config["verify_pool_min_pg_count"]["pool_configs_path"]
            pg_num_min = 128
            with open(pool_configs_path, "r") as fd:
                pool_conf_file = yaml.safe_load(fd)

            for i in pool_configs:
                pool = pool_conf_file[i["type"]][i["conf"]]
                pool["pg_num"] = 16
                pool["pg_num_min"] = pg_num_min
                pool_name = pool["pool_name"]
                create_given_pool(rados_obj, pool)
                pools.append(pool_name)
                rados_obj.bench_write(
                    pool_name=pool_name, max_objs=500, verify_stats=False
                )
                time.sleep(10)

                log.info(
                    f"Pool : {pool_name} created for testing with data written into it."
                    f"Checking if the pool will be autoscaled to pg_num_min, i.e 128"
                )
                init_pg_count = rados_obj.get_pool_property(
                    pool=pool_name, props="pg_num"
                )["pg_num"]
                log.debug(f"init PG count on the pool {pool_name} is: {init_pg_count}")
                pg_count_reached = False
                endtime = datetime.datetime.now() + datetime.timedelta(seconds=3600)
                while datetime.datetime.now() < endtime:
                    pool_pg_num = rados_obj.get_pool_property(
                        pool=pool_name, props="pg_num"
                    )["pg_num"]
                    if pool_pg_num >= pg_num_min:
                        log.info(
                            f"PG count on pool {pool_name} is reached pg_num_min count"
                        )
                        pg_count_reached = True
                        break
                    log.info(
                        f"PG count on pool {pool_name} has not reached pg_num_min count"
                        f"Expected : {pg_num_min}, Current : {pool_pg_num}"
                    )
                    log.debug(
                        "Sleeping for 30 secs and checking the PG states and PG count"
                    )
                    time.sleep(30)
                if not pg_count_reached:
                    log.error(
                        f"pg_num on pool {pool_name} did not reach the desired levels of PG count with pg_min_num"
                        f"Expected : {pg_num_min}"
                    )
                    raise Exception("pg_num_min not achieved")
                rados_obj.delete_pool(pool=pool_name)
                log.info(
                    f"PG count on the pool reached to pg_num_min levels for pool {pool_name}"
                )

        except Exception as err:
            log.error(f"Hit exception during the test execution. Error : {err}")
            return 1
        finally:
            log.info(
                "\n \n ************** Execution of finally block begins here *************** \n \n"
            )
            for pool in pools:
                rados_obj.delete_pool(pool=pool)

            time.sleep(60)
            # log cluster health
            rados_obj.log_cluster_health()
            # check for crashes after test execution
            if rados_obj.check_crash_status():
                log.error("Test failed due to crash at the end of test")
                return 1

        log.info(" Verification of pg_min_num complete")
        return 0


def stop_osd_check_warn(rados_obj, osd_pick, pool_name, pgid):
    """Method to check health warning upon OSD failure

    This method checks for the PG state, when an OSD is brought down.
    The OSD state should not enter inactive state.
    Args:
        rados_obj: cluster object used to run commands and other functionality
        osd_pick: OSD ID which should be stopped and started
        pool_name: Pool on which IO should be run
        pgid: PG ID on which the state should be checked
    Examples:
        stop_osd_check_warn(
            rados_obj=rados_obj,
            osd_pick=2,
            pool_name="test_pool",
            pgid="2.3"
        )

    Returns: None
    """
    if not rados_obj.change_osd_state(action="stop", target=osd_pick):
        log.error(f"Failed to stop OSD : {osd_pick}")
        raise Exception("test-bed set-up failure")

    rados_obj.bench_write(pool_name=pool_name)

    # Checking the PG state. There Should not be inactive state
    pg_state = rados_obj.get_pg_state(pg_id=pgid)
    if any("inactive" in key for key in pg_state.split("+")):
        log.error(f"PG: {pgid} in inactive state after OSD down)")
        raise Exception("PG Inactive. Test Fail")

    # Starting the OSD back
    if not rados_obj.change_osd_state(action="restart", target=osd_pick):
        log.error(f"Failed to stop OSD : {osd_pick}")
        raise Exception("test-bed set-up failure")


def check_pg_warning(rados_obj, pools) -> bool:
    """Method to check if Health warn for PG count is generated.

    This method collects the ceph report, from which it searches for a particular warning,
    for a specified amount of time, running rados bench.
    Example:
            check_pg_warning(rados_obj=rados_obj, pools=pools)
        Args:
            rados_obj: cluster object used to run commands and other functionality
            pools: list of pools on which IO should be run
    Returns:
        Pass -> True, warning present
        Fail -> False warning not present

    """
    end_time = datetime.datetime.now() + datetime.timedelta(seconds=1200)
    flag = False
    while end_time > datetime.datetime.now():
        status_report = rados_obj.run_ceph_command(cmd="ceph report")
        ceph_health_status = status_report["health"]
        log.debug(f"health warnings present on the cluster are : {ceph_health_status}")
        health_warn = "POOL_TOO_FEW_PGS"
        # Checking for any health warnings
        flag = True if health_warn in ceph_health_status["checks"].keys() else False

        if flag:
            log.info("Warning Generated for pool having less than ideal no of PGs")
            break

        time.sleep(10)
        log.info("Waiting for alert to be generated for the PG count increase")
        for pname in pools:
            rados_obj.bench_write(
                pool_name=pname, byte_size="1Kb", rados_write_duration=20
            )

    if not flag:
        log.error(
            "PG count warning not generated for the pools after waiting 5400 seconds"
        )
        return False
    log.info("Warning generated on the cluster")
    return True
