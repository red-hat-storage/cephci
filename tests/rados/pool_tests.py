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
from tests.rados.mute_alerts import get_alerts
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
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    pool_obj = PoolFunctions(node=cephadm)

    if config.get("ec_pool_recovery_improvement"):
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
        acting_pg_set = rados_obj.get_pg_acting_set(pool_name=ec_config["pool_name"])
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
        rados_obj.change_recover_threads(config=ec_config, action="set")

        # Sleeping for 25 seconds ( "osd_heartbeat_grace": "20" ) for osd's to be marked down
        time.sleep(25)

        # Waiting for up to 2.5 hours for the recovery to complete and PG's to enter active + Clean state
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=9000)
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
        acting_pg_set = rados_obj.get_pg_acting_set(pool_name=ec_config["pool_name"])
        if len(acting_pg_set) != ec_config["k"] + ec_config["m"]:
            log.error(
                f"acting set consists of only these : {acting_pg_set} OSD's, less than k+m"
            )
            return 1
        log.info(f" Acting set of the pool consists of OSD's : {acting_pg_set}")
        # Changing recovery threads back to default
        rados_obj.change_recover_threads(config=ec_config, action="rm")

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
        if not rados_obj.detete_pool(pool=ec_config["pool_name"]):
            log.error(f"the pool {ec_config['pool_name']} could not be deleted")
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
        if not rados_obj.bench_write(pool_name=pool_1, **pool_config):
            log.error("Failed to write objects into Pool-1, with compression enabled")
            return 1

        if not rados_obj.bench_write(pool_name=pool_2, **pool_config):
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

        log.info("Pool size is less when compression is enabled")
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
        regex = r"\s*(\d.\d)-rhel-\d"
        build = (re.search(regex, config.get("build", config.get("rhbuild")))).groups()[
            0
        ]
        if not float(build) > 5.0:
            log.info(
                "Test running on version less than 5.1, skipping verifying bulk flags"
            )
            return 0

        # Creating a pool with bulk feature
        pool_name = config.get("pool_name")
        if not pool_obj.set_bulk_flag(pool_name=pool_name):
            log.error("Failed to create a pool with bulk features")
            return 1

        # Checking the autoscaler status, final PG counts, bulk flags
        pg_target_init = pool_obj.get_target_pg_num_bulk_flag(pool_name=pool_name)

        # Unsetting the bulk flag and checking the change in the PG counts
        if not pool_obj.rm_bulk_flag(pool_name=pool_name):
            log.error("Failed to create a pool with bulk features")
            return 1

        # Sleeping for 5 seconds for new PG num to bets et
        time.sleep(5)
        pg_target_interim = pool_obj.get_target_pg_num_bulk_flag(pool_name=pool_name)

        # The target PG's once the flag is disabled must be lesser than when enabled
        if pg_target_interim >= pg_target_init:
            log.error("PG's not reduced after bulk flag disabled")
            return 1

        # Setting the bulk flag on pool again and checking the change in the PG counts
        if not pool_obj.set_bulk_flag(pool_name=pool_name):
            log.error("Failed to disable/remove bulk features on pool")
            return 1

        # Sleeping for 5 seconds for new PG num to bets et
        time.sleep(5)

        pg_target_final = pool_obj.get_target_pg_num_bulk_flag(pool_name=pool_name)

        # The target PG's once the flag is disabled must be lesser than when enabled
        if pg_target_interim >= pg_target_final:
            log.error("PG's not Increased after bulk flag Enabled")
            return 1

        if config.get("delete_pool"):
            rados_obj.detete_pool(pool=pool_name)
        log.info("Verified the workings of bulk flag")
        return 0

    if config.get("verify_pool_target_ratio"):
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
            if not pool_obj.verify_target_ratio_set(
                pool_name=entry["pool_name"], ratio=entry["target_size_ratio"]
            ):
                log.error(
                    f"Could not change the target ratio on the pool: {entry['pool_name']}"
                )
                return 1
            log.debug("Set the ratio. getting the projected pg's")

            rados_obj.change_recover_threads(config=config, action="set")
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
            if new_pg_count <= entry["pg_num"]:
                log.error(
                    f"Count of PG's not increased on the pool: {entry['pool_name']}"
                    f"Initial creation count : {entry['pg_num']}"
                    f"New count after setting num target : {new_pg_count}"
                )
                return 1

            res = wait_for_clean_pg_sets(rados_obj)
            if not res:
                log.error(
                    "PG's in cluster are not active + Clean after the ratio change"
                )
                return 1
            if not pool_obj.verify_target_ratio_set(
                pool_name=entry["pool_name"], ratio=0.0
            ):
                log.error(
                    f"Could not remove the target ratio on the pool: {entry['pool_name']}"
                )
                return 1

            # Sleeping for 2 minutes for rebalancing to start & for new PG count to be updated.
            time.sleep(120)
            # Checking if after the removal of ratio, the PG count has reduced
            end_pg_count = int(
                pool_obj.get_pg_autoscaler_value(
                    pool_name=entry["pool_name"], item="pg_num_target"
                )
            )
            if end_pg_count >= new_pg_count:
                log.error(
                    f"Count of PG's not changed/ reverted on the pool: {entry['pool_name']}"
                    f" after removing the target ratios"
                )
                return 1
            rados_obj.change_recover_threads(config=config, action="rm")
            if entry.get("delete_pool", False):
                rados_obj.detete_pool(pool=entry["pool_name"])
            log.info(
                f"Completed the test of target ratio on pool: {entry['pool_name']} "
            )
        log.info("Target ratio tests completed")
        return 0

    if config.get("verify_mon_target_pg_per_osd"):
        pg_conf = config.get("verify_mon_target_pg_per_osd")
        if not mon_obj.set_config(**pg_conf):
            log.error("Could not set the value for mon_target_pg_per_osd ")
            return 1
        mon_obj.remove_config(**pg_conf)
        log.info("Set and verified the value for mon_target_pg_per_osd ")
        return 0

    if config.get("verify_pg_num_min"):
        log.debug("Verifying pg_num_min on pools")
        target_configs = config["verify_pg_num_min"]["configurations"]
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
                pool=entry["pool_name"], props="pg_num_min", value=entry["pg_num_min"]
            ):
                log.error("Could not set the pg_min_size on the pool")
                return 1

            if entry.get("delete_pool", False):
                rados_obj.detete_pool(pool=entry["pool_name"])
            log.info(f"Completed the test of pg_min_num on pool: {entry['pool_name']} ")
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
            return 1
        if config.get("delete_pool"):
            rados_obj.detete_pool(pool=pool_name)

            # Unsetting the no-autoscale flag
            cmd = f'{"ceph osd pool unset noautoscale"}'
            rados_obj.run_ceph_command(cmd=cmd)
            # setting the debug_mgr to default
            cmd = f'{"ceph tell mgr config set debug_mgr 2"}'
            rados_obj.run_ceph_command(cmd=cmd)
        log.info(" Verification of pg num checking completed")
        return 0

    # Blocked with bug : https://bugzilla.redhat.com/show_bug.cgi?id=2172795
    if config.get("verify_autoscaler_warn"):
        log.debug(
            "Test to verify the warnings generated by autoscaler for PG count on pools"
        )
        pool_configs = config["verify_autoscaler_warn"]["pool_configs"]
        pool_configs_path = config["verify_autoscaler_warn"]["pool_configs_path"]

        with open(pool_configs_path, "r") as fd:
            pool_conf_file = yaml.safe_load(fd)

        # Changing the default autoscaler value to warn for newly created pools
        rados_obj.configure_pg_autoscaler(default_mode="warn")

        pools = []
        for i in pool_configs:
            pool = pool_conf_file[i["type"]][i["conf"]]
            pool["pg_num"] = 1
            create_given_pool(rados_obj, pool)
            pools.append(pool["pool_name"])
        log.info(f"Created {len(pools)} pools for testing. pools : {pools}")

        # Starting to write data continuously onto the two pools and checking for generation of warning
        for pname in pools:
            rados_obj.bench_write(
                pool_name=pname, byte_size="1Kb", rados_write_duration=300
            )

        if check_pg_warning(rados_obj, pools):
            log.error("PG count warning not generated for the pools")
            raise Exception("No warning generated by PG Autoscaler")

        # Changing the default autoscaler value to warn for newly created pools
        rados_obj.configure_pg_autoscaler(default_mode="on")
        for pool in pools:
            rados_obj.set_pool_property(
                pool=pool, props="pg_autoscale_mode", value="on"
            )

        # Starting to write data continuously onto the two pools
        for pname in pools:
            rados_obj.bench_write(pool_name=pname, byte_size="1Kb")

        # Now that the autoscaler has been turned on, the warnings should go and PGs should be scaled accordingly
        time.sleep(300)

        flag = True
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=2000)
        while end_time > datetime.datetime.now():
            alerts = get_alerts(cephadm)
            if "POOL_TOO_FEW_PGS" in alerts["active_alerts"]:
                log.info("Warning for PG could not cleared yet.")
                flag = False
                time.sleep(20)
            else:
                log.info("Alert cleared on the pools")
                flag = True

        if not flag:
            log.error("The warning for the PG count is not yet cleared.")
            raise Exception("POOL_TOO_FEW_PGS present on the cluster")

        log.info(
            "Warning generated for pool having less than ideal no of PGs "
            "and PG autoscaler has scaled it accordingly"
        )
        return 0

    if config.get("Verify_pool_min_size_behaviour"):
        log.info("Test to verify that no Pools are able to serve clients at min_size")
        pool_name = config["Verify_pool_min_size_behaviour"]["pool_name"]
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
        if not rados_obj.set_pool_property(pool=pool_name, props="min_size", value=2):
            log.error(f"failed to set min_size 2 on pool {pool_name}")
            raise Exception("Config set failed on pool")

        pg_set = rados_obj.get_pg_acting_set(pg_num=pgid)
        # bringing down one OSD from the acting set at random
        osd_pick = random.choice(pg_set)
        stop_osd_check_warn(
            rados_obj=rados_obj, osd_pick=osd_pick, pool_name=pool_name, pgid=pgid
        )
        log.info("Tested the behaviour of pools upon same size and min-size")

        rados_obj.detete_pool(pool=pool_name)
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

    This methods collects the ceph report, from which it searches for a particular warning,
    for a specified amount of time, running rados bench.
    Example:
            check_pg_warning(rados_obj=rados_obj, pools=pools)
        Args:
            rados_obj: cluster object used to run commands and other functionality
            pools: list of pools on which IO should be run
    Returns:
        Pass -> True, Fail -> False

    """
    end_time = datetime.datetime.now() + datetime.timedelta(seconds=5400)
    while end_time > datetime.datetime.now():
        status_report = rados_obj.run_ceph_command(cmd="ceph report")
        ceph_health_status = status_report["health"]
        health_warn = "POOL_TOO_FEW_PGS"
        # Checking for any health warnings
        flag = False if health_warn in ceph_health_status["checks"].keys() else True

        if flag:
            log.info("Warning Removed for pool having less than ideal no of PGs")
            break

        time.sleep(10)
        log.info("Waiting for alert to be generated for the PG count")
        for pname in pools:
            rados_obj.bench_write(pool_name=pname, byte_size="100Kb")

    if not flag:
        log.error("PG count warning not generated for the pools")
        raise Exception("No warning generated by PG Autoscaler")

    return True
