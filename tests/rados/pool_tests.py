"""
This file contains various tests/ validations related to pools.
Tests included:
1. Verification of EC pool recovery improvement
2. Effect on size of pools with and without compression
"""
import datetime
import logging
import re
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonConfigMethods

log = logging.getLogger(__name__)


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

    if config.get("check_autoscaler_profile"):
        """
        Verifies that the default auto-scaler profile on 5.1 builds in scale-up
        Verifies bugs : 1. https://bugzilla.redhat.com/show_bug.cgi?id=2021738
        """
        build = config.get("build", config.get("rhbuild"))
        autoscale_conf = config.get("check_autoscaler_profile")
        regex = r"5.1-rhel-\d{1}"
        if re.search(regex, build):
            log.info(
                "Test running on 5.1 builds, checking the default autoscaler profile"
            )
            if not mon_obj.verify_set_config(**autoscale_conf):
                log.error(
                    f"The default value for autoscaler profile is not scale-up in buld {build}"
                )
                return 1
            log.info(f"Autoscale profile is scale-up in release : {build}")
        else:
            log.debug(
                f"The profile is already scale-up by default in release : {build}"
            )
        return 0
