"""
Module to test pool EC pool, based of CLAY profile

Tests Performed:
1. Create EC Pool with CLAY Profile
2. Create replicated metadata pool and use the Clay poll created as data pool for RBD Images
3. Test the pool with various autoscaler values
4. Test the pool with Bulk flags ( PG Splits / Merge )
5. Scrubs and Deep Scrubs on the pool
6. Enable and verify various inline compression algorithms
7. Perform Repair on the PGs on Clay pool
8. Recovery with only K shards of EC Pool
"""

import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw) -> int:
    """
    Module to test pool EC pool, based of CLAY profile
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)

    try:
        ec_config = config["clay_pool"]
        pool_name = ec_config["pool_name"]
        if not rados_obj.create_erasure_pool(
            name=ec_config["profile_name"], **ec_config
        ):
            log.error("Failed to create the CLAY EC Pool")
            raise Exception("Pool creation failed!")

        log.info("Successfully created Clay pool. writing some objects into pool")
        if ec_config.get("test_overwrites_pool"):
            if not rados_obj.verify_ec_overwrites(**ec_config):
                log.error("Failed to create image on the pool and write data")
                raise Exception("Failed to configure RBD Images on the pool")
        else:
            rados_obj.bench_write(**ec_config)
            rados_obj.bench_read(**ec_config)

        if ec_config.get("test_tier3_system_tests"):
            # Changing the autoscaler status and to all possible values and writing data.
            autoscaler_values = ["on", "warn", "off"]
            for values in autoscaler_values:
                if not rados_obj.set_pool_property(
                    pool=pool_name, props="pg_autoscale_mode", value=values
                ):
                    log.error(
                        f"failed to set pg_autoscale_mode {values} on the clay pool : {pool_name}"
                    )
                    raise Exception("failed to set pool property!")

                # Sleeping for 1 min for new configs to be applied by the cluster
                time.sleep(60)
                # error checks
                if not rados_obj.run_pool_sanity_check():
                    log.error("Checks failed post config update")
                    raise Exception("Pool Sanity checks failed")

            rados_obj.set_pool_property(
                pool=pool_name, props="pg_autoscale_mode", value="on"
            )
            log.info(
                f"made autoscaler config changes on the pool {pool_name} and completed sanity checks"
            )

            # Changing the bulk settings on the pool. The PG splits and Merges should happen as expected
            if pool_obj.get_bulk_details(pool_name=pool_name):
                log.info("pool has bulk flag enabled, disabling and testing PG merges")
                if not pool_obj.rm_bulk_flag(pool_name=pool_name):
                    log.error("Failed to remove the bulk flag")
                    raise Exception("Config change error")
                # Sleeping for 1 min for new PG numbers to be calculated by the cluster
                time.sleep(60)
                method_should_succeed(
                    wait_for_clean_pg_sets, rados_obj, test_pool=pool_name
                )
            else:
                if not pool_obj.set_bulk_flag(pool_name=pool_name):
                    log.error("Failed to set the bulk flag")
                    raise Exception("Config change error")

                # Sleeping for 1 min for new PG numbers to be calculated by the cluster
                time.sleep(60)
                method_should_succeed(
                    wait_for_clean_pg_sets, rados_obj, test_pool=pool_name
                )
            # error checks
            if not rados_obj.run_pool_sanity_check():
                log.error("Checks failed post config update")
                raise Exception("Pool Sanity checks failed")
            log.info(
                f"made bulk flag changes on the pool {pool_name} and completed sanity checks"
            )

            # Changing the various compression algorithms and writing data
            if ec_config.get("test_compression"):
                compression_conf = ec_config["test_compression"]
                for conf in compression_conf["configurations"]:
                    for entry in conf.values():
                        if not rados_obj.pool_inline_compression(
                            pool_name=pool_name, **entry
                        ):
                            log.error(
                                f"Error setting compression on pool : {pool_name} for config {conf}"
                            )
                            raise Exception("Compression test failed")

                    # Sleeping for 1 min for new configs to be applied by the cluster
                    time.sleep(60)

                    clay_rbd_write(cephadm=cephadm, **ec_config)
                    # error checks
                    if not rados_obj.run_pool_sanity_check():
                        log.error("Checks failed post config update")
                        raise Exception("Pool Sanity checks failed")
                log.info("Completed compression tests")

            # Performing PG repairs and performing pool sanity check
            # Collecting pool ID and initiating repair on all PGs of the pool
            pool_id = pool_obj.get_pool_id(pool_name=pool_name)
            cmd = "ceph pg dump pgs"
            pg_dump = rados_obj.run_ceph_command(cmd=cmd)
            for entry in pg_dump["pg_stats"]:
                if str(entry["pgid"]).startswith(str(pool_id)):
                    cmd = f"ceph pg repair {entry['pgid']}"
                    cephadm.shell([cmd])
                    log.debug(f"Instructed PG {entry['pgid']} to repair")
            log.info("Completed instruction of repair on all PGs of the pool")
            # Sleeping for 1 min for new configs to be applied by the cluster
            time.sleep(60)

            # Performing writes and then pool sanity checks
            clay_rbd_write(cephadm=cephadm, **ec_config)
            if not rados_obj.run_pool_sanity_check():
                log.error("Checks failed post config update")
                raise Exception("Pool Sanity checks failed")
            log.info("Completed pool repairs successfully")

            # Testing the recovery of Clay based EC pools
            # getting the acting set for the created pool
            acting_pg_set = rados_obj.get_pg_acting_set(pool_name=pool_name)

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
                    raise Exception("Execution error")

            log.debug("Stopped 'm' number of OSD's from, starting to wait for recovery")

            # Sleeping for 25 seconds ( "osd_heartbeat_grace": "20" )
            time.sleep(25)

            # Waiting for recovery to complete after making M OSDs down
            method_should_succeed(pool_obj.wait_for_clean_pool_pgs, pool_name=pool_name)

            # getting the acting set for the created pool after recovery
            acting_pg_set = rados_obj.get_pg_acting_set(
                pool_name=ec_config["pool_name"]
            )
            if len(acting_pg_set) != ec_config["k"] + ec_config["m"]:
                log.error(
                    f"acting set consists of only these : {acting_pg_set} OSD's, less than k+m"
                )
                raise Exception("Test failure")
            log.info(f" Acting set of the pool consists of OSD's : {acting_pg_set}")

            log.debug("Starting the stopped OSD's")
            for osd_id in stop_osds:
                if not rados_obj.change_osd_state(action="restart", target=osd_id):
                    log.error(f"Unable to restart the OSD : {osd_id}")
                    raise Exception("Execution error")

            # Sleeping for 1 min for OSDs to join the cluster
            time.sleep(60)
            log.info("Successfully tested EC pool recovery with K osd's surviving")

            # All tests completed
            log.info("Completed system checks on the pools")
    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        if ec_config.get("delete_pools"):
            rados_obj.delete_pool(pool=pool_name)
            if ec_config.get("test_overwrites_pool"):
                rados_obj.delete_pool(pool=ec_config["metadata_pool"])
            log.debug("All the pools created for testing deleted")

        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
    log.info("Completed testing Clay profile based EC pools!!")
    return 0


def clay_rbd_write(cephadm, **kwargs):
    """Method to write objects into RBD image where the data pool is backed by a EC pool
    Args:
        cephadm: Cluster object to connect and execute commands on the cluster
        **kwargs: Other params that need to be passed to the method
            pool_name: Name of the EC Pool
            image_name: Name of the data pool Image
    Returns: None
    """
    pool_name = kwargs["metadata_pool"]
    image_name = kwargs.get("image_name")
    cmd = f"rbd bench-write {image_name} --pool={pool_name}"
    cephadm.shell([cmd])
