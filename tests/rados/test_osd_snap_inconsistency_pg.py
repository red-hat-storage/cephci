"""
This file contains the  methods to verify the  inconsistent objects.
AS part of verification the script  perform the following tasks-
   1. Creating omaps
   2. Convert the object in to inconsistent object by creating the pool snapshot
"""

import random
import time
import traceback

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test to create an inconsistent object and verify that objects details.
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]

    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    method_should_succeed(wait_for_clean_pg_sets, rados_obj)
    pool_target_configs = config["verify_osd_omap_entries"]["configurations"]
    omap_target_configs = config["verify_osd_omap_entries"]["omap_config"]
    test_secondary = config.get("test_secondary", False)
    if test_secondary:
        log.debug("Passed param to run the test on secondary OSD of the acting set")

    pools = []
    try:
        # Creating pools and starting the test
        for entry in pool_target_configs.values():
            log.debug(
                f"Creating {entry['pool_type']} pool on the cluster with name {entry['pool_name']}"
            )
            if entry["pool_type"] == "replicated":
                method_should_succeed(
                    rados_obj.create_pool,
                    **entry,
                )
            else:
                method_should_succeed(
                    rados_obj.create_erasure_pool,
                    name=entry["pool_name"],
                    **entry,
                )
            pool_name = entry["pool_name"]
            pool_type = entry["pool_type"]
            pools.append(pool_name)
            log.info(
                f"Created the pool {entry['pool_name']}. beginning to create large number of entries on the pool"
            )

            if pool_type == "replicated":
                # Creating omaps on Replicated pools
                if not pool_obj.fill_omap_entries(
                    pool_name=pool_name, **omap_target_configs
                ):
                    log.error(f"Omap entries not generated on pool {pool_name}")
                    return 1
            else:
                # Creating rados bench objects
                if not rados_obj.bench_write(
                    pool_name=pool_name,
                    byte_size="1000KB",
                    max_objs=50,
                    rados_write_duration=30,
                    verify_stats=False,
                ):
                    log.error(f"Objects not written on pool {pool_name}")
                    return 1

        pool_name = random.choice(pools)
        obj_list = rados_obj.get_object_list(pool_name)
        oname = random.choices(obj_list, k=3)
        snapshot_name = pool_obj.create_pool_snap(pool_name=pool_name)
        if not snapshot_name:
            log.error("Cannot able to create the pool snapshot")
            return 1
        log.info(f"The pool snapshot created with the name-{snapshot_name}")

        # Checking if the inconsistent PG needs to be generated on primary or secondary PGs
        # Create inconsistency objects
        inconsistent_chk_flag = False
        for object in oname:
            pg_id = rados_obj.create_inconsistent_obj_snap(
                pool_name, object, secondary=test_secondary
            )
            if pg_id is not None:
                inconsistent_chk_flag = True
                break
        if inconsistent_chk_flag is False:
            log.error("Cannot able to create the inconsistent PG")
            return 1
        inconsistent_pg_list = rados_obj.get_inconsistent_pg_list(pool_name)
        if any(pg_id in search for search in inconsistent_pg_list):
            log.info(f"Inconsistent PG is{pg_id}  ")
        else:
            log.error("Inconsistent PG is not generated")
            return 1
        # check for the crashes in the cluster
        crash_list = rados_obj.do_crash_ls()
        if crash_list:
            log.error(f"Noticed crashes in the cluster.The crash list is- {crash_list}")
            return 1
        log.info("Did not noticed any crashes in the cluster")
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info("Execution of finally block")
        if config.get("delete_pool"):
            method_should_succeed(pool_obj.delete_pool_snap, pool_name)
            method_should_succeed(rados_obj.delete_pool, pool_name)
            time.sleep(5)
            log.info("deleted the snapshot and pool successfully")
            method_should_succeed(wait_for_clean_pg_sets, rados_obj)
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
    return 0
