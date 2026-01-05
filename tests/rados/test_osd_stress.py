"""
Module to perform stress testing on OSD along side multiple OMAP
manipulations.
"""

import datetime
import random
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.perf_workflow import PerfWorkflows
from ceph.rados.pool_workflows import PoolFunctions
from ceph.rados.utils import get_cluster_timestamp
from tests.rados.test_rados_perf import PoolCreationFailed
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    # CEPH-83632238
    Covers:
        - BZ-2360911
        - BZ-2359773
        - BZ-2354192
    Test to perform stress testing on OSD with background Omap I/O
    Steps
    1. Deploy a ceph cluster
    2. Create a replicated and EC pool for data
    3. Create a series of replicated pools for metadata (OMAP data)
    4. Fill the replicated pool and EC pool till 20% pool capacity each
    5. Write OMAP data to each omap replicated pool till 60% cluster capacity is reached
    6. Log OSD fragmentation score -> perform OSD compaction -> Log OSD fragmentation score
    7. Remove half of omap replicated pools
    8. Re-create removed replicated pools and restart OMAP IOs
    9. Perform background IOs using rados bench with cleanup for defined duration
    10. Repeat steps 5-8 in loop for defined test duration
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    perf_obj = PerfWorkflows(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    omap_pools = []

    log.info(
        "Running test case to perform stress testing on OSD around OMAPs manipulation"
    )

    start_time = get_cluster_timestamp(rados_obj.node)
    log.debug(f"Test workflow started. Start time: {start_time}")
    try:
        stress_test_duration = config["stress_config"]["duration"]
        pool_configs = config.get("pool_config")
        log.info(pool_configs)
        # Create a replicated and EC pool for data
        for pool_cfg in pool_configs.values():
            log.info(pool_cfg)
            if not perf_obj.create_pool_per_config(**pool_cfg):
                log.error("Creation of required pool has failed")
                raise PoolCreationFailed
            log.info("Created data pool %s successfully" % pool_cfg["pool_name"])

        omap_pool_count = config["omap_config"].get("pool_count", 10)
        log.debug("Creating %s pools to wrie OMAP data" % omap_pool_count)

        # Create a series of replicated pools for metadata (OMAP data)
        for i in range(omap_pool_count):
            omap_pool_name = f"omap_pool_{i}"
            omap_pools.append(omap_pool_name)
            rados_obj.create_pool(pool_name=omap_pool_name)
            log.info(
                "Created replicated pool %s for omap successfully" % omap_pool_name
            )

        # Start RADOS Bench IO to fill the cluster
        for pool_cfg in pool_configs.values():
            _pool = pool_cfg["pool_name"]
            log.debug(
                "Starting rados bench IO to fill the pool %s till 20%% capacity" % _pool
            )
            obj_size = "16MB"
            # determine 20% of pool capacity
            obj_count = perf_obj.get_object_count(
                fill_percent=20, pool_name=_pool, obj_size=obj_size, num_client=1
            )

            bench_cfg = {
                "rados_write_duration": 3600,
                "byte_size": obj_size,
                "max_objs": obj_count,
            }
            rados_obj.bench_write(pool_name=_pool, **bench_cfg)
            log.info("RADOS bench IO completed on pool" + _pool)

            # start background rados bench IOs on the pool
            for obj_size in ["4KB", "16KB", "32KB", "256KB"]:
                rados_obj.bench_write(
                    pool_name=_pool,
                    byte_size=obj_size,
                    rados_write_duration=stress_test_duration,
                    background=True,
                    nocleanup=False,
                )

        end_time = datetime.datetime.now() + datetime.timedelta(
            seconds=stress_test_duration
        )
        while end_time > datetime.datetime.now():
            # proceed to write OMAP data on the pools
            for omap_pool in omap_pools:
                omap_cfg = config["omap_config"]
                omap_cfg["num_keys_obj"] = random.randint(100, omap_cfg["num_keys_obj"])
                if not pool_obj.fill_omap_entries(pool_name=omap_pool, **omap_cfg):
                    log.error("Omap entries not generated on pool %s", omap_pool)
                    raise Exception(f"Omap entries not generated on pool {omap_pool}")

            up_osds = rados_obj.get_osd_list(status="up")
            log.debug(f"List of 'UP' OSDs: \n{up_osds}")
            for osd in up_osds:
                # log fragmentation score and perform OSD compaction
                rados_obj.check_fragmentation_score(osd_id=osd)
                log.info("Starting OSD compaction for OSD " + osd)
                out, _ = cephadm.shell(
                    args=[f"ceph tell osd.{osd} compact"], timeout=600
                )
                time.sleep(5)
                # log fragmentation score post OSD compaction
                rados_obj.check_fragmentation_score(osd_id=osd)

            # choose 25% of OMAP pools for deletion at random
            delete_pools = random.choices(omap_pools, k=int(omap_pool_count * 0.25))
            log.info("Removing pools: %s", delete_pools)
            for pool in delete_pools:
                rados_obj.delete_pool(pool)
                log.info("Deleted pool %s successfully, now recreating..." % pool)
                rados_obj.create_pool(pool_name=pool)
                log.info("Created replicated pool %s for omap successfully" % pool)

        log.info(
            "Stress test around omaps completed successfully, proceeding to check for crashes"
        )
    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        # delete rados pool
        rados_obj.rados_pool_cleanup()
        # log cluster health
        rados_obj.log_cluster_health()
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
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

    log.info(
        "Stress test around OMAPS for a duration of %s completed without any crashes",
        stress_test_duration,
    )
    return 0
