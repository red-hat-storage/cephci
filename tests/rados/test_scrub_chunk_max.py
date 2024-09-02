"""
Module to verify functionality and effect of OSD scrub_chunk_max parameter.
BZ #1382226 - PG scrub bypasses 'osd_scrub_chunk_max' limit to find hash boundary
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
    # CEPH-10792
    Test to verify PG scrub is within the 'osd_scrub_chunk_max' limit
    1. Create a replicated pool with single pg
    2. Grab the default value of 'osd_scrub_chunk_max'
    3. Set 'osd_scrub_chunk_max' to a low value, preferably lower than
    the default. e.g. 5
    4. Write few objects(>osd_scrub_chunk_max) to the created pool
    5. Start deep-scrub of the pool with 1 pg having few objects
    6. Wait for deep-scrub to complete and verify primary OSD logs
        expected to find logs having entries like below:
            be_scan_list (0/5 5:18dee445:::obj-12:head deep)
            be_scan_list (0/5 5:18dee445:::obj-12:head deep)
    7. Delete the created pools
    """
    log.info(run.__doc__)
    config = kw["config"]
    rhbuild = config["rhbuild"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    pool_obj = PoolFunctions(node=cephadm)
    client_node = ceph_cluster.get_nodes(role="client")[0]
    pg_scrub_log_list = []
    pool_name = "test_scrub_max"
    log.info(
        "Running test case to verify PG scrub is within the 'osd_scrub_chunk_max' limit"
    )

    try:
        # fetch the custom scrub_chunk_max value from config
        custom_scrub_chunk_max = config.get("scrub_chunk_max", 5)

        # create a replicated pool with single pg
        rados_obj.create_pool(pool_name=pool_name, **{"pg_num": 1, "pg_num_max": 1})

        # retrieve default value of osd_scrub_chunk_max
        org_scrub_chunk_max = mon_obj.get_config(
            section="osd", param="osd_scrub_chunk_max"
        )
        log.info(f"Original value of osd_scrub_chunk_max is {org_scrub_chunk_max}")

        # create 23 objects in the pool, for better validation, total number of objects
        # in the pool should not be a multiple of the custom value set in 'osd_scrub_chunk_max' parameter
        # 23 is chosen for sake of convenience, 27, 33, 34, any number which is
        # not a multiple of 5 can be chosen
        pool_obj.do_rados_put(client=client_node, pool=pool_name, nobj=23)

        # set value of osd_scrub_chunk_max as 5 or whatever input provided
        mon_obj.set_config(
            **{
                "section": "osd",
                "name": "osd_scrub_chunk_max",
                "value": custom_scrub_chunk_max,
            }
        )

        time.sleep(10)  # blind wait to let ceph df stats account for all the objects
        objs = rados_obj.get_cephdf_stats(pool_name=pool_name)["stats"]["objects"]
        if objs < 23:
            time.sleep(5)  # additional blind sleep to let objs show up in ceph df
        assert objs == 23

        primary_osd = rados_obj.get_pg_acting_set(pool_name=pool_name)[0]
        pool_pgid = rados_obj.get_pgid(pool_name=pool_name)[0]
        pool_id = pool_obj.get_pool_id(pool_name=pool_name)
        pool_pg_dump = rados_obj.get_ceph_pg_dump(pg_id=pool_pgid)
        osd_host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=primary_osd)

        log.info(f"last_deep_scrub: {pool_pg_dump['last_deep_scrub']}")
        log.info(f"last_deep_scrub_stamp: {pool_pg_dump['last_deep_scrub_stamp']}")
        if rhbuild and rhbuild.split(".")[0] > "5":
            org_objects_scrubbed = int(pool_pg_dump["objects_scrubbed"])
            log.info(f"objects_scrubbed: {org_objects_scrubbed}")

        # log entries necessary for validation are present in log level 10
        # setting log level to 20 for more holistic coverage
        assert mon_obj.set_config(section="osd", name="debug_osd", value="20/20")

        init_time, _ = osd_host.exec_command(cmd="date '+%Y-%m-%d %H:%M:%S'", sudo=True)
        rados_obj.run_deep_scrub(pool=pool_name)

        start_time = datetime.datetime.now()
        while datetime.datetime.now() <= start_time + datetime.timedelta(seconds=600):
            pool_pg_dump = rados_obj.get_ceph_pg_dump(pg_id=pool_pgid)
            if rhbuild and rhbuild.split(".")[0] > "5":
                objects_scrubbed = int(pool_pg_dump["objects_scrubbed"])
                log.info(f"Objects scrubbed: {objects_scrubbed} | Target: {objs}")
                if (objects_scrubbed - org_objects_scrubbed) >= objs:
                    end_time, _ = osd_host.exec_command(
                        cmd="date '+%Y-%m-%d %H:%M:%S'", sudo=True
                    )
                    break
                else:
                    log.info(
                        f"Objects scrubbed are less than target ({objs}). "
                        f"Sleeping for 30 secs"
                    )
                    time.sleep(30)
            if (
                pool_pg_dump["last_deep_scrub"] != "0'0"
                and pool_pg_dump["state"] == "active+clean"
            ):
                log.info(f"Deep-scrub completed, pg state: {pool_pg_dump['state']}")
                end_time, _ = osd_host.exec_command(
                    cmd="date '+%Y-%m-%d %H:%M:%S'", sudo=True
                )
                break
            else:
                log.info(
                    f"deep-scrub is yet to complete, pg state: {pool_pg_dump['state']}. "
                    f"Sleeping for 30 secs"
                )
                time.sleep(30)
        else:
            log.error(
                f"All {objs} objects in pool {pool_name} couldn't get scrubbed within 10 mins"
            )
            return 1

        log.info(f"Final last_deep_scrub: {pool_pg_dump['last_deep_scrub']}")
        log.info(
            f"Final last_deep_scrub_stamp: {pool_pg_dump['last_deep_scrub_stamp']}"
        )
        if rhbuild and rhbuild.split(".")[0] > "5":
            log.info(f"Final objects_scrubbed: {pool_pg_dump['objects_scrubbed']}")

        scrub_logs = rados_obj.get_journalctl_log(
            start_time=init_time,
            end_time=end_time,
            daemon_type="osd",
            daemon_id=primary_osd,
        )

        for line in scrub_logs.splitlines():
            if f"pg[{pool_id}.0(" in line and "be_scan_list" in line:
                pg_scrub_log_list.append(line)

        pg_scrub_log = "\n".join(pg_scrub_log_list)
        log.debug(
            f"\n ==========================================================================="
            f"\n Scrub log entries: \n {pg_scrub_log}"
            f"\n ==========================================================================="
        )

        for i in range(objs % custom_scrub_chunk_max):
            log.info(
                f"Verifying existence of log entry 'be_scan_list ({i}/{objs % custom_scrub_chunk_max}'"
            )
            assert f"be_scan_list ({i}/{objs % custom_scrub_chunk_max}" in pg_scrub_log
            log.info("PASS")

        for i in range(custom_scrub_chunk_max):
            log.info(
                f"Verifying existence of log entry 'be_scan_list ({i}/{custom_scrub_chunk_max}'"
            )
            assert f"be_scan_list ({i}/{custom_scrub_chunk_max}" in pg_scrub_log
            log.info("PASS")

    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1
    finally:
        log.info("*********** Execution of finally block starts ***********")
        mon_obj.remove_config(section="osd", name="osd_scrub_chunk_max")
        mon_obj.remove_config(section="osd", name="debug_osd")
        if config.get("delete_pool"):
            rados_obj.delete_pool(pool=pool_name)
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info("Verification of scrub_chunk_max parameter completed")
    return 0
