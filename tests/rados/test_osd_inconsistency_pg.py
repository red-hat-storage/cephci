"""
This file contains the  methods to verify the  inconsistent objects.
AS part of verification the script  perform the following tasks-
   1. Creating omaps
   2. Convert the object in to inconsistent object
"""

import random
import time
import traceback

from ceph.ceph_admin import CephAdmin
from ceph.rados.bluestoretool_workflows import BluestoreToolWorkflows
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test to create an inconsistent object and verify that objects details can  retrieve with various command.
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]

    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    client_node = ceph_cluster.get_nodes(role="client")[0]
    bluestore_obj = BluestoreToolWorkflows(node=cephadm)
    pool_target_configs = config["verify_osd_omap_entries"]["configurations"]
    omap_target_configs = config["verify_osd_omap_entries"]["omap_config"]
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

        oname_list = random.choices(obj_list, k=10)
        log.debug(
            f"Selected objects {oname_list} at random to generate inconsistent objects."
        )
        inconsistent_obj_count = 0
        for oname in oname_list:
            try:
                # Create inconsistency objects
                pg_id = rados_obj.create_inconsistent_object(pool_name, oname)
                inconsistent_obj_count = inconsistent_obj_count + 1
            except Exception as err:
                log.error(
                    f"Failed to generate inconsistent object.Trying to convert another object. Error : {err}"
                )
                continue
            if inconsistent_obj_count == 1:
                log.info(f"The inconsistent object is created in the PG - {pg_id}")
                break
        if inconsistent_obj_count == 0:
            log.error(
                "The inconsistent object is  not created.Not performing the further tests"
            )
            return 1
        # Restarting the mon
        log.info("Restarting the mon services")
        mon_service = client_node.exec_command(cmd="ceph orch ls | grep mon")
        mon_service_name = mon_service[0].split()[0]
        client_node.exec_command(cmd=f"ceph orch restart {mon_service_name}")
        log.info("Restarted the mon services")

        inconsistent_pg_list = rados_obj.get_inconsistent_pg_list(pool_name)
        if any(pg_id in search for search in inconsistent_pg_list):
            log.info(f"Inconsistent PG is{pg_id}  ")
        else:
            log.error("Inconsistent PG is not generated")
            return 1
        # checking the  inconsistent objects
        object_list = rados_obj.get_inconsistent_object_details(pg_id)
        no_objects = len(object_list["inconsistents"])
        for obj_count in range(no_objects):
            obj_id = object_list["inconsistents"][obj_count]["object"]["name"]
            if obj_id == oname:
                log.info(f"Inconsistent object {obj_id} is exists in the objects list.")
                errors_list = object_list["inconsistents"][obj_count]["errors"]
                log.info(
                    f"Checking the error messages of the inconsistent object {obj_id} "
                )
                log.debug(f"The error list obtained : {errors_list}")
                if "omap_digest_mismatch" not in errors_list:
                    log.error(
                        f"The inconsistent object {obj_id} is not reported with the omap_digest_mismatch error"
                    )
                    return 1
                log.info(
                    f"The inconsistent object {obj_id} is reported with the omap_digest_mismatch error"
                )
            else:
                log.error("Inconsistent object is not exists in the objects list")
                return 1

        osd_map_output = rados_obj.get_osd_map(pool=pool_name, obj=oname)
        primary_osd = osd_map_output["acting_primary"]
        # Executing the fsck on the OSD
        log.info(f" Performing the ceph-bluestore-tool fsck on the OSD-{primary_osd}")
        fsck_output = bluestore_obj.run_consistency_check(primary_osd)
        log.info(f"The fsck output is ::{fsck_output}")
        # Reparing the OSD using the bluestore tool
        log.info(f"Performing the repair on the osd-{primary_osd}")
        repair_output = bluestore_obj.repair(primary_osd)
        log.info(f"The repair status is:{repair_output}")
        health_check = rados_obj.check_inconsistent_health(inconsistent_present=False)
        log.info(f"The health status after the repair is ::{health_check}")
        assert health_check
        log.info("The inconsistent objects are repaired")
        # Checking for the inconsistent pg's
        inconsistent_pg_list = rados_obj.get_inconsistent_pg_list(pool_name)
        if any(pg_id in search for search in inconsistent_pg_list):
            log.info("After repair the inconsistent pg exists in the list ")
            return 1
        else:
            log.error("After repair the inconsistent pg not exists in the list")
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info("Execution of finally block")
        if config.get("delete_pool"):
            for pool_name in pools:
                method_should_succeed(rados_obj.delete_pool, pool_name)
                log.info(f"Pool {pool_name} deleted the pool successfully")
                time.sleep(2)
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
    return 0
