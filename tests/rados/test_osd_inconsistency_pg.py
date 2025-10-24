"""
This file contains the  methods to verify the  inconsistent objects.
AS part of verification the script  perform the following tasks-
   1. Creating omaps
   2. Convert the object in to inconsistent object
"""

import time
import traceback

from ceph.ceph_admin import CephAdmin
from ceph.rados.bluestoretool_workflows import BluestoreToolWorkflows
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.objectstoretool_workflows import objectstoreToolWorkflows
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
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
    client_node = ceph_cluster.get_nodes(role="client")[0]
    bluestore_obj = BluestoreToolWorkflows(node=cephadm)
    objectstore_obj = objectstoreToolWorkflows(node=cephadm, nostart=True)
    pool_name = config["pool_name"]
    try:
        if not rados_obj.create_pool(**config):
            log.error("Failed to create the replicated Pool")
            return 1
        method_should_succeed(wait_for_clean_pg_sets, rados_obj, timeout=300)
        obj_pg_map = rados_obj.create_inconsistent_object(
            objectstore_obj, pool_name, no_of_objects=1
        )
        if obj_pg_map is None:
            log.error(
                "Inconsistent objects are not created.Not executing the further test"
            )
            return 1

        pg_id = list(set(obj_pg_map.values()))[0]
        oname = list(obj_pg_map.keys())[0]
        msg_pgid = (
            f"The inconsistent object created in{pg_id} pg and the pool is {pool_name}"
        )
        log.info(msg_pgid)
        msg_obj = f"The objects in the {pool_name} pool is - {list(obj_pg_map.keys())}"
        log.info(msg_obj)
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
        # Wait time
        timeout = 200
        notFound = 0
        while timeout:
            # Checking for the inconsistent pg's
            inconsistent_pg_list = rados_obj.get_inconsistent_pg_list(pool_name)
            if any(pg_id in search for search in inconsistent_pg_list):
                log.info("Checking for the inconsistent list in the PG...")
                time.sleep(2)
                timeout = timeout - 1
            else:
                log.info("After repair the inconsistent pg not exists in the list")
                notFound = 1
                break

        if timeout == 0 and notFound == 0:
            log.error("After repair the inconsistent pg exists in the list ")
            return 1
        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info(
            "==============================Execution of finally block============================="
        )
        method_should_succeed(rados_obj.delete_pool, pool_name)
        log.info("deleted the pool successfully")
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
