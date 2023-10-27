"""
This file contains the  methods to verify the  inconsistent objects.
AS part of verification the script  perform the following tasks-
   1. Creating omaps
   2. Convert the object in to inconsistent object
"""


import random
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
    try:
        # Creating pools and starting the test
        for entry in pool_target_configs.values():
            log.debug(
                f"Creating {entry['pool_type']} pool on the cluster with name {entry['pool_name']}"
            )
            method_should_succeed(
                rados_obj.create_pool,
                **entry,
            )
            pool_name = entry["pool_name"]
            log.info(
                f"Created the pool {entry['pool_name']}. beginning to create large number of omap entries on the pool"
            )
        # Creating omaps
        if not pool_obj.fill_omap_entries(pool_name=pool_name, **omap_target_configs):
            log.error(f"Omap entries not generated on pool {pool_name}")
            return 1
        obj_list = rados_obj.get_object_list(pool_name)
        oname = random.choice(obj_list)
        # Create inconsistency objects
        pg_id = rados_obj.create_inconsistent_object(pool_name, oname)
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
            else:
                log.error("Inconsistent object is not exists in the objects list")
                return 1
        osd_map_output = rados_obj.get_osd_map(pool=pool_name, obj=oname)
        primary_osd = osd_map_output["acting_primary"]
        # Executing the fsck on the OSD
        log.info(f" Performing the ceph-bluestore-ttol fsck on the OSD-{primary_osd}")
        fsck_output = bluestore_obj.run_consistency_check(primary_osd)
        log.info(f"The fsck output is ::{fsck_output}")
        # Reparing the OSD using the bluestore tool
        log.info(f"Performing the repair on the osd-{primary_osd}")
        repair_output = bluestore_obj.repair(primary_osd)
        log.info(f"The repair status is:{repair_output}")
        health_check = rados_obj.check_inconsistent_health()
        log.info(f"The health status after the repair is ::{health_check}")
        assert not health_check
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
            method_should_succeed(rados_obj.detete_pool, pool_name)
            log.info("deleted the pool successfully")
    return 0
