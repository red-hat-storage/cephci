"""
This file contains the  methods to generate inconsistent objects, collect the OSD epoch,
 and then change the epoch to check if the inconsistency persists.
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
    This file contains the  methods to generate inconsistent objects, collect the OSD epoch,
    and then change the epoch to check if the inconsistency persists.
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]

    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    bluestore_obj = BluestoreToolWorkflows(node=cephadm)
    pool_target_configs = config["verify_osd_omap_entries"]["configurations"]
    omap_target_configs = config["verify_osd_omap_entries"]["omap_config"]
    try:
        method_should_succeed(
            rados_obj.create_pool,
            **pool_target_configs,
        )
        pool_name = pool_target_configs["pool_name"]
        log.info(f"Created the pool {pool_name} to generate inconsistent objects")

        # Creating omaps
        if not pool_obj.fill_omap_entries(pool_name=pool_name, **omap_target_configs):
            log.error(f"Omap entries not generated on pool {pool_name}")
            return 1
        log.debug("Completed writing omap entries onto the pool")

        # Fetching all objects and selecting one object to generate inconsistency
        obj_list = rados_obj.get_object_list(pool_name)
        log.debug(f"All the objects added onto the pool are : {obj_list}")

        oname_list = random.choices(obj_list, k=10)
        log.debug(
            f"Selected objects {oname_list} at random to generate inconsistent objects."
        )
        inconsistent_obj_count = 0
        for oname in oname_list:
            primary_osd = rados_obj.get_osd_map(pool=pool_name, obj=oname)[
                "acting_primary"
            ]
            log.debug(f"The object stored in the primary osd number-{primary_osd}")

            # getting the OSD epoch before generating inconsistent objects
            epoch_cmd = "ceph osd dump"
            init_epoch = rados_obj.run_ceph_command(cmd=epoch_cmd)["epoch"]
            log.info(f"Initial epoch before generating inconsistency is {init_epoch}")

            # Create inconsistency objects
            try:
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

        # Checking for inconsistency in the PG list
        inconsistent_pg_list = rados_obj.get_inconsistent_pg_list(pool_name)
        if any(pg_id in search for search in inconsistent_pg_list):
            log.info(f"Inconsistent PG is{pg_id}  ")
        else:
            log.error("Inconsistent PG is not generated")
            raise Exception("Inconsistent object not generated error")

        epoch_incon = rados_obj.run_ceph_command(cmd=epoch_cmd)["epoch"]
        log.info(f"OSD epoch when Inconsistent object is generated : {epoch_incon}")

        log.debug(
            f"Marking the primary OSD with inconsistency, OSD.{primary_osd} down and out"
        )
        rados_obj.change_osd_state(action="stop", target=primary_osd)
        rados_obj.run_ceph_command(cmd=f"ceph osd out {primary_osd}")

        # Checking for the inconsistent pg's
        inconsistent_pg_list = rados_obj.get_inconsistent_pg_list(pool_name)
        if any(pg_id in search for search in inconsistent_pg_list):
            log.error(
                "Inconsistent pg exists in the cluster even after the OSD epoch was changed with OSD down"
            )
            raise Exception("Inconsistent PG on cluster Error")
        log.info("Inconsistent pg does not exist on the cluster post epoch change.")

        epoch_osd = rados_obj.run_ceph_command(cmd=epoch_cmd)["epoch"]
        log.info(f"OSD epoch when affected OSD is marked out & down: {epoch_osd}")

        if epoch_incon == epoch_osd:
            log.error(
                "The osd epoch has not changed post bringing down the affected OSD"
            )
            raise Exception("OSD epoch not changed error")

        # Marking the OSD in the cluster and bringing it up.
        log.debug(
            f"Marking the primary OSD with inconsistency, OSD.{primary_osd} Up and In"
        )
        rados_obj.change_osd_state(action="start", target=primary_osd)
        rados_obj.run_ceph_command(cmd=f"ceph osd in {primary_osd}")

        # Repairing the OSD using CBT
        log.info(f"Performing the repair on the osd.{primary_osd}")
        op = bluestore_obj.repair(primary_osd)
        log.info(f"CBT repair status :{op}")

        epoch_final = rados_obj.run_ceph_command(cmd=epoch_cmd)["epoch"]
        log.info(
            f"OSD epoch when Inconsistent object is repaiered + affected OSD is brought up : {epoch_final}"
        )

        if epoch_final == epoch_osd:
            log.error(
                "The osd epoch has not changed post PG repair of the affected OSD"
            )
            raise Exception("OSD epoch not changed error")
        log.info("Completed the test. Pass")
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info("------------Execution of finally block-----------------")
        if config.get("delete_pool"):
            method_should_succeed(rados_obj.delete_pool, pool_name)
            log.info("deleted the pool successfully")
        if "primary_osd" in locals() or "primary_osd" in globals():
            rados_obj.change_osd_state(action="start", target=primary_osd)
        rados_obj.run_ceph_command(cmd=f"ceph osd in {primary_osd}")
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
    return 0
