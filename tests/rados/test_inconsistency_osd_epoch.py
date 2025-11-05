"""
This file contains the  methods to generate inconsistent objects, collect the OSD epoch,
 and then change the epoch to check if the inconsistency persists.
"""

import traceback

from ceph.ceph_admin import CephAdmin
from ceph.rados.bluestoretool_workflows import BluestoreToolWorkflows
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.objectstoretool_workflows import objectstoreToolWorkflows
from ceph.rados.pool_workflows import PoolFunctions
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
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
    objectstore_obj = objectstoreToolWorkflows(node=cephadm, nostart=True)
    bluestore_obj = BluestoreToolWorkflows(node=cephadm)
    pool_name = config["pool_name"]
    primary_osd = ""
    try:
        method_should_succeed(
            rados_obj.create_pool,
            **config,
        )
        msg_pg_autoscale_mode = (
            f"Setting the pg autoscale mode to off on {pool_name} pool"
        )
        log.info(msg_pg_autoscale_mode)
        rados_obj.set_pool_property(
            pool=pool_name, props="pg_autoscale_mode", value="off"
        )

        epoch_cmd = "ceph osd dump"
        epoch_incon = rados_obj.run_ceph_command(cmd=epoch_cmd)["epoch"]
        log.info(f"OSD epoch when Inconsistent object is generated : {epoch_incon}")
        method_should_succeed(wait_for_clean_pg_sets, rados_obj, timeout=300)
        log.info(f"Created the pool {pool_name} to generate inconsistent objects")

        obj_pg_map = rados_obj.create_inconsistent_object(
            objectstore_obj, pool_name, no_of_objects=1
        )
        if obj_pg_map is None:
            log.error(
                "Inconsistent objects are not created.Not executing the further test"
            )
            return 1

        for oname, pg_id in obj_pg_map.items():
            msg_pgid = f"The inconsistent object created in{pg_id} pg and the pool is {pool_name}"
            log.info(msg_pgid)
            msg_obj = (
                f"The objects in the {pool_name} pool is - {list(obj_pg_map.keys())}"
            )
            log.info(msg_obj)

            # Checking for inconsistency in the PG list
            inconsistent_pg_list = rados_obj.get_inconsistent_pg_list(pool_name)

            if any(pg_id in search for search in inconsistent_pg_list):
                log.info(f"Inconsistent PG is{pg_id}  ")
            else:
                log.error("Inconsistent PG is not generated")
                raise Exception("Inconsistent object not generated error")

            primary_osd = rados_obj.get_osd_map(pool=pool_name, obj=oname)[
                "acting_primary"
            ]

            log.debug(
                f"Marking the primary OSD with inconsistency, OSD.{primary_osd} down and out"
            )
            rados_obj.change_osd_state(action="stop", target=primary_osd)
            rados_obj.run_ceph_command(cmd=f"ceph osd out {primary_osd}")
            if not wait_for_clean_pg_sets(rados_obj, timeout=600):
                log.error("Cluster cloud not reach active+clean state within 600 secs")
                raise Exception("Cluster cloud not reach active+clean state")

            # Checking for inconsistency in the PG list
            inconsistent_pg_list = rados_obj.get_inconsistent_pg_list(pool_name)
            # Checking for the inconsistent pg's
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
