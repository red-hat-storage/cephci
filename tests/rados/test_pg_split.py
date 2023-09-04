import datetime
import time
import traceback

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from tests.rados.rados_test_util import create_pools, write_to_pools
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log
from utility.utils import method_should_succeed, should_not_be_empty

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Automates PG split and merge test scenarios.
    1. Create replicated pool
    2. Verify the pg count and bulk flag
    3. Set bulk flag to true
    4. Wait for pg to be active+clean
    5. Verify new pg_num has increased and greater that initial pg_num
    6. Set bulk flag to false
    7. Wait for pg to be active+clean
    8. Verify latest pg_num has decreased.
    9. Verify restart osd when split is in progress.
    10. Verify delete object when split is in progress.
    """
    try:
        log.info(run.__doc__)
        config = kw["config"]
        cephadm = CephAdmin(cluster=ceph_cluster, **config)
        rados_obj = RadosOrchestrator(node=cephadm)
        pool_obj = PoolFunctions(node=cephadm)
        client_node = ceph_cluster.get_nodes(role="client")[0]
        timeout = config.get("timeout", 10800)

        log.info("Running PG split merge scenarios")
        pool = create_pools(config, rados_obj, client_node)
        should_not_be_empty(pool, "Failed to retrieve pool details")
        write_to_pools(config, rados_obj, client_node)
        method_should_succeed(wait_for_clean_pg_sets, rados_obj, timeout)
        prop = rados_obj.get_pool_property(pool=pool["pool_name"], props="pg_num")
        if not pool["pg_num"] == prop["pg_num"]:
            log.error(
                f"Actual pg_num {prop['pg_num']} does not match with expected pg_num {pool['pg_num']}"
            )
            raise Exception(
                f"Actual pg_num {prop['pg_num']} does not match with expected pg_num {pool['pg_num']}"
            )
        bulk = pool_obj.get_bulk_details(pool["pool_name"])
        if bulk:
            log.error("Expected bulk flag should be False.")
            raise Exception("Expected bulk flag should be False.")
        new_bulk = pool_obj.set_bulk_flag(pool["pool_name"])
        if not new_bulk:
            log.error("Expected bulk flag should be True.")
            raise Exception("Expected bulk flag should be True.")
        if pool.get("restart_osd", False):
            acting_pg_set = rados_obj.get_pg_acting_set(pool_name=pool["pool_name"])
            log.info(f"Acting set {acting_pg_set}")
            if not acting_pg_set:
                log.error("Failed to retrieve acting pg set")
                raise Exception("Failed to retrieve acting pg set")
            osd_id = acting_pg_set[0]
            if not rados_obj.change_osd_state(action="stop", target=osd_id):
                log.error(f"Unable to stop the OSD : {osd_id}")
                raise Exception(f"Unable to stop the OSD : {osd_id}")
            if not rados_obj.change_osd_state(action="start", target=osd_id):
                log.error(f"Unable to start the OSD : {osd_id}")
                raise Exception(f"Unable to start the OSD : {osd_id}")
        if pool.get("del_obj", False):
            del_objects = [
                {"name": f"obj{i}"} for i in range(pool.get("objs_to_del", 5))
            ]
            if not pool_obj.do_rados_delete(
                pool_name=pool["pool_name"], objects=del_objects
            ):
                log.error("Failed to delete objects from pool.")
                raise Exception("Failed to delete objects from pool.")
        time.sleep(40)
        method_should_succeed(wait_for_clean_pg_sets, rados_obj, timeout)
        endtime = datetime.datetime.now() + datetime.timedelta(seconds=400)
        while datetime.datetime.now() < endtime:
            new_prop = rados_obj.get_pool_property(
                pool=pool["pool_name"], props="pg_num"
            )
            if new_prop["pg_num"] > prop["pg_num"]:
                break
            log.error(
                f"Actual pg_num {new_prop['pg_num']} is expected to be greater than {prop['pg_num']}"
            )
            log.info("Sleeping for 40 secs")
            time.sleep(40)
        else:
            raise Exception(
                f"Actual pg_num {new_prop['pg_num']} is expected to be greater than {prop['pg_num']}"
            )
        pool_obj.rm_bulk_flag(pool["pool_name"])
        time.sleep(10)
        rm_bulk = pool_obj.get_bulk_details(pool["pool_name"])
        if rm_bulk:
            log.error("Expected bulk flag should be False.")
            raise Exception("Expected bulk flag should be False.")
        time.sleep(20)
        method_should_succeed(wait_for_clean_pg_sets, rados_obj, timeout)
        endtime = datetime.datetime.now() + datetime.timedelta(seconds=400)
        while datetime.datetime.now() < endtime:
            new_prop1 = rados_obj.get_pool_property(
                pool=pool["pool_name"], props="pg_num"
            )
            if new_prop1["pg_num"] < new_prop["pg_num"]:
                break
            log.error(
                f"Actual pg_num {new_prop1['pg_num']} is expected to be smaller than {new_prop['pg_num']}"
            )
            log.info("Sleeping for 40 secs")
            time.sleep(40)
        else:
            raise Exception(
                f"Actual pg_num {new_prop1['pg_num']} is expected to be smaller than {new_prop['pg_num']}"
            )

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1

    finally:
        log.info("*********** Execution of finally block starts ***********")
        if config.get("delete_pools"):
            for name in config["delete_pools"]:
                method_should_succeed(rados_obj.detete_pool, name)
            log.info("deleted all the given pools successfully")
    return 0
