"""
Module to Create, test, re-balance objects and delete pool where each object has large number of KW pairs attached,
thereby increasing the OMAP entries generated on the pool.
"""
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test to create a large number of omap entries on the single PG pool and test osd resiliency
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    client_node = rados_obj.ceph_cluster.get_nodes(role="client")[0]

    pool_target_configs = config["verify_osd_omap_entries"]["pool_config"]
    omap_target_configs = config["verify_osd_omap_entries"]["omap_config"]

    # Creating pools and starting the test
    for entry in pool_target_configs.values():
        try:
            log.debug(
                f"Creating {entry['pool_type']} pool on the cluster with name {entry['pool_name']}"
            )
            # omap entries cannot exist on EC pools. Removing the code for EC pools,
            # And testing only on RE pools.
            method_should_succeed(rados_obj.create_pool, **entry)

            for omap_config in omap_target_configs.keys():
                normal_objs = omap_target_configs[omap_config]["normal_objs"]
                if normal_objs > 0:
                    # create n number of objects without any omap entry
                    pool_obj.do_rados_put(
                        client=client_node, pool=entry["pool_name"], nobj=normal_objs
                    )

                # calculate objects to be written with omaps and begin omap creation process
                omap_obj_num = (
                    omap_target_configs[omap_config]["obj_end"]
                    - omap_target_configs[omap_config]["obj_start"]
                )
                log.debug(
                    f"Created the pool. beginning to create omap entries on the pool. Count : {omap_obj_num}"
                )
                if not pool_obj.fill_omap_entries(
                    pool_name=entry["pool_name"], **omap_target_configs[omap_config]
                ):
                    log.error(
                        f"Omap entries not generated on pool {entry['pool_name']}"
                    )
                    raise Exception(
                        f"Omap entries not generated on pool {entry['pool_name']}"
                    )

                assert pool_obj.check_large_omap_warning(
                    pool=entry["pool_name"],
                    obj_num=omap_obj_num,
                    check=omap_target_configs[omap_config]["large_warn"],
                )

            # Fetching the current acting set for the pool
            acting_set = rados_obj.get_pg_acting_set(pool_name=entry["pool_name"])
            rados_obj.change_recover_threads(config={}, action="set")
            log.debug(f"Proceeding to restart OSDs from the acting set {acting_set}")
            for osd_id in acting_set:
                rados_obj.change_osd_state(action="stop", target=osd_id)
                # sleeping for 5 seconds for re-balancing to begin
                time.sleep(5)

                # Waiting for cluster to get clean state after OSD stopped
                if not wait_for_clean_pg_sets(rados_obj):
                    log.error("PG's in cluster are not active + Clean state.. ")
                    raise Exception("PG's in cluster are not active + Clean state.. ")
                rados_obj.change_osd_state(action="restart", target=osd_id)
                log.debug(
                    f"Cluster reached clean state after osd {osd_id} stop and restart"
                )
            log.info(
                f"All the OSD's from the acting set {acting_set} were restarted "
                f"and object movement completed for pool {entry['pool_name']}"
            )
        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            return 1
        finally:
            rados_obj.change_recover_threads(config={}, action="rm")
            # deleting the pool created after the test
            rados_obj.detete_pool(pool=entry["pool_name"])

    log.info("Completed testing effects of large number of omap entries on pools ")
    return 0
