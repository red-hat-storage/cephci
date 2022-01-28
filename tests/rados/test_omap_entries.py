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

    pool_target_configs = config["verify_osd_omap_entries"]["configurations"]
    omap_target_configs = config["verify_osd_omap_entries"]["omap_config"]

    # Creating pools and starting the test
    for entry in pool_target_configs.values():
        log.debug(
            f"Creating {entry['pool_type']} pool on the cluster with name {entry['pool_name']}"
        )
        if entry.get("pool_type", "replicated") == "erasure":
            method_should_succeed(
                rados_obj.create_erasure_pool, name=entry["pool_name"], **entry
            )
        else:
            method_should_succeed(
                rados_obj.create_pool,
                **entry,
            )

        log.debug(
            "Created the pool. beginning to create large number of omap entries on the pool"
        )
        if not pool_obj.fill_omap_entries(
            pool_name=entry["pool_name"], **omap_target_configs
        ):
            log.error(f"Omap entries not generated on pool {entry['pool_name']}")
            return 1

        # Fetching the current acting set for the pool
        acting_set = rados_obj.get_pg_acting_set(pool_name=entry["pool_name"])
        rados_obj.change_recover_threads(config={}, action="set")
        log.debug(f"Proceeding to restart OSd's from the acting set {acting_set}")
        for osd_id in acting_set:
            rados_obj.change_osd_state(action="stop", target=osd_id)
            # sleeping for 5 seconds for re-balancing to begin
            time.sleep(5)

            # Waiting for cluster to get clean state after OSD stopped
            if not wait_for_clean_pg_sets(rados_obj):
                log.error("PG's in cluster are not active + Clean state.. ")
                return 1
            rados_obj.change_osd_state(action="restart", target=osd_id)
            log.debug(
                f"Cluster reached clean state after osd {osd_id} stop and restart"
            )

        rados_obj.change_recover_threads(config={}, action="rm")
        # deleting the pool created after the test
        rados_obj.detete_pool(pool=entry["pool_name"])

        log.info(
            f"All the OSD's from the acting set {acting_set} were restarted "
            f"and object movement completed for pool {entry['pool_name']}"
        )

    log.info("Completed testing effects of large number of omap entries on pools ")
    return 0
