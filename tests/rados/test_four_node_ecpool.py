"""
test Module to :
1. Create a 2+2 ec pool
2. fill the pool
3. Test the effects of bulk flag and no IO stoppage
4. rolling reboot of OSDs of a host
5. OSD start and stop for failure domain level
6. Serviceability tests
"""

import datetime
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test to Verify the ec 2+2 pool
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    pool_obj = PoolFunctions(node=cephadm)

    if not mon_obj.verify_set_config(
        section="mon", name="mon_osd_down_out_subtree_limit", value="host"
    ):
        log.error(
            "mon_osd_down_out_subtree_limit not set on the cluster. setting the same now"
        )
        mon_obj.set_config(
            section="mon", name="mon_osd_down_out_subtree_limit", value="host"
        )

    if not mon_obj.verify_set_config(
        section="osd", name="osd_async_recovery_min_cost", value="1099511627776"
    ):
        log.error(
            "osd_async_recovery_min_cost not set on the cluster. setting the same now"
        )
        mon_obj.set_config(
            section="osd", name="osd_async_recovery_min_cost", value="1099511627776"
        )

    # Creating the EC pool
    ec_config = config.get("ec_pool")
    pool_name = ec_config["pool_name"]
    if not rados_obj.create_erasure_pool(name=ec_config["profile_name"], **ec_config):
        log.error("Failed to create the EC Pool")
        return 1

    cmd = "ceph osd pool autoscale-status"
    pool_status = rados_obj.run_ceph_command(cmd=cmd)

    for entry in pool_status:
        if entry["pool_name"] == pool_name:
            if entry["pg_autoscale_mode"] == "off":
                log.error(
                    f"Pg autoscaler turned off for the new pool : {entry['pool_name']} "
                )
                return 1

    init_pg_count = rados_obj.get_pool_property(pool=pool_name, props="pg_num")[
        "pg_num"
    ]
    log.debug(f"init PG count on the pool : {init_pg_count}")

    bulk = pool_obj.get_bulk_details(pool_name=pool_name)
    if bulk:
        log.error("Expected bulk flag should be False.")
        raise Exception("Expected bulk flag should be False.")

    log.debug(f"Bulk flag not enabled on pool {pool_name}, Proceeding to enable bulk")

    new_bulk = pool_obj.set_bulk_flag(pool_name=pool_name)
    if not new_bulk:
        log.error("Expected bulk flag should be True.")
        raise Exception("Expected bulk flag should be True.")
    # Sleeping for 60 seconds for bulk flag application and PG count to be increased.
    time.sleep(60)

    pg_count_bulk_true = rados_obj.get_pool_details(pool=pool_name)["pg_num_target"]
    log.debug(
        f"PG count on pool {pool_name} post addition of bulk flag : {pg_count_bulk_true}"
        f"Starting to wait for PG count on the pool to go from {init_pg_count} to"
        f" {pg_count_bulk_true} while checking for PG inactivity"
    )

    endtime = datetime.datetime.now() + datetime.timedelta(seconds=10000)
    while datetime.datetime.now() < endtime:
        pool_pg_num = rados_obj.get_pool_property(pool=pool_name, props="pg_num")[
            "pg_num"
        ]
        if pool_pg_num == pg_count_bulk_true:
            log.info(f"PG count on pool {pool_name} is achieved")
            break
        log.info(
            f"PG count on pool {pool_name} has not reached desired levels."
            f"Expected : {pg_count_bulk_true}, Current : {pool_pg_num}"
        )
        if not rados_obj.check_inactive_pgs_on_pool(pool_name=pool_name):
            log.error(f"Inactive PGs found on pool : {pool_name}")
            raise Exception("Inactive PGs upon OSD stop error")

        log.info("Sleeping for 20 secs and checking the PG states and PG count")
        time.sleep(20)
    else:
        raise Exception(
            f"pg_num on pool {pool_name} did not reach the desired levels of PG count "
            f"with bulk flag enabled"
            f"Expected : {pg_count_bulk_true}"
        )
    log.info(
        "PGs increased to desired levels after application of bulk flag on the pool with no inactive PGs"
    )

    # Restarting OSDs belonging to a particular host
    osd_node = ceph_cluster.get_nodes(role="osd")[0]
    osd_list = rados_obj.collect_osd_daemon_ids(osd_node=osd_node)

    for osd_id in osd_list:
        log.debug(f"Rebooting OSD : {osd_id} and checking health status")
        if not rados_obj.change_osd_state(action="restart", target=osd_id):
            log.error(f"Unable to restart the OSD : {osd_id}")
            raise Exception("Execution error")

        time.sleep(5)
        # Waiting for recovery to post OSD reboot
        method_should_succeed(wait_for_clean_pg_sets, rados_obj, test_pool=pool_name)
        log.debug(
            "PG's are active + clean post OSD reboot, proceeding to restart next OSD"
        )

    log.info(
        f"All the planned  OSD reboots have completed for host {osd_node.hostname}"
    )

    # Beginning with OSD stop operations
    log.debug("Stopping 1 OSD from each host. No inactive PGs")
    osd_nodes = ceph_cluster.get_nodes(role="osd")
    for node in osd_nodes:
        osd_list = rados_obj.collect_osd_daemon_ids(osd_node=node)
        if not rados_obj.change_osd_state(action="stop", target=osd_list[0]):
            log.error(f"Unable to stop the OSD : {osd_list[0]}")
            raise Exception("Execution error")
        time.sleep(5)

        if not rados_obj.check_inactive_pgs_on_pool(pool_name=pool_name):
            log.error(f"Inactive PGs found on pool : {pool_name}")
            raise Exception("Inactive PGs upon OSD stop error")

        time.sleep(5)
        # Waiting for recovery to post OSD reboot
        method_should_succeed(wait_for_clean_pg_sets, rados_obj, test_pool=pool_name)
        log.debug(
            "PG's are active + clean post OSD reboot, proceeding to restart next OSD"
        )
        if not rados_obj.change_osd_state(action="start", target=osd_list[0]):
            log.error(f"Unable to stop the OSD : {osd_list[0]}")
            raise Exception("Execution error")
        time.sleep(5)

    # Stopping all OSDs of 1 host and check for inactive PGs
    stop_host = osd_nodes[0]
    osd_list = rados_obj.collect_osd_daemon_ids(osd_node=stop_host)
    for osd in osd_list:
        if not rados_obj.change_osd_state(action="stop", target=osd):
            log.error(f"Unable to stop the OSD : {osd_list[0]}")
            raise Exception("Unable to stop OSDs error")
        time.sleep(5)

    if not rados_obj.check_inactive_pgs_on_pool(pool_name=pool_name):
        log.error(f"Inactive PGs found on pool : {pool_name}")
        raise Exception("Inactive PGs upon OSD stop error")

    for osd in osd_list:
        if not rados_obj.change_osd_state(action="start", target=osd):
            log.error(f"Unable to start the OSD : {osd_list[0]}")
            raise Exception("Unable to start OSDs error")
        time.sleep(5)

    # tbd: Serviceability scenarios remaining.

    if ec_config.get("delete_pool"):
        # Deleting the pool created earlier
        if not rados_obj.detete_pool(pool=pool_name):
            log.error(f"the pool {pool_name} could not be deleted")

    log.info("EC 2+2 pool is working as expected.")
    return 0
