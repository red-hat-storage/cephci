"""
Module to deploy 8+6 EC pool with custom CRUSH rules for testing

"""

import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.crushtool_workflows import CrushToolWorkflows
from ceph.rados.pool_workflows import PoolFunctions
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Module to deploy 8+6 EC pool with custom CRUSH rules for testing
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    crush_obj = CrushToolWorkflows(node=cephadm)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    pool_obj = PoolFunctions(node=cephadm)

    crush_rule = config.get("crush_rule", "rule-86-msr")
    modify_threshold = config.get("modify_threshold", False)
    crush_rules = {
        "rule-86-msr": """id 86
type msr_indep
step take default
step choosemsr 4 type host
step choosemsr 4 type osd
step emit
""",
    }
    try:
        # setting the crush rule on the cluster
        if config.get("pre_create_rule"):
            log.debug("Creating new crush rules on the cluster")
            if not crush_obj.add_crush_rule(
                rule_name=crush_rule, rules=crush_rules[crush_rule]
            ):
                log.error(
                    f"Failed to add the crush rule : {crush_rule} into the cluster"
                )
                raise Exception(
                    f"Failed to add the crush rule : {crush_rule} into the cluster"
                )

        pool_name = config.get("pool_name")
        log.debug("Creating new EC pool on the cluster")
        if not rados_obj.create_erasure_pool(name=crush_rule, **config):
            log.error(f"Failed to create the EC Pool : {pool_name}")
            raise Exception("Failed to create the EC Pool")

        if config.get("change_subtree_limit"):
            bucket = config["change_subtree_limit"]
            log.info(f"Changing subtree limit to {bucket}")
            if not mon_obj.set_config(
                section="mon", name="mon_osd_down_out_subtree_limit", value=bucket
            ):
                log.error(f"Failed to set mon_osd_down_out_subtree_limit to {bucket} ")
                return 1

        time.sleep(5)
        log.info(
            "Completed setting the subtree limit and creating the pool. Writing objects "
        )

        rados_obj.bench_write(pool_name=pool_name, verify_stats=False)

        log.info("Scenario 1: PG split and merge tests with inactive PG check")
        res, inactive_count = pool_obj.run_autoscaler_bulk_test(
            pool=pool_name,
            overwrite_recovery_threads=True,
            test_pg_split=True,
            modify_threshold=modify_threshold,
        )
        if not res:
            log.error("Failed to scale up the pool with bulk flag. Fail")
            return 1
        if inactive_count > 5:
            log.error(
                "Observed multiple PGs in inactive state during PG scale up. Fail"
            )
            return 1

        res, inactive_count = pool_obj.run_autoscaler_bulk_test(
            pool=pool_name, overwrite_recovery_threads=True, test_pg_merge=True
        )
        if not res:
            log.error("Failed to scale up the pool with bulk flag. Fail")
            return 1
        if inactive_count > 5:
            log.error(
                "Observed multiple PGs in inactive state during PG scale down. Fail"
            )
            return 1

        log.info("Scenario 2. OSD failure tests")
        # Beginning with OSD stop operations
        log.info(
            "scenario 2.1 - Stopping 1 OSD from each host. No inactive PGs -> Recovery"
        )
        osd_nodes = ceph_cluster.get_nodes(role="osd")
        stopped_osds = []

        for node in osd_nodes:
            osd_list = rados_obj.collect_osd_daemon_ids(osd_node=node)
            log.debug(
                f"Stop OSDs: Chosen host {node.hostname} , Chosen OSD : {osd_list[0]}"
            )
            if not rados_obj.change_osd_state(action="stop", target=osd_list[0]):
                log.error(f"Unable to stop the OSD : {osd_list[0]}. scenario 2.1")
                raise Exception("Execution error. scenario 2.1")
            time.sleep(5)

            log.debug(f"Stopped OSD {osd_list[0]} from host {node.hostname}")
            stopped_osds.append(osd_list[0])
        log.debug(
            "Completed stopping 1 OSD from all the hosts. Checking for inactive PGs in 30sec"
        )
        time.sleep(30)

        inactive_pgs = 0
        if not rados_obj.check_inactive_pgs_on_pool(pool_name=pool_name):
            log.error(f"Inactive PGs found on pool : {pool_name}. scenario 2.1")
            inactive_pgs += 1

        # Waiting for recovery to post OSD stop
        method_should_succeed(
            wait_for_clean_pg_sets, rados_obj, timeout=12000, test_pool=pool_name
        )
        if inactive_pgs > 5:
            log.error("Found inactive PGs on the cluster post OSd shutdown")
            raise Exception("Inactive PGs post OSD shutdown error. scenario 2.1")

        log.info(
            f"PG's are active + clean post OSD shutdown of these OSDs: {stopped_osds}"
            f"Starting the OSDs back"
        )
        for osd in stopped_osds:
            if not rados_obj.change_osd_state(action="start", target=osd):
                log.error(f"Unable to stop the OSD : {osd}. scenario 2.1")
                raise Exception("Execution error. scenario 2.1")
            time.sleep(5)
            log.debug(f"OSD : {osd} Started.  proceeding to start next OSD")

        log.debug(
            "Completed starting OSDs from all the hosts. Checking for inactive PGs in 30sec"
        )
        time.sleep(30)

        inactive_pgs = 0
        if not rados_obj.check_inactive_pgs_on_pool(pool_name=pool_name):
            log.error(f"Inactive PGs found on pool : {pool_name}. scenario 2.1")
            inactive_pgs += 1

        # Waiting for recovery to post OSD stop
        method_should_succeed(
            wait_for_clean_pg_sets, rados_obj, timeout=12000, test_pool=pool_name
        )

        if inactive_pgs > 5:
            log.error("Found inactive PGs on the cluster post OSD start. scenario 2.1")
            raise Exception("Inactive PGs post OSD start error. scenario 2.1")

        log.debug(
            "Completed scenario 2.1 of stopping 1 OSD from each host + recovery + start all OSDs + recovery"
        )

        log.info("scenario 2.2 - Stopping all OSDs from 1 host. No inactive PGs")
        osd_nodes = ceph_cluster.get_nodes(role="osd")
        test_host = osd_nodes[0]
        osd_list = rados_obj.collect_osd_daemon_ids(osd_node=test_host)
        log.debug(
            f"Stop OSDs: Chosen host {test_host.hostname} , OSD on host: {osd_list}"
        )
        for osd_id in osd_list:
            if not rados_obj.change_osd_state(action="stop", target=osd_id):
                log.error(f"Unable to stop the OSD : {osd_id}. scenario 2.2")
                raise Exception("Execution error")
            time.sleep(5)
        log.debug(f"Stopped all OSDs on host : {test_host.hostname}")
        time.sleep(30)

        inactive_pgs = 0
        if not rados_obj.check_inactive_pgs_on_pool(pool_name=pool_name):
            log.error(f"Inactive PGs found on pool : {pool_name} post scenario 2.2")
            inactive_pgs += 1

        # Waiting for recovery to post OSD stop
        method_should_succeed(
            wait_for_clean_pg_sets, rados_obj, timeout=12000, test_pool=pool_name
        )
        if inactive_pgs > 5:
            log.error(
                "Found inactive PGs on the cluster post OSd shutdown. scenario 2.2"
            )
            raise Exception("Inactive PGs post OSD shutdown error. scenario 2.2")

        log.info(
            f"PG's are active + clean post OSD shutdown of all OSDs on host: {test_host.hostname}"
            f"Starting the OSDs back"
        )

        for osd_id in osd_list:
            if not rados_obj.change_osd_state(action="start", target=osd_id):
                log.error(f"Unable to start the OSD : {osd_id}. scenario 2.2")
                raise Exception("Execution error")
            time.sleep(5)
        log.debug(f"started all OSDs on host : {test_host.hostname}")
        time.sleep(30)

        inactive_pgs = 0
        if not rados_obj.check_inactive_pgs_on_pool(pool_name=pool_name):
            log.error(f"Inactive PGs found on pool : {pool_name} post scenario 2.2")
            inactive_pgs += 1

        # Waiting for recovery to post OSD stop
        method_should_succeed(
            wait_for_clean_pg_sets, rados_obj, timeout=12000, test_pool=pool_name
        )
        if inactive_pgs > 5:
            log.error(
                "Found inactive PGs on the cluster post Host OSD shutdown + Up. scenario 2.2"
            )
            raise Exception("Inactive PGs post OSD shutdown error. scenario 2.2")

        log.debug(
            "Completed scenario 2.2 of stopping All OSD from 1 host + recovery + start all OSDs + recovery"
        )

        # Below OSD scenarios to be added in next PR
        log.debug(
            "Starting scenario 2.3 of stopping 6 OSDs from 4 hosts. IOs paused temporarily"
            "Recovery to occur with K shards remaining, then with K+1 shards in acting set, the IOs to resume again"
        )
        log.debug(
            "Starting scenario 2.4 of stopping 6 OSDs. All OSDs from 1 host + 2 OSDs from 2nd host."
            " IOs paused temporarily"
            "Recovery to occur with K shards remaking, then with K+1 shards in acting set, the IOs to resume again"
        )

        # Below Host scenarios to be added in next PR
        log.debug(
            "Starting scenario 3.1 . Remove 1 Host. IOs to work without interruption"
            "Add the host back. Deploy OSDs on the host. Data moved on to new OSDs"
        )
        log.debug(
            "Starting scenario 3.2. Remove 1 Host and stop 2 OSDs. IO stops temporarily. "
            "Recovery to occur with K shards remaking, then with K+1 shards in acting set, the IOs to resume again"
            "Add the Host back and Start the down OSDs. Recovery"
        )

    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        # removal of rados pool
        if config.get("delete_pool"):
            if not rados_obj.delete_pool(pool=config["pool_name"]):
                log.error("Failed to delete EC Pool")
                return 1

        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1

        if modify_threshold:
            pool_obj.modify_autoscale_threshold(threshold=3.0)

        # reverting the recovery threads on the cluster
        rados_obj.change_recovery_threads(config={}, action="rm")

    log.info("Completed all scenarios for EC 8+6 With MSR crush rules")
    return 0
