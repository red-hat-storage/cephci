"""
This test module is used to test revert stretch mode
includes:
1. revert from health stretch mode to default/non-default crush rule
2. revert from health stretch mode to custom crush rule
3. revert from site down stretch mode to default crush rule
4. revert from site down stretch mode to custome crush rule
5. revert from netsplit scenario to default/non-default crush rule
6. revert from netsplit scenario to custom crush rule
"""

from collections import namedtuple

from ceph.ceph import CephNode
from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from ceph.rados.serviceability_workflows import ServiceabilityMethods
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from tests.rados.test_stretch_revert_class import RevertStretchModeFunctionalities
from tests.rados.test_stretch_site_down import stretch_enabled_checks
from utility.log import Log

log = Log(__name__)

Hosts = namedtuple("Hosts", ["dc_1_hosts", "dc_2_hosts", "tiebreaker_hosts"])


def run(ceph_cluster, **kw):
    """
    performs replacement scenarios in stretch mode
    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster
    Scenarios:
        1. Scenario 1:- revert from health stretch mode to default/non-default crush rule
        1. Scenario 2:- revert from health stretch mode to custom crush rule
        2. Scenario 3:- revert from site down stretch mode to default crush rule
        2. Scenario 4:- revert from site down stretch mode to custome crush rule
        3. Scenario 5:- revert from netsplit scenario to default/non-default crush rule
        3. Scenario 6:- revert from netsplit scenario to custom crush rule
    """

    log.info(run.__doc__)
    config = kw.get("config")
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    pool_obj = PoolFunctions(node=cephadm)
    service_obj = ServiceabilityMethods(cluster=ceph_cluster, **config)
    rhbuild = config.get("rhbuild")
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_name = config.get("pool_name", "test_stretch_io")
    stretch_bucket = config.get("stretch_bucket", "datacenter")
    tiebreaker_mon_site_name = config.get("tiebreaker_mon_site_name", "tiebreaker")
    add_network_delay = config.get("add_network_delay", False)
    client_node = ceph_cluster.get_nodes(role="client")[0]
    scenarios_to_run = config.get(
        "scenarios_to_run",
        [
            "scenario1",
            "scenario2",
        ],
    )
    config = {
        "rados_obj": rados_obj,
        "pool_obj": pool_obj,
        "tiebreaker_mon_site_name": tiebreaker_mon_site_name,
        "stretch_bucket": stretch_bucket,
    }
    try:

        revert_stretch_mode_scenarios = RevertStretchModeScenarios(**config)

        if "scenario1" in scenarios_to_run:
            revert_stretch_mode_scenarios.scenario1(client_node=client_node)

        if "scenario2" in scenarios_to_run:
            revert_stretch_mode_scenarios.scenario2(client_node=client_node)

    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        # log cluster health
        rados_obj.log_cluster_health()
        return 1

    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )

        if (
            "revert_stretch_mode_scenarios" in locals()
            and "revert_stretch_mode_scenarios" in globals()
        ):
            revert_stretch_mode_scenarios.enable_stretch_mode(
                revert_stretch_mode_scenarios.tiebreaker_mon
            )
            wait_for_clean_pg_sets(rados_obj=rados_obj)

        if config.get("delete_pool"):
            rados_obj.delete_pool(pool=pool_name)

        # log cluster health
        rados_obj.log_cluster_health()

        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info("All the tests completed on the cluster, Pass!!!")
    return 0


class RevertStretchModeScenarios(RevertStretchModeFunctionalities):
    """
    Class includes test scenarios of RADOS feature exiting from 2 site + tiebreaker stretch mode.
    Usage:-
        config = {
            "rados_obj": rados_obj,
            "pool_obj": pool_obj,
            "tiebreaker_mon_site_name": tiebreaker_mon_site_name,
            "stretch_bucket": stretch_bucket,
        }
        revert_stretch_mode_scenarios = RevertStretchModeScenarios(**config)
        revert_stretch_mode_scenarios.scenario1(client_node=client_node)

    """

    def scenario1(self, client_node: CephNode):
        """
        Scenario 1:- Revert stretch mode from health stretch cluster to default crush rules
        Steps:-
        1) Check stretch mode is enabled
        2) Create a pool and write IO
        3) Revert from healthy stretch mode
        4) Validate all pools are reverted to default rules
        5) Validate stretch mode related configs are reset in OSD map
        6) Validate stretch mode related configs are reset in MON map
        7) Validate PGs reach active+clean
        8) Re-enter stretch mode for next scenario
        """
        log.info(self.scenario1.__doc__)
        log.info("Step 1 -> Check if stretch mode is enabled")
        if not stretch_enabled_checks(self.rados_obj):
            log.error(
                "The cluster has not cleared the pre-checks to run stretch tests. Exiting..."
            )
            raise Exception("Test pre-execution checks failed")

        log.info("Step 2 -> Create a pool and write IO")
        pool_name = "revert_scenario_1_pool"
        self.create_pool_and_write_io(pool_name, client_node=client_node)

        log.info("Step 3 ->  Revert from healthy stretch mode")
        self.revert_stretch_mode()

        log.info("Step 4 -> Validate all pools are reverted to default rules")
        expected_pool_properties = {
            "size": "3",
            "min_size": "2",
            "crush_rule": "0",  # default crush rule has a crush rule id of 1
        }
        self.validate_pool_configurations_post_revert(expected_pool_properties)

        log.info("Step 5 -> Validate stretch mode related configs are reset in OSD map")
        expected_osd_map_values = {
            "stretch_mode_enabled": False,
            "stretch_bucket_count": 0,
            "degraded_stretch_mode": 0,
            "recovering_stretch_mode": 0,
            "stretch_mode_bucket": 0,
        }
        self.validate_osd_configurations_post_revert(expected_osd_map_values)

        log.info("Step 6 -> Validate stretch mode related configs are reset in MON map")
        expected_mon_map_values = {
            "tiebreaker_mon": "",
            "stretch_mode": False,
            "tiebreaker_mon": "",
        }
        self.validate_mon_configurations_post_revert(expected_mon_map_values)

        log.info("Step 7 -> Validate PGs reach active+clean")
        wait_for_clean_pg_sets(rados_obj=self.rados_obj)

        log.info(
            "Step 8 ->  Re-enter stretch mode for next scenario and wait till PGs are active+clean"
        )
        self.enable_stretch_mode(self.tiebreaker_mon)
        wait_for_clean_pg_sets(rados_obj=self.rados_obj)

    def scenario2(self, client_node: CephNode):
        """
        Scenario 2:- Revert stretch mode from health stretch cluster to non-default crush rules
        Steps:-
        1) Check stretch mode is enabled
        2) Create a pool and write IO
        3) Create a custom crush rule
        4) Revert from healthy stretch mode
        5) Validate all pools are reverted to default rules
        6) Validate stretch mode related configs are reset in OSD map
        7) Validate stretch mode related configs are reset in MON map
        8) Validate PGs reach active+clean
        9) Re-enter stretch mode for next scenario
        """
        log.info(self.scenario2.__doc__)
        log.info("Step 1 -> Check stretch mode is enabled")
        if not stretch_enabled_checks(self.rados_obj):
            log.error(
                "The cluster has not cleared the pre-checks to run stretch tests. Exiting..."
            )
            raise Exception("Test pre-execution checks failed")

        log.info("Step 2 ->  Create a pool and write IO")
        pool_name = "revert_scenario_2_pool"
        self.create_pool_and_write_io(pool_name, client_node=client_node)

        log.info("Step 3 ->  Create a custom crush rule")
        custom_crush_rule_name = "test_rule"
        custom_crush_rule_id = self.create_or_retrieve_crush_rule(
            crush_rule_name=custom_crush_rule_name
        )

        log.info("Step 4 -> Revert from healthy stretch mode to non-default crush rule")
        self.revert_stretch_mode(crush_rule_name=custom_crush_rule_name)

        log.info("Step 5 -> validate pool configurations are reset")
        expected_pool_properties = {
            "size": "3",
            "min_size": "2",
            "crush_rule": custom_crush_rule_id,
        }
        self.validate_pool_configurations_post_revert(expected_pool_properties)

        log.info("Step 6 -> validate osd configurations are reset")
        expected_osd_map_values = {
            "stretch_mode_enabled": False,
            "stretch_bucket_count": 0,
            "degraded_stretch_mode": 0,
            "recovering_stretch_mode": 0,
            "stretch_mode_bucket": 0,
        }
        self.validate_osd_configurations_post_revert(expected_osd_map_values)

        log.info("Step 7 -> validate mon configurations are reset")
        expected_mon_map_values = {
            "tiebreaker_mon": "",
            "stretch_mode": False,
            "tiebreaker_mon": "",
        }
        self.validate_mon_configurations_post_revert(expected_mon_map_values)

        log.info("Step 8 -> validate PG's reached active+clean")
        wait_for_clean_pg_sets(rados_obj=self.rados_obj)

        log.info(
            "Step 9 ->  Re-enter stretch mode for next scenario and wait till PGs are active+clean"
        )
        self.enable_stretch_mode(self.tiebreaker_mon)
        wait_for_clean_pg_sets(rados_obj=self.rados_obj)
