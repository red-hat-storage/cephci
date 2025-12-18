"""
This test module is used to test revert stretch mode
includes:
1. revert from health stretch mode to default crush rule
2. revert from health stretch mode to custom crush rule
3. revert from netsplit scenario to default crush rule
4. revert from netsplit scenario to custom crush rule
5. revert from site down stretch mode to default crush rule
6. revert from site down stretch mode to custom crush rule
7. Revert to regular cluster when 1 mon daemon from DC1 is down and 1
 mon daemon from DC2 is down
8. Revert to regular cluster when tiebreaker mon is down
9. Revert to regular cluster when all mons from DC1 is down
10. Revert stretch mode during 1 MON host in DC1 and 1 MON host in DC2 is down
11. Revert stretch mode during tiebreaker MON host is down
12. Revert stretch mode and do not reenable stretch mode
13. Negative: Revert stretch mode when cluster is in recovery
14. Negative: Execute disable_stretch_mode when not in stretch mode
15. Negative: Incorrect command usage for disabling stretch mode commands
16. Negative: Disable stretch mode without --yes-i-really-mean-it
17. Negative: Revert stretch mode by passing non-existing crush rule
18. Revert from a degraded stretch mode and enable stretch mode with a new datacenter
"""

import random
import time
from collections import namedtuple

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.monitor_workflows import MonitorWorkflows
from ceph.rados.pool_workflows import PoolFunctions
from ceph.rados.serviceability_workflows import ServiceabilityMethods
from ceph.rados.utils import get_cluster_timestamp
from ceph.utils import find_vm_node_by_hostname
from tests.rados.monitor_configurations import MonElectionStrategies
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from tests.rados.test_stretch_revert_class import (
    RevertStretchModeFunctionalities,
    flush_ip_table_rules_on_all_hosts,
    simulate_netsplit_between_hosts,
    wait_till_host_status_reaches,
)
from tests.rados.test_stretch_site_down import stretch_enabled_checks
from tests.rados.test_stretch_site_reboot import get_host_obj_from_hostname
from utility.log import Log
from utility.utils import generate_unique_id

log = Log(__name__)

Hosts = namedtuple("Hosts", ["dc_1_hosts", "dc_2_hosts", "tiebreaker_hosts"])


def run(ceph_cluster, **kw):
    """
    performs replacement scenarios in stretch mode
    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster
    Scenarios:
        1. revert from health stretch mode to default crush rule
        2. revert from health stretch mode to custom crush rule
        3. revert from netsplit scenario to default crush rule
        4. revert from netsplit scenario to custom crush rule
        5. revert from site down stretch mode to default crush rule
        6. revert from site down stretch mode to custom crush rule
        7. Revert to regular cluster when 1 mon daemon from DC1 is down and 1
         mon daemon from DC2 is down
        8. Revert to regular cluster when tiebreaker mon is down
        9. Revert to regular cluster when all mons from DC1 is down
        10. Revert stretch mode during 1 MON host in DC1 and 1 MON host in DC2 is down
        11. Revert stretch mode during tiebreaker MON host is down
        12. Revert stretch mode and do not reenable stretch mode
        13. Negative: Revert stretch mode when cluster is in recovery
        14. Negative: Execute disable_stretch_mode when not in stretch mode
        15. Negative: Incorrect command usage for disabling stretch mode commands
        16. Negative: Disable stretch mode without --yes-i-really-mean-it
        17. Negative: Revert stretch mode by passing non-existing crush rule
        18. Revert from a degraded stretch mode and enable stretch mode with a new datacenter
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
    mon_obj = MonitorWorkflows(node=cephadm)
    mon_election_obj = MonElectionStrategies(rados_obj=rados_obj)
    test_seprator = "-" * 30
    scenarios_to_run = config.get(
        "scenarios_to_run",
        [
            "scenario1",
            "scenario2",
            "scenario3",
            "scenario4",
            "scenario5",
            "scenario6",
            "scenario7",
            "scenario8",
            "scenario9",
            "scenario10",
            "scenario11",
            "scenario12",
            "scenario13",
            "scenario14",
            "scenario15",
            "scenario16",
            "scenario17",
            "scenario18",
        ],
    )
    config = {
        "rados_obj": rados_obj,
        "pool_obj": pool_obj,
        "tiebreaker_mon_site_name": tiebreaker_mon_site_name,
        "stretch_bucket": stretch_bucket,
        "client_node": client_node,
        "mon_obj": mon_obj,
        "mon_election_obj": mon_election_obj,
    }
    start_time = get_cluster_timestamp(rados_obj.node)
    log.debug(f"Test workflow started. Start time: {start_time}")
    try:
        revert_stretch_mode_scenarios = RevertStretchModeScenarios(**config)
        custom_crush_rule_name = "test_rule"
        custom_crush_rule_id = (
            revert_stretch_mode_scenarios.create_or_retrieve_crush_rule(
                crush_rule_name=custom_crush_rule_name
            )
        )
        custom_crush_rule = {"name": custom_crush_rule_name, "id": custom_crush_rule_id}
        default_crush_rule = {"id": 0}
        dc_1_hosts = revert_stretch_mode_scenarios.site_1_hosts
        dc_2_hosts = revert_stretch_mode_scenarios.site_2_hosts
        tiebreaker_hosts = revert_stretch_mode_scenarios.tiebreaker_hosts

        if "scenario1" in scenarios_to_run:
            log.info(test_seprator)
            revert_stretch_mode_scenarios.scenario1(default_crush_rule)
            log.info(test_seprator)

        if "scenario2" in scenarios_to_run:
            log.info(test_seprator)
            revert_stretch_mode_scenarios.scenario2(custom_crush_rule)
            log.info(test_seprator)

        if "scenario3" in scenarios_to_run:
            log.info(test_seprator)
            revert_stretch_mode_scenarios.netsplit_scenario(
                default_crush_rule, dc_1_hosts, dc_2_hosts
            )
            log.info(test_seprator)

        if "scenario4" in scenarios_to_run:
            log.info(test_seprator)
            revert_stretch_mode_scenarios.netsplit_scenario(
                custom_crush_rule, dc_1_hosts, dc_2_hosts
            )
            log.info(test_seprator)

        if "scenario5" in scenarios_to_run:
            log.info(test_seprator)
            revert_stretch_mode_scenarios.shutdown_scenario(
                default_crush_rule, dc_1_hosts, ceph_cluster, config, mon_obj
            )
            log.info(test_seprator)

        if "scenario6" in scenarios_to_run:
            log.info(test_seprator)
            revert_stretch_mode_scenarios.shutdown_scenario(
                custom_crush_rule, tiebreaker_hosts, ceph_cluster, config, mon_obj
            )
            log.info(test_seprator)

        if "scenario7" in scenarios_to_run:
            log.info(test_seprator)
            revert_stretch_mode_scenarios.scenario7(default_crush_rule)
            log.info(test_seprator)

        if "scenario8" in scenarios_to_run:
            log.info(test_seprator)
            revert_stretch_mode_scenarios.scenario8(default_crush_rule)
            log.info(test_seprator)

        if "scenario9" in scenarios_to_run:
            log.info(test_seprator)
            revert_stretch_mode_scenarios.scenario9(default_crush_rule)
            log.info(test_seprator)

        if "scenario10" in scenarios_to_run:
            log.info(test_seprator)
            revert_stretch_mode_scenarios.scenario10(default_crush_rule, ceph_cluster)
            log.info(test_seprator)

        if "scenario11" in scenarios_to_run:
            log.info(test_seprator)
            revert_stretch_mode_scenarios.scenario11(default_crush_rule, ceph_cluster)
            log.info(test_seprator)

        if "scenario12" in scenarios_to_run:
            log.info(test_seprator)
            revert_stretch_mode_scenarios.scenario12(default_crush_rule)
            log.info(test_seprator)

        if "scenario13" in scenarios_to_run:
            log.info(test_seprator)
            revert_stretch_mode_scenarios.scenario13()
            log.info(test_seprator)

        if "scenario14" in scenarios_to_run:
            log.info(test_seprator)
            revert_stretch_mode_scenarios.scenario14()
            log.info(test_seprator)

        if "scenario15" in scenarios_to_run:
            log.info(test_seprator)
            revert_stretch_mode_scenarios.scenario15()
            log.info(test_seprator)

        if "scenario16" in scenarios_to_run:
            log.info(test_seprator)
            revert_stretch_mode_scenarios.scenario16()
            log.info(test_seprator)

        if "scenario17" in scenarios_to_run:
            log.info(test_seprator)
            revert_stretch_mode_scenarios.scenario17()
            log.info(test_seprator)

        if "scenario18" in scenarios_to_run:
            log.info(test_seprator)
            revert_stretch_mode_scenarios.scenario18(
                ceph_cluster=ceph_cluster,
                config=config,
                crush_rule=default_crush_rule,
            )
            log.info(test_seprator)

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
        test_end_time = get_cluster_timestamp(rados_obj.node)
        log.debug(
            f"Test workflow completed. Start time: {start_time}, End time: {test_end_time}"
        )
        if rados_obj.check_crash_status(start_time=start_time, end_time=test_end_time):
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

    expected_pool_properties = {
        "size": "3",
        "min_size": "2",
        "crush_rule": 0,
    }

    expected_mon_map_values = {
        "tiebreaker_mon": "",
        "stretch_mode": False,
        "disallowed_leaders": "",
    }

    expected_osd_map_values = {
        "stretch_mode_enabled": False,
        "stretch_bucket_count": 0,
        "degraded_stretch_mode": 0,
        "recovering_stretch_mode": 0,
        "stretch_mode_bucket": 0,
    }

    def scenario1(
        self,
        crush_rule: dict,
        skip_wait_for_clean_pg_sets=False,
        skip_stretch_enabled_checks=False,
    ):
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
        if not skip_wait_for_clean_pg_sets:
            log.info(
                "Step 1 -> Wait for clean PGs before starting scenario and Check if stretch mode is enabled"
            )
            if wait_for_clean_pg_sets(rados_obj=self.rados_obj) is False:
                raise Exception(
                    "PG did not reach active+clean before start of site down scenario"
                )

        if not skip_stretch_enabled_checks:
            if not stretch_enabled_checks(self.rados_obj):
                log.error(
                    "The cluster has not cleared the pre-checks to run stretch tests. Exiting..."
                )
                raise Exception("Test pre-execution checks failed")

        log.info("Step 2 -> Create a pool and write IO")
        pool_name = "revert_scenario_1_pool"
        self.create_pool_and_write_io(pool_name, client_node=self.client_node)

        log.info("Step 3 ->  Revert from healthy stretch mode")
        self.revert_stretch_mode()

        log.info("Step 4 -> Validate all pools are reverted to default rules")
        self.expected_pool_properties["crush_rule"] = crush_rule["id"]
        self.validate_pool_configurations_post_revert(self.expected_pool_properties)

        log.info("Step 5 -> Validate stretch mode related configs are reset in OSD map")
        self.validate_osd_configurations_post_revert(self.expected_osd_map_values)

        log.info("Step 6 -> Validate stretch mode related configs are reset in MON map")
        self.validate_mon_configurations_post_revert(self.expected_mon_map_values)

        if skip_wait_for_clean_pg_sets:
            log.info("[SKIPPED] Step 7 -> Validate PGs reach active+clean ")
        else:
            log.info("Step 7 -> Validate PGs reach active+clean")
            if wait_for_clean_pg_sets(rados_obj=self.rados_obj) is False:
                raise Exception(
                    "PGs did not reach active+clean post revert from stretch mode"
                )

        log.info(
            "Step 8 ->  Re-enter stretch mode for next scenario and wait till PGs are active+clean"
        )
        self.enable_stretch_mode(self.tiebreaker_mon)
        if skip_wait_for_clean_pg_sets:
            log.info(
                "[SKIPPED] Validating PGs reach active+clean post enabling stretch mode"
            )
        else:
            if wait_for_clean_pg_sets(rados_obj=self.rados_obj) is False:
                raise Exception(
                    "PGs did not reach active+clean post re-enabling stretch mode"
                )

            if not stretch_enabled_checks(self.rados_obj):
                log.error(
                    "The cluster has not cleared the pre-checks to run stretch tests. Exiting..."
                )
                raise Exception("Test pre-execution checks failed")

    def scenario2(self, crush_rule: dict):
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
        9) Re-enter stretch mode for next scenario and wait for active+clean PG
        """
        log.info(self.scenario2.__doc__)
        log.info(
            "Step 1 -> Wait for clean PGs before starting scenario and Check stretch mode is enabled"
        )
        if wait_for_clean_pg_sets(rados_obj=self.rados_obj) is False:
            raise Exception(
                "PG did not reach active+clean before start of site down scenario"
            )
        if not stretch_enabled_checks(self.rados_obj):
            log.error(
                "The cluster has not cleared the pre-checks to run stretch tests. Exiting..."
            )
            raise Exception("Test pre-execution checks failed")

        log.info("Step 2 ->  Create a pool and write IO")
        pool_name = "revert_scenario_2_pool"
        self.create_pool_and_write_io(pool_name, client_node=self.client_node)

        log.info("Step 4 -> Revert from healthy stretch mode to non-default crush rule")
        self.revert_stretch_mode(crush_rule_name=crush_rule["name"])

        log.info("Step 5 -> validate pool configurations are reset")
        self.expected_pool_properties["crush_rule"] = crush_rule["id"]
        self.validate_pool_configurations_post_revert(self.expected_pool_properties)

        log.info("Step 6 -> validate osd configurations are reset")
        self.validate_osd_configurations_post_revert(self.expected_osd_map_values)

        log.info("Step 7 -> validate mon configurations are reset")
        self.validate_mon_configurations_post_revert(self.expected_mon_map_values)

        log.info("Step 8 -> validate PG's reached active+clean")
        if wait_for_clean_pg_sets(rados_obj=self.rados_obj) is False:
            raise Exception(
                "PGs did not reach active+clean post revert from stretch mode"
            )

        log.info(
            "Step 9 ->  Re-enter stretch mode for next scenario and wait till PGs are active+clean"
        )
        self.enable_stretch_mode(self.tiebreaker_mon)
        if wait_for_clean_pg_sets(rados_obj=self.rados_obj) is False:
            raise Exception(
                "PGs did not reach active+clean post revert from stretch mode"
            )

        if not stretch_enabled_checks(self.rados_obj):
            log.error(
                "The cluster has not cleared the pre-checks to run stretch tests. Exiting..."
            )
            raise Exception("Test pre-execution checks failed")

    def netsplit_scenario(self, crush_rule: dict, group_1_hosts: list, group_2_hosts):
        """
        Scenario :- Revert from netsplit scenario to default/custom crush rule
        Polarion:- CEPH-83620057
        Steps:-
            1) Check stretch mode is enabled
            2) Create a pool and write IO
            3) Simulate a network partition between passed hosts
            4) Revert from degraded stretch mode
            5) Validate all pools are reverted to default rules
            6) Validate stretch mode related configs are reset in OSD map
            7) Validate stretch mode related configs are reset in MON map
            8) Remove the simulated network partition
            9) Validate PGs reach active+clean
            10)  Write IO to the cluster post revert
            11) Re-enter stretch mode for next scenario and wait for active+clean PG
        """
        log.info(self.netsplit_scenario.__doc__)
        log_msg = f"Passed crush rule -> {crush_rule}"
        log.info(log_msg)

        log.info(
            "Step 1 -> Wait for clean PGs before starting scenario and Check stretch mode is enabled"
        )
        if wait_for_clean_pg_sets(rados_obj=self.rados_obj) is False:
            raise Exception(
                "PG did not reach active+clean before start of netsplit scenario"
            )
        if not stretch_enabled_checks(self.rados_obj):
            log.error(
                "The cluster has not cleared the pre-checks to run stretch tests. Exiting..."
            )
            raise Exception("Test pre-execution checks failed")

        log.info("Step 2 ->  Create a pool and write IO")
        pool_name = "netsplit_scenario_" + generate_unique_id(3)
        self.create_pool_and_write_io(pool_name, client_node=self.client_node)
        time.sleep(10)
        init_objects = self.rados_obj.get_cephdf_stats(pool_name=pool_name)["stats"][
            "objects"
        ]

        log.info("Step 3 ->  Simulate a network partition between passed hosts")
        simulate_netsplit_between_hosts(self.rados_obj, group_1_hosts, group_2_hosts)

        # If the netsplit occurs between all hosts of DC1 and all hosts of DC2.
        # Cluster enters into degraded stretch mode.
        # comparing lists as set, since the ordering of the hosts in the list might be differed.
        # list comparison -> [host2, host1] == [host1, host2] -> False
        # set comparison -> set([host2, host1]) == set([host1, host2]) -> True
        if (
            set(group_1_hosts) == set(self.site_1_hosts)
            and set(group_2_hosts) == set(self.site_2_hosts)
        ) or (
            set(group_1_hosts) == set(self.site_2_hosts)
            and set(group_2_hosts) == set(self.site_1_hosts)
        ):
            time.sleep(120)  # sleep for sometime for cluster to enter degraded mode
            if self.is_degraded_stretch_mode() is False:
                raise Exception("Degraded stretch mode is not enabled")

        log.info("Step 4 ->  Revert from degraded stretch mode")
        if crush_rule["id"] == 0:
            self.revert_stretch_mode()
        else:
            self.revert_stretch_mode(crush_rule_name=crush_rule["name"])

        log.info("Step 5 -> validate pool configurations are reset")
        self.expected_pool_properties["crush_rule"] = crush_rule["id"]
        self.validate_pool_configurations_post_revert(self.expected_pool_properties)

        log.info("Step 6 -> validate osd configurations are reset")
        self.validate_osd_configurations_post_revert(self.expected_osd_map_values)

        log.info("Step 7 -> validate mon configurations are reset")
        self.validate_mon_configurations_post_revert(self.expected_mon_map_values)

        log.info("Step 8 -> Remove the simulated network partition")
        flush_ip_table_rules_on_all_hosts(self.rados_obj, group_1_hosts + group_2_hosts)

        log.info("Step 9 -> validate PG's reached active+clean")
        if wait_for_clean_pg_sets(rados_obj=self.rados_obj) is False:
            raise Exception("PG did not reach active+clean post netsplit removal")

        log.info("Step 10 -> Write IO to the cluster post revert")
        self.write_io_and_validate_objects(
            pool_name=pool_name, init_objects=init_objects, obj_name="post_revert"
        )

        log.info(
            "Step 11 ->  Re-enter stretch mode for next scenario and wait till PGs are active+clean"
        )
        self.enable_stretch_mode(self.tiebreaker_mon)
        if wait_for_clean_pg_sets(rados_obj=self.rados_obj) is False:
            raise Exception(
                "PG did not reach active+clean post enabling stretch mode for next scenario"
            )

        if not stretch_enabled_checks(self.rados_obj):
            log.error(
                "The cluster has not cleared the pre-checks to run stretch tests. Exiting..."
            )
            raise Exception("Test pre-execution checks failed")

    def shutdown_scenario(
        self,
        crush_rule,
        hosts_to_shutdown,
        ceph_cluster,
        config,
        mon_obj: MonitorWorkflows,
    ):
        """
        Scenario :- Revert from site down scenario to default/custom crush rule
        Polarion :- CEPH-83620057
        Steps:-
            1) Check stretch mode is enabled
            2) Create a pool and write IO
            3) Simulate a site down scenario
            4) Revert from degraded stretch mode
            5) Validate all pools are reverted to default rules
            6) Validate stretch mode related configs are reset in OSD map
            7) Validate stretch mode related configs are reset in MON map
            8) Remove the down hosts from the cluster
            9) Validate PGs reach active+clean
            10) Write IO to the cluster post revert
            11) Reboot hosts, Add removed hosts & Re-enter stretch mode for next scenario and wait for active+clean PG
        """

        # Pre checks and logging details about parameters
        log.info(self.shutdown_scenario.__doc__)
        log_msg = (
            f"Passed crush rule -> {crush_rule}\n"
            f"Hosts to shutdown -> {hosts_to_shutdown}"
        )
        log.info(log_msg)
        if len(hosts_to_shutdown) == 0:
            raise Exception("Hosts must be passed for shutdown scenario")

        log.info(
            "Step 1 -> Wait for clean PGs before starting scenario and Check stretch mode is enabled"
        )
        if wait_for_clean_pg_sets(rados_obj=self.rados_obj) is False:
            raise Exception(
                "PG did not reach active+clean before start of site down scenario"
            )
        if not stretch_enabled_checks(self.rados_obj):
            log.error(
                "The cluster has not cleared the pre-checks to run stretch tests. Exiting..."
            )
            raise Exception("Test pre-execution checks failed")

        log.info("Step 2 ->  Create a pool and write IO")
        pool_name = "site_down_scenario_" + generate_unique_id(3)
        self.create_pool_and_write_io(pool_name, client_node=self.client_node)
        time.sleep(10)
        init_objects = self.rados_obj.get_cephdf_stats(pool_name=pool_name)["stats"][
            "objects"
        ]

        # {'host1': ["mon", "osd", "mgr"],....
        # Storing host labels inorder to identify mon hosts post addition.
        host_labels_map = {}
        for host in self.site_1_hosts + self.site_2_hosts + self.tiebreaker_hosts:
            host_labels_map[host] = mon_obj.get_host_labels(host=host)
        log_msg = f"Host labels are {host_labels_map}"
        log.info(log_msg)

        log.info("Step 3 ->  Simulate site down scenario")
        for host in hosts_to_shutdown:
            log.debug(f"Proceeding to shutdown host {host}")
            target_node = find_vm_node_by_hostname(ceph_cluster, host)
            target_node.shutdown(wait=True)
        log.info(f"Completed shutdown of all the hosts ->  {hosts_to_shutdown}.")

        # If DC1 site is down or DC2 site is down.
        # Cluster enters into degraded stretch mode
        # comparing lists as set, since the ordering of the hosts in the list might be differ.
        # list comparison -> [host2, host1] == [host1, host2] -> False
        # set comparison -> set([host2, host1]) == set([host1, host2]) -> True
        if set(hosts_to_shutdown) == set(self.site_1_hosts) or set(
            hosts_to_shutdown
        ) == set(self.site_2_hosts):
            time.sleep(120)  # sleep for sometime for cluster to enter degraded mode
            if self.is_degraded_stretch_mode() is False:
                raise Exception("Degraded stretch mode is not enabled")

        log.info("Step 4 ->  Revert from degraded stretch mode")
        if crush_rule["id"] == 0:
            self.revert_stretch_mode()
        else:
            self.revert_stretch_mode(crush_rule_name=crush_rule["name"])

        log.info("Step 5 -> validate pool configurations are reset")
        self.expected_pool_properties["crush_rule"] = crush_rule["id"]
        self.validate_pool_configurations_post_revert(self.expected_pool_properties)

        log.info("Step 6 -> validate osd configurations are reset")
        self.validate_osd_configurations_post_revert(self.expected_osd_map_values)

        log.info("Step 7 -> validate mon configurations are reset")
        self.validate_mon_configurations_post_revert(self.expected_mon_map_values)

        log.info("Step 8 -> Remove the down hosts from the cluster")
        serviceability_methods = ServiceabilityMethods(cluster=ceph_cluster, **config)
        for host_name in hosts_to_shutdown:
            serviceability_methods.remove_offline_host(host_node_name=host_name)

        log.info("Step 9 -> validate PG's reached active+clean")
        if wait_for_clean_pg_sets(rados_obj=self.rados_obj) is False:
            raise Exception(
                "PGs did not reach active+clean post revert and offline host removal"
            )

        log.info("Step 10 -> Write IO to the cluster post revert")
        self.write_io_and_validate_objects(
            pool_name=pool_name, init_objects=init_objects, obj_name="post_revert"
        )

        log.info(
            "Step 11 -> Reboot hosts, Add removed hosts &"
            "Re-enter stretch mode for next scenario and wait till PGs are active+clean"
        )
        # Reverting back the cluster status for next scenario
        # 1) Reboot the hosts which were shutdown
        # 2) Add back the removed hosts back to the cluster
        # 3) Move the added hosts under datcenter crush bucket
        # 4) Set mon crush locations
        # 5) Re-enable stretch mode and wait for clean PGs
        log.info(f"Proceeding to reboot hosts -> {hosts_to_shutdown}")
        for host in hosts_to_shutdown:
            log.debug(f"Proceeding to restart host {host}")
            target_node = find_vm_node_by_hostname(ceph_cluster, host)
            target_node.power_on()
            fsid = self.rados_obj.run_ceph_command(cmd="ceph fsid", client_exec=True)[
                "fsid"
            ]
            host_obj = get_host_obj_from_hostname(
                hostname=host, rados_obj=self.rados_obj
            )
            cmd = f"cephadm rm-cluster --force --zap-osds --fsid {fsid}"
            host_obj.exec_command(cmd=cmd, sudo=True)
        log.info(f"Completed restart of all the hosts -> {hosts_to_shutdown}")

        log.info(f"Proceeding to Add back the removed hosts -> {hosts_to_shutdown}")
        for host in hosts_to_shutdown:
            self.rados_obj.set_service_managed_type(service_type="osd", unmanaged=True)
            log.debug(f"Proceeding to add host {host}")
            serviceability_methods.add_new_hosts(
                add_nodes=[host], deploy_osd=True, osd_label="osd"
            )
        self.rados_obj.set_service_managed_type(service_type="osd", unmanaged=False)

        log.info("Proceeding to move the added hosts under Datacenter buckets")
        for crush_bucket_name in hosts_to_shutdown:
            site_name = self.tiebreaker_mon_site_name
            if crush_bucket_name in self.site_1_hosts:
                site_name = self.site_1_name
            elif crush_bucket_name in self.site_2_hosts:
                site_name = self.site_2_name
            log_info_msg = f"Moving crush bucket {crush_bucket_name} under crush bucket {site_name}"
            log.info(log_info_msg)
            cmd = f"ceph osd crush move {crush_bucket_name} {self.stretch_bucket}={site_name}"
            if self.rados_obj.run_ceph_command(cmd=cmd) is None:
                log_msg = f"Failed to move crush bucket {crush_bucket_name} under crush bucket {site_name}"
                log.error(log_msg)
                raise Exception(log_msg)
            log_info_msg = f"Successfully moved crush bucket {crush_bucket_name} under crush bucket {site_name}"
            log.info(log_info_msg)

        log.info("Setting crush location for each monitor for next scenario")
        for host in hosts_to_shutdown:
            if "mon" not in host_labels_map[host]:
                log_msg = (
                    f"host {host} did not have mon daemon before removal, Skipping"
                )
                log.info(log_msg)
                continue
            log_info_msg = f"Setting location for mon {host}"
            log.info(log_info_msg)
            site_name = self.tiebreaker_mon_site_name
            if host in self.site_1_hosts:
                site_name = self.site_1_name
            elif host in self.site_2_hosts:
                site_name = self.site_2_name
            cmd = f"ceph mon set_location {host} {self.stretch_bucket}={site_name}"
            if self.rados_obj.run_ceph_command(cmd=cmd) is None:
                log_msg = f"Failed to set mon location of {host} to {site_name}"
                log.error(log_msg)
                raise Exception(log_msg)
            log_info_msg = f"Successfully set mon location of {host} to {site_name}"
            log.info(log_info_msg)

        log.info(f"Completed adding all the hosts -> {hosts_to_shutdown}")

        log.info("Proceeding to re-enable stretch mode")
        self.enable_stretch_mode(self.tiebreaker_mon)
        if wait_for_clean_pg_sets(rados_obj=self.rados_obj) is False:
            raise Exception(
                "PG did not reach active+clean post enabling stretch mode for next scenario"
            )

        if not stretch_enabled_checks(self.rados_obj):
            log.error(
                "The cluster has not cleared the pre-checks to run stretch tests. Exiting..."
            )
            raise Exception("Test pre-execution checks failed")

    def scenario7(self, crush_rule):
        """
        Scenario 7:- Revert stretch mode during 1 mon daemon in DC1 Host1 and 1 mon daemon in DC2 Host1 failure
        Steps:-
            1) Shutdown 1 mon daemon in DC1 Host1 and 1 mon daemon in DC2 Host1
            2) Check stretch mode is enabled
            3) Create a pool and write IO
            4) Revert from healthy stretch mode
            5) Validate all pools are reverted to default rules
            6) Validate stretch mode related configs are reset in OSD map
            7) Validate stretch mode related configs are reset in MON map
            8) Validate PGs reach active+clean
            9) Re-enter stretch mode for next scenario
        """
        # Stop MON in stretch mode
        # shutdown 1 mon in DC1
        # shutdown 1 mon in DC2
        log.info(
            "Scenario 7 ->  Shutdown 1 mon daemon in DC1 Host1 and 1 mon daemon in DC2 Host1"
        )
        for mon in [self.site_1_mon_hosts[0], self.site_2_mon_hosts[0]]:
            log.debug(f"Proceeding to shutdown MON {mon}")
            if not self.rados_obj.change_daemon_systemctl_state(
                action="stop",
                daemon_type="mon",
                daemon_id=mon,
            ):
                log.error(f"Failed to stop mon on host {mon}")
                raise Exception("Mon stop failure error")
            log.info(f"Completed shutdown of mons ->  {mon}.")

        # Revert stretch mode -> perform validation -> enable stretch mode
        self.scenario1(crush_rule, skip_stretch_enabled_checks=True)

        # Restart MON in stretch mode
        # start 1 mon in DC1
        # start 1 mon in DC2
        for mon in [self.site_1_mon_hosts[0], self.site_2_mon_hosts[0]]:
            log.debug(f"Proceeding to start MON {mon}")
            if not self.rados_obj.change_daemon_systemctl_state(
                action="start",
                daemon_type="mon",
                daemon_id=mon,
            ):
                log.error(f"Failed to start mon on host {mon}")
                raise Exception("Mon start failure error")
            log.info(f"Completed startup of mons ->  {mon}.")

        self.check_mon_in_running_state()

    def scenario8(self, crush_rule):
        """
        Scenario 8:- Revert stretch mode during tiebreaker mon daemon failure
        Steps:-
            1) Shutdown tiebreaker mon daemon
            2) Check stretch mode is enabled
            3) Create a pool and write IO
            4) Revert from healthy stretch mode
            5) Validate all pools are reverted to default rules
            6) Validate stretch mode related configs are reset in OSD map
            7) Validate stretch mode related configs are reset in MON map
            8) Validate PGs reach active+clean
            9) Re-enter stretch mode for next scenario
        """
        # stop the tiebreaker mon
        log.info("Scenario 8 ->  Shutdown tiebreaker mon daemon")
        mon = self.tiebreaker_mon
        log.debug(f"Proceeding to shutdown MON {mon}")
        if not self.rados_obj.change_daemon_systemctl_state(
            action="stop",
            daemon_type="mon",
            daemon_id=mon,
        ):
            log.error(f"Failed to stop mon on host {mon}")
            raise Exception("Mon stop failure error")
        log.info(f"Completed shutdown of mons ->  {mon}.")

        # Revert stretch mode -> perform validation -> enable stretch mode
        self.scenario1(crush_rule, skip_stretch_enabled_checks=True)

        #  start the tiebreaker mon
        log.debug(f"Proceeding to start MON {mon}")
        if not self.rados_obj.change_daemon_systemctl_state(
            action="start",
            daemon_type="mon",
            daemon_id=mon,
        ):
            log.error(f"Failed to start mon on host {mon}")
            raise Exception("Mon start failure error")
        log.info(f"Completed startup of mons ->  {mon}.")

        self.check_mon_in_running_state()

    def scenario9(self, crush_rule):
        """
        Scenario 9:- Revert stretch mode during all mon daemons in DC1 failure
        Steps:-
            1) stop all mon daemons in DC1 using systemctl
            2) Check stretch mode is enabled
            3) Create a pool and write IO
            4) Revert from healthy stretch mode
            5) Validate all pools are reverted to default rules
            6) Validate stretch mode related configs are reset in OSD map
            7) Validate stretch mode related configs are reset in MON map
            8) Validate PGs reach active+clean
            9) Re-enter stretch mode for next scenario
        """
        # stop all mons in DC1
        log.info("Scenario 9 ->  Stop all mon daemons in DC1 using systemctl")
        for mon in self.site_1_mon_hosts:
            log.debug(f"Proceeding to shutdown MON {mon}")
            if not self.rados_obj.change_daemon_systemctl_state(
                action="stop",
                daemon_type="mon",
                daemon_id=mon,
            ):
                log.error(f"Failed to stop mon on host {mon}")
                raise Exception("Mon stop failure error")
            log.info(f"Completed shutdown of mons ->  {mon}.")

        # Wait till stretch mode enters degraded state
        self.wait_till_stretch_mode_status(degraded=True)

        # Revert stretch mode -> perform validation -> enable stretch mode
        self.scenario1(
            crush_rule,
            skip_wait_for_clean_pg_sets=True,
            skip_stretch_enabled_checks=True,
        )

        # Restart all mons from DC1
        for mon in self.site_1_mon_hosts:
            log.debug(f"Proceeding to start MON {mon}")
            if not self.rados_obj.change_daemon_systemctl_state(
                action="start",
                daemon_type="mon",
                daemon_id=mon,
            ):
                log.error(f"Failed to start mon on host {mon}")
                raise Exception("Mon start failure error")
            log.info(f"Completed startup of mons ->  {mon}.")

        self.check_mon_in_running_state()
        if wait_for_clean_pg_sets(rados_obj=self.rados_obj) is False:
            raise Exception(
                "PGs did not reach active+clean post starting all Mons from DC1"
            )

    def scenario10(self, crush_rule, ceph_cluster):
        """
        Scenario 10:- Revert stretch mode during 1 MON host in DC1 and 1 MON host in DC2 is down
        Steps:-
            1) Shutdown 1 MON host in DC1 and 1 MON host in DC2
            2) Disable stretch mode
            3) Validate all pools are reverted to default rules
            4) Validate stretch mode related configs are reset in OSD map
            5) Validate stretch mode related configs are reset in MON map
            6)  Wait for clean PG sets
            7) Reboot 1 MON host in DC1 and 1 MON host in DC2
            8) Re-enter stretch mode for next scenario
            9) Wait for PGs to reach active+clean
            10) Perform stretch mode checks
        """
        log.info(self.scenario10.__doc__)
        # Shut down 1 MON host in DC1 and 1 MON host in DC2
        log.info("Scenario 10 ->  1 MON host in DC1 and 1 MON host in DC2")
        DC1_mon_host = self.site_1_mon_hosts[0]
        DC2_mon_host = self.site_2_mon_hosts[0]

        log.info("Step 1 -> Shutdown 1 MON host in DC1 and 1 MON host in DC2")
        for mon_host in [DC1_mon_host, DC2_mon_host]:
            log.debug(f"Proceeding to shutdown host {mon_host}")
            target_node = find_vm_node_by_hostname(ceph_cluster, mon_host)
            target_node.shutdown(wait=True)
            log.info(f"Completed shutdown of mon host ->  {mon_host}.")

        log.info("Step 2 ->  Disable stretch mode")
        self.revert_stretch_mode()

        log.info("Step 3 -> Validate all pools are reverted to default rules")
        self.expected_pool_properties["crush_rule"] = crush_rule["id"]
        self.validate_pool_configurations_post_revert(self.expected_pool_properties)

        log.info("Step 4 -> Validate stretch mode related configs are reset in OSD map")
        self.validate_osd_configurations_post_revert(self.expected_osd_map_values)

        log.info("Step 5 -> Validate stretch mode related configs are reset in MON map")
        self.validate_mon_configurations_post_revert(self.expected_mon_map_values)

        log.info("Step 6 -> Wait for clean PG sets")
        if wait_for_clean_pg_sets(rados_obj=self.rados_obj) is False:
            raise Exception(
                "PGs did not reach active+clean post starting tiebreaker mon host"
            )

        log.info("Step 7 -> Reboot 1 MON host in DC1 and 1 MON host in DC2")
        for mon_host in [DC1_mon_host, DC2_mon_host]:
            log.debug(f"Proceeding to restart host {mon_host}")
            target_node = find_vm_node_by_hostname(ceph_cluster, mon_host)
            target_node.power_on()
            log.info(
                f"Completed Reboot of 1 MON host in DC1 and 1 MON host in DC2 ->  {mon_host}."
            )

        log.info("Step 8 -> Enable Stretch mode")
        self.enable_stretch_mode(self.tiebreaker_mon)

        log.info("Step 9 -> Wait for clean PG sets")
        if wait_for_clean_pg_sets(rados_obj=self.rados_obj) is False:
            raise Exception(
                "PGs did not reach active+clean post starting Reboot 1 MON host in DC1 and 1 MON host in DC2"
            )

        log.info("Step 10 -> Perform stretch mode checks")
        if not stretch_enabled_checks(self.rados_obj):
            log.error("The cluster has not cleared the stretch mode checks. Exiting...")
            raise Exception("Stretch mode checks failed")

    def scenario11(self, crush_rule, ceph_cluster):
        """
        Scenario 11:- Revert stretch mode during tiebreaker MON host is down
        Steps:-
            1) Shutdown Tiebreaker mon host
            2) Revert from stretch mode
            3) Validate all pools are reverted to default rules
            4) Validate stretch mode related configs are reset in OSD map
            5) Validate stretch mode related configs are reset in MON map
            6) Enable stretch mode
            7) Restart Tiebreaker MON host
            8) Wait for clean PG sets
            9) Perform stretch mode checks
        """
        log.info(self.scenario11.__doc__)
        log.info("Scenario 11 ->  Shut down tiebreaker host")
        mon_host = self.tiebreaker_mon

        log.debug(f"Step 1 -> Proceeding to shutdown host {mon_host}")
        target_node = find_vm_node_by_hostname(ceph_cluster, mon_host)
        target_node.shutdown(wait=True)
        log.info(f"Completed shutdown of mon host ->  {mon_host}.")

        log.info("Step 2 ->  Disable stretch mode")
        self.revert_stretch_mode()

        log.info("Step 3 -> Validate all pools are reverted to default rules")
        self.expected_pool_properties["crush_rule"] = crush_rule["id"]
        self.validate_pool_configurations_post_revert(self.expected_pool_properties)

        log.info("Step 4 -> Validate stretch mode related configs are reset in OSD map")
        self.validate_osd_configurations_post_revert(self.expected_osd_map_values)

        log.info("Step 5 -> Validate stretch mode related configs are reset in MON map")
        self.validate_mon_configurations_post_revert(self.expected_mon_map_values)

        # Method needs to be updated to handle client request, since installer node
        # is down
        # if wait_for_clean_pg_sets(rados_obj=self.rados_obj) is False:
        #     raise Exception(
        #         "PGs did not reach active+clean post starting tiebreaker mon host"
        #     )

        log.info("Step 6 -> Enable stretch mode")
        self.enable_stretch_mode(self.tiebreaker_mon)

        log.info("Step 7 -> Restart Tiebreaker MON host")
        log.debug(f"Proceeding to restart host {mon_host}")
        target_node = find_vm_node_by_hostname(ceph_cluster, mon_host)
        target_node.power_on()
        log.info(f"Completed Reboot of Tiebreaker MON host ->  {mon_host}.")

        log.info("Step 8 -> Wait for clean PG sets")
        if wait_for_clean_pg_sets(rados_obj=self.rados_obj) is False:
            raise Exception(
                "PGs did not reach active+clean post starting tiebreaker mon host"
            )

        log.info("Step 9 -> Perform stretch mode checks")
        if not stretch_enabled_checks(self.rados_obj):
            log.error("The cluster has not cleared the stretch mode checks. Exiting...")
            raise Exception("Stretch mode checks failed")

    def scenario12(
        self,
        crush_rule: dict,
    ):
        """
        Scenario 12:- Revert stretch mode from healthy stretch cluster to default crush rules
        and do not re-enable stretch mode
        Steps:-
        1) Check stretch mode is enabled
        2) Revert from healthy stretch mode
        3) Validate all pools are reverted to default rules
        4) Validate stretch mode related configs are reset in OSD map
        5) Validate stretch mode related configs are reset in MON map
        6) Validate PGs reach active+clean
        """
        log.info(self.scenario12.__doc__)
        if wait_for_clean_pg_sets(rados_obj=self.rados_obj) is False:
            raise Exception(
                "PG did not reach active+clean before start of site down scenario"
            )

        if not stretch_enabled_checks(self.rados_obj):
            log.error(
                "The cluster has not cleared the pre-checks to run stretch tests. Exiting..."
            )
            raise Exception("Test pre-execution checks failed")

        self.revert_stretch_mode()

        self.expected_pool_properties["crush_rule"] = crush_rule["id"]
        self.validate_pool_configurations_post_revert(self.expected_pool_properties)

        self.validate_osd_configurations_post_revert(self.expected_osd_map_values)

        self.validate_mon_configurations_post_revert(self.expected_mon_map_values)

        if wait_for_clean_pg_sets(rados_obj=self.rados_obj) is False:
            raise Exception(
                "PGs did not reach active+clean post revert from stretch mode"
            )

    def scenario13(self):
        """
        Scenario 13:- Negative test: Revert stretch mode when cluster is in recovery
        Steps:-
            1) Check stretch mode is enabled
            2) Create a pool and write IO
            3) Trigger recovery by stopping an OSD
            4) Verify cluster is in recovery state
            5) Attempt to revert stretch mode (should fail)
            6) Verify error message
            7) Unset recovery flags and wait for recovery to complete
            8) Re-enter stretch mode for next scenario
        """
        log.info(self.scenario13.__doc__)
        log.info(
            "Step 1 -> Wait for clean PGs before starting scenario and Check stretch mode is enabled"
        )
        if wait_for_clean_pg_sets(rados_obj=self.rados_obj) is False:
            raise Exception(
                "PG did not reach active+clean before start of recovery scenario"
            )
        if not stretch_enabled_checks(self.rados_obj):
            log.error(
                "The cluster has not cleared the pre-checks to run stretch tests. Exiting..."
            )
            raise Exception("Test pre-execution checks failed")

        log.info("Step 2 -> Create a pool and write IO")
        pool_name = "rados_" + generate_unique_id(3)
        self.create_pool_and_write_io(pool_name, client_node=self.client_node)

        log.info("Step 3 -> Make stretch cluster enter degraded stretch mode state")
        simulate_netsplit_between_hosts(
            self.rados_obj, self.site_1_hosts, self.site_2_hosts
        )
        simulate_netsplit_between_hosts(
            self.rados_obj, self.site_2_hosts, self.site_1_hosts
        )

        self.wait_till_stretch_mode_status_reaches(status="degraded")

        log.info("Step 4 -> Make stretch cluster enter recovering stretch mode state")
        flush_ip_table_rules_on_all_hosts(self.rados_obj, self.site_1_hosts)
        flush_ip_table_rules_on_all_hosts(self.rados_obj, self.site_2_hosts)

        self.wait_till_stretch_mode_status_reaches(status="recovering")

        log.info("Step 5 -> Attempt to revert stretch mode (should fail)")
        cmd = "ceph mon disable_stretch_mode --yes-i-really-mean-it"
        try:
            self.client_node.exec_command(cmd=cmd, pretty_print=True, check_ec=True)
            log.error(
                "Expected revert_stretch_mode to fail when cluster is in recovery, but it succeeded"
            )
            raise Exception(
                "Negative test failed: revert_stretch_mode should have failed during recovery"
            )
        except Exception as e:
            log.info("Step 6 -> Verify error message:{}".format(str(e)))
            """
            Error message:
                Error EBUSY: stretch mode is currently recovering and cannot be disabled
            """
            if "recovering" in str(e):
                log.info("Correctly received error about recovery state. err msg is :")
                log.info(str(e))
            else:
                raise Exception(e)

        self.rados_obj.delete_pool(pool=pool_name)

        log.info("Step 7 -> Verify stretch mode is still enabled")
        stretch_details = self.rados_obj.get_stretch_mode_dump()
        if not stretch_details["stretch_mode_enabled"]:
            log.error("Stretch mode was disabled by incorrect commands")
            raise Exception(
                "Stretch mode should still be enabled after incorrect commands"
            )
        log.info("Stretch mode is still enabled as expected")

    def scenario14(self):
        """
        Scenario 14:- Negative test: Execute disable_stretch_mode when not in stretch mode
        Steps:-
            1) Check stretch mode is enabled
            2) Disable stretch mode
            3) Verify stretch mode is disabled
            4) Attempt to disable stretch mode again (should fail)
            5) Verify error message
            6) Re-enter stretch mode for next scenario
        """
        log.info(self.scenario14.__doc__)
        log.info(
            "Step 1 -> Wait for clean PGs before starting scenario and Check stretch mode is enabled"
        )
        if wait_for_clean_pg_sets(rados_obj=self.rados_obj) is False:
            raise Exception(
                "PG did not reach active+clean before start of negative scenario"
            )
        if not stretch_enabled_checks(self.rados_obj):
            log.error(
                "The cluster has not cleared the pre-checks to run stretch tests. Exiting..."
            )
            raise Exception("Test pre-execution checks failed")

        log.info("Step 2 -> Disable stretch mode")
        self.revert_stretch_mode()

        log.info("Step 3 -> Verify stretch mode is disabled")
        stretch_details = self.rados_obj.get_stretch_mode_dump()
        if stretch_details["stretch_mode_enabled"] == 1:
            log.error("Stretch mode is still enabled after disable command")
            raise Exception("Stretch mode disable verification failed")
        log.info("Stretch mode is successfully disabled")

        log.info(
            "Step 4 -> Attempt to disable stretch mode when not in stretch mode (should fail)"
        )
        cmd = "ceph mon disable_stretch_mode --yes-i-really-mean-it"
        try:
            self.client_node.exec_command(cmd=cmd, pretty_print=True, check_ec=True)
            log.error(
                "Expected revert_stretch_mode to fail when not in stretch mode, but it succeeded"
            )
            raise Exception(
                "Negative test failed: revert_stretch_mode should have failed when not in stretch mode"
            )
        except Exception as e:
            log.info(f"Step 5 -> Verify error message: {str(e)}")
            """
            Error message:
                Error EINVAL: stretch mode is already disabled
            """
            if "stretch mode is already disabled" in str(e):
                log.info(
                    "Correctly received error about stretch mode not being enabled \n err msg is :{}".format(
                        str(e)
                    )
                )
            else:
                raise Exception(e)

        log.info("Step 6 -> Re-enter stretch mode for next scenario")
        self.enable_stretch_mode(self.tiebreaker_mon)
        if wait_for_clean_pg_sets(rados_obj=self.rados_obj) is False:
            raise Exception(
                "PGs did not reach active+clean post re-enabling stretch mode"
            )

        if not stretch_enabled_checks(self.rados_obj):
            log.error(
                "The cluster has not cleared the pre-checks to run stretch tests. Exiting..."
            )
            raise Exception("Test pre-execution checks failed")

    def scenario15(self):
        """
        Scenario 15:- Negative test: Incorrect command usage for disabling stretch mode commands
        Steps:-
            1) Check stretch mode is enabled
            2) Attempt to use incorrect command syntax (should fail)
            3) Verify error message
            4) Verify stretch mode is still enabled
        """
        log.info(self.scenario15.__doc__)
        log.info(
            "Step 1 -> Wait for clean PGs before starting scenario and Check stretch mode is enabled"
        )
        if wait_for_clean_pg_sets(rados_obj=self.rados_obj) is False:
            raise Exception(
                "PG did not reach active+clean before start of negative scenario"
            )
        if not stretch_enabled_checks(self.rados_obj):
            log.error(
                "The cluster has not cleared the pre-checks to run stretch tests. Exiting..."
            )
            raise Exception("Test pre-execution checks failed")

        log.info("Step 2 -> Attempt to use incorrect command syntax (should fail)")
        incorrect_commands = [
            "ceph mon disable_stretch_mode --invalid-flag",
            "ceph mon disable_stretch_mode --yes-i-really-mean-it invalid_rule_name extra_arg",
        ]

        for cmd in incorrect_commands:
            log.info(f"Testing incorrect command: {cmd}")
            try:
                self.client_node.exec_command(cmd=cmd, pretty_print=True, check_ec=True)
                log.error(f"Expected command '{cmd}' to fail, but it succeeded")
                raise Exception(
                    "Negative test failed: incorrect command should have failed: {}".format(
                        cmd
                    )
                )
            except Exception as e:
                log.info(f"Step 3 -> Verify error message for '{cmd}': {str(e)}")
                """
                Error message 1:
                    Invalid command: Unexpected argument '--invalid-flag'
                    mon disable_stretch_mode [<crush_rule>] [--yes-i-really-mean-it] :  disable stretch mode,
                     reverting to normal peering rules
                    Error EINVAL: invalid command

                Error message 2:
                    extra_arg not valid:  extra_arg not one of 'true', 'false'
                    Invalid command: unused arguments: ['extra_arg']
                    mon disable_stretch_mode [<crush_rule>] [--yes-i-really-mean-it] :  disable stretch mode,
                     reverting to normal peering rules
                    Error EINVAL: invalid command
                """
                if (
                    "invalid command" in str(e)
                    or "unused arguments" in str(e)
                    or "extra_arg not valid" in str(e)
                ):
                    log.info(
                        "Correctly received error for incorrect command syntax \n {}".format(
                            str(e)
                        )
                    )
                else:
                    raise Exception(e)

        log.info("Step 4 -> Verify stretch mode is still enabled")
        stretch_details = self.rados_obj.get_stretch_mode_dump()
        if not stretch_details["stretch_mode_enabled"]:
            log.error("Stretch mode was disabled by incorrect commands")
            raise Exception(
                "Stretch mode should still be enabled after incorrect commands"
            )
        log.info("Stretch mode is still enabled as expected")

    def scenario16(self):
        """
        Scenario 16:- Negative test: Disable stretch mode without --yes-i-really-mean-it
        Steps:-
            1) Check stretch mode is enabled
            2) Attempt to disable stretch mode without --yes-i-really-mean-it flag (should fail)
            3) Verify error message
            4) Verify stretch mode is still enabled
        """
        log.info(self.scenario16.__doc__)
        log.info(
            "Step 1 -> Wait for clean PGs before starting scenario and Check stretch mode is enabled"
        )
        if wait_for_clean_pg_sets(rados_obj=self.rados_obj) is False:
            raise Exception(
                "PG did not reach active+clean before start of negative scenario"
            )
        if not stretch_enabled_checks(self.rados_obj):
            log.error(
                "The cluster has not cleared the pre-checks to run stretch tests. Exiting..."
            )
            raise Exception("Test pre-execution checks failed")

        log.info(
            "Step 2 -> Attempt to disable stretch mode without "
            "--yes-i-really-mean-it flag (should fail)"
        )
        cmd = "ceph mon disable_stretch_mode"
        try:
            self.client_node.exec_command(cmd=cmd, pretty_print=True, check_ec=True)
            log.error(
                f"Expected command '{cmd}' to fail without "
                "--yes-i-really-mean-it, but it succeeded"
            )
            raise Exception(
                "Negative test failed: disable_stretch_mode should have failed "
                "without confirmation flag"
            )
        except Exception as e:
            log.info(f"Step 3 -> Verify error message: {str(e)}")
            """
            Error message:
                Error EPERM:  This command will disable stretch mode,
                 which means all your pools will be reverted
                back to the default size, min_size and crush_rule. Pass --yes-i-really-mean-it to proceed.
            """
            if "yes-i-really-mean-it" in str(e):
                log.info(
                    "Correctly received error about missing confirmation flag. \n {}".format(
                        str(e)
                    )
                )
            else:
                raise Exception(e)

        log.info("Step 4 -> Verify stretch mode is still enabled")
        stretch_details = self.rados_obj.get_stretch_mode_dump()
        if not stretch_details["stretch_mode_enabled"]:
            log.error("Stretch mode was disabled without confirmation flag")
            raise Exception(
                "Stretch mode should still be enabled without confirmation flag"
            )
        log.info("Stretch mode is still enabled as expected")

    def scenario17(self):
        """
        Scenario 17:- Negative test: Revert stretch mode by passing non-existing crush rule
        Steps:-
            1) Check stretch mode is enabled
            2) Attempt to revert stretch mode with non-existing crush rule (should fail)
            3) Verify error message
            4) Verify stretch mode is still enabled
        """
        log.info(self.scenario17.__doc__)
        log.info(
            "Step 1 -> Wait for clean PGs before starting scenario and Check stretch mode is enabled"
        )
        if wait_for_clean_pg_sets(rados_obj=self.rados_obj) is False:
            raise Exception(
                "PG did not reach active+clean before start of negative scenario"
            )
        if not stretch_enabled_checks(self.rados_obj):
            log.error(
                "The cluster has not cleared the pre-checks to run stretch tests. Exiting..."
            )
            raise Exception("Test pre-execution checks failed")

        log.info(
            "Step 2 -> Attempt to revert stretch mode with non-existing "
            "crush rule (should fail)"
        )
        non_existing_rule = "non_existing_rule_" + generate_unique_id(5)
        cmd = (
            f"ceph mon disable_stretch_mode {non_existing_rule} "
            "--yes-i-really-mean-it"
        )
        try:
            self.client_node.exec_command(cmd=cmd, pretty_print=True, check_ec=True)
            log.error(
                f"Expected command '{cmd}' to fail with non-existing rule, "
                "but it succeeded"
            )
            raise Exception(
                f"Negative test failed: disable_stretch_mode should have failed "
                f"with non-existing rule: {non_existing_rule}"
            )
        except Exception as e:
            log.info(f"Step 3 -> Verify error message: {str(e)}")
            """
            Error message:
                Error EINVAL: unrecognized crush rule non_existing_rule_ME48G
            """
            if "unrecognized crush rule" in str(e):
                log.info(
                    "Correctly received error about non-existing crush rule. \n {}".format(
                        str(e)
                    )
                )
            else:
                raise Exception(e)

        log.info("Step 4 -> Verify stretch mode is still enabled")
        stretch_details = self.rados_obj.get_stretch_mode_dump()
        if not stretch_details["stretch_mode_enabled"]:
            log.error("Stretch mode was disabled with non-existing rule")
            raise Exception(
                "Stretch mode should still be enabled with non-existing rule"
            )
        log.info("Stretch mode is still enabled as expected")

    def scenario18(self, ceph_cluster, config, crush_rule, shutdown=True):
        """
        Scenario 18:- Revert from a degraded stretch mode and enable stretch mode with a new datacenter
        Steps:-
            1) Wait for clean PGs before starting scenario and verify stretch mode is enabled
            2) Create a pool and write IO
            3) Simulate Datacenter DC2 failure by shutting down all hosts in DC2 (or skip if shutdown=False)
            4) Revert stretch mode to regular cluster
            5) Validate pool configurations are reset
            6) Validate OSD configurations are reset
            7) Validate MON configurations are reset
            8) Remove failed hosts from the cluster
            9) Wait for PGs to reach active+clean after host removal
            10) Add new bucket DC3 of type datacenter and move it under root
            11) Restart the stopped hosts and remove the cluster from those nodes
            12) Add new hosts to the cluster and move them under DC3
            13) Modify the site-affinity CRUSH rule to remove DC2 and include DC3
            14) Remove DC2 from the CRUSH map
            15) Enable stretch mode with surviving datacenter DC1 and new datacenter DC3
            16) Wait for PGs to reach active+clean
            17) Perform stretch mode checks
            18) Write IO to the cluster post stretch mode enable
        """
        log.info(self.scenario18.__doc__)
        log.info("=" * 80)
        log.info(
            "Starting Scenario 18: Revert from degraded stretch mode and enable with new datacenter"
        )
        log.info("=" * 80)
        log.info(self.scenario18.__doc__)
        log.info("=" * 80)

        # Step 1: Wait for clean PGs and verify stretch mode is enabled
        log.info("")
        log.info("=" * 80)
        log.info(
            "Step 1): Wait for clean PGs before starting scenario and verify stretch mode is enabled"
        )
        log.info("=" * 80)
        log.info("Checking if PGs are in active+clean state...")
        if wait_for_clean_pg_sets(rados_obj=self.rados_obj) is False:
            log.error("PGs did not reach active+clean before start of scenario")
            raise Exception("PG did not reach active+clean before start of scenario")
        log.info("All PGs are in active+clean state")

        log.info("Verifying stretch mode pre-checks...")
        if not stretch_enabled_checks(self.rados_obj):
            log.error(
                "The cluster has not cleared the pre-checks to run stretch tests. Exiting..."
            )
            raise Exception("Test pre-execution checks failed")
        log.info("Stretch mode pre-checks passed successfully")
        log.info("Step 1) completed successfully")
        log.info("")

        # Step 2: Create a pool and write IO
        log.info("=" * 80)
        log.info("Step 2): Create a pool and write IO")
        log.info("=" * 80)
        pool_name = "rados_scenario18_pool_" + generate_unique_id(3)
        log.info(f"Creating pool: {pool_name}")
        self.create_pool_and_write_io(pool_name, client_node=self.client_node)
        log.info(f"Pool {pool_name} created and IO written successfully")
        log.info("Waiting 10 seconds for IO to stabilize...")
        time.sleep(10)
        init_objects = self.rados_obj.get_cephdf_stats(pool_name=pool_name)["stats"][
            "objects"
        ]
        log.info(f"Initial object count in pool {pool_name}: {init_objects}")
        log.info("Step 2) completed successfully")
        log.info("")

        # Step 3: Simulate Datacenter DC2 failure
        log.info("=" * 80)
        log.info(
            "Step 3): Simulate Datacenter DC2 failure by shutting down all hosts in DC2"
        )
        log.info("=" * 80)
        dc2_hosts_to_remove = self.site_2_hosts
        log.info(f"Hosts in DC2 to be shut down: {dc2_hosts_to_remove}")
        log.info(f"Shutdown parameter: {shutdown}")

        if shutdown:
            log.info("Shutting down all hosts in DC2...")
            for host in dc2_hosts_to_remove:
                log.info(f"Shutting down host: {host}")
                target_node = find_vm_node_by_hostname(ceph_cluster, host)
                target_node.shutdown(wait=True)
                log.info(f"Host {host} has been shut down successfully")
            log.info(f"Completed shutdown of all hosts in DC2: {dc2_hosts_to_remove}")

            log.info("Waiting for stretch mode to reach degraded status...")
            self.wait_till_stretch_mode_status_reaches(status="degraded")
            log.info("Stretch mode has reached degraded status")
        else:
            log.info(
                "Skipping shutdown step - testing with healthy stretch mode cluster"
            )
        log.info("Step 3) completed successfully")
        log.info("")

        # Step 4: Revert stretch mode to regular cluster
        log.info("=" * 80)
        log.info("Step 4): Revert stretch mode to regular cluster")
        log.info("=" * 80)
        if crush_rule["id"] == 0:
            log.info("Reverting stretch mode to default crush rule (id=0)...")
            self.revert_stretch_mode()
        else:
            log.info(
                f"Reverting stretch mode to custom crush rule: {crush_rule['name']}"
            )
            self.revert_stretch_mode(crush_rule_name=crush_rule["name"])
        log.info("Stretch mode reverted successfully")
        log.info("Step 4) completed successfully")
        log.info("")

        # Step 5: Validate pool configurations are reset
        log.info("=" * 80)
        log.info("Step 5): Validate pool configurations are reset")
        log.info("=" * 80)
        self.expected_pool_properties["crush_rule"] = crush_rule["id"]
        log.info(f"Expected pool properties: {self.expected_pool_properties}")
        self.validate_pool_configurations_post_revert(self.expected_pool_properties)
        log.info("Pool configurations validated successfully")
        log.info("Step 5) completed successfully")
        log.info("")

        # Step 6: Validate OSD configurations are reset
        log.info("=" * 80)
        log.info("Step 6): Validate OSD configurations are reset")
        log.info("=" * 80)
        log.info(f"Expected OSD map values: {self.expected_osd_map_values}")
        self.validate_osd_configurations_post_revert(self.expected_osd_map_values)
        log.info("OSD configurations validated successfully")
        log.info("Step 6) completed successfully")
        log.info("")

        # Step 7: Validate MON configurations are reset
        log.info("=" * 80)
        log.info("Step 7): Validate MON configurations are reset")
        log.info("=" * 80)
        log.info(f"Expected MON map values: {self.expected_mon_map_values}")
        self.validate_mon_configurations_post_revert(self.expected_mon_map_values)
        log.info("MON configurations validated successfully")
        log.info("Step 7) completed successfully")
        log.info("")

        # Step 8: Remove failed hosts from the cluster
        log.info("=" * 80)
        log.info("Step 8): Remove failed hosts from the cluster")
        log.info("=" * 80)
        serviceability_methods = ServiceabilityMethods(cluster=ceph_cluster, **config)
        if shutdown:
            log.info("Waiting for hosts to reach Offline status (timeout: 600s)...")
            wait_till_host_status_reaches(
                rados_obj=self.rados_obj,
                status="Offline",
                hostnames=dc2_hosts_to_remove,
                duration=600,
            )
            log.info(f"All hosts {dc2_hosts_to_remove} are now Offline")
            log.info("Removing offline hosts from the cluster...")
            for host_name in dc2_hosts_to_remove:
                log.info(f"Removing offline host: {host_name}")
                serviceability_methods.remove_offline_host(
                    host_node_name=host_name, rm_crush_entry=True
                )
                log.info(f"Successfully removed offline host: {host_name}")
        else:
            log.info("Removing custom hosts from the cluster (shutdown=False)...")
            for host_name in dc2_hosts_to_remove:
                log.info(f"Removing custom host: {host_name}")
                serviceability_methods.remove_custom_host(
                    host_node_name=host_name, rm_crush_entry=True
                )
                log.info(f"Successfully removed custom host: {host_name}")
        log.info(f"All hosts {dc2_hosts_to_remove} have been removed from the cluster")
        log.info("Step 8) completed successfully")
        log.info("")

        # Step 9: Wait for PGs to reach active+clean after host removal
        log.info("=" * 80)
        log.info("Step 9): Wait for PGs to reach active+clean after host removal")
        log.info("=" * 80)
        log.info("Waiting for PGs to stabilize after host removal...")
        if wait_for_clean_pg_sets(rados_obj=self.rados_obj) is False:
            log.error("PGs did not reach active+clean post revert")
            raise Exception("PGs did not reach active+clean post revert")
        log.info("All PGs are in active+clean state")
        log.info("Step 9) completed successfully")
        log.info("")

        # Step 10: Add new bucket DC3 of type datacenter and move it under root
        log.info("=" * 80)
        log.info(
            "Step 10): Add new bucket DC3 of type datacenter and move it under root"
        )
        log.info("=" * 80)
        dc3_name = "DC3"
        log.info(f"Adding new bucket {dc3_name} of type datacenter...")
        cmd = f"ceph osd crush add-bucket {dc3_name} datacenter"
        if self.rados_obj.run_ceph_command(cmd=cmd) is None:
            log.error(f"Failed to add bucket {dc3_name}")
            raise Exception(f"Failed to add bucket {dc3_name}")
        log.info(f"Successfully added bucket {dc3_name}")

        log.info(f"Moving bucket {dc3_name} under root=default...")
        cmd = f"ceph osd crush move {dc3_name} root=default"
        if self.rados_obj.run_ceph_command(cmd=cmd) is None:
            log.error(f"Failed to move bucket {dc3_name} under root")
            raise Exception(f"Failed to move bucket {dc3_name} under root")
        log.info(f"Successfully moved bucket {dc3_name} under root")
        log.info("Step 10) completed successfully")
        log.info("")

        # Step 11: Restart the stopped hosts and remove the cluster from those nodes
        log.info("=" * 80)
        log.info(
            "Step 11): Restart the stopped hosts and remove the cluster from those nodes"
        )
        log.info("=" * 80)
        log.info("Retrieving cluster FSID...")
        fsid = self.rados_obj.run_ceph_command(cmd="ceph fsid", client_exec=True)[
            "fsid"
        ]
        log.info(f"Cluster FSID: {fsid}")

        for host in dc2_hosts_to_remove:
            if shutdown:
                log.info(f"Powering on host: {host}")
                target_node = find_vm_node_by_hostname(ceph_cluster, host)
                target_node.power_on()
                log.info(f"Host {host} restarted successfully")
                log.info("Waiting 60 seconds for host to stabilize...")
                time.sleep(60)

            log.info(f"Removing cluster from host: {host}")
            host_obj = get_host_obj_from_hostname(
                hostname=host, rados_obj=self.rados_obj
            )
            cmd = f"cephadm rm-cluster --force --zap-osds --fsid {fsid}"
            host_obj.exec_command(cmd=cmd, sudo=True)
            log.info(f"Cluster removed from host {host} successfully")
        log.info(
            f"Completed restart and cluster removal from all hosts: {dc2_hosts_to_remove}"
        )
        log.info("Step 11) completed successfully")
        log.info("")

        # Step 12: Add new hosts to the cluster and move them under DC3
        log.info("=" * 80)
        log.info("Step 12): Add new hosts to the cluster and move them under DC3")
        log.info("=" * 80)
        log.info("Setting OSD service to unmanaged mode...")

        for host in dc2_hosts_to_remove:
            self.rados_obj.set_service_managed_type(service_type="osd", unmanaged=True)
            log.info(f"Adding host {host} back to the cluster with OSD deployment...")
            serviceability_methods.add_new_hosts(
                add_nodes=[host], deploy_osd=True, osd_label="osd"
            )
            log.info(f"Successfully added host {host} to the cluster")
        log.info("Setting OSD service back to managed mode...")
        self.rados_obj.set_service_managed_type(service_type="osd", unmanaged=False)
        log.info("Waiting 60 seconds for OSDs to stabilize...")
        time.sleep(60)

        log.info(f"Moving hosts into DC3 bucket ({dc3_name}) in CRUSH map...")
        for host in self.site_2_hosts:
            log.info(f"Moving host {host} to {self.stretch_bucket}={dc3_name}...")
            cmd = f"ceph osd crush move {host} {self.stretch_bucket}={dc3_name}"
            if self.rados_obj.run_ceph_command(cmd=cmd) is None:
                log.error(f"Failed to move host {host} under bucket {dc3_name}")
                raise Exception(f"Failed to move host {host} under bucket {dc3_name}")
            log.info(f"Successfully moved host {host} under bucket {dc3_name}")

        log.info(f"Adding mon crush location for DC3 ({dc3_name})...")
        log.info(f"MON hosts to configure: {self.site_2_mon_hosts}")
        self.set_mon_location(
            location_name=dc3_name,
            hostnames=self.site_2_mon_hosts,
            location_type="datacenter",
        )
        log.info(f"Successfully configured mon crush location for {dc3_name}")
        log.info("Step 12) completed successfully")
        log.info("")

        # Step 13: Modify the site-affinity CRUSH rule to remove DC2 and include DC3
        log.info("=" * 80)
        log.info(
            "Step 13): Modify the site-affinity CRUSH rule to remove DC2 and include DC3"
        )
        log.info("=" * 80)
        log.info(f"Creating new CRUSH rule with {self.site_1_name} and {dc3_name}...")

        rules = f"""id {random.randint(10, 100)}
type replicated
min_size 1
max_size 10
step take {self.site_1_name}
step chooseleaf firstn 2 type host
step emit
step take {dc3_name}
step chooseleaf firstn 2 type host
step emit"""

        # Add the modified rule
        new_stretch_rule_name = "stretch_rule_" + str(random.randint(10, 100))
        log.info(f"New stretch rule name: {new_stretch_rule_name}")
        log.debug(f"CRUSH rule definition:\n{rules}")
        if not self.rados_obj.add_custom_crush_rules(
            rule_name=new_stretch_rule_name, rules=rules
        ):
            log.error("Failed to add modified stretch rule")
            raise Exception("Failed to add modified stretch rule")
        log.info(
            f"Successfully created modified "
            f"stretch rule {new_stretch_rule_name} using {self.site_1_name} and {dc3_name}"
        )
        log.info("Step 13) completed successfully")
        log.info("")

        # Step 14: Remove DC2 from the CRUSH map
        log.info("=" * 80)
        log.info("Step 14): Remove DC2 from the CRUSH map")
        log.info("=" * 80)
        log.info("Removing old stretch_rule from CRUSH map...")
        cmd = "ceph osd crush rule rm stretch_rule"
        if self.rados_obj.run_ceph_command(cmd=cmd) is None:
            log.error("Failed to remove crush rule stretch_rule")
            raise Exception("Failed to remove crush rule stretch_rule")
        log.info("Successfully removed old stretch_rule")

        log.info(f"Removing bucket {self.site_2_name} from CRUSH map...")
        cmd = f"ceph osd crush remove {self.site_2_name}"
        if self.rados_obj.run_ceph_command(cmd=cmd) is None:
            log.error(f"Failed to remove bucket {self.site_2_name}")
            raise Exception(f"Failed to remove bucket {self.site_2_name}")
        log.info(f"Successfully removed bucket {self.site_2_name} from CRUSH map")
        log.info("Step 14) completed successfully")
        log.info("")

        # Step 15: Enable stretch mode with DC1 and DC3
        log.info("=" * 80)
        log.info("Step 15): Enable stretch mode with DC1 and DC3")
        log.info("=" * 80)
        log.info(f"Enabling stretch mode with tiebreaker mon: {self.tiebreaker_mon}")
        log.info(f"Using stretch rule: {new_stretch_rule_name}")
        self.enable_stretch_mode(self.tiebreaker_mon, new_stretch_rule_name)
        log.info("Stretch mode enabled successfully")
        log.info("Step 15) completed successfully")
        log.info("")

        # Step 16: Wait for PGs to reach active+clean
        log.info("=" * 80)
        log.info("Step 16): Wait for PGs to reach active+clean")
        log.info("=" * 80)
        log.info(
            "Waiting for PGs to reach active+clean state after enabling stretch mode..."
        )
        if wait_for_clean_pg_sets(rados_obj=self.rados_obj) is False:
            log.error(
                "PGs did not reach active+clean post enabling stretch mode with DC1 and DC3"
            )
            raise Exception(
                "PGs did not reach active+clean post enabling stretch mode with DC2 and DC3"
            )
        log.info("All PGs are in active+clean state")
        log.info("Step 16) completed successfully")
        log.info("")

        # Step 17: Perform stretch mode checks
        log.info("=" * 80)
        log.info("Step 17): Perform stretch mode checks")
        log.info("=" * 80)
        log.info("Running stretch mode validation checks...")
        if not stretch_enabled_checks(self.rados_obj):
            log.error(
                "The cluster has not cleared the pre-checks after enabling stretch mode. Exiting..."
            )
            raise Exception("Stretch mode checks failed")
        log.info("All stretch mode checks passed successfully")
        log.info("Step 17) completed successfully")
        log.info("")

        # Step 18: Write IO to the cluster post stretch mode enable
        log.info("=" * 80)
        log.info("Step 18): Write IO to the cluster post stretch mode enable")
        log.info("=" * 80)
        log.info(f"Writing IO to pool {pool_name}...")
        log.info(f"Initial object count: {init_objects}")
        self.write_io_and_validate_objects(
            pool_name=pool_name,
            init_objects=init_objects,
            obj_name="post_stretch_enable",
        )
        log.info("IO written and validated successfully")
        log.info("Step 18) completed successfully")
        log.info("")

        log.info("=" * 80)
        log.info("Scenario 18 completed successfully!")
        log.info("=" * 80)
