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
"""

import time
from collections import namedtuple

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.monitor_workflows import MonitorWorkflows
from ceph.rados.pool_workflows import PoolFunctions
from ceph.rados.serviceability_workflows import ServiceabilityMethods
from ceph.utils import find_vm_node_by_hostname
from tests.rados.monitor_configurations import MonElectionStrategies
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from tests.rados.test_stretch_revert_class import (
    RevertStretchModeFunctionalities,
    flush_ip_table_rules_on_all_hosts,
    simulate_netsplit_between_hosts,
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
