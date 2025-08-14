"""
This test module is used to test Netsplit detection and warning feature
includes:
 Scenario 1 -> [stretch mode] Netsplit between mons of same datacenter
 Scenario 2 -> [stretch mode] Netsplit between 1 mon of DC1 and 1 mon of DC2
 Scenario 3 -> [stretch mode] Netsplit between mon1 of DC2 and mon2 of DC1. mon1 of DC2 and mon2 of DC1
 Scenario 4 -> [stretch mode] Netsplit between DC1 mon and tiebreaker
 Scenario 5 -> [Regular cluster with mon crush location] Netsplit between mons of same CRUSH location
 Scenario 6 -> [Regular cluster with mon crush location] Netsplit between 2 mons across CRUSH location
 Scenario 7 -> [Regular cluster with mon crush location] Netsplit between 1 mon of crush location 1
                and 2 mons of crush location 2
 Scenario 8 -> [Regular cluster with mon crush location] Netsplit between all mons of DC1 and all mons of DC2
"""

import random
from collections import namedtuple

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from ceph.rados.serviceability_workflows import ServiceabilityMethods
from cli.utilities.operations import wait_for_cluster_health
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from tests.rados.test_stretch_revert_class import (
    RevertStretchModeFunctionalities,
    StretchMode,
    flush_ip_table_rules_on_all_hosts,
    simulate_netsplit_between_hosts,
)
from tests.rados.test_stretch_site_down import stretch_enabled_checks
from utility.log import Log
from utility.retry import retry

log = Log(__name__)

Hosts = namedtuple("Hosts", ["dc_1_hosts", "dc_2_hosts", "tiebreaker_hosts"])


def run(ceph_cluster, **kw):
    """
    This test module is used to test Netsplit detection and warning feature
    includes:
     Scenario 1 -> [stretch mode] Netsplit in  between mons of same datacenter
     Scenario 2 -> [stretch mode] Netsplit between 1 mon of DC1 and 1 mon of DC2
     Scenario 3 -> [stretch mode] Netsplit between mon1 of DC2 and mon2 of DC1. mon1 of DC2 and mon2 of DC1
     Scenario 4 -> [stretch mode] Netsplit between DC1 mon and tiebreaker
     Scenario 5 -> [Regular cluster with mon crush location] Netsplit between mons of same CRUSH location
     Scenario 6 -> [Regular cluster with mon crush location] Netsplit between 2 mons across CRUSH location
     Scenario 7 -> [Regular cluster with mon crush location] Netsplit between 1 mon of crush location 1
                    and 2 mons of crush location 2
     Scenario 8 -> [Regular cluster with mon crush location] Netsplit between all mons of DC1 and all mons of DC2
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
            "scenario3",
            "scenario4",
            "scenario5",
            "scenario6",
            "scenario7",
            "scenario8",
        ],
    )
    config = {
        "rados_obj": rados_obj,
        "pool_obj": pool_obj,
        "tiebreaker_mon_site_name": tiebreaker_mon_site_name,
        "stretch_bucket": stretch_bucket,
        "client_node": client_node,
    }
    try:

        stretch_mode = StretchMode(**config)
        dc_1_mon_hosts = list()
        for host in stretch_mode.site_1_hosts:
            if "mon" in rados_obj.get_host_label(host_name=host):
                dc_1_mon_hosts.append(host)
        dc_2_mon_hosts = list()
        for host in stretch_mode.site_2_hosts:
            if "mon" in rados_obj.get_host_label(host_name=host):
                dc_2_mon_hosts.append(host)
        tiebreaker_hosts = stretch_mode.tiebreaker_hosts

        if "scenario1" in scenarios_to_run:
            log.info(
                "Scenario 1 -> [Stretch mode] Netsplit between mons of same datacenter"
            )
            group_1_hosts = list()
            group_2_hosts = list()

            if not stretch_enabled_checks(rados_obj):
                log.error(
                    "The cluster has not cleared the pre-checks to run stretch tests. Exiting...\n"
                )
                raise Exception("Test pre-execution checks failed")

            random_host = random.choice(dc_1_mon_hosts)
            group_1_hosts.append(random_host)

            # selecting hosts which are not in group1
            random_host = random.choice(list(set(dc_1_mon_hosts) - set(group_1_hosts)))
            group_2_hosts.append(random_host)

            log_debug = (
                f"\nSelected mon hosts are"
                f"\nDC1 -> {group_1_hosts}"
                f"\nDC1 -> {group_2_hosts}"
            )
            log.debug(log_debug)

            log_info_msg = f"Step 1: Simulating netsplit between {group_1_hosts} <-> {group_2_hosts}"
            log.info(log_info_msg)
            simulate_netsplit_between_hosts(rados_obj, group_1_hosts, group_2_hosts)

            log_info_msg = f"Step 2: Validating netsplit warning between {group_1_hosts} <-> {group_2_hosts}"
            log.info(log_info_msg)
            check_individual_netsplit_warning(rados_obj, group_1_hosts, group_2_hosts)

            log_info_msg = f"Step 3: Flushing IP tables rules of {group_1_hosts} and {group_2_hosts}"
            log.info(log_info_msg)
            flush_ip_table_rules_on_all_hosts(rados_obj, group_1_hosts)
            flush_ip_table_rules_on_all_hosts(rados_obj, group_2_hosts)

            log.info("Step 4: Wait for clean PGs")
            wait_for_clean_pg_sets(rados_obj=rados_obj)

            log.info("Step 5: Wait for cluster HEALTH_OK")
            wait_for_cluster_health(client_node, "HEALTH_OK")

        if "scenario2" in scenarios_to_run:
            log.info(
                "Scenario 2 -> [stretch mode] Netsplit between 1 mon of DC1 and 1 mon of DC2"
            )
            group_1_hosts = list()
            group_2_hosts = list()

            if not stretch_enabled_checks(rados_obj):
                log.error(
                    "The cluster has not cleared the pre-checks to run stretch tests. Exiting..."
                )
                raise Exception("Test pre-execution checks failed")

            random_host = random.choice(dc_1_mon_hosts)
            group_1_hosts.append(random_host)

            # selecting hosts which are not in group1
            random_host = random.choice(dc_2_mon_hosts)
            group_2_hosts.append(random_host)

            log_debug = (
                f"\nSelected mon hosts are"
                f"\nDC1 -> {group_1_hosts}"
                f"\nDC2 -> {group_2_hosts}"
            )
            log.debug(log_debug)

            log_info_msg = f"Step 1: Simulating netsplit between {group_1_hosts} <-> {group_2_hosts}"
            log.info(log_info_msg)
            simulate_netsplit_between_hosts(rados_obj, group_1_hosts, group_2_hosts)

            log_info_msg = f"Step 2: Validating netsplit warning between {group_1_hosts} <-> {group_2_hosts}"
            log.info(log_info_msg)
            check_individual_netsplit_warning(rados_obj, group_1_hosts, group_2_hosts)

            log_info_msg = f"Step 3: Flushing IP tables rules of {group_1_hosts} and {group_2_hosts}"
            log.info(log_info_msg)
            flush_ip_table_rules_on_all_hosts(rados_obj, group_1_hosts)
            flush_ip_table_rules_on_all_hosts(rados_obj, group_2_hosts)

            log.info("Step 4: Wait for clean PGs")
            wait_for_clean_pg_sets(rados_obj=rados_obj)

            log.info("Step 5: Wait for cluster HEALTH_OK")
            wait_for_cluster_health(client_node, "HEALTH_OK")

        if "scenario3" in scenarios_to_run:
            log.info(
                "Scenario 3 -> Netsplit in stretch mode between mon1 of DC2 and mon2 of DC1. \n"
                "mon1 of DC2 and mon2 of DC1"
            )
            group_1_hosts = list()
            group_2_hosts = list()

            if not stretch_enabled_checks(rados_obj):
                log.error(
                    "The cluster has not cleared the pre-checks to run stretch tests. Exiting..."
                )
                raise Exception("Test pre-execution checks failed")

            random_host = random.choice(dc_1_mon_hosts)
            group_1_hosts.append(random_host)

            # selecting more than 1 host from DC2
            for _ in range(2):
                random_host = random.choice(
                    list(set(dc_2_mon_hosts) - set(group_2_hosts))
                )
                group_2_hosts.append(random_host)

            log_debug = (
                f"\nSelected mon hosts are"
                f"\nDC1 -> {group_1_hosts}"
                f"\nDC2 -> {group_2_hosts}"
            )
            log.debug(log_debug)

            log_info_msg = f"Step 1: Simulating netsplit between {group_1_hosts} <-> {group_2_hosts}"
            log.info(log_info_msg)
            simulate_netsplit_between_hosts(rados_obj, group_1_hosts, group_2_hosts)

            log_info_msg = f"Step 2: Validating netsplit warning between {group_1_hosts} <-> {group_2_hosts}"
            log.info(log_info_msg)
            check_individual_netsplit_warning(rados_obj, group_1_hosts, group_2_hosts)

            log_info_msg = f"Step 3: Flushing IP tables rules of {group_1_hosts} and {group_2_hosts}"
            log.info(log_info_msg)
            flush_ip_table_rules_on_all_hosts(rados_obj, group_1_hosts)
            flush_ip_table_rules_on_all_hosts(rados_obj, group_2_hosts)

            log.info("Step 4: Wait for clean PGs")
            wait_for_clean_pg_sets(rados_obj=rados_obj)

            log.info("Step 5: Wait for cluster HEALTH_OK")
            wait_for_cluster_health(client_node, "HEALTH_OK")

        if "scenario4" in scenarios_to_run:
            log.info("Scenario 4 -> Netsplit between DC1 mon and tiebreaker")
            group_1_hosts = list()
            group_2_hosts = list()

            if not stretch_enabled_checks(rados_obj):
                log.error(
                    "The cluster has not cleared the pre-checks to run stretch tests. Exiting..."
                )
                raise Exception("Test pre-execution checks failed")

            random_host = random.choice(dc_1_mon_hosts)
            group_1_hosts.append(random_host)

            random_host = random.choice(tiebreaker_hosts)
            group_2_hosts.append(random_host)

            log_debug = (
                f"\nSelected mon hosts are"
                f"\nDC1 -> {group_1_hosts}"
                f"\nDC2 -> {group_2_hosts}"
            )
            log.debug(log_debug)

            log_info_msg = f"Step 1: Simulating netsplit between {group_1_hosts} <-> {group_2_hosts}"
            log.info(log_info_msg)
            simulate_netsplit_between_hosts(rados_obj, group_1_hosts, group_2_hosts)

            log_info_msg = f"Step 2: Validating netsplit warning between {group_1_hosts} <-> {group_2_hosts}"
            log.info(log_info_msg)
            check_individual_netsplit_warning(rados_obj, group_1_hosts, group_2_hosts)

            log_info_msg = f"Step 3: Flushing IP tables rules of {group_1_hosts} and {group_2_hosts}"
            log.info(log_info_msg)
            flush_ip_table_rules_on_all_hosts(rados_obj, group_1_hosts)
            flush_ip_table_rules_on_all_hosts(rados_obj, group_2_hosts)

            log.info("Step 4: Wait for clean PGs")
            wait_for_clean_pg_sets(rados_obj=rados_obj)

            log.info("Step 5: Wait for cluster HEALTH_OK")
            wait_for_cluster_health(client_node, "HEALTH_OK")

        revert_stretch_mode = RevertStretchModeFunctionalities(**config)
        revert_stretch_mode.revert_stretch_mode()

        # Regular Ceph cluster - Netsplit detection and warning
        # Hence stretch mode should be disabled
        if "scenario5" in scenarios_to_run:
            log.info(
                "Scenario 5 -> [Regular cluster with crush location] Netsplit between mons of same CRUSH location"
            )
            group_1_hosts = list()
            group_2_hosts = list()

            if stretch_enabled_checks(rados_obj):
                log.error("stretch mode is enabled on cluster. Exiting...")
                raise Exception("Stretch mode is enabled on cluster")

            random_host = random.choice(dc_1_mon_hosts)
            group_1_hosts.append(random_host)

            # selecting hosts which are not in group1
            random_host = random.choice(list(set(dc_1_mon_hosts) - set(group_1_hosts)))
            group_2_hosts.append(random_host)

            log_debug = (
                f"\nSelected mon hosts are"
                f"\nDC1 -> {group_1_hosts}"
                f"\nDC2 -> {group_2_hosts}"
            )
            log.debug(log_debug)

            log_info_msg = f"Step 1: Simulating netsplit between {group_1_hosts} <-> {group_2_hosts}"
            log.info(log_info_msg)
            simulate_netsplit_between_hosts(rados_obj, group_1_hosts, group_2_hosts)

            log_info_msg = f"Step 2: Validating netsplit warning between {group_1_hosts} <-> {group_2_hosts}"
            log.info(log_info_msg)
            check_individual_netsplit_warning(rados_obj, group_1_hosts, group_2_hosts)

            log_info_msg = f"Step 3: Flushing IP tables rules of {group_1_hosts} and {group_2_hosts}"
            log.info(log_info_msg)
            flush_ip_table_rules_on_all_hosts(rados_obj, group_1_hosts)
            flush_ip_table_rules_on_all_hosts(rados_obj, group_2_hosts)

            log.info("Step 4: Wait for clean PGs")
            wait_for_clean_pg_sets(rados_obj=rados_obj)

            log.info("Step 5: Wait for cluster HEALTH_OK")
            wait_for_cluster_health(client_node, "HEALTH_OK")

        # Regular Ceph cluster - Netsplit detection and warning
        # Hence stretch mode should be disabled
        if "scenario6" in scenarios_to_run:
            log.info(
                "Scenario 6 -> [Regular cluster with mon crush location] Netsplit between 2 mons across CRUSH location"
            )
            group_1_hosts = list()
            group_2_hosts = list()

            if stretch_enabled_checks(rados_obj):
                log.error("stretch mode is enabled on cluster. Exiting...")
                raise Exception("Stretch mode is enabled on cluster")

            random_host = random.choice(dc_1_mon_hosts)
            group_1_hosts.append(random_host)

            random_host = random.choice(dc_2_mon_hosts)
            group_2_hosts.append(random_host)

            log_debug = (
                f"\nSelected mon hosts are"
                f"\nDC1 -> {group_1_hosts}"
                f"\nDC2 -> {group_2_hosts}"
            )
            log.debug(log_debug)

            log_info_msg = f"Step 1: Simulating netsplit between {group_1_hosts} <-> {group_2_hosts}"
            log.info(log_info_msg)
            simulate_netsplit_between_hosts(rados_obj, group_1_hosts, group_2_hosts)

            log_info_msg = f"Step 2: Validating netsplit warning between {group_1_hosts} <-> {group_2_hosts}"
            log.info(log_info_msg)
            check_individual_netsplit_warning(rados_obj, group_1_hosts, group_2_hosts)

            log_info_msg = f"Step 3: Flushing IP tables rules of {group_1_hosts} and {group_2_hosts}"
            log.info(log_info_msg)
            flush_ip_table_rules_on_all_hosts(rados_obj, group_1_hosts)
            flush_ip_table_rules_on_all_hosts(rados_obj, group_2_hosts)

            log_info_msg = "Step 4: Wait for clean PGs"
            log.info(log_info_msg)
            wait_for_clean_pg_sets(rados_obj=rados_obj)

            log_info_msg = "Step 5: Wait for cluster HEALTH_OK"
            log.info(log_info_msg)
            wait_for_cluster_health(client_node, "HEALTH_OK")

        # Regular Ceph cluster - Netsplit detection and warning
        # Hence stretch mode should be disabled
        if "scenario7" in scenarios_to_run:
            log.info(
                "Scenario 7 -> [Regular cluster with mon crush location] Netsplit between 1 mon of crush "
                "location 1 and 2 mons of crush location 2"
            )
            group_1_hosts = list()
            group_2_hosts = list()

            # Stretch mode should be disabled
            if stretch_enabled_checks(rados_obj):
                log.error("stretch mode is enabled on cluster. Exiting...")
                raise Exception("Stretch mode is enabled on cluster")

            random_host = random.choice(dc_1_mon_hosts)
            group_1_hosts.append(random_host)

            # selecting more than 1 host from DC2
            for _ in range(2):
                # ensuring same mon is not being selected when picking at random
                random_host = random.choice(
                    list(set(dc_2_mon_hosts) - set(group_2_hosts))
                )
                group_2_hosts.append(random_host)

            log_debug = (
                f"\nSelected mon hosts are"
                f"\nDC1 -> {group_1_hosts}"
                f"\nDC2 -> {group_2_hosts}"
            )
            log.debug(log_debug)

            log_info_msg = f"Step 1: Simulating netsplit between {group_1_hosts} <-> {group_2_hosts}"
            log.info(log_info_msg)
            simulate_netsplit_between_hosts(rados_obj, group_1_hosts, group_2_hosts)

            log_info_msg = f"Step 2: Validating netsplit warning between {group_1_hosts} <-> {group_2_hosts}"
            log.info(log_info_msg)
            check_individual_netsplit_warning(rados_obj, group_1_hosts, group_2_hosts)

            log_info_msg = f"Step 3: Flushing IP tables rules of {group_1_hosts} and {group_2_hosts}"
            log.info(log_info_msg)
            flush_ip_table_rules_on_all_hosts(rados_obj, group_2_hosts)
            flush_ip_table_rules_on_all_hosts(rados_obj, group_1_hosts)

            log.info("Step 4: Wait for clean PGs")
            wait_for_clean_pg_sets(rados_obj=rados_obj)

            log.info("Step 5: Wait for cluster HEALTH_OK")
            wait_for_cluster_health(client_node, "HEALTH_OK")

        # Regular Ceph cluster - Netsplit detection and warning
        # Hence stretch mode should be disabled
        if "scenario8" in scenarios_to_run:
            log.info(
                "Scenario 8 -> [Regular cluster with mon crush location] Netsplit between all mons of DC1 and"
                " all mons of DC2"
            )
            group_1_hosts = dc_1_mon_hosts
            group_2_hosts = dc_2_mon_hosts

            # Stretch mode should be disabled
            if stretch_enabled_checks(rados_obj):
                log.error("stretch mode is enabled on cluster. Exiting...")
                raise Exception("Stretch mode is enabled on cluster")

            log_debug = (
                f"\nSelected mon hosts are"
                f"\nDC1 -> {group_1_hosts}"
                f"\nDC2 -> {group_2_hosts}"
            )
            log.debug(log_debug)

            log_info_msg = f"Step 1: Simulating netsplit between {group_1_hosts} <-> {group_2_hosts}"
            log.info(log_info_msg)
            simulate_netsplit_between_hosts(rados_obj, group_1_hosts, group_2_hosts)

            log_info_msg = f"Step 2: Validating netsplit warning between {group_1_hosts} <-> {group_2_hosts}"
            log.info(log_info_msg)
            check_location_netsplit_warning(
                rados_obj, stretch_mode.site_1_name, stretch_mode.site_2_name
            )

            log_info_msg = f"Step 3: Flushing IP tables rules of {group_1_hosts} and {group_2_hosts}"
            log.info(log_info_msg)
            flush_ip_table_rules_on_all_hosts(rados_obj, group_1_hosts)
            flush_ip_table_rules_on_all_hosts(rados_obj, group_2_hosts)

            log.info("Step 4: Wait for clean PGs")
            wait_for_clean_pg_sets(rados_obj=rados_obj)

            log.info("Step 5: Wait for cluster HEALTH_OK")
            wait_for_cluster_health(client_node, "HEALTH_OK")

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
            "stretch_mode" in locals() or "stretch_mode" in globals()
        ) and not stretch_enabled_checks(rados_obj):
            stretch_mode.enable_stretch_mode(tiebreaker_mon=stretch_mode.tiebreaker_mon)
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


@retry((Exception), backoff=1, tries=3, delay=20)
def check_individual_netsplit_warning(
    rados_obj: RadosOrchestrator,
    group_1: list,
    group_2: list,
    expected_warning_count=None,
):
    """
    Method to validate individual level netsplit warning between 2 monitors.
    args:
        rados_obj:  RadosOrchestrator object
        group_1: List. example:- ["depressa003","depressa004"]
        group_2: List. example:- ["depressa005","depressa006"]
        expected_warning_count(optional, default=len(group_1)*len(group_2), type=int):
                          Expected number of MON_NETSPLIT warnings to be generated.
    returns:
        None. raises Exception if MON_NETSPLIT warning is not generated
    """
    # $ ceph health detail
    # {
    #     "status": "HEALTH_WARN",
    #     "checks": {
    #         "MON_NETSPLIT": {
    #             "severity": "HEALTH_WARN",
    #             "summary": {
    #                 "message": "2 network partitions detected",
    #                 "count": 2
    #             },
    #             "detail": [
    #                 {
    #                     "message": "Netsplit detected between mon.depressa003 and mon.depressa006"
    #                 },
    #                 {
    #                     "message": "Netsplit detected between mon.depressa003 and mon.depressa007"
    #                 }
    #             ],
    #             "muted": false
    #         },
    #     ................<redacted>................
    # }
    out = rados_obj.run_ceph_command(cmd="ceph health detail", print_output=True)
    checks = out["checks"]
    if "MON_NETSPLIT" not in checks:
        raise Exception("MON_NETSPLIT warning is not generated")

    count = checks["MON_NETSPLIT"]["summary"]["count"]
    if expected_warning_count is None:
        expected_warning_count = len(group_1) * len(group_2)

    if count != expected_warning_count:
        err_msg = (
            f"Expected partitions -> {expected_warning_count}\n"
            f"Current partitions as per ceph health detail -> {count}"
            f"ceph health detail -> {out}"
        )
        log.error(err_msg)
        raise Exception(err_msg)

    detail = checks["MON_NETSPLIT"]["detail"]
    for group_1_mon in group_1:
        for group_2_mon in group_2:
            for message_info in detail:
                if (
                    group_1_mon in message_info["message"]
                    and group_2_mon in message_info["message"]
                ):
                    info_msg = (
                        f"Netsplit warning generated for {group_1_mon} and {group_2_mon}\n"
                        f"Message -> {message_info['message']}"
                    )
                    log.info(info_msg)
                    break
            else:
                err_msg = (
                    f"Netsplit warning not generated for {group_1_mon} and {group_2_mon}\n"
                    f"Ceph health detail -> {out}"
                )
                log.error(err_msg)
                raise Exception(err_msg)
    log_info = (
        f"Netsplit detected and warning generated between below set of mons\n"
        f"Group 1 mons -> {group_1}\n"
        f"Group 2 mons -> {group_2}"
    )
    log.info(log_info)


@retry((Exception), backoff=1, tries=3, delay=20)
def check_location_netsplit_warning(
    rados_obj: RadosOrchestrator,
    site_1_name: str,
    site_2_name: str,
    expected_warning_count=None,
):
    """
    Method to validate location level netsplit warning such as datacenter, rack, row etc
    args:
        rados_obj:  RadosOrchestrator object
        site_1_name: string. example:- DC1, DC2
        site_2_name: string. example:- DC1, DC2
        expected_warning_count(optional, default=1): int. Expected number of MON_NETSPLIT warnings to be generated.
    returns:
        None. raises Exception if MON_NETSPLIT warning is not generated
    """

    # $ ceph health detail
    # {
    # "status": "HEALTH_WARN",
    # "checks": {
    #     ................<redacted>................
    #     "MON_NETSPLIT": {
    #         "severity": "HEALTH_WARN",
    #         "summary": {
    #             "message": "1 network partition detected",
    #             "count": 1
    #         },
    #         "detail": [
    #             {
    #                 "message": "Netsplit detected between DC1 and DC2"
    #             }
    #         ],
    #         "muted": false
    #     },
    #      ................<redacted>................
    # }

    out = rados_obj.run_ceph_command(cmd="ceph health detail", print_output=True)
    checks = out["checks"]
    if "MON_NETSPLIT" not in checks:
        raise Exception("MON_NETSPLIT warning is not generated")

    count = checks["MON_NETSPLIT"]["summary"]["count"]
    if expected_warning_count is None:
        expected_warning_count = 1

    if count != expected_warning_count:
        err_msg = (
            f"Expected partitions -> {expected_warning_count}\n"
            f"Current partitions as per ceph health detail -> {count}\n"
            f"ceph health detail -> {out}"
        )
        log.error(err_msg)
        raise Exception(err_msg)

    detail = checks["MON_NETSPLIT"]["detail"][0]
    if site_1_name not in detail or site_2_name not in detail:
        err_msg = (
            f"Netsplit detected and warning generated between\n"
            f"site 1 name -> {site_1_name}\n"
            f"site 2 name -> {site_2_name}\n"
            f"ceph health detail -> {out}"
        )
        log.error(err_msg)
        raise Exception(err_msg)
    log.info("MON_NETSPLIT warning generated successfully")
