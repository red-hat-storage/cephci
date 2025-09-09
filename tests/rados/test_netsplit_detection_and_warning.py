"""
This test module is used to test Netsplit detection and warning feature
includes:
 Scenario 1 -> Netsplit between mons of same datacenter
 Scenario 2 -> Netsplit between 1 mon of DC1 and 1 mon of DC2
 Scenario 3 -> Netsplit between mon1 of DC2 and mon2 of DC1. mon1 of DC2 and mon2 of DC1
 Scenario 4 -> Netsplit between DC1 mon and tiebreaker
 Scenario 5 -> Netsplit between all mons of DC1 and all mons of DC2
 Scenario 6 -> Netsplit between NAZ
"""

import random
from collections import namedtuple

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from ceph.rados.serviceability_workflows import ServiceabilityMethods
from cli.utilities.operations import wait_for_cluster_health
from tests.rados.monitor_configurations import MonElectionStrategies
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
         Scenario 1 -> Netsplit between mons of same datacenter
         Scenario 2 -> Netsplit between 1 mon of DC1 and 1 mon of DC2
         Scenario 3 -> Netsplit between mon1 of DC2 and mon2 of DC1. mon1 of DC2 and mon2 of DC1
         Scenario 4 -> Netsplit between DC1 mon and tiebreaker
         Scenario 5 -> Netsplit between all mons of DC1 and all mons of DC2
         Scenario 6 -> Netsplit between NAZ
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
    mon_election_obj = MonElectionStrategies(rados_obj=rados_obj)
    regular_cluster_with_mon_location = config.get(
        "regular_cluster_with_mon_location", False
    )
    separator = "-" * 40
    scenarios_to_run = config.get("scenarios_to_run", [])
    config = {
        "rados_obj": rados_obj,
        "pool_obj": pool_obj,
        "tiebreaker_mon_site_name": tiebreaker_mon_site_name,
        "stretch_bucket": stretch_bucket,
        "client_node": client_node,
    }
    try:

        STRETCH_MODE = False
        REGULAR_CLUSTER_WITH_MON_LOCATION = False
        NAZ_STRETCH_CLUSTER = False
        REGULAR_CLUSTER_WITHOUT_MON_LOCATION = False
        logging_code = None

        osd_tree_cmd = "ceph osd tree"
        buckets = rados_obj.run_ceph_command(osd_tree_cmd)
        dc_names = list()
        if buckets is not None:
            dc_buckets = [
                d for d in buckets["nodes"] if d.get("type") == stretch_bucket
            ]
            dc_names = [name["name"] for name in dc_buckets]

        if regular_cluster_with_mon_location is False and stretch_enabled_checks(
            rados_obj
        ):
            STRETCH_MODE = True
            logging_code = "Stretch Mode"

        elif len(dc_names) > 2:
            NAZ_STRETCH_CLUSTER = True
            logging_code = "N-AZ"

        elif regular_cluster_with_mon_location is True:
            REGULAR_CLUSTER_WITH_MON_LOCATION = True
            logging_code = "Regular cluster with mon crush location"

        else:
            REGULAR_CLUSTER_WITHOUT_MON_LOCATION = True
            logging_code = "Regular cluster without mon crush location"

        if not STRETCH_MODE:
            if mon_election_obj.set_election_strategy(mode="connectivity") is False:
                log.error("Unable to set mon election strategy")
                raise Exception("Unable to enable stretch pool")
            log.info(
                "Successfully set mon election strategy to connectivity for enabling stretch pools"
            )

        stretch_site = list()
        if STRETCH_MODE or REGULAR_CLUSTER_WITH_MON_LOCATION or NAZ_STRETCH_CLUSTER:
            osd_tree_cmd = "ceph osd tree"
            buckets = rados_obj.run_ceph_command(osd_tree_cmd)
            dc_buckets = [
                d for d in buckets["nodes"] if d.get("type") == stretch_bucket
            ]
            dc_names = [name["name"] for name in dc_buckets]
            all_hosts = rados_obj.get_multi_az_stretch_site_hosts(
                num_data_sites=len(dc_names), stretch_bucket=stretch_bucket
            )

            stretch_site = [list() for _ in range(len(dc_names))]
            for i, site in enumerate(dc_names):
                for host in getattr(all_hosts, site):
                    if "mon" in rados_obj.get_host_label(host_name=host):
                        stretch_site[i].append(host)
                log.debug(
                    f"Hosts present in Datacenter : {site} : {getattr(all_hosts, site)}"
                    f"MON Hosts present in Datacenter : {site} : {stretch_site[i]}"
                )
            stretch_mode = StretchMode(**config)
            tiebreaker_hosts = stretch_mode.tiebreaker_hosts

        if REGULAR_CLUSTER_WITHOUT_MON_LOCATION:
            node = ceph_cluster.get_nodes(role="mon")
            stretch_site = [list() for _ in range(2)]
            stretch_site[0] = [host_obj.hostname for host_obj in node[: len(node) // 2]]
            stretch_site[1] = [host_obj.hostname for host_obj in node[len(node) // 2 :]]

        if REGULAR_CLUSTER_WITH_MON_LOCATION:
            # If excuted along with stretch mode suite, then revert back to regular cluster
            # CRUSH locations for mon would still be present for netsplit detection & warning test cases
            if stretch_enabled_checks(rados_obj):
                revert_stretch_mode = RevertStretchModeFunctionalities(**config)
                revert_stretch_mode.revert_stretch_mode()

        log.info(f"[{logging_code}] stretch site configurations are {stretch_site}")

        if (
            "scenario1" in scenarios_to_run
            or STRETCH_MODE
            or REGULAR_CLUSTER_WITH_MON_LOCATION
            or NAZ_STRETCH_CLUSTER
        ):
            info_msg = (
                f"\n{separator}"
                f"\nScenario 1 -> [{logging_code}] Netsplit between mons of same datacenter"
                f"\n{separator}"
            )
            log.info(info_msg)
            group_1_hosts = list()
            group_2_hosts = list()

            # stretch_site = [['depressa003', 'depressa004'], ['depressa005', 'depressa006']]
            random_host = random.choice(stretch_site[0])
            group_1_hosts.append(random_host)

            # selecting hosts which are not in group1
            random_host = random.choice(list(set(stretch_site[0]) - set(group_1_hosts)))
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

        if (
            "scenario2" in scenarios_to_run
            or STRETCH_MODE
            or REGULAR_CLUSTER_WITH_MON_LOCATION
            or NAZ_STRETCH_CLUSTER
            or REGULAR_CLUSTER_WITHOUT_MON_LOCATION
        ):
            info_msg = (
                f"\n{separator}"
                f"\nScenario 2 -> [{logging_code}] Netsplit between 1 mon of DC1 and 1 mon of DC2"
                f"\n{separator}"
            )
            log.info(info_msg)
            group_1_hosts = list()
            group_2_hosts = list()

            # stretch_site = [['depressa003', 'depressa004'], ['depressa005', 'depressa006']]
            random_host = random.choice(stretch_site[0])
            group_1_hosts.append(random_host)

            # selecting hosts which are not in group1
            # stretch_site = [['depressa003', 'depressa004'], ['depressa005', 'depressa006']]
            random_host = random.choice(stretch_site[1])
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

        if (
            "scenario3" in scenarios_to_run
            or STRETCH_MODE
            or REGULAR_CLUSTER_WITH_MON_LOCATION
            or NAZ_STRETCH_CLUSTER
        ):
            info_msg = (
                f"\n{separator}"
                f"\nScenario 3 -> [{logging_code}] Netsplit between monA of DC2 and monB of DC1. "
                "monC of DC2 and monB of DC1"
                f"\n{separator}"
            )
            log.info(info_msg)
            group_1_hosts = list()
            group_2_hosts = list()

            # stretch_site = [['depressa003', 'depressa004'], ['depressa005', 'depressa006']]
            random_host = random.choice(stretch_site[0])
            group_1_hosts.append(random_host)

            # selecting 2 host from DC2
            # stretch_site = [['depressa003', 'depressa004'], ['depressa005', 'depressa006']]
            for _ in range(2):
                # Using set to remove already selected hosts in group_2_hosts
                random_host = random.choice(
                    list(set(stretch_site[1]) - set(group_2_hosts))
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

        if "scenario4" in scenarios_to_run or STRETCH_MODE:
            info_msg = (
                f"\n{separator}"
                f"\nScenario 4 -> [{logging_code}] Netsplit between DC1 mon and tiebreaker"
                f"\n{separator}"
            )
            log.info(info_msg)

            group_1_hosts = list()
            group_2_hosts = list()

            # stretch_site = [['depressa003', 'depressa004'], ['depressa005', 'depressa006']]
            random_host = random.choice(stretch_site[0])
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

        if (
            "scenario5" in scenarios_to_run
            or REGULAR_CLUSTER_WITH_MON_LOCATION
            or NAZ_STRETCH_CLUSTER
        ):
            info_msg = (
                f"\n{separator}"
                f"\nScenario 5 -> [{logging_code}] Netsplit between all mons of DC1 and all mons of DC2"
                f"\n{separator}"
            )
            log.info(info_msg)

            group_1_hosts = stretch_site[0]
            group_2_hosts = stretch_site[1]

            log_debug = (
                f"\nSelected mon hosts are"
                f"\nDC1 -> {group_1_hosts}"
                f"\nDC2 -> {group_2_hosts}"
            )
            log.debug(log_debug)

            log_info_msg = f"Step 1: Simulating netsplit between {group_1_hosts} <-> {group_2_hosts}"
            log.info(log_info_msg)
            simulate_netsplit_between_hosts(rados_obj, group_1_hosts, group_2_hosts)
            simulate_netsplit_between_hosts(rados_obj, group_2_hosts, group_1_hosts)

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

        if "scenario6" in scenarios_to_run or NAZ_STRETCH_CLUSTER:
            info_msg = (
                f"\n{separator}"
                f"\nScenario 6 -> [{logging_code}] Netsplit between "
                f"\n mon A of Site 1 <-> mon B of Site 2"
                f"\b mon A of Site 1 <-> mon B of Site 3"
                f"\n{separator}"
            )
            log.info(info_msg)

            selected_hosts = [None] * len(dc_names)

            for i, site_hosts in enumerate(stretch_site):
                random_host = random.choice(site_hosts)
                selected_hosts[i] = random_host
                log_debug = (
                    f"\nSelected mon hosts for Site {i} are" f"\n{selected_hosts[i]}"
                )
                log.debug(log_debug)

            for i in range(0, len(selected_hosts) - 1):
                log_info_msg = f"Step 1: Simulating netsplit between {selected_hosts[i]} <-> {selected_hosts[i + 1]}"
                log.info(log_info_msg)
                simulate_netsplit_between_hosts(
                    rados_obj, [selected_hosts[i]], [selected_hosts[i + 1]]
                )

            for i in range(0, len(selected_hosts) - 1):
                log_info_msg = (
                    f"Step 2: Validating netsplit warning between {selected_hosts[i]} "
                    f"<-> {selected_hosts[i + 1]}"
                )
                log.info(log_info_msg)
                check_individual_netsplit_warning(
                    rados_obj,
                    [selected_hosts[i]],
                    [selected_hosts[i + 1]],
                    expected_warning_count=2,
                )

            for i in range(0, len(selected_hosts) - 1):
                log_info_msg = f"Step 3: Flushing IP tables rules of {selected_hosts[i]} ,{selected_hosts[i + 1]}"
                log.info(log_info_msg)
                flush_ip_table_rules_on_all_hosts(
                    rados_obj, [selected_hosts[i], selected_hosts[i + 1]]
                )

            log.info("Step 4: Wait for clean PGs")
            wait_for_clean_pg_sets(rados_obj=rados_obj)

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
            ("stretch_mode" in locals() or "stretch_mode" in globals())
            and REGULAR_CLUSTER_WITH_MON_LOCATION
            and not stretch_enabled_checks(rados_obj)
        ):
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
    out = rados_obj.run_ceph_command(
        cmd="ceph health detail", print_output=True, client_exec=True
    )
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

    out = rados_obj.run_ceph_command(
        cmd="ceph health detail", print_output=True, client_exec=True
    )
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

    detail = checks["MON_NETSPLIT"]["detail"]
    for message_detail in detail:
        message = message_detail["message"]
        if site_1_name in message and site_2_name in message:
            info_msg = (
                f"message -> {message}"
                f"Netsplit detected and warning generated between\n"
                f"site 1 name -> {site_1_name}\n"
                f"site 2 name -> {site_2_name}\n"
                f"ceph health detail -> {out}"
            )
            log.info(info_msg)
            break
    else:
        err_msg = (
            f"Netsplit not detected and warning generated between\n"
            f"site 1 name -> {site_1_name}\n"
            f"site 2 name -> {site_2_name}\n"
            f"ceph health detail -> {out}"
        )
        log.error(err_msg)
        raise Exception(err_msg)
    log.info("MON_NETSPLIT warning generated successfully")
