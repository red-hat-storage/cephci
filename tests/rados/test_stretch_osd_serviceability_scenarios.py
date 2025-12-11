"""
This test module is used to test OSD & OSD host replacement scenarios with recovery in the stretch environment
includes:

1. OSD daemon removed
2. OSD daemon added.
3. OSD host removed.
4. OSD host added.
5. Heath warning generated on the Cluster upon uneven weights of data sites.
6. 1 OSD removed from DC1 and 1 OSD removed from DC2
7. 1 OSD removed from all hosts of DC1
8. 1 OSD removed from all hosts of DC1 and all hosts of DC2
9. all OSD removed from 1 host of DC1
10. 1 OSD host removed from DC1 and 1 OSD host removed from DC2
11. Perform below checks post removal and addition
    - Health warning - UNEVEN_WEIGHTS_STRETCH_MODE
    - PG acting set has 2 OSD from datacenter DC1 and 2 OSD from datacenter DC2
    - PGs should reach active+clean state
    - Pool sanity checks should be successful
    - No inactive PGs on the pool
    - Write IO should succeed
"""

import json
import random
import time
from collections import namedtuple

from ceph.ceph_admin import CephAdmin
from ceph.rados import utils
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.monitor_workflows import MonitorWorkflows
from ceph.rados.pool_workflows import PoolFunctions
from ceph.rados.serviceability_workflows import ServiceabilityMethods
from cli.utilities.operations import wait_for_osd_daemon_state
from tests.rados.rados_test_util import (
    get_device_path,
    wait_for_daemon_status,
    wait_for_device_rados,
)
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from tests.rados.test_stretch_site_down import (
    get_stretch_site_hosts,
    stretch_enabled_checks,
)
from utility.log import Log
from utility.utils import generate_unique_id, method_should_succeed, should_not_be_empty

log = Log(__name__)

Hosts = namedtuple("Hosts", ["dc_1_hosts", "dc_2_hosts", "tiebreaker_hosts"])


def run(ceph_cluster, **kw):
    """
    performs replacement scenarios in stretch mode
    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster
    Scenarios:
        scenario-1: 1 OSD daemon removed and added back
        scenario-2: 1 OSD host removed and added back
        scenario-3: 1 OSD removed from DC1 and 1 OSD removed from DC2
        scenario-4: 1 OSD removed from all hosts of DC1
        scenario-5: 1 OSD removed from all hosts of DC1 and all hosts of DC2
        scenario-6: all OSD removed from 1 host of DC1
        scenario-7: 1 OSD host removed from DC1 and 1 OSD host removed from DC2
    """

    log.info(run.__doc__)
    config = kw.get("config")
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    service_obj = ServiceabilityMethods(cluster=ceph_cluster, **config)
    client_node = ceph_cluster.get_nodes(role="client")[0]
    rhbuild = config.get("rhbuild")
    pool_name = config.get("pool_name", "test_stretch_io")
    stretch_bucket = config.get("stretch_bucket", "datacenter")
    tiebreaker_mon_site_name = config.get("tiebreaker_mon_site_name", "tiebreaker")
    add_network_delay = config.get("add_network_delay", False)
    mon_obj = MonitorWorkflows(node=cephadm)
    scenarios_to_run = config.get(
        "scenarios_to_run",
        [
            "scenario-1",
            "scenario-2",
            "scenario-3",
            "scenario-4",
            "scenario-5",
            "scenario-6",
            "scenario-7",
        ],
    )

    def check_stretch_health_warning():
        """
        Method to check if Warn "UNEVEN_WEIGHTS_STRETCH_MODE" is generated on cluster.
        Bug Fix : https://bugzilla.redhat.com/show_bug.cgi?id=2125107

        Args:
        Returns (bool) :
            True -> Warning present on the cluster
            False -> Warning not present on the cluster

        """
        log.info(
            "Checking if health warning for different site weights is Present on cluster"
        )
        status_report = rados_obj.run_ceph_command(cmd="ceph report", client_exec=True)
        ceph_health_status = list(status_report["health"]["checks"].keys())
        log.debug(f"Ceph report: \n\n {status_report} \n\n")
        expected_warn = "UNEVEN_WEIGHTS_STRETCH_MODE"
        if expected_warn in ceph_health_status:
            log.info("warning  present on the cluster")
            log.info(f"Warnings generated on the cluster : {ceph_health_status}")
            return True
        log.info("Warning not present on the cluster")
        return False

    def validate_osd_removal_scenario(
        dc_1_osds_to_remove: list, dc_2_osds_to_remove: list
    ):
        """
        Method to perform osd removal scenario in stretch mode.

        Steps:
        1) collect number of objects in the pool
        2) Remove osd from DC1 and DC2 which as passed as arguements
        3) Perform checks on the cluster post osd removal
            - Health warning - UNEVEN_WEIGHTS_STRETCH_MODE
            - PG acting set has 2 OSD from datacenter DC1 and 2 OSD from datacenter DC2
            - PGs should reach active+clean state
            - Pool sanity checks should be successful
            - No inactive PGs on the pool
            - Write IO should succeed
        4) Add back removed osds
        5) Perform checks on the cluster post osd addition
            - Health warning should be removed UNEVEN_WEIGHTS_STRETCH_MODE
            - PG acting set has 2 OSD from datacenter DC1 and 2 OSD from datacenter DC2
            - PGs should reach active+clean state
            - Pool sanity checks should be successful
            - No inactive PGs on the pool
            - Write IO should succeed
            - Objects should be more than the initial no of objects
        """
        log.info(f"{dc_1_name} osds to remove and add back: {dc_1_osds_to_remove}")
        log.info(f"{dc_2_name} osds to remove and add back: {dc_2_osds_to_remove}")

        # Collecting the initial number of objects on the pool, before osd removal
        pool_stat = rados_obj.get_cephdf_stats(pool_name=pool_name)
        log.debug(f"Pool stat before osd removal: {pool_stat}")
        init_objects = pool_stat["stats"]["objects"]
        rados_object_name = f"obj-{generate_unique_id(4)}"

        rados_obj.change_recovery_threads(config={}, action="set")
        dev_paths = {}
        osd_to_test_hosts_map = {}

        log.info(
            "Proceeding to perform Check (1) PG has 2 OSD from datacenter DC1 and 2 OSD from datacenter DC2"
        )
        method_should_succeed(validate_acting_set)

        services_list = rados_obj.list_orch_services(service_type="osd")
        for service in services_list:
            rados_obj.set_unmanaged_flag(service_type="osd", service_name=service)

        log.info("Proceeding with OSD removal scenario")
        for target_osd in dc_1_osds_to_remove + dc_2_osds_to_remove:
            test_host = rados_obj.fetch_host_node(
                daemon_type="osd", daemon_id=target_osd
            )
            log.info(
                f"Removing OSD {target_osd} from host {test_host.hostname} on datacenter DC1"
            )
            dev_paths[target_osd] = get_device_path(test_host, target_osd)
            osd_to_test_hosts_map[target_osd] = test_host
            utils.osd_remove(
                ceph_cluster=ceph_cluster, osd_id=target_osd, zap=True, force=True
            )
            method_should_succeed(wait_for_clean_pg_sets, rados_obj, timeout=12000)
            method_should_succeed(
                wait_for_device_rados, test_host, target_osd, action="remove"
            )

        log.info(
            """Performing following checks on the cluster after OSD removal
1) PG has 2 OSD from datacenter DC1 and 2 OSD from datacenter DC2
2) Health warning - UNEVEN_WEIGHTS_STRETCH_MODE
3) PGs should reach active+clean state
4) Pool sanity checks should be successful
5) No inactive PGs on the pool
6) Write IO should succeed"""
        )

        log.info(
            "Proceeding to perform Check (3) Health warning - UNEVEN_WEIGHTS_STRETCH_MODE post OSD removal"
        )
        if float(rhbuild.split("-")[0]) >= 7.1 and len(dc_1_osds_to_remove) != len(
            dc_2_osds_to_remove
        ):
            if not check_stretch_health_warning():
                log.error("Warnings not removed on the cluster post osd addition")
                raise Exception("Warning present on cluster")

        log.info(
            "Proceeding to perform Check (4) PGs should reach active+clean state post OSD removal"
        )
        method_should_succeed(wait_for_clean_pg_sets, rados_obj, timeout=12000)

        log.info(
            "Proceeding to perform Check (5) Pool sanity checks should be successful post OSD removal"
        )
        method_should_succeed(rados_obj.run_pool_sanity_check)

        log.info(
            "Proceeding to perform Check (6) No inactive PGs on the pool post OSD removal"
        )
        if not rados_obj.check_inactive_pgs_on_pool(pool_name=pool_name):
            log.error(f"Inactive PGs found on pool : {pool_name}")

        log.info(
            "Proceeding to perform Check (7) Write IO should succeed post OSD removal"
        )
        pool_obj.do_rados_put(
            client=client_node,
            pool=pool_name,
            nobj=200,
            timeout=50,
            obj_name=rados_object_name,
        )

        log.info(
            f"Proceeding to add back removed OSDs:\
                 DC1 OSDs: {dc_1_osds_to_remove} \
                 DC2 OSDs: {dc_2_osds_to_remove}"
        )

        for target_osd in dc_1_osds_to_remove + dc_2_osds_to_remove:
            test_host = osd_to_test_hosts_map[target_osd]
            log.info(f"Adding OSD {target_osd} to host {test_host.hostname}")
            utils.add_osd(
                ceph_cluster,
                test_host.hostname,
                dev_paths[target_osd],
                target_osd,
            )

        for target_osd in dc_1_osds_to_remove + dc_2_osds_to_remove:
            client = ceph_cluster.get_nodes(role="client")[0]
            log.info(f'Waiting for OSD {target_osd} to be "up" state')
            wait_for_osd_daemon_state(client, target_osd, "up")

        assert service_obj.add_osds_to_managed_service()

        log.info(
            f"Successfully removed and added back removed OSDs:\
                 DC1 OSDs: {dc_1_osds_to_remove} \
                 DC2 OSDs: {dc_2_osds_to_remove}"
        )

        log.info(
            """Performing following checks on the cluster after OSD addition post removal
1) Health warning should be removed UNEVEN_WEIGHTS_STRETCH_MODE
2) PG has 2 OSD from datacenter DC1 and 2 OSD from datacenter DC2
3) PGs should reach active+clean state
4) Pool sanity checks should be successful
5) No inactive PGs on the pool
6) Write IO should succeed
7) Objects should be more than the initial no of objects"""
        )

        log.info(
            "Proceeding to perform Check (1) Health warning should be removed UNEVEN_WEIGHTS_STRETCH_MODE"
        )
        if float(rhbuild.split("-")[0]) >= 7.1:
            if check_stretch_health_warning():
                log.error("Warnings not removed on the cluster post osd addition")
                raise Exception("Warning present on cluster")

        log.info(
            "Proceeding to perform Check (1) PG has 2 OSD from datacenter DC1 and 2 OSD from datacenter DC2"
        )
        method_should_succeed(validate_acting_set)

        log.info(
            "Proceeding to perform Check (4) PGs should reach active+clean state post OSD removal and addition"
        )
        method_should_succeed(wait_for_clean_pg_sets, rados_obj, timeout=12000)

        log.info(
            "Proceeding to perform Check (5) Pool sanity checks should be successful post OSD removal and addition"
        )
        method_should_succeed(rados_obj.run_pool_sanity_check)

        log.info(
            "Proceeding to perform Check (6) No inactive PGs on the pool post OSD removal and addition"
        )
        if not rados_obj.check_inactive_pgs_on_pool(pool_name=pool_name):
            log.error(f"Inactive PGs found on pool : {pool_name}")

        log.info(
            "Proceeding to perform Check (7) Write IO should succeed post OSD removal and addition"
        )
        pool_obj.do_rados_put(
            client=client_node,
            pool=pool_name,
            nobj=200,
            timeout=50,
            obj_name=rados_object_name,
        )

        services_list = rados_obj.list_orch_services(service_type="osd")
        for service in services_list:
            rados_obj.set_managed_flag(service_type="osd", service_name=service)

        log.debug("sleeping for 20 seconds for the objects to be displayed in ceph df")
        time.sleep(20)

        pool_stat = rados_obj.get_cephdf_stats(pool_name=pool_name)
        log.debug(f"Pool stat post osd removal: {pool_stat}")
        post_osd_rm_objs = pool_stat["stats"]["objects"]

        # Objects should be more than the initial no of objects
        if post_osd_rm_objs <= init_objects:
            log.error(
                "Write ops should be possible, number of objects in the pool has not changed"
            )
            raise Exception(
                f"Pool {pool_name} has {pool_stat['stats']['objects']} objs"
            )
        log.info(
            f"Successfully wrote {pool_stat['stats']['objects']} on pool {pool_name}"
        )

    def validate_host_removal_scenario(dc_1_hosts_to_remove, dc_2_hosts_to_remove):
        """
        1) Drain osd host - ceph orch host drain
        2) Wait until all process are drained
        3) Check osd rm status
        4) Once hosts are drained host is removed
        5) Checks are performed once host is removed from cluster
            - Health warning - UNEVEN_WEIGHTS_STRETCH_MODE
            - PG has 2 OSD from datacenter DC1 and 2 OSD from datacenter DC2
            - PGs should reach active+clean state
            - Pool sanity checks should be successful
            - No inactive PGs on the pool
            - Write IO should succeed
        6) Add removed hosts back to the cluster
        7) Checks are performed once removed hosts are added back
            - Health warning - UNEVEN_WEIGHTS_STRETCH_MODE
            - PG has 2 OSD from datacenter DC1 and 2 OSD from datacenter DC2
            - PGs should reach active+clean state
            - Pool sanity checks should be successful
            - No inactive PGs on the pool
            - Write IO should succeed
            - Objects should be more than the initial no of objects
        """

        pool_stat = rados_obj.get_cephdf_stats(pool_name=pool_name)
        log.debug(f"Pool stat before osd host removal: {pool_stat}")
        init_objects = pool_stat["stats"]["objects"]
        test_hosts_map = {}
        rados_object_name = f"obj-{generate_unique_id(4)}"

        dc_1_osds_removed, dc_2_osds_removed = [], []

        for hostname in dc_1_hosts_to_remove + dc_2_hosts_to_remove:
            osds = rados_obj.collect_osd_daemon_ids(hostname_to_cephnode_map[hostname])
            if hostname in dc_1_hosts:
                dc_1_osds_removed += osds
            else:
                dc_2_osds_removed += osds

        log.info(
            "Proceeding to perform Check (1) PG has 2 OSD from datacenter DC1 and 2 OSD from datacenter DC2"
        )
        method_should_succeed(validate_acting_set)

        log.info(
            f"Proceeding to remove hosts: {dc_1_hosts_to_remove + dc_2_hosts_to_remove}"
        )
        for hostname in dc_1_hosts_to_remove + dc_2_hosts_to_remove:
            log.info(f"Selected host {hostname} for removal")
            test_host = rados_obj.get_host_object(hostname=hostname)
            test_hosts_map[hostname] = test_host
            service_obj.remove_custom_host(host_node_name=test_host.hostname)
        time.sleep(10)

        log.info(
            """Performing following checks on the cluster post OSD Host removal
1) PG has 2 OSD from datacenter DC1 and 2 OSD from datacenter DC2
3) Health warning - UNEVEN_WEIGHTS_STRETCH_MODE
4) PGs should reach active+clean state
5) Pool sanity checks should be successful
6) No inactive PGs on the pool
7) Write IO should succeed"""
        )

        log.info(
            "Proceeding to perform Check (3) No inactive PGs on the pool post OSD Host removal"
        )
        if not rados_obj.check_inactive_pgs_on_pool(pool_name=pool_name):
            log.error(f"Inactive PGs found on pool : {pool_name}")

        log.info(
            "Proceeding to perform Check (4) PGs should reach active+clean state post OSD Host removal"
        )
        method_should_succeed(wait_for_clean_pg_sets, rados_obj, timeout=12000)

        log.info(
            "Proceeding to perform Check (5) Pool sanity checks should be successful post OSD Host removal"
        )
        method_should_succeed(rados_obj.run_pool_sanity_check)

        log.info(
            "Proceeding to perform Check (6) Health warning - UNEVEN_WEIGHTS_STRETCH_MODE post OSD Host removal"
        )
        # Checking if the expected health warning about the different site weights is displayed post removal of OSD host
        if float(rhbuild.split("-")[0]) >= 7.1 and len(dc_1_hosts_to_remove) != len(
            dc_1_hosts_to_remove
        ):
            if not check_stretch_health_warning():
                log.error(
                    "Warnings is not displayed on the cluster post osd host removal"
                )
                raise Exception("Warning not present on cluster")

        log.info(
            "Proceeding to perform Check (7) Write IO should succeed post OSD Host removal"
        )
        pool_obj.do_rados_put(
            client=client_node,
            pool=pool_name,
            nobj=200,
            timeout=50,
            obj_name=rados_object_name,
        )

        log.info(
            f"Proceeding to Add hosts after successful OSD Host removal: {dc_1_hosts_to_remove + dc_2_hosts_to_remove}"
        )

        rados_obj.set_unmanaged_flag(service_type="mon", service_name="mon")

        for hostname in dc_1_hosts_to_remove + dc_2_hosts_to_remove:
            test_host = test_hosts_map[hostname]

            if hostname in dc_1_hosts:
                crush_bucket_val = dc_1_name
            else:
                crush_bucket_val = dc_2_name

            log.info(f"Adding OSD host {test_host.hostname} to cluster")
            try:
                service_obj.add_new_hosts(
                    add_nodes=[test_host.hostname],
                    crush_bucket_name=test_host.hostname,
                    crush_bucket_type=stretch_bucket,
                    crush_bucket_val=crush_bucket_val,
                    deploy_osd=False,
                )
                log.info(f"Competed adding OSD host {test_host.hostname} to cluster")
                time.sleep(60)
            except Exception as e:
                log.error(f"Could not add host {test_host.hostname} to cluster: {e}")
                raise Exception(e)

        log.info(
            f"Competed adding hosts {dc_1_hosts_to_remove + dc_2_hosts_to_remove} to the cluster"
        )
        for target_osd in dc_1_osds_removed + dc_2_osds_removed:
            client = ceph_cluster.get_nodes(role="client")[0]
            log.info(f'Waiting for OSD {target_osd} to be "up" state')
            wait_for_osd_daemon_state(client, target_osd, "up")

        log.info("Setting crush location for each monitor for next scenario")
        for hostname in dc_1_hosts_to_remove + dc_2_hosts_to_remove:
            ceph_node_obj = test_hosts_map[hostname]
            if "mon" not in rados_obj.get_host_label(host_name=hostname):
                continue

            if hostname in dc_1_hosts:
                crush_bucket_val = dc_1_name
            else:
                crush_bucket_val = dc_2_name

            if (
                mon_obj.add_mon_service(
                    host=ceph_node_obj,
                    location_type=stretch_bucket,
                    location_name=crush_bucket_val,
                )
                is False
            ):
                log.error(f"Could not set mon location for host {hostname}")
                raise Exception(f"Could not set mon location for host {hostname}")

            log_info_msg = (
                f"Successfully set mon location of {hostname} to {crush_bucket_val}"
            )
            log.info(log_info_msg)

        rados_obj.set_managed_flag(service_type="mon", service_name="mon")

        log.info(
            """Performing following checks on the cluster after OSD removal
1) PG has 2 OSD from datacenter DC1 and 2 OSD from datacenter DC2
3) Health warning - UNEVEN_WEIGHTS_STRETCH_MODE
4) PGs should reach active+clean state
5) Pool sanity checks should be successful
6) No inactive PGs on the pool
7) Write IO should succeed"""
        )

        log.info(
            "Proceeding to perform Check (1) PG has 2 OSD from datacenter DC1 and 2 OSD from datacenter DC2"
        )
        method_should_succeed(validate_acting_set)

        log.info(
            "Proceeding to perform Check (3) No inactive PGs on the pool post OSD Host addition"
        )
        # Waiting for recovery to post OSD host Addition
        if not rados_obj.check_inactive_pgs_on_pool(pool_name=pool_name):
            log.error(f"Inactive PGs found on pool : {pool_name}")

        log.info(
            "Proceeding to perform Check (4) PGs should reach active+clean state post OSD addition"
        )
        method_should_succeed(wait_for_clean_pg_sets, rados_obj, timeout=12000)

        log.info(
            "Proceeding to perform Check (5) Pool sanity checks should be successful post OSD addition"
        )
        method_should_succeed(rados_obj.run_pool_sanity_check)

        log.info(
            "Proceeding to perform Check (6) Health warning - UNEVEN_WEIGHTS_STRETCH_MODE post OSD addition"
        )
        # Checking if the health warning about the different site weights is removed post addition of OSD host
        if float(rhbuild.split("-")[0]) >= 7.1:
            if check_stretch_health_warning():
                log.error(
                    "Warnings is not removed on the cluster post OSD Host addition"
                )
                raise Exception("Warning present on cluster post OSD Host addition")

        log.info(
            "Proceeding to perform Check (7) Write IO should succeed post OSD Host addition"
        )
        pool_obj.do_rados_put(
            client=client_node,
            pool=pool_name,
            nobj=200,
            timeout=50,
            obj_name=rados_object_name,
        )

        log.debug("sleeping for 20 seconds for the objects to be displayed in ceph df")
        time.sleep(20)

        pool_stat = rados_obj.get_cephdf_stats(pool_name=pool_name)
        log.debug(f"Pool stat post osd host removal: {pool_stat}")
        post_osd_rm_objs = pool_stat["stats"]["objects"]

        # Objects should be more than the initial no of objects
        if post_osd_rm_objs <= init_objects:
            log.error(
                "Write ops should be possible, number of objects in the pool has not changed"
            )
            raise Exception(
                f"Pool {pool_name} has {pool_stat['stats']['objects']} objs"
            )
        log.info(
            f"Successfully wrote {pool_stat['stats']['objects']} on pool {pool_name}"
        )

    def validate_acting_set() -> bool:
        """
        Method to check if PG acting set has 2 OSD from DC1 and 2 OSD from DC2 for stretch mode
        Args used:
            rados_obj: RadosOrchestrator object
            scrub_object: RadosScrubber object
            dc_1_hosts: hosts present in dc_1_name
            dc_2_hosts: hosts present in dc_2_name

        Returns:
            True: If all PG acting set has 2 OSD from DC1 and 2 OSD from DC2
            False: If above condition is false

        Usage:
            validate_acting_set()
        """

        dc_1_osds, dc_2_osds = [], []

        log.info(f"Collecting {dc_1_name} osds")
        for hostname in dc_1_hosts:
            dc_1_osds += rados_obj.collect_osd_daemon_ids(
                hostname_to_cephnode_map[hostname]
            )

        log.info(f"Collecting {dc_2_name} osds")
        for hostname in dc_2_hosts:
            dc_2_osds += rados_obj.collect_osd_daemon_ids(
                hostname_to_cephnode_map[hostname]
            )

        log.info(f"{dc_1_name} osds: {dc_1_osds}")
        log.info(f"{dc_2_name} osds: {dc_2_osds}")

        _cmd = "ceph pg dump_json pgs"
        client = ceph_cluster.get_nodes(role="client")[0]
        dump_out_str, _ = client.exec_command(cmd=_cmd)
        if dump_out_str.isspace():
            log.error("Output of ceph pg dump is empty")
            return False

        log.info("Proceeding to validate PG acting set acting count in each PG")
        dump_out = json.loads(dump_out_str)
        pg_stats = dump_out["pg_map"]["pg_stats"]
        for pg_stat in pg_stats:
            log.debug(f"Checking OSD count acting set for PG {pg_stat['pgid']}")
            dc_1_osd_count = 0
            dc_2_osd_count = 0
            for osd in pg_stat["acting"]:
                if osd in dc_1_osds:
                    dc_1_osd_count += 1
                if osd in dc_2_osds:
                    dc_2_osd_count += 1
            if dc_1_osd_count != 2 and dc_2_osd_count != 2:
                log.error(
                    f"PG {pg_stat['pgid']} set does not have 2 OSDs from each of DC1 and DC2"
                    f"PG acting set -> {pg_stat['acting']}"
                )
                return False
        log.info("All PG acting set has 2 OSD from DC1 and 2 OSD from DC2")
        return True

    try:
        if not stretch_enabled_checks(rados_obj=rados_obj):
            log.error(
                "The cluster has not cleared the pre-checks to run stretch tests. Exiting..."
            )
            raise Exception("Test pre-execution checks failed")

        log.info(
            "Starting tests performing replacement of OSDs and Hosts in Stretch mode Pre-checks Passed"
        )
        osd_tree_cmd = "ceph osd tree"
        buckets = rados_obj.run_ceph_command(osd_tree_cmd)
        dc_buckets = [d for d in buckets["nodes"] if d.get("type") == stretch_bucket]
        dc_1 = dc_buckets.pop()
        dc_1_name = dc_1["name"]
        dc_2 = dc_buckets.pop()
        dc_2_name = dc_2["name"]
        all_hosts = get_stretch_site_hosts(
            rados_obj=rados_obj, tiebreaker_mon_site_name=tiebreaker_mon_site_name
        )
        dc_1_hosts = all_hosts.dc_1_hosts
        dc_2_hosts = all_hosts.dc_2_hosts
        tiebreaker_hosts = all_hosts.tiebreaker_hosts

        log.debug(f"Hosts present in {stretch_bucket} : {dc_1_name} : {dc_1_hosts}")
        log.debug(f"Hosts present in {stretch_bucket} : {dc_2_name} : {dc_2_hosts}")
        log.debug(
            f"Hosts present in {stretch_bucket} : {tiebreaker_mon_site_name} : {tiebreaker_hosts}"
        )
        if add_network_delay:
            for host in dc_1_hosts:
                rados_obj.add_network_delay_on_host(
                    hostname=host, delay="10ms", set_delay=True
                )
            for host in dc_2_hosts:
                rados_obj.add_network_delay_on_host(
                    hostname=host, delay="10ms", set_delay=True
                )
            for host in tiebreaker_hosts:
                rados_obj.add_network_delay_on_host(
                    hostname=host, delay="100ms", set_delay=True
                )

            log.info("Successfully set network delays on the Stretch cluster nodes")

        # Creating test pool to check the effect of Site down on the Pool IO
        if not rados_obj.create_pool(pool_name=pool_name):
            log.error(f"Failed to create pool : {pool_name}")
            raise Exception("Test execution failed")

        if "scenario-1" in scenarios_to_run:

            # Sleeping for 10 seconds for pool to be populated in the cluster
            time.sleep(10)

            # Collecting the init no of objects on the pool, before site down
            pool_stat = rados_obj.get_cephdf_stats(pool_name=pool_name)
            init_objects = pool_stat["stats"]["objects"]
            inactive_pgs = 0

            # Increasing the recovery threads on the cluster
            rados_obj.change_recovery_threads(config={}, action="set")

            # Proceeding to remove one OSD from any of the data sites
            target_osd = rados_obj.get_pg_acting_set(pool_name=pool_name)[0]
            log.debug(
                f"Proceeding to remove one OSD from any of the data sites. OSD selected at random : {target_osd}"
            )
            log.debug(
                f"Ceph osd tree before OSD removal : \n\n {rados_obj.run_ceph_command(cmd='ceph osd tree')} \n\n"
            )
            test_host = rados_obj.fetch_host_node(
                daemon_type="osd", daemon_id=target_osd
            )
            should_not_be_empty(test_host, "Failed to fetch host details")
            dev_path = get_device_path(test_host, target_osd)
            target_osd_spec_name = service_obj.get_osd_spec(osd_id=target_osd)
            log_lines = (
                f"\nosd device path  : {dev_path},\n osd_id : {target_osd},\n hostname : {test_host.hostname},\n"
                f"Target OSD Spec : {target_osd_spec_name}"
            )
            log.debug(log_lines)
            rados_obj.set_service_managed_type(service_type="osd", unmanaged=True)
            method_should_succeed(utils.set_osd_out, ceph_cluster, target_osd)
            method_should_succeed(wait_for_clean_pg_sets, rados_obj, timeout=12000)
            log.debug("Cluster clean post draining of OSD for removal")
            utils.osd_remove(ceph_cluster, target_osd)
            method_should_succeed(wait_for_clean_pg_sets, rados_obj, timeout=12000)
            method_should_succeed(
                utils.zap_device, ceph_cluster, test_host.hostname, dev_path
            )
            method_should_succeed(
                wait_for_device_rados, test_host, target_osd, action="remove"
            )

            # Waiting for recovery to post OSD host removal
            method_should_succeed(wait_for_clean_pg_sets, rados_obj, timeout=12000)

            # Checking if the expected health warning about the different site weights are seen.
            if float(rhbuild.split("-")[0]) >= 7.1:
                if not check_stretch_health_warning():
                    log.error("Warnings not generated on the cluster")
                    raise Exception("Warning not present on cluster")

            # Checking cluster health after OSD removal
            method_should_succeed(rados_obj.run_pool_sanity_check)
            if not rados_obj.check_inactive_pgs_on_pool(pool_name=pool_name):
                log.error(f"Inactive PGs found on pool : {pool_name}")
                inactive_pgs += 1

            log.debug(
                f"Ceph osd tree after OSD removal : \n\n {rados_obj.run_ceph_command(cmd='ceph osd tree')} \n\n"
            )
            log.info(
                f"Removal of OSD : {target_osd} is successful. Proceeding to add back the OSD daemon."
            )

            # Adding the removed OSD back and checking the cluster status
            log.debug("Adding the removed OSD back and checking the cluster status")
            utils.add_osd(ceph_cluster, test_host.hostname, dev_path, target_osd)
            method_should_succeed(
                wait_for_device_rados, test_host, target_osd, action="add"
            )
            method_should_succeed(
                wait_for_daemon_status,
                rados_obj=rados_obj,
                daemon_type="osd",
                daemon_id=target_osd,
                status="running",
                timeout=300,
            )
            assert service_obj.add_osds_to_managed_service(
                osds=[target_osd], spec=target_osd_spec_name
            )
            time.sleep(30)
            log.debug(
                "Completed addition of OSD post removal. Checking for inactive PGs post OSD addition"
            )
            if not rados_obj.check_inactive_pgs_on_pool(pool_name=pool_name):
                log.error(f"Inactive PGs found on pool : {pool_name}")
                inactive_pgs += 1

            if inactive_pgs > 10:
                log.error(
                    "Found inactive PGs on the cluster during OSD removal/Addition"
                )
                raise Exception("Inactive PGs during stop error")

            # Checking if the expected health warning about the different site weights is removed post addition of OSD.
            if float(rhbuild.split("-")[0]) >= 7.1:
                if check_stretch_health_warning():
                    log.error("Warnings not removed on the cluster post osd addition")
                    raise Exception("Warning present on cluster")

            # Checking cluster health after OSD Addition
            method_should_succeed(rados_obj.run_pool_sanity_check)
            log.info(
                f"Addition of OSD : {target_osd} back into the cluster was successful, and the health is good!"
            )
            services_list = rados_obj.list_orch_services(service_type="osd")
            for service in services_list:
                if not rados_obj.set_managed_flag(
                    service_type="osd", service_name=service
                ):
                    log_error = f"Service {service} could not be set to managed"
                    raise Exception(log_error)

            log.info("Completed the removal and addition of OSD daemons")

            # perform rados put to check if write ops is possible
            pool_obj.do_rados_put(
                client=client_node, pool=pool_name, nobj=200, timeout=50
            )

            log.debug(
                "sleeping for 20 seconds for the objects to be displayed in ceph df"
            )
            time.sleep(20)

            # Getting the number of objects post write, to check if writes were successful
            pool_stat = rados_obj.get_cephdf_stats(pool_name=pool_name)
            log.debug(pool_stat)
            post_osd_rm_objs = pool_stat["stats"]["objects"]

            # Objects should be more than the initial no of objects
            if post_osd_rm_objs <= init_objects:
                log.error(
                    "Write ops should be possible, number of objects in the pool has not changed"
                )
                raise Exception(
                    f"Pool {pool_name} has {pool_stat['stats']['objects']} objs"
                )
            log.info(
                f"Successfully wrote {pool_stat['stats']['objects']} on pool {pool_name} in degraded mode\n"
                f"Proceeding to bring up the nodes and recover the cluster from degraded mode"
            )
            log.info("Completed OSD daemon replacement scenarios")

        if "scenario-2" in scenarios_to_run:

            log.info("Proceeding to do OSD host replacement in Stretch mode")
            test_host_site = (
                dc_1_name if test_host.hostname in dc_1_hosts else dc_2_name
            )
            log.info(
                f"OSD host to be removed : {test_host.hostname} from site {test_host_site}"
            )

            log.debug(
                f"Ceph osd tree before host removal : \n\n {rados_obj.run_ceph_command(cmd='ceph osd tree')} \n\n"
            )
            try:
                service_obj.remove_custom_host(host_node_name=test_host.hostname)
            except Exception as err:
                log.error(
                    f"Could not remove host : {test_host.hostname}. Error : {err}"
                )
                raise Exception("Host not removed error")
            time.sleep(10)

            # Waiting for recovery to post OSD host removal
            if not rados_obj.check_inactive_pgs_on_pool(pool_name=pool_name):
                log.error(f"Inactive PGs found on pool : {pool_name}")
                inactive_pgs += 1
            method_should_succeed(wait_for_clean_pg_sets, rados_obj, timeout=12000)
            method_should_succeed(rados_obj.run_pool_sanity_check)
            if inactive_pgs > 10:
                log.error(
                    "Found inactive PGs on the cluster during OSD removal/Addition"
                )
                raise Exception("Inactive PGs during stop error")

            # Checking if the expected health warning about the different
            #  site weights is displayed post removal of OSD host
            if float(rhbuild.split("-")[0]) >= 7.1:
                if not check_stretch_health_warning():
                    log.error(
                        "Warnings is not displayed on the cluster post osd host removal"
                    )
                    raise Exception("Warning not present on cluster")

            log.debug(
                f"Ceph osd tree after host removal : \n\n {rados_obj.run_ceph_command(cmd='ceph osd tree')} \n\n"
            )
            # Adding a new host to the cluster
            osdcount_pre = service_obj.get_osd_count()
            out = client_node.exec_command(cmd="ceph orch ls")
            log.info(out)
            try:
                service_obj.add_new_hosts(
                    add_nodes=[test_host.hostname],
                    crush_bucket_name=test_host.hostname,
                    crush_bucket_type=stretch_bucket,
                    crush_bucket_val=test_host_site,
                    deploy_osd=False,
                )
            except Exception as err:
                log.error(
                    f"Could not add host : {test_host.hostname} into the cluster and deploy OSDs. Error : {err}"
                )
                raise Exception("Host not added error")
            time.sleep(60)
            out = client_node.exec_command(cmd="ceph orch ls")
            log.info(out)
            osdcount_post = service_obj.get_osd_count()
            log_info_msg = (
                f"OSD count before new_osds service deployment: {osdcount_pre}"
                f"\nOSD count post new_osds service deployment: {osdcount_post}"
            )
            log.info(log_info_msg)
            if not osdcount_pre < osdcount_post:
                log.error("New OSDs were not added into the cluster")
                raise Exception("Execution error")

            # Waiting for recovery to post OSD host Addition
            if not rados_obj.check_inactive_pgs_on_pool(pool_name=pool_name):
                log.error(f"Inactive PGs found on pool : {pool_name}")
                inactive_pgs += 1
            method_should_succeed(wait_for_clean_pg_sets, rados_obj, timeout=12000)
            method_should_succeed(rados_obj.run_pool_sanity_check)
            if inactive_pgs > 10:
                log.error(
                    "Found inactive PGs on the cluster during OSD removal/Addition"
                )
                raise Exception("Inactive PGs during stop error")

            # Checking if the health warning about the different site weights is removed post addition of OSD host
            if float(rhbuild.split("-")[0]) >= 7.1:
                if check_stretch_health_warning():
                    log.error(
                        "Warnings is not removed on the cluster post osd host addition"
                    )
                    raise Exception("Warning present on cluster")

            # perform rados put to check if write ops is possible
            pool_obj.do_rados_put(
                client=client_node, pool=pool_name, nobj=200, timeout=50
            )

            log.debug(
                "sleeping for 20 seconds for the objects to be displayed in ceph df"
            )
            time.sleep(20)

            log.info("Completed OSD Host replacement scenarios")
            log.info("Successfully removed and added hosts on stretch mode cluster")

        # collecting CephNode object for each host and store in hashmap
        hostname_to_cephnode_map = dict()
        for hostname in dc_1_hosts + dc_2_hosts:
            hostname_to_cephnode_map[hostname] = ceph_cluster.get_node_by_hostname(
                hostname=hostname
            )

        if "scenario-3" in scenarios_to_run:
            log.info(
                f"Test #3: Remove 1 osd from 1 host in {dc_1_name} and 1 osd from 1 host in {dc_2_name}"
            )

            dc_1_osds_to_remove = [
                random.choice(
                    [
                        str(osd_id)
                        for osd_id in rados_obj.collect_osd_daemon_ids(
                            hostname_to_cephnode_map[random.choice(dc_1_hosts)]
                        )
                    ]
                )
            ]
            dc_2_osds_to_remove = [
                random.choice(
                    [
                        str(osd_id)
                        for osd_id in rados_obj.collect_osd_daemon_ids(
                            hostname_to_cephnode_map[random.choice(dc_2_hosts)]
                        )
                    ]
                )
            ]

            log.info(f"{dc_1_name} osds to remove are {dc_1_osds_to_remove}")
            log.info(f"{dc_2_name} osds to remove are {dc_2_osds_to_remove}")
            validate_osd_removal_scenario(dc_1_osds_to_remove, dc_2_osds_to_remove)

        if "scenario-4" in scenarios_to_run:
            log.info(f"Test #4: Remove 1 osd from all host in {dc_1_name}")
            dc_1_osds_to_remove, dc_2_osds_to_remove = [], []
            for hostname in dc_1_hosts:
                osd_list = [
                    str(osd_id)
                    for osd_id in rados_obj.collect_osd_daemon_ids(
                        hostname_to_cephnode_map[hostname]
                    )
                ]
                dc_1_osds_to_remove.append(random.choice(osd_list))

            log.info(f"{dc_1_name} osds to remove are {dc_1_osds_to_remove}")
            log.info(f"{dc_2_name} osds to remove are {dc_2_osds_to_remove}")
            validate_osd_removal_scenario(dc_1_osds_to_remove, dc_2_osds_to_remove)

        if "scenario-5" in scenarios_to_run:
            log.info(
                f"Test #5: Remove 1 osd from all host in {dc_1_name} and 1 osd from all host in {dc_2_name}"
            )
            dc_1_osds_to_remove, dc_2_osds_to_remove = [], []
            for hostname in dc_1_hosts:
                osd_list = [
                    str(osd_id)
                    for osd_id in rados_obj.collect_osd_daemon_ids(
                        hostname_to_cephnode_map[hostname]
                    )
                ]
                dc_1_osds_to_remove.append(random.choice(osd_list))

            for hostname in dc_2_hosts:
                osd_list = [
                    str(osd_id)
                    for osd_id in rados_obj.collect_osd_daemon_ids(
                        hostname_to_cephnode_map[hostname]
                    )
                ]
                dc_2_osds_to_remove.append(random.choice(osd_list))

            log.info(f"{dc_1_name} osds to remove are {dc_1_osds_to_remove}")
            log.info(f"{dc_2_name} osds to remove are {dc_2_osds_to_remove}")
            validate_osd_removal_scenario(dc_1_osds_to_remove, dc_2_osds_to_remove)

        if "scenario-6" in scenarios_to_run:
            log.info("Test #6: Remove all osd from 1 host")
            dc_1_osds_to_remove = [
                str(osd_id)
                for osd_id in rados_obj.collect_osd_daemon_ids(
                    hostname_to_cephnode_map[random.choice(dc_1_hosts)]
                )
            ]
            dc_2_osds_to_remove = []

            log.info(f"{dc_1_name} osds to remove are {dc_1_osds_to_remove}")
            log.info(f"{dc_2_name} osds to remove are {dc_2_osds_to_remove}")
            validate_osd_removal_scenario(dc_1_osds_to_remove, dc_2_osds_to_remove)

        if "scenario-7" in scenarios_to_run:
            log.info(
                f"Test #7: Remove 1 osd host from {dc_1_name} and 1 osd host from {dc_2_name}"
            )
            dc_1_hosts_to_remove = [random.choice(dc_1_hosts)]
            dc_2_hosts_to_remove = [random.choice(dc_2_hosts)]
            validate_host_removal_scenario(dc_1_hosts_to_remove, dc_2_hosts_to_remove)

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
        if add_network_delay:
            for host in dc_1_hosts:
                rados_obj.add_network_delay_on_host(hostname=host, set_delay=False)
            for host in dc_2_hosts:
                rados_obj.add_network_delay_on_host(hostname=host, set_delay=False)
            for host in tiebreaker_hosts:
                rados_obj.add_network_delay_on_host(hostname=host, set_delay=False)

            log.info(
                "Successfully removed the network delays added on the Stretch cluster nodes"
            )
        # restoring the recovery threads on the cluster
        rados_obj.change_recovery_threads(config={}, action="rm")
        # remove empty service specs after host removal
        rados_obj.remove_empty_service_spec()

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
