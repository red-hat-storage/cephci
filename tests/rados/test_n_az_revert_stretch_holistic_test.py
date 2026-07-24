"""
This test module is used to test holistic tests in N-AZ cluster
includes:
1. 3 AZ stretch cluster -> 2 site stretch cluster -> non-stretch cluster

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
from tests.rados.rados_test_util import wait_for_daemon_status
from tests.rados.stretch_cluster import (
    setup_crush_rule_with_no_affinity,
    wait_for_clean_pg_sets,
)
from tests.rados.test_stretch_revert_class import (
    RevertStretchModeFunctionalities,
    wait_till_host_status_reaches,
)
from tests.rados.test_stretch_site_reboot import get_host_obj_from_hostname
from utility.log import Log

log = Log(__name__)

Hosts = namedtuple("Hosts", ["dc_1_hosts", "dc_2_hosts", "tiebreaker_hosts"])


def run(ceph_cluster, **kw):
    log.info("=" * 80)
    log.info("Starting test execution")
    log.info("=" * 80)
    log.info(run.__doc__)

    log.info("Initializing test components...")
    config = kw.get("config")
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    pool_obj = PoolFunctions(node=cephadm)
    rados_obj = RadosOrchestrator(node=cephadm)
    stretch_bucket = config.get("stretch_bucket", "datacenter")
    tiebreaker_mon_site_name = config.get("tiebreaker_mon_site_name", "tiebreaker")
    client_node = ceph_cluster.get_nodes(role="client")[0]
    mon_obj = MonitorWorkflows(node=cephadm)
    mon_election_obj = MonElectionStrategies(rados_obj=rados_obj)
    rule_name = config.get("rule_name", "3az_rule")
    log.info(f"Stretch bucket type: {stretch_bucket}")
    log.info(f"Tiebreaker mon site name: {tiebreaker_mon_site_name}")
    log.info("Test components initialized successfully")
    log.info("")

    config = {
        "rados_obj": rados_obj,
        "pool_obj": pool_obj,
        "tiebreaker_mon_site_name": tiebreaker_mon_site_name,
        "stretch_bucket": stretch_bucket,
        "client_node": client_node,
        "mon_obj": mon_obj,
        "mon_election_obj": mon_election_obj,
    }
    test_pool_name = None
    revert_stretch_mode_scenarios = None
    try:
        log.info("=" * 80)
        log.info(
            "Test Case: 3 AZ stretch cluster -> 2 site stretch cluster -> non-stretch cluster"
        )
        log.info("=" * 80)
        log.info(
            """Step 1): Verify 3 AZ stretch cluster setup
Step 2): Simulate datacenter DC2 failure
Step 3): Revert from 3 AZ stretched pool cluster
Step 4): Remove all offline hosts from DC2 bucket
Step 5): Remove the DC2 bucket from CRUSH map
Step 6): Restart hosts from DC2
Step 7): Remove ceph cluster from restarted hosts
Step 8): Add the first restarted host back to the cluster
Step 9): Enter 2-site stretch mode with DC1, DC3 and tiebreaker
Step 10): Simulate datacenter DC3 failure
Step 11): Revert 2-site stretch mode back to non-stretch cluster
Step 12): Remove all offline hosts from DC3 bucket"""
        )
        log.info("=" * 80)

        # Step 1: Verify we are in 3 AZ stretch cluster with DC1, DC2, DC3
        log.info("")
        log.info("=" * 80)
        log.info("Step 1): Verifying 3 AZ stretch cluster setup")
        log.info("=" * 80)
        log.info("Retrieving OSD tree to identify datacenters...")
        osd_tree_cmd = "ceph osd tree"
        buckets = rados_obj.run_ceph_command(osd_tree_cmd)
        dc_buckets = [d for d in buckets["nodes"] if d.get("type") == stretch_bucket]
        dc_names = [name["name"] for name in dc_buckets]
        log.info(f"Datacenters found in cluster: {dc_names}")

        if len(dc_names) < 3:
            log.error(
                f"Expected at least 3 datacenters for 3 AZ setup, found: {dc_names}"
            )
            raise Exception(
                f"Expected at least 3 datacenters for 3 AZ setup, found: {dc_names}"
            )

        # Get hosts for all datacenters
        log.info("Retrieving hosts for all datacenters...")
        all_hosts = rados_obj.get_multi_az_stretch_site_hosts(
            num_data_sites=len(dc_names), stretch_bucket=stretch_bucket
        )
        for site in dc_names:
            log.info(f"Hosts present in Datacenter {site}: {getattr(all_hosts, site)}")

        dc1_name = dc_names[0]
        dc2_name = dc_names[1]
        dc3_name = dc_names[2]

        dc1_hosts = getattr(all_hosts, dc1_name)
        dc2_hosts = getattr(all_hosts, dc2_name)
        dc3_hosts = getattr(all_hosts, dc3_name)

        log.info(f"DC1 ({dc1_name}) hosts: {dc1_hosts}")
        log.info(f"DC2 ({dc2_name}) hosts: {dc2_hosts}")
        log.info(f"DC3 ({dc3_name}) hosts: {dc3_hosts}")

        log.info(
            "If stretch mode pools is enabled skip, else enable stretch mode on pools"
        )

        for pool in rados_obj.list_pools():
            try:
                log.debug("Checking if the stretch mode op the pool : " + pool)
                cmd = f"ceph osd pool stretch show {pool}"
                out = rados_obj.run_ceph_command(cmd=cmd)
                log.debug("Stretch pool details on pool : %s : \n %s \n ", pool, out)
            except Exception as err:
                log.info(
                    f"Stretch mode pool not enabled on pool {pool}. Enabling stretch mode on the pool."
                )
                if not rados_obj.create_n_az_stretch_pool(
                    pool_name=pool,
                    rule_name=rule_name,
                    rule_id=101,
                    peer_bucket_barrier=stretch_bucket,
                    num_sites=3,
                    num_copies_per_site=2,
                    total_buckets=3,
                    req_peering_buckets=2,
                ):
                    raise Exception(
                        "stretch mode not enabled on the pool : %s. \n Error message : %s \n",
                        pool,
                        err,
                    )
                log.info(f"Stretch mode enabled on the pool {pool}")

        # Wait for clean PGs before starting
        log.info(
            "Waiting for PGs to reach active+clean state before starting scenario..."
        )
        if not wait_for_clean_pg_sets(rados_obj=rados_obj):
            log.error("PG did not reach active+clean before start of scenario")
            raise Exception("PG did not reach active+clean before start of scenario")
        log.info("All PGs are in active+clean state. Proceeding with test scenario.")
        log.info("Step 1) completed successfully")

        # Step 2: Simulate datacenter DC2 failure
        log.info("=" * 80)
        log.info(f"Step 2): Simulating datacenter {dc2_name} failure")
        log.info("=" * 80)
        log.info(f"Shutting down all hosts in datacenter {dc2_name}...")
        for host in dc2_hosts:
            log.info(f"Shutting down host: {host}")
            target_node = find_vm_node_by_hostname(ceph_cluster, host)
            target_node.shutdown(wait=True)
            log.info(f"Host {host} has been shut down successfully")

        log.info(f"All hosts in datacenter {dc2_name} have been shut down")

        # Verify cluster health shows DC2 down
        log.info("Checking cluster health status after DC2 shutdown...")
        status_report = rados_obj.run_ceph_command(cmd="ceph report", client_exec=True)
        ceph_health_status = list(status_report["health"]["checks"].keys())
        log.info(f"Cluster health status after DC2 shutdown: {ceph_health_status}")
        log.info("Step 2) completed successfully")
        log.info("")

        # Step 3: Revert from 3 AZ stretched pool cluster
        log.info("=" * 80)
        log.info("Step 3): Reverting from 3 AZ stretched pool cluster")
        log.info("=" * 80)
        log.info("Retrieving list of pools...")
        pools = "ceph df"
        pools_out = rados_obj.run_ceph_command(
            cmd=pools, client_exec=True, print_output=True
        )
        log.info(f"Found {len(pools_out['pools'])} pools in the cluster")

        log.info("Unsetting 3 AZ stretch mode for all pools...")
        for pool in pools_out["pools"]:
            test_pool_name = pool["name"]
            unset_cmd = (
                f"ceph osd pool stretch unset {test_pool_name} replicated_rule 3 2"
            )
            try:
                log.info(f"Unsetting stretch mode for pool: {test_pool_name}")
                _ = client_node.exec_command(
                    cmd=unset_cmd,
                    print_output=True,
                )
                log.info(f"Successfully unset 3 AZ stretch pool: {test_pool_name}")
            except Exception as e:
                log.error(f"Failed to unset stretch pool {test_pool_name}: {e}")
                raise Exception(f"Failed to unset stretch pool {test_pool_name}: {e}")
        log.info("Step 3) completed successfully")
        log.info("")

        # Step 4: Remove all offline hosts from DC2 bucket
        log.info("=" * 80)
        log.info(f"Step 4): Removing all offline hosts from DC2 bucket ({dc2_name})")
        log.info("=" * 80)
        serviceability_methods = ServiceabilityMethods(cluster=ceph_cluster, **config)
        for host in dc2_hosts:
            try:
                log.info(f"Waiting for host {host} to reach Offline status...")
                wait_till_host_status_reaches(
                    rados_obj=rados_obj,
                    hostnames=[host],
                    status="Offline",
                )
                log.info(f"Host {host} is now Offline. Removing from cluster...")
                serviceability_methods.remove_offline_host(
                    host_node_name=host, rm_crush_entry=True
                )
                log.info(f"Successfully removed offline host: {host}")
            except Exception as err:
                log.error(f"Failed to remove host {host}: {err}")
                raise Exception(f"Failed to remove host {host}: {err}")
        log.info(f"All offline hosts from DC2 bucket ({dc2_name}) have been removed")
        log.info("Step 4) completed successfully")
        log.info("")

        # Step 5: Remove the DC2 bucket
        log.info("=" * 80)
        log.info(f"Step 5): Removing DC2 bucket ({dc2_name}) from CRUSH map")
        log.info("=" * 80)
        cmd = f"ceph osd crush remove {dc2_name}"
        try:
            log.info(f"Executing command: {cmd}")
            rados_obj.run_ceph_command(cmd=cmd, client_exec=True)
            log.info(f"Successfully removed bucket {dc2_name} from CRUSH map")
        except Exception as err:
            log.error(f"Failed to remove bucket {dc2_name}: {err}")
            raise Exception(f"Failed to remove bucket {dc2_name}: {err}")
        log.info("Step 5) completed successfully")
        log.info("")

        tiebreaker_bucket_name = "tiebreaker"
        tiebreaker_host_name = dc2_hosts[0]
        log.info(f"Tiebreaker host selected: {tiebreaker_host_name}")

        # Step 6: Restart hosts from DC2
        log.info("=" * 80)
        log.info("Step 6): Restarting hosts from DC2")
        log.info("=" * 80)
        log.info(f"Powering on hosts from DC2: {dc2_hosts}")
        for host in dc2_hosts:
            log.info(f"Powering on host: {host}")
            target_node = find_vm_node_by_hostname(ceph_cluster, host)
            target_node.power_on()
            log.info(f"Successfully restarted host: {host}")
        log.info("Step 6) completed successfully")
        log.info("")

        # Step 7: Remove the ceph cluster from the restarted hosts using rm-cluster command
        log.info("=" * 80)
        log.info(
            f"Step 7): Removing ceph cluster from restarted hosts {dc2_hosts} using rm-cluster command"
        )
        log.info("=" * 80)
        log.info("Retrieving cluster FSID...")
        fsid = rados_obj.run_ceph_command(cmd="ceph fsid", client_exec=True)["fsid"]
        log.info(f"Cluster FSID: {fsid}")
        for host in dc2_hosts:
            log.info(f"Removing cluster from host {host}...")
            host_obj = get_host_obj_from_hostname(hostname=host, rados_obj=rados_obj)
            cmd = f"cephadm rm-cluster --force --zap-osds --fsid {fsid}"
            try:
                host_obj.exec_command(cmd=cmd, sudo=True)
                log.info(f"Successfully removed cluster from host {host}")
            except Exception as err:
                log.error(f"Failed to remove cluster from host {host}: {err}")
                raise Exception(f"Failed to remove cluster from host {host}: {err}")
        log.info("Step 7) completed successfully")
        log.info("")

        # Step 8: Add the first restarted host back to the cluster
        # Which will act as tiebreaker
        log.info("=" * 80)
        log.info(
            f"Step 8): Adding the first restarted host {dc2_hosts[0]} back to the cluster"
        )
        log.info("=" * 80)
        try:
            log.info(f"Adding host {dc2_hosts[0]} back to cluster...")
            cmd = f"ceph orch host add {dc2_hosts[0]} '' mon mgr"
            client_node.exec_command(cmd=cmd, pretty_print=True)
            log.info(f"Successfully added host {dc2_hosts[0]} back to the cluster")
        except Exception as err:
            log.error(f"Failed to add host {dc2_hosts[0]} back to the cluster: {err}")
            raise Exception(
                f"Failed to add host {dc2_hosts[0]} back to the cluster: {err}"
            )

        # wait till daemon is in running state
        for _ in range(3):
            try:
                if wait_for_daemon_status(rados_obj, "mon", dc2_hosts[0], "running"):
                    log.info(
                        f"Mon daemon is in running state after addition {dc2_hosts[0]}"
                    )
                    break
            except Exception:
                log.info(
                    f"Sleeping for 20s before rechecking mon status {dc2_hosts[0]}"
                )
            time.sleep(20)
        else:
            raise Exception(
                f"Mon daemon not in running state after addition in {dc2_hosts[0]}"
            )
        log.info("Step 8) completed successfully")
        log.info("")

        # Step 9: Enter 2-site stretch mode with DC1, DC3 and tiebreaker
        log.info("=" * 80)
        log.info(
            f"Step 9): Entering 2-site stretch mode with DC1 ({dc1_name}), DC3 ({dc3_name}) and tiebreaker"
        )
        log.info("=" * 80)

        revert_stretch_mode_scenarios = RevertStretchModeFunctionalities(**config)

        log.info(f"Using tiebreaker monitor: {tiebreaker_host_name}")
        log.info("Setting up CRUSH rule with no affinity...")
        if not setup_crush_rule_with_no_affinity(
            node=client_node,
            rule_name="stretch_rule",
        ):
            log.error("Failed to Add crush rules in the crush map")
            raise Exception("Stretch mode deployment Failed")
        log.info("Successfully created CRUSH rule: stretch_rule")

        log.info(f"Setting mon location for {tiebreaker_host_name} as tiebreaker...")
        cmd = f"ceph mon set_location {tiebreaker_host_name} {stretch_bucket}={tiebreaker_bucket_name}"
        try:
            rados_obj.run_ceph_command(cmd=cmd, client_exec=True)
            log.info("Successfully added mon location as tiebreaker")
        except Exception as err:
            log.error(f"Failed to add mon location as tiebreaker: {err}")
            raise Exception(f"Failed to add mon location as tiebreaker: {err}")

        # Enable 2-site stretch mode
        # set election strategy to connectivity
        mon_election_obj.set_election_strategy(mode="connectivity")

        log.info("Enabling 2-site stretch mode...")
        revert_stretch_mode_scenarios.enable_stretch_mode(
            tiebreaker_host_name, "stretch_rule"
        )
        log.info("Successfully enabled 2-site stretch mode")

        # Wait for PGs to be clean
        log.info(
            "Waiting for PGs to reach active+clean state after enabling stretch mode..."
        )
        if not wait_for_clean_pg_sets(rados_obj=rados_obj):
            log.warning("PGs not in clean state after enabling 2-site stretch mode")
        else:
            log.info("All PGs are in active+clean state")
        log.info("Step 9) completed successfully")
        log.info("")

        # Step 10: Simulate datacenter DC3 failure
        log.info("=" * 80)
        log.info(f"Step 10): Simulating datacenter {dc3_name} failure")
        log.info("=" * 80)
        log.info(f"Shutting down all hosts in datacenter {dc3_name}...")
        for host in dc3_hosts:
            log.info(f"Shutting down host: {host}")
            target_node = find_vm_node_by_hostname(ceph_cluster, host)
            target_node.shutdown(wait=True)
            log.info(f"Host {host} has been shut down successfully")

        log.info(f"Completed shutdown of all hosts in datacenter {dc3_name}")
        log.info("Waiting for cluster to stabilize...")
        time.sleep(10)
        log.info("Step 10) completed successfully")
        log.info("")

        # Step 11: Revert 2-site stretch mode back to non-stretch cluster
        log.info("=" * 80)
        log.info("Step 11): Reverting 2-site stretch mode back to non-stretch cluster")
        log.info("=" * 80)
        log.info(
            "Using command: ceph mon disable_stretch_mode stretch_rule --yes-i-really-mean-it"
        )

        # Revert stretch mode
        log.info("Reverting stretch mode...")
        revert_stretch_mode_scenarios.revert_stretch_mode()
        log.info("Successfully reverted 2-site stretch mode to non-stretch cluster")

        log.info(f"Waiting for hosts in DC3 ({dc3_name}) to reach Offline status...")
        wait_till_host_status_reaches(
            rados_obj=rados_obj,
            hostnames=dc3_hosts,
            status="Offline",
        )
        log.info(f"All hosts in DC3 ({dc3_name}) are now Offline")
        log.info("Step 11) completed successfully")
        log.info("")

        # Step 12: Remove all offline hosts from DC3 bucket
        log.info("=" * 80)
        log.info(f"Step 12): Removing all offline hosts from DC3 bucket ({dc3_name})")
        log.info("=" * 80)
        serviceability_methods = ServiceabilityMethods(cluster=ceph_cluster, **config)
        for host in dc3_hosts:
            try:
                log.info(f"Removing offline host: {host}")
                serviceability_methods.remove_offline_host(
                    host_node_name=host, rm_crush_entry=True
                )
                log.info(f"Successfully removed offline host: {host}")
            except Exception as err:
                log.error(f"Failed to remove host {host}: {err}")
                raise Exception(f"Failed to remove host {host}: {err}")
        log.info(f"All offline hosts from DC3 bucket ({dc3_name}) have been removed")
        log.info("Step 12) completed successfully")
        log.info("")

        # Wait for PGs to be clean
        log.info(
            "Waiting for PGs to reach active+clean state after reverting stretch mode..."
        )
        if not wait_for_clean_pg_sets(rados_obj=rados_obj):
            log.warning("PGs not in clean state after reverting stretch mode")
        else:
            log.info("All PGs are in active+clean state")

        log.info("")
        log.info("=" * 80)
        log.info("All test steps completed successfully!")
        log.info("=" * 80)

    except Exception as e:
        log.error("=" * 80)
        log.error("Test execution failed with exception")
        log.error("=" * 80)
        log.error(f"Exception type: {type(e).__name__}")
        log.error(f"Exception message: {str(e)}")
        if e.__doc__:
            log.error(f"Exception documentation: {e.__doc__}")
        log.exception(e)
        log.error("=" * 80)

        # log cluster health
        log.info("Logging cluster health status after failure...")
        rados_obj.log_cluster_health()
        return 1

    finally:
        log.info("")
        log.info("=" * 80)
        log.info("Execution of finally block begins here")
        log.info("=" * 80)

        if config.get("delete_pool") and "pool_name" in config:
            log.info(f"Cleaning up pool: {config['pool_name']}")
            try:
                rados_obj.delete_pool(pool=config["pool_name"])
                log.info(f"Successfully deleted pool: {config['pool_name']}")
            except Exception as pool_cleanup_err:
                log.warning(
                    f"Failed to delete pool {config['pool_name']}: {pool_cleanup_err}"
                )

        # log cluster health
        log.info("Logging final cluster health status...")
        rados_obj.log_cluster_health()

        # check for crashes after test execution
        log.info("Checking for crashes after test execution...")
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
        else:
            log.info("No crashes detected after test execution")

        log.info("=" * 80)
        log.info("Finally block execution completed")
        log.info("=" * 80)

    log.info("")
    log.info("=" * 80)
    log.info("All the tests completed on the cluster, Pass!!!")
    log.info("=" * 80)
    return 0
