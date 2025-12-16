"""
This test module is used to test mon and mgr serviceability scenarios for 3 AZ stretch cluster

includes:
    1. Add & Remove a mon in DC1
    2. Add & Remove a mgr in DC1
    3. Add & Remove a mon from all DCs
    4. Add & Remove a mgr from all DCs
    5. Remove all mons from 1 DC
    6. Remove all mgrs from 1 DC
    7. Add mon daemons via spec file
"""

import random
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.mgr_workflows import MgrWorkflows
from ceph.rados.monitor_workflows import MonitorWorkflows
from ceph.rados.utils import get_cluster_timestamp
from tests.rados.rados_test_util import wait_for_daemon_status
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    This test module is used to test mon and mgr serviceability scenarios for 3 AZ stretch cluster

    includes:
        1. Add & Remove a mon in DC1
        2. Add & Remove a mgr in DC1
        3. Add & Remove a mon from all DCs
        4. Add & Remove a mgr from all DCs
        5. Remove all mons from 1 DC
        6. Remove all mgrs from 1 DC
        7. Add mon daemons via spec file
    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster
    """

    log.info(run.__doc__)
    config = kw.get("config")
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonitorWorkflows(node=cephadm)
    mgr_obj = MgrWorkflows(node=cephadm)
    client_node = ceph_cluster.get_nodes(role="client")[0]
    stretch_bucket = config.get("stretch_bucket", "datacenter")
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
    start_time = get_cluster_timestamp(rados_obj.node)
    log.debug(f"Test workflow started. Start time: {start_time}")
    try:
        # Get datacenter information
        osd_tree_cmd = "ceph osd tree"
        buckets = rados_obj.run_ceph_command(osd_tree_cmd)
        dc_buckets = [d for d in buckets["nodes"] if d.get("type") == stretch_bucket]
        dc_names = [name["name"] for name in dc_buckets]
        pool_name = "test_stretch_mon_mgr_serviceability_pool"

        log.info(f"Datacenters found in cluster: {dc_names}")

        if not rados_obj.run_pool_sanity_check():
            log.error(
                "Cluster PGs not in active + clean state before starting the tests"
            )
            raise Exception("Pre-execution checks failed on the cluster")

        # log cluster health
        rados_obj.log_cluster_health()

        # Get hosts by datacenter
        all_hosts = rados_obj.get_multi_az_stretch_site_hosts(
            num_data_sites=len(dc_names), stretch_bucket=stretch_bucket
        )
        for site in dc_names:
            log.info(
                f"Hosts present in Datacenter : {site} : {getattr(all_hosts, site)}"
            )

        # Get all cluster nodes
        cluster_nodes = ceph_cluster.get_nodes(ignore="client")

        # generate hostname -> node object map
        hostname_to_node_map = {}
        for dc in dc_names:
            for hostname in getattr(all_hosts, dc):
                for node in cluster_nodes:
                    if node.hostname == hostname:
                        hostname_to_node_map[hostname] = node
                        break

        # Setting the mon service as unmanaged
        if not mon_obj.set_mon_service_managed_type(unmanaged=True):
            log.error("Could not set the mon service to unmanaged")
            raise Exception("mon service not unmanaged error")

        assert rados_obj.create_pool(pool_name=pool_name)

        # Scenario 1: Add & Remove a mon in DC1
        if "scenario-1" in scenarios_to_run:
            log.info("\n" + "=" * 50)
            log.info("Scenario 1: Add & Remove a mon in DC1")
            log.info("=" * 50)

            target_dc = dc_names[0]  # DC1
            dc1_hosts = getattr(all_hosts, target_dc)
            log.info(f"Target DC: {target_dc}, Hosts: {dc1_hosts}")

            # Find a host in DC1 that has a mon
            target_host = None
            for hostname in dc1_hosts:
                if rados_obj.check_daemon_exists_on_host(
                    host=hostname, daemon_type="mon"
                ):
                    target_host = hostname_to_node_map[hostname]
                    break

            if target_host is None:
                log.error(f"No mon found in {target_dc}")
                raise Exception("Scenario 1: No mon found in target DC")

            client_node.exec_command(
                cmd="ceph orch ps --daemon-type mon", pretty_print=True, verbose=True
            )

            log.info(f"Selected host: {target_host.hostname}")

            # Remove mon
            log.info(f"Removing mon from {target_host.hostname}")
            if not mon_obj.remove_mon_service(host=target_host.hostname):
                log.error(f"Failed to remove mon from {target_host.hostname}")
                raise Exception("Scenario 1: Failed to remove mon")

            client_node.exec_command(
                cmd="ceph orch ps --daemon-type mon", pretty_print=True, verbose=True
            )

            if not rados_obj.bench_write(
                pool_name=pool_name, byte_size="5M", max_objs=100
            ):
                log.error(
                    "Failed to write objects into Pool-1, with compression enabled"
                )
                return 1

            # Add mon back
            log.info(f"Adding mon to {target_host.hostname}")
            if not mon_obj.add_mon_service(
                host=target_host,
                location_type=stretch_bucket,
                location_name=target_dc,
            ):
                log.error(f"Failed to add mon to {target_host.hostname}")
                raise Exception("Scenario 1: Failed to add mon")

            # wait till daemon is in running state
            for _ in range(3):
                try:
                    if wait_for_daemon_status(
                        rados_obj, "mon", target_host.hostname, "running"
                    ):
                        log.info("Mon daemon is in running state after addition")
                        break
                except Exception:
                    log.info("Sleeping for 60s before rechecking mon status")
                time.sleep(20)
            else:
                raise Exception(
                    "Scenario 1: Mon daemon not in running state after addition"
                )

            # Verify mon is in quorum
            quorum = mon_obj.get_mon_quorum_hosts()
            if target_host.hostname not in quorum:
                log.error(
                    f"Mon on {target_host.hostname} is not in quorum after addition"
                )
                raise Exception("Scenario 1: Mon not in quorum after addition")

            client_node.exec_command(
                cmd="ceph orch ps --daemon-type mon", pretty_print=True, verbose=True
            )

            log.info("Scenario 1 completed successfully")

        # Scenario 2: Add & Remove a mgr in DC1
        if "scenario-2" in scenarios_to_run:
            log.info("\n" + "=" * 50)
            log.info("Scenario 2: Add & Remove a mgr in DC1")
            log.info("=" * 50)

            target_dc = dc_names[0]  # DC1
            dc1_hosts = getattr(all_hosts, target_dc)
            log.info(f"Target DC: {target_dc}, Hosts: {dc1_hosts}")

            # Find a host in DC1 that has a mgr
            target_host = None
            for hostname in dc1_hosts:
                if rados_obj.check_daemon_exists_on_host(
                    host=hostname, daemon_type="mgr"
                ):
                    target_host = hostname
                    break

            if target_host is None:
                log.error(f"No mgr found in {target_dc}")
                raise Exception("Scenario 2: No mgr found in target DC")

            client_node.exec_command(
                cmd="ceph orch ps --daemon-type mgr", pretty_print=True, verbose=True
            )

            log.info(f"Selected host: {target_host}")

            # Remove mgr
            log.info(f"Removing mgr from {target_host}")
            if not mgr_obj.remove_mgr_with_label(hostname=target_host):
                log.error(f"Failed to remove mgr from {target_host}")
                raise Exception("Scenario 2: Failed to remove mgr")

            client_node.exec_command(
                cmd="ceph orch ps --daemon-type mgr", pretty_print=True, verbose=True
            )

            # Add mgr back
            log.info(f"Adding mgr to {target_host}")
            if not mgr_obj.add_mgr_with_label(hostname=target_host):
                log.error(f"Failed to add mgr to {target_host}")
                raise Exception("Scenario 2: Failed to add mgr")

            # Verify mgr exists
            if not rados_obj.check_daemon_exists_on_host(
                host=target_host, daemon_type="mgr"
            ):
                log.error(f"Mgr on {target_host} does not exist after addition")
                raise Exception("Scenario 2: Mgr not found after addition")

            # get mgr daemon id
            out = rados_obj.run_ceph_command(cmd="ceph orch ps --daemon-type mgr")
            mgr_daemon_id = None
            for mgr_detail in out:
                if target_host == mgr_detail.get("hostname", None):
                    mgr_daemon_id = mgr_detail["daemon_id"]
                    break

            if mgr_daemon_id is None:
                log.error(
                    f"Scenario 2: Could not find mgr daemon id for host {target_host}"
                )
                raise

            # wait till daemon is in running state
            for _ in range(3):
                try:
                    if wait_for_daemon_status(
                        rados_obj, "mgr", mgr_daemon_id, "running"
                    ):
                        log.info("Mgr daemon is in running state after addition")
                        break
                except Exception:
                    log.info("Sleeping for 60s before rechecking mgr status")
                time.sleep(20)
            else:
                raise Exception(
                    "Scenario 2: Mgr daemon not in running state after addition"
                )

            client_node.exec_command(
                cmd="ceph orch ps --daemon-type mgr", pretty_print=True, verbose=True
            )

            log.info("Scenario 2 completed successfully")

        # Scenario 3: Add & Remove a mon from all DCs
        if "scenario-3" in scenarios_to_run:
            log.info("\n" + "=" * 50)
            log.info("Scenario 3: Add & Remove a mon from all DCs")
            log.info("=" * 50)

            removed_mons = []

            client_node.exec_command(
                cmd="ceph orch ps --daemon-type mon", pretty_print=True, verbose=True
            )

            for target_dc in dc_names:
                dc_hosts = getattr(all_hosts, target_dc)
                log.info(f"Processing DC: {target_dc}, Hosts: {dc_hosts}")

                # Find a host with mon in this DC
                target_host = None
                for hostname in dc_hosts:
                    if mon_obj.check_mon_exists_on_host(host=hostname):
                        target_host = hostname_to_node_map[hostname]
                        break

                if target_host is None:
                    log.warning(f"No mon found in {target_dc}")
                    raise Exception(f"Scenario 3: No mon found in {target_dc}")

                log.info(f"Removing mon from {target_host.hostname} in {target_dc}")
                if not mon_obj.remove_mon_service(host=target_host.hostname):
                    log.error(f"Failed to remove mon from {target_host.hostname}")
                    raise Exception(f"Scenario 3: Failed to remove mon in {target_dc}")
                removed_mons.append((target_host, target_dc))

            client_node.exec_command(
                cmd="ceph orch ps --daemon-type mon", pretty_print=True, verbose=True
            )

            if not rados_obj.bench_write(
                pool_name=pool_name, byte_size="5M", max_objs=100
            ):
                log.error(
                    "Failed to write objects into Pool-1, with compression enabled"
                )
                return 1

            # Add mons back
            for target_host, target_dc in removed_mons:
                log.info(f"Adding mon to {target_host.hostname} in {target_dc}")
                if not mon_obj.add_mon_service(
                    host=target_host,
                    location_type=stretch_bucket,
                    location_name=target_dc,
                ):
                    log.error(f"Failed to add mon to {target_host.hostname}")
                    raise Exception(f"Scenario 3: Failed to add mon in {target_dc}")

            client_node.exec_command(
                cmd="ceph orch ps --daemon-type mon", pretty_print=True, verbose=True
            )

            # Verify all mons are in quorum
            quorum = mon_obj.get_mon_quorum_hosts()
            for target_host, target_dc in removed_mons:
                # wait till daemon is in running state
                for _ in range(3):
                    try:
                        if wait_for_daemon_status(
                            rados_obj, "mon", target_host.hostname, "running"
                        ):
                            log.info("Mon daemon is in running state after addition")
                            break
                    except Exception:
                        log.info("Sleeping for 60s before rechecking mon status")
                    time.sleep(20)
                else:
                    raise Exception(
                        "Scenario 3: Mon daemon not in running state after addition"
                    )

                if target_host.hostname not in quorum:
                    log.error(
                        f"Mon on {target_host.hostname} is not in quorum after addition"
                    )
                    raise Exception(
                        f"Scenario 3: Mon not in quorum after addition in {target_dc}"
                    )

            client_node.exec_command(
                cmd="ceph orch ps --daemon-type mon", pretty_print=True, verbose=True
            )

            log.info("Scenario 3 completed successfully")

        # Scenario 4: Add & Remove a mgr from all DCs
        if "scenario-4" in scenarios_to_run:
            log.info("\n" + "=" * 50)
            log.info("Scenario 4: Add & Remove a mgr from all DCs")
            log.info("=" * 50)

            removed_mgrs = []

            client_node.exec_command(
                cmd="ceph orch ps --daemon-type mgr", pretty_print=True, verbose=True
            )

            for target_dc in dc_names:
                dc_hosts = getattr(all_hosts, target_dc)
                log.info(f"Processing DC: {target_dc}, Hosts: {dc_hosts}")

                # Find a host with mgr in this DC
                target_host = None
                target_hostname = None
                for hostname in dc_hosts:
                    if rados_obj.check_daemon_exists_on_host(
                        host=hostname, daemon_type="mgr"
                    ):
                        target_host = hostname_to_node_map[hostname]
                        target_hostname = hostname
                        break

                if target_host is None:
                    log.warning(f"No mgr found in {target_dc}, skipping")
                    raise Exception(f"Scenario 4: No mgr found in {target_dc}")

                log.info(f"Removing mgr from {target_hostname} in {target_dc}")
                if not mgr_obj.remove_mgr_with_label(hostname=target_hostname):
                    log.error(f"Failed to remove mgr from {target_hostname}")
                    raise Exception(f"Scenario 4: Failed to remove mgr in {target_dc}")

                removed_mgrs.append((target_host, target_dc))

            # Add mgrs back
            for target_host, target_dc in removed_mgrs:
                log.info(f"Adding mgr to {target_host} in {target_dc}")
                if not mgr_obj.add_mgr_with_label(hostname=target_host.hostname):
                    log.error(f"Failed to add mgr to {target_host}")
                    raise Exception(f"Scenario 4: Failed to add mgr in {target_dc}")

            client_node.exec_command(
                cmd="ceph orch ps --daemon-type mgr", pretty_print=True, verbose=True
            )

            # Verify all mgrs exist
            for target_host, target_dc in removed_mgrs:
                # get mgr daemon id
                out = rados_obj.run_ceph_command(cmd="ceph orch ps --daemon-type mgr")
                mgr_daemon_id = None
                for mgr_detail in out:
                    if target_host.hostname == mgr_detail["hostname"]:
                        mgr_daemon_id = mgr_detail["daemon_id"]
                        break

                if mgr_daemon_id is None:
                    log.error(
                        f"Scenario 2: Could not find mgr daemon id for host {target_host}"
                    )
                    raise

                # wait till daemon is in running state
                for _ in range(3):
                    try:
                        if wait_for_daemon_status(
                            rados_obj, "mgr", mgr_daemon_id, "running"
                        ):
                            log.info("Mgr daemon is in running state after addition")
                            break
                    except Exception:
                        log.info("Sleeping for 60s before rechecking mgr status")
                    time.sleep(20)
                else:
                    raise Exception(
                        "Scenario 4: Mgr daemon not in running state after addition"
                    )

                if not rados_obj.check_daemon_exists_on_host(
                    host=target_host.hostname, daemon_type="mgr"
                ):
                    log.error(f"Mgr on {target_host} does not exist after addition")
                    raise Exception(
                        f"Scenario 4: Mgr not found after addition in {target_dc}"
                    )

            client_node.exec_command(
                cmd="ceph orch ps --daemon-type mgr", pretty_print=True, verbose=True
            )

            log.info("Scenario 4 completed successfully")

        # Scenario 5: Remove all mons from 1 DC
        if "scenario-5" in scenarios_to_run:
            log.info("\n" + "=" * 50)
            log.info("Scenario 5: Remove all mons from 1 DC")
            log.info("=" * 50)

            target_dc = random.choice(dc_names)
            dc_hosts = getattr(all_hosts, target_dc)
            log.info(f"Target DC: {target_dc}, Hosts: {dc_hosts}")

            client_node.exec_command(
                cmd="ceph orch ps --daemon-type mon", pretty_print=True, verbose=True
            )

            # Find all mons in this DC
            mons_to_remove = []
            for hostname in dc_hosts:
                if mon_obj.check_mon_exists_on_host(host=hostname):
                    mons_to_remove.append(hostname_to_node_map[hostname])

            if not mons_to_remove:
                log.warning(f"No mons found in {target_dc}, skipping scenario-5")
                raise Exception(f"Scenario 5: No mons found in {target_dc}")

            log.info(f"Found {len(mons_to_remove)} mons to remove in {target_dc}")

            # Remove all mons
            for mon_host in mons_to_remove:
                log.info(f"Removing mon from {mon_host.hostname}")
                if not mon_obj.remove_mon_service(host=mon_host.hostname):
                    log.error(f"Failed to remove mon from {mon_host.hostname}")
                    raise Exception("Scenario 5: Failed to remove mon")

            client_node.exec_command(
                cmd="ceph orch ps --daemon-type mon", pretty_print=True, verbose=True
            )

            # Verify mons are removed
            quorum = mon_obj.get_mon_quorum_hosts()
            for mon_host in mons_to_remove:
                if mon_host.hostname in quorum:
                    log.error(
                        f"Mon on {mon_host.hostname} is still in quorum after removal"
                    )
                    raise Exception("Scenario 5: Mon still in quorum after removal")

                if mon_obj.check_mon_exists_on_host(host=mon_host.hostname):
                    log.error(f"Mon on {mon_host.hostname} still exists after removal")
                    raise Exception(
                        f"Scenario 5: Mon still exists after removal in {target_dc}"
                    )

            if not rados_obj.bench_write(
                pool_name=pool_name, byte_size="5M", max_objs=100
            ):
                log.error(
                    "Failed to write objects into Pool-1, with compression enabled"
                )
                return 1

            # Add mons back
            for mon_host in mons_to_remove:
                log.info(f"Adding mon back to {mon_host.hostname}")
                if not mon_obj.add_mon_service(
                    host=mon_host,
                    location_type=stretch_bucket,
                    location_name=target_dc,
                ):
                    log.error(f"Failed to add mon to {mon_host.hostname}")
                    raise Exception("Scenario 5: Failed to add mon back")

            client_node.exec_command(
                cmd="ceph orch ps --daemon-type mon", pretty_print=True, verbose=True
            )

            # Verify mons are back in quorum
            quorum = mon_obj.get_mon_quorum_hosts()
            for mon_host in mons_to_remove:
                # wait till daemon is in running state
                for _ in range(3):
                    try:
                        if wait_for_daemon_status(
                            rados_obj, "mon", mon_host.hostname, "running"
                        ):
                            log.info("Mon daemon is in running state after addition")
                            break
                    except Exception:
                        log.info("Sleeping for 60s before rechecking mon status")
                    time.sleep(20)
                else:
                    raise Exception(
                        "Scenario 5: Mon daemon not in running state after addition"
                    )

                if mon_host.hostname not in quorum:
                    log.error(
                        f"Mon on {mon_host.hostname} is not in quorum after re-addition"
                    )
                    raise Exception("Scenario 5: Mon not in quorum after re-addition")

            client_node.exec_command(
                cmd="ceph orch ps --daemon-type mon", pretty_print=True, verbose=True
            )

            log.info("Scenario 5 completed successfully")

        # Scenario 6: Remove all mgrs from 1 DC
        if "scenario-6" in scenarios_to_run:
            log.info("\n" + "=" * 50)
            log.info("Scenario 6: Remove all mgrs from 1 DC")
            log.info("=" * 50)

            target_dc = random.choice(dc_names)
            dc_hosts = getattr(all_hosts, target_dc)
            log.info(f"Target DC: {target_dc}, Hosts: {dc_hosts}")

            client_node.exec_command(
                cmd="ceph orch ps --daemon-type mgr", pretty_print=True, verbose=True
            )

            # Find all mgrs in this DC
            mgrs_to_remove = []
            for hostname in dc_hosts:
                if rados_obj.check_daemon_exists_on_host(
                    host=hostname, daemon_type="mgr"
                ):
                    mgrs_to_remove.append(hostname_to_node_map[hostname])

            if not mgrs_to_remove:
                log.warning(f"No mgrs found in {target_dc}, skipping scenario-6")
                raise Exception(f"Scenario 6: No mgrs found in {target_dc}")

            log.info(f"Found {len(mgrs_to_remove)} mgrs to remove in {target_dc}")

            # Remove all mgrs
            for mgr_host in mgrs_to_remove:
                log.info(f"Removing mgr from {mgr_host.hostname}")
                if not mgr_obj.remove_mgr_with_label(hostname=mgr_host.hostname):
                    log.error(f"Failed to remove mgr from {mgr_host.hostname}")
                    raise Exception("Scenario 6: Failed to remove mgr")

            client_node.exec_command(
                cmd="ceph orch ps --daemon-type mgr", pretty_print=True, verbose=True
            )

            # Verify mgrs are removed
            for mgr_host in mgrs_to_remove:
                if rados_obj.check_daemon_exists_on_host(
                    host=mgr_host.hostname, daemon_type="mgr"
                ):
                    log.error(f"Mgr on {mgr_host.hostname} still exists after removal")
                    raise Exception("Scenario 6: Mgr still exists after removal")

            # Add mgrs back
            for mgr_host in mgrs_to_remove:
                log.info(f"Adding mgr back to {mgr_host.hostname}")
                if not mgr_obj.add_mgr_with_label(hostname=mgr_host.hostname):
                    log.error(f"Failed to add mgr to {mgr_host.hostname}")
                    raise Exception("Scenario 6: Failed to add mgr back")

            rados_obj.run_ceph_command(
                cmd="ceph orch ps --daemon-type mgr", print_output=True
            )

            # Verify mgrs are back
            for mgr_host in mgrs_to_remove:
                # get mgr daemon id
                out = rados_obj.run_ceph_command(cmd="ceph orch ps --daemon-type mgr")
                mgr_daemon_id = None
                for mgr_detail in out:
                    if mgr_host.hostname == mgr_detail["hostname"]:
                        mgr_daemon_id = mgr_detail["daemon_id"]
                        break

                if mgr_daemon_id is None:
                    log.error(
                        f"Scenario 2: Could not find mgr daemon id for host {mgr_host.hostname}"
                    )
                    raise

                # wait till daemon is in running state
                for _ in range(3):
                    try:
                        if wait_for_daemon_status(
                            rados_obj, "mgr", mgr_daemon_id, "running"
                        ):
                            log.info("Mgr daemon is in running state after addition")
                            break
                    except Exception:
                        log.info("Sleeping for 60s before rechecking mgr status")
                    time.sleep(20)
                else:
                    raise Exception(
                        "Scenario 6: Mgr daemon not in running state after addition"
                    )

                if not rados_obj.check_daemon_exists_on_host(
                    host=mgr_host.hostname, daemon_type="mgr"
                ):
                    log.error(
                        f"Mgr on {mgr_host.hostname} does not exist after re-addition"
                    )
                    raise Exception("Scenario 6: Mgr not found after re-addition")

        log.info("Scenario 6 completed successfully")

        # Scenario 7: Perform addition of mon daemons via spec file
        if "scenario-7" in scenarios_to_run:
            log.info("\n" + "=" * 50)
            log.info("Scenario 7: Add mon daemons via spec file")
            log.info("=" * 50)

            client_node.exec_command(
                cmd="ceph orch ps --daemon-type mon", pretty_print=True, verbose=True
            )

            # First, find a host without mon to add via spec
            target_dc = dc_names[0]  # DC1
            dc1_hosts = getattr(all_hosts, target_dc)
            target_host = None

            for hostname in dc1_hosts:
                if not mon_obj.check_mon_exists_on_host(host=hostname):
                    target_host = hostname_to_node_map[hostname]
                    break

            if target_host is None:
                log.info(
                    "All hosts in DC1 have mons. Removing one first to test spec file addition"
                )
                # Find a host with mon to remove first
                hostname = random.choice(dc1_hosts)
                target_host = hostname_to_node_map[hostname]

                if not mon_obj.remove_mon_service(host=target_host.hostname):
                    log.error(f"Failed to remove mon from {target_host.hostname}")
                    raise Exception("Scenario 7: Failed to remove mon")

                if mon_obj.check_mon_exists_on_host(host=target_host.hostname):
                    log.error(
                        f"Mon on {target_host.hostname} still exists after removal"
                    )
                    raise Exception(
                        f"Scenario 7: Mon still exists after removal in {target_dc}"
                    )

            client_node.exec_command(
                cmd="ceph orch ps --daemon-type mon", pretty_print=True, verbose=True
            )

            log.info(f"Adding mon to {target_host.hostname} via spec file")
            try:
                cmd = f"ceph orch host label add {target_host.hostname} mon"
                client_node.exec_command(cmd=cmd, pretty_print=True)

                # Setting the mon service as managed
                if not mon_obj.set_mon_service_managed_type(unmanaged=False):
                    log.error("Could not set the mon service to managed")
                    raise Exception("Scenario 7: mon service not managed error")

                # wait till daemon is in running state
                for _ in range(5):
                    try:
                        if wait_for_daemon_status(
                            rados_obj, "mon", target_host.hostname, "running"
                        ):
                            log.info("Mon daemon is in running state after addition")
                            break
                    except Exception:
                        log.info("Sleeping for 60s before rechecking mon status")
                    time.sleep(60)
                else:
                    raise Exception(
                        "Scenario 7: Mon daemon not in running state after addition"
                    )

                # Verify mon was added
                if not mon_obj.check_mon_exists_on_host(host=target_host.hostname):
                    log.error(
                        f"Mon on {target_host.hostname} was not added via spec file"
                    )
                    raise Exception("Scenario 7: Mon not added via spec file")

                client_node.exec_command(
                    cmd="ceph orch ps --daemon-type mon",
                    pretty_print=True,
                    verbose=True,
                )

                # Verify mon is in quorum
                quorum = mon_obj.get_mon_quorum_hosts()
                if target_host.hostname not in quorum:
                    log.error(
                        f"Mon on {target_host.hostname} is not in quorum after spec file addition"
                    )
                    raise Exception(
                        "Scenario 7: Mon not in quorum after spec file addition"
                    )

                log.info("Mon added successfully via spec file")
            except Exception as e:
                log.error(f"Failed to add mon via spec file: {e}")
                raise Exception(f"Scenario 7: Failed to add mon via spec file: {e}")

            log.info("Scenario 7 completed successfully")

        # Final health check
        if not rados_obj.run_pool_sanity_check():
            log.error("Cluster PGs not in active + clean state after all scenarios")
            raise Exception("Post-execution checks failed on the cluster")

        log.info("All scenarios completed successfully")

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

        # Setting the mon service as managed
        if not mon_obj.set_mon_service_managed_type(unmanaged=False):
            log.error("Could not set the mon service to managed")
            raise Exception("mon service not managed error")

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
