"""
Module to perform tier-4 tests on pools WRT OSD daemons.

1. Reboot of single osd and health check
2. Rolling Reboot OSD hosts
3. Stopping and starting OSD daemons
4. restart all OSD daemons belonging to single pg
5. Removal and addition of OSD daemons
6. Removal and addition of OSD Hosts
7. Replacement of a failed OSD host

"""

import datetime
import random
import time

import yaml

from ceph.ceph_admin import CephAdmin
from ceph.rados import utils
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.monitor_workflows import MonitorWorkflows
from ceph.rados.serviceability_workflows import ServiceabilityMethods
from ceph.utils import get_node_by_id
from tests.rados.rados_test_util import get_device_path, wait_for_device_rados
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from tests.rados.test_data_migration_bw_pools import create_given_pool
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw) -> int:
    """
    Test to perform tier-4 tests on pools related to OSD daemons
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    service_obj = ServiceabilityMethods(cluster=ceph_cluster, **config)
    pool_configs = config.get("pool_configs")
    pool_configs_path = config.get("pool_configs_path")
    osd_nodes = ceph_cluster.get_nodes(role="osd")
    installer_node = ceph_cluster.get_nodes(role="installer")[0]

    if config.get("pool_configs_path"):
        with open(pool_configs_path, "r") as fd:
            pool_conf_file = yaml.safe_load(fd)

    if config.get("pool_configs"):
        pools = []
        for i in pool_configs:
            pool = pool_conf_file[i["type"]][i["conf"]]
            create_given_pool(rados_obj, pool)
            pools.append(pool["pool_name"])
        log.info(f"Created {len(pools)} pools for testing. pools : {pools}")

        # Checking cluster health before starting the tests
        method_should_succeed(rados_obj.run_pool_sanity_check)
        log.info(
            "Completed health check of the cluster after test pool creation. Cluster health good!!"
        )

    log.info("\n\n---- Starting All workflows ----\n\n")
    if not config.get("osd_host_fail", False):
        # rebooting one OSD and checking cluster health
        log.info(
            "---- Starting workflow ----\n---- 1. Reboot of single osd and health check ----"
        )
        for pool in pools:
            target_osd = rados_obj.get_pg_acting_set(pool_name=pool)[0]
            log.debug(f"Rebooting OSD : {target_osd} and checking health status")
            if not rados_obj.change_osd_state(action="restart", target=target_osd):
                log.error(f"Unable to restart the OSD : {target_osd}")
                raise Exception("Execution error")

            # Waiting for recovery post OSD reboot
            method_should_succeed(wait_for_clean_pg_sets, rados_obj, test_pool=pool)
            log.debug(
                "PG's are active + clean post OSD reboot, proceeding to restart next OSD"
            )

        log.info("All the planned primary OSD reboots have completed")
        # Checking cluster health after the tests
        method_should_succeed(rados_obj.run_pool_sanity_check)
        log.info("----Completed workflows 1. Reboot of single osd and health check----")

        log.info("---- Starting workflow ----\n---- 2. Rolling Reboot OSD hosts ----")
        for nodes in osd_nodes:
            log.info(f"Rebooting node : {nodes.hostname}")
            nodes.exec_command(sudo=True, cmd="reboot", long_running=True)

            # Sleeping for 10 seconds and starting verification.
            time.sleep(10)

            # Waiting for recovery to post OSD host reboot
            method_should_succeed(wait_for_clean_pg_sets, rados_obj)
            log.info(f"PG's are active + clean post reboot of host {nodes.hostname}")

        log.info(
            "Completed reboot of all the OSD hosts, Checking cluster health status"
        )
        # Checking cluster health after the test
        method_should_succeed(rados_obj.run_pool_sanity_check)

        log.info("Wait for rebooted hosts to come online")
        timeout_time = datetime.datetime.now() + datetime.timedelta(seconds=900)
        while datetime.datetime.now() < timeout_time:
            try:
                for node in osd_nodes:
                    assert rados_obj.check_host_status(hostname=node.hostname)
                log.info("Rebooted hosts are up")
                break
            except AssertionError:
                time.sleep(25)
                if datetime.datetime.now() >= timeout_time:
                    log.error(f"{node.hostname} status is still offline after 15 mins")
                    return 1
        log.info(
            "---- Completed workflows 2. Rolling Reboot OSD hosts and health check ----"
        )

        log.info(
            "---- Starting workflow ----\n---- 3. Stopping and starting OSD daemons ----"
        )
        for pool in pools:
            target_osd = rados_obj.get_pg_acting_set(pool_name=pool)[0]
            log.debug(f"Stopping OSD : {target_osd} and checking health status")
            if not rados_obj.change_osd_state(action="stop", target=target_osd):
                log.error(f"Unable to stop the OSD : {target_osd}")
                raise Exception("Execution error")

            # Waiting for recovery to post OSD stop
            method_should_succeed(wait_for_clean_pg_sets, rados_obj, test_pool=pool)
            log.debug(
                f"PG's are active + clean post OSD stop of {target_osd}, proceeding to start OSD"
            )

            log.debug(f"Starting OSD : {target_osd} and checking health status")
            if not rados_obj.change_osd_state(action="start", target=target_osd):
                log.error(f"Unable to start the OSD : {target_osd}")
                raise Exception("Execution error")

            # Waiting for recovery to post OSD start
            method_should_succeed(wait_for_clean_pg_sets, rados_obj, test_pool=pool)
            log.debug(
                f"PG's are active + clean post OSD start of {target_osd}, proceeding to restart next OSD"
            )

        log.info(
            "Completed start and stop for all targeted OSDs. Checking cluster health"
        )
        # Checking cluster health after the tests
        method_should_succeed(rados_obj.run_pool_sanity_check)
        log.info("--- Completed workflows 3. Stopping and starting OSD daemons ---")

        log.info(
            "---- Starting workflow ----\n---- 4. restart all OSD daemons belonging to single pg----"
        )
        for pool in pools:
            pg_set = rados_obj.get_pg_acting_set(pool_name=pool)
            log.debug(f"Acting set of OSDs for testing reboot are : {pg_set}")
            for target_osd in pg_set:
                log.debug(f"Restarting OSD : {target_osd} and checking health status")
                if not rados_obj.change_osd_state(action="restart", target=target_osd):
                    log.error(f"Unable to restart the OSD : {target_osd}")
                    raise Exception("Execution error")

                # Waiting for recovery to post OSD restart
                method_should_succeed(wait_for_clean_pg_sets, rados_obj)
                log.debug(
                    f"PG's are active + clean post OSD restart of {target_osd}, proceeding to restart next OSD"
                )

        # Checking cluster health after the tests
        method_should_succeed(rados_obj.run_pool_sanity_check)
        log.info(
            "--- Completed workflows 4. restart OSD daemons belonging to single pg ---"
        )

        log.info(
            "---- Starting workflow ----\n---- 5. Removal and addition of OSD daemons"
        )

        # Removing a mon daemon and adding it back post OSD addition.
        mon_obj = MonitorWorkflows(node=cephadm)
        # Setting the mon service as unmanaged by cephadm
        if not mon_obj.set_mon_service_managed_type(unmanaged=True):
            log.error("Could not set the mon service to managed")
            raise Exception("mon service not managed error")

        init_mon_nodes = ceph_cluster.get_nodes(role="mon")
        # Selecting one mon host to be removed at random during OSD down
        test_mon_host = random.choice(init_mon_nodes)

        # Removing mon service
        if not mon_obj.remove_mon_service(host=test_mon_host.hostname):
            log.error("Could not remove the new mon added previously")
            raise Exception("mon service not removed error")

        quorum = mon_obj.get_mon_quorum_hosts()
        if test_mon_host.hostname in quorum:
            log.error(
                f"selected host : {test_mon_host.hostname} is present as part of Quorum post removal"
            )
            raise Exception("Mon in quorum error post removal error")

        for pool in pools:
            pg_set = rados_obj.get_pg_acting_set(pool_name=pool)
            log.debug(f"Acting set for removal and addition of OSDs {pg_set}")
            target_osd = pg_set[0]
            host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=target_osd)

            dev_path = get_device_path(host, target_osd)
            log.debug(
                f"osd device path  : {dev_path}, osd_id : {target_osd}, host.hostname : {host.hostname}"
            )

            utils.set_osd_devices_unmanaged(ceph_cluster, target_osd, unmanaged=True)
            method_should_succeed(utils.set_osd_out, ceph_cluster, target_osd)
            method_should_succeed(wait_for_clean_pg_sets, rados_obj)
            utils.osd_remove(ceph_cluster, target_osd)
            method_should_succeed(wait_for_clean_pg_sets, rados_obj)
            method_should_succeed(
                utils.zap_device, ceph_cluster, host.hostname, dev_path
            )
            method_should_succeed(
                wait_for_device_rados, host, target_osd, action="remove"
            )

            # Checking cluster health after OSD removal
            method_should_succeed(rados_obj.run_pool_sanity_check)
            log.info(
                f"Removal of OSD : {target_osd} is successful. Proceeding to add back the OSD daemon."
            )

            # Adding the removed OSD back and checking the cluster status
            utils.add_osd(ceph_cluster, host.hostname, dev_path, target_osd)
            method_should_succeed(wait_for_device_rados, host, target_osd, action="add")
            time.sleep(10)

            # Checking cluster health after OSD removal
            method_should_succeed(rados_obj.run_pool_sanity_check)
            log.info(
                f"Addition of OSD : {target_osd} back into the cluster was successful, and the health is good!"
            )

            utils.set_osd_devices_unmanaged(ceph_cluster, target_osd, unmanaged=False)

        # Adding the mon back to the cluster
        if not mon_obj.add_mon_service(host=test_mon_host):
            log.error(f"Could not add mon service on host {test_mon_host}")
            raise Exception("mon service not added error")

        # Setting the mon service as managed by cephadm
        if not mon_obj.set_mon_service_managed_type(unmanaged=False):
            log.error("Could not set the mon service to managed")
            raise Exception("mon service not managed error")

        # Checking if the mon added is part of Mon Quorum
        quorum = mon_obj.get_mon_quorum_hosts()
        if test_mon_host.hostname not in quorum:
            log.error(
                f"selected host : {test_mon_host.hostname} does not have mon as part of Quorum post addition "
            )
            raise Exception("Mon not in quorum error")
        log.info("---- Completed workflows 5. Removal and addition of OSD daemons ----")

        log.info(
            "---- Starting workflow ----\n---- 6. Removal and addition of OSD Hosts ----"
        )
        service_obj.add_new_hosts()
        # Waiting for recovery to post OSD addition into cluster
        method_should_succeed(wait_for_clean_pg_sets, rados_obj)
        log.debug("PG's are active + clean post OSD addition")
        # Checking cluster health after the tests
        method_should_succeed(rados_obj.run_pool_sanity_check)
        log.info(
            "Cluster is healthy post addition of New hosts and osds."
            " Proceeding to remove one of the added host and it's OSDs"
        )

        # Removing newly added OSD host and checking status
        service_obj.remove_custom_host(
            host_node_name=config.get("remove_host", "node13")
        )
        # Waiting for recovery to post OSD host remove
        method_should_succeed(wait_for_clean_pg_sets, rados_obj)
        log.debug("PG's are active + clean post OSD removal")

        # Checking cluster health after the tests
        method_should_succeed(rados_obj.run_pool_sanity_check)
        log.info("---- Completed workflows 6. Removal and addition of OSD Hosts ----")

        log.info("Completed All the workflows")
        return 0
    if config.get("osd_host_fail"):
        log.info(
            "---- Starting workflow ----\n---- 7. Replacement of a failed OSD host"
        )
        try:
            osd_hosts = rados_obj.get_osd_hosts()
            fail_host = get_node_by_id(ceph_cluster, osd_hosts[0])

            # workflow to fail the host and make it offline
            # Blocks all incoming traffic on selected OSD node, except for SSH
            out, _ = installer_node.exec_command(
                sudo=True, cmd=f"iptables -A INPUT -d {fail_host.ip_address} -j REJECT"
            )
            out, _ = installer_node.exec_command(
                sudo=True, cmd=f"iptables -A OUTPUT -d {fail_host.ip_address} -j REJECT"
            )

            # smart wait to check the status of osd host
            end_time = datetime.datetime.now() + datetime.timedelta(seconds=400)
            while datetime.datetime.now() < end_time:
                if rados_obj.check_host_status(hostname=osd_hosts[0], status="offline"):
                    break
                else:
                    log.info(
                        f"{osd_hosts[0]} is yet to become offline. Sleeping for 60 secs"
                    )
                    time.sleep(60)
            else:
                log.error(f"{osd_hosts[0]} is still Online after 8 mins.")
                raise Exception(
                    f"{osd_hosts[0]} should have been Offline, still Online after 8 mins."
                )
            log.info(f"{osd_hosts[0]} is offline as expected.")

            # proceeding to remove the offline host
            service_obj.remove_offline_host(host_node_name=osd_hosts[0])

            # adding new OSD host which will serve as replacement to the offline host
            service_obj.add_new_hosts(add_nodes=["node13"])

            # Waiting for recovery to post OSD host addition
            method_should_succeed(wait_for_clean_pg_sets, rados_obj)
            log.info("PG's are active + clean post OSD removal")
        except Exception as e:
            log.error(f"Failed with exception: {e.__doc__}")
            log.exception(e)
            return 1
        finally:
            log.info("*********** Execution of finally block starts ***********")
            # Flush iptables to reset the rules
            out, _ = installer_node.exec_command(sudo=True, cmd="iptables -F")
            # log cluster health
            rados_obj.log_cluster_health()
            # check for crashes after test execution
            if rados_obj.check_crash_status():
                log.error("Test failed due to crash at the end of test")
                return 1
            # log.info(
            #     f"----- Adding the failed OSD host {osd_hosts[0]} which was removed -------"
            # )
            # add_new_hosts(add_nodes=[osd_hosts[0]])
            # # proceeding to remove the newly added OSD host
            # log.info("----- Removing the newly added OSD host -------")
            # remove_custom_host(host_node_name="node13")
        return 0
