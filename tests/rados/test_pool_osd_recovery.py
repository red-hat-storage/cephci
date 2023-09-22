"""
Module to perform tier-4 tests on pools WRT OSD daemons.

1. Reboot of single osd and health check
2. Rolling Reboot OSD hosts
3. Stopping and starting OSD daemons
4. restart all OSD daemons belonging to single pg
5. Removal and addition of OSD daemons
6. Removal and addition of OSD Hosts

"""
import datetime
import json
import time

import yaml

from ceph.ceph_admin import CephAdmin
from ceph.rados import utils
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.utils import get_node_by_id
from tests.ceph_installer.test_cephadm import run as add_osd
from tests.cephadm.test_host import run as deploy_host
from tests.rados.rados_test_util import get_device_path, wait_for_device
from tests.rados.stretch_cluster import get_osd_details, wait_for_clean_pg_sets
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
    pool_configs = config["pool_configs"]
    pool_configs_path = config["pool_configs_path"]
    osd_nodes = ceph_cluster.get_nodes(role="osd")

    with open(pool_configs_path, "r") as fd:
        pool_conf_file = yaml.safe_load(fd)

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

        # Waiting for recovery to post OSD reboot
        method_should_succeed(wait_for_clean_pg_sets, rados_obj)
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

    log.info("Completed reboot of all the OSD hosts, Checking cluster health status")
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
        method_should_succeed(wait_for_clean_pg_sets, rados_obj)
        log.debug(
            f"PG's are active + clean post OSD stop of {target_osd}, proceeding to start OSD"
        )

        log.debug(f"Starting OSD : {target_osd} and checking health status")
        if not rados_obj.change_osd_state(action="start", target=target_osd):
            log.error(f"Unable to start the OSD : {target_osd}")
            raise Exception("Execution error")

        # Waiting for recovery to post OSD start
        method_should_succeed(wait_for_clean_pg_sets, rados_obj)
        log.debug(
            f"PG's are active + clean post OSD start of {target_osd}, proceeding to restart next OSD"
        )

    log.info("Completed start and stop for all targeted OSDs. Checking cluster health")
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

    log.info("---- Starting workflow ----\n---- 5. Removal and addition of OSD daemons")
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
        method_should_succeed(utils.zap_device, ceph_cluster, host.hostname, dev_path)
        method_should_succeed(wait_for_device, host, target_osd, action="remove")

        # Checking cluster health after OSD removal
        method_should_succeed(rados_obj.run_pool_sanity_check)
        log.info(
            f"Removal of OSD : {target_osd} is successful. Proceeding to add back the OSD daemon."
        )

        # Adding the removed OSD back and checking the cluster status
        utils.add_osd(ceph_cluster, host.hostname, dev_path, target_osd)
        method_should_succeed(wait_for_device, host, target_osd, action="add")
        time.sleep(10)

        # Checking cluster health after OSD removal
        method_should_succeed(rados_obj.run_pool_sanity_check)
        log.info(
            f"Addition of OSD : {target_osd} back into the cluster was successful, and the health is good!"
        )

        utils.set_osd_devices_unmanaged(ceph_cluster, target_osd, unmanaged=False)

    log.info("---- Completed workflows 5. Removal and addition of OSD daemons ----")

    log.info(
        "---- Starting workflow ----\n---- 6. Removal and addition of OSD Hosts ----"
    )
    # Adding new hosts to the cluster
    add_args = {
        "command": "add_hosts",
        "service": "host",
        "args": {
            "nodes": ["node12", "node13"],
            "attach_address": True,
            "labels": "apply-all-labels",
        },
    }
    add_args.update(config)

    ncount_pre = len(rados_obj.run_ceph_command(cmd="ceph orch host ls"))
    deploy_host(ceph_cluster=ceph_cluster, config=add_args)

    if not ncount_pre <= len(rados_obj.run_ceph_command(cmd="ceph orch host ls")):
        log.error("New hosts are not added into the cluster")
        raise Exception("Execution error")

    log.info(
        "New hosts added to the cluster successfully, Proceeding to deploy OSDs on the same."
    )
    # Deploying OSDs on the new nodes.
    osd_args = {
        "steps": [
            {
                "config": {
                    "command": "apply_spec",
                    "service": "orch",
                    "validate-spec-services": True,
                    "specs": [
                        {
                            "service_type": "osd",
                            "service_id": "new_osds",
                            "encrypted": "true",
                            "placement": {"label": "osd-bak"},
                            "spec": {"data_devices": {"all": "true"}},
                        }
                    ],
                }
            }
        ]
    }
    osd_args.update(config)
    ocount_pre = len(get_osd_details(node=cephadm))
    add_osd(ceph_cluster=ceph_cluster, config=osd_args)
    if not ocount_pre <= len(get_osd_details(node=cephadm)):
        log.error("New OSDs were not added into the cluster")
        raise Exception("Execution error")

    log.info("Deployed new hosts and deployed OSDs on them")
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
    rm_host_id = config.get("remove_host", "node13")
    rm_host = get_node_by_id(ceph_cluster, rm_host_id)
    log.info(f"Identified host : {rm_host.hostname} to be removed from the cluster")

    # Starting to drain the host
    drain_cmd = f"ceph orch host drain {rm_host.hostname}"
    cephadm.shell([drain_cmd])
    # Sleeping for 2 seconds for removal to have started
    time.sleep(2)
    log.debug(f"Started drain operation on node : {rm_host.hostname}")

    status_cmd = "ceph orch osd rm status -f json"
    end_time = datetime.datetime.now() + datetime.timedelta(seconds=3000)
    flag = False
    while end_time > datetime.datetime.now():
        out, err = cephadm.shell([status_cmd])
        try:
            drain_ops = json.loads(out)
            for entry in drain_ops:
                log.debug(
                    f"Drain operations are going on host {rm_host.hostname} \nOperations: {entry}"
                )
        except json.JSONDecodeError:
            log.info(f"Drain operations completed on host : {rm_host.hostname}")
            flag = True
            break
        except Exception as error:
            log.error(f"Hit issue during drain operations: {error}")
            raise Exception(error)
        log.debug("Sleeping for 10 seconds and checking again....")
        time.sleep(10)

    if not flag:
        log.error(
            "Drain operation not completed on the cluster even after 3000 seconds"
        )
        raise Exception("Execution Error")

    log.info(
        f"Completed drain operation on the host. {rm_host.hostname}\n Removing host from the cluster"
    )
    time.sleep(5)
    rm_cmd = f"ceph orch host rm {rm_host.hostname} --force"
    cephadm.shell([rm_cmd])
    time.sleep(5)

    # Checking if the host still exists on the cluster
    ls_cmd = "ceph orch host ls"
    hosts = rados_obj.run_ceph_command(cmd=ls_cmd)
    for host in hosts:
        if host["hostname"] == rm_host.hostname:
            log.error(f"Host : {rm_host.hostname} still present on the cluster")
            raise Exception("Host not removed error")
    log.info(
        f"Successfully removed host : {rm_host.hostname} from the cluster. Checking status after removal"
    )

    # Waiting for recovery to post OSD host remove
    method_should_succeed(wait_for_clean_pg_sets, rados_obj)
    log.debug("PG's are active + clean post OSD removal")

    # Checking cluster health after the tests
    method_should_succeed(rados_obj.run_pool_sanity_check)

    log.info("---- Completed workflows 6. Removal and addition of OSD Hosts ----")

    log.info("Completed All the workflows")
    return 0
