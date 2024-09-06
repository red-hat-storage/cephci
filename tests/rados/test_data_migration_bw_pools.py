"""
Module to Create, test pool where Objects from one pool are migrated / copied from one pool to another.
Scenarios:
    RE -> RE
    RE -> EC
    EC -> RE
    EC -> EC
Methods:
    rados cppool
    rados import/export
"""

import datetime
import time

import yaml

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from tests.rados.test_9281 import do_rados_get, do_rados_put
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw) -> int:
    """
    Test to copy data from one pool to another
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    client_node = ceph_cluster.get_nodes(role="client")[0]

    pool_configs_path = config.get("pool_configs_path")

    with open(pool_configs_path, "r") as fd:
        pool_configs = yaml.safe_load(fd)

    pool_orig = pool_configs[config["pool-1-type"]][config["pool-1-conf"]]
    pool_target = pool_configs[config["pool-2-type"]][config["pool-2-conf"]]
    try:
        create_given_pool(rados_obj, pool_orig)
        create_given_pool(rados_obj, pool_target)

        # Writing objects with omap entries
        if pool_orig["pool_type"] == "replicated":
            if not pool_obj.fill_omap_entries(
                pool_name=pool_orig["pool_name"], obj_end=50, num_keys_obj=100
            ):
                log.error(
                    f"Omap entries not generated on pool {pool_orig['pool_name']}"
                )
                return 1

        do_rados_put(mon=client_node, pool=pool_orig["pool_name"], nobj=50)

        snapshots = []
        for _ in range(5):
            snap = pool_obj.create_pool_snap(pool_name=pool_orig["pool_name"])
            if snap:
                snapshots.append(snap)
            else:
                log.error("Could not create snapshot on the pool")
                return 1

        copy_start_time = datetime.datetime.now()
        # Using cppool to copy contents b/w the pools
        cmd = f"rados cppool {pool_orig['pool_name']} {pool_target['pool_name']}"
        client_node.exec_command(sudo=True, cmd=cmd, long_running=True)
        copy_end_time = datetime.datetime.now()
        # Sleeping for 2 seconds after copy to perform get operations
        time.sleep(2)

        copy_duration = copy_end_time - copy_start_time
        log.info(
            f"Time taken for copying the data via  'rados cppool' is {copy_duration}"
        )
        do_rados_get(client_node, pool_target["pool_name"], 1)

        # Checking if the snapshots of pool was also copied
        # Snapshots of pool should not be copied
        for snap_name in snapshots:
            if pool_obj.check_snap_exists(
                snap_name=snap_name, pool_name=pool_target["pool_name"]
            ):
                log.error("Snapshot of pool exists")
                return 1

        # deleting the Target pool created after cppool
        rados_obj.delete_pool(pool=pool_target["pool_name"])

        # Creating new target pool to test import/export
        create_given_pool(rados_obj, pool_target)

        # Creating temp file to hold pool info
        client_node.exec_command(
            cmd="touch /tmp/file",
        )

        copy_start_time = datetime.datetime.now()
        # crating export of data on old pool
        cmd = f"rados export -p {pool_orig['pool_name']} /tmp/file"
        client_node.exec_command(sudo=True, cmd=cmd, long_running=True)

        # Importing the file into the new pool
        cmd = f"rados import -p {pool_target['pool_name']} /tmp/file"
        client_node.exec_command(sudo=True, cmd=cmd, long_running=True)
        copy_end_time = datetime.datetime.now()

        copy_duration = copy_end_time - copy_start_time
        log.info(
            f"Time taken for copying the data via  'rados import / export' is {copy_duration}"
        )

        # Sleeping for 2 seconds after copy to perform get operations
        time.sleep(2)

        do_rados_get(client_node, pool_target["pool_name"], 1)

        # Checking if the snapshots of pool was also copied
        # Snapshots of pool should not be copied
        for snap_name in snapshots:
            if pool_obj.check_snap_exists(
                snap_name=snap_name, pool_name=pool_target["pool_name"]
            ):
                log.error("Snapshot of pool exists")
                return 1
    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        # deleting the Original & Target pool created after cppool
        rados_obj.delete_pool(pool=pool_target["pool_name"])
        rados_obj.delete_pool(pool=pool_orig["pool_name"])
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
    return 0


def create_given_pool(obj, conf):
    """
    This function will create given pool, EC or replicated
    Args:
        obj: Rados Object to perform operations
        conf: kw params for pool creation

    Returns: None

    """
    log.debug(
        f"Creating {conf['pool_type']} pool on the cluster with name {conf['pool_name']}"
    )
    if conf.get("pool_type", "replicated") == "erasure":
        method_should_succeed(obj.create_erasure_pool, name=conf["pool_name"], **conf)
    else:
        method_should_succeed(
            obj.create_pool,
            **conf,
        )

    log.debug("Created the pool.")
    return None
