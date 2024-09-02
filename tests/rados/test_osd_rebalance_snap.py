import traceback

from ceph.ceph_admin import CephAdmin
from ceph.rados import utils
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from tests.rados.rados_test_util import (
    create_pools,
    get_device_path,
    wait_for_device_rados,
    write_to_pools,
)
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from tests.rados.test_9281 import do_rados_get, do_rados_snap_get
from utility.log import Log
from utility.utils import method_should_succeed, should_not_be_empty

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Automates taking snapshots during re-balance test scenarios.
    1. Create replicated pool
    2. Identify the osd to be removed
    3. Fetch the host by daemon_type=osd and osd id
    4. Fetch container id and device path
    5. Mark osd out and wait for pgs to be active+clean
    6. Remove OSD
    7. Zap device and wait for device not present
    8. Add osd.
    9. Take pool snapshots and wait for device present and pgs to be active+clean
    10. Verify pool snapshots are present.
    """
    log.info(run.__doc__)
    config = kw["config"]
    rhbuild = config.get("rhbuild")
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    pool_obj = PoolFunctions(node=cephadm)
    client_node = ceph_cluster.get_nodes(role="client")[0]
    timeout = config.get("timeout", 10800)

    log.info("Running osd in progress rebalance tests")
    try:
        pool = create_pools(config, rados_obj, client_node)
        should_not_be_empty(pool, "Failed to retrieve pool details")
        write_to_pools(config, rados_obj, client_node)

        # Set recover threads configurations
        if not rhbuild.startswith("6"):
            rados_obj.change_recovery_threads(config=pool, action="set")

        # Set mclock_profile
        if rhbuild.startswith("6") and config.get("mclock_profile"):
            rados_obj.set_mclock_profile(profile=config["mclock_profile"])
        pool_name = pool["pool_name"]
        acting_pg_set = rados_obj.get_pg_acting_set(pool_name=pool_name)
        log.info(f"Acting set {acting_pg_set}")
        should_not_be_empty(acting_pg_set, "Failed to retrieve acting pg set")
        osd_id = acting_pg_set[0]
        host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=osd_id)
        should_not_be_empty(host, "Failed to fetch host details")
        dev_path = get_device_path(host, osd_id)
        log.debug(
            f"osd1 device path  : {dev_path}, osd_id : {osd_id}, host.hostname : {host.hostname}"
        )
        utils.set_osd_devices_unmanaged(ceph_cluster, osd_id, unmanaged=True)
        method_should_succeed(utils.set_osd_out, ceph_cluster, osd_id)
        method_should_succeed(
            wait_for_clean_pg_sets, rados_obj, timeout=timeout, test_pool=pool_name
        )
        utils.osd_remove(ceph_cluster, osd_id)
        if pool.get("rados_put", False):
            do_rados_get(client_node, pool["pool_name"], 1)
        method_should_succeed(
            wait_for_clean_pg_sets, rados_obj, timeout=timeout, test_pool=pool_name
        )
        method_should_succeed(utils.zap_device, ceph_cluster, host.hostname, dev_path)
        method_should_succeed(wait_for_device_rados, host, osd_id, action="remove")

        utils.add_osd(ceph_cluster, host.hostname, dev_path, osd_id)
        method_should_succeed(wait_for_device_rados, host, osd_id, action="add")

        snapshots = []
        if pool.get("snapshot", "False"):
            for _ in range(pool.get("num_snaps", 1)):
                snap = pool_obj.create_pool_snap(pool.get("pool_name"))
                if snap:
                    snapshots.append(snap)
                else:
                    log.error("Could not create snapshot on the pool")
                    raise Exception("Could not create snapshot on the pool")

        method_should_succeed(
            wait_for_clean_pg_sets, rados_obj, timeout=timeout, test_pool=pool_name
        )
        write_to_pools(config, rados_obj, client_node)
        method_should_succeed(
            wait_for_clean_pg_sets, rados_obj, timeout=timeout, test_pool=pool_name
        )
        if pool.get("rados_put", False):
            do_rados_get(client_node, pool["pool_name"], 1)

        if pool.get("snapshot", "False"):
            log.info(f"Snapshots created are : {snapshots}")
            for snap_name in snapshots:
                if not pool_obj.check_snap_exists(
                    snap_name=snap_name, pool_name=pool["pool_name"]
                ):
                    log.error("Snapshot of pool does not exists")
                    raise Exception("Snapshot of pool does not exists")
                do_rados_snap_get(client_node, pool["pool_name"], 1, snap_name)

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1

    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        out, _ = cephadm.shell(args=["ceph osd ls"])
        active_osd_list = out.strip().split("\n")
        log.debug(f"List of active OSDs: \n{active_osd_list}")
        if osd_id not in active_osd_list:
            utils.set_osd_devices_unmanaged(ceph_cluster, osd_id, unmanaged=True)
            utils.add_osd(ceph_cluster, host.hostname, dev_path, osd_id)
            method_should_succeed(wait_for_device_rados, host, osd_id, action="add")

        utils.set_osd_devices_unmanaged(ceph_cluster, osd_id, unmanaged=False)
        rados_obj.change_recovery_threads(config=pool, action="rm")
        # removal of rados pool
        if config.get("delete_pools"):
            for name in config["delete_pools"]:
                method_should_succeed(rados_obj.delete_pool, name)
            log.info("deleted all the given pools successfully")

        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1

    return 0
