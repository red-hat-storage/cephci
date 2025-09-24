import time
import traceback

from ceph.ceph_admin import CephAdmin
from ceph.rados import utils
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.serviceability_workflows import ServiceabilityMethods
from tests.rados.rados_test_util import (
    create_pools,
    get_device_path,
    wait_for_daemon_status,
    wait_for_device_rados,
    write_to_pools,
)
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from tests.rados.test_9281 import do_rados_get
from utility.log import Log
from utility.utils import method_should_succeed, should_not_be_empty

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Automates OSD re-balance test scenarios.
    1. Create replicated and/or erasure pool/pools
    2. Identify the first osd to be removed
    3. Fetch the host by daemon_type=osd and osd id
    4. Fetch container id and device path
    5. Mark osd out and wait for pgs to be active+clean
    6. Remove OSD
    7. Zap device and wait for device not present
    8. Identify the second osd to be removed
    9. Fetch the host by daemon_type=osd and osd id
    10. Fetch container id and device path
    11. Mark osd out
    12. Add first osd and wait for device present and pgs to be active+clean
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    client_node = ceph_cluster.get_nodes(role="client")[0]
    service_obj = ServiceabilityMethods(cluster=ceph_cluster, **config)

    log.info("Running osd in progress rebalance tests")
    try:
        pool = create_pools(config, rados_obj, client_node)
        should_not_be_empty(pool, "Failed to retrieve pool details")
        pool_name = pool["pool_name"]
        write_to_pools(config, rados_obj, client_node)
        rados_obj.change_recovery_threads(config=pool, action="set")
        acting_pg_set = rados_obj.get_pg_acting_set(pool_name=pool["pool_name"])
        log.info(f"Acting set {acting_pg_set}")
        should_not_be_empty(acting_pg_set, "Failed to retrieve acting pg set")
        osd_id = acting_pg_set[0]
        host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=osd_id)
        should_not_be_empty(host, "Failed to fetch host details")
        dev_path = get_device_path(host, osd_id)
        target_osd1_spec_name = service_obj.get_osd_spec(osd_id=osd_id)
        log_lines = (
            f"\nosd2 device path  : {dev_path},\n osd_id : {osd_id},\n hostname : {host.hostname},\n"
            f"Target OSD Spec : {target_osd1_spec_name}"
        )
        log.debug(log_lines)
        rados_obj.set_service_managed_type(service_type="osd", unmanaged=True)
        method_should_succeed(utils.set_osd_out, ceph_cluster, osd_id)
        method_should_succeed(wait_for_clean_pg_sets, rados_obj, test_pool=pool_name)
        utils.osd_remove(ceph_cluster, osd_id, zap=True)
        method_should_succeed(wait_for_clean_pg_sets, rados_obj, test_pool=pool_name)
        method_should_succeed(utils.zap_device, ceph_cluster, host.hostname, dev_path)
        method_should_succeed(wait_for_device_rados, host, osd_id, action="remove")
        osd_id1 = acting_pg_set[1]
        host1 = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=osd_id1)
        should_not_be_empty(host1, "Failed to fetch host details")
        dev_path1 = get_device_path(host1, osd_id1)
        target_osd2_spec_name = service_obj.get_osd_spec(osd_id=osd_id1)
        log_lines = (
            f"\nosd2 device path  : {dev_path},\n osd_id : {osd_id1},\n hostname : {host1.hostname},\n"
            f"Target OSD Spec : {target_osd2_spec_name}"
        )
        log.debug(log_lines)
        method_should_succeed(utils.set_osd_out, ceph_cluster, osd_id1)
        utils.add_osd(ceph_cluster, host.hostname, dev_path, osd_id1)
        method_should_succeed(wait_for_device_rados, host, osd_id1, action="add")
        method_should_succeed(
            wait_for_daemon_status,
            rados_obj=rados_obj,
            daemon_type="osd",
            daemon_id=osd_id1,
            status="running",
            timeout=60,
        )
        assert service_obj.add_osds_to_managed_service()
        method_should_succeed(wait_for_clean_pg_sets, rados_obj, test_pool=pool_name)

        acting_pg_set1 = rados_obj.get_pg_acting_set(pool_name=pool["pool_name"])
        if len(acting_pg_set) != len(acting_pg_set1):
            log.error(
                f"Acting pg set count before {acting_pg_set} and after {acting_pg_set1} rebalance mismatched"
            )
            return 1

        if pool.get("rados_put", False):
            do_rados_get(client_node, pool["pool_name"], 1)
        log.info("verification of OSD re-balancing completed")
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        active_osd_list = rados_obj.get_osd_list(status="up")
        log.info(f"List of active OSDs: \n{active_osd_list}")
        if osd_id not in active_osd_list:
            rados_obj.set_service_managed_type(service_type="osd", unmanaged=True)
            utils.add_osd(ceph_cluster, host.hostname, dev_path, osd_id)
            method_should_succeed(wait_for_device_rados, host, osd_id, action="add")
            method_should_succeed(
                wait_for_daemon_status,
                rados_obj=rados_obj,
                daemon_type="osd",
                daemon_id=osd_id,
                status="running",
                timeout=60,
            )
            assert service_obj.add_osds_to_managed_service(
                osds=[osd_id], spec=target_osd1_spec_name
            )

        if osd_id1 not in active_osd_list:
            rados_obj.set_service_managed_type(service_type="osd", unmanaged=True)
            utils.add_osd(ceph_cluster, host1.hostname, dev_path1, osd_id1)
            method_should_succeed(wait_for_device_rados, host1, osd_id1, action="add")
            method_should_succeed(
                wait_for_daemon_status,
                rados_obj=rados_obj,
                daemon_type="osd",
                daemon_id=osd_id,
                status="running",
                timeout=60,
            )
            assert service_obj.add_osds_to_managed_service(
                osds=[osd_id1], spec=target_osd2_spec_name
            )

        rados_obj.set_service_managed_type(service_type="osd", unmanaged=False)
        time.sleep(10)
        active_osd_list = rados_obj.get_osd_list(status="up")
        in_osd_list = rados_obj.get_osd_list(status="in")
        up_not_in = list(set(active_osd_list) - set(in_osd_list))
        for osdid in up_not_in:
            rados_obj.run_ceph_command(cmd=f"ceph osd in {osdid}")
            time.sleep(2)
        rados_obj.change_recovery_threads(config=pool, action="rm")
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
