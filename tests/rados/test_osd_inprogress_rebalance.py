import traceback

from ceph.ceph_admin import CephAdmin
from ceph.rados import utils
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.rados_test_util import (
    create_pools,
    get_device_path,
    wait_for_device,
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
    try:
        log.info(run.__doc__)
        config = kw["config"]
        cephadm = CephAdmin(cluster=ceph_cluster, **config)
        rados_obj = RadosOrchestrator(node=cephadm)
        client_node = ceph_cluster.get_nodes(role="client")[0]

        log.info("Running osd in progress rebalance tests")
        pool = create_pools(config, rados_obj, client_node)
        should_not_be_empty(pool, "Failed to retrieve pool details")
        write_to_pools(config, rados_obj, client_node)
        rados_obj.change_recover_threads(config=pool, action="set")
        acting_pg_set = rados_obj.get_pg_acting_set(pool_name=pool["pool_name"])
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
        method_should_succeed(wait_for_clean_pg_sets, rados_obj)
        utils.osd_remove(ceph_cluster, osd_id)
        method_should_succeed(wait_for_clean_pg_sets, rados_obj)
        method_should_succeed(utils.zap_device, ceph_cluster, host.hostname, dev_path)
        method_should_succeed(wait_for_device, host, osd_id, action="remove")
        osd_id1 = acting_pg_set[1]
        host1 = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=osd_id1)
        should_not_be_empty(host1, "Failed to fetch host details")
        dev_path1 = get_device_path(host1, osd_id1)
        log.debug(
            f"osd2 device path  : {dev_path1}, osd_id : {osd_id1}, host.hostname : {host1.hostname}"
        )
        method_should_succeed(utils.set_osd_out, ceph_cluster, osd_id1)
        utils.add_osd(ceph_cluster, host.hostname, dev_path, osd_id)
        method_should_succeed(wait_for_device, host, osd_id, action="add")
        method_should_succeed(wait_for_clean_pg_sets, rados_obj)

        acting_pg_set1 = rados_obj.get_pg_acting_set(pool_name=pool["pool_name"])
        if len(acting_pg_set) != len(acting_pg_set1):
            log.error(
                f"Acting pg set count before {acting_pg_set} and after {acting_pg_set1} rebalance mismatched"
            )
            return 1

        if pool.get("rados_put", False):
            do_rados_get(client_node, pool["pool_name"], 1)
        utils.set_osd_devices_unmanaged(ceph_cluster, osd_id, unmanaged=False)
        rados_obj.change_recover_threads(config=pool, action="rm")
        if config.get("delete_pools"):
            for name in config["delete_pools"]:
                method_should_succeed(rados_obj.detete_pool, name)
            log.info("deleted all the given pools successfully")
        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
