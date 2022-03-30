import datetime
import json
import random
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados import utils
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from tests.rados.test_9281 import do_rados_get, do_rados_put
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Automates OSD re-balance test scenarios.
    1. Create replicated and/or erasure pool/pools
    2. Identify the osd to be removed
    3. Fetch the host by daemon_type=osd and osd id
    4. Fetch container id and device path
    5. Mark osd out and wait for pgs to be active+clean
    6. Remove OSD
    7. Zap device and wait for device not present
    8. Add OSD and wait for device present and pgs to be active+clean
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    client_node = ceph_cluster.get_nodes(role="client")[0]

    log.info("Running create pool test case")
    if config.get("create_pools"):
        pools = config.get("create_pools")
        for each_pool in pools:
            cr_pool = each_pool["create_pool"]
            if cr_pool.get("pool_type", "replicated") == "erasure":
                method_should_succeed(
                    rados_obj.create_erasure_pool, name=cr_pool["pool_name"], **cr_pool
                )
            else:
                method_should_succeed(rados_obj.create_pool, **cr_pool)
            if cr_pool.get("rados_put", False):
                do_rados_put(mon=client_node, pool=cr_pool["pool_name"], nobj=100)
            else:
                method_should_succeed(rados_obj.bench_write, **cr_pool)
        pool = random.choice(pools)["create_pool"]
    if not pool:
        log.error("Failed to retrieve pool details")
        return 1

    rados_obj.change_recover_threads(config=pool, action="set")
    acting_pg_set = rados_obj.get_pg_acting_set(pool_name=pool["pool_name"])
    log.info(f"Acting set {acting_pg_set}")
    if not acting_pg_set:
        log.error("Failed to retrieve acting pg set")
        return 1
    osd_id = acting_pg_set[0]
    host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=osd_id)
    if not host:
        log.error("Failed to fetch host details")
        return 1
    # fetch container id
    out, _ = host.exec_command(sudo=True, cmd="podman ps --format json")
    container_id = [
        item["Names"][0]
        for item in json.loads(out)
        if f"osd.{osd_id}" in item["Command"]
    ][0]
    if not container_id:
        log.error("Failed to retrieve container id")
        return 1
    # fetch device path by osd_id
    volume_out, _ = host.exec_command(
        sudo=True,
        cmd=f"podman exec {container_id} ceph-volume lvm list --format json",
    )
    dev_path = [
        v[0]["devices"][0]
        for k, v in json.loads(volume_out).items()
        if str(k) == str(osd_id)
    ][0]
    if not dev_path:
        log.error("Failed to get device path")
        return 1
    log.debug(
        f"device path  : {dev_path}, osd_id : {osd_id}, host.hostname : {host.hostname}"
    )
    utils.set_osd_devices_unamanged(ceph_cluster, unmanaged=True)
    method_should_succeed(utils.set_osd_out, ceph_cluster, osd_id)
    method_should_succeed(wait_for_clean_pg_sets, rados_obj)
    utils.osd_remove(ceph_cluster, osd_id)
    if cr_pool.get("rados_put", False):
        do_rados_get(client_node, pool["pool_name"], 1)

    method_should_succeed(wait_for_clean_pg_sets, rados_obj)
    method_should_succeed(utils.zap_device, ceph_cluster, host.hostname, dev_path)
    method_should_succeed(wait_for_device, host, container_id, osd_id, action="remove")
    utils.add_osd(ceph_cluster, host.hostname, dev_path, osd_id)
    method_should_succeed(wait_for_device, host, container_id, osd_id, action="add")
    method_should_succeed(wait_for_clean_pg_sets, rados_obj)
    do_rados_put(mon=client_node, pool=pool["pool_name"], nobj=1000)
    method_should_succeed(wait_for_clean_pg_sets, rados_obj)
    if cr_pool.get("rados_put", False):
        do_rados_get(client_node, pool["pool_name"], 1)
    utils.set_osd_devices_unamanged(ceph_cluster, unmanaged=False)
    rados_obj.change_recover_threads(config=pool, action="rm")

    if config.get("delete_pools"):
        for name in config["delete_pools"]:
            method_should_succeed(rados_obj.detete_pool, name)
        log.info("deleted all the given pools successfully")

    return 0


def wait_for_device(host, container_id, osd_id, action: str) -> bool:
    """
    Waiting for the device to be removed/added based on the action
    Args:
        host: host object
        container_id: container name for running podman exec
        osd_id: osd id
        action: add/remove device path
    Returns:  True -> pass, False -> fail
    """
    end_time = datetime.datetime.now() + datetime.timedelta(seconds=9000)
    while end_time > datetime.datetime.now():
        flag = True

        out, _ = host.exec_command(sudo=True, cmd="podman ps --format json")
        container = [
            item["Names"][0] for item in json.loads(out) if "ceph" in item["Command"]
        ]
        if not container:
            log.error("Failed to retrieve container ids")
            return False

        volume_out, _ = host.exec_command(
            sudo=True,
            cmd=f"podman exec {container[0]} ceph-volume lvm list --format json",
        )
        dev_path = [
            v[0]["devices"][0]
            for k, v in json.loads(volume_out).items()
            if str(k) == str(osd_id)
        ]
        log.info(f"dev_path  : {dev_path}")
        if action == "remove":
            if dev_path:
                flag = False
        else:
            if not dev_path:
                flag = False
        if flag:
            log.info(f"The OSD {action} is completed.")
            return True
        log.info(
            f"Waiting for OSD {osd_id} to {action}. checking status again in 2 minutes"
        )
        time.sleep(120)
    return False
