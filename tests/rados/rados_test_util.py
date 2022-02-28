import datetime
import json
import random
import time

from tests.rados.test_9281 import do_rados_put
from utility.log import Log
from utility.utils import method_should_succeed, should_not_be_empty

log = Log(__name__)


def create_pools(config, rados_obj, client_node):
    """
    This function will create pool, write data to pool and return pool information
    Args:
        config: config parameters from suite file
        rados_obj: RadosOrchestrator object
        client_node: client node details
    Returns:  pool information
    """
    pool = {}
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
        pool = random.choice(pools)["create_pool"]
    return pool


def write_to_pools(config, rados_obj, client_node):
    """
    This function will create pool, write data to pool and return pool information
    Args:
        config: config parameters from suite file
        rados_obj: RadosOrchestrator object
        client_node: client node details
    Returns:  pool information
    """
    pools = config.get("create_pools")
    for each_pool in pools:
        cr_pool = each_pool["create_pool"]
        if cr_pool.get("rados_put", False):
            do_rados_put(mon=client_node, pool=cr_pool["pool_name"], nobj=100)
        else:
            method_should_succeed(rados_obj.bench_write, **cr_pool)


def wait_for_device(host, osd_id, action: str) -> bool:
    """
    Waiting for the device to be removed/added based on the action
    Args:
        host: host object
        osd_id: osd id
        action: add/remove device path
    Returns:  True -> pass, False -> fail
    """
    end_time = datetime.datetime.now() + datetime.timedelta(seconds=9000)
    while end_time > datetime.datetime.now():
        flag = True

        out, _ = host.exec_command(sudo=True, cmd="podman ps --format json")
        container = [
            item["Names"][0]
            for item in json.loads(out.read().decode())
            if "ceph" in item["Command"]
        ]
        should_not_be_empty(container, "Failed to retrieve container ids")
        vol_out, _ = host.exec_command(
            sudo=True,
            cmd=f"podman exec {container[0]} ceph-volume lvm list --format json",
        )
        volume_out = vol_out.read().decode()
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


def get_device_path(host, osd_id):
    """
    Function to fetch device path
    Args:
        host: host details
        osd_id: osd_id to fetch device path
    Returns:  device path
    """
    out, _ = host.exec_command(sudo=True, cmd="podman ps --format json")
    container_id = [
        item["Names"][0]
        for item in json.loads(out.read().decode())
        if f"osd.{osd_id}" in item["Command"]
    ][0]
    should_not_be_empty(container_id, "Failed to retrieve container id")
    # fetch device path by osd_id
    vol_out, _ = host.exec_command(
        sudo=True,
        cmd=f"podman exec {container_id} ceph-volume lvm list --format json",
    )
    volume_out = vol_out.read().decode()
    dev_path = [
        v[0]["devices"][0]
        for k, v in json.loads(volume_out).items()
        if str(k) == str(osd_id)
    ][0]
    should_not_be_empty(dev_path, "Failed to get device path")
    return dev_path
