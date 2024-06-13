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
        nobj = cr_pool.get("num_objs", 100)
        if cr_pool.get("rados_put", False):
            do_rados_put(mon=client_node, pool=cr_pool["pool_name"], nobj=nobj)
        else:
            method_should_succeed(rados_obj.bench_write, **cr_pool)


def wait_for_device(host, osd_id, action: str, timeout: int = 9000) -> bool:
    """
    Waiting for the device to be removed/added based on the action
    Args:
        host: host object
        osd_id: osd id
        action: add/remove device path
        timeout: wait timeout in seconds
    Returns:  True -> pass, False -> fail
    """
    end_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
    while end_time > datetime.datetime.now():
        flag = True

        out, _ = host.exec_command(sudo=True, cmd="podman ps --format json")
        container = [
            item["Names"][0]
            for item in json.loads(out)
            if any("osd" in name for name in item["Names"])
        ]
        should_not_be_empty(container, "Failed to retrieve container ids")
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


def get_device_path(host, osd_id):
    """
    Function to fetch device path
    Args:
        host: host details
        osd_id: osd_id to fetch device path
    Returns:  device path
    """
    out, _ = host.exec_command(sudo=True, cmd="podman ps --format json")
    out = json.loads(out)
    log.debug(f"containers on the host :\n {out}\n")
    try:
        container_id = [
            item["Names"][0] for item in out if f"osd.{osd_id}" in item["Command"]
        ][0]
    except Exception as err:
        log.error(f"host exception : {err}")
    should_not_be_empty(container_id, "Failed to retrieve container id")
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
    should_not_be_empty(dev_path, "Failed to get device path")
    return dev_path


def get_slow_requests_log(node, start_time, end_time, service_name="mon"):
    """
    Retrieve slow op requests log using journalctl command
    Args:
        node: ceph node details
        start_time: time to start reading the journalctl logs - format ('2022-07-20 09:40:10')
        end_time: time to stop reading the journalctl logs - format ('2022-07-20 10:58:49')
        service_name: ceph service name (mon, mgr ...)
    Returns:  journal_logs
    """
    j_log = []
    try:
        d_out, d_err = node.exec_command(
            cmd=f"systemctl list-units --type=service | grep ceph | grep {service_name} | head -n 1"
        )
        daemon = d_out.lstrip().split(" ")[0].rstrip()
        j_log, err = node.exec_command(
            cmd=f"sudo journalctl -u {daemon} --since '{start_time}' --until '{end_time}' | grep 'slow requests'"
        )
        log.info(f"output ----- {j_log}")
    except Exception as er:
        log.error(f"Exception hit while command execution. {er}")
    should_not_be_empty(j_log, "Failed to retrieve slow requests")
    return j_log


def wait_for_device_rados(host, osd_id, action: str, timeout: int = 900) -> bool:
    """
    Waiting for the device to be removed/added based on the action
    Args:
        host: host object
        osd_id: osd id
        action: add/remove device path
        timeout: wait timeout in seconds
    Returns:  True -> pass, False -> fail
    """
    dev_path = None
    end_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
    while end_time > datetime.datetime.now():
        flag = True

        base_cmd = "cephadm shell -- "
        volume_cmd = f"ceph-volume lvm list {osd_id} --format json"
        out, _ = host.exec_command(cmd=f"{base_cmd} {volume_cmd}", sudo=True)

        for item in json.loads(out)[osd_id]:
            if "osd-block" in item["lv_name"]:
                dev_path = item["devices"][0]
                break

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
