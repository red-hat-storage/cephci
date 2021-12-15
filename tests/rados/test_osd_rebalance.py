import datetime
import json
import logging
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.rados_service import RadosService
from tests.rados.stretch_cluster import wait_for_clean_pg_sets

log = logging.getLogger(__name__)


def run(ceph_cluster, **kw):
    """
    Automates OSD re-balance test scenarios.
    1. Create Pool
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
    rados_obj = RadosService(node=cephadm)

    log.info("Running create pool test case")
    if config.get("create_pool"):
        pool = config.get("create_pool")
        if not rados_obj.create_pool(
            pool_name=pool["pool_name"], pg_num=pool["pg_num"]
        ):
            log.error(f"Failed to create the {pool['pool_name']} Pool")
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
            for item in json.loads(out.read().decode())
            if f"osd.{osd_id}" in item["Command"]
        ][0]
        if not container_id:
            log.error("Failed to retrieve container id")
            return 1
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
        if not dev_path:
            log.error("Failed to get device path")
            return 1
        log.info(f"device path  : {dev_path}, osd_id : {osd_id}")
        log.info(f"host.hostname  : {host.hostname}")
        if not rados_obj.set_osd_devices_unamanged(unmanaged=True):
            log.error("Failed to set osd devices unamnaged.")
            return 1
        if not rados_obj.bench_write(**pool):
            log.error("Failed to write objects into the Pool")
            return 1
        if not rados_obj.set_osd_out(osd_id):
            log.error("Failed to set osd out")
            return 1
        if not wait_for_clean_pg_sets(rados_obj):
            return 1
        if not rados_obj.remove_osd(osd_id):
            log.error("Failed to remove osd")
            return 1
        if not wait_for_clean_pg_sets(rados_obj):
            return 1
        if not rados_obj.zap_device(host.hostname, dev_path):
            log.error("Failed to zap device.")
            return 1
        if not wait_for_device(host, container_id, osd_id, action="remove"):
            log.error("Failed to remove OSD from host.")
            return 1
        if not rados_obj.add_osd(host.hostname, dev_path, osd_id):
            log.error("Failed to add osd.")
            return 1
        if not wait_for_device(host, container_id, osd_id, action="add"):
            log.error("Failed to add OSD to host.")
            return 1
        if not wait_for_clean_pg_sets(rados_obj):
            return 1
        if not rados_obj.bench_write(**pool):
            log.error("Failed to write objects into the Pool")
            return 1
        if not wait_for_clean_pg_sets(rados_obj):
            return 1
        if not rados_obj.set_osd_devices_unamanged(unmanaged=False):
            log.error("Failed to set osd devices unamnaged.")
            return 1
        rados_obj.change_recover_threads(config=pool, action="rm")
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
            item["Names"][0]
            for item in json.loads(out.read().decode())
            if "ceph" in item["Command"]
        ]
        if not container:
            log.error("Failed to retrieve container ids")
            return 1

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
