"""
   This module contains the wrapper functions to perform general ceph cluster modification operations.
    1. Remove OSD
    2. Add OSD
    3. Set osd out
    3. Zap device path
"""
from ceph.ceph_admin.daemon import Daemon
from ceph.ceph_admin.device import Device
from ceph.ceph_admin.osd import OSD
from utility.log import Log

log = Log(__name__)


def set_osd_devices_unamanged(ceph_cluster, unmanaged):
    """
    Sets osd device unmanaged as true/false
    Args:
        ceph_cluster: ceph cluster
        unmanaged: true/false
    """
    config = {
        "command": "apply",
        "service": "osd",
        "args": {"all-available-devices": True, "unmanaged": unmanaged},
        "verify": False,
    }
    log.info(f"Executing OSD {config.pop('command')} service")
    osd = OSD(cluster=ceph_cluster, **config)
    osd.apply(config)


def set_osd_out(ceph_cluster, osd_id):
    """
    Sets osd out
    Args:
        ceph_cluster: ceph cluster
        osd_id: osd id
    Returns:
        Pass->true, Fail->false
    """
    config = {"command": "out", "service": "osd", "pos_args": [osd_id]}
    log.info(f"Executing OSD {config.pop('command')} service")
    osd = OSD(cluster=ceph_cluster, **config)
    out, err = osd.out(config)
    if f"marked out osd.{osd_id}" in err:
        return True
    return False


def osd_remove(ceph_cluster, osd_id):
    """
    osd remove
    Args:
        ceph_cluster: ceph cluster
        osd_id: osd id
    """
    config = {"command": "rm", "service": "osd", "pos_args": [osd_id]}
    log.info(f"Executing OSD {config.pop('command')} service")
    osd = OSD(cluster=ceph_cluster, **config)
    osd.rm(config)


def zap_device(ceph_cluster, host, device_path):
    """
    Zap device
    Args:
        ceph_cluster: ceph cluster
        host: hostname
        device_path: device path
    Returns:
        Pass->true, Fail->false
    """
    config = {
        "command": "out",
        "pos_args": [host, device_path],
        "args": {"force": True},
    }
    log.info(f"Executing device {config.pop('command')} service")
    device = Device(cluster=ceph_cluster, **config)
    out, err = device.zap(config)
    if not out:
        return True
    return False


def add_osd(ceph_cluster, host, device_path, osd_id):
    """
    add osd
    Args:
        ceph_cluster: ceph cluster
        host: hostname
        device_path: device path
        osd_id: osd id
    Returns:
        Pass->true, Fail->false
    """
    config = {"command": "add", "service": "osd", "pos_args": [host, device_path]}
    log.info(f"Executing daemon {config.pop('command')} service")
    daemon = Daemon(cluster=ceph_cluster, **config)
    daemon.add(config)
