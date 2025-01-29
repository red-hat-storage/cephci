"""
This module contains the wrapper functions to perform general ceph cluster modification operations.
 1. Remove OSD
 2. Add OSD
 3. Set osd out
 3. Zap device path
"""

from json import loads

from ceph.ceph_admin.daemon import Daemon
from ceph.ceph_admin.device import Device
from ceph.ceph_admin.osd import OSD
from utility.log import Log

log = Log(__name__)


def set_osd_devices_unmanaged(ceph_cluster, osd_id, unmanaged):
    """Sets osd device unmanaged as true/false.

    Args:
        ceph_cluster: ceph cluster
        osd_id: OSD id
        unmanaged: true/false
    """
    out = err = None
    config = {
        "command": "apply",
        "service": "osd",
        "verify": False,
    }
    osd = OSD(cluster=ceph_cluster, **config)
    osd_dmns, _ = osd.ps(
        {"base_cmd_args": {"format": "json"}, "args": {"daemon_type": "osd"}}
    )

    # fetch service name using osd.id
    service_name = None
    for daemon in loads(osd_dmns):
        if daemon["daemon_id"] == str(osd_id):
            if daemon.get("service_name") == "osd":
                return
            service_name = daemon["service_name"]
            break

    if not service_name:
        log.error(f"No orch service found for osd: {osd_id}")
        return
    log.info(f"Setting OSD service {service_name} to unmanaged={unmanaged}")

    # fetch OSD service information
    out, err = osd.ls(
        {"base_cmd_args": {"format": "json"}, "args": {"service_name": service_name}}
    )

    # return if no services found
    if "No services reported" in out or "No services reported" in err:
        log.debug(out)
        log.error(err)
        return
    svc = loads(out)[0]

    # return if it is at required state
    if svc.get("unmanaged") == unmanaged:
        log.info(f"{service_name} is already set to unmanaged={unmanaged}")
        return

    # apply to required "unmanaged" state
    svc_spec = {
        "service_type": "osd",
        "service_id": svc["service_id"],
        "unmanaged": unmanaged,
        "spec": svc["spec"],
        "placement": svc["placement"],
    }

    if svc_spec["placement"].get("hosts"):
        svc_spec["placement"]["nodes"] = svc_spec["placement"]["hosts"]

    osd.apply_spec({"specs": [svc_spec]})


def get_containers(host, role=None):
    """Return all containers.

    Args:
        host: CephNode object
        role: ceph role type (example: mon, osd, mgr)
    Returns:
        list of all containers or by role
    """
    out, _ = host.exec_command(sudo=True, cmd="podman ps --format json")
    containers = loads(out)
    if not role:
        return containers

    ceph_roles = {
        "mon": "/usr/bin/ceph-mon",
        "mgr": "/usr/bin/ceph-mgr",
        "node_exporter": "/bin/node_exporter",
        "alert_manager": "/bin/alertmanager",
        "prometheus": "/bin/prometheus",
        "osd": "/usr/bin/ceph-osd",
        "crash": "/usr/bin/ceph-crash",
    }

    if role not in ceph_roles.keys():
        return list()

    _containers = []
    for container in containers:
        inspect, _ = host.exec_command(
            sudo=True,
            cmd="podman inspect %s --format {{.Config.Entrypoint}}" % container["Id"],
            check_ec=False,
        )
        if ceph_roles[role] in inspect:
            _containers.append(container)

    return _containers


def podman_exec(host, container, cmd, shell="bash"):
    """Podman exec operation with command

    Args:
        host: host where podman execution
        container: container id or name
        cmd: command to be executed
        shell: executor (default: bash)

    Returns:
        podman exec response
    """
    response, _ = host.exec_command(
        sudo=True, cmd=f"podman exec {container} {shell} -c {repr(cmd)}"
    )
    return response


def set_osd_out(ceph_cluster, osd_id):
    """
    Sets osd out
    Args:
        ceph_cluster: ceph cluster
        osd_id: osd id
    Returns:
        Pass->true, Fail->false
    """
    out = err = None
    config = {"command": "out", "service": "osd", "pos_args": [osd_id]}
    log.info(f"Executing OSD {config.pop('command')} service")
    osd = OSD(cluster=ceph_cluster, **config)
    out, err = osd.out(config)
    log.debug(out)
    log.debug(err)
    if f"marked out osd.{osd_id}" in err or f"marked out osd.{osd_id}" in out:
        return True
    return False


def set_osd_in(
    ceph_cluster, osd_id: int = None, osd_ids: list = None, all: bool = False
):
    """
    Sets osd in
    if only 'ceph_cluster' argument is provided,
    all the previously 'out' osds will be set 'in'
    Args:
        ceph_cluster: ceph cluster
        osd_id: osd id
        osd_ids: list of osds which need to be marked in
        all: set all the previously 'out' osds 'in'
    Returns:
        Pass->true, Fail->false
    """
    if osd_id is None and osd_ids is None:
        all = True
    config = {"command": "in", "service": "osd", "pos_args": [osd_id, osd_ids, all]}
    log.info(f"Executing OSD {config.pop('command')} service")
    osd = OSD(cluster=ceph_cluster, **config)
    out, err = osd.osd_in(config)
    if all and ("marked in osd" in err or "" in err):
        return True
    if osd_id and (
        f"marked in osd.{osd_id}" in err or f"osd.{osd_id} is already in" in err
    ):
        return True
    ret_val = True
    if osd_ids:
        for o_id in osd_ids:
            if f"marked in osd.{o_id}" not in err:
                ret_val = False
    return ret_val


def osd_remove(ceph_cluster, osd_id, zap=False, force=False):
    """
    osd remove
    Args:
        ceph_cluster: ceph cluster
        osd_id: osd id
        zap: flag to control zapping of device
        force: flag to remove the OSD forcefully
    """
    config = {"command": "rm", "service": "osd", "pos_args": [osd_id]}
    cmd_args = {}
    if zap:
        cmd_args["zap"] = True
        cmd_args["force"] = True
    if force:
        cmd_args["force"] = True
    if bool(cmd_args):
        config["base_cmd_args"] = cmd_args
    log.info(f"Executing OSD {config.pop('command')} service")
    osd = OSD(cluster=ceph_cluster, **config)
    osd.rm(config)


def osd_replace(ceph_cluster, osd_id):
    """
    same as osd_remove, with one exception: the OSD is not permanently
    removed from the CRUSH hierarchy, but is instead assigned a ‘destroyed’ flag.
    Args:
        ceph_cluster: ceph cluster
        osd_id: osd id
    """
    config = {
        "command": "rm",
        "service": "osd",
        "pos_args": [osd_id],
        "base_cmd_args": {"zap": True, "replace": True},
        "validate": False,
    }
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
    if "zap successful" in out or "" in out:
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
    out, err = daemon.add(config)
    if "Created osd" in out:
        return True
    return False


def configure_osd_flag(ceph_cluster, action, flag):
    """
    set/unset the osd flag
    Command example: ceph osd set noscrub
    Args:
        ceph_cluster: ceph cluster
        action: set or unset
          example: set|unset
        flag:  value of the flag
          example:pause|noup|nodown|noout|noin|nobackfill|
            norebalance|norecover|noscrub|nodeep-scrub|notieragent
    Returns: True/False
    """
    config = {"command": action, "service": "osd", "flag": flag}
    log.info(f"Executing OSD {action} {flag}")
    osd = OSD(cluster=ceph_cluster, **config)
    out, err = osd.flag(config)
    if f"{flag} is {action}" in err:
        return True
    return False
