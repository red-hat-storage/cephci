import json

from cli.cephadm.cephadm import CephAdm
from ceph.ceph import CommandFailed
from utility.log import Log

LOG = Log(__name__)


def get_node_disks(node):
    """
    Identifies the disks associated with the node
    Args:
        node (ceph): Node in which the cmd is executed

    Returns (list): List of available disks
    """

    disks = []
    conf = {"refresh": True, "format": "json"}
    available_disks = json.loads(CephAdm(node).ceph.orch.device.ls(**conf))
    for key in available_disks:
        if key.get("devices"):
            for dev in key.get("devices"):
                disks.append(dev.get("path"))
    return list(set(disks))


def lvm_prepare(node, device_path, dmcrypt=False):
    """
    Adds metadata to logical volumes
    Args:
        node (ceph): Node in which the cmd is executed
        device_path(str) : Block/raw device

    Returns:
        boolean
    """
    try:
        if dmcrypt is False:
            out, _ = node.exec_command(
                cmd=f"ceph-volume lvm prepare --bluestore --data {device_path}", sudo=True)
            LOG.info(f"lvm prepare is successful: {out}")
            return True
        else:
            out, _ = node.exec_command(
                cmd=f"ceph-volume lvm prepare --bluestore --dmcrypt --data {device_path}", sudo=True)
            LOG.info(f"lvm prepare is successful : {out}")
            return True
    except CommandFailed as err:
        LOG.error(f"Error: {err}")
        return False


def lvm_activate(node, osd_id, osd_fsid):
    """
    Activate newly prepared OSD
    Args:
        node (ceph): Node in which the cmd is executed
        osd_id(str): OSD id (an integer unique to each OSD)
        osd_fsid(str): OSD FSID (unique identifier of an OSD)

    Returns:
        boolean
    """
    try:
        out, _ = node.exec_command(
            cmd=f"ceph-volume lvm activate --bluestore {osd_id} {osd_fsid}", sudo=True)
        LOG.info(f"lvm activate is successful: {out}")
        return True
    except CommandFailed as err:
        LOG.error(f"Error: {err}")
        return False


def lvm_create(node, device_path):
    """
    The create subcommand calls the prepare subcommand,
    and then calls the activate subcommand.
    Args:
        node (ceph): Node in which the cmd is executed
        device_path(str) : Block/raw device

    Returns:
        boolean
    """
    try:
        out, _ = node.exec_command(
            cmd=f"ceph-volume lvm create --bluestore --data {device_path}", sudo=True)
        LOG.info(f"lvm create is successful: {out}")
        return True
    except CommandFailed as err:
        LOG.error(f"Error: {err}")
        return False
