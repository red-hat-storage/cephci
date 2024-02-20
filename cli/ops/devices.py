import json

from cli.cephadm.cephadm import CephAdm


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
