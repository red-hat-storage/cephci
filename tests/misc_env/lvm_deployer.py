from copy import deepcopy
from time import sleep

from ceph.utils import get_nodes_by_ids, parallel
from utility.log import Log
from utility.lvm_utils import pvcreate, vgcreate

LOG = Log(__name__)


def find_storage_unit(value):
    """Return with storage Unit based on the provided value.

    Example::

        if value is 10Gb|10G|10Gi|10g,
        then return 10G.

        K --> Kilo byte
        M --> Mega byte
        G --> Giga byte
        P --> Peta Byte

    Args:
        value: storage size with unit.

    Returns:
        storage metric value with right unit.
    """
    value = value.lower()
    metric = "".join([i for i in value if i.isdigit()])

    if "k" in value:
        return f"{metric}K"
    elif "m" in value:
        return f"{metric}M"
    elif "g" in value:
        return f"{metric}G"
    elif "t" in value:
        return f"{metric}T"


def create_lvms(node, count, size):
    """Create LVs on node.

    Args:
        node: CephNode obj
        count: number of LVMs
        size: LVM size
    """
    vg_name = "data_vg"
    if not hasattr(node, "volume_list"):
        raise Exception(f"{node.hostname} has no volume_list!!!")

    devices = [vol.path for vol in node.volume_list]
    pvcreate(node, " ".join(devices))
    vgcreate(node, vg_name, " ".join(devices))
    lvms = []
    for i in range(count):
        node.exec_command(
            cmd=f"lvcreate -y -L {find_storage_unit(size)} -n lv{i} {vg_name}",
            sudo=True,
        )
        lvms.append(f"/dev/{vg_name}/lv{i}")
    node.volume_list = deepcopy(lvms)


def run(ceph_cluster, **kw):
    """Test utility to create LVs based on the configuration.

    Module creates LVs on each node using Custom configuration
    values or from the test case. This module create LVMs and
    assign newly created LVs to node.volume_list.

    Example::

        python3 run.py .... --custom-config lvm_count=3 \
         --custom-config lvm_size=100G \
         --custom-config lvm_nodes=node1,node2

        DO-NOT-USE:
            python3 run.py .... \
             --custom-config lvm_size=5G,lvm_count=3,lvm_nodes=node1,node2

    Example::

        - test:
            name: Deploy LVMs for OSD deployment
            desc: Deploy LVM using custom configuration
            module: lvm_deployer.py
            config:
                size: 20gb
                count: 2
                nodes: ["node1", "node2"]

    Args:
        ceph_cluster: Ceph cluster Obj
        kw (dict): Test data

    Returns:
        0 - Success
        1 - Failure
    """
    LOG.info("Deploying LVMs.....")
    config = kw.get("config", {})
    custom_config = kw.get("test_data", {}).get("custom-config", [])
    num_of_lvms = config.get("count", 0)
    lvm_size = config.get("size", "")
    nodes = config.get("nodes", [])

    # Apply overrides
    for i in custom_config:
        key, value = i.split("=")
        if key.lower() == "lvm_count":
            num_of_lvms = int(value)
        elif key.lower() == "lvm_size":
            lvm_size = value
        elif key.lower() == "lvm_nodes":
            nodes = value.split(",")

    if not (num_of_lvms and lvm_size and nodes):
        raise Exception("'lvm_count or lvm_size or nodes' not found!!!")

    with parallel() as p:
        for node in get_nodes_by_ids(ceph_cluster, nodes):
            p.spawn(create_lvms, node, num_of_lvms, lvm_size)
            sleep(2)
    return 0
