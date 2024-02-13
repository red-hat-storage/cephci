from json import loads

from ceph.waiter import WaitUntil
from cephci.utils.configs import get_cloud_credentials, get_configs
from cli.cephadm.cephadm import CephAdm
from cli.cloudproviders import CloudProvider
from cli.cluster.node import Node
from cli.cluster.volume import Volume
from cli.exceptions import OperationFailedError
from cli.utilities.operations import wait_for_osd_daemon_state
from cli.utilities.utils import (
    create_yaml_config,
    get_lvm_on_osd_container,
    get_running_containers,
)


def _add_node_volumes(nodes, config, size, count):
    """Create volume in bulk and attach to node
    Args:
        nodes (ceph): nodes on volumes to create
        config (dict): ceph config
        size (str): size of volume
        count (int): number of volume
    """
    # Get cloud creadentials
    cloud_type = config.get("cloud-type")
    cloud_configs = get_cloud_credentials(cloud_type)
    cloud = CloudProvider(cloud_type, **cloud_configs)

    nodes = nodes if type(nodes) in (list, tuple) else [nodes]
    for node in nodes:
        for i in range(count):
            volname = f"{node.ceph_nodename}-vol-{size}-{i}"
            Volume(volname, cloud).create(size=size)
            Node(node.ceph_nodename, cloud).attach_volume(volname)
    return True


def _get_osd_db_id(osd_ids, lvm_list):
    """Get OSD id and associated devices name
    Args:
        osd_ids (list): list of osd ids
        lvm_list (list): list of lvm
    return (dict): osd id associated with device
    """
    osds = {}
    for id in osd_ids:
        for item in lvm_list.get(id):
            if item.get("type") == "db":
                osds[id] = item["devices"]
    return osds


def run(ceph_cluster, **kw):
    """Re-deploy non-collocated OSDs with wrong dedicated DB size"""

    # Get configs
    get_configs()
    config = kw.get("config")

    # Get the installer and OSD nodes
    installer = ceph_cluster.get_nodes(role="installer")[0]
    osd_nodes = ceph_cluster.get_nodes(role="osd")

    # Get device spec
    device_spec = config.get("spec", {}).get("spec", {})

    # Create volume for data device
    size = device_spec.get("data_devices", {}).get("size", {}).replace("GB", "")
    _add_node_volumes(nodes=osd_nodes, config=config, size=size, count=4)

    # Get required host from config specs
    nodes = config.get("spec", {}).get("placement", {}).get("hosts")
    host = [ceph_cluster.get_nodes()[int(node[-1])].hostname for node in nodes]

    # Create a spec file specific to hostname
    specs = config.get("spec", {})
    specs["placement"]["hosts"] = host

    # Refresh ceph orch devices
    c = {"refresh": True}
    if not CephAdm(installer).ceph.orch.device.ls(**c):
        raise OperationFailedError("Devices are not re-freshed")

    # Generate a yaml file
    file = create_yaml_config(installer, specs)

    # Create OSDs with yaml file
    c = {"pos_args": [], "input": file}
    CephAdm(nodes=installer, mount=file).ceph.orch.apply(**c)

    # Wait for OSDs ids
    timeout, interval = 300, 6
    for w in WaitUntil(timeout=timeout, interval=interval):
        if CephAdm(installer).ceph.osd.ls():
            break
    if w.expired:
        raise OperationFailedError("Failed to wait for OSD to generate")

    # Wait for OSDs to be ready and running state
    out = CephAdm(installer).ceph.osd.ls()
    for osd in out:
        wait_for_osd_daemon_state(installer, osd, "up")

    # Get the lvm list before OSD Zap
    running_containers, _ = get_running_containers(
        osd_nodes[0], format="json", expr="name=osd", sudo=True
    )
    container_ids = [item.get("Names")[0] for item in loads(running_containers)]
    lvm_list = get_lvm_on_osd_container(container_ids[0], osd_nodes[0])

    # Identify an OSD ID to perform
    osd_ids = list(lvm_list.keys())

    # Get OSD and device before zap
    osd_db_before = _get_osd_db_id(osd_ids=osd_ids, lvm_list=lvm_list)

    # Perform osd zap
    conf = {"zap": True, "force": True}
    for osd_id in osd_ids:
        osd_rm = CephAdm(installer).ceph.orch.osd.rm(osd_id=osd_id, **conf)
        if not osd_rm:
            raise OperationFailedError("Failed to remove osd")

    # Wait until the rm operation is complete
    timeout, interval = 300, 6
    for w in WaitUntil(timeout=timeout, interval=interval):
        conf = {"format": "json"}
        out = CephAdm(installer).ceph.orch.osd.rm(status=True, **conf)
        if "No OSD remove/replace operations reported" in out:
            break
    if w.expired:
        raise OperationFailedError("Failed to perform osd rm operation. Timed out!")

    # Get the lvm list after OSD Zap
    running_containers, _ = get_running_containers(
        osd_nodes[0], format="json", expr="name=osd", sudo=True
    )
    container_ids = [item.get("Names")[0] for item in loads(running_containers)]

    # Get the lvm list afer OSD zap
    lvm_list = get_lvm_on_osd_container(container_ids[0], osd_nodes[0])

    # Identify an OSD ID to perform
    osd_ids = list(lvm_list.keys())

    # Get OSD and device after zap
    osd_db_after = _get_osd_db_id(osd_ids=osd_ids, lvm_list=lvm_list)

    # Validate if db device has changed
    if osd_db_before != osd_db_after:
        raise OperationFailedError("Faild to re-deploy non-collocated OSD")

    return 0
