from cephci.utils.configs import get_cloud_credentials, get_configs
from cli.cephadm.cephadm import CephAdm
from cli.cloudproviders import CloudProvider
from cli.cluster.node import Node
from cli.cluster.volume import Volume
from cli.exceptions import OperationFailedError
from cli.ops.devices import get_node_disks
from cli.utilities.operations import wait_for_osd_daemon_state
from cli.utilities.utils import WaitUntil, create_yaml_config


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


def run(ceph_cluster, **kw):
    """Wrong block_db_size computed when adding OSD"""

    # Get configs
    get_configs()
    config = kw.get("config")

    # Get the installer and OSD nodes
    installer = ceph_cluster.get_nodes(role="installer")[0]
    osd_nodes = ceph_cluster.get_nodes(role="osd")

    # Get specs from config
    device_spec = config.get("specs", {})

    # Add RHOS-d volume for data device
    size = device_spec.get("block_db_size", {}).replace("G", "")
    _add_node_volumes(nodes=osd_nodes, config=config, size=size, count=6)

    # Get required host from config specs
    nodes = config.get("specs", {}).get("placement", {}).get("hosts")
    host = [ceph_cluster.get_nodes()[int(node[-1])].hostname for node in nodes]

    # Refresh ceph orch devices
    c = {"refresh": True}
    devices = CephAdm(installer).ceph.orch.device.ls(**c)
    if not devices:
        raise OperationFailedError("Devices are not re-freshed")

    # Identify available devices on node
    timeout, interval = 300, 6
    for w in WaitUntil(timeout=timeout, interval=interval):
        disks = get_node_disks(installer)
        if len(disks) > 2:
            break
    if w.expired:
        raise OperationFailedError("Failed to wait for OSD to generate")

    # Create spec file specific to hostname, and devices
    specs = config.get("specs", {})
    specs["placement"]["hosts"] = host
    specs["data_devices"]["paths"] = disks[3:]
    specs["db_devices"]["paths"] = disks[:2]
    specs["block_db_size"] = "2G"

    # Create a spec file after updating devices
    file = create_yaml_config(installer, specs)

    # Create OSDs with spec file
    c = {"pos_args": [], "input": file}
    CephAdm(nodes=installer, mount=file).ceph.orch.apply(**c)

    # Wait for OSDs ids
    timeout, interval = 300, 6
    for w in WaitUntil(timeout=timeout, interval=interval):
        out = CephAdm(installer).ceph.osd.ls()
        if out:
            break
    if w.expired:
        raise OperationFailedError("Failed to wait for OSD to generate")

    # Wait for OSDs to be ready and running state
    for osd in out:
        wait_for_osd_daemon_state(installer, osd, "up")

    return 0
