from cli.cephadm.cephadm import CephAdm
from cli.ops.devices import get_node_disks
from cli.utilities.operations import wait_for_osd_daemon_state
from cli.utilities.utils import OperationFailedError, WaitUntil, create_yaml_config
from utility.lvm_utils import lvm_create, pvcreate, vgcreate


def run(ceph_cluster, **kw):
    """Create OSDs with pre-created LVMs"""

    # Get configs
    config = kw.get("config")

    # Get the installer and OSD nodes
    installer = ceph_cluster.get_nodes(role="installer")[0]

    # Identify available devices on node
    disks = get_node_disks(installer)

    # Get required host from config specs
    nodes = config.get("specs", {}).get("placement", {}).get("hosts")
    host = [ceph_cluster.get_nodes()[int(node[-1])].hostname for node in nodes]

    # Create spec file specific to hostname, and devices
    specs = config.get("specs", {})
    specs["placement"]["hosts"] = host

    # Create lvm manually
    vg_name = "vg_disk_pool"
    lv_sizes = {"data_devices": "4G", "db_devices": "2G", "wal_devices": "2G"}
    pvcreate(installer, disks[0])
    vgcreate(installer, vg_name, disks[0])

    # Create and update LVM in spec
    for name, size in lv_sizes.items():
        lvm_name = lvm_create(installer, name, vg_name, size)
        specs[name]["paths"] = [f"/dev/{vg_name}/{lvm_name}"]

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

    # Wait for OSD to be ready and running state
    for osd in out:
        wait_for_osd_daemon_state(installer, osd, "up")

    return 0
