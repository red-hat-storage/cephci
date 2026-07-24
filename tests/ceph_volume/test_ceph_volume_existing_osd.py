import tempfile
from json import loads

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from cli.ops.devices import get_node_disks
from tests.ceph_volume.test_ceph_volume_lvm_tags_after_osd_rm import RemoveOsdError

OSD_KEYRING_PATH = "/var/lib/ceph/bootstrap-osd/ceph.keyring"
OSD_CONF = "/etc/ceph/ceph.conf"
OSD_KEYRING = "/var/lib/ceph/osd/ceph-{}/keyring"


def create_keyring_file(node, keyring):
    """Create temp keyring file
    Args:
        node (ceph): ceph node
        keyring (dict): ceph auth keyring
    Return: file name
    """
    # Create temporory file path
    temp_file = tempfile.NamedTemporaryFile()

    # Create temporary file and dump data
    with node.remote_file(sudo=True, file_name=temp_file.name, file_mode="w") as file:
        file.write(keyring + "\n")

    return temp_file.name


def run(ceph_cluster, **kw):
    """Perform OSD starting and stopping of an existing OSD created using ceph-volume"""
    # Get the installer and OSD nodes
    installer = ceph_cluster.get_nodes(role="installer")[0]

    # Get client osd bootstrap auth key
    osd_auth_key = CephAdm(installer).ceph.auth.get("client.bootstrap-osd")

    # Create keyring file on host
    keyring_file = create_keyring_file(installer, osd_auth_key)

    # Set LVM object
    lvm = CephAdm(
        installer, src_mount=keyring_file, mount=OSD_KEYRING_PATH
    ).ceph_volume.lvm

    # Identify available devices on node
    disks = get_node_disks(installer)

    # Prepare ceph-volume lvm
    out = lvm.prepare(data=disks[0])
    if "successful" not in out:
        OperationFailedError(f"Failed to prepare disk: {disks[0]}")

    # Get ceph-volume lvm list
    lvm_list = loads(lvm.list(data=disks[0]))

    # Get osd_id and osd_fsid
    vol_info = [i for v in lvm_list.values() for i in v][0]
    osd_id = vol_info.get("tags", {}).get("ceph.osd_id", {})
    osd_fsid = vol_info.get("tags", {}).get("ceph.osd_fsid", {})

    # Activate ceph-volume OSD
    out = lvm.activate(osd_id, osd_fsid, bluestore=False)
    if "successful" not in out:
        OperationFailedError(f"Failed to activate osd: {osd_id}")

    # Mount the OSD to bring it up
    CephAdm(installer).ceph_osd(
        id=osd_id, conf=OSD_CONF, keyring=OSD_KEYRING.format(osd_id)
    )

    # Get OSD tree
    out = CephAdm(installer).ceph.osd.tree(format="json")
    osd_tree = loads(out)
    for osd_ in osd_tree.get("nodes"):
        if osd_.get("id") == osd_id and osd_.get("status") == "up":
            raise RemoveOsdError("The osd is up after OSD mount")

    # Perform OSD out
    CephAdm(installer).ceph.osd.out(osd_id)

    # Perform OSD in
    CephAdm(installer).ceph.osd._in(osd_id)

    # Get OSD tree
    out = CephAdm(installer).ceph.osd.tree(format="json")
    osd_tree = loads(out)
    for osd_ in osd_tree.get("nodes"):
        if osd_.get("id") == osd_id and osd_.get("status") == "up":
            raise RemoveOsdError("The osd is up after OSD mount")

    return 0
