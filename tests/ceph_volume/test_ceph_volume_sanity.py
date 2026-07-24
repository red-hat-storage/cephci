import tempfile
from json import loads

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from cli.ops.devices import get_node_disks
from cli.utilities.utils import reboot_node
from tests.cephadm.test_verify_cluster_health_after_reboot import RebootFailed

OSD_KEYRING_PATH = "/var/lib/ceph/bootstrap-osd/ceph.keyring"


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
    """Perform ceph-volume basic lvm operations such as
    Activate, deactivate, prepare, zap.
    Rebooting the node should not affect the basic operations
    """
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

    # Reboot installer node
    status = reboot_node(installer)
    if not status:
        raise RebootFailed(f"Node {installer.hostname} failed to reboot.")

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

    # Deactivate ceph-volume OSD
    lvm.deactivate(osd_id, osd_fsid)

    # Reboot installer node
    status = reboot_node(installer)
    if not status:
        raise RebootFailed(f"Node {installer.hostname} failed to reboot.")

    # Zap ceph-volume OSD
    out = lvm.zap(data=disks[0])
    if "successful" not in out:
        OperationFailedError(f"Failed to prepare disk: {disks[0]}")

    # Remove ceph auth key for removed OSD
    CephAdm(installer).ceph.auth.rm(f"osd.{osd_id}")

    return 0
