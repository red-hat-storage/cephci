import re
import tempfile
from json import loads

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from cli.utilities.utils import reboot_node
from tests.ceph_volume.utils import get_available_devices, get_ceph_volume
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
    disks = get_available_devices(installer)

    # Get client osd bootstrap auth key
    osd_auth_key = CephAdm(installer).ceph.auth.get("client.bootstrap-osd")

    # Create keyring file on host
    keyring_file = create_keyring_file(installer, osd_auth_key)

    # Set LVM object
    lvm = get_ceph_volume(installer, mounts=(f"{keyring_file}:{OSD_KEYRING_PATH}",)).lvm

    # Prepare ceph-volume lvm
    out = lvm.prepare(data=disks[0], check_ec=True)
    if "successful" not in out:
        raise OperationFailedError(f"Failed to prepare disk: {disks[0]}")

    # Reboot installer node
    status = reboot_node(installer)
    if not status:
        raise RebootFailed(f"Node {installer.hostname} failed to reboot.")

    # Get ceph-volume lvm list
    lvm_list = loads(lvm.list(data=disks[0], check_ec=True))

    # Get osd_id and osd_fsid
    vol_info = [i for v in lvm_list.values() for i in v][0]
    osd_id = vol_info.get("tags", {}).get("ceph.osd_id", {})
    osd_fsid = vol_info.get("tags", {}).get("ceph.osd_fsid", {})
    if not re.fullmatch(r"[0-9]+", str(osd_id)):
        raise OperationFailedError(
            f"Invalid OSD ID in prepared disk metadata: {osd_id!r}"
        )

    # Activate ceph-volume OSD
    out = lvm.activate(osd_id, osd_fsid, bluestore=False, check_ec=True)
    if "successful" not in out:
        raise OperationFailedError(f"Failed to activate osd: {osd_id}")

    # Deactivate ceph-volume OSD
    lvm.deactivate(osd_id, osd_fsid, bluestore=False, check_ec=True)

    # Reboot installer node
    status = reboot_node(installer)
    if not status:
        raise RebootFailed(f"Node {installer.hostname} failed to reboot.")

    # Zap ceph-volume OSD
    out = lvm.zap(data=disks[0], check_ec=True)
    if "successful" not in out:
        raise OperationFailedError(f"Failed to zap disk: {disks[0]}")

    # Purge the zapped OSD's map, CRUSH, and auth entries before further tests.
    CephAdm(installer).execute(
        cmd=f"cephadm shell -- ceph osd purge {osd_id} --yes-i-really-mean-it",
        sudo=True,
        check_ec=True,
    )

    return 0
