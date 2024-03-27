from cli.ops.devices import lvm_prepare, lvm_activate, lvm_create
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Passing incorrect arguements to ceph-volume lvm
    prepare, activate and create command should not
    cause crashes. User understandable error/warning shuld be
    displayed
    """
    # Get configs
    configs = kw.get("config")

    # Get specs from config
    device_path = configs.get("device_path")
    osd_id = configs.get("osd_id")
    osd_fsid = configs.get("osd_fsid")

    # Get the installer node
    node = ceph_cluster.get_nodes(role="installer")[0]

    # Run the command lvm prepare
    out = lvm_prepare(node, device_path)

    # Run the command lvm activate
    out = lvm_activate(node, osd_id, osd_fsid)

    # Run the command lvm create
    out = lvm_create(node, device_path)
    return 0
