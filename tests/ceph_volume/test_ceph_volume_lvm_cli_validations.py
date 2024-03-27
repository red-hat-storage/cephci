from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Passing incorrect arguements to ceph-volume lvm
    prepare, activate and create command should not
    cause crashes. User understandable error/warning should be
    displayed
    """
    # Get configs
    configs = kw.get("config")

    # Get specs from config
    data = configs.get("device_path")
    osd_id = configs.get("osd_id")
    osd_fsid = configs.get("osd_fsid")
    if not (data and osd_id and osd_fsid):
        raise ConfigError("Mandatory config is not provided")

    # Get the installer node
    node = ceph_cluster.get_nodes(role="installer")[0]

    # Set LVM object
    lvm = CephAdm(node).ceph_volume.lvm

    # Run the command lvm prepare
    out = lvm.prepare(data)
    if f"lsblk: {osd_id}: not a block device" in out:
        log.info("lvm prepare failed as expected")

    # Run the command lvm activate
    out = lvm.activate(osd_id, osd_fsid)
    if f"RuntimeError: could not find {osd_id} with osd_fsid {osd_fsid}" in out:
        log.info("lvm activate failed as expected")

    # Run the command lvm create
    out = lvm.create(data)
    if f"lsblk: {osd_id}: not a block device" in out:
        log.info("lvm create failed as expected")
    return 0
