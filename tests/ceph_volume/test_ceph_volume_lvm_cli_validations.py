from collections.abc import Mapping

from cli.exceptions import ConfigError, OperationFailedError
from tests.ceph_volume.utils import get_ceph_volume
from utility.log import Log

log = Log(__name__)


def _validate_rejection(command, node, output, expected_errors):
    """Require a failed command and an actionable error without a crash."""
    # Lvm returns combined stdout/stderr; exec_command also records the status
    # on the node, so inspect it before executing the next command.
    exit_status = getattr(node, "exit_status", None)
    output = str(output or "")
    details = f"Exit status: {exit_status}; output: {output}"
    if not isinstance(exit_status, int) or isinstance(exit_status, bool):
        raise OperationFailedError(
            f"Unable to verify ceph-volume lvm {command} exit status. {details}"
        )
    if exit_status == 0:
        raise OperationFailedError(
            f"ceph-volume lvm {command} unexpectedly accepted invalid arguments. {details}"
        )
    crash_markers = (
        "traceback (most recent call last)",
        "segmentation fault",
        "core dumped",
        "fatal python error",
        "assertionerror",
    )
    if (
        exit_status < 0
        or exit_status >= 128
        or any(marker in output.lower() for marker in crash_markers)
    ):
        raise OperationFailedError(
            f"ceph-volume lvm {command} crashed instead of rejecting invalid arguments. {details}"
        )
    if f"ceph-volume lvm {command} successful" in output.lower():
        raise OperationFailedError(
            f"ceph-volume lvm {command} reported success for invalid arguments. {details}"
        )
    normalized_output = " ".join(output.split())
    if not any(error in normalized_output for error in expected_errors):
        raise OperationFailedError(
            f"ceph-volume lvm {command} did not report the expected validation error. {details}"
        )
    log.info(f"lvm {command} rejected invalid arguments as expected")


def run(ceph_cluster, **kw):
    """Require actionable errors for invalid LVM prepare, activate and create arguments."""
    configs = kw.get("config")
    if not isinstance(configs, Mapping):
        raise ConfigError("Mandatory ceph-volume validation config is not provided")

    required = ("device_path", "osd_id", "osd_fsid")
    missing = [
        key
        for key in required
        if configs.get(key) is None or not str(configs.get(key)).strip()
    ]
    if missing:
        raise ConfigError(f"Mandatory config is not provided: {', '.join(missing)}")
    data = configs.get("device_path")
    osd_id = configs.get("osd_id")
    osd_fsid = configs.get("osd_fsid")

    node = ceph_cluster.get_nodes(role="installer")[0]
    lvm = get_ceph_volume(node).lvm
    device_errors = (
        f"lsblk: {data}: not a block device",
        f"Unable to proceed with non-existing device: {data}",
        f"Cannot use device ({data}). A vg/lv path or an existing device is needed",
    )

    out = lvm.prepare(data, check_ec=False)
    _validate_rejection("prepare", node, out, device_errors)

    out = lvm.activate(osd_id, osd_fsid, check_ec=False)
    _validate_rejection(
        "activate",
        node,
        out,
        (
            f"could not find osd.{osd_id} with osd_fsid {osd_fsid}",
            f"could not find {osd_id} with osd_fsid {osd_fsid}",
        ),
    )

    out = lvm.create(data, check_ec=False)
    _validate_rejection("create", node, out, device_errors)
    return 0
