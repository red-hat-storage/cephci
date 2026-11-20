"""Local ceph-volume execution and device inventory validation for tests."""

import json
from shlex import quote

from cli.ceph.ceph_volume.ceph_volume import CephVolume
from cli.exceptions import OperationFailedError

CEPH_VOLUME_MOUNTS = (
    "/dev:/dev",
    "/run/udev:/run/udev",
    "/sys:/sys",
    "/run/lvm:/run/lvm",
    "/run/lock/lvm:/run/lock/lvm",
    # Pacific 16.2.0 rewrites any third mount field as :z; never relabel host /.
    "/:/rootfs",
)


def ceph_volume_shell(base_cmd="cephadm", mounts=(), env=()):
    """Build the host device/LVM context needed by ceph-volume tests.

    Keep all mounts in one option: cephadm's nargs='+' parser replaces values
    when --mount is repeated. The host root mount supports run_on_host/nsenter.
    """
    mount_args = " ".join(quote(path) for path in (*CEPH_VOLUME_MOUNTS, *mounts))
    env_args = " ".join(
        f"--env {quote(value)}" for value in ("CEPH_VOLUME_SKIP_RESTORECON=yes", *env)
    )
    return f"{base_cmd} shell --mount {mount_args} {env_args} --"


def get_ceph_volume(node, mounts=(), env=(), base_cmd="cephadm"):
    """Construct the existing CLI wrapper with the test's host device context."""
    return CephVolume(
        node, ceph_volume_shell(base_cmd=base_cmd, mounts=mounts, env=env)
    )


def parse_inventory_json(output, hostname):
    """Require a nonempty inventory of device records with usable paths."""
    try:
        devices = json.loads(output)
    except (TypeError, json.JSONDecodeError) as error:
        raise OperationFailedError(
            f"Invalid ceph-volume inventory JSON on {hostname}: {error}"
        ) from error
    if not isinstance(devices, list) or not devices:
        raise OperationFailedError(
            f"ceph-volume inventory on {hostname} must contain a nonempty device list"
        )
    if any(
        not isinstance(device, dict)
        or not isinstance(device.get("path"), str)
        or not device["path"].strip()
        for device in devices
    ):
        raise OperationFailedError(
            f"ceph-volume inventory on {hostname} contains a device without a valid path"
        )
    return devices


def get_available_devices(node):
    """Select only available local devices, excluding used and system disks."""
    output, _ = node.exec_command(
        cmd=f"{ceph_volume_shell()} ceph-volume inventory --format json",
        sudo=True,
        check_ec=True,
    )
    devices = parse_inventory_json(output, node.hostname)
    paths = sorted(
        {
            device["path"]
            for device in devices
            if device.get("available") is True and device["path"].startswith("/dev/")
        }
    )
    if not paths:
        raise OperationFailedError(
            f"No available local devices for ceph-volume on {node.hostname}"
        )
    return paths
