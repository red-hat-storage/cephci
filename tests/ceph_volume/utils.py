"""Local device inventory validation for ceph-volume tests."""

import json

from cli.cephadm.cephadm import ceph_volume_shell
from cli.exceptions import OperationFailedError


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
