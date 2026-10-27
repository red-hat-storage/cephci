"""Keep udev and optional keyring mounts visible to ceph-volume commands."""

import argparse
import shlex
from unittest.mock import Mock

import pytest

from cli.cephadm.cephadm import CEPH_VOLUME_MOUNTS, CephAdm, ceph_volume_shell


def _parse_shell_mounts(command):
    # Match cephadm's nargs='+' / store parser: repeated --mount loses values.
    parser = argparse.ArgumentParser()
    parser.add_argument("--mount", "-m", nargs="+")
    parser.add_argument("--env", "-e", action="append", default=[])
    parser.add_argument("command", nargs=argparse.REMAINDER)
    tokens = shlex.split(command)
    return parser.parse_args(tokens[tokens.index("shell") + 1 :])


def test_default_volume_query_mounts_host_udev_and_checks_exit_status():
    node = Mock()
    node.exec_command.return_value = ("{}", "")

    assert CephAdm(node).ceph_volume.lvm.list(data="", check_ec=True) == "{}"

    node.exec_command.assert_called_once_with(
        cmd=(
            "cephadm shell --mount /dev:/dev /run/udev:/run/udev /sys:/sys "
            "/run/lvm:/run/lvm /run/lock/lvm:/run/lock/lvm /:/rootfs "
            "--env CEPH_VOLUME_SKIP_RESTORECON=yes -- ceph-volume lvm list  --format json"
        ),
        sudo=True,
        long_running=False,
        check_ec=True,
        timeout=3600,
    )


@pytest.mark.parametrize(
    "kwargs, expected_mount",
    [
        (
            {
                "src_mount": "/tmp/osd.keyring",
                "mount": "/var/lib/ceph/bootstrap-osd/ceph.keyring",
            },
            "/tmp/osd.keyring:/var/lib/ceph/bootstrap-osd/ceph.keyring",
        ),
        ({"mount": "/tmp/osd-spec.yaml"}, "/tmp/osd-spec.yaml:/tmp/osd-spec.yaml"),
        (
            {
                "src_mount": "/tmp/osd keyring",
                "mount": "/var/lib/ceph/bootstrap-osd/ceph.keyring",
            },
            "/tmp/osd keyring:/var/lib/ceph/bootstrap-osd/ceph.keyring",
        ),
    ],
)
def test_volume_prepare_preserves_existing_mount_and_udev_in_one_option(
    kwargs, expected_mount
):
    node = Mock()
    node.exec_command.return_value = ("prepare successful", "")
    CephAdm(node, **kwargs).ceph_volume.lvm.prepare("/dev/vdb", check_ec=True)

    command = node.exec_command.call_args.kwargs["cmd"]
    parsed = _parse_shell_mounts(command)
    assert parsed.mount == [*CEPH_VOLUME_MOUNTS, expected_mount]
    assert parsed.env == ["CEPH_VOLUME_SKIP_RESTORECON=yes"]
    assert shlex.split(command).count("--mount") == 1
    assert parsed.command == [
        "--",
        "ceph-volume",
        "lvm",
        "prepare",
        "--data",
        "/dev/vdb",
        "--bluestore",
    ]


@pytest.mark.parametrize(
    "kwargs, expected_prefix",
    [
        ({}, "cephadm shell"),
        (
            {
                "src_mount": "/tmp/osd.keyring",
                "mount": "/var/lib/ceph/bootstrap-osd/ceph.keyring",
            },
            "cephadm shell --mount /tmp/osd.keyring:/var/lib/ceph/bootstrap-osd/ceph.keyring --",
        ),
        (
            {"mount": "/tmp/spec.yaml"},
            "cephadm shell --mount /tmp/spec.yaml:/tmp/spec.yaml --",
        ),
    ],
)
def test_ceph_commands_retain_existing_shell_and_mount_behavior(
    kwargs, expected_prefix
):
    node = Mock()
    node.exec_command.return_value = ("ceph version", "")
    cephadm = CephAdm(node, **kwargs)

    assert cephadm.ceph.version() == "ceph version"

    assert cephadm.base_shell_cmd == expected_prefix
    assert (
        node.exec_command.call_args.kwargs["cmd"] == f"{expected_prefix} ceph version"
    )
    assert "/run/udev" not in node.exec_command.call_args.kwargs["cmd"]


def test_volume_mounts_preserve_custom_cephadm_image_selection():
    node = Mock()
    node.exec_command.return_value = ("{}", "")
    base_cmd = "cephadm --image example.test/ceph:stable"
    cephadm = CephAdm(node, base_cmd=base_cmd)

    cephadm.ceph_volume.lvm.list(data="", check_ec=True)

    command = node.exec_command.call_args.kwargs["cmd"]
    assert command.startswith(f"{base_cmd} shell --mount ")
    parsed = _parse_shell_mounts(command)
    assert parsed.mount == list(CEPH_VOLUME_MOUNTS)
    assert parsed.env == ["CEPH_VOLUME_SKIP_RESTORECON=yes"]
    assert parsed.command[:3] == ["--", "ceph-volume", "lvm"]


def test_shared_shell_has_host_devices_lvm_runtime_and_root_namespace_access():
    parsed = _parse_shell_mounts(
        f"{ceph_volume_shell()} ceph-volume inventory --format json"
    )
    assert parsed.mount == [
        "/dev:/dev",
        "/run/udev:/run/udev",
        "/sys:/sys",
        "/run/lvm:/run/lvm",
        "/run/lock/lvm:/run/lock/lvm",
        "/:/rootfs",
    ]
    assert parsed.env == ["CEPH_VOLUME_SKIP_RESTORECON=yes"]
    assert parsed.command == ["--", "ceph-volume", "inventory", "--format", "json"]


def test_shared_shell_preserves_loop_fixture_env_and_extra_mount():
    prefix = ceph_volume_shell(
        mounts=("/tmp/owned keyring:/tmp/keyring",),
        env=("CEPH_VOLUME_ALLOW_LOOP_DEVICES=1", "CUSTOM_SETTING=value with spaces"),
    )
    parsed = _parse_shell_mounts(f"{prefix} ceph-volume inventory --format json")
    assert parsed.mount == [*CEPH_VOLUME_MOUNTS, "/tmp/owned keyring:/tmp/keyring"]
    assert parsed.env == [
        "CEPH_VOLUME_SKIP_RESTORECON=yes",
        "CEPH_VOLUME_ALLOW_LOOP_DEVICES=1",
        "CUSTOM_SETTING=value with spaces",
    ]
    assert shlex.split(prefix).count("--mount") == 1
    assert "CEPH_VOLUME_DEBUG=1" not in parsed.env


def test_default_mounts_do_not_trigger_pacific_16_2_0_host_root_relabel():
    parsed = _parse_shell_mounts(
        f"{ceph_volume_shell()} ceph-volume lvm list --format json"
    )
    # v16.2.0 command_shell interprets every third field as ':z', even 'rslave'.
    # Model that historical transformation to protect callers using older Tools.
    legacy_mounts = {}
    for mapping in parsed.mount:
        fields = mapping.split(":")
        legacy_mounts[fields[0]] = fields[1] + ":z" if len(fields) == 3 else fields[1]
    assert legacy_mounts["/"] == "/rootfs"
    assert legacy_mounts["/dev"] == "/dev"
    assert all(not destination.endswith(":z") for destination in legacy_mounts.values())
