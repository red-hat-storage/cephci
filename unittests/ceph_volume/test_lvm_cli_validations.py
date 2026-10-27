"""Regression tests for the ceph-volume invalid-argument workflow."""

from unittest.mock import Mock

import pytest

from cli.exceptions import ConfigError, OperationFailedError
from tests.ceph_volume import test_ceph_volume_lvm_cli_validations as workflow

CONFIG = {
    "device_path": "/dev/vdz",
    "osd_id": "70",
    "osd_fsid": "299b7964-4753-4711-a449-9b79d70test",
}
DEVICE_ERROR = "lsblk: /dev/vdz: not a block device"
ACTIVATE_ERROR = (
    "RuntimeError: could not find osd.70 with osd_fsid " + CONFIG["osd_fsid"]
)


@pytest.fixture
def cluster():
    """Exercise the real CLI wrappers with mocked command results."""
    node = Mock()
    responses = {
        "prepare": ("", DEVICE_ERROR, 1),
        "activate": ("", ACTIVATE_ERROR, 1),
        "create": ("", DEVICE_ERROR, 1),
    }

    def execute(cmd, **kwargs):
        command = cmd.split(" lvm ")[1].split()[0]
        stdout, stderr, status = responses[command]
        node.exit_status = status
        return stdout, stderr

    node.exec_command.side_effect = execute
    ceph_cluster = Mock()
    ceph_cluster.get_nodes.return_value = [node]
    return ceph_cluster, node, responses


@pytest.mark.parametrize(
    "device_error",
    [
        DEVICE_ERROR,
        "ceph-volume lvm prepare: error: Unable to proceed with non-existing device: /dev/vdz",
        "RuntimeError: Cannot use device (/dev/vdz). A vg/lv path or an existing device is needed",
    ],
)
@pytest.mark.parametrize("osd_prefix", ["osd.", ""])
def test_accepts_expected_validation_errors(cluster, device_error, osd_prefix):
    ceph_cluster, node, responses = cluster
    responses["prepare"] = ("", device_error, 2)
    responses["create"] = ("", device_error, 2)
    responses["activate"] = (
        "",
        f"RuntimeError: could not find {osd_prefix}70 with osd_fsid {CONFIG['osd_fsid']}",
        1,
    )

    assert workflow.run(ceph_cluster, config=CONFIG) == 0
    assert node.exec_command.call_count == 3
    for call in node.exec_command.call_args_list:
        assert call.kwargs["check_ec"] is False


@pytest.mark.parametrize("command", ["prepare", "activate", "create"])
def test_rejects_unexpected_success_even_with_expected_error(cluster, command):
    ceph_cluster, _, responses = cluster
    stdout, stderr, _ = responses[command]
    responses[command] = (stdout, stderr, 0)

    with pytest.raises(OperationFailedError, match="unexpectedly accepted"):
        workflow.run(ceph_cluster, config=CONFIG)


@pytest.mark.parametrize("command", ["prepare", "activate", "create"])
@pytest.mark.parametrize("error", ["", "unrelated command failure"])
def test_rejects_missing_or_unrelated_validation_error(cluster, command, error):
    ceph_cluster, _, responses = cluster
    responses[command] = ("", error, 1)

    with pytest.raises(OperationFailedError, match="expected validation error"):
        workflow.run(ceph_cluster, config=CONFIG)


@pytest.mark.parametrize("command", ["prepare", "activate", "create"])
@pytest.mark.parametrize(
    "crash", ["Traceback (most recent call last):", "Segmentation fault (core dumped)"]
)
def test_rejects_crash_even_with_expected_validation_error(cluster, command, crash):
    ceph_cluster, _, responses = cluster
    _, stderr, _ = responses[command]
    responses[command] = (crash, stderr, 1)

    with pytest.raises(OperationFailedError, match="crashed"):
        workflow.run(ceph_cluster, config=CONFIG)


@pytest.mark.parametrize("status", [-11, 139])
def test_rejects_signal_exit_even_with_expected_error(cluster, status):
    ceph_cluster, _, responses = cluster
    responses["prepare"] = ("", DEVICE_ERROR, status)

    with pytest.raises(OperationFailedError, match="crashed"):
        workflow.run(ceph_cluster, config=CONFIG)


def test_rejects_success_message_with_nonzero_status(cluster):
    ceph_cluster, _, responses = cluster
    responses["prepare"] = (
        "ceph-volume lvm prepare successful for: /dev/vdz",
        DEVICE_ERROR,
        1,
    )

    with pytest.raises(OperationFailedError, match="reported success"):
        workflow.run(ceph_cluster, config=CONFIG)


def test_device_error_must_identify_configured_device(cluster):
    ceph_cluster, _, responses = cluster
    responses["prepare"] = ("", "lsblk: 70: not a block device", 1)

    with pytest.raises(OperationFailedError, match="expected validation error"):
        workflow.run(ceph_cluster, config=CONFIG)


def test_activation_error_must_identify_configured_osd_and_fsid(cluster):
    ceph_cluster, _, responses = cluster
    responses["activate"] = (
        "",
        "RuntimeError: could not find osd.71 with osd_fsid another-fsid",
        1,
    )

    with pytest.raises(OperationFailedError, match="expected validation error"):
        workflow.run(ceph_cluster, config=CONFIG)


@pytest.mark.parametrize("config", [None, {}, [], "invalid"])
def test_missing_config_fails_before_any_command(cluster, config):
    ceph_cluster, node, _ = cluster

    with pytest.raises(ConfigError, match="Mandatory"):
        workflow.run(ceph_cluster, config=config)
    node.exec_command.assert_not_called()


@pytest.mark.parametrize("key", ["device_path", "osd_id", "osd_fsid"])
@pytest.mark.parametrize("value", [None, "", " "])
def test_missing_required_value_fails_before_any_command(cluster, key, value):
    ceph_cluster, node, _ = cluster
    config = dict(CONFIG, **{key: value})

    with pytest.raises(ConfigError, match=key):
        workflow.run(ceph_cluster, config=config)
    node.exec_command.assert_not_called()


def test_missing_exit_status_cannot_pass(cluster):
    ceph_cluster, _, responses = cluster
    responses["prepare"] = ("", DEVICE_ERROR, None)

    with pytest.raises(OperationFailedError, match="Unable to verify"):
        workflow.run(ceph_cluster, config=CONFIG)
