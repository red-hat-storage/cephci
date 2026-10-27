"""Regression checks for inventory validation and local disk selection."""

import json
from unittest.mock import Mock

import pytest

from cli.exceptions import ConfigError, OperationFailedError
from tests.ceph_volume import test_ceph_volume_get_inventory_list as inventory
from tests.ceph_volume import test_ceph_volume_sanity as sanity
from tests.ceph_volume.utils import get_available_devices

IMAGE = "example.test/ceph:stable"
JSON_OUTPUT = json.dumps([{"path": "/dev/vdb", "available": True}])


@pytest.fixture
def cluster():
    installer = Mock(hostname="bootstrap")
    other = Mock(hostname="osd-host")
    roles = {"installer": [installer], "osd": [installer, other]}
    responses = {}
    for node in (installer, other):
        responses[node.hostname] = {
            "plain": ("Device Path /dev/vdb", "", 0),
            "json": (JSON_OUTPUT, "", 0),
        }

        def execute(cmd, _node=node, **kwargs):
            output_format = cmd.split("--format=")[1]
            stdout, stderr, status = responses[_node.hostname][output_format]
            _node.exit_status = status
            return stdout, stderr

        node.exec_command.side_effect = execute
    ceph_cluster = Mock()
    ceph_cluster.get_nodes.side_effect = lambda role: roles[role]
    return ceph_cluster, installer, other, responses, roles


@pytest.mark.parametrize("image_key", ["ceph_image", "container_image"])
def test_inventory_uses_configured_image_on_both_hosts(cluster, image_key):
    ceph_cluster, installer, other, _, _ = cluster

    assert inventory.run(ceph_cluster, config={image_key: IMAGE}) == 0
    for node in (installer, other):
        assert node.exec_command.call_count == 2
        for call in node.exec_command.call_args_list:
            assert IMAGE in call.kwargs["cmd"]
            assert "--privileged" in call.kwargs["cmd"]
            assert "--entrypoint ceph-volume" in call.kwargs["cmd"]
            assert call.kwargs["long_running"] is False


def test_inventory_accepts_runner_image(cluster):
    ceph_cluster, _, _, _, _ = cluster

    assert inventory.run(ceph_cluster, config={}, container_image=IMAGE) == 0


def test_explicit_ceph_image_takes_precedence(cluster):
    ceph_cluster, installer, _, _, _ = cluster

    assert (
        inventory.run(
            ceph_cluster,
            config={"ceph_image": IMAGE, "container_image": "example.test/other:tag"},
        )
        == 0
    )
    assert IMAGE in installer.exec_command.call_args.kwargs["cmd"]


@pytest.mark.parametrize("hostname", ["bootstrap", "osd-host"])
@pytest.mark.parametrize("output_format", ["plain", "json"])
def test_inventory_rejects_failed_commands_with_plausible_output(
    cluster, hostname, output_format
):
    ceph_cluster, _, _, responses, _ = cluster
    stdout, stderr, _ = responses[hostname][output_format]
    responses[hostname][output_format] = (stdout, stderr, 1)

    with pytest.raises(OperationFailedError, match="exit status: 1"):
        inventory.run(ceph_cluster, config={"ceph_image": IMAGE})


@pytest.mark.parametrize("hostname", ["bootstrap", "osd-host"])
@pytest.mark.parametrize(
    "output",
    [
        "KeyError: 'ceph.cluster_name'",
        "unrelated malformed JSON",
        "null",
        "{}",
        "[]",
        "[null]",
        '[{"path": ""}]',
        '[{"path": " "}]',
        '[{"path": 3}]',
        "[{}]",
    ],
)
def test_inventory_rejects_invalid_json_or_device_records(cluster, hostname, output):
    ceph_cluster, _, _, responses, _ = cluster
    responses[hostname]["json"] = (output, "", 0)

    with pytest.raises(OperationFailedError, match="inventory"):
        inventory.run(ceph_cluster, config={"ceph_image": IMAGE})


@pytest.mark.parametrize("hostname", ["bootstrap", "osd-host"])
def test_inventory_rejects_empty_plain_output(cluster, hostname):
    ceph_cluster, _, _, responses, _ = cluster
    responses[hostname]["plain"] = (" \n", "", 0)

    with pytest.raises(OperationFailedError, match="plain inventory is empty"):
        inventory.run(ceph_cluster, config={"ceph_image": IMAGE})


@pytest.mark.parametrize("config", [None, [], "invalid", {}, {"ceph_image": " "}])
def test_inventory_requires_config_and_image(cluster, config):
    ceph_cluster, installer, other, _, _ = cluster

    with pytest.raises(ConfigError):
        inventory.run(ceph_cluster, config=config)
    installer.exec_command.assert_not_called()
    other.exec_command.assert_not_called()


@pytest.mark.parametrize("role", ["installer", "osd"])
def test_inventory_requires_both_target_hosts(cluster, role):
    ceph_cluster, installer, other, _, roles = cluster
    roles[role] = [] if role == "installer" else [installer]

    with pytest.raises(ConfigError, match="node"):
        inventory.run(ceph_cluster, config={"ceph_image": IMAGE})
    installer.exec_command.assert_not_called()
    other.exec_command.assert_not_called()


def test_inventory_requires_known_success_status(cluster):
    ceph_cluster, _, _, responses, _ = cluster
    responses["bootstrap"]["plain"] = ("Device Path /dev/vdb", "", None)

    with pytest.raises(OperationFailedError, match="exit status: None"):
        inventory.run(ceph_cluster, config={"ceph_image": IMAGE})


def test_available_devices_excludes_used_system_and_untrusted_availability():
    node = Mock(hostname="installer")
    node.exec_command.return_value = (
        json.dumps(
            [
                {"path": "/dev/vda", "available": False},
                {"path": "/dev/vdc", "available": True},
                {"path": "/dev/vdb", "available": True},
                {"path": "/dev/vdb", "available": True},
                {"path": "/dev/vdd", "available": "true"},
                {"path": "/dev/vde", "available": 1},
                {"path": "/dev/vdf"},
                {"path": "remote-host:/dev/vdg", "available": True},
            ]
        ),
        "",
    )

    assert get_available_devices(node) == ["/dev/vdb", "/dev/vdc"]
    node.exec_command.assert_called_once_with(
        cmd=(
            "cephadm shell --mount /dev:/dev /run/udev:/run/udev /sys:/sys "
            "/run/lvm:/run/lvm /run/lock/lvm:/run/lock/lvm /:/rootfs "
            "--env CEPH_VOLUME_SKIP_RESTORECON=yes -- "
            "ceph-volume inventory --format json"
        ),
        sudo=True,
        check_ec=True,
    )


def test_available_devices_requires_a_free_local_device():
    node = Mock(hostname="installer")
    node.exec_command.return_value = (
        json.dumps([{"path": "/dev/vda", "available": False}]),
        "",
    )

    with pytest.raises(OperationFailedError, match="No available local devices"):
        get_available_devices(node)


@pytest.mark.parametrize("output", ["invalid JSON", "[]", "{}", "[{}]"])
def test_available_devices_rejects_malformed_inventory(output):
    node = Mock(hostname="installer")
    node.exec_command.return_value = (output, "")

    with pytest.raises(OperationFailedError, match="inventory"):
        get_available_devices(node)


def test_sanity_stops_before_prepare_if_no_devices(monkeypatch):
    installer = Mock(hostname="installer")
    installer.exec_command.return_value = (
        json.dumps([{"path": "/dev/vda", "available": False}]),
        "",
    )
    ceph_cluster = Mock()
    ceph_cluster.get_nodes.return_value = [installer]
    cephadm = Mock()
    monkeypatch.setattr(sanity, "CephAdm", cephadm)

    with pytest.raises(OperationFailedError, match="No available local devices"):
        sanity.run(ceph_cluster)
    cephadm.assert_not_called()
