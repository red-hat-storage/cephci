"""Regression checks for ceph-volume test failures previously reported as passes."""

import json
from unittest.mock import MagicMock

import pytest

from cli.exceptions import ConfigError, OperationFailedError, UnexpectedStateError
from tests.ceph_volume import test_ceph_volume_sanity as sanity
from tests.ceph_volume import (
    test_osd_deploy_with_spec_crush_device_class as crush_class,
)


@pytest.fixture
def sanity_environment(monkeypatch):
    installer = MagicMock(hostname="installer")
    cluster = MagicMock()
    cluster.get_nodes.return_value = [installer]
    cephadm = MagicMock()
    lvm = cephadm.ceph_volume.lvm
    lvm.prepare.return_value = "prepare successful"
    lvm.activate.return_value = "activate successful"
    lvm.zap.return_value = "zap successful"
    lvm.list.return_value = json.dumps(
        {"0": [{"tags": {"ceph.osd_id": "0", "ceph.osd_fsid": "osd-fsid"}}]}
    )
    monkeypatch.setattr(sanity, "CephAdm", MagicMock(return_value=cephadm))
    monkeypatch.setattr(
        sanity, "create_keyring_file", MagicMock(return_value="/tmp/osd.keyring")
    )
    monkeypatch.setattr(
        sanity, "get_available_devices", MagicMock(return_value=["/dev/vdb"])
    )
    reboot = MagicMock(return_value=True)
    monkeypatch.setattr(sanity, "reboot_node", reboot)
    return cluster, cephadm, lvm, reboot


@pytest.mark.parametrize(
    "operation, error, expected_reboots",
    [
        ("prepare", "Failed to prepare disk: /dev/vdb", 0),
        ("activate", "Failed to activate osd: 0", 1),
        ("zap", "Failed to zap disk: /dev/vdb", 2),
    ],
)
def test_sanity_stops_after_failed_operation(
    sanity_environment, operation, error, expected_reboots
):
    cluster, cephadm, lvm, reboot = sanity_environment
    getattr(lvm, operation).return_value = "operation failed"

    with pytest.raises(OperationFailedError, match=error):
        sanity.run(cluster)

    assert reboot.call_count == expected_reboots
    cephadm.execute.assert_not_called()
    if operation == "prepare":
        lvm.list.assert_not_called()
        lvm.activate.assert_not_called()
    if operation in ("prepare", "activate"):
        lvm.deactivate.assert_not_called()
        lvm.zap.assert_not_called()


def test_sanity_success_purges_zapped_osd(sanity_environment):
    cluster, cephadm, lvm, reboot = sanity_environment

    assert sanity.run(cluster) == 0

    assert reboot.call_count == 2
    lvm.prepare.assert_called_once_with(data="/dev/vdb", check_ec=True)
    lvm.list.assert_called_once_with(data="/dev/vdb", check_ec=True)
    lvm.activate.assert_called_once_with(
        "0", "osd-fsid", bluestore=False, check_ec=True
    )
    lvm.deactivate.assert_called_once_with("0", "osd-fsid", check_ec=True)
    lvm.zap.assert_called_once_with(data="/dev/vdb", check_ec=True)
    cephadm.execute.assert_called_once_with(
        cmd="cephadm shell -- ceph osd purge 0 --yes-i-really-mean-it",
        sudo=True,
        check_ec=True,
    )
    cephadm.ceph.auth.rm.assert_not_called()


@pytest.mark.parametrize("osd_id", ["12", 12])
def test_sanity_purges_numeric_osd_id_from_prepared_metadata(
    sanity_environment, osd_id
):
    cluster, cephadm, lvm, _ = sanity_environment
    lvm.list.return_value = json.dumps(
        {"12": [{"tags": {"ceph.osd_id": osd_id, "ceph.osd_fsid": "osd-fsid"}}]}
    )

    assert sanity.run(cluster) == 0

    cephadm.execute.assert_called_once_with(
        cmd="cephadm shell -- ceph osd purge 12 --yes-i-really-mean-it",
        sudo=True,
        check_ec=True,
    )


@pytest.mark.parametrize(
    "osd_id", [None, "", {}, "0; unexpected-command", "-1", "osd.0", True, 0.5]
)
def test_sanity_rejects_invalid_osd_id_before_osd_commands(sanity_environment, osd_id):
    cluster, cephadm, lvm, _ = sanity_environment
    lvm.list.return_value = json.dumps(
        {"0": [{"tags": {"ceph.osd_id": osd_id, "ceph.osd_fsid": "osd-fsid"}}]}
    )

    with pytest.raises(OperationFailedError, match="Invalid OSD ID"):
        sanity.run(cluster)

    lvm.activate.assert_not_called()
    lvm.deactivate.assert_not_called()
    lvm.zap.assert_not_called()
    cephadm.execute.assert_not_called()


def test_sanity_fails_if_osd_purge_fails(sanity_environment):
    cluster, cephadm, lvm, _ = sanity_environment
    cephadm.execute.side_effect = OperationFailedError("OSD purge failed")

    with pytest.raises(OperationFailedError, match="OSD purge failed"):
        sanity.run(cluster)

    lvm.zap.assert_called_once_with(data="/dev/vdb", check_ec=True)
    cephadm.ceph.auth.rm.assert_not_called()


@pytest.fixture
def crush_environment(monkeypatch):
    installer = MagicMock()
    cluster = MagicMock()
    cluster.get_nodes.return_value = [installer]
    cephadm = MagicMock()
    monkeypatch.setattr(crush_class, "CephAdm", MagicMock(return_value=cephadm))
    create_yaml = MagicMock(return_value="/tmp/osd.yaml")
    monkeypatch.setattr(crush_class, "create_yaml_config", create_yaml)
    monkeypatch.setattr(crush_class, "wait_for_cluster_health", MagicMock())
    return installer, cluster, cephadm, create_yaml


@pytest.mark.parametrize("actual_class", ["hdd", None])
def test_crush_class_checks_osds_after_non_osd_bucket(crush_environment, actual_class):
    _, cluster, cephadm, _ = crush_environment
    cephadm.ceph.osd.tree.return_value = json.dumps(
        {
            "nodes": [
                {"type": "root", "name": "default"},
                {"type": "osd", "name": "osd.0", "device_class": "ssd"},
                {"type": "osd", "name": "osd.1", "device_class": actual_class},
            ]
        }
    )

    with pytest.raises(UnexpectedStateError, match="crush_device_class for osd.1"):
        crush_class.run(cluster, config={"spec": {"crush_device_class": "ssd"}})


@pytest.mark.parametrize("nodes", [[], [{"type": "root", "name": "default"}]])
def test_crush_class_rejects_tree_without_osds(crush_environment, nodes):
    _, cluster, cephadm, _ = crush_environment
    cephadm.ceph.osd.tree.return_value = json.dumps({"nodes": nodes})

    with pytest.raises(UnexpectedStateError, match="No OSDs found"):
        crush_class.run(cluster, config={"spec": {"crush_device_class": "ssd"}})


@pytest.mark.parametrize(
    "spec",
    [
        {"service_type": "osd", "crush_device_class": "hdd"},
        {"service_type": "osd", "spec": {"crush_device_class": "hdd"}},
    ],
)
def test_crush_class_applies_spec_and_validates_configured_class(
    crush_environment, spec
):
    installer, cluster, cephadm, create_yaml = crush_environment
    cephadm.ceph.osd.tree.return_value = json.dumps(
        {
            "nodes": [
                {"type": "root", "name": "default"},
                {"type": "osd", "name": "osd.0", "device_class": "hdd"},
                {"type": "osd", "name": "osd.1", "device_class": "hdd"},
            ]
        }
    )

    assert crush_class.run(cluster, config={"spec": spec}) == 0

    create_yaml.assert_called_once_with(installer, spec)
    cephadm.ceph.orch.apply.assert_called_once_with(
        pos_args=[], input="/tmp/osd.yaml", check_ec=True
    )


@pytest.mark.parametrize("config", [None, {}, {"spec": {"service_type": "osd"}}])
def test_crush_class_requires_configured_device_class(crush_environment, config):
    _, cluster, cephadm, create_yaml = crush_environment

    with pytest.raises(ConfigError, match="must define 'crush_device_class'"):
        crush_class.run(cluster, config=config)

    create_yaml.assert_not_called()
    cephadm.ceph.orch.apply.assert_not_called()
