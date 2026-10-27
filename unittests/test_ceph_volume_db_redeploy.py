"""Regression checks for complete, host-scoped dedicated DB validation."""

from copy import deepcopy
from json import dumps
from types import SimpleNamespace
from unittest.mock import MagicMock, call

import pytest

from ceph.ceph import CommandFailed
from cli.exceptions import OperationFailedError
from tests.ceph_volume import test_ceph_volume_non_collocated as non_collocated
from tests.ceph_volume import test_ceph_volume_redeploy as redeploy


def _osd(data="/dev/vdb", db="/dev/vdc", service="osd_hdd", fsid=None):
    entries = [
        {
            "type": "block",
            "devices": [data],
            "tags": {
                "ceph.osdspec_affinity": service,
                "ceph.osd_fsid": fsid or f"original-{data.rsplit('/', 1)[-1]}",
            },
        }
    ]
    if db is not None:
        entries.append({"type": "db", "devices": [db]})
    return entries


def _cluster():
    installer = SimpleNamespace(hostname="admin.example")
    node = SimpleNamespace(hostname="osd.example", ceph_nodename="node1")
    cluster = MagicMock()
    cluster.get_nodes.side_effect = lambda role=None: {
        "installer": [installer],
        "osd": [node],
        None: [node, installer],
    }[role]
    return cluster, installer, node


def _config():
    return {
        "spec": {
            "service_type": "osd",
            "service_id": "osd_hdd",
            "placement": {"hosts": ["node0"]},
            "spec": {"data_devices": {"size": "16GB"}},
        }
    }


def _expected_replacement_waits(installer, node, before, original_lvm):
    calls = [call(installer, node, before)]
    replaced = {}
    for osd_id, (data, _) in before.items():
        replaced = {
            **replaced,
            data: {non_collocated._get_osd_fsid(original_lvm, osd_id)},
        }
        calls.append(call(installer, node, before, replaced))
    return calls


def test_db_mapping_reads_all_entries_independent_of_order():
    lvm_list = {"1": _osd() + [{"type": "db", "devices": ["/dev/vdd"]}]}
    assert non_collocated._get_osd_db_id(["1"], lvm_list) == {
        "1": ("/dev/vdc", "/dev/vdd")
    }


@pytest.mark.parametrize(
    "osd_ids,lvm_list",
    [
        ([], {}),
        (["1"], {}),
        (["1"], {"1": _osd(db=None)}),
        (["1", "2"], {"1": _osd(), "2": _osd(db=None)}),
        (["1"], {"1": [{"type": "db", "devices": []}]}),
        (["1"], {"1": [{"type": "db", "devices": [""]}]}),
        (["1"], {"1": [{"type": "db", "devices": "/dev/vdc"}]}),
        (["1"], {"1": [{"type": "db", "devices": [None]}]}),
    ],
)
def test_incomplete_db_mapping_cannot_pass(osd_ids, lvm_list):
    with pytest.raises(OperationFailedError):
        non_collocated._get_osd_db_id(osd_ids, lvm_list)


@pytest.mark.parametrize(
    "entries",
    [_osd(db="/dev/vdb"), [{"type": "db", "devices": ["/dev/vdc"]}]],
)
def test_layout_requires_separate_data_and_db_devices(entries):
    with pytest.raises(OperationFailedError):
        non_collocated._get_osd_layout({"1": entries})


def test_layout_rejects_db_pool_used_by_another_target_data_osd():
    with pytest.raises(OperationFailedError, match="DB pool overlaps"):
        non_collocated._get_osd_layout({"1": _osd(), "2": _osd("/dev/vdc", "/dev/vdd")})


def test_redeployment_accepts_new_ids_and_valid_db_reassignment():
    before = non_collocated._get_osd_layout(
        {"1": _osd(), "2": _osd("/dev/vdd", "/dev/vde")}
    )
    after = {"8": _osd(db="/dev/vde"), "9": _osd("/dev/vdd", "/dev/vdc")}
    assert set(non_collocated._validate_redeployment(before, after)) == {"8", "9"}


def test_unrelated_osds_cannot_supply_the_expected_db():
    before = non_collocated._get_osd_layout({"1": _osd()})
    after = {"8": _osd(db=None), "9": _osd("/dev/vdd", "/dev/vdc")}
    with pytest.raises(OperationFailedError, match="OSD 8 has no dedicated db"):
        non_collocated._validate_redeployment(before, after)


def test_unrelated_colocated_osds_are_outside_the_target_scope():
    before = non_collocated._get_osd_layout({"1": _osd()})
    after = {"8": _osd(), "9": _osd("/dev/vdd", db=None)}
    assert set(non_collocated._validate_redeployment(before, after)) == {"8"}


@pytest.mark.parametrize(
    "after",
    [
        {},
        {"8": _osd(db=None)},
        {"8": _osd(db="/dev/vdb")},
        {"8": _osd(db="/dev/foreign")},
        {"8": _osd("/dev/foreign", "/dev/vdc")},
        {"8": _osd(), "9": _osd()},
    ],
)
def test_invalid_redeployment_fails(after):
    before = non_collocated._get_osd_layout({"1": _osd()})
    with pytest.raises(OperationFailedError):
        non_collocated._validate_redeployment(before, after)


def test_redeployment_cannot_pass_with_missing_data_device():
    before = non_collocated._get_osd_layout(
        {"1": _osd(), "2": _osd("/dev/vdd", "/dev/vdc")}
    )
    with pytest.raises(OperationFailedError, match="data devices"):
        non_collocated._validate_redeployment(before, {"8": _osd()})


def test_service_scope_excludes_unrelated_osds():
    lvm_list = {"1": _osd(), "2": _osd(service="other")}
    assert non_collocated._get_service_lvm_list(lvm_list, "osd_hdd") == {
        "1": lvm_list["1"]
    }
    with pytest.raises(OperationFailedError, match="service_id"):
        non_collocated._get_service_lvm_list(lvm_list, None)


def test_local_lvm_reads_do_not_require_a_running_osd_container(monkeypatch):
    node = SimpleNamespace(hostname="osd.example")
    cephadm = MagicMock()
    cephadm.return_value.ceph_volume.lvm.list.return_value = "{}"
    monkeypatch.setattr(non_collocated, "CephAdm", cephadm)
    assert non_collocated._get_host_lvm_list(node) == {}
    cephadm.assert_called_once_with(node)
    cephadm.return_value.ceph_volume.lvm.list.assert_called_once_with(
        data="", check_ec=True
    )


def test_wait_retries_partial_deployment_and_checks_new_osd_state(monkeypatch):
    before = non_collocated._get_osd_layout({"1": _osd()})
    monkeypatch.setattr(non_collocated, "WaitUntil", lambda **kw: iter(range(3)))
    monkeypatch.setattr(
        non_collocated,
        "_get_host_lvm_list",
        MagicMock(side_effect=[{}, {"8": _osd(db=None)}, {"8": _osd()}]),
    )
    cephadm = MagicMock()
    cephadm.return_value.execute.return_value = (
        dumps({"nodes": [{"type": "osd", "id": 8, "status": "up"}]}),
        "",
    )
    monkeypatch.setattr(non_collocated, "CephAdm", cephadm)
    node = SimpleNamespace(hostname="osd.example")
    result = non_collocated._wait_for_redeployment("installer", node, before)
    assert list(result) == ["8"]
    cephadm.assert_called_once_with("installer")
    cephadm.return_value.execute.assert_called_once_with(
        cmd="cephadm shell -- ceph osd tree --format json", sudo=True, check_ec=True
    )


def test_timeout_reports_the_missing_db(monkeypatch):
    before = non_collocated._get_osd_layout({"1": _osd()})
    monkeypatch.setattr(non_collocated, "WaitUntil", lambda **kw: iter(range(1)))
    monkeypatch.setattr(
        non_collocated, "_get_host_lvm_list", lambda node: {"8": _osd(db=None)}
    )
    with pytest.raises(OperationFailedError, match="osd.example.*no dedicated db"):
        non_collocated._wait_for_redeployment(
            "installer", SimpleNamespace(hostname="osd.example"), before
        )


@pytest.mark.parametrize(
    "status", ["[]\n", "No OSD remove/replace operations reported"]
)
def test_removal_wait_accepts_empty_json_and_text(monkeypatch, status):
    cephadm = MagicMock()
    cephadm.return_value.execute.return_value = (status, "")
    monkeypatch.setattr(non_collocated, "CephAdm", cephadm)
    monkeypatch.setattr(non_collocated, "WaitUntil", lambda **kw: iter(range(1)))
    non_collocated._wait_for_osd_removal("installer")
    cephadm.return_value.execute.assert_called_once_with(
        cmd="cephadm shell -- ceph orch osd rm status --format json",
        sudo=True,
        check_ec=True,
    )


def test_spec_resolution_keeps_input_and_supports_multi_digit_nodes():
    spec = _config()["spec"]
    spec["placement"]["hosts"] = ["node10", "actual.example"]
    original = deepcopy(spec)
    cluster = MagicMock()
    cluster.get_nodes.return_value = [
        SimpleNamespace(hostname=f"host-{number}.example") for number in range(11)
    ]
    resolved = non_collocated._resolve_spec_hosts(cluster, spec)
    assert resolved["placement"]["hosts"] == ["host-10.example", "actual.example"]
    assert spec == original


def test_placement_must_select_an_osd_host():
    cluster, _, _ = _cluster()
    with pytest.raises(OperationFailedError, match="No OSD hosts"):
        non_collocated._get_spec_osd_nodes(
            cluster, {"placement": {"hosts": ["another.example"]}}
        )


def test_non_collocated_restores_managed_service_on_removal_failure(monkeypatch):
    cluster, _, node = _cluster()
    config = _config()
    original = deepcopy(config)
    monkeypatch.setattr(non_collocated, "get_configs", MagicMock())
    inventory = MagicMock(return_value={"1": _osd()})
    monkeypatch.setattr(non_collocated, "_get_host_lvm_list", inventory)
    specs = []

    def capture_spec(installer, spec):
        specs.append(deepcopy(spec))
        return "/tmp/spec.yaml"

    monkeypatch.setattr(non_collocated, "create_yaml_config", capture_spec)
    cephadm = MagicMock()
    cephadm.return_value.execute.side_effect = OperationFailedError(
        "OSD removal failed"
    )
    monkeypatch.setattr(non_collocated, "CephAdm", cephadm)
    with pytest.raises(OperationFailedError, match="OSD removal failed"):
        non_collocated.run(cluster, config=config)
    inventory.assert_called_once_with(node)
    assert [spec["unmanaged"] for spec in specs] == [True, False]
    assert all(
        command.kwargs["check_ec"] is True
        for command in cephadm.return_value.ceph.orch.apply.call_args_list
    )
    assert config == original


def test_existing_osds_do_not_satisfy_new_volume_deployment(monkeypatch):
    cluster, _, _ = _cluster()
    existing = {str(i): _osd(f"/dev/data{i}") for i in range(4)}
    monkeypatch.setattr(redeploy, "get_configs", MagicMock())
    monkeypatch.setattr(redeploy, "_add_node_volumes", MagicMock())
    monkeypatch.setattr(redeploy, "_get_host_lvm_list", lambda node: existing)
    monkeypatch.setattr(redeploy, "WaitUntil", lambda **kw: iter(range(1)))
    monkeypatch.setattr(redeploy, "create_yaml_config", MagicMock())
    cephadm = MagicMock()
    monkeypatch.setattr(redeploy, "CephAdm", cephadm)
    with pytest.raises(OperationFailedError, match="4 new OSDs"):
        redeploy.run(cluster, config=_config())
    cephadm.return_value.ceph.orch.osd.rm.assert_not_called()


def test_redeploy_removes_only_new_service_osds(monkeypatch):
    cluster, installer, node = _cluster()
    existing = {"1": _osd("/dev/old", service="other")}
    new = {str(i): _osd(f"/dev/data{i}") for i in range(10, 14)}
    monkeypatch.setattr(redeploy, "get_configs", MagicMock())
    monkeypatch.setattr(redeploy, "_add_node_volumes", MagicMock())
    monkeypatch.setattr(
        redeploy,
        "_get_host_lvm_list",
        MagicMock(side_effect=[existing, {**existing, **new}]),
    )
    monkeypatch.setattr(redeploy, "WaitUntil", lambda **kw: iter(range(1)))
    monkeypatch.setattr(redeploy, "create_yaml_config", MagicMock())
    cephadm = MagicMock()
    monkeypatch.setattr(redeploy, "CephAdm", cephadm)
    monkeypatch.setattr(redeploy, "_wait_for_osd_removal", MagicMock())
    remove = MagicMock()
    monkeypatch.setattr(redeploy, "_remove_osd", remove)
    wait = MagicMock()
    monkeypatch.setattr(redeploy, "_wait_for_redeployment", wait)
    assert redeploy.run(cluster, config=_config()) == 0
    assert remove.call_args_list == [call(installer, osd_id) for osd_id in new]
    before = non_collocated._get_osd_layout(new)
    assert wait.call_args_list == _expected_replacement_waits(
        installer, node, before, new
    )


def test_preattached_mode_reuses_existing_service_without_cloud_calls(monkeypatch):
    cluster, installer, node = _cluster()
    existing = {str(i): _osd(f"/dev/data{i}") for i in range(4)}
    config = _config()
    config["create_volumes"] = False
    config["spec"]["spec"]["data_devices"] = {"paths": ["/dev/data0"]}
    monkeypatch.setattr(redeploy, "get_configs", MagicMock())
    cloud = MagicMock()
    monkeypatch.setattr(redeploy, "_add_node_volumes", cloud)
    monkeypatch.setattr(redeploy, "_get_host_lvm_list", lambda node: existing)
    monkeypatch.setattr(redeploy, "create_yaml_config", MagicMock())
    cephadm = MagicMock()
    monkeypatch.setattr(redeploy, "CephAdm", cephadm)
    monkeypatch.setattr(redeploy, "_wait_for_osd_removal", MagicMock())
    remove = MagicMock()
    monkeypatch.setattr(redeploy, "_remove_osd", remove)
    wait = MagicMock()
    monkeypatch.setattr(redeploy, "_wait_for_redeployment", wait)
    assert redeploy.run(cluster, config=config) == 0
    cloud.assert_not_called()
    assert remove.call_args_list == [call(installer, osd_id) for osd_id in existing]
    before = non_collocated._get_osd_layout(existing)
    assert wait.call_args_list == _expected_replacement_waits(
        installer, node, before, existing
    )


def test_preattached_mode_rejects_incomplete_initial_service(monkeypatch):
    cluster, _, _ = _cluster()
    config = _config()
    config["create_volumes"] = False
    monkeypatch.setattr(redeploy, "get_configs", MagicMock())
    monkeypatch.setattr(redeploy, "_get_host_lvm_list", lambda node: {"1": _osd()})
    cephadm = MagicMock()
    monkeypatch.setattr(redeploy, "CephAdm", cephadm)
    with pytest.raises(OperationFailedError, match="Expected 4 existing service OSDs"):
        redeploy.run(cluster, config=config)
    cephadm.assert_not_called()


def test_remove_command_requires_success_and_does_not_emit_check_ec_cli_flag(
    monkeypatch,
):
    cephadm = MagicMock()
    cephadm.return_value.execute.side_effect = OperationFailedError("remove rejected")
    monkeypatch.setattr(non_collocated, "CephAdm", cephadm)
    with pytest.raises(OperationFailedError, match="remove rejected"):
        non_collocated._remove_osd("installer", "12")
    cephadm.return_value.execute.assert_called_once_with(
        cmd="cephadm shell -- ceph orch osd rm 12 --zap --force",
        sudo=True,
        check_ec=True,
    )
    cephadm.return_value.ceph.orch.osd.rm.assert_not_called()


def test_status_command_failure_cannot_be_mistaken_for_empty_queue(monkeypatch):
    cephadm = MagicMock()
    cephadm.return_value.execute.side_effect = OperationFailedError("status denied")
    monkeypatch.setattr(non_collocated, "CephAdm", cephadm)
    monkeypatch.setattr(non_collocated, "WaitUntil", lambda **kw: iter(range(1)))
    with pytest.raises(OperationFailedError, match="status denied"):
        non_collocated._wait_for_osd_removal("installer")


def test_local_lvm_command_failure_propagates(monkeypatch):
    cephadm = MagicMock()
    cephadm.return_value.ceph_volume.lvm.list.side_effect = OperationFailedError(
        "LVM command failed"
    )
    monkeypatch.setattr(non_collocated, "CephAdm", cephadm)
    with pytest.raises(OperationFailedError, match="LVM command failed"):
        non_collocated._get_host_lvm_list(SimpleNamespace(hostname="osd.example"))


def test_local_lvm_inventory_must_be_an_osd_mapping(monkeypatch):
    cephadm = MagicMock()
    cephadm.return_value.ceph_volume.lvm.list.return_value = "[]"
    monkeypatch.setattr(non_collocated, "CephAdm", cephadm)
    with pytest.raises(OperationFailedError, match="Invalid LVM inventory"):
        non_collocated._get_host_lvm_list(SimpleNamespace(hostname="osd.example"))


@pytest.mark.parametrize("new_osd_id", ["1", "8"])
def test_unchanged_fsid_cannot_prove_osd_replacement(new_osd_id):
    initial = {"1": _osd(fsid="old-identity")}
    before = non_collocated._get_osd_layout(initial)
    actual = {new_osd_id: _osd(fsid="old-identity")}
    with pytest.raises(OperationFailedError, match="still have removed OSD FSID"):
        non_collocated._validate_redeployment(
            before, actual, {before["1"][0]: {"old-identity"}}
        )


def test_fsid_change_proves_replacement_even_when_numeric_id_is_reused():
    before = non_collocated._get_osd_layout({"1": _osd(fsid="old-identity")})
    actual = {"1": _osd(fsid="replacement-identity")}
    assert (
        non_collocated._validate_redeployment(
            before, actual, {before["1"][0]: {"old-identity"}}
        )
        == before
    )


def test_fsid_change_is_required_only_for_selected_data_devices():
    original = {
        "1": _osd(fsid="replaced-identity"),
        "2": _osd("/dev/vdd", "/dev/vdc", fsid="unchanged-identity"),
    }
    before = non_collocated._get_osd_layout(original)
    actual = {"8": _osd(fsid="replacement-identity"), "2": original["2"]}
    after = non_collocated._validate_redeployment(
        before, actual, {before["1"][0]: {"replaced-identity"}}
    )
    assert set(after) == {"8", "2"}


def test_missing_new_fsid_cannot_pass_replacement_validation():
    before = non_collocated._get_osd_layout({"1": _osd(fsid="old-identity")})
    actual = {"8": _osd()}
    del actual["8"][0]["tags"]["ceph.osd_fsid"]
    with pytest.raises(OperationFailedError, match="no unique block LV OSD FSID"):
        non_collocated._validate_redeployment(
            before, actual, {before["1"][0]: {"old-identity"}}
        )


def test_wait_retries_stale_original_osd_until_its_fsid_changes(monkeypatch):
    original = {"1": _osd(fsid="old-identity")}
    before = non_collocated._get_osd_layout(original)
    replacement = {"1": _osd(fsid="replacement-identity")}
    inventory = MagicMock(side_effect=[original, replacement])
    monkeypatch.setattr(non_collocated, "_get_host_lvm_list", inventory)
    monkeypatch.setattr(non_collocated, "WaitUntil", lambda **kw: iter(range(2)))
    cephadm = MagicMock()
    cephadm.return_value.execute.return_value = (
        dumps({"nodes": [{"type": "osd", "id": 1, "status": "up"}]}),
        "",
    )
    monkeypatch.setattr(non_collocated, "CephAdm", cephadm)
    node = SimpleNamespace(hostname="osd.example")
    non_collocated._wait_for_redeployment(
        "installer", node, before, {before["1"][0]: {"old-identity"}}
    )
    assert inventory.call_count == 2
    cephadm.return_value.execute.assert_called_once_with(
        cmd="cephadm shell -- ceph osd tree --format json", sudo=True, check_ec=True
    )


@pytest.mark.parametrize("nested", [True, False])
def test_reused_selectors_come_from_actual_layout_and_preserve_db_size(nested):
    selectors = {
        "data_devices": {"paths": ["/dev/wrong-data"]},
        "db_devices": {"paths": ["/dev/wrong-db"]},
        "block_db_size": "12G",
    }
    spec = {"service_id": "osd_hdd", "spec": selectors} if nested else selectors
    before = non_collocated._get_osd_layout(
        {"1": _osd("/dev/vde", "/dev/vdc"), "2": _osd("/dev/vdf", "/dev/vdd")}
    )
    non_collocated._reuse_existing_device_selectors(spec, before)
    actual = spec["spec"] if nested else spec
    assert actual["data_devices"] == {"paths": ["/dev/vde", "/dev/vdf"]}
    assert actual["db_devices"] == {"paths": ["/dev/vdc", "/dev/vdd"]}
    assert actual["block_db_size"] == "12G"


@pytest.mark.parametrize("module", [non_collocated, redeploy])
def test_reuse_requires_existing_matching_service_before_apply(monkeypatch, module):
    cluster, _, _ = _cluster()
    config = _config()
    config.update(reuse_existing_devices=True, create_volumes=False)
    monkeypatch.setattr(module, "get_configs", MagicMock())
    # A different service's OSDs cannot supply the baseline.
    monkeypatch.setattr(
        module,
        "_get_host_lvm_list",
        lambda node: {"1": _osd(service="unrelated")},
    )
    cephadm = MagicMock()
    monkeypatch.setattr(module, "CephAdm", cephadm)
    with pytest.raises(OperationFailedError, match="requires existing OSDs"):
        module.run(cluster, config=config)
    cephadm.assert_not_called()


def test_non_collocated_reuses_actual_service_paths_and_requires_new_identity(
    monkeypatch,
):
    cluster, installer, node = _cluster()
    config = _config()
    config["reuse_existing_devices"] = True
    config["spec"]["spec"] = {"block_db_size": "12G"}
    original = deepcopy(config)
    service_lvm = {
        "1": _osd("/dev/vde", "/dev/vdc", fsid="old-first"),
        "2": _osd("/dev/vdf", "/dev/vdd", fsid="old-second"),
    }
    monkeypatch.setattr(non_collocated, "get_configs", MagicMock())
    monkeypatch.setattr(non_collocated, "_get_host_lvm_list", lambda node: service_lvm)
    generated = []

    def capture_spec(installer, spec):
        generated.append(deepcopy(spec))
        return "/tmp/spec.yaml"

    monkeypatch.setattr(non_collocated, "create_yaml_config", capture_spec)
    cephadm = MagicMock()
    monkeypatch.setattr(non_collocated, "CephAdm", cephadm)
    monkeypatch.setattr(non_collocated, "_remove_osd", MagicMock())
    monkeypatch.setattr(non_collocated, "_wait_for_osd_removal", MagicMock())
    wait = MagicMock()
    monkeypatch.setattr(non_collocated, "_wait_for_redeployment", wait)
    assert non_collocated.run(cluster, config=config) == 0
    assert config == original
    assert [spec["unmanaged"] for spec in generated] == [True, False]
    assert all(
        spec["spec"]
        == {
            "data_devices": {"paths": ["/dev/vde", "/dev/vdf"]},
            "db_devices": {"paths": ["/dev/vdc", "/dev/vdd"]},
            "block_db_size": "12G",
        }
        for spec in generated
    )
    before = non_collocated._get_osd_layout(service_lvm)
    wait.assert_called_once_with(
        installer, node, before, {before["1"][0]: {"old-first"}}
    )
    assert all(
        command.kwargs["check_ec"] is True
        for command in cephadm.return_value.ceph.orch.apply.call_args_list
    )


def test_full_host_replacement_waits_for_each_new_identity_before_next_removal(
    monkeypatch,
):
    cluster, installer, node = _cluster()
    config = _config()
    config["reuse_existing_devices"] = True
    config["spec"]["spec"] = {"block_db_size": "12G"}
    existing = {str(i): _osd(f"/dev/data{i}", fsid=f"old-{i}") for i in range(4)}
    monkeypatch.setattr(redeploy, "get_configs", MagicMock())
    monkeypatch.setattr(redeploy, "_get_host_lvm_list", lambda node: existing)
    cloud = MagicMock()
    monkeypatch.setattr(redeploy, "_add_node_volumes", cloud)
    generated = []

    def capture_spec(installer, spec):
        generated.append(deepcopy(spec))
        return "/tmp/spec.yaml"

    monkeypatch.setattr(redeploy, "create_yaml_config", capture_spec)
    cephadm = MagicMock()
    monkeypatch.setattr(redeploy, "CephAdm", cephadm)
    events = []
    monkeypatch.setattr(
        redeploy,
        "_remove_osd",
        lambda installer, osd_id: events.append(("remove", osd_id)),
    )
    monkeypatch.setattr(
        redeploy,
        "_wait_for_osd_removal",
        lambda installer: events.append(("queue-empty",)),
    )

    def wait_for_replacement(installer, node, before, replaced_fsids=None):
        events.append(("ready", deepcopy(replaced_fsids or {})))

    monkeypatch.setattr(redeploy, "_wait_for_redeployment", wait_for_replacement)
    assert redeploy.run(cluster, config=config) == 0
    expected_events = [("ready", {})]
    replaced = {}
    for osd_id in existing:
        replaced = {**replaced, (f"/dev/data{osd_id}",): {f"old-{osd_id}"}}
        expected_events.extend(
            [("remove", osd_id), ("queue-empty",), ("ready", deepcopy(replaced))]
        )
    assert events == expected_events
    cloud.assert_not_called()
    assert generated[0]["spec"]["data_devices"] == {
        "paths": [f"/dev/data{i}" for i in range(4)]
    }
    assert generated[0]["spec"]["db_devices"] == {"paths": ["/dev/vdc"]}
    assert generated[0]["spec"]["block_db_size"] == "12G"
    cephadm.return_value.ceph.orch.apply.assert_called_once_with(
        pos_args=[], input="/tmp/spec.yaml", check_ec=True
    )


def test_redeployment_waits_until_every_baseline_osd_is_up(monkeypatch):
    original = {"1": _osd(), "2": _osd("/dev/vdd", "/dev/vdc")}
    before = non_collocated._get_osd_layout(original)
    monkeypatch.setattr(non_collocated, "_get_host_lvm_list", lambda node: original)
    monkeypatch.setattr(non_collocated, "WaitUntil", lambda **kw: iter(range(2)))
    cephadm = MagicMock()
    cephadm.return_value.execute.side_effect = [
        (
            dumps(
                {
                    "nodes": [
                        {"type": "osd", "id": 1, "status": "up"},
                        {"type": "osd", "id": 2, "status": "down"},
                    ]
                }
            ),
            "",
        ),
        (
            dumps(
                {
                    "nodes": [
                        {"type": "osd", "id": 1, "status": "up"},
                        {"type": "osd", "id": 2, "status": "up"},
                    ]
                }
            ),
            "",
        ),
    ]
    monkeypatch.setattr(non_collocated, "CephAdm", cephadm)
    assert (
        non_collocated._wait_for_redeployment(
            "installer", SimpleNamespace(hostname="osd.example"), before
        )
        == before
    )
    assert cephadm.return_value.execute.call_count == 2
    assert all(
        item
        == call(
            cmd="cephadm shell -- ceph osd tree --format json", sudo=True, check_ec=True
        )
        for item in cephadm.return_value.execute.call_args_list
    )


def test_redeployment_times_out_when_an_osd_never_becomes_up(monkeypatch):
    original = {"1": _osd(), "2": _osd("/dev/vdd", "/dev/vdc")}
    before = non_collocated._get_osd_layout(original)
    monkeypatch.setattr(non_collocated, "_get_host_lvm_list", lambda node: original)
    monkeypatch.setattr(non_collocated, "WaitUntil", lambda **kw: iter(range(1)))
    cephadm = MagicMock()
    cephadm.return_value.execute.return_value = (
        dumps({"nodes": [{"type": "osd", "id": 1, "status": "up"}]}),
        "",
    )
    monkeypatch.setattr(non_collocated, "CephAdm", cephadm)
    with pytest.raises(OperationFailedError, match="OSDs are not up:.*2"):
        non_collocated._wait_for_redeployment(
            "installer", SimpleNamespace(hostname="osd.example"), before
        )


def test_redeployment_cannot_pass_failed_osd_tree_command(monkeypatch):
    original = {"1": _osd()}
    before = non_collocated._get_osd_layout(original)
    monkeypatch.setattr(non_collocated, "_get_host_lvm_list", lambda node: original)
    monkeypatch.setattr(non_collocated, "WaitUntil", lambda **kw: iter(range(1)))
    cephadm = MagicMock()
    failure = CommandFailed("OSD tree returned exit status 1")
    cephadm.return_value.execute.side_effect = failure
    monkeypatch.setattr(non_collocated, "CephAdm", cephadm)
    with pytest.raises(CommandFailed) as raised:
        non_collocated._wait_for_redeployment(
            "installer", SimpleNamespace(hostname="osd.example"), before
        )
    assert raised.value is failure
    cephadm.return_value.execute.assert_called_once_with(
        cmd="cephadm shell -- ceph osd tree --format json", sudo=True, check_ec=True
    )


@pytest.mark.parametrize("output", ["not JSON", "[]", "{}"])
def test_redeployment_rejects_invalid_osd_tree_response(monkeypatch, output):
    original = {"1": _osd()}
    before = non_collocated._get_osd_layout(original)
    monkeypatch.setattr(non_collocated, "_get_host_lvm_list", lambda node: original)
    monkeypatch.setattr(non_collocated, "WaitUntil", lambda **kw: iter(range(1)))
    cephadm = MagicMock()
    cephadm.return_value.execute.return_value = (output, "")
    monkeypatch.setattr(non_collocated, "CephAdm", cephadm)
    with pytest.raises(OperationFailedError, match="Invalid OSD tree"):
        non_collocated._wait_for_redeployment(
            "installer", SimpleNamespace(hostname="osd.example"), before
        )
