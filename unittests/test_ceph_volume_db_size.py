"""Reject missing OSDs, colocated DBs, and incorrect actual DB LV sizes."""

import json
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from cli.cephadm.cephadm import ceph_volume_shell
from cli.exceptions import ConfigError, OperationFailedError
from tests.ceph_volume import test_ceph_volume_wrong_db_size_compute as db_size

GIB = 1024**3
DB_DEVICES = ["/dev/vdb", "/dev/vdc"]
DATA_DEVICES = ["/dev/vdd", "/dev/vde", "/dev/vdf", "/dev/vdg"]


def _layout(
    data_devices=DATA_DEVICES, db_devices=DB_DEVICES, first_id=10, service_id="db-size"
):
    lvm, sizes = {}, {}
    for index, data in enumerate(data_devices):
        osd_id = str(first_id + index)
        db_path = f"/dev/ceph-vg/osd-db-{osd_id}"
        lvm[osd_id] = [
            {
                "type": "block",
                "devices": [data],
                "tags": {"ceph.osdspec_affinity": service_id},
            },
            {
                "type": "db",
                "devices": [db_devices[index % len(db_devices)]],
                "lv_path": db_path,
                "tags": {"ceph.block_db_size": str(12 * GIB)},
            },
        ]
        sizes[db_path] = 12 * GIB
    return lvm, sizes


def _config(hosts=None):
    return {
        "create_volumes": False,
        "timeout": 2,
        "interval": 1,
        "specs": {
            "service_type": "osd",
            "service_id": "db-size",
            "placement": {"hosts": hosts or ["node0"]},
            "data_devices": {"paths": ["PATH"]},
            "db_devices": {"paths": ["PATH"]},
            "block_db_size": "12G",
        },
    }


def _cluster(nodes, osd_nodes=None):
    cluster = MagicMock()
    cluster.get_nodes.side_effect = lambda role=None: (
        nodes
        if role is None
        else [nodes[0]] if role == "installer" else osd_nodes or nodes
    )
    return cluster


@pytest.mark.parametrize(
    "value, expected",
    [
        (12884901888, 12 * GIB),
        ("12884901888", 12 * GIB),
        ("12884901888.00", 12 * GIB),
        ("12G", 12 * GIB),
        ("12GiB", 12 * GIB),
        ("12GB", 12 * GIB),
        (" 1024M ", GIB),
        ("1.5G", GIB + GIB // 2),
        ("1T", 1024 * GIB),
    ],
)
def test_parse_db_size(value, expected):
    assert db_size._size_bytes(value) == expected


@pytest.mark.parametrize("value", [None, True, False, 0, -1, "0G", "12Q", "0.5", ""])
def test_reject_invalid_db_size(value):
    with pytest.raises(ConfigError):
        db_size._size_bytes(value)


def test_validate_all_requested_data_devices_and_actual_lv_bytes():
    lvm, sizes = _layout()
    # Unrelated preexisting OSDs neither satisfy nor obstruct this deployment.
    lvm["99"] = [{"type": "block", "devices": ["/dev/vdz"]}]
    assert db_size._validate_db_allocations(
        lvm, sizes, DATA_DEVICES, DB_DEVICES, 12 * GIB, {"99"}, "db-size"
    ) == {"10", "11", "12", "13"}


def test_configured_tags_cannot_hide_wrong_actual_db_size():
    lvm, sizes = _layout()
    sizes[lvm["10"][1]["lv_path"]] = 2 * GIB
    with pytest.raises(
        OperationFailedError, match="2147483648 bytes; expected 12884901888"
    ):
        db_size._validate_db_allocations(
            lvm, sizes, DATA_DEVICES, DB_DEVICES, 12 * GIB, set(), "db-size"
        )


@pytest.mark.parametrize("mode", ["missing_osd", "existing_ids", "unrelated_only"])
def test_existing_or_partial_osds_cannot_satisfy_all_data_devices(mode):
    lvm, sizes = _layout()
    original_ids = set()
    if mode == "missing_osd":
        del lvm["13"]
    elif mode == "existing_ids":
        original_ids = set(lvm)
    else:
        lvm = {"99": [{"type": "block", "devices": ["/dev/vdz"]}]}
    with pytest.raises(OperationFailedError, match="Missing new OSDs for data devices"):
        db_size._validate_db_allocations(
            lvm, sizes, DATA_DEVICES, DB_DEVICES, 12 * GIB, original_ids, "db-size"
        )


@pytest.mark.parametrize(
    "mode", ["missing", "colocated", "outside_pool", "empty_devices", "missing_lv"]
)
def test_reject_missing_or_invalid_dedicated_db(mode):
    lvm, sizes = _layout()
    if mode == "missing":
        lvm["10"] = lvm["10"][:1]
    elif mode == "colocated":
        lvm["10"][1]["devices"] = [DATA_DEVICES[0]]
    elif mode == "outside_pool":
        lvm["10"][1]["devices"] = ["/dev/vdz"]
    elif mode == "empty_devices":
        lvm["10"][1]["devices"] = []
    else:
        del sizes[lvm["10"][1]["lv_path"]]
    with pytest.raises(OperationFailedError):
        db_size._validate_db_allocations(
            lvm, sizes, DATA_DEVICES, DB_DEVICES, 12 * GIB, set(), "db-size"
        )


def test_reject_two_osds_on_one_requested_data_disk():
    lvm, sizes = _layout()
    lvm["14"] = deepcopy(lvm["10"])
    with pytest.raises(OperationFailedError, match="Multiple new OSDs"):
        db_size._validate_db_allocations(
            lvm, sizes, DATA_DEVICES, DB_DEVICES, 12 * GIB, set(), "db-size"
        )


def test_reject_osd_spanning_unselected_data_disk():
    lvm, sizes = _layout()
    lvm["10"][0]["devices"].append("/dev/vdz")
    with pytest.raises(OperationFailedError, match="unexpected data devices"):
        db_size._validate_db_allocations(
            lvm, sizes, DATA_DEVICES, DB_DEVICES, 12 * GIB, set(), "db-size"
        )


def test_read_actual_lv_sizes_bypasses_host_devices_file_for_container_created_pvs():
    node = MagicMock()
    node.exec_command.return_value = (
        json.dumps(
            {
                "report": [
                    {"lv": [{"lv_path": " /dev/ceph/db ", "lv_size": "12884901888.00"}]}
                ]
            }
        ),
        "",
    )
    assert db_size._get_lv_sizes(node) == {"/dev/ceph/db": 12 * GIB}
    node.exec_command.assert_called_once_with(
        cmd="lvs --devicesfile '' --reportformat json --units b --nosuffix -o lv_path,lv_size",
        sudo=True,
        check_ec=True,
    )


def test_inventory_reads_target_host_and_excludes_used_or_system_devices(monkeypatch):
    node = SimpleNamespace(hostname="osd-host")
    cephadm = MagicMock()
    cephadm.execute.return_value = (
        json.dumps(
            [
                {"path": "/dev/vda", "available": False},
                {"path": "/dev/vdc", "available": True},
                {"path": "/dev/vdb", "available": True},
                {"path": "/dev/vdd", "available": "true"},
                {"path": "/tmp/untrusted", "available": True},
            ]
        ),
        "",
    )
    factory = MagicMock(return_value=cephadm)
    monkeypatch.setattr(db_size, "CephAdm", factory)
    assert db_size._available_devices(node) == ["/dev/vdb", "/dev/vdc"]
    factory.assert_called_once_with(node)
    cephadm.execute.assert_called_once_with(
        cmd=f"{ceph_volume_shell()} ceph-volume inventory --format json",
        sudo=True,
        check_ec=True,
    )


def test_zero_based_placement_supports_multidigit_node_indices():
    nodes = [SimpleNamespace(hostname=f"host-{index}") for index in range(11)]
    cluster = _cluster(nodes, osd_nodes=[nodes[10]])
    assert db_size._resolve_target_nodes(cluster, ["node10"]) == [nodes[10]]
    assert db_size._resolve_target_nodes(cluster, ["host-10"]) == [nodes[10]]


@pytest.mark.parametrize(
    "hosts", [["node0"], ["node3"], ["missing"], ["node1", "node1"], [], "node1"]
)
def test_placement_rejects_non_osd_out_of_range_unknown_and_duplicate_hosts(hosts):
    nodes = [
        SimpleNamespace(hostname="installer"),
        SimpleNamespace(hostname="osd-host"),
    ]
    with pytest.raises(ConfigError):
        db_size._resolve_target_nodes(_cluster(nodes, [nodes[1]]), hosts)


@pytest.fixture
def run_environment(monkeypatch):
    node = SimpleNamespace(hostname="osd-host", ceph_nodename="cloud-osd-node1")
    cluster = _cluster([node])
    cephadm = MagicMock()
    cephadm.ceph.osd.tree.return_value = json.dumps(
        {
            "nodes": [
                {"type": "osd", "id": index, "status": "up"} for index in range(10, 14)
            ]
        }
    )
    monkeypatch.setattr(db_size, "CephAdm", MagicMock(return_value=cephadm))
    inventory = MagicMock(return_value=DB_DEVICES + DATA_DEVICES)
    monkeypatch.setattr(db_size, "_available_devices", inventory)
    lvm, sizes = _layout()
    existing = {"99": [{"type": "block", "devices": ["/dev/vdz"]}]}
    host_lvm = MagicMock(side_effect=[existing, lvm])
    monkeypatch.setattr(db_size, "_get_host_lvm", host_lvm)
    monkeypatch.setattr(db_size, "_get_lv_sizes", MagicMock(return_value=sizes))
    cloud = MagicMock()
    monkeypatch.setattr(db_size, "_add_node_volumes", cloud)
    get_configs = MagicMock()
    monkeypatch.setattr(db_size, "get_configs", get_configs)
    yaml = MagicMock(return_value="/tmp/db-size.yaml")
    monkeypatch.setattr(db_size, "create_yaml_config", yaml)
    return SimpleNamespace(
        node=node,
        cluster=cluster,
        cephadm=cephadm,
        inventory=inventory,
        host_lvm=host_lvm,
        cloud=cloud,
        get_configs=get_configs,
        yaml=yaml,
    )


def test_preattached_run_preserves_config_and_checks_all_four_new_osds(run_environment):
    env = run_environment
    config = _config()
    original = deepcopy(config)
    assert db_size.run(env.cluster, config=config) == 0
    assert config == original
    env.cloud.assert_not_called()
    env.get_configs.assert_not_called()
    env.inventory.assert_called_once_with(env.node)
    generated = env.yaml.call_args.args[1]
    assert generated["placement"]["hosts"] == [env.node.hostname]
    assert generated["data_devices"] == {"paths": DATA_DEVICES}
    assert generated["db_devices"] == {"paths": DB_DEVICES}
    assert generated["block_db_size"] == "12G"
    assert generated["service_id"] == "db-size"
    env.cephadm.ceph.orch.apply.assert_called_once_with(
        pos_args=[], input="/tmp/db-size.yaml", check_ec=True
    )


def test_dynamic_run_selects_only_newly_attached_devices_on_target_host(
    run_environment,
):
    env = run_environment
    config = _config()
    config.update(create_volumes=True, **{"cloud-type": "openstack"})
    env.inventory.side_effect = [["/dev/vdz"], ["/dev/vdz"] + DB_DEVICES + DATA_DEVICES]
    assert db_size.run(env.cluster, config=config) == 0
    env.cloud.assert_called_once_with([env.node], config, size=32, count=6)
    env.get_configs.assert_called_once_with()
    generated = env.yaml.call_args.args[1]
    assert "/dev/vdz" not in generated["data_devices"]["paths"]
    assert "/dev/vdz" not in generated["db_devices"]["paths"]


@pytest.mark.parametrize("mutation", ["missing_osd", "wrong_size", "osd_down"])
def test_polling_is_bounded_and_cannot_pass_partial_wrong_or_down_osds(
    run_environment, monkeypatch, mutation
):
    env = run_environment
    lvm, sizes = _layout()
    if mutation == "missing_osd":
        del lvm["13"]
    elif mutation == "wrong_size":
        sizes[lvm["10"][1]["lv_path"]] = 2 * GIB
    else:
        env.cephadm.ceph.osd.tree.return_value = json.dumps(
            {
                "nodes": [
                    {"type": "osd", "id": index, "status": "up"}
                    for index in range(10, 13)
                ]
            }
        )
    env.host_lvm.side_effect = None
    env.host_lvm.return_value = lvm
    monkeypatch.setattr(db_size, "_get_lv_sizes", MagicMock(return_value=sizes))
    monkeypatch.setattr(db_size, "WaitUntil", lambda **kwargs: iter([None, None]))
    with pytest.raises(OperationFailedError, match="Timed out validating DB size"):
        db_size._wait_for_db_osds(
            env.node,
            [(env.node, DATA_DEVICES, DB_DEVICES, set(), "db-size")],
            12 * GIB,
            2,
            1,
        )
    assert env.host_lvm.call_count == 2


def test_device_wait_cannot_take_devices_used_before_dynamic_attachment(monkeypatch):
    node = SimpleNamespace(hostname="osd-host")
    monkeypatch.setattr(
        db_size, "_available_devices", MagicMock(return_value=DB_DEVICES + DATA_DEVICES)
    )
    monkeypatch.setattr(db_size, "WaitUntil", lambda **kwargs: iter([None, None]))
    with pytest.raises(OperationFailedError, match="six available devices"):
        db_size._wait_for_devices(node, set(DB_DEVICES), 2, 1)


@pytest.mark.parametrize(
    "change",
    [
        "no_size",
        "bad_boolean",
        "bad_timeout",
        "cloud_without_type",
        "incomplete_service_ids",
    ],
)
def test_invalid_run_config_fails_before_remote_or_cloud_operations(
    run_environment, change
):
    env = run_environment
    config = _config()
    if change == "no_size":
        del config["specs"]["block_db_size"]
    elif change == "bad_boolean":
        config["create_volumes"] = "false"
    elif change == "bad_timeout":
        config["timeout"] = False
    elif change == "cloud_without_type":
        config["create_volumes"] = True
    else:
        config["service_ids"] = {"node9": "db-size-other"}
    with pytest.raises(ConfigError):
        db_size.run(env.cluster, config=config)
    env.cloud.assert_not_called()
    env.inventory.assert_not_called()
    env.yaml.assert_not_called()


def test_multiple_hosts_keep_separate_inventory_service_ids_and_complete_osd_checks(
    monkeypatch,
):
    installer = SimpleNamespace(hostname="installer", ceph_nodename="cloud-installer")
    first = SimpleNamespace(hostname="osd-first", ceph_nodename="cloud-node1")
    second = SimpleNamespace(hostname="osd-second", ceph_nodename="cloud-node2")
    cluster = _cluster([installer, first, second], [first, second])
    config = _config(["node1", "node2"])
    config["service_ids"] = {"node1": "db-first", "node2": "db-second"}
    original = deepcopy(config)
    second_db = ["/dev/sdb", "/dev/sdc"]
    second_data = ["/dev/sdd", "/dev/sde", "/dev/sdf", "/dev/sdg"]
    first_layout, first_sizes = _layout(service_id="db-first")
    second_layout, second_sizes = _layout(
        second_data, second_db, first_id=20, service_id="db-second"
    )
    inventory = MagicMock(
        side_effect=lambda node: (
            DB_DEVICES + DATA_DEVICES if node is first else second_db + second_data
        )
    )
    monkeypatch.setattr(db_size, "_available_devices", inventory)
    calls = {first.hostname: 0, second.hostname: 0}

    def host_lvm(node):
        calls[node.hostname] += 1
        if calls[node.hostname] == 1:
            return {}
        return first_layout if node is first else second_layout

    monkeypatch.setattr(db_size, "_get_host_lvm", host_lvm)
    monkeypatch.setattr(
        db_size,
        "_get_lv_sizes",
        lambda node: first_sizes if node is first else second_sizes,
    )
    cephadm = MagicMock()
    cephadm.ceph.osd.tree.return_value = json.dumps(
        {
            "nodes": [
                {"type": "osd", "id": osd_id, "status": "up"}
                for osd_id in list(first_layout) + list(second_layout)
            ]
        }
    )
    monkeypatch.setattr(db_size, "CephAdm", MagicMock(return_value=cephadm))
    yaml = MagicMock(side_effect=lambda node, spec: f"/tmp/{spec['service_id']}.yaml")
    monkeypatch.setattr(db_size, "create_yaml_config", yaml)
    cloud = MagicMock()
    monkeypatch.setattr(db_size, "_add_node_volumes", cloud)

    assert db_size.run(cluster, config=config) == 0

    assert config == original
    assert [call.args[0] for call in inventory.call_args_list] == [first, second]
    first_spec, second_spec = [call.args[1] for call in yaml.call_args_list]
    assert first_spec["service_id"] == "db-first"
    assert second_spec["service_id"] == "db-second"
    assert first_spec["placement"]["hosts"] == [first.hostname]
    assert second_spec["placement"]["hosts"] == [second.hostname]
    assert first_spec["data_devices"]["paths"] == DATA_DEVICES
    assert second_spec["data_devices"]["paths"] == second_data
    assert calls == {first.hostname: 2, second.hostname: 2}
    assert cephadm.ceph.orch.apply.call_count == 2
    cephadm.execute.assert_called_once_with(
        cmd="cephadm shell -- ceph orch device ls --refresh --format json",
        sudo=True,
        check_ec=True,
    )
    cloud.assert_not_called()


def test_dedicated_db_lv_cannot_be_shared_by_two_osds():
    lvm, sizes = _layout()
    lvm["11"][1]["lv_path"] = lvm["10"][1]["lv_path"]
    with pytest.raises(OperationFailedError, match="shared by multiple new OSDs"):
        db_size._validate_db_allocations(
            lvm, sizes, DATA_DEVICES, DB_DEVICES, 12 * GIB, set(), "db-size"
        )


def test_one_global_osd_id_cannot_satisfy_data_devices_on_multiple_hosts(
    run_environment, monkeypatch
):
    env = run_environment
    lvm, sizes = _layout()
    env.host_lvm.side_effect = None
    env.host_lvm.return_value = lvm
    monkeypatch.setattr(db_size, "WaitUntil", lambda **kwargs: iter([None, None]))
    deployments = [
        (env.node, DATA_DEVICES, DB_DEVICES, set(), "db-size"),
        (env.node, DATA_DEVICES, DB_DEVICES, set(), "db-size"),
    ]
    with pytest.raises(
        OperationFailedError, match="New OSD IDs appear on multiple hosts"
    ):
        db_size._wait_for_db_osds(env.node, deployments, 12 * GIB, 2, 1)


@pytest.mark.parametrize("affinity", [None, "", "competing-service", "osd.db-size"])
def test_every_new_osd_must_match_the_applied_service_affinity(affinity):
    lvm, sizes = _layout()
    # The last OSD has the correct data disk and 12 GiB DB; ownership alone fails.
    if affinity is None:
        del lvm["13"][0]["tags"]["ceph.osdspec_affinity"]
    else:
        lvm["13"][0]["tags"]["ceph.osdspec_affinity"] = affinity
    with pytest.raises(
        OperationFailedError,
        match="OSD 13 has ceph.osdspec_affinity.*expected 'db-size'",
    ):
        db_size._validate_db_allocations(
            lvm, sizes, DATA_DEVICES, DB_DEVICES, 12 * GIB, set(), "db-size"
        )


def test_correct_custom_service_affinity_is_accepted_for_all_new_osds():
    lvm, sizes = _layout(service_id="host-specific-service")
    assert db_size._validate_db_allocations(
        lvm, sizes, DATA_DEVICES, DB_DEVICES, 12 * GIB, set(), "host-specific-service"
    ) == {"10", "11", "12", "13"}


def test_run_rejects_competing_service_even_with_correct_devices_sizes_and_up_osds(
    run_environment, monkeypatch
):
    env = run_environment
    lvm, _ = _layout(service_id="competing-service")
    env.host_lvm.side_effect = [{}, lvm, lvm]
    monkeypatch.setattr(db_size, "WaitUntil", lambda **kwargs: iter([None, None]))
    with pytest.raises(
        OperationFailedError,
        match="ceph.osdspec_affinity 'competing-service'; expected 'db-size'",
    ):
        db_size.run(env.cluster, config=_config())
    env.cephadm.ceph.orch.apply.assert_called_once()
    env.cephadm.ceph.osd.tree.assert_not_called()
