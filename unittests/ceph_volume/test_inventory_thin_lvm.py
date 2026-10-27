"""Mocked reproducer and cleanup checks for hidden-LV inventory regressions."""

import json
from types import SimpleNamespace

import pytest

from cli.exceptions import ConfigError, OperationFailedError
from tests.ceph_volume import test_ceph_volume_inventory_thin_lvm as workflow


class _Host:
    hostname = "bootstrap"

    def __init__(self):
        self.commands = []
        self.failures = {}
        self.outputs = {}
        self.files = set()
        self.loops = {"/dev/loop0": "/var/tmp/unrelated.img"}
        self.pvs = {"/dev/vda3": "system-vg"}
        self.vg = None
        self.lv_rows = []
        self.allocated_output = None
        self.missing_hidden = False
        self.wrong_segtype = False
        self.foreign_vg_on_cleanup = False

    def exec_command(self, cmd, **kwargs):
        assert kwargs == {"sudo": True, "check_ec": True}
        self.commands.append(cmd)
        for prefix, error in self.failures.items():
            if cmd.startswith(prefix):
                raise error
        for prefix, output in self.outputs.items():
            if cmd.startswith(prefix):
                return output, ""
        words = cmd.split()
        if cmd.startswith("(set -C;"):
            image = cmd.split("> ")[1].rstrip(")")
            if image in self.files:
                raise OperationFailedError("File already exists")
            self.files.add(image)
        elif cmd.startswith("dd "):
            pass
        elif cmd.startswith("losetup --find"):
            loop = f"/dev/loop{41 + len(self.loops)}"
            self.loops[loop] = words[-1]
            return self.allocated_output or loop, ""
        elif cmd.startswith("losetup --list"):
            return (
                json.dumps(
                    {
                        "loopdevices": [
                            {"name": loop, "back-file": image}
                            for loop, image in self.loops.items()
                        ]
                    }
                ),
                "",
            )
        elif cmd.startswith("pvcreate "):
            self.pvs.update({loop: "" for loop in words[2:]})
        elif cmd.startswith("vgcreate "):
            self.vg = words[2]
            self.pvs.update({loop: self.vg for loop in words[3:]})
        elif cmd.startswith("lvcreate "):
            segtype = words[words.index("--type") + 1]
            name = words[words.index("--name") + 1]
            self.lv_rows = [
                {
                    "vg_name": self.vg,
                    "lv_name": name,
                    "segtype": "linear" if self.wrong_segtype else segtype,
                }
            ]
            hidden = (
                ["thinpool_tdata", "thinpool_tmeta"]
                if segtype == "thin-pool"
                else [
                    f"mirror_lv_{kind}_{index}"
                    for kind in ("rimage", "rmeta")
                    for index in range(2)
                ]
            )
            if not self.missing_hidden:
                self.lv_rows.extend(
                    {"vg_name": self.vg, "lv_name": f"[{name}]", "segtype": "linear"}
                    for name in hidden
                )
        elif cmd.startswith(("lvs ", f"{workflow.SHELL} lvs ")):
            return json.dumps({"report": [{"lv": self.lv_rows}]}), ""
        elif cmd.startswith("pvs "):
            rows = [
                {
                    "pv_name": pv,
                    "vg_name": (
                        "unowned-vg"
                        if self.foreign_vg_on_cleanup and pv.startswith("/dev/loop")
                        else vg
                    ),
                }
                for pv, vg in self.pvs.items()
            ]
            return json.dumps({"report": [{"pv": rows}]}), ""
        elif cmd.startswith("vgs "):
            return (
                json.dumps(
                    {
                        "report": [
                            {
                                "vg": [
                                    {
                                        "vg_name": self.vg,
                                        "pv_count": str(
                                            sum(
                                                vg == self.vg
                                                for vg in self.pvs.values()
                                            )
                                        ),
                                    }
                                ]
                            }
                        ]
                    }
                ),
                "",
            )
        elif cmd.startswith("wipefs "):
            return json.dumps({"signatures": []}), ""
        elif cmd.startswith("vgremove "):
            assert words[-1] == self.vg
            self.pvs = {pv: "" if vg == self.vg else vg for pv, vg in self.pvs.items()}
            self.vg = None
            self.lv_rows = []
        elif cmd.startswith("pvremove "):
            del self.pvs[words[-1]]
        elif cmd.startswith("losetup --detach"):
            assert words[-1] != "/dev/loop0"
            del self.loops[words[-1]]
        elif cmd.startswith("rm -- "):
            self.files.remove(words[-1])
        elif "ceph-volume inventory --format json" in cmd:
            assert "CEPH_VOLUME_ALLOW_LOOP_DEVICES=1" in cmd
            return (
                json.dumps(
                    [{"path": "/dev/vda"}]
                    + [
                        {"path": loop}
                        for loop, image in self.loops.items()
                        if image != "/var/tmp/unrelated.img"
                    ]
                ),
                "",
            )
        elif "ceph-volume inventory --format plain" in cmd:
            return "Device Path /dev/vda", ""
        else:
            raise AssertionError(f"Unexpected command: {cmd}")
        return "", ""


@pytest.fixture
def environment(monkeypatch):
    monkeypatch.setattr(workflow, "uuid4", lambda: SimpleNamespace(hex="a" * 32))
    host = _Host()
    roles = {"osd": [host], "installer": [host]}
    cluster = SimpleNamespace(get_nodes=lambda role: roles[role])
    return cluster, host, roles


def _assert_clean(host):
    assert host.files == set()
    assert host.loops == {"/dev/loop0": "/var/tmp/unrelated.img"}
    assert host.pvs == {"/dev/vda3": "system-vg"}
    assert host.vg is None


@pytest.mark.parametrize("fixture_type, count", [("thin-pool", 1), ("raid1", 2)])
def test_verified_reproducer_runs_both_inventory_formats_and_cleans_up(
    environment, fixture_type, count
):
    cluster, host, _ = environment

    assert workflow.run(cluster, config={"fixture_type": fixture_type}) == 0

    assert len([cmd for cmd in host.commands if cmd.startswith("dd ")]) == count
    inventory_commands = [
        cmd for cmd in host.commands if "ceph-volume inventory" in cmd
    ]
    assert inventory_commands == [
        f"{workflow.SHELL} ceph-volume inventory --format plain",
        f"{workflow.SHELL} ceph-volume inventory --format json",
    ]
    shell_commands = [cmd for cmd in host.commands if cmd.startswith("cephadm shell ")]
    for mount in (
        "/dev:/dev",
        "/run/udev:/run/udev",
        "/sys:/sys",
        "/run/lvm:/run/lvm",
        "/run/lock/lvm:/run/lock/lvm",
        "/:/rootfs",
    ):
        assert all(mount in cmd.split() for cmd in shell_commands)
    assert all(
        "--env CEPH_VOLUME_SKIP_RESTORECON=yes" in cmd
        and "--env CEPH_VOLUME_ALLOW_LOOP_DEVICES=1" in cmd
        for cmd in shell_commands
    )
    assert next(
        i for i, cmd in enumerate(host.commands) if cmd.startswith("lvs ")
    ) < host.commands.index(inventory_commands[0])
    assert next(
        i
        for i, cmd in enumerate(host.commands)
        if cmd.startswith(f"{workflow.SHELL} lvs ")
    ) < host.commands.index(inventory_commands[0])
    cleanup_prefixes = [
        cmd.split()[0]
        for cmd in host.commands
        if cmd.startswith(("vgremove", "pvremove", "losetup --detach", "rm --"))
    ]
    assert (
        cleanup_prefixes
        == ["vgremove"]
        + [name for _ in range(count) for name in ("pvremove", "losetup")]
        + ["rm"] * count
    )
    _assert_clean(host)


def test_thin_pool_is_default(environment):
    cluster, host, _ = environment
    assert workflow.run(cluster) == 0
    assert any("--type thin-pool" in cmd for cmd in host.commands)
    _assert_clean(host)


@pytest.mark.parametrize("fixture_type", ["thin-pool", "raid1"])
@pytest.mark.parametrize("failure", ["missing_hidden", "wrong_segtype"])
def test_inventory_is_not_run_without_verified_fixture(
    environment, fixture_type, failure
):
    cluster, host, _ = environment
    setattr(host, failure, True)

    with pytest.raises(OperationFailedError, match="LV"):
        workflow.run(cluster, config={"fixture_type": fixture_type})

    assert not any("ceph-volume inventory" in cmd for cmd in host.commands)
    _assert_clean(host)


@pytest.mark.parametrize("fixture_type", ["thin-pool", "raid1"])
@pytest.mark.parametrize(
    "failure", ["absent_vg", "missing_hidden", "wrong_hidden", "wrong_segtype"]
)
def test_container_must_see_verified_parent_and_hidden_lvs(
    environment, fixture_type, failure
):
    cluster, host, _ = environment
    name = "thinpool" if fixture_type == "thin-pool" else "mirror_lv"
    vg = f"cephci_inventory_{'a' * 32}"
    rows = [{"vg_name": vg, "lv_name": name, "segtype": fixture_type}]
    if failure == "absent_vg":
        rows = []
    elif failure == "wrong_hidden":
        rows.append(
            {"vg_name": vg, "lv_name": "[unrelated_metadata]", "segtype": "linear"}
        )
    elif failure == "wrong_segtype":
        rows[0]["segtype"] = "linear"
    host.outputs[f"{workflow.SHELL} lvs "] = json.dumps({"report": [{"lv": rows}]})

    with pytest.raises(OperationFailedError, match="LV"):
        workflow.run(cluster, config={"fixture_type": fixture_type})

    assert any(cmd.startswith("lvs ") for cmd in host.commands)
    assert any(cmd.startswith(f"{workflow.SHELL} lvs ") for cmd in host.commands)
    assert not any("ceph-volume inventory" in cmd for cmd in host.commands)
    _assert_clean(host)


def test_container_lvs_failure_preserves_error_and_cleans_up(environment):
    cluster, host, _ = environment
    primary = OperationFailedError("container cannot inspect fixture VG")
    host.failures[f"{workflow.SHELL} lvs "] = primary

    with pytest.raises(OperationFailedError) as raised:
        workflow.run(cluster)

    assert raised.value is primary
    assert not any("ceph-volume inventory" in cmd for cmd in host.commands)
    _assert_clean(host)


@pytest.mark.parametrize(
    "prefix", ["dd ", "losetup --find", "pvcreate ", "vgcreate ", "lvcreate ", "lvs "]
)
def test_setup_failure_preserves_primary_error_and_cleans_created_resources(
    environment, prefix
):
    cluster, host, _ = environment
    primary = OperationFailedError("fixture command failed")
    host.failures[prefix] = primary

    with pytest.raises(OperationFailedError) as raised:
        workflow.run(cluster)

    assert raised.value is primary
    _assert_clean(host)


@pytest.mark.parametrize(
    "returned",
    [
        "/dev/vdb",
        "/dev/loop0; unexpected-command",
        "/dev/loop42\n/dev/loop43",
        "/dev/loop0",
    ],
)
def test_untrusted_loop_output_never_reaches_pvcreate(environment, returned):
    cluster, host, _ = environment
    host.allocated_output = returned

    with pytest.raises(OperationFailedError, match="loop"):
        workflow.run(cluster)

    assert not any(cmd.startswith("pvcreate ") for cmd in host.commands)
    _assert_clean(host)


@pytest.mark.parametrize("output_format", ["plain", "json"])
def test_failed_inventory_command_cleans_fixture(environment, output_format):
    cluster, host, _ = environment
    primary = OperationFailedError("inventory exit status 1")
    host.failures[
        f"{workflow.SHELL} ceph-volume inventory --format {output_format}"
    ] = primary

    with pytest.raises(OperationFailedError) as raised:
        workflow.run(cluster)

    assert raised.value is primary
    _assert_clean(host)


@pytest.mark.parametrize(
    "output",
    ["Traceback (most recent call last):", "Segmentation fault (core dumped)", ""],
)
def test_crash_or_empty_output_cannot_pass_with_zero_status(environment, output):
    cluster, host, _ = environment
    host.outputs[f"{workflow.SHELL} ceph-volume inventory --format plain"] = output

    with pytest.raises(OperationFailedError, match="[Ii]nventory"):
        workflow.run(cluster)

    _assert_clean(host)


@pytest.mark.parametrize("output", ["not JSON", "[]", "{}", "[{}]"])
def test_json_inventory_must_be_valid_device_records(environment, output):
    cluster, host, _ = environment
    host.outputs[f"{workflow.SHELL} ceph-volume inventory --format json"] = output

    with pytest.raises(OperationFailedError, match="inventory"):
        workflow.run(cluster)

    _assert_clean(host)


def test_cleanup_error_does_not_replace_primary_inventory_failure(environment):
    cluster, host, _ = environment
    primary = OperationFailedError("primary inventory failure")
    host.failures[f"{workflow.SHELL} ceph-volume inventory --format plain"] = primary
    host.failures["vgremove "] = OperationFailedError("owned VG busy")

    with pytest.raises(OperationFailedError) as raised:
        workflow.run(cluster)

    assert raised.value is primary
    assert "owned VG busy" in str(raised.value.__cause__)
    assert "retained" in str(raised.value.__cause__)
    assert not any(
        cmd.startswith(("pvremove", "losetup --detach", "rm --"))
        for cmd in host.commands
    )


@pytest.mark.parametrize(
    "prefix", ["vgremove ", "pvremove ", "losetup --detach", "rm -- "]
)
def test_cleanup_failure_fails_an_otherwise_successful_workflow(environment, prefix):
    cluster, host, _ = environment
    host.failures[prefix] = OperationFailedError("cleanup failure")

    with pytest.raises(OperationFailedError, match="cleanup failure"):
        workflow.run(cluster)


def test_cleanup_refuses_pv_assigned_to_another_vg(environment):
    cluster, host, _ = environment
    host.foreign_vg_on_cleanup = True

    with pytest.raises(OperationFailedError, match="unowned VG"):
        workflow.run(cluster)

    assert not any(
        cmd.startswith(("vgremove", "pvremove", "losetup --detach", "rm --"))
        for cmd in host.commands
    )


def test_cleanup_refuses_owned_vg_with_foreign_member_pv(environment):
    cluster, host, _ = environment
    vg = f"cephci_inventory_{'a' * 32}"
    host.outputs["pvs "] = json.dumps(
        {
            "report": [
                {
                    "pv": [
                        {"pv_name": "/dev/loop42", "vg_name": vg},
                        {"pv_name": "/dev/vdb", "vg_name": vg},
                    ]
                }
            ]
        }
    )

    with pytest.raises(OperationFailedError, match="unowned member PV /dev/vdb"):
        workflow.run(cluster)

    assert not any(
        cmd.startswith(("vgremove", "pvremove", "losetup --detach", "rm --"))
        for cmd in host.commands
    )
    assert host.files


def test_cleanup_retains_created_pv_omitted_by_ownership_report(environment):
    cluster, host, _ = environment
    host.outputs["pvs "] = json.dumps(
        {"report": [{"pv": [{"pv_name": "/dev/vda3", "vg_name": "system-vg"}]}]}
    )

    with pytest.raises(OperationFailedError, match="Created PVs missing"):
        workflow.run(cluster)

    assert not any(
        cmd.startswith(("vgremove", "pvremove", "losetup --detach", "rm --"))
        for cmd in host.commands
    )
    assert host.files


def test_partial_pv_creation_with_hidden_signature_retains_resources(environment):
    cluster, host, _ = environment
    primary = OperationFailedError("pvcreate only partially completed")
    host.failures["pvcreate "] = primary
    host.outputs["wipefs "] = json.dumps({"signatures": [{"type": "LVM2_member"}]})

    with pytest.raises(OperationFailedError) as raised:
        workflow.run(cluster)

    assert raised.value is primary
    assert "has signatures" in str(raised.value.__cause__)
    assert not any(
        cmd.startswith(("vgremove", "pvremove", "losetup --detach", "rm --"))
        for cmd in host.commands
    )


@pytest.mark.parametrize("fixture_type", ["thin-pool", "raid1"])
def test_inventory_must_include_every_owned_loop(environment, fixture_type):
    cluster, host, _ = environment
    host.outputs[f"{workflow.SHELL} ceph-volume inventory --format json"] = json.dumps(
        [{"path": "/dev/vda"}]
    )

    with pytest.raises(OperationFailedError, match="omitted fixture loops"):
        workflow.run(cluster, config={"fixture_type": fixture_type})

    _assert_clean(host)


@pytest.mark.parametrize(
    "records", [[{}], [{"pv_name": "/dev/loop42"}], [{"pv_name": "", "vg_name": ""}]]
)
def test_incomplete_pv_ownership_cannot_trigger_destructive_cleanup(
    environment, records
):
    cluster, host, _ = environment
    host.outputs["pvs "] = json.dumps({"report": [{"pv": records}]})

    with pytest.raises(OperationFailedError, match="Invalid PV ownership"):
        workflow.run(cluster)

    assert not any(
        cmd.startswith(("vgremove", "pvremove", "losetup --detach", "rm --"))
        for cmd in host.commands
    )


@pytest.mark.parametrize(
    "record", [{}, {"name": "/dev/loop42"}, {"name": "/dev/loop42", "back-file": None}]
)
def test_incomplete_loop_ownership_retains_fixture_with_failure_evidence(
    environment, record
):
    cluster, host, _ = environment
    host.outputs["losetup --list"] = json.dumps({"loopdevices": [record]})

    with pytest.raises(OperationFailedError, match="loop ownership") as raised:
        workflow.run(cluster)

    assert "retained" in str(raised.value.__cause__)
    assert host.files
    assert not any(
        cmd.startswith(
            ("pvcreate", "vgremove", "pvremove", "losetup --detach", "rm --")
        )
        for cmd in host.commands
    )


def test_existing_image_is_not_overwritten_or_cleaned(environment):
    cluster, host, _ = environment
    path = f"/var/tmp/cephci-inventory-{'a' * 32}-0.img"
    host.files.add(path)

    with pytest.raises(OperationFailedError, match="File already exists"):
        workflow.run(cluster)

    assert host.files == {path}
    assert len(host.commands) == 1


@pytest.mark.parametrize("config", [None, [], {"fixture_type": "unknown"}])
def test_invalid_configuration_performs_no_commands(environment, config):
    cluster, host, _ = environment

    with pytest.raises(ConfigError):
        workflow.run(cluster, config=config)
    assert host.commands == []


def test_missing_osd_host_performs_no_commands(environment):
    cluster, host, roles = environment
    roles["osd"] = []

    with pytest.raises(ConfigError, match="OSD host"):
        workflow.run(cluster)
    assert host.commands == []
