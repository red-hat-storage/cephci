"""Unit tests for utility.odf_defaults (ODF Rook bootstrap profile merge)."""

import json
import os

import pytest
import yaml

from utility.odf_defaults import (
    APPLY_ODF_DEFAULTS_KEY,
    DEFAULT_ODF_PROFILE_PATH,
    apply_odf_defaults_to_bootstrap_config,
    apply_v2_only_mon_addrs,
    is_truthy,
    load_odf_defaults_profile,
    merge_odf_into_bootstrap_config,
    overrides_enabled,
    verify_odf_defaults,
)


def test_odf_profile_file_exists_and_has_required_keys():
    assert os.path.isfile(DEFAULT_ODF_PROFILE_PATH)
    assert DEFAULT_ODF_PROFILE_PATH.endswith(
        os.path.join("conf", "tentacle", "rook", "odf_rook_defaults.yaml")
    )
    profile = load_odf_defaults_profile()
    assert "global" in profile
    assert profile["global"]["mon_osd_full_ratio"] == "0.85"
    assert profile["global"]["mon_max_pg_per_osd"] == 1000
    assert profile["global"]["ms_bind_msgr1"] == "false"
    assert profile["global"]["rbd_default_map_options"] == "ms_mode=prefer-crc"
    assert profile["osd"]["osd_memory_target_cgroup_limit_ratio"] == "0.8"
    assert profile["mds"]["mds_cache_memory_limit"] == 3221225472


@pytest.mark.parametrize(
    "value,expected",
    [
        (True, True),
        ("true", True),
        ("True", True),
        ("1", True),
        ("yes", True),
        (False, False),
        ("false", False),
        ("0", False),
        (None, False),
        ("", False),
    ],
)
def test_is_truthy(value, expected):
    assert is_truthy(value) is expected


def test_overrides_enabled():
    assert overrides_enabled({APPLY_ODF_DEFAULTS_KEY: "true"}, APPLY_ODF_DEFAULTS_KEY)
    assert not overrides_enabled({}, APPLY_ODF_DEFAULTS_KEY)
    assert not overrides_enabled(None, APPLY_ODF_DEFAULTS_KEY)


def test_merge_suite_keys_win():
    odf = {
        "global": {
            "mon_max_pg_per_osd": 1000,
            "public_network": "10.0.0.0/24",
        },
        "osd": {"osd_memory_target_cgroup_limit_ratio": 0.8},
    }
    suite = {"global": {"public_network": "192.168.0.0/16"}}
    merged = merge_odf_into_bootstrap_config(suite, odf)
    assert merged["global"]["public_network"] == "192.168.0.0/16"
    assert merged["global"]["mon_max_pg_per_osd"] == 1000
    assert merged["osd"]["osd_memory_target_cgroup_limit_ratio"] == 0.8


def test_apply_odf_defaults_noop_without_flag():
    args = {"mon-ip": "node1"}
    apply_odf_defaults_to_bootstrap_config(args, overrides={})
    assert "config" not in args


def test_apply_odf_defaults_merges_when_flag_set():
    args = {
        "mon-ip": "node1",
        "config": {"global": {"public_network": "10.1.0.0/24"}},
    }
    apply_odf_defaults_to_bootstrap_config(
        args, overrides={APPLY_ODF_DEFAULTS_KEY: "true"}
    )
    assert args["config"]["global"]["public_network"] == "10.1.0.0/24"
    assert args["config"]["global"]["mon_osd_full_ratio"] == "0.85"
    assert args["config"]["global"]["ms_bind_msgr1"] == "false"
    assert args["config"]["global"]["rbd_default_map_options"] == "ms_mode=prefer-crc"
    assert args["config"]["mds"]["mds_cache_memory_limit"] == 3221225472


def test_apply_odf_topology_alone_merges_msgr2_bootstrap_keys():
    from utility.odf_defaults import APPLY_ODF_TOPOLOGY_KEY

    args = {"mon-ip": "node1"}
    apply_odf_defaults_to_bootstrap_config(
        args, overrides={APPLY_ODF_TOPOLOGY_KEY: "true"}
    )
    assert args["config"]["global"]["ms_bind_msgr1"] == "false"
    assert args["config"]["global"]["rbd_default_map_options"] == "ms_mode=prefer-crc"
    # Full ODF profile knobs should not be present for topology-only
    assert "mon_max_pg_per_osd" not in args["config"]["global"]


def test_apply_odf_defaults_creates_config_when_absent():
    args = {"mon-ip": "node1"}
    apply_odf_defaults_to_bootstrap_config(
        args, overrides={APPLY_ODF_DEFAULTS_KEY: True}
    )
    assert "mon_target_pg_per_osd" in args["config"]["global"]


def test_apply_v2_only_mon_addrs():
    calls = []

    def shell_fn(args, check_status=True):
        calls.append(list(args))
        cmd = " ".join(args)
        if "mon dump" in cmd and "-f" in args:
            return (
                json.dumps(
                    {
                        "mons": [
                            {
                                "name": "node1",
                                "public_addrs": {
                                    "addrvec": [
                                        {
                                            "type": "v2",
                                            "addr": "10.0.64.166:3300",
                                            "nonce": 0,
                                        },
                                        {
                                            "type": "v1",
                                            "addr": "10.0.64.166:6789",
                                            "nonce": 0,
                                        },
                                    ]
                                },
                            },
                            {
                                "name": "node2",
                                "public_addrs": {
                                    "addrvec": [
                                        {
                                            "type": "v2",
                                            "addr": "10.0.66.196:3300",
                                            "nonce": 0,
                                        },
                                        {
                                            "type": "v1",
                                            "addr": "10.0.66.196:6789",
                                            "nonce": 0,
                                        },
                                    ]
                                },
                            },
                        ]
                    }
                ),
                "",
            )
        if "mon dump" in cmd:
            return "epoch 4\n0: v2:10.0.64.166:3300/0 mon.node1\n", ""
        return "", ""

    apply_v2_only_mon_addrs(shell_fn)
    set_addrs = [c for c in calls if len(c) >= 4 and c[2] == "set-addrs"]
    assert len(set_addrs) == 2
    assert set_addrs[0] == [
        "ceph",
        "mon",
        "set-addrs",
        "node1",
        "[v2:10.0.64.166:3300/0]",
    ]
    assert set_addrs[1] == [
        "ceph",
        "mon",
        "set-addrs",
        "node2",
        "[v2:10.0.66.196:3300/0]",
    ]


def test_verify_odf_defaults_with_mock_shell():
    profile = load_odf_defaults_profile()

    def shell_fn(args):
        cmd = " ".join(args)
        if "config dump" in cmd:
            entries = []
            for section, opts in profile.items():
                for name, value in opts.items():
                    if name.startswith("mon_osd_"):
                        continue
                    entries.append(
                        {"section": section, "name": name, "value": str(value)}
                    )
            return json.dumps(entries), ""
        if "osd dump" in cmd:
            return (
                json.dumps(
                    {
                        "full_ratio": 0.85,
                        "backfillfull_ratio": 0.80,
                        "nearfull_ratio": 0.75,
                    }
                ),
                "",
            )
        if "mon dump" in cmd:
            return (
                json.dumps(
                    {
                        "mons": [
                            {
                                "name": "a",
                                "public_addrs": {
                                    "addrvec": [
                                        {
                                            "type": "v2",
                                            "addr": "10.0.0.1:3300",
                                            "nonce": 0,
                                        }
                                    ]
                                },
                            }
                        ]
                    }
                ),
                "",
            )
        return "", ""

    assert verify_odf_defaults(shell_fn, profile) == []


def test_verify_odf_defaults_flags_v1_in_monmap():
    profile = {
        "global": {"rbd_default_map_options": "ms_mode=prefer-crc"},
        "osd": {},
        "mds": {},
    }

    def shell_fn(args):
        cmd = " ".join(args)
        if "config dump" in cmd:
            return (
                json.dumps(
                    [
                        {
                            "section": "global",
                            "name": "rbd_default_map_options",
                            "value": "ms_mode=prefer-crc",
                        }
                    ]
                ),
                "",
            )
        if "osd dump" in cmd:
            return json.dumps({}), ""
        if "mon dump" in cmd:
            return (
                json.dumps(
                    {
                        "mons": [
                            {
                                "name": "a",
                                "public_addrs": {
                                    "addrvec": [
                                        {
                                            "type": "v2",
                                            "addr": "10.0.0.1:3300",
                                            "nonce": 0,
                                        },
                                        {
                                            "type": "v1",
                                            "addr": "10.0.0.1:6789",
                                            "nonce": 0,
                                        },
                                    ]
                                },
                            }
                        ]
                    }
                ),
                "",
            )
        return "", ""

    mismatches = verify_odf_defaults(shell_fn, profile)
    assert any("still has v1" in m for m in mismatches)


def test_profile_yaml_roundtrip_structure():
    with open(DEFAULT_ODF_PROFILE_PATH) as fh:
        data = yaml.safe_load(fh)
    assert set(data.keys()) == {"global", "osd", "mds"}


@pytest.mark.parametrize(
    "mons,use_msgr2,expected",
    [
        ("10.0.0.1,10.0.0.2", False, "10.0.0.1,10.0.0.2"),
        ("10.0.0.1,10.0.0.2", True, "10.0.0.1:3300,10.0.0.2:3300"),
        ("10.0.0.1:6789", True, "10.0.0.1:3300"),
        ("10.0.0.1:3300", True, "10.0.0.1:3300"),
        ("v2:10.0.0.1:3300/0", True, "10.0.0.1:3300"),
        ("v1:10.0.0.1:6789/0", True, "10.0.0.1:3300"),
        (["10.0.0.1", "10.0.0.2"], True, "10.0.0.1:3300,10.0.0.2:3300"),
    ],
)
def test_format_mons_for_kernel_mount(mons, use_msgr2, expected):
    from utility.odf_defaults import format_mons_for_kernel_mount

    assert format_mons_for_kernel_mount(mons, use_msgr2) == expected


@pytest.mark.parametrize(
    "use_msgr2,existing,expected",
    [
        (True, "", ",ms_mode=crc"),
        (True, "ms_mode=legacy", ""),
        (True, ",ms_mode=crc", ""),
        (False, "", ""),
    ],
)
def test_kernel_ms_mode_opt(use_msgr2, existing, expected):
    from utility.odf_defaults import kernel_ms_mode_opt

    assert kernel_ms_mode_opt(use_msgr2, existing) == expected


def test_is_msgr1_disabled():
    from utility.odf_defaults import is_msgr1_disabled

    class FakeClient:
        def __init__(self, out):
            self._out = out

        def exec_command(self, **kwargs):
            return self._out, 0

    assert is_msgr1_disabled(FakeClient("false\n")) is True
    assert is_msgr1_disabled(FakeClient("true\n")) is False
    assert is_msgr1_disabled(FakeClient("0")) is True
