"""Unit tests for utility.odf_topology."""

from utility.odf_defaults import APPLY_ODF_TOPOLOGY_KEY
from utility.odf_topology import (
    _hostnames_from_cluster,
    apply_odf_topology,
    apply_zone_failure_domains,
)


class _Node:
    def __init__(self, hostname, role="osd"):
        self.hostname = hostname
        self.shortname = hostname
        self.role = role


class _Cluster:
    def __init__(self, nodes):
        self._nodes = nodes

    def get_nodes(self, role=None):
        if role is None:
            return list(self._nodes)
        return [n for n in self._nodes if n.role == role]


def test_hostnames_from_cluster_dedupes():
    cluster = _Cluster(
        [
            _Node("h1", "osd"),
            _Node("h1", "mon"),
            _Node("h2", "osd"),
            _Node("h3", "osd"),
        ]
    )
    assert _hostnames_from_cluster(cluster) == ["h1", "h2", "h3"]


def test_apply_odf_topology_noop_without_flag():
    calls = []

    def shell_fn(args, check_status=True):
        calls.append(args)
        return "", ""

    apply_odf_topology(_Cluster([]), shell_fn, overrides={})
    assert calls == []


def test_apply_zone_failure_domains_skips_with_few_hosts():
    calls = []

    def shell_fn(args, check_status=True):
        calls.append(args)
        return "", ""

    apply_zone_failure_domains(shell_fn, ["h1", "h2"])
    assert calls == []


def test_apply_zone_failure_domains_issues_crush_cmds():
    calls = []

    def shell_fn(args, check_status=True):
        calls.append(list(args))
        return "", ""

    apply_zone_failure_domains(shell_fn, ["h1", "h2", "h3"])
    flat = [" ".join(c) for c in calls]
    assert any("add-bucket region-1 region" in x for x in flat)
    assert any("move h1 zone=zone-a" in x for x in flat)
    assert any("move h3 zone=zone-c" in x for x in flat)


def test_apply_odf_topology_runs_when_flag_set():
    calls = []

    def shell_fn(args, check_status=True):
        calls.append(list(args))
        return "0\n1\n2", ""

    cluster = _Cluster([_Node("a", "osd"), _Node("b", "osd"), _Node("c", "osd")])
    apply_odf_topology(
        cluster,
        shell_fn,
        overrides={APPLY_ODF_TOPOLOGY_KEY: "true"},
    )
    flat = [" ".join(c) for c in calls]
    assert any("crush rule create-replicated odf-block" in x for x in flat)
    assert any("set-device-class ssd" in x for x in flat)
    # msgr2 is bootstrap --config; not in default post-OSD steps
    assert not any("ms_bind_msgr1 false" in x for x in flat)
    assert not any("set-addrs" in x for x in flat)  # msgr2 not in default steps
