"""Unit tests for deploy_rook_defaults_ceph helpers."""

from tests.ceph_installer.deploy_rook_defaults_ceph import (
    ClusterHealthError,
    ClusterUnreachableError,
    CrashDeployError,
    CrushDeviceClassError,
    CrushTopologyError,
    DeployRookDefaultsError,
    HostDeployError,
    MgrDeployError,
    MissingOsdVolumesError,
    MonDeployError,
    OsdDeployError,
    OsdsAlreadyPresentError,
    host_specs_for_cluster,
    osd_nodes_missing_volumes,
)


class _Role:
    def __init__(self, roles):
        self.role_list = roles


class _Node:
    def __init__(self, hostname, roles, volumes=0, node_id=None):
        self.hostname = hostname
        self.shortname = hostname
        self.id = node_id
        self.role = _Role(roles)
        self.volume_list = [object()] * volumes


def test_osd_nodes_missing_volumes_reports_osd_without_disks():
    nodes = [
        _Node("n1", ["mon", "osd"], volumes=2, node_id="node1"),
        _Node("n2", ["osd"], volumes=0, node_id="node2"),
        _Node("n3", ["client"], volumes=0, node_id="node4"),
    ]
    missing = osd_nodes_missing_volumes(nodes)
    assert len(missing) == 1
    assert "n2" in missing[0]
    assert "osd role but no volumes" in missing[0]


def test_osd_nodes_missing_volumes_ok_when_disks_present():
    nodes = [
        _Node("n1", ["osd"], volumes=6, node_id="node1"),
        _Node("client", ["client"], volumes=0, node_id="node4"),
    ]
    assert osd_nodes_missing_volumes(nodes) == []


def test_host_specs_skip_client_and_assign_racks_to_osd_roles():
    nodes = [
        _Node("host-a", ["_admin", "mon", "mgr", "osd"], volumes=6, node_id="node1"),
        _Node("host-b", ["mon", "mgr", "osd"], volumes=6, node_id="node2"),
        _Node("host-c", ["mon", "osd"], volumes=6, node_id="node3"),
        _Node("host-client", ["client"], volumes=0, node_id="node4"),
    ]
    specs, hostnames = host_specs_for_cluster(nodes)
    assert hostnames == ["host-a", "host-b", "host-c"]
    assert [s["nodes"][0] for s in specs] == ["node1", "node2", "node3"]
    assert [s["location"]["rack"] for s in specs] == ["rack0", "rack1", "rack2"]
    assert all(s["location"]["root"] == "default" for s in specs)
    assert all(s["labels"] == "apply-all-labels" for s in specs)


def test_host_specs_no_location_without_osd_role():
    nodes = [
        _Node("mon-only", ["mon", "mgr"], volumes=0, node_id="node1"),
        _Node("osd-node", ["osd"], volumes=3, node_id="node2"),
    ]
    specs, _ = host_specs_for_cluster(nodes)
    assert "location" not in specs[0]
    assert specs[1]["location"] == {"root": "default", "rack": "rack0"}


def test_specific_errors_are_deploy_rook_defaults_error():
    for cls in (
        ClusterUnreachableError,
        MissingOsdVolumesError,
        OsdsAlreadyPresentError,
        HostDeployError,
        MonDeployError,
        MgrDeployError,
        CrashDeployError,
        OsdDeployError,
        CrushDeviceClassError,
        CrushTopologyError,
        ClusterHealthError,
    ):
        assert issubclass(cls, DeployRookDefaultsError)
        err = cls("x")
        assert isinstance(err, DeployRookDefaultsError)
        assert type(err).__name__ == cls.__name__
