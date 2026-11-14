"""Creation of the system under test over the provider registry."""

import mock
import pytest

from cephci.orchestration import provisioning
from cephci.providers.base import CloudProvider, NodeLogin


class FakeNode:
    """Stands in for the node object a cloud returns."""

    def __init__(self, role="mon", hostname="node1"):
        self.ip_address = "10.0.0.5"
        self.hostname = hostname
        self.subnet = "10.0.0.0/24"
        self.id = "node-1"
        self.role = role
        self.root_login = True
        self.no_of_volumes = 2


class FakeProvider(CloudProvider):
    """A provider that records what the orchestration asked it to do."""

    name = "fake"

    def __init__(self, nodes=None):
        self.nodes = nodes or {"node1": FakeNode()}
        self.created = []
        self.cleaned = []
        self.prepared = 0

    def prepare(self, osp_cred, custom_config=None):
        self.prepared += 1

    def create(self, cluster, inventory, osp_cred, run_id, prefix, **kwargs):
        self.created.append((cluster, prefix, kwargs))
        return self.nodes

    def cleanup(self, osp_cred, prefix, custom_config=None):
        self.cleaned.append((prefix, custom_config))

    def login(self, node, osp_cred, custom_config=None):
        return NodeLogin(private_ip="192.168.0.5", ceph_nodename=node.hostname)


CLUSTER_CONF = {
    "globals": [
        {
            "ceph-cluster": {
                "name": "ceph",
                "networks": {"public": ["10.0.0.0/24"]},
                "node1": {"role": ["mon"]},
            }
        }
    ]
}


@pytest.fixture
def no_side_effects():
    """Keep the generic provisioning away from the network and the validators."""
    with mock.patch.object(provisioning, "validate_conf"), mock.patch.object(
        provisioning, "validate_image"
    ), mock.patch.object(
        provisioning, "resolve_use_ipv6", return_value=False
    ), mock.patch.object(
        provisioning, "time"
    ), mock.patch.object(
        provisioning, "CephNode"
    ) as ceph_node, mock.patch.object(
        provisioning, "Ceph"
    ) as ceph:
        yield ceph_node, ceph


def test_the_environment_is_built_from_the_provider(no_side_effects):
    ceph_node, ceph = no_side_effects
    provider = FakeProvider()

    clusters, clients = provisioning.create_nodes(
        CLUSTER_CONF, None, {}, "abc123", provider, "fake", "ci-abc"
    )

    assert list(clusters) == ["ceph"]
    assert clients == []
    assert provider.prepared == 1
    ceph.assert_called_once_with("ceph", [ceph_node.return_value])


def test_login_details_reach_the_node(no_side_effects):
    ceph_node, _ = no_side_effects

    provisioning.create_nodes(
        CLUSTER_CONF, None, {}, "abc123", FakeProvider(), "fake", "ci-abc"
    )

    created = ceph_node.call_args.kwargs
    assert created["private_ip"] == "192.168.0.5"
    assert created["ceph_nodename"] == "node1"
    assert created["username"] == "cephuser"
    assert created["root_password"] == "passwd"
    assert created["use_ipv6"] is False


def test_windows_clients_are_kept_apart(no_side_effects):
    provider = FakeProvider(nodes={"win1": FakeNode(role="win-iscsi-clients")})

    with mock.patch.object(provisioning, "WinNode") as win_node:
        _, clients = provisioning.create_nodes(
            CLUSTER_CONF, None, {}, "abc123", provider, "fake", "ci-abc"
        )

    assert clients == [win_node.return_value]
    win_node.assert_called_once_with(ip_address="10.0.0.5", private_ip="192.168.0.5")


def test_existing_environments_are_removed_first(no_side_effects):
    provider = FakeProvider()

    provisioning.create_nodes(
        CLUSTER_CONF, None, {}, "abc123", provider, "fake", "ci-abc"
    )

    assert provider.cleaned == [("ci-abc", None)]


def test_user_provided_infrastructure_is_never_removed(no_side_effects):
    provider = FakeProvider()
    provider.cleanup_before_create = False

    provisioning.create_nodes(
        CLUSTER_CONF, None, {}, "abc123", provider, "baremetal", "ci-abc"
    )

    assert provider.cleaned == []


@pytest.mark.parametrize(
    "cloud, expected",
    [
        ("onecloud", ["profile=small"]),
        ("ocpvirt", ["profile=small"]),
        # The other clouds resolve their platform from the credentials file.
        ("openstack", None),
        ("ibmc", None),
        ("aws", None),
    ],
)
def test_custom_config_reaches_only_the_clouds_that_use_it(cloud, expected):
    provider = FakeProvider()
    provider.name = cloud

    provisioning.cleanup_environment(provider, {}, "ci-abc", ["profile=small"])

    assert provider.cleaned == [("ci-abc", expected)]


def test_cluster_networks_are_carried_when_supported(no_side_effects):
    _, ceph = no_side_effects
    provider = FakeProvider()
    provider.supports_cluster_networks = True

    clusters = provisioning.create_nodes(
        CLUSTER_CONF, None, {}, "abc123", provider, "baremetal", "ci-abc"
    )[0]

    assert clusters["ceph"].networks == {"public": ["10.0.0.0/24"]}


def test_creation_is_retried_for_the_clouds_that_ask_for_it(no_side_effects):
    class Flaky(FakeProvider):
        retry_exceptions = (ValueError,)

        def __init__(self):
            super().__init__()
            self.attempts = 0

        def create(self, *args, **kwargs):
            self.attempts += 1
            if self.attempts < 2:
                raise ValueError("the cloud is busy")

            return super().create(*args, **kwargs)

    provider = Flaky()
    with mock.patch("utility.retry.time.sleep"):
        provisioning.create_nodes(
            CLUSTER_CONF, None, {}, "abc123", provider, "fake", "ci-abc"
        )

    assert provider.attempts == 2


def test_enable_eus_is_only_offered_to_openstack(no_side_effects):
    provider = FakeProvider()
    provider.name = "openstack"

    provisioning.create_nodes(
        CLUSTER_CONF, None, {}, "abc123", provider, "openstack", "ci-abc", True
    )

    assert provider.created[0][2]["enable_eus"] is True

    other = FakeProvider()
    provisioning.create_nodes(
        CLUSTER_CONF, None, {}, "abc123", other, "fake", "ci-abc", True
    )

    assert "enable_eus" not in other.created[0][2]
