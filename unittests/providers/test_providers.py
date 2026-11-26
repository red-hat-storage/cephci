"""The infrastructure backends and how they are selected."""

import mock
import pytest

from cephci.providers import (
    AWSProvider,
    BaremetalProvider,
    IBMCProvider,
    OCPVirtProvider,
    OneCloudProvider,
    OpenStackProvider,
    get_provider,
)


class FakeNode:
    """Stands in for the node object a cloud returns."""

    def __init__(self, **attributes):
        self.ip_address = "10.0.0.5"
        self.hostname = "node1.example.com"
        self.private_key = None
        self.root_password = None
        self.subnet = "10.0.0.0/24"
        self.id = "node-1"
        self.role = "mon"
        self.root_login = True
        self.no_of_volumes = 2
        self.node = mock.MagicMock()
        self.node.name = "ci-abc-node1"
        self.__dict__.update(attributes)

    def get_private_ip(self):
        return "192.168.0.5"


@pytest.mark.parametrize(
    "cloud, expected",
    [
        ("openstack", OpenStackProvider),
        ("ibmc", IBMCProvider),
        ("aws", AWSProvider),
        ("onecloud", OneCloudProvider),
        ("ocpvirt", OCPVirtProvider),
        ("baremetal", BaremetalProvider),
        # Any name mentioning baremetal is user provided infrastructure.
        ("baremetal-upi", BaremetalProvider),
    ],
)
def test_provider_selection(cloud, expected):
    assert isinstance(get_provider(cloud), expected)


@pytest.mark.parametrize("cloud", [None, "", "gcp"])
def test_unsupported_clouds_have_no_provider(cloud):
    assert get_provider(cloud) is None


def test_openstack_login_uses_the_instance_name():
    login = OpenStackProvider().login(FakeNode(), {})

    assert login.private_ip == "192.168.0.5"
    assert login.ceph_nodename == "ci-abc-node1"
    assert login.username == "cephuser"
    assert login.password == "cephuser"
    assert login.root_password == "passwd"
    assert login.look_for_key is False


def test_ibmc_login_uses_the_credentials_key():
    credentials = {"globals": {"ibm-credentials": {"private_key_path": "~/.ssh/ibm"}}}

    login = IBMCProvider().login(FakeNode(), credentials)

    assert login.look_for_key is True
    assert login.private_key_path.endswith("/.ssh/ibm")
    assert login.ceph_nodename == "node1.example.com"


def test_aws_login_tolerates_a_missing_key():
    login = AWSProvider().login(FakeNode(), {"globals": {"aws-credentials": {}}})

    assert login.private_key_path == ""
    assert login.look_for_key is True


def test_baremetal_login_prefers_the_node_details():
    node = FakeNode(private_key="/root/.ssh/id_rsa", root_password="secret")

    login = BaremetalProvider().login(node, {})

    assert login.private_key_path == "/root/.ssh/id_rsa"
    assert login.look_for_key is True
    assert login.root_password == "secret"


def test_baremetal_login_without_a_key_falls_back_to_a_password():
    login = BaremetalProvider().login(FakeNode(), {})

    assert login.private_key_path == ""
    assert login.look_for_key is False
    assert login.root_password == "passwd"


def test_onecloud_login_is_key_based():
    credentials = {
        "globals": {
            "onecloud-credentials": {
                "ssh_user": "onecloud-user",
                "private_key_path": "~/.ssh/onecloud",
                "bootstrap_key_path": "~/.ssh/bootstrap",
                "bootstrap_key_password": "phrase",
            }
        }
    }

    provider = OneCloudProvider()
    login = provider.login(FakeNode(), credentials)

    assert login.username == "onecloud-user"
    assert login.root_username == "onecloud-user"
    assert login.password == "cephuser"
    # Privilege escalation never uses a password on OneCloud.
    assert login.root_password == ""
    assert login.look_for_key is True
    assert login.private_key_path.endswith("/.ssh/onecloud")
    assert login.bootstrap_key_path.endswith("/.ssh/bootstrap")
    assert login.bootstrap_key_password == "phrase"


def test_onecloud_reads_the_credentials_of_the_cephci_configuration():
    credentials = {"credentials": {"cloud": {"onecloud": {"ssh_user": "root"}}}}

    login = OneCloudProvider().login(FakeNode(), credentials)

    assert login.username == "root"
    assert login.password == "passwd"


def test_ocpvirt_login_requires_a_key_to_look_for_one():
    with mock.patch(
        "cephci.providers.ocpvirt.resolve_ocpvirt_credentials", return_value={}
    ):
        login = OCPVirtProvider().login(FakeNode(), {})

    assert login.look_for_key is False

    with mock.patch(
        "cephci.providers.ocpvirt.resolve_ocpvirt_credentials",
        return_value={"private_key_path": "~/.ssh/ocp"},
    ):
        login = OCPVirtProvider().login(FakeNode(), {})

    assert login.look_for_key is True


def test_only_the_clouds_that_can_be_torn_down_are_cleaned_before_creation():
    assert OpenStackProvider().cleanup_before_create is True
    assert BaremetalProvider().cleanup_before_create is False


def test_onecloud_waits_longer_for_its_instances():
    assert OneCloudProvider().post_create_wait > OpenStackProvider().post_create_wait


def test_cleanup_is_delegated_to_the_cloud():
    with mock.patch("cephci.providers.openstack.cleanup_ceph_nodes") as cleanup:
        OpenStackProvider().cleanup({"globals": {}}, "ci-abc")

    cleanup.assert_called_once_with({"globals": {}}, "ci-abc")

    with mock.patch("cephci.providers.ibmc.cleanup_ibmc_ceph_nodes") as cleanup:
        IBMCProvider().cleanup({"globals": {}}, "ci-abc", custom_config=["a=b"])

    cleanup.assert_called_once_with({"globals": {}}, "ci-abc", custom_config=["a=b"])


def test_baremetal_cleanup_is_not_supported():
    with mock.patch("cephci.providers.base.log") as logger:
        BaremetalProvider().cleanup({}, "ci-abc")

    logger.warning.assert_called_once_with("Unknown cloud type.")


class TestCreation:
    """Each provider hands the right arguments to the cloud it wraps."""

    ARGUMENTS = ({"ceph-cluster": {}}, {"instance": {}}, {"globals": {}}, "abc123")

    def test_openstack(self):
        with mock.patch("cephci.providers.openstack.create_ceph_nodes") as create:
            OpenStackProvider().create(
                *self.ARGUMENTS,
                "ci-abc",
                custom_config=["a=b"],
                platform="rhel-9",
                enable_eus=True,
            )

        create.assert_called_once_with(
            *self.ARGUMENTS,
            "ci-abc",
            enable_eus=True,
            custom_config=["a=b"],
        )

    def test_ibmc(self):
        with mock.patch("cephci.providers.ibmc.create_ibmc_ceph_nodes") as create:
            IBMCProvider().create(*self.ARGUMENTS, "ci-abc", custom_config=["a=b"])

        create.assert_called_once_with(*self.ARGUMENTS, "ci-abc", ["a=b"])

    def test_aws(self):
        with mock.patch("cephci.providers.aws.create_aws_ceph_nodes") as create:
            AWSProvider().create(*self.ARGUMENTS, "ci-abc", custom_config=["a=b"])

        create.assert_called_once_with(*self.ARGUMENTS, "ci-abc", ["a=b"])

    def test_ocpvirt(self):
        with mock.patch("cephci.providers.ocpvirt.create_ocpvirt_ceph_nodes") as create:
            OCPVirtProvider().create(*self.ARGUMENTS, "ci-abc", custom_config=["a=b"])

        create.assert_called_once_with(*self.ARGUMENTS, "ci-abc", ["a=b"])

    def test_onecloud(self):
        with mock.patch(
            "cephci.providers.onecloud.create_onecloud_ceph_nodes"
        ) as create:
            OneCloudProvider().create(
                *self.ARGUMENTS, "ci-abc", custom_config=["a=b"], platform="rhel-9"
            )

        create.assert_called_once_with(
            *self.ARGUMENTS, "ci-abc", ["a=b"], platform="rhel-9"
        )

    def test_baremetal_only_needs_the_cluster(self):
        with mock.patch(
            "cephci.providers.baremetal.create_baremetal_ceph_nodes"
        ) as create:
            BaremetalProvider().create(*self.ARGUMENTS, "ci-abc")

        create.assert_called_once_with(self.ARGUMENTS[0])
