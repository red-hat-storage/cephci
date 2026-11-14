"""Resolution of the build under test."""

import mock
import pytest

from cephci.orchestration import build
from cephci.orchestration.config import RunConfig


def make_config(**overrides):
    """Build a run configuration with the mandatory fields filled in."""
    defaults = {
        "suites": ["suite.yaml"],
        "cluster_conf_file": "conf.yaml",
        "inventory_file": None,
        "osp_cred_file": None,
        "product": "ibm",
        "release": "8.1",
        "platform": "rhel-9",
        "rhbuild": "8.1",
        "build": "nightly",
    }
    defaults.update(overrides)

    return RunConfig(**defaults)


def make_manifest(**overrides):
    """Build a stand in for the Ceph test manifest."""
    manifest = mock.MagicMock()
    manifest.repository = overrides.get("repository", "http://example.com/compose")
    manifest.ceph_image_dtr = overrides.get("dtr", "registry.example.com")
    manifest.ceph_image_path = overrides.get("path", "rhceph/rhceph-8-rhel9")
    manifest.ceph_image_tag = overrides.get("tag", "latest")
    manifest.ceph_version = overrides.get("version", "19.2.0-1")

    return manifest


class TestEffectiveBuild:
    """Selection of the manifest section to read."""

    def test_other_builds_are_left_alone(self):
        assert build.effective_build(make_config(build="nightly")) == "nightly"
        assert build.effective_build(make_config(build=None)) is None

    def test_released_is_used_when_published(self):
        with mock.patch.object(build, "CephTestManifest") as manifest:
            manifest.return_value.build_info = {"version": "19.2.0"}

            assert build.effective_build(make_config(build="released")) == "released"

    def test_released_falls_back_to_nightly(self):
        with mock.patch.object(build, "CephTestManifest") as manifest:
            type(manifest.return_value).build_info = mock.PropertyMock(
                side_effect=RuntimeError("no released section")
            )

            assert build.effective_build(make_config(build="released")) == "nightly"


class TestScrapeCephVersion:
    """Reading the version out of a compose."""

    def test_version_is_read_from_the_package_listing(self):
        listing = '<a href="ceph-common-19.2.0-1.el9cp.x86_64.rpm">'
        with mock.patch.object(build, "get_html_page", return_value=listing) as page:
            versions = build.scrape_ceph_version("http://example.com", "openstack")

        assert versions == ["19.2.0-1.el9cp"]
        page.assert_called_once_with(
            url="http://example.com/compose/Tools/x86_64/os/Packages/"
        )

    def test_ibm_cloud_uses_its_own_layout(self):
        with mock.patch.object(build, "get_html_page", return_value="") as page:
            build.scrape_ceph_version("http://example.com", "ibmc")

        page.assert_called_once_with(url="http://example.com/Tools/Packages/")

    def test_missing_listing_yields_no_version(self):
        with mock.patch.object(build, "get_html_page", return_value=""):
            assert build.scrape_ceph_version("http://example.com", "openstack") == []


class TestResolve:
    """The artifacts a run installs."""

    def test_manifest_supplies_the_artifacts(self):
        with mock.patch.object(build, "CephTestManifest") as manifest_cls, mock.patch(
            "cephci.orchestration.build.check_build_overrides", return_value=False
        ), mock.patch.object(build, "get_html_page", return_value=""):
            manifest_cls.return_value = make_manifest()
            artifacts = build.resolve(make_config())

        assert artifacts.base_url == "http://example.com/compose"
        assert artifacts.docker_registry == "registry.example.com"
        assert artifacts.docker_image == "rhceph/rhceph-8-rhel9"
        assert artifacts.docker_tag == "latest"
        assert artifacts.container_image == (
            "registry.example.com/rhceph/rhceph-8-rhel9:latest"
        )

    def test_command_line_overrides_win(self):
        config = make_config(
            rhs_ceph_repo="http://override/compose",
            docker_registry="registry.override.com",
            docker_image="ceph/ceph",
            docker_tag="8.1",
        )

        with mock.patch.object(build, "CephTestManifest") as manifest_cls, mock.patch(
            "cephci.orchestration.build.check_build_overrides", return_value=True
        ), mock.patch.object(build, "get_html_page", return_value=""):
            manifest_cls.return_value = make_manifest()
            artifacts = build.resolve(config)

        assert artifacts.base_url == "http://override/compose"
        assert artifacts.docker_registry == "registry.override.com"
        assert artifacts.container_image == "registry.override.com/ceph/ceph:8.1"

    @pytest.mark.parametrize("build_type", ["released", "cvp", "upstream", None])
    def test_unscrapeable_builds_are_not_fetched(self, build_type):
        config = make_config(build=build_type, upstream_build="squid")

        with mock.patch.object(build, "CephTestManifest") as manifest_cls, mock.patch(
            "cephci.orchestration.build.check_build_overrides", return_value=False
        ), mock.patch.object(build, "get_html_page") as page:
            manifest_cls.return_value = make_manifest()
            # A released build is only confirmed, never switched, when published.
            manifest_cls.return_value.build_info = {"version": "19.2.0"}
            artifacts = build.resolve(config)

        page.assert_not_called()
        assert "19.2.0-1" in artifacts.ceph_version

    def test_upstream_build_is_reported_as_a_version(self):
        config = make_config(build="upstream", upstream_build="squid")

        with mock.patch.object(build, "CephTestManifest") as manifest_cls, mock.patch(
            "cephci.orchestration.build.check_build_overrides", return_value=False
        ):
            manifest_cls.return_value = make_manifest()
            artifacts = build.resolve(config)

        assert sorted(artifacts.ceph_version.split(", ")) == ["19.2.0-1", "squid"]

    def test_scraped_version_joins_the_manifest_version(self):
        listing = '<a href="ceph-common-19.2.1-5.el9cp.x86_64.rpm">'

        with mock.patch.object(build, "CephTestManifest") as manifest_cls, mock.patch(
            "cephci.orchestration.build.check_build_overrides", return_value=False
        ), mock.patch.object(build, "get_html_page", return_value=listing):
            manifest_cls.return_value = make_manifest()
            artifacts = build.resolve(make_config(build="nightly"))

        assert sorted(artifacts.ceph_version.split(", ")) == [
            "19.2.0-1",
            "19.2.1-5.el9cp",
        ]
