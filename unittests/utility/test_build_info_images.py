"""Unit tests for CephTestManifest image ref handling (datacenter aware)."""

import pytest

from cephci.utils.build_info import CephTestManifest

DIGEST = "79c0a55aff338be4196827c5d050384c0302e80ce44c84eb926b6cd7128bbc94"


def _details(**overrides):
    images = {
        "ceph-base": f"quay.io/rhceph-ci/rhceph@sha256:{DIGEST}",
        "grafana_image": f"quay.io/rhceph-ci/grafana@sha256:{DIGEST}",
        "crimson_image": f"quay.io/rhceph-ci/crimson@sha256:{DIGEST}",
        "nvmeof_cli_image": f"quay.io/rhceph-ci/nvmeof-cli@sha256:{DIGEST}",
        "cephcsi_image": "quay.io/rhceph-ci/cephcsi:latest",
    }
    images.update(overrides)
    return {
        "version": "19.2.1-1",
        "repositories": {"default": {"rhel-9": "http://repo"}},
        "repo_ids": {"rhel-9": "1"},
        "images": images,
    }


def _manifest(monkeypatch, datacenter, crimson=False):
    manifest = CephTestManifest(
        product="redhat",
        release="9.2",
        build_type="released",
        platform="rhel-9",
        datacenter=datacenter,
        crimson=crimson,
    )
    monkeypatch.setattr(manifest, "get_build_details", _details)
    return manifest


def test_default_datacenter_uses_digest_references(monkeypatch):
    manifest = _manifest(monkeypatch, "default")
    assert manifest.uses_tag_style_images is False
    assert manifest.ceph_image == f"quay.io/rhceph-ci/rhceph@sha256:{DIGEST}"
    assert manifest.ceph_image_dtr == "quay.io"
    assert manifest.ceph_image_path == "rhceph-ci/rhceph"
    assert manifest.ceph_image_tag == f"sha256:{DIGEST}"


def test_eu_de_keeps_tag_style_digests(monkeypatch):
    manifest = _manifest(monkeypatch, "eu-de")
    assert manifest.uses_tag_style_images is True
    assert manifest.ceph_image == f"quay.io/rhceph-ci/rhceph:{DIGEST}"
    assert manifest.ceph_image_tag == DIGEST
    assert manifest.ceph_image_path == "rhceph-ci/rhceph"
    assert (
        manifest.custom_images["grafana_image"] == f"quay.io/rhceph-ci/grafana:{DIGEST}"
    )
    assert manifest.nvme_cli_image == f"quay.io/rhceph-ci/nvmeof-cli:{DIGEST}"


def test_tag_style_manifest_is_normalized_for_default_datacenter(monkeypatch):
    manifest = _manifest(monkeypatch, "default")
    monkeypatch.setattr(
        manifest,
        "get_build_details",
        lambda: _details(**{"ceph-base": f"quay.io/rhceph-ci/rhceph:{DIGEST}"}),
    )
    assert manifest.ceph_image == f"quay.io/rhceph-ci/rhceph@sha256:{DIGEST}"
    assert manifest.ceph_image_tag == f"sha256:{DIGEST}"


def test_crimson_uses_crimson_image_parts(monkeypatch):
    manifest = _manifest(monkeypatch, "default", crimson=True)
    assert manifest.ceph_image_path == "rhceph-ci/crimson"


@pytest.mark.parametrize("datacenter", ["tucson", "default"])
def test_non_eu_de_datacenters_are_not_tag_style(monkeypatch, datacenter):
    assert _manifest(monkeypatch, datacenter).uses_tag_style_images is False
