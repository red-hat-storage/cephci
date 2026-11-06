import os
from unittest import mock

import pytest

from utility.utils import (
    compose_container_image,
    custom_ceph_config,
    get_cephci_config,
    is_image_digest,
    normalize_container_image,
    split_container_image,
)

suite_config = {
    "global": {
        "osd_pool_default_pg_num": 64,
        "osd_default_pool_size": 2,
        "osd_pool_default_pgp_num": 64,
        "mon_max_pg_per_osd": 1024,
        "osd_objectstore": "bluestore",
    }
}
cli_config = [
    "osd_pool_default_pg_num=128",
    "osd_default_pool_size=2",
    "osd_pool_default_pgp_num=128",
    "mon_max_pg_per_osd=1024",
]


@pytest.fixture
def config_file(fixtures_dir):
    return os.path.join(fixtures_dir, "custom_ceph_config.yaml")


def test_custom_ceph_config_no_values():
    expected = {}
    result = custom_ceph_config(None, None, None)
    assert result == expected


def test_custom_ceph_config_suite_only():
    result = custom_ceph_config(suite_config, None, None)
    assert result == suite_config


def test_custom_ceph_config_cli_only():
    expected = {
        "global": {
            "osd_pool_default_pg_num": "128",
            "osd_default_pool_size": "2",
            "osd_pool_default_pgp_num": "128",
            "mon_max_pg_per_osd": "1024",
        }
    }
    result = custom_ceph_config(None, cli_config, None)
    assert result == expected


def test_custom_ceph_config_file_only(config_file):
    expected = {
        "global": {
            "osd_pool_default_pg_num": 64,
            "osd_default_pool_size": 2,
            "osd_pool_default_pgp_num": 64,
            "mon_max_pg_per_osd": 2048,
            "osd_journal_size": 10000,
        },
        "mon": {"mon_osd_full_ratio": 0.80, "mon_osd_nearfull_ratio": 0.70},
    }
    result = custom_ceph_config(None, None, config_file)
    assert result == expected


def test_custom_ceph_config_suite_and_cli():
    expected = {
        "global": {
            "osd_pool_default_pg_num": "128",
            "osd_default_pool_size": "2",
            "osd_pool_default_pgp_num": "128",
            "mon_max_pg_per_osd": "1024",
            "osd_objectstore": "bluestore",
        }
    }
    result = custom_ceph_config(suite_config, cli_config, None)
    assert result == expected


def test_custom_ceph_config_suite_and_file(config_file):
    expected = {
        "global": {
            "osd_pool_default_pg_num": 64,
            "osd_default_pool_size": 2,
            "osd_pool_default_pgp_num": 64,
            "mon_max_pg_per_osd": 2048,
            "osd_objectstore": "bluestore",
            "osd_journal_size": 10000,
        },
        "mon": {"mon_osd_full_ratio": 0.80, "mon_osd_nearfull_ratio": 0.70},
    }

    result = custom_ceph_config(suite_config, None, config_file)
    assert result == expected


def test_custom_ceph_config_cli_and_file(config_file):
    expected = {
        "global": {
            "osd_pool_default_pg_num": "128",
            "osd_default_pool_size": "2",
            "osd_pool_default_pgp_num": "128",
            "mon_max_pg_per_osd": "1024",
            "osd_journal_size": 10000,
        },
        "mon": {"mon_osd_full_ratio": 0.80, "mon_osd_nearfull_ratio": 0.70},
    }
    result = custom_ceph_config(None, cli_config, config_file)
    assert result == expected


def test_custom_ceph_config_all(config_file):
    expected = {
        "global": {
            "osd_pool_default_pg_num": "128",
            "osd_default_pool_size": "2",
            "osd_pool_default_pgp_num": "128",
            "mon_max_pg_per_osd": "1024",
            "osd_objectstore": "bluestore",
            "osd_journal_size": 10000,
        },
        "mon": {"mon_osd_full_ratio": 0.80, "mon_osd_nearfull_ratio": 0.70},
    }
    result = custom_ceph_config(suite_config, cli_config, config_file)
    assert result == expected


def test_get_cephi_config(monkeypatch, fixtures_dir):
    """Test loading of cephci configuration."""

    def mock_return(x):
        return fixtures_dir

    monkeypatch.setattr(os.path, "expanduser", mock_return)
    ceph_cfg = get_cephci_config()

    assert ceph_cfg.get("email", {}).get("address") == "cephci@redhat.com"


@mock.patch("os.path.expanduser")
def test_get_cephci_config_raises(mock_expanduser):
    """Test exception thrown when invalid file is provided."""
    try:
        mock_expanduser.return_value = "./"
        get_cephci_config()
    except IOError as exception:
        assert mock_expanduser.call_count == 1
        assert exception.errno == 2


DIGEST = "79c0a55aff338be4196827c5d050384c0302e80ce44c84eb926b6cd7128bbc94"


@pytest.mark.parametrize(
    "value,expected",
    [
        (DIGEST, True),
        (f"sha256:{DIGEST}", True),
        ("latest", False),
        ("8-178", False),
        (
            "sha256-79c0a55aff338be4196827c5d050384c0302e80ce44c84eb926b6cd7128bbc94",
            False,
        ),
        ("", False),
        (None, False),
    ],
)
def test_is_image_digest(value, expected):
    assert is_image_digest(value) is expected


@pytest.mark.parametrize(
    "registry,image,tag,expected",
    [
        (
            "quay.io",
            "rhceph-ci/rhceph",
            DIGEST,
            f"quay.io/rhceph-ci/rhceph@sha256:{DIGEST}",
        ),
        (
            "quay.io",
            "rhceph-ci/rhceph",
            f"sha256:{DIGEST}",
            f"quay.io/rhceph-ci/rhceph@sha256:{DIGEST}",
        ),
        (
            "quay.io",
            "rhceph-ci/rhceph",
            "latest",
            "quay.io/rhceph-ci/rhceph:latest",
        ),
        (
            "cp.stg.icr.io",
            "cp/ibm-ceph/ceph-8-rhel9",
            "8-178",
            "cp.stg.icr.io/cp/ibm-ceph/ceph-8-rhel9:8-178",
        ),
        (
            "registry.redhat.io",
            "rhceph/rhceph-8-rhel9",
            "latest",
            "registry.redhat.io/rhceph/rhceph-8-rhel9:latest",
        ),
    ],
)
def test_compose_container_image(registry, image, tag, expected):
    assert compose_container_image(registry, image, tag) == expected


@pytest.mark.parametrize(
    "ref,expected",
    [
        (
            f"quay.io/rhceph-ci/rhceph:{DIGEST}",
            f"quay.io/rhceph-ci/rhceph@sha256:{DIGEST}",
        ),
        (
            f"quay.io/rhceph-ci/rhceph:sha256:{DIGEST}",
            f"quay.io/rhceph-ci/rhceph@sha256:{DIGEST}",
        ),
        (
            f"quay.io/rhceph-ci/rhceph@sha256:{DIGEST}",
            f"quay.io/rhceph-ci/rhceph@sha256:{DIGEST}",
        ),
        (
            "quay.io/rhceph-ci/keepalived:861cc2046a51ed5063bfaa63f06671092bdab08d1e02a43ebd410ba7e97c596d",
            "quay.io/rhceph-ci/keepalived@sha256:861cc2046a51ed5063bfaa63f06671092bdab08d1e02a43ebd410ba7e97c596d",
        ),
        (
            "cp.stg.icr.io/cp/ibm-ceph/ceph-8-rhel9:8-178",
            "cp.stg.icr.io/cp/ibm-ceph/ceph-8-rhel9:8-178",
        ),
        (
            "registry.redhat.io/rhceph/rhceph-8-rhel9:latest",
            "registry.redhat.io/rhceph/rhceph-8-rhel9:latest",
        ),
        (
            "localhost:5000/ceph/ceph:latest",
            "localhost:5000/ceph/ceph:latest",
        ),
    ],
)
def test_normalize_container_image(ref, expected):
    assert normalize_container_image(ref) == expected


def test_split_container_image_digest_and_tag():
    registry, path, tag = split_container_image(f"quay.io/rhceph-ci/rhceph:{DIGEST}")
    assert registry == "quay.io"
    assert path == "rhceph-ci/rhceph"
    assert tag == f"sha256:{DIGEST}"

    registry, path, tag = split_container_image(
        "cp.stg.icr.io/cp/ibm-ceph/ceph-8-rhel9:8-178"
    )
    assert registry == "cp.stg.icr.io"
    assert path == "cp/ibm-ceph/ceph-8-rhel9"
    assert tag == "8-178"
