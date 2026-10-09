import pytest

from cli.exceptions import ConfigError
from cli.utilities.configs import get_registry_details
from utility.utils import (
    get_registry_info,
    registry_host_from_image,
    resolve_registry_host,
    resolve_registry_login,
)


def test_registry_host_from_image():
    assert (
        registry_host_from_image("preprod.icr.io/ibm/ceph/ceph:9.2") == "preprod.icr.io"
    )
    assert registry_host_from_image("") is None
    assert registry_host_from_image(None) is None


def test_resolve_registry_host_prefers_custom_config():
    host = resolve_registry_host(
        overrides={"bootstrap-registry": "registry.stage.redhat.io"},
        image="quay.io/ceph/ceph:latest",
        key="bootstrap-registry",
    )
    assert host == "registry.stage.redhat.io"


def test_resolve_registry_host_falls_back_to_image():
    host = resolve_registry_host(
        overrides={},
        image="preprod.icr.io/cp/ibm-ceph/ceph-9-rhel9:v9.2",
        key="bootstrap-registry",
    )
    assert host == "preprod.icr.io"


def test_resolve_registry_host_upgrade_key():
    host = resolve_registry_host(
        overrides={"upgrade-registry": "cp.icr.io"},
        image="preprod.icr.io/cp/ibm-ceph/ceph:tag",
        key="upgrade-registry",
    )
    assert host == "cp.icr.io"


def test_get_registry_info_and_login_args(monkeypatch):
    config = {
        "registries": {
            "preprod.icr.io": {
                "product": "ibm",
                "username": "preprod-user",
                "password": "preprod-pass",
            }
        }
    }
    monkeypatch.setattr("utility.utils.get_cephci_config", lambda: config)

    info = get_registry_info("preprod.icr.io")
    assert info["username"] == "preprod-user"
    assert info["password"] == "preprod-pass"

    args = resolve_registry_login("preprod.icr.io")
    assert args == {
        "registry-url": "preprod.icr.io",
        "registry-username": "preprod-user",
        "registry-password": "preprod-pass",
    }


def test_get_registry_info_accepts_user_alias(monkeypatch):
    config = {
        "registries": {
            "cp.icr.io": {
                "user": "legacy-user",
                "password": "legacy-pass",
            }
        }
    }
    monkeypatch.setattr("utility.utils.get_cephci_config", lambda: config)
    info = get_registry_info("cp.icr.io")
    assert info["username"] == "legacy-user"


def test_get_registry_info_missing_raises(monkeypatch):
    monkeypatch.setattr("utility.utils.get_cephci_config", lambda: {"registries": {}})
    with pytest.raises(KeyError):
        get_registry_info("missing.example")


def test_get_registry_details_uses_image_host(monkeypatch):
    config = {
        "registries": {
            "preprod.icr.io": {
                "username": "preprod-user",
                "password": "preprod-pass",
            }
        }
    }
    monkeypatch.setattr("utility.utils.get_cephci_config", lambda: config)

    details = get_registry_details(
        image="preprod.icr.io/cp/ibm-ceph/ceph-9-rhel10:v9.9.2",
    )
    assert details["registry-url"] == "preprod.icr.io"
    assert details["registry-username"] == "preprod-user"
    assert details["registry-password"] == "preprod-pass"


def test_get_registry_details_requires_host(monkeypatch):
    monkeypatch.setattr("utility.utils.get_cephci_config", lambda: {"registries": {}})
    with pytest.raises(ConfigError):
        get_registry_details()
