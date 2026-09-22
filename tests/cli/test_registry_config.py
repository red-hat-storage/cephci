import pytest

from cli.utilities.configs import (
    get_registry_details,
    ibm_registry_tier_from_host,
    registry_host_from_image,
)


def test_registry_host_from_image():
    assert (
        registry_host_from_image("preprod.icr.io/ibm/ceph/ceph:9.2") == "preprod.icr.io"
    )
    assert registry_host_from_image("") is None
    assert registry_host_from_image(None) is None


@pytest.mark.parametrize(
    "registry,expected",
    [
        ("preprod.icr.io", "preprod"),
        ("preprod.icr.io/ibm/ceph/ceph:9.2", "preprod"),
        ("cp.stg.icr.io", "stage"),
        ("cp.stg.icr.io/ibm/ceph/ceph:9.2", "stage"),
        ("cp.icr.io", None),
        ("registry.redhat.io", None),
    ],
)
def test_ibm_registry_tier_from_host(registry, expected):
    assert ibm_registry_tier_from_host(registry) == expected


def test_get_registry_details_prefers_image_registry(monkeypatch):
    config = {
        "ibm_registry_credentials": {
            "registry": "cp.stg.icr.io",
            "username": "legacy-user",
            "password": "legacy-pass",
        },
        "credentials": {
            "registry": {
                "ibm": {
                    "preprod": {
                        "registry": "cp.stg.icr.io",
                        "username": "preprod-user",
                        "password": "preprod-pass",
                    }
                }
            }
        },
    }
    monkeypatch.setattr("cli.utilities.configs.get_cephci_config", lambda: config)

    details = get_registry_details(
        ibm_build=True,
        image="preprod.icr.io/cp/ibm-ceph/ceph-9-rhel10:v9.9.2",
    )

    assert details["registry-url"] == "preprod.icr.io"
    assert details["registry-username"] == "preprod-user"
    assert details["registry-password"] == "preprod-pass"
