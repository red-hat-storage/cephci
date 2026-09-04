import os

import yaml

from cli.exceptions import ConfigError


def get_cephci_config():
    """Get data from ~/.cephci.yaml"""
    # Create path for cephci.yaml config
    home_dir = os.path.expanduser("~")
    cfg_file = os.path.join(home_dir, ".cephci.yaml")

    # Read config file
    try:
        with open(cfg_file, "r") as yml:
            return yaml.safe_load(yml)
    except ConfigError:
        raise ConfigError("Failed to read ~/.cephci.yaml")


def get_registry_details(ibm_build=False, registry=None):
    """Get registry credentials

    Args:
        ibm_build (bool): IBM build flag
        registry (str): Registry URL — used to select the correct credential
            tier when multiple staging registries are configured
            (e.g. preprod.icr.io vs cp.stg.icr.io).
    """
    _vendor = "ibm" if ibm_build else "rh"

    # Get cephci configs
    config = get_cephci_config()

    # Detect tier from registry URL: preprod.icr.io -> "preprod", else "stage"
    _tier = None
    if ibm_build and registry:
        if "preprod" in registry:
            _tier = "preprod"
        elif "stg" in registry or "stage" in registry:
            _tier = "stage"

    # Try nested credentials.registry.<vendor>.<tier> path first
    creds = None
    if _tier:
        creds = (
            config.get("credentials", {})
            .get("registry", {})
            .get(_vendor, {})
            .get(_tier)
        )

    # Fall back to flat top-level key (legacy config layout)
    if not creds:
        creds = config.get(f"{_vendor}_registry_credentials")

    if not creds:
        raise ConfigError("Failed to read registry credentials")

    # Create registry dict
    return {
        "registry-url": creds.get("registry"),
        "registry-username": creds.get("username"),
        "registry-password": creds.get("password"),
    }
