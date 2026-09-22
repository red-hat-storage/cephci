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


def registry_host_from_image(image):
    """Return registry host from a container image reference."""
    if not image or not isinstance(image, str):
        return None
    return image.split("/")[0] or None


def ibm_registry_tier_from_host(registry):
    """Map an IBM registry host/URL to a credentials.registry.ibm tier key."""
    if not registry:
        return None
    if "preprod.icr.io" in registry:
        return "preprod"
    if "cp.stg.icr.io" in registry:
        return "stage"
    if "stg" in registry or "stage" in registry:
        return "stage"
    return None


def get_registry_details(ibm_build=False, registry=None, image=None):
    """Get registry credentials

    Args:
        ibm_build (bool): IBM build flag
        registry (str): Registry URL or host — used to select the correct credential
            tier when multiple staging registries are configured
            (e.g. preprod.icr.io vs cp.stg.icr.io).
        image (str): Container image reference; registry host is derived when
            ``registry`` is not provided.
    """
    vendor = "ibm" if ibm_build else "rh"

    # Get cephci configs
    config = get_cephci_config()

    registry_host = registry or registry_host_from_image(image)
    tier = ibm_registry_tier_from_host(registry_host) if ibm_build else None

    # Try nested credentials.registry.<vendor>.<tier> path first
    creds = None
    if tier:
        creds = (
            config.get("credentials", {}).get("registry", {}).get(vendor, {}).get(tier)
        )

    # Fall back to flat top-level key (legacy config layout)
    if not creds:
        creds = config.get(f"{vendor}_registry_credentials")

    if not creds:
        raise ConfigError("Failed to read registry credentials")

    # Create registry dict
    return {
        "registry-url": registry_host or creds.get("registry"),
        "registry-username": creds.get("username"),
        "registry-password": creds.get("password"),
    }
