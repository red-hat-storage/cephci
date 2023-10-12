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


def get_registry_details(ibm_build=False):
    """Get registry credentials

    Args:
        ibm_build (bool): IBM build flag
    """
    # TODO (vamahaja): Retrieve credentials based on registry name
    build_type = "ibm" if ibm_build else "rh"

    # Get cephci configs
    config = get_cephci_config()

    # Get registry details
    creds = config.get(f"{build_type}_registry_credentials")
    if not creds:
        raise ConfigError("Failed to read registry credentials")

    # Create registry dict
    return {
        "registry-url": creds.get("registry"),
        "registry-username": creds.get("username"),
        "registry-password": creds.get("password"),
    }
